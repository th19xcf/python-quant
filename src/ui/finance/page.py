#!/usr/bin/env python3

"""「财务分析」页：财务指标卡片 + 近四期趋势表 + 近期资讯表。

设计约束：
- 取数全部走 ``global_task_manager`` 后台线程，UI 线程不做任何网络调用；
- 全局任务信号的每个回调都先按 ``task_id`` 过滤，再按请求 token 防旧结果覆盖新标的；
- 页面销毁/主程序退出时主动断开任务信号，避免回调访问已销毁控件；
- 任何空数据/异常都进入可读状态页，不渲染半成品。
"""

from __future__ import annotations

import contextlib
from typing import Any

from loguru import logger
from PySide6.QtCore import Qt, QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QSplitter,
    QStackedWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from src.data.services.finance_service import (
    METRIC_DEFS,
    fetch_finance_snapshot,
    normalize_security_code,
)
from src.utils.code_converter import is_index_code

# 指标分组：与 finance_service.METRIC_DEFS 的 key 对齐
_METRIC_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ('盈利能力', ('roe', 'net_margin', 'gross_margin')),
    ('成长性', ('revenue_yoy', 'profit_yoy', 'eps')),
    ('偿债与运营', ('debt_ratio', 'current_ratio', 'quick_ratio', 'inventory_turnover')),
    ('估值', ('pe_ttm', 'pe_static', 'pb', 'total_mv', 'float_mv')),
)
_METRIC_LABELS = dict(METRIC_DEFS)

_TASK_TIMEOUT_SECONDS = 90


def _fetch_finance_task(code: str, name: str, token: int, refresh: bool = False,
                        task_id: str | None = None, signals: Any = None) -> dict[str, Any]:
    """后台任务入口：只返回普通 dict，不触碰任何 Qt 对象。"""
    try:
        snapshot = fetch_finance_snapshot(code, name, refresh=refresh,
                                          use_cache=not refresh)
    except Exception as exc:  # noqa: BLE001 - 后台线程必须兜底，异常回投主线程
        message = f'{type(exc).__name__}: {exc}'
        logger.error(f'[finance-ui] 后台取数异常 {code}: {message}')
        return {'token': token, 'code': code, 'snapshot': None, 'error': message}
    return {'token': token, 'code': code, 'snapshot': snapshot, 'error': ''}


class FinancePage(QWidget):
    """财务分析主页面。"""

    def __init__(self, host: Any = None, task_manager: Any = None,
                 parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName('financePage')
        self.host = host
        self._task_manager = task_manager
        self._target_code = ''
        self._target_name = ''
        self._loaded_code = ''
        self._dirty = False
        self._loading = False
        self._task_id: str | None = None
        self._token = 0
        self._alive = True
        self._news_items: list[dict[str, Any]] = []
        self._metric_labels: dict[str, QLabel] = {}
        self._metric_values: dict[str, QLabel] = {}
        self._build_ui()
        self._bind_task_manager()
        self._bind_lifecycle()

    # ------------------------------------------------------------------ UI
    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(10, 8, 10, 8)
        root.setSpacing(6)

        header = QHBoxLayout()
        title_box = QVBoxLayout()
        self.title_label = QLabel('财务分析')
        self.title_label.setObjectName('financeTitle')
        self.meta_label = QLabel('请选择一只股票后查看财务指标与资讯')
        self.meta_label.setObjectName('financeMeta')
        self.meta_label.setWordWrap(True)
        title_box.addWidget(self.title_label)
        title_box.addWidget(self.meta_label)
        header.addLayout(title_box, 1)

        self.refresh_button = QPushButton('重新获取')
        self.refresh_button.setObjectName('financeRefresh')
        self.refresh_button.setCursor(Qt.CursorShape.PointingHandCursor)
        self.refresh_button.clicked.connect(self._on_refresh_clicked)
        header.addWidget(self.refresh_button, 0, Qt.AlignmentFlag.AlignTop)
        root.addLayout(header)

        self.stack = QStackedWidget()
        self.content = self._build_content()
        self.state_label = QLabel()
        self.state_label.setObjectName('financeState')
        self.state_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.state_label.setWordWrap(True)
        self.stack.addWidget(self.content)
        self.stack.addWidget(self.state_label)
        root.addWidget(self.stack, 1)

        self.setStyleSheet("""
            QWidget#financePage { background-color: #0b0f14; color: #c8d3e0;
                font-family: "Microsoft YaHei"; font-size: 12px; }
            QLabel#financeTitle { color: #e6edf5; font-size: 17px; font-weight: bold; }
            QLabel#financeMeta { color: #7f8fa6; font-size: 11px; }
            QLabel#financeState { color: #8fa3b8; font-size: 13px; padding: 28px; }
            QFrame#financeCard { background-color: #111823; border: 1px solid #223042;
                border-radius: 6px; }
            QLabel#cardTitle { color: #e6edf5; font-size: 13px; font-weight: bold;
                padding-bottom: 4px; }
            QLabel#metricName { color: #7f8fa6; font-size: 11px; }
            QLabel#metricValue { color: #e6edf5; font-size: 15px; font-weight: bold; }
            QLabel#sectionTitle { color: #e6edf5; font-size: 13px; font-weight: bold; }
            QLabel#emptyHint { color: #6f8299; font-size: 12px; padding: 12px; }
            QPushButton#financeRefresh { background-color: #17222f; color: #c8d3e0;
                border: 1px solid #2a3a4d; border-radius: 4px; padding: 4px 12px; }
            QPushButton#financeRefresh:hover { background-color: #1e2c3d; }
            QPushButton#financeRefresh:disabled { color: #56677a; }
            QTableWidget { background-color: #0f1620; color: #c8d3e0;
                gridline-color: #1e2a37; font-size: 12px; }
            QHeaderView::section { background-color: #17222f; color: #9fb2c5;
                border: 1px solid #223042; padding: 4px; }
            QTableWidget::item:selected { background-color: #1f3b57; color: #ffffff; }
        """)

    def _build_content(self) -> QWidget:
        content = QWidget()
        layout = QVBoxLayout(content)
        layout.setContentsMargins(0, 0, 0, 0)

        splitter = QSplitter(Qt.Orientation.Vertical)
        splitter.addWidget(self._build_financial_panel())
        splitter.addWidget(self._build_news_panel())
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)
        layout.addWidget(splitter)
        return content

    def _build_financial_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        cards = QGridLayout()
        cards.setSpacing(8)
        for index, (title, keys) in enumerate(_METRIC_GROUPS):
            card = QFrame()
            card.setObjectName('financeCard')
            card_layout = QVBoxLayout(card)
            card_layout.setContentsMargins(10, 8, 10, 8)
            card_layout.setSpacing(2)
            title_label = QLabel(title)
            title_label.setObjectName('cardTitle')
            card_layout.addWidget(title_label)
            grid = QGridLayout()
            grid.setHorizontalSpacing(14)
            grid.setVerticalSpacing(2)
            for pos, key in enumerate(keys):
                name_label = QLabel(_METRIC_LABELS.get(key, key))
                name_label.setObjectName('metricName')
                value_label = QLabel('—')
                value_label.setObjectName('metricValue')
                grid.addWidget(name_label, pos // 2 * 2, pos % 2)
                grid.addWidget(value_label, pos // 2 * 2 + 1, pos % 2)
                self._metric_labels[key] = name_label
                self._metric_values[key] = value_label
            card_layout.addLayout(grid)
            cards.addWidget(card, index // 2, index % 2)
        layout.addLayout(cards)

        self.financial_empty = QLabel('暂无财务指标（接口未返回或该标的未披露财报，详见日志）')
        self.financial_empty.setObjectName('emptyHint')
        self.financial_empty.setVisible(False)
        layout.addWidget(self.financial_empty)

        history_title = QLabel('近四期主要指标')
        history_title.setObjectName('sectionTitle')
        layout.addWidget(history_title)
        self.history_table = self._make_table(['指标'])
        self.history_empty = QLabel('暂无历史报告期数据')
        self.history_empty.setObjectName('emptyHint')
        layout.addWidget(self.history_table, 1)
        layout.addWidget(self.history_empty)
        return panel

    def _build_news_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        title = QLabel('近期资讯（公告 / 新闻 / 业绩预告 / 分红 / 风险提示）')
        title.setObjectName('sectionTitle')
        layout.addWidget(title)

        self.news_table = self._make_table(['时间', '类型', '标题', '来源'])
        header = self.news_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        self.news_table.cellClicked.connect(self._on_news_clicked)
        self.news_table.cellDoubleClicked.connect(self._on_news_double_clicked)
        layout.addWidget(self.news_table, 1)

        self.news_empty = QLabel('近一年暂无相关资讯')
        self.news_empty.setObjectName('emptyHint')
        layout.addWidget(self.news_empty)

        self.news_detail = QLabel('单击条目查看摘要，双击有原文链接的新闻可直接打开。')
        self.news_detail.setObjectName('financeMeta')
        self.news_detail.setWordWrap(True)
        self.news_detail.setMinimumHeight(38)
        layout.addWidget(self.news_detail)
        return panel

    @staticmethod
    def _make_table(headers: list[str]) -> QTableWidget:
        table = QTableWidget(0, len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        table.verticalHeader().setVisible(False)
        table.setAlternatingRowColors(False)
        return table

    # -------------------------------------------------------------- 生命周期
    def _bind_task_manager(self) -> None:
        if self._task_manager is None:
            try:
                from src.ui.task_manager import global_task_manager  # noqa: PLC0415

                self._task_manager = global_task_manager
            except (ImportError, RuntimeError) as exc:
                logger.error(f'[finance-ui] 任务管理器不可用，财务页将无法后台取数: {exc}')
                self._task_manager = None

    def _bind_lifecycle(self) -> None:
        app = QApplication.instance()
        if app is not None:
            with contextlib.suppress(RuntimeError, TypeError):
                app.aboutToQuit.connect(self._on_app_quit)

    def closeEvent(self, event):  # noqa: N802 - Qt 命名
        self._on_app_quit()
        super().closeEvent(event)

    def _on_app_quit(self) -> None:
        """窗口关闭/主程序退出：置死并断开全局任务信号，避免回调访问已销毁控件。"""
        self._alive = False
        self._invalidate_inflight('页面关闭或程序退出')

    # ------------------------------------------------------------ 标的切换
    def set_stock(self, code: Any, name: str = '', *, force: bool = False) -> None:
        """由主窗口在切换标的时调用（UI 线程）。"""
        if not self._alive:
            logger.warning('[finance-ui] 页面已销毁，忽略标的切换')
            return
        if not code:
            logger.warning('[finance-ui] 收到空标的代码，忽略切换')
            return
        ts_code = normalize_security_code(code)
        changed = ts_code != self._target_code
        if changed:
            self._invalidate_inflight(f'切换到 {ts_code}')
            self._target_code = ts_code
            self._target_name = str(name or '')
            self._loaded_code = ''
            self._dirty = True
        elif name and name != self._target_name:
            self._target_name = str(name)
        self._update_title()
        logger.info(f'[finance-ui] 当前标的: {ts_code} changed={changed} force={force}')

        if not changed and not force and self._loaded_code == ts_code and not self._loading:
            return
        if self._loading and not changed:
            logger.info(f'[finance-ui] 同一标的仍在加载，忽略重复请求: {ts_code}')
            return
        # 未加载成功（含上次失败）时再次选中同一标的，也允许重试
        self._dirty = True
        self._loaded_code = ''
        if is_index_code(ts_code):
            self._dirty = False
            self._show_state('指数无财报与个股资讯', '请选择个股后查看财务分析')
            return
        if self.isVisible():
            self.load_async(force=force)

    def refresh_if_needed(self) -> None:
        """页签切到前台时调用：只有脏数据或尚未加载时才请求。"""
        if not self._alive:
            return
        if not self._target_code and self.host is not None:
            code = getattr(self.host, 'current_stock_code', '')
            name = getattr(self.host, 'current_stock_name', '')
            if code:
                self.set_stock(code, name)
                return
        if self._dirty:
            self.load_async()

    def _update_title(self) -> None:
        if self._target_code:
            label = f'{self._target_name} {self._target_code}'.strip()
        else:
            label = '财务分析'
        self.title_label.setText(label)

    # ------------------------------------------------------------ 异步加载
    def load_async(self, force: bool = False) -> None:
        """后台加载当前标的；不阻塞 UI 线程。"""
        if not self._alive:
            logger.warning('[finance-ui] 页面已销毁，忽略加载请求')
            return
        if not self._target_code:
            self._show_state('暂无标的', '请先在行情/技术分析窗口选择一只股票')
            return
        if self._task_manager is None:
            logger.error('[finance-ui] 任务管理器缺失，无法后台取数')
            self._show_state('加载失败', '任务管理器不可用，请检查启动日志')
            return
        if self._loading:
            logger.info(f'[finance-ui] 已有请求在途，忽略重复加载: {self._target_code}')
            return

        self._token += 1
        token = self._token
        code = self._target_code
        name = self._target_name
        self._loading = True
        self._dirty = False
        self._refresh_button_busy(True)
        self._show_state('正在获取财务与资讯…',
                         '数据来自东方财富 / 同花顺，首次加载通常需要数秒；失败时会给出降级提示。')
        try:
            task_id = self._task_manager.create_task(
                f'财务分析: {name}({code})',
                _fetch_finance_task,
                (code, name, token),
                kwargs={'refresh': force},
                timeout_seconds=_TASK_TIMEOUT_SECONDS,
            )
        except Exception as exc:  # noqa: BLE001 - 任务创建失败也不能崩 UI
            self._loading = False
            self._refresh_button_busy(False)
            message = f'{type(exc).__name__}: {exc}'
            logger.error(f'[finance-ui] 创建取数任务失败 {code}: {message}')
            self._show_state('加载失败', f'无法创建后台任务：{message}')
            return
        if not task_id:
            self._loading = False
            self._refresh_button_busy(False)
            logger.error(f'[finance-ui] 任务管理器返回空 task_id，放弃本次取数: {code}')
            self._show_state('加载失败', '无法创建后台任务（task_id 为空）')
            return
        self._task_id = task_id
        self._connect_task_signals()
        logger.info(f'[finance-ui] 已提交后台取数 task_id={task_id} code={code} token={token}')

    def _connect_task_signals(self) -> None:
        manager = self._task_manager
        with contextlib.suppress(RuntimeError, TypeError):
            manager.task_completed.connect(self._on_task_completed)
            manager.task_error.connect(self._on_task_error)
            manager.task_finished.connect(self._on_task_finished)

    def _detach_task_signals(self) -> None:
        manager = self._task_manager
        if manager is None:
            return
        for signal, handler in (
            (manager.task_completed, self._on_task_completed),
            (manager.task_error, self._on_task_error),
            (manager.task_finished, self._on_task_finished),
        ):
            with contextlib.suppress(RuntimeError, TypeError):
                signal.disconnect(handler)

    def _invalidate_inflight(self, reason: str) -> None:
        """作废在途请求：断开回调并递增 token，旧结果即使返回也不会渲染。"""
        if not self._loading:
            return
        logger.warning(f'[finance-ui] 作废未完成请求 task_id={self._task_id}: {reason}')
        self._detach_task_signals()
        self._task_id = None
        self._loading = False
        self._token += 1
        self._refresh_button_busy(False)

    def _refresh_button_busy(self, busy: bool) -> None:
        self.refresh_button.setEnabled(not busy)
        self.refresh_button.setText('获取中…' if busy else '重新获取')

    def _on_refresh_clicked(self) -> None:
        if not self._target_code:
            self._show_state('暂无标的', '请先选择一只股票')
            return
        self._loaded_code = ''
        self.load_async(force=True)

    # ------------------------------------------------------- 全局任务回调
    def _on_task_completed(self, task_id: str, result: Any) -> None:
        # 全局信号：其他任务完成时也会触发，必须先按 task_id 过滤
        if task_id != self._task_id:
            return
        if not self._alive:
            logger.warning(f'[finance-ui] 页面已销毁，忽略完成任务 task_id={task_id}')
            return
        self._detach_task_signals()
        self._finish_loading()

        payload = result if isinstance(result, dict) else None
        if payload is None:
            logger.error(
                f'[finance-ui] 取数任务返回类型异常: task_id={task_id} '
                f'type={type(result).__name__}')
            self._show_state('加载失败', '任务返回结果格式异常，请重试')
            return
        token = payload.get('token')
        if not isinstance(token, int):
            logger.error(
                f'[finance-ui] 结果缺少有效 token，按格式异常处理: task_id={task_id} '
                f'token={token!r}')
            self._show_state('加载失败', '任务返回结果缺少请求标识，请重试')
            return
        if token != self._token:
            logger.warning(
                f'[finance-ui] 丢弃过期结果: task_id={task_id} '
                f'token={token} current={self._token}')
            return
        error = payload.get('error')
        if error:
            logger.error(f'[finance-ui] 取数任务返回错误 {payload.get("code")}: {error}')
            self._show_state('加载失败', f'数据获取失败：{error}')
            return
        snapshot = payload.get('snapshot')
        if not isinstance(snapshot, dict):
            logger.error(
                f'[finance-ui] 取数结果缺少 snapshot 或类型错误: '
                f'task_id={task_id} type={type(snapshot).__name__}')
            self._show_state('加载失败', '未获取到有效数据，请重试')
            return
        snapshot_code = snapshot.get('code')
        if snapshot_code and snapshot_code != self._target_code:
            logger.warning(
                f'[finance-ui] 快照标的与当前标的不一致，丢弃: '
                f'snapshot={snapshot_code} current={self._target_code}')
            self._show_state('已切换标的', '请求结果属于其他标的，已丢弃，请重新加载。')
            return
        if snapshot.get('not_applicable'):
            self._show_state('指数无财报与个股资讯', '请选择个股后查看财务分析')
            return
        self._render_snapshot(snapshot)

    def _on_task_error(self, task_id: str, error_message: str) -> None:
        if task_id != self._task_id:
            return
        if not self._alive:
            logger.warning(f'[finance-ui] 页面已销毁，忽略错误回调 task_id={task_id}')
            return
        self._detach_task_signals()
        self._finish_loading()
        logger.error(f'[finance-ui] 后台取数失败 task_id={task_id}: {error_message}')
        self._show_state('加载失败', f'数据获取失败：{error_message}')

    def _on_task_finished(self, task_id: str) -> None:
        if task_id != self._task_id:
            return
        if not self._alive:
            return
        # 完成/错误回调均已断开；走到这里说明任务结束但没有任何结果
        if self._loading:
            logger.error(f'[finance-ui] 任务结束但未返回结果 task_id={task_id}')
            self._detach_task_signals()
            self._finish_loading()
            self._show_state('加载失败', '后台任务已结束但未返回数据，请重试')

    def _finish_loading(self) -> None:
        self._loading = False
        self._task_id = None
        self._refresh_button_busy(False)

    # -------------------------------------------------------------- 渲染
    def _show_state(self, title: str, message: str) -> None:
        self.state_label.setText(f'{title}\n\n{message}')
        self.state_label.setStyleSheet('')
        self.stack.setCurrentWidget(self.state_label)

    def _render_snapshot(self, snapshot: dict[str, Any]) -> None:
        if not isinstance(snapshot, dict):
            logger.error('[finance-ui] 快照不是 dict，放弃渲染')
            self._show_state('加载失败', '数据格式异常')
            return
        try:
            metrics = self._merged_metrics(snapshot)
            self._render_metrics(metrics)
            self._render_header(snapshot)
            self._render_history(snapshot.get('history'))
            self._render_news(snapshot.get('news'))
            if not metrics and not self._news_items:
                self._show_state('暂无数据', '该标的暂无可展示的财务指标与资讯')
                return
            self.stack.setCurrentWidget(self.content)
            self._loaded_code = self._target_code
        except Exception as exc:  # noqa: BLE001 - 渲染失败不能崩主窗口
            logger.exception(f'[finance-ui] 渲染快照失败 {self._target_code}: {exc}')
            self._show_state('渲染失败', '数据已获取但渲染异常，请查看日志后重试')

    @staticmethod
    def _merged_metrics(snapshot: dict[str, Any]) -> dict[str, Any]:
        metrics = snapshot.get('metrics')
        result = dict(metrics) if isinstance(metrics, dict) else {}
        valuation = snapshot.get('valuation')
        if isinstance(valuation, dict) and isinstance(valuation.get('metrics'), dict):
            result.update(valuation['metrics'])
        return result

    def _render_metrics(self, metrics: dict[str, Any]) -> None:
        self.financial_empty.setVisible(not metrics)
        for key, label in self._metric_values.items():
            item = metrics.get(key)
            if isinstance(item, dict):
                display = str(item.get('display') or '—')
            elif item is None:
                display = '—'
            else:
                display = str(item)
            label.setText(display)

    def _render_header(self, snapshot: dict[str, Any]) -> None:
        name = str(snapshot.get('name') or self._target_name or '').strip()
        code = str(snapshot.get('code') or self._target_code)
        self._target_name = name
        self.title_label.setText(f'{name} {code}'.strip())

        parts: list[str] = []
        report = snapshot.get('report')
        if isinstance(report, dict):
            if report.get('period'):
                parts.append(f'报告期 {report["period"]}')
            if report.get('notice_date'):
                parts.append(f'披露 {report["notice_date"]}')
            if report.get('source'):
                parts.append(f'财务来源 {report["source"]}')
        valuation = snapshot.get('valuation')
        if isinstance(valuation, dict):
            if valuation.get('as_of'):
                parts.append(f'估值日 {valuation["as_of"]}')
            if valuation.get('source'):
                parts.append(f'估值来源 {valuation["source"]}')
        industry = snapshot.get('industry')
        if industry:
            parts.append(f'行业 {industry}')
        errors = snapshot.get('errors')
        if isinstance(errors, list) and errors:
            parts.append(f'{len(errors)} 条降级（详见日志）')
        warnings = snapshot.get('warnings')
        if isinstance(warnings, list) and warnings:
            parts.append(f'{len(warnings)} 条数据提示')
        self.meta_label.setText(' · '.join(parts) or '暂无报告期信息')

    def _render_history(self, history: Any) -> None:
        periods = []
        rows = []
        if isinstance(history, dict):
            raw_periods = history.get('periods')
            raw_rows = history.get('rows')
            if isinstance(raw_periods, list):
                periods = [str(p) for p in raw_periods]
            if isinstance(raw_rows, list):
                rows = [row for row in raw_rows if isinstance(row, dict)]
        if not periods or not rows:
            self.history_table.setVisible(False)
            self.history_empty.setVisible(True)
            self.history_empty.setText('暂无历史报告期数据（接口可能只返回最新一期）')
            return

        self.history_empty.setVisible(False)
        self.history_table.setVisible(True)
        self.history_table.setColumnCount(1 + len(periods))
        self.history_table.setHorizontalHeaderLabels(['指标'] + periods)
        self.history_table.setRowCount(len(rows))
        for row_index, row in enumerate(rows):
            label = str(row.get('label') or row.get('key') or '')
            self.history_table.setItem(row_index, 0, QTableWidgetItem(label))
            values = row.get('values')
            values = values if isinstance(values, list) else []
            for col_index in range(len(periods)):
                value = values[col_index] if col_index < len(values) else '—'
                self.history_table.setItem(
                    row_index, col_index + 1, QTableWidgetItem(str(value)))
        self.history_table.resizeRowsToContents()

    def _render_news(self, news: Any) -> None:
        items = []
        if isinstance(news, list):
            items = [item for item in news if isinstance(item, dict)]
        self._news_items = items
        self.news_table.setRowCount(len(items))
        for row, item in enumerate(items):
            self.news_table.setItem(row, 0, QTableWidgetItem(str(item.get('time') or '')))
            self.news_table.setItem(row, 1, QTableWidgetItem(str(item.get('type') or '')))
            self.news_table.setItem(row, 2, QTableWidgetItem(str(item.get('title') or '')))
            self.news_table.setItem(row, 3, QTableWidgetItem(str(item.get('source') or '')))
        self.news_table.setVisible(bool(items))
        self.news_empty.setVisible(not items)
        if items:
            self.news_detail.setText('单击条目查看摘要，双击有原文链接的新闻可直接打开。')
        else:
            self.news_detail.setText('未获取到公告、新闻、业绩预告、分红或风险提示。')

    def _on_news_clicked(self, row: int, column: int) -> None:
        if not 0 <= row < len(self._news_items):
            return
        item = self._news_items[row]
        title = str(item.get('title') or '')
        summary = str(item.get('summary') or '').strip()
        source = str(item.get('source') or '')
        text = f'{title}\n{summary or "（该条暂无摘要）"}'
        if source:
            text += f'\n来源：{source}'
        self.news_detail.setText(text)

    def _on_news_double_clicked(self, row: int, column: int) -> None:
        if not 0 <= row < len(self._news_items):
            return
        item = self._news_items[row]
        url = str(item.get('url') or '').strip()
        if not url:
            self.news_detail.setText('该条资讯没有原文链接，已展示摘要。')
            return
        logger.info(f'[finance-ui] 打开资讯原文: {url}')
        QDesktopServices.openUrl(QUrl(url))

    # ---------------------------------------------------------- 测试辅助
    def snapshot_state_text(self) -> str:
        """当前状态页文案，供测试断言。"""
        return self.state_label.text()

    def current_stack_is_content(self) -> bool:
        return self.stack.currentWidget() is self.content
