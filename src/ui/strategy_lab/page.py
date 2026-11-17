"""策略试算标签页：规则文件下拉 → 按类型回测 → 净值、回撤与调仓三张图。

界面只做三件事：从 ``config/strategy_lab/rules/*.json`` 选规则、调用
:mod:`src.quant.strategy_lab` 的解析与回测适配层、展示结果。不执行用户输入
的任何代码，参数由规则 JSON 决定；新增或修改规则请直接编辑规则文件。
表格列宽统一走 ``src.ui.table_sizing``，不在页面内写死宽度。
"""
from __future__ import annotations

from typing import Any, Dict, List

from loguru import logger
from PySide6.QtCore import Qt, QThread, Signal
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from src.quant.strategy_lab.rules import (
    RULE_TYPES,
    RuleError,
    list_rules,
    load_rule,
    summarize,
)
from src.ui.table_sizing import apply_content_widths, use_interactive_columns

COLOR_EQUITY = '#2f7ed8'
COLOR_BENCH = '#c0504d'
COLOR_DRAWDOWN = '#c0504d'
COLOR_MARKER = '#e3b341'
PLOT_LEGEND_EQUITY = '策略净值'
PLOT_LEGEND_BENCH = '基准净值'

_METRIC_ROWS = (
    ('annual', '年化收益', 'pct'),
    ('max_drawdown', '最大回撤', 'pct'),
    ('sharpe', 'Sharpe', 'num'),
    ('volatility', '年化波动', 'pct'),
    ('turnover_one_way', '单边年换手', 'x'),
    ('annual_cost_pct', '年化成本', 'pct'),
    ('benchmark_annual', '基准年化', 'pct'),
    ('excess_annual', '年化超额', 'pct'),
    ('information_ratio', '信息比率 IR', 'num'),
    ('final_equity', '期末净值', 'money'),
    ('trades', '成交笔数', 'int'),
    ('rebalances', '调仓次数', 'int'),
    ('members_median', '成分中位', 'int'),
    ('members_min', '成分最少', 'int'),
    ('members_max', '成分最多', 'int'),
)


class _RunWorker(QThread):
    ok = Signal(dict)
    failed = Signal(str)

    def __init__(self, rule: Dict[str, Any], parent=None):
        super().__init__(parent)
        self.rule = rule

    def run(self) -> None:
        try:
            from src.quant.strategy_lab.backtest import run_rule

            self.ok.emit(run_rule(self.rule))
        except Exception as exc:  # noqa: BLE001 - 线程顶层必须兜底，异常不能穿透事件循环
            logger.exception('策略试算回测失败')
            self.failed.emit(str(exc))


class StrategyLabPage(QWidget):
    """策略试算主页面：规则文件驱动，三张图展示净值、回撤与调仓。"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker: _RunWorker | None = None
        self._rule: Dict[str, Any] | None = None
        self._nav_dates: List[str] = []
        self._build_ui()
        self._refresh_rule_list()

    # -- 布局 --------------------------------------------------------------
    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(self._build_editor())
        splitter.addWidget(self._build_result())
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 4)
        root.addWidget(splitter)

    def _build_editor(self) -> QWidget:
        box = QGroupBox('规则选择')
        outer = QVBoxLayout(box)

        head = QHBoxLayout()
        head.addWidget(QLabel('规则文件'))
        self.rule_combo = QComboBox()
        self.rule_combo.setMinimumWidth(260)
        self.rule_combo.currentIndexChanged.connect(lambda *_: self._on_select())
        head.addWidget(self.rule_combo, 2)
        self.refresh_btn = QPushButton('刷新列表')
        self.refresh_btn.clicked.connect(self._refresh_rule_list)
        head.addWidget(self.refresh_btn)
        self.run_btn = QPushButton('运行试算')
        self.run_btn.setDefault(True)
        self.run_btn.clicked.connect(self._on_run)
        head.addWidget(self.run_btn)
        outer.addLayout(head)

        self.rule_text_label = QLabel('规则目录为空：请在 config/strategy_lab/rules/ 下放置规则 JSON')
        self.rule_text_label.setWordWrap(True)
        self.rule_text_label.setStyleSheet('color: palette(mid);')
        outer.addWidget(self.rule_text_label)
        return box

    def _build_result(self) -> QWidget:
        box = QGroupBox('回测结果')
        layout = QVBoxLayout(box)
        split = QSplitter(Qt.Horizontal)

        self.metrics_table = QTableWidget(0, 2)
        self.metrics_table.setHorizontalHeaderLabels(['指标', '数值'])
        self.metrics_table.verticalHeader().setVisible(False)
        self.metrics_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.metrics_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        use_interactive_columns(self.metrics_table)
        self.metrics_table.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeToContents)
        split.addWidget(self.metrics_table)

        charts = QWidget()
        charts_layout = QVBoxLayout(charts)
        charts_layout.setContentsMargins(0, 0, 0, 0)
        self.nav_plot = self._add_plot(charts_layout, '净值对比（起点=1）', '净值')
        self.drawdown_plot = self._add_plot(charts_layout, '回撤', '回撤')
        self.marker_plot = self._add_plot(charts_layout, '调仓 / 持仓', '仓位或成分数')
        split.addWidget(charts)
        split.setStretchFactor(0, 1)
        split.setStretchFactor(1, 3)
        layout.addWidget(split, 1)

        self.status_label = QLabel('选择规则后点击「运行试算」')
        self.status_label.setWordWrap(True)
        layout.addWidget(self.status_label)
        return box

    @staticmethod
    def _add_plot(layout: QVBoxLayout, title: str, ylabel: str):
        """三张图共用一套样式：无背景、浅网格、左侧单位标签。"""
        layout.addWidget(_SectionLabel(title))
        try:
            import pyqtgraph as pg

            plot = pg.PlotWidget()
            plot.setBackground(None)
            plot.showGrid(x=True, y=True, alpha=0.2)
            plot.addLegend()
            plot.setLabel('left', ylabel)
            plot.setMinimumHeight(120)
        except Exception:  # noqa: BLE001 - 缺少 pyqtgraph 时降级为提示标签
            logger.warning('pyqtgraph 不可用，{} 改为文本提示', title)
            plot = QLabel('当前环境缺少 pyqtgraph，仅展示指标')
            plot.setWordWrap(True)
        layout.addWidget(plot, 1)
        return plot

    # -- 规则 --------------------------------------------------------------
    def _refresh_rule_list(self) -> None:
        current = self.rule_combo.currentData()
        self.rule_combo.blockSignals(True)
        self.rule_combo.clear()
        for item in list_rules():
            try:
                rule = load_rule(item['path'])
                label = f'{rule["name"]}（{RULE_TYPES[rule["rule_type"]]}）'
            except RuleError as exc:
                logger.warning('规则文件不可用：{}（{}）', item['path'], exc)
                label = f'{item["name"]}（读取失败）'
            self.rule_combo.addItem(label, item['path'])
        self.rule_combo.blockSignals(False)
        index = self.rule_combo.findData(current) if current else -1
        if index >= 0:
            self.rule_combo.setCurrentIndex(index)
        self._on_select()

    def _on_select(self) -> None:
        path = self.rule_combo.currentData()
        if not path:
            self._rule = None
            self.rule_text_label.setText(
                '规则目录为空：请在 config/strategy_lab/rules/ 下放置规则 JSON')
            self.rule_text_label.setStyleSheet('color: palette(mid);')
            return
        try:
            rule = load_rule(path)
        except RuleError as exc:
            self._rule = None
            self.rule_text_label.setText(f'规则不可用：{exc}')
            self.rule_text_label.setStyleSheet(f'color: {COLOR_BENCH};')
            return
        self._rule = rule
        summary = summarize(rule)
        self.rule_text_label.setText(f'[{summary.type_label}] {summary.text}')
        self.rule_text_label.setStyleSheet('color: palette(mid);')

    def _on_run(self) -> None:
        path = self.rule_combo.currentData()
        if not path:
            QMessageBox.information(self, '暂无规则',
                                    '规则目录里没有 JSON 文件')
            return
        try:
            rule = load_rule(path)
        except RuleError as exc:
            QMessageBox.warning(self, '规则不可用', str(exc))
            return
        self._rule = rule
        if self._worker is not None and self._worker.isRunning():
            self.status_label.setText('已有一次试算在运行，请等待完成')
            return
        self.run_btn.setEnabled(False)
        self.status_label.setText(f'试算中：{RULE_TYPES[rule["rule_type"]]}规则，'
                                  '加载本地行情与 PIT 字段面板…')
        self._worker = _RunWorker(rule, self)
        self._worker.ok.connect(self._on_result)
        self._worker.failed.connect(self._on_failed)
        self._worker.finished.connect(lambda: self.run_btn.setEnabled(True))
        self._worker.start()

    def _on_failed(self, message: str) -> None:
        self.status_label.setText(f'试算失败：{message}')

    # -- 展示 --------------------------------------------------------------
    def _on_result(self, payload: Dict[str, Any]) -> None:
        if payload.get('status') != 'ok':
            self.status_label.setText(f'空结果：{payload.get("reason", "无可用结果")}')
            self._fill_metrics({'rebalances': 0})
            self._plot_curves([], [])
            self._plot_drawdown([])
            self._plot_markers([])
            return
        self._fill_metrics(payload['metrics'],
                           (payload.get('dividend_coverage') or {}).get('message') or '')
        self._plot_curves(payload.get('nav') or [], payload.get('benchmark_curve') or [])
        self._plot_drawdown(payload.get('drawdown') or [])
        self._plot_markers(payload.get('markers') or [])
        hint = (payload.get('dividend_coverage') or {}).get('message') or ''
        self.status_label.setText(
            f'{payload["rule_text"]}  用时 {payload["elapsed_seconds"]}s '
            f'（引擎 {payload["engine_seconds"]}s，参与标的 {payload["used_codes"]} 只）'
            + (f'  {hint}' if hint else ''))

    def _fill_metrics(self, metrics: Dict[str, Any],
                      coverage_hint: str = '') -> None:
        rows = [(label, self._format(kind, metrics.get(key)))
                for key, label, kind in _METRIC_ROWS]
        if coverage_hint:
            rows.append(('分红覆盖', coverage_hint))
        self.metrics_table.setRowCount(len(rows))
        for i, (label, value) in enumerate(rows):
            self.metrics_table.setItem(i, 0, QTableWidgetItem(label))
            item = QTableWidgetItem(value)
            item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.metrics_table.setItem(i, 1, item)
        apply_content_widths(self.metrics_table)

    @staticmethod
    def _format(kind: str, value: Any) -> str:
        if value is None:
            return '—'
        if kind == 'pct':
            return f'{float(value) * 100:.2f}%'
        if kind == 'num':
            return f'{float(value):.3f}'
        if kind == 'x':
            return f'{float(value):.2f}x'
        if kind == 'money':
            return f'{float(value):,.0f}'
        return str(value)

    def _plot_curves(self, nav: List[List[Any]], bench: List[List[Any]]) -> None:
        self._nav_dates = [str(row[0]) for row in nav]
        if not hasattr(self.nav_plot, 'plot'):
            return
        self.nav_plot.clear()
        if nav:
            base = float(nav[0][1]) or 1.0
            self.nav_plot.plot(list(range(len(nav))),
                               [float(v) / base for _, v in nav],
                               pen=QColor(COLOR_EQUITY), name=PLOT_LEGEND_EQUITY)
            self.nav_plot.setLabel(
                'bottom', f'{nav[0][0]} ~ {nav[-1][0]}（交易日序号）')
        if bench:
            base = float(bench[0][1]) or 1.0
            self.nav_plot.plot(list(range(len(bench))),
                               [float(v) / base for _, v in bench],
                               pen=QColor(COLOR_BENCH), name=PLOT_LEGEND_BENCH,
                               style=Qt.DashLine)

    def _plot_drawdown(self, drawdown: List[List[Any]]) -> None:
        if not hasattr(self.drawdown_plot, 'plot'):
            return
        self.drawdown_plot.clear()
        if not drawdown:
            return
        xs = list(range(len(drawdown)))
        ys = [float(v) * 100.0 for _, v in drawdown]
        self.drawdown_plot.plot(xs, ys, pen=QColor(COLOR_DRAWDOWN),
                                fillLevel=0, brush=QColor(192, 80, 77, 60))
        self.drawdown_plot.setLabel('bottom', f'{drawdown[0][0]} ~ {drawdown[-1][0]}')

    def _plot_markers(self, markers: List[Dict[str, Any]]) -> None:
        """调仓标记：横轴对齐净值日期序号，纵轴为仓位（择时/切换）或成分数（选股）。"""
        if not hasattr(self.marker_plot, 'plot'):
            return
        self.marker_plot.clear()
        if not markers:
            return
        index = {day: i for i, day in enumerate(self._nav_dates)}
        xs: List[int] = []
        ys: List[float] = []
        for marker in markers:
            x = index.get(str(marker.get('date')))
            if x is None:
                continue
            if marker.get('members') is not None:
                y = float(marker['members'])
            else:
                y = float(marker.get('position') or 1.0)
            xs.append(x)
            ys.append(y)
        if not xs:
            return
        self.marker_plot.plot(xs, ys, pen=None, symbol='o', symbolSize=6,
                              symbolBrush=QColor(COLOR_MARKER),
                              symbolPen=QColor(COLOR_MARKER))
        for x in xs:
            line = self.marker_plot.addLine(x=x, pen=QColor(227, 179, 65, 90),
                                            movable=False)
            line.setZValue(-10)


class _SectionLabel(QLabel):
    """图标题：小号、次要色，与指标表同一字体族。"""

    def __init__(self, text: str, parent=None):
        super().__init__(text, parent)
        self.setStyleSheet('color: palette(mid); font-size: 12px;')
