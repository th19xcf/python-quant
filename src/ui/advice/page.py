#!/usr/bin/env python3

"""每日推荐页：KPI 卡片 + 三档分段控件 + 推荐表格 + 详情面板。

数据只经 ``src.core.usecases.get_daily_recommendations`` 获取，
生成/加载全部走 global_task_manager 后台线程，结果回投主线程刷新表格。
"""

from __future__ import annotations

from typing import Any

from loguru import logger
from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QButtonGroup,
    QComboBox,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMenu,
    QPushButton,
    QSizePolicy,
    QSplitter,
    QStackedWidget,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from src.core.usecases.get_daily_recommendations import (
    DailyRecommendationBundle,
    get_daily_recommendations,
)
from src.ui.advice.table_model import (
    ASSET_FILTERS,
    COL_CODE,
    COL_CONFIDENCE,
    COL_WEIGHT,
    COLUMNS,
    DTO_ROLE,
    AdviceFilterProxyModel,
    AdviceTableModel,
)
from src.ui.advice.tokens import TOKENS, palette_for
from src.ui.advice.widgets import (
    AdviceItemDelegate,
    DetailPanel,
    EmptyState,
    KpiCard,
    SegmentedControl,
    SkeletonWidget,
)

TIER_OF_SEGMENT = {'short': 'short', 'mid': 'mid', 'long': 'long'}


def _fetch_bundle(token: int, refresh: bool, task_id: str | None = None,
                  signals: Any = None) -> dict[str, Any]:
    """后台任务：一次性取回三档清单（跨线程只传 DTO，不传 Qt 对象）。"""
    bundle = get_daily_recommendations(horizon='all', refresh=refresh)
    return {'token': token, 'bundle': bundle}


class AdvicePage(QWidget):
    """每日推荐主页面。"""

    theme_changed = Signal(str)

    def __init__(self, host: Any = None, parent: QWidget | None = None,
                 theme: str = 'dark') -> None:
        super().__init__(parent)
        self.setObjectName('advicePage')
        self.host = host
        self.theme = 'light' if str(theme).lower() == 'light' else 'dark'
        self._pal = palette_for(self.theme)
        self._bundle: DailyRecommendationBundle | None = None
        self._tier = 'short'
        self._busy = False
        self._token = 0
        self._kpi_accents: dict[str, str] = {}
        self._build_ui()
        self.set_theme(self.theme)
        self._connect_task_signals()
        self._populate_history()

    # ------------------------------------------------------------------ UI
    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(TOKENS.space_lg, TOKENS.space_lg,
                                TOKENS.space_lg, TOKENS.space_lg)
        root.setSpacing(TOKENS.space_md)

        root.addLayout(self._build_header())
        root.addWidget(self._build_kpi_row())

        toolbar = QHBoxLayout()
        toolbar.setSpacing(TOKENS.space_md)
        self.segmented = SegmentedControl(self._pal)
        self.segmented.set_current('short', animate=False)
        self.segmented.changed.connect(self._on_tier_changed)
        toolbar.addWidget(self.segmented)
        toolbar.addStretch(1)

        self.search_box = QLineEdit()
        self.search_box.setObjectName('searchBox')
        self.search_box.setPlaceholderText('搜索代码 / 名称 / 因子')
        self.search_box.setClearButtonEnabled(True)
        self.search_box.setFixedWidth(220)
        self.search_box.textChanged.connect(self._on_search_changed)
        toolbar.addWidget(self.search_box)

        self.sort_combo = QComboBox()
        self.sort_combo.setObjectName('sortCombo')
        self.sort_combo.addItems(['置信度 ↓', '仓位 ↓', '代码 ↑'])
        self.sort_combo.currentIndexChanged.connect(self._on_sort_changed)
        toolbar.addWidget(self.sort_combo)
        root.addLayout(toolbar)

        filters = QHBoxLayout()
        filters.setSpacing(TOKENS.space_sm)
        self._direction_group = self._build_chips(
            filters, '方向',
            ('全部', '买入', '卖出', '持有', '回避'),
            self._on_direction_filter, values=('全部', 'BUY', 'SELL', 'HOLD', 'AVOID'))
        filters.addSpacing(TOKENS.space_md)
        self._asset_group = self._build_chips(
            filters, '资产', ASSET_FILTERS, self._on_asset_filter)
        filters.addStretch(1)
        self.result_label = QLabel('—')
        self.result_label.setObjectName('mutedLabel')
        filters.addWidget(self.result_label)
        root.addLayout(filters)

        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.setChildrenCollapsible(False)
        self.stack = QStackedWidget()

        self.table_card = QFrame()
        self.table_card.setObjectName('tableCard')
        card_layout = QVBoxLayout(self.table_card)
        card_layout.setContentsMargins(1, 1, 1, 1)
        card_layout.setSpacing(0)
        card_layout.addWidget(self._build_table())
        self.stack.addWidget(self.table_card)

        self.skeleton = SkeletonWidget(self._pal)
        self.stack.addWidget(self.skeleton)

        self.empty_state = EmptyState(self._pal)
        self.empty_state.generate_requested.connect(self._on_generate_clicked)
        self.stack.addWidget(self.empty_state)

        splitter.addWidget(self.stack)
        self.detail = DetailPanel(self._pal)
        splitter.addWidget(self.detail)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([940, 360])
        root.addWidget(splitter, 1)

    def _build_header(self) -> QHBoxLayout:
        header = QHBoxLayout()
        header.setSpacing(TOKENS.space_md)
        title_box = QVBoxLayout()
        title_box.setSpacing(2)
        self.title_label = QLabel('今日决策')
        self.title_label.setObjectName('adviceTitle')
        self.subtitle_label = QLabel('收盘后 15:30 生成 · 人工执行')
        self.subtitle_label.setObjectName('adviceSubtitle')
        title_box.addWidget(self.title_label)
        title_box.addWidget(self.subtitle_label)
        header.addLayout(title_box)
        header.addStretch(1)

        self.fresh_label = QLabel('数据加载中')
        self.fresh_label.setObjectName('freshBadge')
        header.addWidget(self.fresh_label)

        self.history_combo = QComboBox()
        self.history_combo.setObjectName('historyCombo')
        self.history_combo.setMinimumWidth(150)
        self.history_combo.setToolTip('选择已归档的历史交易日；「当前」回到最新清单')
        self.history_combo.currentIndexChanged.connect(self._on_history_changed)
        header.addWidget(self.history_combo)

        self.theme_button = QPushButton('浅色')
        self.theme_button.setObjectName('themeBtn')
        self.theme_button.setCursor(Qt.CursorShape.PointingHandCursor)
        self.theme_button.clicked.connect(self.toggle_theme)
        header.addWidget(self.theme_button)

        self.refresh_button = QPushButton('重新生成')
        self.refresh_button.setObjectName('refreshBtn')
        self.refresh_button.setCursor(Qt.CursorShape.PointingHandCursor)
        self.refresh_button.clicked.connect(self._on_generate_clicked)
        header.addWidget(self.refresh_button)
        return header

    def _build_kpi_row(self) -> QWidget:
        row = QWidget()
        layout = QHBoxLayout(row)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(TOKENS.space_md)
        specs = (
            ('count', '今日推荐数量', 'accent'),
            ('flow', '买入 / 卖出', 'buy'),
            ('turnover', '预计组合换手（单边）', None),
            ('trade_date', '数据日期 / 新鲜度', None),
        )
        self._kpi: dict[str, KpiCard] = {}
        self._kpi_accents = {key: color for key, _, color in specs if color}
        for key, caption, _ in specs:
            card = KpiCard(caption, self._pal)
            card.setMinimumHeight(84)
            layout.addWidget(card, 1)
            self._kpi[key] = card
        return row

    def _build_table(self) -> QTableView:
        self.model = AdviceTableModel()
        self.proxy = AdviceFilterProxyModel()
        self.proxy.setSourceModel(self.model)
        self.delegate = AdviceItemDelegate(self._pal)
        table = QTableView()
        table.setObjectName('adviceTable')
        table.setModel(self.proxy)
        table.setItemDelegate(self.delegate)
        table.setSortingEnabled(True)
        table.setShowGrid(False)
        table.setAlternatingRowColors(True)
        table.setMouseTracking(True)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setHorizontalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        table.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        table.verticalHeader().setVisible(False)
        table.verticalHeader().setDefaultSectionSize(TOKENS.row_height)

        header = table.horizontalHeader()
        header.setObjectName('adviceHeader')
        header.setFixedHeight(TOKENS.header_height)
        header.setHighlightSections(False)
        for index, column in enumerate(COLUMNS):
            table.setColumnWidth(index, column.width)
        header.setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        header.setStretchLastSection(True)

        table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        table.customContextMenuRequested.connect(self._show_context_menu)
        table.doubleClicked.connect(self._on_row_double_clicked)
        table.selectionModel().currentRowChanged.connect(self._on_current_changed)
        header.sectionClicked.connect(self._on_header_clicked)
        self.table = table
        self.table.sortByColumn(COL_CONFIDENCE, Qt.SortOrder.DescendingOrder)
        return table

    def _build_chips(self, layout: QHBoxLayout, caption: str,
                     options: tuple[str, ...], callback,
                     values: tuple[str, ...] | None = None) -> QButtonGroup:
        label = QLabel(caption)
        label.setObjectName('mutedLabel')
        layout.addWidget(label)
        group = QButtonGroup(self)
        group.setExclusive(True)
        payload = values or options
        for index, option in enumerate(options):
            button = QPushButton(option)
            button.setObjectName('filterChip')
            button.setCheckable(True)
            button.setCursor(Qt.CursorShape.PointingHandCursor)
            button.setChecked(index == 0)
            group.addButton(button, index)
            layout.addWidget(button)
        group.idClicked.connect(lambda idx: callback(payload[idx]))
        return group

    def _connect_task_signals(self) -> None:
        from src.ui.task_manager import global_task_manager
        self._task_manager = global_task_manager
        global_task_manager.task_completed.connect(self._on_task_completed)
        global_task_manager.task_error.connect(self._on_task_error)

    # --------------------------------------------------------------- theme
    def set_theme(self, theme: str) -> None:
        from src.ui.advice.tokens import advice_qss
        self.theme = 'light' if str(theme).lower() == 'light' else 'dark'
        self._pal = palette_for(self.theme)
        self.setStyleSheet(advice_qss(self.theme))
        self.theme_button.setText('浅色' if self.theme == 'dark' else '深色')
        for widget in (self.segmented, self.delegate, self.skeleton,
                       self.empty_state, self.detail):
            if hasattr(widget, 'set_palette'):
                widget.set_palette(self._pal)
        for key, card in self._kpi.items():
            color_key = self._kpi_accents.get(key)
            card.set_accent(getattr(self._pal, color_key) if color_key else None)
        self.table.viewport().update()
        self.theme_changed.emit(self.theme)

    def toggle_theme(self) -> None:
        self.set_theme('light' if self.theme == 'dark' else 'dark')

    # -------------------------------------------------------------- loading
    def load_async(self, refresh: bool = False) -> None:
        """后台加载/生成推荐，完成后由全局任务信号回投主线程。"""
        if self._busy:
            return
        self._busy = True
        self._token += 1
        token = self._token
        self.refresh_button.setEnabled(False)
        self.refresh_button.setText('生成中…')
        self.fresh_label.setText('正在生成…')
        self.fresh_label.setObjectName('freshBadge')
        self.fresh_label.setStyleSheet('')
        self.stack.setCurrentWidget(self.skeleton)
        self.skeleton.start()
        self._task_manager.create_task(
            f'生成每日推荐#{token}', _fetch_bundle,
            kwargs={'token': token, 'refresh': refresh},
            timeout_seconds=None)

    def _on_generate_clicked(self) -> None:
        if self.history_combo.currentData() is not None:
            self.history_combo.blockSignals(True)
            self.history_combo.setCurrentIndex(0)
            self.history_combo.blockSignals(False)
        self.load_async(refresh=True)

    def _populate_history(self) -> None:
        """刷新头部历史归档下拉框（「当前」+ 升序归档，界面显示倒序）。"""
        from src.core.usecases.get_daily_recommendations import list_archived_dates
        dates = list_archived_dates()
        keep = self.history_combo.currentData()
        self.history_combo.blockSignals(True)
        self.history_combo.clear()
        self.history_combo.addItem('当前（最新）', None)
        for trade_date in reversed(dates):
            self.history_combo.addItem(trade_date, trade_date)
        index = self.history_combo.findData(keep)
        self.history_combo.setCurrentIndex(max(0, index))
        self.history_combo.blockSignals(False)

    def _on_history_changed(self, index: int) -> None:
        trade_date = self.history_combo.itemData(index)
        if trade_date is None:
            self.load_async(refresh=False)
            return
        from src.core.usecases.get_daily_recommendations import (
            load_archived_recommendations,
        )
        bundle = load_archived_recommendations(trade_date, horizon='all')
        if bundle is None or not bundle.items:
            self.empty_state.set_message(
                '归档不可用', f'{trade_date} 的推荐归档未找到或为空', True)
            self.stack.setCurrentWidget(self.empty_state)
            return
        self.set_bundle(bundle)

    def _on_task_completed(self, task_id: str, result: Any) -> None:
        if not isinstance(result, dict):
            return
        token = result.get('token')
        if token is not None and token != self._token:
            return
        bundle = result.get('bundle')
        if bundle is None:
            self._finish_busy()
            return
        self._finish_busy()
        self.set_bundle(bundle)
        self._populate_history()

    def _on_task_error(self, task_id: str, error_message: str) -> None:
        if not self._busy:
            return
        self._finish_busy()
        logger.error(f'[advice] 推荐生成失败: {error_message}')
        self.empty_state.set_message('推荐生成失败', error_message, True)
        self.stack.setCurrentWidget(self.empty_state)

    def _finish_busy(self) -> None:
        self._busy = False
        self.refresh_button.setEnabled(True)
        self.refresh_button.setText('重新生成')
        self.skeleton.stop()

    # -------------------------------------------------------------- binding
    def set_bundle(self, bundle: DailyRecommendationBundle,
                   select_code: str | None = None) -> None:
        """把用例层 bundle 灌入页面（截图/测试也走同一入口）。"""
        self._bundle = bundle
        self._update_header()
        self._refresh_table()
        if select_code:
            self.select_code(select_code)
        else:
            self._ensure_selection()

    def _update_header(self) -> None:
        bundle = self._bundle
        if bundle is None:
            return
        trade_date = bundle.trade_date.isoformat() if bundle.trade_date else '—'
        subtitle = (f'数据日期 {trade_date} · 生成时间 '
                    f'{bundle.generated_at:%m-%d %H:%M} · '
                    f'参考资金 ¥{bundle.equity:,.0f}')
        if bundle.is_sample:
            subtitle += ' · 样例数据'
        if bundle.source == 'archive':
            subtitle += ' · 历史归档（不重算）'
        self.subtitle_label.setText(subtitle)
        self.subtitle_label.setToolTip('\n'.join(bundle.warnings))
        if bundle.is_sample:
            self.fresh_label.setText('样例数据')
            self.fresh_label.setObjectName('freshBadge')
        elif bundle.source == 'archive':
            self.fresh_label.setText('历史归档')
            self.fresh_label.setObjectName('freshBadge')
        elif bundle.is_stale:
            self.fresh_label.setText(f'数据落后 {bundle.data_lag_days} 天')
            self.fresh_label.setObjectName('freshBadge')
        else:
            self.fresh_label.setText('数据已更新')
            self.fresh_label.setObjectName('freshOk')
        self.fresh_label.setStyleSheet('')
        self.fresh_label.style().unpolish(self.fresh_label)
        self.fresh_label.style().polish(self.fresh_label)

    def _refresh_table(self) -> None:
        items = self._tier_items()
        self.model.set_items(items)
        self._update_kpis(items)
        self.result_label.setText(f'共 {len(items)} 条')
        self._update_view_state()

    def _tier_items(self) -> list:
        if self._bundle is None:
            return []
        return self._bundle.by_tier(self._tier)

    def _update_kpis(self, items: list) -> None:
        counts: dict[str, int] = {}
        for item in items:
            counts[item.direction] = counts.get(item.direction, 0) + 1
        actionable = (counts.get('BUY', 0) + counts.get('SELL', 0)
                      + counts.get('HOLD', 0))
        buy, sell = counts.get('BUY', 0), counts.get('SELL', 0)
        self._kpi['count'].set_value(str(actionable))
        self._kpi['flow'].set_value(f'{buy} / {sell}')
        summary: dict[str, Any] = {}
        if self._bundle is not None:
            summary = (self._bundle.summary or {}).get(self._tier, {}) or {}
        turnover = 0.0
        if self._bundle is not None and self._bundle.source == 'archive':
            self._kpi['turnover'].set_value('—')
            self._kpi['turnover'].set_caption('组合换手（归档未记录）')
        else:
            turnover = float(summary.get('turnover_pct') or 0.0)
            self._kpi['turnover'].set_value(f'{turnover:.1f}%')
            self._kpi['turnover'].set_caption('预计组合换手（单边）')

        bundle = self._bundle
        if bundle is not None and bundle.trade_date:
            self._kpi['trade_date'].set_value(bundle.trade_date.isoformat())
            lag = bundle.data_lag_days
            self._kpi['trade_date'].set_caption(
                f'数据日期 · {("落后 " + str(lag) + " 天") if lag > 1 else "最新"}')
        elif bundle is not None:
            self._kpi['trade_date'].set_value('无数据')
            self._kpi['trade_date'].set_caption('数据日期 / 新鲜度')

        self._kpi['flow'].set_accent(self._pal.buy)
        self._kpi['turnover'].set_accent(
            self._pal.hold if turnover > 50 else None)
        stale = bundle.is_stale if bundle is not None else False
        self._kpi['trade_date'].set_accent(
            self._pal.hold if stale else None)
        self._kpi['count'].set_accent(self._pal.accent)

    def _update_view_state(self) -> None:
        bundle = self._bundle
        if bundle is None or not bundle.items:
            self.empty_state.set_message(
                '暂无推荐结果',
                '本地列存还没有可用数据，或今日清单尚未生成。'
                '点击「立即生成」将按最近交易日计算三档清单。', True)
            self.stack.setCurrentWidget(self.empty_state)
            self.detail.set_dto(None)
            return
        if not self._tier_items():
            label = {'short': '短线', 'mid': '中线', 'long': '长线'}[self._tier]
            self.empty_state.set_message(
                f'{label}档暂无推荐',
                '该档位今日没有可执行标的（可能全部被风险约束或换手控制拦下），'
                '可切换其他档位或重新生成。', True)
            self.stack.setCurrentWidget(self.empty_state)
            self.detail.set_dto(None)
            return
        if self.proxy.rowCount() == 0:
            self.empty_state.set_message(
                '没有符合条件的推荐',
                '当前方向 / 资产 / 关键词筛选下没有结果，试试清除筛选条件。',
                False)
            self.stack.setCurrentWidget(self.empty_state)
            self.detail.set_dto(None)
            return
        self.stack.setCurrentWidget(self.table_card)

    def _ensure_selection(self) -> None:
        if self.proxy.rowCount() == 0:
            self.detail.set_dto(None)
            return
        current = self.table.currentIndex()
        if current.isValid() and current.row() < self.proxy.rowCount():
            self._show_detail(current)
            return
        self.table.selectRow(0)
        self._show_detail(self.proxy.index(0, 0))

    def select_code(self, ts_code: str) -> bool:
        for row in range(self.proxy.rowCount()):
            index = self.proxy.index(row, 0)
            dto = self.proxy.data(index, DTO_ROLE)
            if dto is not None and dto.ts_code == ts_code:
                self.table.selectRow(row)
                self._show_detail(index)
                return True
        return False

    def current_dto(self):
        index = self.table.currentIndex()
        return index.data(DTO_ROLE) if index.isValid() else None

    # -------------------------------------------------------------- handlers
    def _on_tier_changed(self, key: str) -> None:
        self._tier = TIER_OF_SEGMENT.get(key, 'short')
        self._refresh_table()

    def _on_search_changed(self, text: str) -> None:
        self.proxy.set_search(text)
        self._update_view_state()
        self._ensure_selection()

    def _on_direction_filter(self, value: str) -> None:
        self.proxy.set_direction(value)
        self._update_view_state()
        self._ensure_selection()

    def _on_asset_filter(self, value: str) -> None:
        self.proxy.set_asset_type(value)
        self._update_view_state()
        self._ensure_selection()

    def _on_sort_changed(self, index: int) -> None:
        mapping = {0: (COL_CONFIDENCE, True), 1: (COL_WEIGHT, True),
                   2: (COL_CODE, False)}
        column, descending = mapping.get(index, (COL_CONFIDENCE, True))
        order = Qt.SortOrder.DescendingOrder if descending else Qt.SortOrder.AscendingOrder
        self.table.sortByColumn(column, order)

    def _on_header_clicked(self, section: int) -> None:
        header = self.table.horizontalHeader()
        order = header.sortIndicatorOrder()
        self.table.sortByColumn(section, order)

    def _on_current_changed(self, current, previous) -> None:
        self._show_detail(current)

    def _show_detail(self, index) -> None:
        dto = index.data(DTO_ROLE) if index.isValid() else None
        self.detail.set_dto(dto)

    def _on_row_double_clicked(self, index) -> None:
        dto = index.data(DTO_ROLE)
        if dto is None or self.host is None:
            return
        try:
            if hasattr(self.host, 'tab_widget'):
                self.host.tab_widget.setCurrentIndex(1)
            if hasattr(self.host, 'process_stock_data'):
                self.host.process_stock_data(dto.ts_code, dto.name)
            if hasattr(self.host, 'statusBar'):
                self.host.statusBar().showMessage(
                    f'跳转 {dto.name}({dto.ts_code}) K 线', 3000)
        except (AttributeError, RuntimeError, ValueError) as exc:
            logger.warning(f'[advice] 跳转 K 线失败: {exc}')

    def _show_context_menu(self, pos) -> None:
        index = self.table.indexAt(pos)
        dto = index.data(DTO_ROLE) if index.isValid() else None
        if dto is None:
            return
        menu = QMenu(self)
        copy_action = menu.addAction('复制代码')
        watch_action = menu.addAction('加入自选')
        chosen = menu.exec(self.table.viewport().mapToGlobal(pos))
        if chosen is copy_action:
            QApplication.clipboard().setText(dto.ts_code)
            self._status(f'已复制 {dto.ts_code}')
        elif chosen is watch_action:
            self._add_to_watchlist(dto)

    def _add_to_watchlist(self, dto) -> None:
        manager = getattr(self.host, 'data_manager', None)
        adder = getattr(manager, 'add_to_watchlist', None)
        if callable(adder):
            try:
                adder(dto.ts_code, dto.name)
                self._status(f'已加入自选 {dto.name}({dto.ts_code})')
                return
            except (AttributeError, RuntimeError, ValueError) as exc:
                logger.warning(f'[advice] 加入自选失败: {exc}')
        QApplication.clipboard().setText(dto.ts_code)
        self._status('自选功能尚未接入，已复制代码备用')

    def _status(self, message: str) -> None:
        status = getattr(self.host, 'statusBar', None)
        if callable(status):
            try:
                status().showMessage(message, 3000)
            except (AttributeError, RuntimeError):
                pass
