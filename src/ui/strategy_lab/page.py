"""策略试算标签页：规则文件下拉 → 按类型回测 → 净值、回撤与调仓三张图。

界面只做三件事：从 ``config/strategy_lab/rules/*.json`` 选规则、调用
:mod:`src.quant.strategy_lab` 的解析与回测适配层、展示结果。不执行用户输入
的任何代码，参数由规则 JSON 决定；新增或修改规则请直接编辑规则文件。

视觉与「市场趋势」「行业热度」两个分析页同源：底色、卡片、边框、网格线与
选中行全部取自 :mod:`src.ui.advice.tokens` 的 ``palette``，字号取自 ``TOKENS``
与 :mod:`src.ui.table_sizing`，页面内不写死色值与字号；表格列宽统一走
``apply_content_widths``。
"""
from __future__ import annotations

from typing import Any, Dict, List

from loguru import logger
from PySide6.QtCore import Qt, QThread, Signal
from PySide6.QtGui import QColor, QFont
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QFrame,
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
from src.ui.advice.tokens import TOKENS, palette_for
from src.ui.table_sizing import (
    FIXED_ROW_HEIGHT,
    HEADER_ROW_HEIGHT,
    TABLE_FONT_SIZE_PX,
    apply_content_widths,
    use_interactive_columns,
)
from src.ui.theme_manager import is_dark_palette

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


def _qss(theme: str) -> str:
    """页面 QSS：与市场趋势 / 行业热度页同源，只从 palette 与 TOKENS 取值。"""
    palette = palette_for(theme)
    tokens = TOKENS
    return f"""
QWidget#strategyLabPage {{
    background: {palette.bg};
    color: {palette.text};
    font-family: {tokens.font_family};
    font-size: {TABLE_FONT_SIZE_PX}px;
}}
QLabel#slTitle {{
    font-size: {tokens.font_title}px;
    font-weight: 700;
    color: {palette.text};
}}
QLabel#slSubtitle, QLabel#slStatus, QLabel#slNotes {{
    font-size: {tokens.font_small}px;
    color: {palette.muted};
}}
QLabel#slSectionTitle {{
    font-size: {tokens.font_section}px;
    font-weight: 700;
    color: {palette.text};
}}
QFrame#slCard {{
    background: {palette.card};
    border: 1px solid {palette.border};
    border-radius: {tokens.radius}px;
}}
QPushButton {{
    background: {palette.accent_soft};
    color: {palette.accent};
    border: 1px solid {palette.accent};
    border-radius: {tokens.radius_sm}px;
    padding: 5px 16px;
}}
QPushButton:disabled {{
    color: {palette.faint};
    border-color: {palette.border};
    background: {palette.elevated};
}}
QComboBox {{
    background: {palette.surface};
    color: {palette.text};
    border: 1px solid {palette.border};
    border-radius: {tokens.radius_sm}px;
    padding: 5px 12px;
}}
QComboBox:disabled {{
    color: {palette.faint};
}}
QComboBox QAbstractItemView {{
    background: {palette.card};
    color: {palette.text};
    selection-background-color: {palette.selection};
    selection-color: {palette.text};
}}
QTableWidget {{
    background: {palette.card};
    border: 1px solid {palette.border};
    border-radius: {tokens.radius_sm}px;
    gridline-color: {palette.faint};
    selection-background-color: {palette.selection};
}}
QHeaderView {{
    background: {palette.card};
}}
QHeaderView::section {{
    background: {palette.card};
    color: {palette.muted};
    border: none;
    border-bottom: 1px solid {palette.border};
    border-right: 1px solid {palette.border};
    padding: 6px 8px;
    font-weight: 600;
}}
QTableWidget::item {{
    background-color: {palette.card};
    padding: 6px 8px;
}}
"""


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
        self.setObjectName('strategyLabPage')
        self.theme = 'dark' if is_dark_palette() else 'light'
        self._pal = palette_for(self.theme)
        self._worker: _RunWorker | None = None
        self._rule: Dict[str, Any] | None = None
        self._nav_dates: List[str] = []
        self._build_ui()
        self.setStyleSheet(_qss(self.theme))
        self._refresh_rule_list()

    # -- 布局 --------------------------------------------------------------
    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 14, 16, 14)
        root.setSpacing(12)

        header = QHBoxLayout()
        title_box = QVBoxLayout()
        title_box.setSpacing(2)
        title = QLabel('策略试算')
        title.setObjectName('slTitle')
        subtitle = QLabel('规则文件驱动的 ETF 回测：选股 / 择时 / 切换三类规则，只读本地行情')
        subtitle.setObjectName('slSubtitle')
        title_box.addWidget(title)
        title_box.addWidget(subtitle)
        header.addLayout(title_box)
        header.addStretch(1)

        self.status_label = QLabel('选择规则后点击「运行试算」')
        self.status_label.setObjectName('slStatus')
        self.status_label.setWordWrap(True)
        header.addWidget(self.status_label, 1)

        self.run_btn = QPushButton('运行试算')
        self.run_btn.setDefault(True)
        self.run_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.run_btn.clicked.connect(self._on_run)
        header.addWidget(self.run_btn)
        root.addLayout(header)

        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(self._build_editor())
        splitter.addWidget(self._build_result())
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 4)
        root.addWidget(splitter, 1)

    def _build_editor(self) -> QWidget:
        card = QFrame()
        card.setObjectName('slCard')
        outer = QVBoxLayout(card)
        outer.setContentsMargins(14, 12, 14, 12)
        outer.setSpacing(8)

        head = QHBoxLayout()
        caption = QLabel('规则文件')
        caption.setObjectName('slSubtitle')
        head.addWidget(caption)
        self.rule_combo = QComboBox()
        self.rule_combo.setMinimumWidth(260)
        self.rule_combo.currentIndexChanged.connect(lambda *_: self._on_select())
        head.addWidget(self.rule_combo, 1)
        self.refresh_btn = QPushButton('刷新列表')
        self.refresh_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.refresh_btn.clicked.connect(self._refresh_rule_list)
        head.addWidget(self.refresh_btn)
        outer.addLayout(head)

        self.rule_text_label = QLabel('规则目录为空：请在 config/strategy_lab/rules/ 下放置规则 JSON')
        self.rule_text_label.setObjectName('slNotes')
        self.rule_text_label.setWordWrap(True)
        outer.addWidget(self.rule_text_label)
        return card

    def _build_result(self) -> QWidget:
        card = QFrame()
        card.setObjectName('slCard')
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(8)

        section = QLabel('回测结果')
        section.setObjectName('slSectionTitle')
        layout.addWidget(section)

        split = QSplitter(Qt.Horizontal)

        self.metrics_table = QTableWidget(0, 2)
        self.metrics_table.setHorizontalHeaderLabels(['指标', '数值'])
        self.metrics_table.verticalHeader().setVisible(False)
        self.metrics_table.verticalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.Fixed)
        self.metrics_table.verticalHeader().setDefaultSectionSize(FIXED_ROW_HEIGHT)
        self.metrics_table.horizontalHeader().setFixedHeight(HEADER_ROW_HEIGHT)
        self.metrics_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.metrics_table.setSelectionMode(QAbstractItemView.NoSelection)
        self.metrics_table.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        use_interactive_columns(self.metrics_table)
        split.addWidget(self.metrics_table)

        charts = QWidget()
        charts_layout = QVBoxLayout(charts)
        charts_layout.setContentsMargins(0, 0, 0, 0)
        charts_layout.setSpacing(8)
        self.nav_plot = self._add_plot(charts_layout, '净值对比（起点=1）', '净值')
        self.drawdown_plot = self._add_plot(charts_layout, '回撤', '回撤')
        self.marker_plot = self._add_plot(charts_layout, '调仓 / 持仓', '仓位或成分数')
        split.addWidget(charts)
        split.setStretchFactor(0, 1)
        split.setStretchFactor(1, 3)
        layout.addWidget(split, 1)
        return card

    def _add_plot(self, layout: QVBoxLayout, title: str, ylabel: str):
        """三张图共用一套样式：卡片底、palette 网格、TOKENS 坐标轴与图例字号。"""
        section = QLabel(title)
        section.setObjectName('slSectionTitle')
        layout.addWidget(section)
        try:
            import pyqtgraph as pg

            plot = pg.PlotWidget()
            plot.setBackground(None)
            plot.showGrid(x=True, y=True, alpha=0.2)
            plot.setMinimumHeight(120)
            self._style_plot(plot)
            plot.setLabel('left', ylabel, color=self._pal.muted)
        except Exception:  # noqa: BLE001 - 缺少 pyqtgraph 时降级为提示标签
            logger.warning('pyqtgraph 不可用，{} 改为文本提示', title)
            plot = QLabel('当前环境缺少 pyqtgraph，仅展示指标')
            plot.setObjectName('slNotes')
            plot.setWordWrap(True)
        layout.addWidget(plot, 1)
        return plot

    def _style_plot(self, plot) -> None:
        """坐标轴字号与颜色、图例字号统一从 TOKENS 与 palette 取。"""
        import pyqtgraph as pg

        font = QFont()
        font.setPixelSize(TOKENS.font_small)
        for name in ('left', 'bottom'):
            axis = plot.getAxis(name)
            axis.setPen(pg.mkPen(self._pal.faint))
            axis.setTextPen(pg.mkPen(self._pal.muted))
            axis.setStyle(tickFont=font)
        legend = plot.addLegend()
        legend.setLabelTextSize(f'{TOKENS.font_small}px')
        legend.setLabelTextColor(self._pal.muted)

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
            self.rule_text_label.setStyleSheet('')
            return
        try:
            rule = load_rule(path)
        except RuleError as exc:
            self._rule = None
            self.rule_text_label.setText(f'规则不可用：{exc}')
            self.rule_text_label.setStyleSheet(f'color: {self._pal.sell};')
            return
        self._rule = rule
        summary = summarize(rule)
        self.rule_text_label.setText(f'[{summary.type_label}] {summary.text}')
        self.rule_text_label.setStyleSheet('')

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
                               pen=QColor(self._pal.accent),
                               name=PLOT_LEGEND_EQUITY)
            self.nav_plot.setLabel(
                'bottom', f'{nav[0][0]} ~ {nav[-1][0]}（交易日序号）',
                color=self._pal.muted)
        if bench:
            base = float(bench[0][1]) or 1.0
            self.nav_plot.plot(list(range(len(bench))),
                               [float(v) / base for _, v in bench],
                               pen=QColor(self._pal.muted), name=PLOT_LEGEND_BENCH,
                               style=Qt.DashLine)

    def _plot_drawdown(self, drawdown: List[List[Any]]) -> None:
        if not hasattr(self.drawdown_plot, 'plot'):
            return
        self.drawdown_plot.clear()
        if not drawdown:
            return
        xs = list(range(len(drawdown)))
        ys = [float(v) * 100.0 for _, v in drawdown]
        fill = QColor(self._pal.sell)
        fill.setAlpha(60)
        self.drawdown_plot.plot(xs, ys, pen=QColor(self._pal.sell),
                                fillLevel=0, brush=fill)
        self.drawdown_plot.setLabel('bottom', f'{drawdown[0][0]} ~ {drawdown[-1][0]}',
                                    color=self._pal.muted)

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
                              symbolBrush=QColor(self._pal.hold),
                              symbolPen=QColor(self._pal.hold))
        guide = QColor(self._pal.hold)
        guide.setAlpha(90)
        for x in xs:
            line = self.marker_plot.addLine(x=x, pen=guide, movable=False)
            line.setZValue(-10)
