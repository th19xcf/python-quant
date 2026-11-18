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

from html import escape
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

from src.quant.strategy_lab.markers import marker_side
from src.quant.strategy_lab.rules import (
    RULE_TYPES,
    RuleError,
    list_rules,
    load_rule,
    summarize,
)
from src.quant.strategy_lab.save import save_run
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
PLOT_LEGEND_BUY = '买入 / 补仓'
PLOT_LEGEND_SELL = '止盈清仓'

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
QSplitter::handle {{
    background: transparent;
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
        # 本页是直接入 QTabWidget 的 QWidget 子类（其他分析页都套了 QScrollArea），
        # QSS 的 background 只有置位 WA_StyledBackground 才会绘制；否则页面区域透明，
        # 透出 QTabWidget 面板底色（暗色下为 #434343），表现为页头带比别页更亮。
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        self.theme = 'dark' if is_dark_palette() else 'light'
        self._pal = palette_for(self.theme)
        self._worker: _RunWorker | None = None
        self._rule: Dict[str, Any] | None = None
        self._last_payload: Dict[str, Any] | None = None
        self._nav_dates: List[str] = []
        self._build_ui()
        self.setStyleSheet(_qss(self.theme))
        self._refresh_rule_list()

    # -- 布局 --------------------------------------------------------------
    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 14, 16, 14)
        root.setSpacing(12)

        header = QVBoxLayout()
        header.setSpacing(6)
        top_row = QHBoxLayout()
        title_box = QVBoxLayout()
        title_box.setSpacing(2)
        title = QLabel('策略试算')
        title.setObjectName('slTitle')
        subtitle = QLabel('规则文件驱动的 ETF 回测：选股 / 择时 / 切换三类规则，只读本地行情')
        subtitle.setObjectName('slSubtitle')
        title_box.addWidget(title)
        title_box.addWidget(subtitle)
        top_row.addLayout(title_box)
        top_row.addStretch(1)

        self.run_btn = QPushButton('运行试算')
        self.run_btn.setDefault(True)
        self.run_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.run_btn.clicked.connect(self._on_run)
        top_row.addWidget(self.run_btn)

        self.save_btn = QPushButton('保存结果')
        self.save_btn.setEnabled(False)
        self.save_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.save_btn.clicked.connect(self._on_save)
        top_row.addWidget(self.save_btn)
        header.addLayout(top_row)

        # 说明文字放到左上角：另起一行、左对齐、占满可用宽度再折行，
        # 不再与标题挤在同一行、也不再被按钮占掉宽度。
        self.status_label = QLabel('选择规则后点击「运行试算」')
        self.status_label.setObjectName('slStatus')
        self.status_label.setWordWrap(True)
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        # QLabel 高度按字体的 ascent+descent 算，QSS 又没有内边距，首行字头正好
        # 贴在 label 上边缘（离屏实测首墨迹行=0），中文小字看上去像被裁掉一截。
        # 只补 3px 上留白，不动宽度策略与两行结构。
        self.status_label.setContentsMargins(0, 3, 0, 0)
        header.addWidget(self.status_label)
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

        split = QSplitter(Qt.Horizontal)

        # 「回测结果」标题放进左列，与表格同列：右列图表容器的顶边因此与标题
        # 顶部齐平（原先标题独占整行，图表容器整体低 21px）。
        left = QWidget()
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(8)
        left_layout.addWidget(section)

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
        left_layout.addWidget(self.metrics_table, 1)
        split.addWidget(left)

        charts = QWidget()
        charts_layout = QVBoxLayout(charts)
        charts_layout.setContentsMargins(0, 0, 0, 0)
        charts_layout.setSpacing(8)
        self.nav_plot = self._add_plot(
            charts_layout, '净值对比（起点=1）', '净值',
            legend=[(PLOT_LEGEND_EQUITY, self._pal.accent, 'solid'),
                    (PLOT_LEGEND_BENCH, self._pal.muted, 'dash')])
        self.drawdown_plot = self._add_plot(charts_layout, '回撤', '回撤')
        # 买卖标记图例：只有真的出现买/卖标记时才显示，避免空条目
        self.marker_plot = self._add_plot(
            charts_layout, '调仓 / 持仓', '仓位或成分数',
            legend=[(PLOT_LEGEND_BUY, self._pal.buy, 'up'),
                    (PLOT_LEGEND_SELL, self._pal.sell, 'down')],
            legend_visible=False)
        self._link_x_axes()
        split.addWidget(charts)
        split.setStretchFactor(0, 1)
        split.setStretchFactor(1, 3)
        layout.addWidget(split, 1)
        return card

    def _add_plot(self, layout: QVBoxLayout, title: str, ylabel: str, *,
                  legend: List[tuple] | None = None, legend_visible: bool = True):
        """三张图共用一套样式：卡片底、palette 网格、TOKENS 坐标轴；图例按需建在标题行。"""
        section = QLabel(title)
        section.setObjectName('slSectionTitle')
        badge = None
        if legend:
            # 图例放在标题行右侧：与标题同高、右对齐，完全不进入绘图区，
            # 任何数据形态都不会被图例遮住。
            badge = self._legend_badge(legend)
            badge.setVisible(legend_visible)
            title_row = QHBoxLayout()
            title_row.setContentsMargins(0, 0, 0, 0)
            title_row.setSpacing(8)
            title_row.addWidget(section)
            title_row.addStretch(1)
            title_row.addWidget(badge)
            layout.addLayout(title_row)
        else:
            layout.addWidget(section)
        try:
            import pyqtgraph as pg

            plot = pg.PlotWidget()
            plot.setBackground(self._pal.card)
            plot.showGrid(x=True, y=True, alpha=0.2)
            plot.setMinimumHeight(120)
            self._style_plot(plot)
            plot.setLabel('left', ylabel, color=self._pal.muted)
        except Exception:  # noqa: BLE001 - 缺少 pyqtgraph 时降级为提示标签
            logger.warning('pyqtgraph 不可用，{} 改为文本提示', title)
            plot = QLabel('当前环境缺少 pyqtgraph，仅展示指标')
            plot.setObjectName('slNotes')
            plot.setWordWrap(True)
        plot.legend_badge = badge
        layout.addWidget(plot, 1)
        return plot

    def _legend_badge(self, entries: List[tuple]) -> QWidget:
        """标题行图例：条目 = (名称, 颜色, 线型/标记)，底色半透明。"""
        badge = QWidget()
        badge.setObjectName('slLegend')
        badge.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        bg = QColor(self._pal.card)
        badge.setStyleSheet(
            f'QWidget#slLegend {{ background: rgba({bg.red()},{bg.green()},{bg.blue()},200);'
            f' border: 1px solid {self._pal.border};'
            f' border-radius: {TOKENS.radius_sm}px; }}'
            f' QLabel {{ color: {self._pal.muted};'
            f' font-size: {TOKENS.font_small}px; }}')
        box = QHBoxLayout(badge)
        box.setContentsMargins(8, 2, 10, 2)
        box.setSpacing(14)
        for text, color, kind in entries:
            entry = QHBoxLayout()
            entry.setSpacing(5)
            swatch = QLabel()
            swatch.setPixmap(self._legend_swatch(color, kind))
            swatch.setFixedSize(16, 8)
            name = QLabel(text)
            entry.addWidget(swatch, 0, Qt.AlignmentFlag.AlignVCenter)
            entry.addWidget(name, 0, Qt.AlignmentFlag.AlignVCenter)
            box.addLayout(entry)
        return badge

    @staticmethod
    def _legend_swatch(color: str, kind: str) -> Any:
        """图例色条：solid/dash 画线段，up/down 画买卖三角，与绘图标记同一套规格。"""
        from PySide6.QtGui import QPainter, QPainterPath, QPen, QPixmap

        pixmap = QPixmap(16, 8)
        pixmap.fill(Qt.GlobalColor.transparent)
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        if kind in ('up', 'down'):
            path = QPainterPath()
            if kind == 'up':
                path.moveTo(8, 0)
                path.lineTo(14, 7)
                path.lineTo(2, 7)
            else:
                path.moveTo(2, 0)
                path.lineTo(14, 0)
                path.lineTo(8, 7)
            path.closeSubpath()
            painter.setPen(QPen(QColor(color), 1))
            painter.setBrush(QColor(color))
            painter.drawPath(path)
        else:
            pen = QPen(QColor(color))
            pen.setWidth(2)
            pen.setStyle(Qt.PenStyle.DashLine if kind == 'dash'
                         else Qt.PenStyle.SolidLine)
            painter.setPen(pen)
            painter.drawLine(0, 4, 15, 4)
        painter.end()
        return pixmap

    def _link_x_axes(self) -> None:
        """三图共用净值时间轴：X 轴联动缩放/平移，Y 轴仍各自独立。"""
        plots = (self.nav_plot, self.drawdown_plot, self.marker_plot)
        if not all(hasattr(plot, 'setXLink') for plot in plots):
            return
        self.drawdown_plot.setXLink(self.nav_plot.plotItem)
        self.marker_plot.setXLink(self.nav_plot.plotItem)

    def _style_plot(self, plot) -> None:
        """坐标轴字号与颜色统一从 TOKENS 与 palette 取；图例由标题行承载，不建在坐标区内。"""
        import pyqtgraph as pg

        font = QFont()
        font.setPixelSize(TOKENS.font_small)
        for name in ('left', 'bottom'):
            axis = plot.getAxis(name)
            axis.setPen(pg.mkPen(self._pal.faint))
            axis.setTextPen(pg.mkPen(self._pal.muted))
            axis.setStyle(tickFont=font)

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
        self.save_btn.setEnabled(False)
        self._last_payload = None
        self.status_label.setText(f'试算中：{RULE_TYPES[rule["rule_type"]]}规则，'
                                  '加载本地行情与 PIT 字段面板…')
        self._worker = _RunWorker(rule, self)
        self._worker.ok.connect(self._on_result)
        self._worker.failed.connect(self._on_failed)
        self._worker.finished.connect(lambda: self.run_btn.setEnabled(True))
        self._worker.start()

    def _on_failed(self, message: str) -> None:
        self._last_payload = None
        self.save_btn.setEnabled(False)
        self.status_label.setText(f'试算失败：{message}')

    def _on_save(self) -> None:
        """把最近一次成功试算落盘；同一结果可多次保存，各自独立成目录。"""
        if not self._last_payload:
            return
        try:
            info = save_run(self._last_payload, self._rule or {})
        except Exception as exc:  # noqa: BLE001 - 保存失败只提示，不影响已有结果
            logger.exception('策略试算结果保存失败')
            self.status_label.setText(f'保存失败：{exc}')
            return
        self.status_label.setText(f'已保存：{info["run_id"]}')

    # -- 展示 --------------------------------------------------------------
    def _on_result(self, payload: Dict[str, Any]) -> None:
        if payload.get('status') != 'ok':
            self._last_payload = None
            self.save_btn.setEnabled(False)
            self.status_label.setText(f'空结果：{payload.get("reason", "无可用结果")}')
            self._fill_metrics({'rebalances': 0})
            self._plot_curves([], [])
            self._plot_drawdown([])
            self._plot_markers([])
            return
        self._last_payload = payload
        self.save_btn.setEnabled(True)
        self._fill_metrics(payload['metrics'],
                           (payload.get('dividend_coverage') or {}).get('message') or '')
        self._plot_curves(payload.get('nav') or [], payload.get('benchmark_curve') or [])
        self._plot_drawdown(payload.get('drawdown') or [])
        self._plot_markers(payload.get('markers') or [])
        hint = (payload.get('dividend_coverage') or {}).get('message') or ''
        self._set_status_result(
            f'{payload["rule_text"]}  用时 {payload["elapsed_seconds"]}s '
            f'（引擎 {payload["engine_seconds"]}s，参与标的 {payload["used_codes"]} 只）'
            + (f'  {hint}' if hint else ''))

    def _set_status_result(self, text: str) -> None:
        """结果说明用富文本：只把规则名加粗上色，其余文字仍是正文样式与折行。

        规则名带「」且固定出现在 rule_text 开头，按首次出现替换成带样式
        片段；正文与名称的字号都显式写出，不依赖 QLabel 的 QSS 继承。
        """
        name = (self._rule or {}).get('name') or ''
        body = escape(text)
        marker = escape(f'「{name}」') if name else ''
        if marker and marker in body:
            body = body.replace(
                marker,
                f'<span style="font-size:{TOKENS.font_small + 1}px;'
                f'font-weight:600;color:{self._pal.accent};">{marker}</span>',
                1)
        self.status_label.setText(
            f'<span style="font-size:{TOKENS.font_small}px;'
            f'color:{self._pal.muted};">{body}</span>')

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
        # 表格在 QSplitter 里按 sizeHint 只分到约 150 px，列宽按实测给足后会被横向
        # 滚动条挤出视口（数值列看不到、像被截断）。用同一次实测的列宽合计把表格
        # 最小宽撑到刚好放下全部列：既不留空白，也不出横向滚动条。
        table = self.metrics_table
        columns = sum(table.columnWidth(i) for i in range(table.columnCount()))
        table.setMinimumWidth(columns + 2 * table.frameWidth()
                              + table.verticalScrollBar().sizeHint().width())

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
        badge = getattr(self.marker_plot, 'legend_badge', None)
        if not markers:
            if badge is not None:
                badge.setVisible(False)
            return
        index = {day: i for i, day in enumerate(self._nav_dates)}
        buy_x: List[int] = []
        buy_y: List[float] = []
        sell_x: List[int] = []
        sell_y: List[float] = []
        other_x: List[int] = []
        other_y: List[float] = []
        prev_position = 0.0
        for marker in sorted(markers, key=lambda m: index.get(str(m.get('date')), -1)):
            x = index.get(str(marker.get('date')))
            if x is None:
                continue
            position = marker.get('position')
            if marker.get('members') is not None:
                y = float(marker['members'])
            else:
                y = float(position or 1.0)
            # 方向判定与离线出图共用 markers.marker_side，不在这里各写一份
            kind, prev_position = marker_side(marker, prev_position)
            if kind == 'buy':
                buy_x.append(x)
                buy_y.append(y)
            elif kind == 'sell':
                sell_x.append(x)
                sell_y.append(y)
            else:
                # 选股调仓没有买卖方向，沿用中性圆点
                other_x.append(x)
                other_y.append(y)
        xs = buy_x + sell_x + other_x
        if not xs:
            return
        if other_x:
            self.marker_plot.plot(other_x, other_y, pen=None, symbol='o', symbolSize=6,
                                  symbolBrush=QColor(self._pal.hold),
                                  symbolPen=QColor(self._pal.hold))
        # 买入用红色上三角、卖出用绿色下三角，颜色取页内涨跌 token；描边用卡片底
        # 色隔开重叠标记。同一交易日同向多笔共用一个 x，标记完全叠放（不做偏移）。
        if buy_x:
            self.marker_plot.plot(buy_x, buy_y, pen=None, symbol='t', symbolSize=9,
                                  symbolBrush=QColor(self._pal.buy),
                                  symbolPen=QColor(self._pal.card))
        if sell_x:
            self.marker_plot.plot(sell_x, sell_y, pen=None, symbol='t1', symbolSize=9,
                                  symbolBrush=QColor(self._pal.sell),
                                  symbolPen=QColor(self._pal.card))
        badge = getattr(self.marker_plot, 'legend_badge', None)
        if badge is not None:
            badge.setVisible(bool(buy_x or sell_x))
        guide = QColor(self._pal.hold)
        guide.setAlpha(90)
        for x in xs:
            line = self.marker_plot.addLine(x=x, pen=guide, movable=False)
            line.setZValue(-10)
