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
from pathlib import Path
from typing import Any, Dict, List

from loguru import logger
from PySide6.QtCore import QDate, QEvent, Qt, QThread, Signal
from PySide6.QtGui import QColor, QFont
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QDateEdit,
    QDialog,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMessageBox,
    QPushButton,
    QSizePolicy,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from src.quant.strategy_lab.markers import layout_markers
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
    FIXED_ROW_HEIGHT_THRESHOLD,
    HEADER_ROW_HEIGHT,
    TABLE_FONT_SIZE_PX,
    TOOLTIP_TEXT_MIN_LEN,
    WrappedRowDelegate,
    apply_content_widths,
    sort_rows_by_column,
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


def _security_names() -> Dict[str, str]:
    """读本地名录（``data/local_store/meta/names.json``）给流水弹窗补名称列。"""
    try:
        from src.data.name_consistency import load_names_json

        root = Path(__file__).resolve().parents[3] / 'data' / 'local_store'
        return {str(k): str(v) for k, v in (load_names_json(root) or {}).items()}
    except Exception:  # noqa: BLE001 - 名录缺失只影响名称列，不该挡住流水
        logger.warning('读取证券名称表失败，交易流水名称列留空')
        return {}


class _TradeDialog(QDialog):
    """逐笔成交明细弹窗：日期正序、表头可排序、末行合计、空数据给中文提示。"""

    COLUMNS = ('日期', '代码', '名称', '方向', '成交价', '数量', '金额', '费用',
               '已实现盈亏', '收益率', '持有盈亏', '持有收益率')
    _NUMERIC = (4, 5, 6, 7, 8, 9, 10, 11)

    def __init__(self, payload: Dict[str, Any], parent=None):
        super().__init__(parent)
        self.theme = 'dark' if is_dark_palette() else 'light'
        self._pal = palette_for(self.theme)
        self.setWindowTitle('交易流水')
        self.setStyleSheet(_qss(self.theme))
        self._sort_column = 0
        self._sort_desc = False
        rows = self._rows(payload)

        root = QVBoxLayout(self)
        root.setContentsMargins(14, 12, 14, 12)
        root.setSpacing(8)
        if rows:
            caption_text = f'共 {len(rows)} 笔成交（按日期正序；点击表头可排序）'
            valuation_date = (payload.get('valuation') or {}).get('date')
            if valuation_date and any(row['holding_pnl'] is not None
                                      for row in rows):
                caption_text += (f'；持有盈亏按 {valuation_date} 收盘价估值，'
                                 f'按各笔买入成本占比分摊到仍持有的买入行')
            caption = QLabel(caption_text)
            caption.setObjectName('slNotes')
            root.addWidget(caption)
            self.table = self._build_table(rows)
            root.addWidget(self.table, 1)
        else:
            empty = QLabel('该规则在统计区间内没有成交记录')
            empty.setObjectName('slNotes')
            empty.setAlignment(Qt.AlignmentFlag.AlignCenter)
            root.addWidget(empty, 1)

        close = QPushButton('关闭')
        close.setCursor(Qt.CursorShape.PointingHandCursor)
        close.clicked.connect(self.accept)
        bottom = QHBoxLayout()
        bottom.addStretch(1)
        bottom.addWidget(close)
        root.addLayout(bottom)
        self.resize(1320, 620)

    @staticmethod
    def _rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        """把逐笔成交按时间顺序推进持仓成本，卖出时算该笔已实现盈亏与收益率。

        口径：同一标的的卖出，按「卖出价 − 该标的持仓单位成本（买入费用已摊入）
        × 数量 − 本笔卖出费用」计算已实现盈亏；收益率 = 该笔已实现盈亏 ÷ 该笔
        对应持仓成本（单位成本 × 数量，含买入费用）。买入行两项留空。

        持有盈亏只给回测结束时仍持仓的标的，按各笔仍持有买入的成本占比把整只
        未实现盈亏分摊到这些买入行上：整只浮亏按「估值日收盘价 − 剩余持仓单位
        成本」× 剩余股数算出，再按各笔买入成本（金额 + 买入费用）占比拆到行，
        末笔吸收分币残差使各行之和精确等于整只浮亏；收益率 = 该笔分摊盈亏 ÷
        该笔买入成本，不重复计入已实现盈亏。已清仓的标的两列保持空。
        """
        records = payload.get('trades') or []
        if not records:
            return []
        names = _security_names()
        book: Dict[str, tuple[float, float]] = {}
        lots: Dict[str, List[Dict[str, Any]]] = {}
        rows: List[Dict[str, Any]] = []
        for record in sorted(records, key=lambda r: (r['date'], r['side'] == 'sell')):
            code = str(record.get('code') or '')
            shares = float(record.get('shares') or 0.0)
            price = float(record.get('price') or 0.0)
            fee = float(record.get('cost') or 0.0)
            held, held_cost = book.get(code, (0.0, 0.0))
            realized = None
            basis = None
            if record.get('side') == 'buy':
                book[code] = (held + shares, held_cost + shares * price + fee)
            else:
                unit_cost = held_cost / held if held > 0 else 0.0
                basis = unit_cost * shares
                realized = (price - unit_cost) * shares - fee
                book[code] = (held - shares, max(held_cost - basis, 0.0))
                if book[code][0] <= 0:
                    # 清仓后之前的买入不再持有，重启买入另起一批成本占比
                    lots[code] = []
            row = {'date': record['date'], 'code': code,
                   'name': names.get(code, ''), 'side': record.get('side'),
                   'price': price, 'shares': shares,
                   'amount': price * shares, 'fee': fee,
                   'realized': realized, 'basis': basis,
                   'holding_pnl': None, 'holding_cost': None,
                   'holding_yield': None}
            rows.append(row)
            if row['side'] == 'buy':
                lots.setdefault(code, []).append(
                    {'row': row, 'cost': row['amount'] + fee})
        _TradeDialog._attach_holdings(rows, book, payload, lots)
        # 日期正序；同一日期内买入在前，保持与成本推进顺序一致
        rows.sort(key=lambda row: row['side'] == 'sell')
        rows.sort(key=lambda row: row['date'])
        return rows

    @staticmethod
    def _attach_holdings(rows: List[Dict[str, Any]],
                         book: Dict[str, tuple[float, float]],
                         payload: Dict[str, Any],
                         lots: Dict[str, List[Dict[str, Any]]]) -> None:
        """把整只未实现盈亏按成本占比分摊到各笔仍持有的买入行。"""
        held = {str(code): float(qty or 0.0)
                for code, qty in (payload.get('positions') or {}).items()
                if float(qty or 0.0) > 0}
        if not held:
            return
        close = ((payload.get('valuation') or {}).get('close') or {})
        for code, qty in held.items():
            shares, cost = book.get(code, (0.0, 0.0))
            price = close.get(code)
            code_lots = lots.get(code) or []
            total_cost = sum(lot['cost'] for lot in code_lots)
            if shares <= 0 or price is None or not code_lots or total_cost <= 0:
                continue
            total_pnl = (float(price) - cost / shares) * qty
            # 逐笔按成本占比分摊，末笔吸收分币残差，行和精确等于整只浮亏
            allocated = 0.0
            for index, lot in enumerate(code_lots):
                if index == len(code_lots) - 1:
                    pnl = round(round(total_pnl, 2) - allocated, 2)
                else:
                    pnl = round(total_pnl * lot['cost'] / total_cost, 2)
                    allocated = round(allocated + pnl, 2)
                lot['row']['holding_pnl'] = pnl
                lot['row']['holding_cost'] = lot['cost']
                lot['row']['holding_yield'] = (pnl / lot['cost']
                                               if lot['cost'] > 0 else None)

    @staticmethod
    def _totals(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """合计行口径：金额分买入/卖出小计，收益率为总已实现 ÷ 总买入成本。

        持有盈亏合计 = 各持仓未实现盈亏之和；持有收益率 = 合计未实现盈亏 ÷
        各持仓成本之和（成本加权），不是各行收益率的算术平均。
        """
        buy_amount = sum(row['amount'] for row in rows if row['side'] == 'buy')
        sell_amount = sum(row['amount'] for row in rows if row['side'] == 'sell')
        buy_cost = sum(row['amount'] + row['fee'] for row in rows
                       if row['side'] == 'buy')
        realized = sum(row['realized'] for row in rows
                       if row['realized'] is not None)
        holding_rows = [row for row in rows if row['holding_pnl'] is not None]
        holding_pnl = sum(row['holding_pnl'] for row in holding_rows)
        holding_cost = sum(row['holding_cost'] for row in holding_rows)
        return {'buy_amount': buy_amount, 'sell_amount': sell_amount,
                'realized': realized,
                'yield': realized / buy_cost if buy_cost > 0 else None,
                'holding_count': len(holding_rows),
                'holding_pnl': holding_pnl,
                'holding_yield': (holding_pnl / holding_cost
                                  if holding_cost > 0 else None)}

    def _build_table(self, rows: List[Dict[str, Any]]) -> QTableWidget:
        table = QTableWidget(len(rows), len(self.COLUMNS))
        table.setHorizontalHeaderLabels(list(self.COLUMNS))
        table.setWordWrap(True)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.verticalHeader().setVisible(False)
        table.horizontalHeader().setHighlightSections(False)
        table.horizontalHeader().setFixedHeight(HEADER_ROW_HEIGHT)
        table.setItemDelegate(WrappedRowDelegate(table))

        for r, row in enumerate(rows):
            realized = row['realized']
            yield_pct = (row['realized'] / row['basis']
                         if row['basis'] else None)
            holding = row['holding_pnl']
            holding_yield = row['holding_yield']
            cells = (
                row['date'], row['code'], row['name'],
                '买入' if row['side'] == 'buy' else '卖出',
                f'{row["price"]:.3f}', f'{row["shares"]:.0f}',
                f'{row["amount"]:,.2f}', f'{row["fee"]:,.2f}',
                '' if realized is None else f'{realized:+,.2f}',
                '' if yield_pct is None else f'{yield_pct * 100:+.2f}%',
                '' if holding is None else f'{holding:+,.2f}',
                '' if holding_yield is None else f'{holding_yield * 100:+.2f}%',
            )
            numeric = (row['price'], row['shares'], row['amount'], row['fee'],
                       realized, yield_pct, holding, holding_yield)
            for c, text in enumerate(cells):
                item = QTableWidgetItem(text)
                if c in self._NUMERIC:
                    item.setTextAlignment(Qt.AlignmentFlag.AlignRight
                                          | Qt.AlignmentFlag.AlignVCenter)
                    if numeric[c - 4] is not None:
                        item.setData(Qt.ItemDataRole.UserRole, numeric[c - 4])
                table.setItem(r, c, item)

        self.table = table
        self._totals_data = self._totals(rows)
        self._append_total_row()
        apply_content_widths(table)
        if table.rowCount() > FIXED_ROW_HEIGHT_THRESHOLD:
            table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
            table.verticalHeader().setDefaultSectionSize(FIXED_ROW_HEIGHT)
            for r in range(table.rowCount()):
                table.setRowHeight(r, FIXED_ROW_HEIGHT)
                for c in range(table.columnCount()):
                    item = table.item(r, c)
                    if item is not None and len(item.text()) >= TOOLTIP_TEXT_MIN_LEN:
                        item.setToolTip(item.text())
        else:
            table.resizeRowsToContents()
        use_interactive_columns(table)
        table.horizontalHeader().sectionClicked.connect(self._on_sort)
        return table

    def _append_total_row(self) -> None:
        """末行合计：加粗 + 底色区分，固定在最底部，不参与排序。"""
        table = self.table
        totals = self._totals_data
        yield_total = totals['yield']
        realized = totals['realized']
        holding_pnl = totals['holding_pnl']
        holding_yield = totals['holding_yield']
        cells = ('合计', '', '', '', '', '',
                 f'买 {totals["buy_amount"]:,.2f} / 卖 {totals["sell_amount"]:,.2f}',
                 '', f'{realized:+,.2f}',
                 '' if yield_total is None else f'{yield_total * 100:+.2f}%',
                 '' if not totals['holding_count'] else f'{holding_pnl:+,.2f}',
                 '' if holding_yield is None else f'{holding_yield * 100:+.2f}%')
        row = table.rowCount()
        table.insertRow(row)
        bold = QFont(table.font())
        bold.setBold(True)
        background = QColor(self._pal.elevated)
        for c, text in enumerate(cells):
            item = QTableWidgetItem(text)
            item.setFont(bold)
            item.setBackground(background)
            item.setFlags(Qt.ItemFlag.ItemIsEnabled)
            if c in self._NUMERIC:
                item.setTextAlignment(Qt.AlignmentFlag.AlignRight
                                      | Qt.AlignmentFlag.AlignVCenter)
            elif c == 0:
                item.setTextAlignment(Qt.AlignmentFlag.AlignLeft
                                      | Qt.AlignmentFlag.AlignVCenter)
            table.setItem(row, c, item)
        table.setRowHeight(row, FIXED_ROW_HEIGHT)

    def _on_sort(self, column: int) -> None:
        if getattr(self, 'table', None) is None:
            return
        if column == self._sort_column:
            self._sort_desc = not self._sort_desc
        else:
            self._sort_column = column
            self._sort_desc = False
        # 合计行固定在最底部：排序前摘掉、排完再补回
        self.table.setRowCount(self.table.rowCount() - 1)
        sort_rows_by_column(self.table, column, descending=self._sort_desc)
        self._append_total_row()


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
        # 标题块用 Maximum 策略包成独立控件：否则它会吃掉整行剩余宽度，
        # 说明块的左边界会被推到最右侧（实测副标题被拉到 1023px 宽）。
        title_block = QWidget()
        title_block.setSizePolicy(QSizePolicy.Policy.Maximum, QSizePolicy.Policy.Preferred)
        title_box = QVBoxLayout(title_block)
        title_box.setContentsMargins(0, 0, 0, 0)
        title_box.setSpacing(2)
        title = QLabel('策略试算')
        title.setObjectName('slTitle')
        subtitle = QLabel('规则文件驱动的 ETF 回测：选股 / 择时 / 切换三类规则，只读本地行情')
        subtitle.setObjectName('slSubtitle')
        title_box.addWidget(title)
        title_box.addWidget(subtitle)
        self.title_label = title
        self.subtitle_label = subtitle
        top_row.addWidget(title_block)
        # 说明块紧跟标题块右侧 24px 起排、右侧停在按钮列左缘前 16px；
        # 宽度由 _sync_status_max_width 按标题块 sizeHint 与按钮列左缘实测算出。
        top_row.addSpacing(24)
        # 说明块放页头右侧空白区：文字左对齐、贴顶部，宽度按可用区域自适应并折行。
        self.status_label = QLabel('选择规则后点击「运行试算」')
        self.status_label.setObjectName('slStatus')
        self.status_label.setWordWrap(True)
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        # 横向 Ignored + 只设最大宽：说明块铺满标题行剩余宽度，但不参与页面最小宽
        # 计算。若用固定宽，页面最小宽会被顶到接近当前宽，窗口宽度来回抖动且在
        # 窄档退不下去（离屏实测 1553 档最小宽 1535、1176 档回不去）。
        self.status_label.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        self.status_label.setMinimumWidth(240)
        # QLabel 高度按字体的 ascent+descent 算，QSS 又没有内边距，首行字头正好
        # 贴在 label 上边缘（离屏实测首墨迹行=0），中文小字看上去像被裁掉一截。
        self.status_label.setContentsMargins(0, 3, 0, 0)
        top_row.addWidget(self.status_label, 1)
        header.addLayout(top_row)
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
        # QSS 的 padding 5px + border 1px 上下共 12px 不计入 QComboBox 的 sizeHint，
        # 默认 20px 高只剩 8px 内容区，12px 文字上下各被裁约 2px；按字体行高 +
        # 内边距 + 上下各 2px 余量给最小高度。
        self.rule_combo.setMinimumHeight(self.rule_combo.fontMetrics().height() + 16)
        self.rule_combo.currentIndexChanged.connect(lambda *_: self._on_select())
        head.addWidget(self.rule_combo, 1)
        # 试算起点：默认取规则文件 period.start，可覆盖；只改净值/指标/三图起算日，
        # 预热（滚动分位、动量、财报 as-of）仍用起点前的完整历史，不裁剪。
        head.addWidget(QLabel('试算起点'))
        self.start_date = QDateEdit()
        self.start_date.setObjectName('slStartDate')
        self.start_date.setCalendarPopup(True)
        self.start_date.setDisplayFormat('yyyy-MM-dd')
        self.start_date.setMinimumWidth(120)
        self.start_date.setMinimumHeight(self.rule_combo.minimumHeight())
        self.start_date.setToolTip('默认取规则文件的 period.start；'
                                   '只影响净值、指标与三张图的起算日，'
                                   '预热仍使用起点之前的完整历史')
        head.addWidget(self.start_date)
        self.refresh_btn = QPushButton('刷新列表')
        self.refresh_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.refresh_btn.clicked.connect(self._refresh_rule_list)
        head.addWidget(self.refresh_btn)

        # 三个按钮同组并排放在规则行右端：与标题区解耦，窄窗口下也不会压到说明块。
        self.run_btn = QPushButton('运行试算')
        self.run_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.run_btn.setDefault(True)
        self.run_btn.clicked.connect(self._on_run)
        head.addWidget(self.run_btn)

        self.save_btn = QPushButton('保存结果')
        self.save_btn.setEnabled(False)
        self.save_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.save_btn.clicked.connect(self._on_save)
        head.addWidget(self.save_btn)

        self.trade_btn = QPushButton('查看流水')
        self.trade_btn.setEnabled(False)
        self.trade_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.trade_btn.clicked.connect(self._on_trades)
        head.addWidget(self.trade_btn)
        outer.addLayout(head)

        self.rule_text_label = QLabel('规则目录为空：请在 config/strategy_lab/rules/ 下放置规则 JSON')
        self.rule_text_label.setObjectName('slNotes')
        self.rule_text_label.setWordWrap(True)
        # 与本页 slStatus 同一个成因：QLabel 高度只按 ascent+descent 算、QSS 无内边距，
        # 首行字头顶在控件上边缘、末行字底贴下边缘，看上去像被裁；上下各补留白。
        self.rule_text_label.setContentsMargins(0, 3, 0, 2)
        outer.addWidget(self.rule_text_label)

        self.period_hint = QLabel(
            '试算起点只影响净值、指标与三张图的起算日；滚动分位、动量与财报 as-of '
            '等预热仍使用起点之前的完整历史。')
        self.period_hint.setObjectName('slNotes')
        self.period_hint.setWordWrap(True)
        self.period_hint.setContentsMargins(0, 0, 0, 2)
        outer.addWidget(self.period_hint)
        self._editor_card = card
        card.installEventFilter(self)
        self._sync_editor_min_height()
        return card

    def _sync_status_max_width(self) -> None:
        """说明块宽度：左贴标题块右缘 +24px，右缘与同列控件（卡片/按钮行）右缘对齐。

        标题块按 sizeHint（Maximum 策略）取宽，不能用它的实际 width——实测被布局
        拉宽到 1023px 时，算出的上限会退化成兜底值、说明块反被推到最右侧。
        取「页面右缘」而不是「按钮列左缘」：按钮在下一行的卡片里，两者不同行，
        按按钮列收会让说明块右侧空出一大片（实测 125% 缩放下空 484px）。
        左边锚点取根布局左边距而不是 title_label.mapTo：布局未稳定时 mapTo 会拿到
        中间态 x（实测 104/43/23 逐轮收敛到 16），算出的最大宽偏小、右缘差 28px。
        """
        if not hasattr(self, 'refresh_btn') or not hasattr(self, 'title_label'):
            return
        block_width = max(self.title_label.sizeHint().width(),
                          self.subtitle_label.sizeHint().width())
        block_left = self.layout().contentsMargins().left()
        block_right = block_left + block_width + 24
        page_right = self.width() - self.layout().contentsMargins().right()
        width = max(page_right - block_right, 240)
        if self.status_label.maximumWidth() != width:
            self.status_label.setMaximumWidth(width)

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._sync_status_max_width()

    def _sync_editor_min_height(self) -> None:
        """编辑卡片的最小高按说明块实际折行算。

        QSplitter 分给这张卡片的只有布局最小高，而最小高既不含下拉框的 QSS
        内边距也不含说明块的折行高度，实测会把控件间距从 8px 压到 2px、把说明块
        压成一行高：结果下拉框文字下半截、说明块首行上半截同时被裁。按「上下边距
        + 控件行高 + 行距 + 说明块折行高度 + 说明块上下留白」显式给最小高。
        """
        layout = self._editor_card.layout()
        margins = layout.contentsMargins()
        note_margins = self.rule_text_label.contentsMargins()
        row_height = max(self.rule_combo.minimumHeight(),
                         self.refresh_btn.sizeHint().height())
        # 折行高度必须按卡片宽度算：说明块自身的 width 在布局过程中是旧值，
        # 用它会把最小高算成 4~5 行（实测 84px），反把卡片撑高。
        width = max(self._editor_card.width() - margins.left() - margins.right(), 120)
        text_height = self.rule_text_label.heightForWidth(width)
        hint_height = self.period_hint.heightForWidth(width)
        needed = (margins.top() + margins.bottom() + row_height + layout.spacing()
                  + text_height + note_margins.top() + note_margins.bottom()
                  + hint_height + layout.spacing())
        if self._editor_card.minimumHeight() != needed:
            self._editor_card.setMinimumHeight(needed)

    def eventFilter(self, obj, event) -> bool:
        # 卡片几何变化后：说明块折行行数会变（重算最小高），按钮列左缘也会变
        # （重算页头说明块宽度）；两处都按值比较，值不变时不重设，避免布局往复。
        if obj is getattr(self, '_editor_card', None) and event.type() == QEvent.Type.Resize:
            self._sync_editor_min_height()
            self._sync_status_max_width()
        return super().eventFilter(obj, event)

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
        self.start_date.setDate(QDate.fromString(rule['period']['start'], 'yyyy-MM-dd'))
        self.start_date.setMaximumDate(
            QDate.fromString(rule['period']['end'], 'yyyy-MM-dd'))
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
        # 起点覆盖规则里的 period.start：预热仍按新起点往前取 WARMUP_DAYS，
        # 因此只改变净值/指标/三图的起算日，不裁剪任何历史信号。
        start_text = self.start_date.date().toString('yyyy-MM-dd')
        if start_text >= rule['period']['end']:
            QMessageBox.warning(
                self, '试算起点无效',
                f'试算起点 {start_text} 必须早于规则终点 {rule["period"]["end"]}。')
            return
        rule['period']['start'] = start_text
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
            rule = dict(self._rule or {})
            period = (self._last_payload or {}).get('period_effective') or {}
            if period:
                merged = dict(rule.get('period') or {})
                merged['start'] = period.get('start') or merged.get('start')
                merged['warmup_start'] = (period.get('warmup_start')
                                         or merged.get('warmup_start'))
                rule['period'] = merged
                rule['effective_period'] = period
            info = save_run(self._last_payload, rule)
        except Exception as exc:  # noqa: BLE001 - 保存失败只提示，不影响已有结果
            logger.exception('策略试算结果保存失败')
            self.status_label.setText(f'保存失败：{exc}')
            return
        self.status_label.setText(f'已保存：{info["run_id"]}')

    def _on_trades(self) -> None:
        """弹出最近一次试算的逐笔成交明细（含按持仓成本算的该笔已实现盈亏）。"""
        if not self._last_payload:
            return
        dialog = _TradeDialog(self._last_payload, self)
        dialog.exec()

    # -- 展示 --------------------------------------------------------------
    def _on_result(self, payload: Dict[str, Any]) -> None:
        if payload.get('status') != 'ok':
            self._last_payload = None
            self.save_btn.setEnabled(False)
            self.trade_btn.setEnabled(False)
            self.status_label.setText(f'空结果：{payload.get("reason", "无可用结果")}')
            self._fill_metrics({'rebalances': 0})
            self._plot_curves([], [])
            self._plot_drawdown([])
            self._plot_markers([])
            return
        self._last_payload = payload
        self.save_btn.setEnabled(True)
        self.trade_btn.setEnabled(True)
        self._fill_metrics(payload['metrics'],
                           (payload.get('dividend_coverage') or {}).get('message') or '')
        self._plot_curves(payload.get('nav') or [], payload.get('benchmark_curve') or [])
        self._plot_drawdown(payload.get('drawdown') or [])
        self._plot_markers(payload.get('markers') or [])
        hint = (payload.get('dividend_coverage') or {}).get('message') or ''
        self._set_status_result(
            f'{payload["rule_text"]}  用时 {payload["elapsed_seconds"]}s '
            f'（引擎 {payload["engine_seconds"]}s，参与标的 {payload["used_codes"]} 只）'
            + (f'  {hint}' if hint else '')
            + self._period_hint_text(payload))

    def _period_hint_text(self, payload: Dict[str, Any]) -> str:
        """起点相关的两条提示：被前移到实际最早日、预热交易日不足。"""
        period = payload.get('period_effective') or {}
        start, requested = period.get('start'), period.get('requested_start')
        parts: List[str] = []
        if start and requested and start > requested:
            parts.append(f'起点 {requested} 早于可用行情首日，已按实际最早日 {start} 起算')
        prior = period.get('prior_trading_days')
        if prior is not None and prior < 250:
            parts.append(f'起点之前仅 {prior} 个交易日，滚动分位等信号可能不完整')
        return ''.join(f'  {text}' for text in parts)

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
        # 竖直滚动条出现时视口会窄掉一条滚动条宽，列宽合计刚好等于视口，最后一条
        # 右侧网格线会落在视口最后一列上被描边吃掉（实测含「分红覆盖」长提示时
        # 合计 496 = 视口 496）。按「合计 + 边框 + 滚动条 + 4px 右侧留白」定最小宽。
        table.setMinimumWidth(columns + 2 * table.frameWidth()
                              + table.verticalScrollBar().sizeHint().width() + 4)

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
        buy_x: List[float] = []
        buy_y: List[float] = []
        sell_x: List[float] = []
        sell_y: List[float] = []
        other_x: List[float] = []
        other_y: List[float] = []
        # 方向判定与点位整理与离线出图共用 markers.layout_markers，不在这里各写一份
        for x, side, y in layout_markers(markers, index):
            if side == 'buy':
                buy_x.append(x)
                buy_y.append(y)
            elif side == 'sell':
                sell_x.append(x)
                sell_y.append(y)
            else:
                # 无买卖方向（例如权重没变化的中性标记），沿用中性圆点
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
