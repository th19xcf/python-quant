#!/usr/bin/env python3

"""
标的搜索对话框
实现类似通达信的代码/名称模糊搜索功能，范围覆盖个股、指数与场内基金
（ETF / LOF），名录只读本地 ``data/local_store/meta``，不访问数据库。
"""

import re

import polars as pl
from loguru import logger
from polars.exceptions import PolarsError
from pypinyin import Style, lazy_pinyin
from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QKeyEvent
from PySide6.QtWidgets import (
    QAbstractItemView,
    QDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from src.data.local_store.paths import resolve_store_root
from src.ui.table_sizing import apply_content_widths, use_interactive_columns

#: 基金 ``fund_basic.fund_type`` → 展示用类型标签；未知类型保留原值，不猜
_FUND_TYPE_LABELS = {'ETF': 'ETF', 'LOF': 'LOF', '分级': '分级基金'}

#: ``sh510300`` / ``SZ000001`` 这类「市场前缀 + 6 位代码」的输入
_PREFIXED_CODE_RE = re.compile(r'(SH|SZ|BJ)(\d{6})')


def _read_meta_frame(meta_dir, file_name):
    """读取本地名录 parquet；缺失或不可读时返回空表并告警。"""
    path = meta_dir / file_name
    if not path.exists():
        logger.warning(f"本地名录不存在: {path}")
        return pl.DataFrame()
    try:
        return pl.read_parquet(path)
    except (OSError, ValueError, PolarsError) as exc:
        logger.warning(f"读取本地名录失败 {path}: {exc}")
        return pl.DataFrame()


def _split_ts_code(ts_code):
    """``600000.SH`` → (``600000``, ``SH``)。"""
    code, _, suffix = (ts_code or '').partition('.')
    return code, suffix.upper()


def _build_record(ts_code, name, type_label, market, category):
    """构造一条检索记录：统一带类型标签、拼音首字母与检索文本。"""
    ts_code = (ts_code or '').strip()
    name = (name or '').strip()
    symbol, suffix = _split_ts_code(ts_code)
    market = (market or suffix).upper()
    pinyin_first_letters = (
        ''.join(p[0].upper() for p in lazy_pinyin(name, style=Style.FIRST_LETTER))
        if name else ''
    )
    # 代码的三种写法（510300.SH / 510300 / SH510300）与名称、拼音一起进检索文本，
    # 从而 510300、510300.SH、sh510300 都能命中
    search_text = ' '.join(filter(None, [
        ts_code.upper(), symbol, f'{market}{symbol}', pinyin_first_letters, name,
    ]))
    return {
        'ts_code': ts_code,
        'symbol': symbol,
        'name': name,
        'type': type_label,
        'pinyin_first_letters': pinyin_first_letters,
        'market': market or '-',
        'category': category or '-',
        'search_text': search_text,
    }


def load_instrument_records(config=None):
    """读取个股 / 指数 / 基金（ETF、LOF、分级）的本地名录并集。

    只读 ``meta/stock_basic.parquet``、``meta/index_basic.parquet``、
    ``meta/fund_basic.parquet``，不连数据库；任一文件缺失时该类别为空并告警。
    """
    meta_dir = resolve_store_root(config) / 'meta'
    records = []

    stocks = _read_meta_frame(meta_dir, 'stock_basic.parquet')
    for row in stocks.iter_rows(named=True):
        records.append(_build_record(
            row.get('ts_code'), row.get('name'), '个股',
            row.get('market'), row.get('board'),
        ))

    indexes = _read_meta_frame(meta_dir, 'index_basic.parquet')
    for row in indexes.iter_rows(named=True):
        records.append(_build_record(
            row.get('ts_code'), row.get('name'), '指数',
            row.get('market'), row.get('category') or row.get('index_type'),
        ))

    funds = _read_meta_frame(meta_dir, 'fund_basic.parquet')
    for row in funds.iter_rows(named=True):
        fund_type = row.get('fund_type') or ''
        type_label = _FUND_TYPE_LABELS.get(fund_type, f'基金({fund_type})' if fund_type else '基金')
        records.append(_build_record(
            row.get('ts_code'), row.get('name'), type_label,
            row.get('market'), fund_type,
        ))

    return records


def normalize_query(text):
    """把 ``sh510300`` 这类输入归一成 ``510300.SH``；其余只去空白转大写。"""
    query = (text or '').strip().upper()
    matched = _PREFIXED_CODE_RE.fullmatch(query)
    if matched:
        return f'{matched.group(2)}.{matched.group(1)}'
    return query


def _match_rank(record, query):
    """返回匹配优先级（0 最优先），不匹配返回 None。"""
    ts_code = record['ts_code'].upper()
    if query == ts_code:
        return 0
    if query == record['symbol']:
        return 1
    if ts_code.startswith(query) or record['symbol'].startswith(query):
        return 2
    if query in record['pinyin_first_letters'] or query in record['name']:
        return 3
    if query in record['search_text']:
        return 4
    return None


def search_instruments(records, text, limit=50):
    """在名录并集中检索，按「完全代码 > 代码前缀 > 名称/拼音 > 包含」排序。"""
    query = normalize_query(text)
    if not query:
        return []

    scored = []
    for record in records:
        rank = _match_rank(record, query)
        if rank is not None:
            scored.append((rank, record['ts_code'], record))
    scored.sort(key=lambda item: (item[0], item[1]))
    return [record for _, _, record in scored[:limit]]


class StockSearchDialog(QDialog):
    """
    标的搜索对话框

    功能：
    1. 支持个股 / 指数 / 基金（ETF、LOF）的代码、名称、拼音首字母模糊搜索
    2. 实时显示搜索结果，并按类型列区分标的类型
    3. 支持键盘上下键选择
    4. 回车键确认选择
    """

    def __init__(self, parent=None, data_manager=None, initial_text=""):
        """
        初始化搜索对话框

        Args:
            parent: 父窗口
            data_manager: 数据管理器，用于解析本地名录根目录
            initial_text: 初始搜索文本
        """
        super().__init__(parent)
        self.data_manager = data_manager
        self.selected_stock = None
        self.all_instruments = []  # 缓存个股 + 指数 + 基金名录
        self.initial_text = initial_text

        self.init_ui()
        self.load_all_instruments()  # 预加载名录并集

        # 如果有初始文本，设置并触发搜索
        if initial_text:
            self.search_edit.setText(initial_text)
            self.perform_search()

    def init_ui(self):
        """初始化UI"""
        self.setWindowTitle("标的搜索")
        self.setFixedSize(500, 400)

        # 设置窗口标志，使其更像一个下拉框
        self.setWindowFlags(
            Qt.Dialog |
            Qt.FramelessWindowHint |
            Qt.WindowStaysOnTopHint
        )

        layout = QVBoxLayout(self)

        # 搜索输入框
        search_layout = QHBoxLayout()
        search_label = QLabel("代码/名称:")
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("输入个股/指数/ETF/LOF 的代码或名称...")
        self.search_edit.textChanged.connect(self.on_search_text_changed)

        search_layout.addWidget(search_label)
        search_layout.addWidget(self.search_edit)
        layout.addLayout(search_layout)

        # 提示标签
        self.tip_label = QLabel("请输入代码、名称或拼音首字母，支持个股/指数/ETF/LOF")
        self.tip_label.setStyleSheet("color: gray; font-size: 12px;")
        layout.addWidget(self.tip_label)

        # 搜索结果表格
        self.result_table = QTableWidget()
        self.result_table.setColumnCount(6)
        self.result_table.setHorizontalHeaderLabels(["代码", "类型", "名称", "拼音首字母", "市场", "分类"])
        use_interactive_columns(self.result_table)
        self.result_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.result_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.result_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.result_table.itemDoubleClicked.connect(self.on_item_double_clicked)

        layout.addWidget(self.result_table)

        # 按钮区域
        btn_layout = QHBoxLayout()

        self.ok_btn = QPushButton("确定")
        self.ok_btn.clicked.connect(self.on_ok_clicked)
        self.ok_btn.setEnabled(False)

        self.cancel_btn = QPushButton("取消")
        self.cancel_btn.clicked.connect(self.reject)

        btn_layout.addStretch()
        btn_layout.addWidget(self.ok_btn)
        btn_layout.addWidget(self.cancel_btn)

        layout.addLayout(btn_layout)

        # 设置焦点到搜索框
        self.search_edit.setFocus()

        # 延迟搜索定时器
        self.search_timer = QTimer(self)
        self.search_timer.setSingleShot(True)
        self.search_timer.timeout.connect(self.perform_search)

    def load_all_instruments(self):
        """预加载本地名录（个股 + 指数 + 基金）到内存"""
        try:
            config = getattr(self.data_manager, 'config', None)
            self.all_instruments = load_instrument_records(config)

            counts = {}
            for record in self.all_instruments:
                counts[record['type']] = counts.get(record['type'], 0) + 1
            logger.info(f"预加载了 {len(self.all_instruments)} 个标的: {counts}")

            if not self.all_instruments:
                logger.warning("本地名录为空，请检查 meta/stock_basic、index_basic、fund_basic")

        except (OSError, RuntimeError, KeyError) as e:
            logger.exception(f"加载标的列表失败: {e}")

    def on_search_text_changed(self, text):
        """搜索文本变化时触发"""
        # 延迟搜索，避免频繁查询
        self.search_timer.stop()
        if text.strip():
            self.search_timer.start(300)  # 300ms后执行搜索
        else:
            self.clear_results()

    def perform_search(self):
        """执行搜索"""
        search_text = self.search_edit.text().strip()
        if not search_text:
            self.clear_results()
            return

        try:
            filtered = search_instruments(self.all_instruments, search_text)
            logger.debug(f"搜索 '{search_text}' 找到 {len(filtered)} 条结果")
            self.update_results(filtered)

        except (OSError, RuntimeError) as e:
            logger.exception(f"搜索标的失败: {e}")

    def update_results(self, instruments):
        """更新搜索结果表格"""
        self.result_table.setRowCount(len(instruments))

        for i, record in enumerate(instruments):
            # 代码
            code_item = QTableWidgetItem(record['ts_code'])
            code_item.setData(Qt.UserRole, record)  # 存储完整数据
            self.result_table.setItem(i, 0, code_item)

            # 类型（个股 / 指数 / ETF / LOF）
            self.result_table.setItem(i, 1, QTableWidgetItem(record['type']))

            # 名称
            self.result_table.setItem(i, 2, QTableWidgetItem(record['name']))

            # 拼音首字母
            self.result_table.setItem(i, 3, QTableWidgetItem(record['pinyin_first_letters']))

            # 市场
            self.result_table.setItem(i, 4, QTableWidgetItem(record['market']))

            # 分类（个股板块 / 指数类别 / 基金类型）
            self.result_table.setItem(i, 5, QTableWidgetItem(record['category']))

        # 列宽按当前结果实测，与其它页面共用 table_sizing 的统一口径
        apply_content_widths(self.result_table)

        # 更新提示
        if instruments:
            self.tip_label.setText(f"找到 {len(instruments)} 条结果 (显示前50条)")
            self.tip_label.setStyleSheet("color: green; font-size: 12px;")
            self.ok_btn.setEnabled(True)
            # 默认选中第一行
            self.result_table.selectRow(0)
        else:
            self.tip_label.setText("未找到匹配的标的")
            self.tip_label.setStyleSheet("color: red; font-size: 12px;")
            self.ok_btn.setEnabled(False)

    def clear_results(self):
        """清空搜索结果"""
        # 手动释放QTableWidgetItem对象
        for row in range(self.result_table.rowCount()):
            for col in range(self.result_table.columnCount()):
                item = self.result_table.takeItem(row, col)
                if item:
                    del item
        self.result_table.setRowCount(0)
        self.tip_label.setText("请输入代码、名称或拼音首字母，支持个股/指数/ETF/LOF")
        self.tip_label.setStyleSheet("color: gray; font-size: 12px;")
        self.ok_btn.setEnabled(False)

    def on_item_double_clicked(self, item):
        """双击选中"""
        self.on_ok_clicked()

    def on_ok_clicked(self):
        """确定按钮点击"""
        current_row = self.result_table.currentRow()
        if current_row >= 0:
            item = self.result_table.item(current_row, 0)
            if item:
                self.selected_stock = item.data(Qt.UserRole)
                self.accept()

    def keyPressEvent(self, event: QKeyEvent):
        """键盘事件处理"""
        if event.key() == Qt.Key.Key_Up:
            # 向上选择
            current_row = self.result_table.currentRow()
            if current_row > 0:
                self.result_table.selectRow(current_row - 1)
        elif event.key() == Qt.Key.Key_Down:
            # 向下选择
            current_row = self.result_table.currentRow()
            if current_row < self.result_table.rowCount() - 1:
                self.result_table.selectRow(current_row + 1)
        elif event.key() == Qt.Key.Key_Return or event.key() == Qt.Key.Key_Enter:
            # 回车确认
            self.on_ok_clicked()
        elif event.key() == Qt.Key.Key_Escape:
            # ESC取消
            self.reject()
        else:
            super().keyPressEvent(event)

    def get_selected_stock(self):
        """
        获取选中的股票

        Returns:
            dict: 选中的股票信息，如果没有选中则返回None
        """
        return self.selected_stock

    def move_to_position(self, x, y):
        """
        移动对话框到指定位置

        Args:
            x: x坐标
            y: y坐标
        """
        self.move(int(x), int(y))

    def showEvent(self, event):
        """
        对话框显示事件
        在显示后设置光标位置，避免文本被选中
        """
        super().showEvent(event)
        # 延迟设置光标位置，确保对话框已完全显示
        from PySide6.QtCore import QTimer
        QTimer.singleShot(10, self._set_cursor_position)

    def _set_cursor_position(self):
        """设置光标到文本末尾"""
        text = self.search_edit.text()
        if text:
            self.search_edit.setCursorPosition(len(text))
