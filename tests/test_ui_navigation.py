"""导航 Mixin 与表格交互管理器测试（离屏，无网络/数据库）。"""

from types import SimpleNamespace

import pytest
from PySide6.QtCore import QEvent, Qt
from PySide6.QtGui import QColor, QKeyEvent
from PySide6.QtWidgets import QTableWidget, QTableWidgetItem

from src.ui.main_window_navigation_mixin import MainWindowNavigationMixin
from src.ui.table_interaction_manager import TableInteractionManager

COLUMNS = 13


class _Recorder:
    def __init__(self):
        self.calls = []

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))

    def __bool__(self):
        return bool(self.calls)


class _Dispatcher:
    """按方法名记录调用。"""

    def __init__(self):
        self.names = []
        self.args = []

    def __getattr__(self, name):
        def _method(*args, **kwargs):
            self.names.append(name)
            self.args.append(args)

        return _method


class _FakeTabs:
    def __init__(self):
        self.current = None
        self.index = None

    def currentWidget(self):
        return self.current

    def setCurrentWidget(self, widget):
        self.current = widget

    def setCurrentIndex(self, index):
        self.index = index


class _Cell:
    def __init__(self, text):
        self._text = text

    def text(self, column=0):
        return self._text


class _FakeTable:
    def __init__(self, rows):
        self.rows = rows

    def rowCount(self):
        return len(self.rows)

    def item(self, row, column):
        if row >= len(self.rows):
            return None
        code, name = self.rows[row]
        if column == 1:
            return _Cell(code)
        if column == 2:
            return _Cell(name)
        return None


def _make_host(frame=None):
    host = MainWindowNavigationMixin()
    host.status_messages = []
    host.statusBar = lambda: SimpleNamespace(
        showMessage=lambda msg, timeout=0: host.status_messages.append((msg, timeout))
    )
    host.tab_widget = _FakeTabs()
    host.tech_tab = 'tech'
    host.market_tab = 'market'
    host.current = frame
    return host


def _key_event(key, text=''):
    return QKeyEvent(QEvent.Type.KeyPress, key, Qt.KeyboardModifier.NoModifier, text)


def _make_table(row_count=3, values=None):
    table = QTableWidget(row_count, COLUMNS)
    for row in range(row_count):
        for col in range(COLUMNS):
            text = str(row) if values is None else values[row][col]
            table.setItem(row, col, QTableWidgetItem(text))
    return table


def _make_manager(table):
    window = SimpleNamespace(
        stock_table=table,
        column_sort_states={},
        current_sorted_column=-1,
    )
    return TableInteractionManager(window), window


# ---------------------------------------------------------------------------
# 导航 Mixin：点击与导航联动
# ---------------------------------------------------------------------------


def test_on_stock_clicked_shows_status_message():
    host = _make_host()
    host.stock_table = _FakeTable([('600519', '贵州茅台')])

    host.on_stock_clicked(SimpleNamespace(row=lambda: 0))

    assert host.status_messages == [('Selected: 贵州茅台(600519)', 0)]


def test_on_stock_clicked_without_items_does_nothing():
    host = _make_host()
    host.stock_table = _FakeTable([])

    host.on_stock_clicked(SimpleNamespace(row=lambda: 0))

    assert host.status_messages == []


def test_on_stock_double_clicked_switches_tab_and_processes():
    host = _make_host()
    host.stock_table = _FakeTable([('600519', '贵州茅台')])
    processed = _Recorder()
    host.process_stock_data = processed

    host.on_stock_double_clicked(SimpleNamespace(row=lambda: 0))

    assert host.tab_widget.index == 1
    assert processed.calls == [(('600519', '贵州茅台'), {})]


def test_on_stock_double_clicked_without_process_method():
    host = _make_host()
    host.stock_table = _FakeTable([('600519', '贵州茅台')])

    host.on_stock_double_clicked(SimpleNamespace(row=lambda: 0))

    assert host.tab_widget.index == 1


def test_on_stock_double_clicked_without_items_keeps_tab():
    host = _make_host()
    host.stock_table = _FakeTable([])

    host.on_stock_double_clicked(SimpleNamespace(row=lambda: 0))

    assert host.tab_widget.index is None


def test_on_nav_item_clicked_delegates_to_impl():
    host = _make_host()
    recorder = _Recorder()
    host._on_nav_item_clicked_impl = recorder
    item = _Cell('沪深京指数')

    host.on_nav_item_clicked(item, 0)

    assert recorder.calls[0][0] == (item, 0)


@pytest.mark.parametrize(('text', 'method', 'expected_args'), [
    ('沪深京指数', 'show_index_overview', ()),
    ('沪市指数', 'show_sh_index_overview', ()),
    ('深市指数', 'show_sz_index_overview', ()),
    ('京市指数', 'show_bj_index_overview', ()),
    ('创业板指', 'show_index_data_by_category', ('创业板',)),
    ('科创板指', 'show_index_data_by_category', ('科创板',)),
    ('沪深京个股', 'show_stock_data_by_type', ('全部A股',)),
    ('沪深京A股', 'show_stock_data_by_type', ('全部A股',)),
    ('全部A股', 'show_stock_data_by_type', ('全部A股',)),
    ('上证A股', 'show_stock_data_by_type', ('上证A股',)),
    ('深证A股', 'show_stock_data_by_type', ('深证A股',)),
    ('京市个股', 'show_stock_data_by_type', ('京市个股',)),
    ('创业板', 'show_stock_data_by_type', ('创业板',)),
    ('科创板', 'show_stock_data_by_type', ('科创板',)),
])
def test_nav_item_click_dispatches_to_data_view(text, method, expected_args):
    host = _make_host()
    dispatcher = _Dispatcher()
    host.data_view_manager = dispatcher

    host._on_nav_item_clicked_impl(_Cell(text), 0)

    assert dispatcher.names == [method]
    assert dispatcher.args == [expected_args]


@pytest.mark.parametrize(('text', 'method'), [
    ('开放式基金', 'show_etf_funds'),
    ('封闭式基金', 'show_closed_funds'),
])
def test_nav_item_click_dispatches_to_fund_section(text, method):
    host = _make_host()
    dispatcher = _Dispatcher()
    host.fund_section = dispatcher

    host._on_nav_item_clicked_impl(_Cell(text), 0)

    assert dispatcher.names == [method]


def test_nav_item_click_resets_sort_state_on_view_change():
    host = _make_host()
    host.current_market_view = '沪市指数'
    reset = _Recorder()
    host.table_interaction_manager = SimpleNamespace(reset_sort_state=reset)
    host.data_view_manager = _Dispatcher()

    host._on_nav_item_clicked_impl(_Cell('沪深京指数'), 0)

    assert reset.calls
    assert host.current_market_view == '沪深京指数'
    assert host.tab_widget.index == 0


def test_nav_item_click_same_view_keeps_sort_state():
    host = _make_host()
    host.current_market_view = '沪深京指数'
    reset = _Recorder()
    host.table_interaction_manager = SimpleNamespace(reset_sort_state=reset)

    host._on_nav_item_clicked_impl(_Cell('沪深京指数'), 0)

    assert not reset.calls
    assert host.tab_widget.index == 0


def test_nav_item_click_unknown_text_does_nothing():
    host = _make_host()
    host.data_view_manager = _Dispatcher()
    host.fund_section = _Dispatcher()

    host._on_nav_item_clicked_impl(_Cell('未知节点'), 0)

    assert host.data_view_manager.names == []
    assert host.fund_section.names == []
    assert host.tab_widget.index == 0


def test_nav_item_click_without_managers_is_safe():
    host = _make_host()

    host._on_nav_item_clicked_impl(_Cell('沪深京指数'), 0)

    assert host.tab_widget.index == 0


# ---------------------------------------------------------------------------
# 导航 Mixin：键盘
# ---------------------------------------------------------------------------


def test_escape_returns_to_market_tab():
    host = _make_host()
    host.tab_widget.current = 'tech'

    host.keyPressEvent(_key_event(Qt.Key_Escape))

    assert host.tab_widget.current == 'market'


def test_escape_on_market_tab_does_nothing():
    host = _make_host()
    host.tab_widget.current = 'market'

    host.keyPressEvent(_key_event(Qt.Key_Escape))

    assert host.tab_widget.current == 'market'


@pytest.mark.parametrize(('key', 'direction'), [
    (Qt.Key_Up, 'prev'),
    (Qt.Key_Down, 'next'),
])
def test_arrow_keys_switch_adjacent_stock(key, direction):
    host = _make_host()
    host.tab_widget.current = 'tech'
    switch = _Recorder()
    host._switch_to_adjacent_stock = lambda value: switch(value)

    host.keyPressEvent(_key_event(key))

    assert switch.calls == [((direction,), {})]


def test_arrow_key_outside_tech_tab_does_not_switch():
    host = _make_host()
    host.tab_widget.current = 'market'
    switch = _Recorder()
    host._switch_to_adjacent_stock = switch

    host.keyPressEvent(_key_event(Qt.Key_Up))

    assert not switch.calls


@pytest.mark.parametrize(('key', 'handler'), [
    (Qt.Key_Left, '_on_zoom_in'),
    (Qt.Key_Right, '_on_zoom_out'),
])
def test_left_right_keys_zoom_in_tech_tab(key, handler):
    host = _make_host()
    host.tab_widget.current = 'tech'
    recorder = _Recorder()
    setattr(host, handler, recorder)

    host.keyPressEvent(_key_event(key))

    assert recorder.calls == [((), {})]


def test_left_right_keys_outside_tech_tab_do_nothing():
    host = _make_host()
    host.tab_widget.current = 'market'
    zoom_in = _Recorder()
    host._on_zoom_in = zoom_in

    host.keyPressEvent(_key_event(Qt.Key_Left))

    assert not zoom_in.calls


def test_functional_key_is_ignored():
    host = _make_host()
    host.tab_widget.current = 'tech'
    search = _Recorder()
    host._show_global_search_dialog = search

    host.keyPressEvent(_key_event(Qt.Key_F5))

    assert not search.calls


def test_printable_key_opens_search_dialog():
    host = _make_host()
    host.tab_widget.current = 'market'
    search = _Recorder()
    host._show_global_search_dialog = search

    host.keyPressEvent(_key_event(Qt.Key_A, 'a'))

    assert search.calls == [(('a',), {})]


def test_non_printable_key_opens_no_dialog():
    host = _make_host()
    host.tab_widget.current = 'market'
    search = _Recorder()
    host._show_global_search_dialog = search

    host.keyPressEvent(_key_event(Qt.Key_Control, ''))

    assert not search.calls


# ---------------------------------------------------------------------------
# 导航 Mixin：代码标准化与相邻切换
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(('code', 'expected'), [
    ('sh000016', '000016.SH'),
    ('sz399001', '399001.SZ'),
    ('bj899001', '899001.BJ'),
    ('600519', '600519.SH'),
    ('830000', '830000.BJ'),
    ('430047', '430047.BJ'),
    ('920001', '920001.BJ'),
    ('000001', '000001.SZ'),
    ('600519.SH', '600519.SH'),
])
def test_normalize_adjacent_code(code, expected):
    host = _make_host()

    assert host._normalize_adjacent_code(code) == expected


def test_locate_adjacent_code_exact_and_fuzzy():
    host = _make_host()
    items = ['000001.SZ', '600519.SH']

    assert host._locate_adjacent_code(items, '600519.SH', '股票') == 1
    assert host._locate_adjacent_code(items, '600519', '股票') == 1
    assert host._locate_adjacent_code(items, '000002.SZ', '股票') is None


@pytest.mark.parametrize(('current', 'length', 'direction', 'expected'), [
    (0, 3, 'prev', 2),
    (2, 3, 'next', 0),
    (1, 3, 'prev', 0),
    (1, 3, 'next', 2),
])
def test_next_list_index_wraps(current, length, direction, expected):
    host = _make_host()

    assert host._next_list_index(current, length, direction) == expected


def test_switch_adjacent_stock_without_current_code():
    host = _make_host()
    action = SimpleNamespace(_load_stock_chart=_Recorder())
    host.action_manager = action

    host._switch_to_adjacent_stock('next')

    assert not action._load_stock_chart.calls


def test_switch_adjacent_stock_loads_next_stock():
    host = _make_host()
    host.current_stock_code = '600519.SH'
    host.stock_table = _FakeTable([('600519', '贵州茅台'), ('000001', '平安银行')])
    action = SimpleNamespace(_load_stock_chart=_Recorder())
    host.action_manager = action

    host._switch_to_adjacent_stock('next')

    assert action._load_stock_chart.calls == [(('000001.SZ', '平安银行'), {})]


def test_switch_adjacent_stock_wraps_to_last():
    host = _make_host()
    host.current_stock_code = '600519.SH'
    host.stock_table = _FakeTable([('600519', '贵州茅台'), ('000001', '平安银行')])
    action = SimpleNamespace(_load_stock_chart=_Recorder())
    host.action_manager = action

    host._switch_to_adjacent_stock('prev')

    assert action._load_stock_chart.calls == [(('000001.SZ', '平安银行'), {})]


def test_switch_adjacent_stock_index_uses_index_list():
    host = _make_host()
    host.current_stock_code = '000001.SH'
    host.stock_table = _FakeTable([('sh000001', '上证指数'), ('sz399001', '深证成指')])
    action = SimpleNamespace(_load_stock_chart=_Recorder())
    host.action_manager = action

    host._switch_to_adjacent_stock('next')

    assert action._load_stock_chart.calls == [(('399001.SZ', '深证成指'), {})]


def test_switch_adjacent_stock_without_action_manager():
    host = _make_host()
    host.current_stock_code = '600519.SH'
    host.stock_table = _FakeTable([('600519', '贵州茅台'), ('000001', '平安银行')])

    host._switch_to_adjacent_stock('next')


def test_switch_adjacent_stock_empty_list():
    host = _make_host()
    host.current_stock_code = '600519.SH'
    host.stock_table = _FakeTable([])
    action = SimpleNamespace(_load_stock_chart=_Recorder())
    host.action_manager = action

    host._switch_to_adjacent_stock('next')

    assert not action._load_stock_chart.calls


@pytest.mark.parametrize('handler', ['_on_prev_stock_clicked', '_on_next_stock_clicked'])
def test_prev_next_buttons_dispatch(handler):
    host = _make_host()
    switch = _Recorder()
    host._switch_to_adjacent_stock = switch

    getattr(host, handler)()

    expected = 'prev' if handler == '_on_prev_stock_clicked' else 'next'
    assert switch.calls == [((expected,), {})]


# ---------------------------------------------------------------------------
# 导航 Mixin：列表与名称查询
# ---------------------------------------------------------------------------


def test_get_current_stock_list_converts_codes():
    host = _make_host()
    host.stock_table = _FakeTable([
        ('sh000001', ''), ('600519', ''), ('830000', ''),
        ('159915', ''), ('510300', ''), ('000001.SZ', ''),
        ('sz399001', ''),
    ])

    result = host._get_current_stock_list()

    assert result == [
        '000001.SH', '600519.SH', '830000.BJ',
        '159915.SZ', '510300.SH', '000001.SZ', '399001.SZ',
    ]


def test_get_current_stock_list_falls_back_to_data_manager():
    host = _make_host()
    host.stock_table = _FakeTable([])
    host.data_manager = SimpleNamespace(list_stock_ts_codes=lambda: ['600519.SH'])

    assert host._get_current_stock_list() == ['600519.SH']


def test_get_current_stock_list_without_sources_is_empty():
    host = _make_host()
    host.stock_table = _FakeTable([])

    assert host._get_current_stock_list() == []


@pytest.mark.parametrize(('ts_code', 'expected'), [
    ('600519.SH', '贵州茅台'),
    ('000001.SZ', '平安银行'),
])
def test_get_stock_name_from_table(ts_code, expected):
    host = _make_host()
    host.stock_table = _FakeTable([
        ('600519', '贵州茅台'), ('sz000001', '平安银行'), ('sz600519', '重复'),
    ])

    assert host._get_stock_name(ts_code) == expected


def test_get_stock_name_falls_back_to_data_manager_and_code():
    host = _make_host()
    host.stock_table = _FakeTable([])
    host.data_manager = SimpleNamespace(get_stock_name=lambda code: '名称')
    assert host._get_stock_name('600519.SH') == '名称'

    host.data_manager = SimpleNamespace(get_stock_name=lambda code: None)
    assert host._get_stock_name('999999.SZ') == '999999.SZ'


def test_get_index_name_from_table_and_fallback():
    host = _make_host()
    host.stock_table = _FakeTable([('sh000001', '上证指数')])

    assert host._get_index_name('000001.SH') == '上证指数'
    assert host._get_index_name('000300.SH') == '000300.SH'


def test_get_index_name_without_table():
    host = _make_host()

    assert host._get_index_name('000001.SH') == '000001.SH'


@pytest.mark.parametrize(('code', 'expected'), [
    ('000001.SH', True),
    ('399001.SZ', True),
    ('899001.BJ', True),
    ('600519.SH', False),
    ('sh000001', True),
    ('sz399001', True),
    ('bj899001', True),
    ('sh600519', False),
    ('600519', False),
])
def test_is_index_code(code, expected):
    host = _make_host()

    assert host._is_index_code(code) is expected


def test_get_current_index_list_filters_codes():
    host = _make_host()
    host.stock_table = _FakeTable([
        ('sh000001', ''), ('sz399001', ''), ('bj899001', ''),
        ('000300', ''), ('399005', ''), ('899050', ''), ('600519', ''),
    ])

    result = host._get_current_index_list()

    assert result == [
        '000001.SH', '399001.SZ', '899001.BJ',
        '000300.SH', '399005.SZ', '899050.BJ',
    ]


def test_get_current_index_list_empty_table():
    host = _make_host()
    host.stock_table = _FakeTable([('600519', '')])

    assert host._get_current_index_list() == []


@pytest.mark.parametrize(('ts_code', 'market', 'tdx_code'), [
    ('600519.SH', 'sh', 'sh600519'),
    ('000001.SZ', 'sz', 'sz000001'),
    ('830000.BJ', 'bj', 'bj830000'),
    ('600519', 'sh', 'sh600519'),
    ('830000', 'bj', 'bj830000'),
    ('000001', 'sz', 'sz000001'),
])
def test_check_stock_data_exists(tmp_path, ts_code, market, tdx_code):
    host = _make_host()
    host.data_manager = SimpleNamespace(
        config=SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))
    )
    lday = tmp_path / market / 'lday'
    lday.mkdir(parents=True)
    (lday / f'{tdx_code}.day').write_bytes(b'')

    assert host._check_stock_data_exists(ts_code) is True


def test_check_stock_data_exists_missing_and_no_manager(tmp_path):
    host = _make_host()
    host.data_manager = SimpleNamespace(
        config=SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))
    )
    assert host._check_stock_data_exists('600519.SH') is False

    host = _make_host()
    assert host._check_stock_data_exists('600519.SH') is False


@pytest.mark.parametrize(('ts_code', 'market', 'tdx_code'), [
    ('000001.SH', 'sh', 'sh000001'),
    ('399001.SZ', 'sz', 'sz399001'),
    ('899001.BJ', 'bj', 'bj899001'),
    ('000001', 'sh', 'sh000001'),
    ('899001', 'bj', 'bj899001'),
    ('399001', 'sz', 'sz399001'),
])
def test_check_index_data_exists(tmp_path, ts_code, market, tdx_code):
    host = _make_host()
    host.data_manager = SimpleNamespace(
        config=SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))
    )
    lday = tmp_path / market / 'lday'
    lday.mkdir(parents=True)
    (lday / f'{tdx_code}.day').write_bytes(b'')

    assert host._check_index_data_exists(ts_code) is True


def test_check_index_data_exists_missing_and_no_manager(tmp_path):
    host = _make_host()
    host.data_manager = SimpleNamespace(
        config=SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))
    )
    assert host._check_index_data_exists('000001.SH') is False

    host = _make_host()
    assert host._check_index_data_exists('000001.SH') is False


# ---------------------------------------------------------------------------
# TableInteractionManager：排序
# ---------------------------------------------------------------------------


def test_header_click_cycles_sort_states(qapp):
    table = _make_table()
    manager, window = _make_manager(table)

    manager.on_header_clicked(0)
    assert table.isSortingEnabled() is True
    assert table.horizontalHeader().sortIndicatorOrder() == Qt.SortOrder.AscendingOrder
    assert window.current_sorted_column == 0

    manager.on_header_clicked(0)
    assert table.horizontalHeader().sortIndicatorOrder() == Qt.SortOrder.DescendingOrder
    assert window.current_sorted_column == 0

    manager.on_header_clicked(0)
    assert table.isSortingEnabled() is False
    assert window.current_sorted_column == -1
    assert table.rowCount() == 3


def test_reset_sort_and_restore_rows_keeps_colors(qapp):
    table = _make_table(values=[['2024', 'sh600519', '贵州茅台', '+3.5', '10.0', '+2.0',
                                 '9.0', '3.0', '10.0', '10.0', '10.0', '9.0', '0'],
                                 ['2024', 'sz000001', '平安银行', '-2.0', '8.0', '-1.0',
                                  '9.0', '1.0', '8.0', '8.0', '8.0', '10.0', '0'],
                                 ['2024', 'sz399001', '深证成指', '0.00', '9.0', '0.00',
                                  '9.0', '0.0', '9.0', '9.0', '9.0', '9.0', '0']])
    manager, window = _make_manager(table)

    manager._reset_sort_and_restore_rows(window)

    assert table.isSortingEnabled() is False
    assert table.rowCount() == 3
    assert window.current_sorted_column == -1
    assert table.item(0, 3).foreground().color() == QColor(255, 0, 0)
    assert table.item(1, 3).foreground().color() == QColor(0, 255, 0)
    assert table.item(2, 3).foreground().color() == QColor(204, 204, 204)


def test_reset_sort_state_clears_state(qapp):
    table = _make_table()
    table.setSortingEnabled(True)
    window = SimpleNamespace(
        stock_table=table,
        column_sort_states={0: 1},
        current_sorted_column=0,
    )
    manager = TableInteractionManager(window)

    manager.reset_sort_state()

    assert table.isSortingEnabled() is False
    assert window.column_sort_states == {}
    assert window.current_sorted_column == -1
    assert table.horizontalHeader().isSortIndicatorShown() is False


@pytest.mark.parametrize(('column', 'states', 'sorting'), [
    (-1, {}, False),
    (0, {0: 1}, True),
    (0, {0: 2}, True),
    (0, {0: 0}, False),
    (5, {}, False),
])
def test_restore_sort_state(qapp, column, states, sorting):
    table = _make_table()
    window = SimpleNamespace(
        stock_table=table,
        column_sort_states=states,
        current_sorted_column=column,
    )
    manager = TableInteractionManager(window)

    manager.restore_sort_state()

    assert table.isSortingEnabled() is sorting
    if sorting:
        expected = (
            Qt.SortOrder.AscendingOrder if states[column] == 1
            else Qt.SortOrder.DescendingOrder
        )
        assert table.horizontalHeader().sortIndicatorSection() == column
        assert table.horizontalHeader().sortIndicatorOrder() == expected


# ---------------------------------------------------------------------------
# TableInteractionManager：单元格配色与代码解析
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(('column', 'value', 'preclose', 'expected'), [
    (3, '+3.5', '10.0', QColor(255, 0, 0)),
    (3, '-2.0', '10.0', QColor(0, 255, 0)),
    (3, '0.00', '10.0', QColor(204, 204, 204)),
    (5, '+1.2', '10.0', QColor(255, 0, 0)),
    (4, '10.0', '9.0', QColor(255, 0, 0)),
    (4, '8.0', '9.0', QColor(0, 255, 0)),
    (8, '9.0', '9.0', QColor(204, 204, 204)),
])
def test_apply_restored_cell_style(column, value, preclose, expected):
    row_data = [''] * COLUMNS
    row_data[11] = preclose
    item = QTableWidgetItem(value)

    TableInteractionManager._apply_restored_cell_style(item, row_data, column, value)

    assert item.foreground().color() == expected


def test_apply_restored_cell_style_short_row_uses_zero():
    item = QTableWidgetItem('-1.0')
    row_data = [''] * COLUMNS
    row_data[11] = '-'

    TableInteractionManager._apply_restored_cell_style(item, row_data, 4, '-1.0')

    assert item.foreground().color() == QColor(0, 255, 0)


def test_apply_restored_cell_style_aligns_numeric_columns():
    item = QTableWidgetItem('1.0')

    TableInteractionManager._apply_restored_cell_style(item, [''] * COLUMNS, 7, '1.0')

    assert item.textAlignment() == int(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)


@pytest.mark.parametrize(('code', 'market', 'tdx_code'), [
    ('sh000001', 'sh', 'sh000001'),
    ('sz399001', 'sz', 'sz399001'),
    ('bj899001', 'bj', 'bj899001'),
    ('600519', 'sh', 'sh600519'),
    ('510300', 'sh', 'sh510300'),
    ('159915', 'sz', 'sz159915'),
    ('000001.SH', 'sh', 'sh000001'),
    ('000001.SZ', 'sz', 'sz000001'),
    ('999999', 'sh', 'sh000001'),
    ('880001', 'sh', 'sh880001'),
    ('920001', 'bj', 'bj920001'),
    ('600000', 'sh', 'sh600000'),
])
def test_resolve_tdx_code(code, market, tdx_code):
    assert TableInteractionManager._resolve_tdx_code(code) == (market, tdx_code)


def test_double_click_without_items_does_not_start_thread(qapp):
    table = _make_table(row_count=1)
    table.setRowCount(0)
    manager, window = _make_manager(table)

    manager.on_stock_double_clicked(0, 0)

    assert not hasattr(window, 'data_read_thread')


# ---------------------------------------------------------------------------
# TableInteractionManager：进度与结果回调
# ---------------------------------------------------------------------------


class _ProgressBar:
    def __init__(self):
        self.values = []
        self.visible = []

    def setValue(self, value):
        self.values.append(value)

    def setVisible(self, visible):
        self.visible.append(visible)


def test_on_data_read_progress_updates_bar():
    bar = _ProgressBar()
    manager, window = _make_manager(_make_table())
    window.market_progress_bar = bar

    manager.on_data_read_progress(40, 100)

    assert bar.values == [20]


def test_on_data_read_progress_without_bar():
    manager, window = _make_manager(_make_table())

    manager.on_data_read_progress(40, 100)

    assert not hasattr(window, 'market_progress_bar')


def test_on_indicator_calculated_plots_chart():
    bar = _ProgressBar()
    manager, window = _make_manager(_make_table())
    window.market_progress_bar = bar
    plotted = _Recorder()
    window.plot_k_line = plotted
    messages = []
    window.statusBar = lambda: SimpleNamespace(
        showMessage=lambda msg, timeout=0: messages.append(msg)
    )

    manager.on_indicator_calculated('df', '贵州茅台', '600519.SH')

    assert plotted.calls == [(('df', '贵州茅台', '600519.SH'), {})]
    assert bar.values == [90, 100]
    assert bar.visible == [False]
    assert messages == [
        '正在绘制 贵州茅台(600519.SH) 图表...',
        '贵州茅台(600519.SH) 图表绘制完成',
    ]


def test_on_indicator_calculated_without_bar():
    manager, window = _make_manager(_make_table())
    window.plot_k_line = _Recorder()
    messages = []
    window.statusBar = lambda: SimpleNamespace(
        showMessage=lambda msg, timeout=0: messages.append(msg)
    )

    manager.on_indicator_calculated('df', '贵州茅台', '600519.SH')

    assert messages == [
        '正在绘制 贵州茅台(600519.SH) 图表...',
        '贵州茅台(600519.SH) 图表绘制完成',
    ]


def test_on_data_read_error_shows_message(qapp):
    bar = _ProgressBar()
    manager, window = _make_manager(_make_table())
    window.market_progress_bar = bar
    messages = []
    window.statusBar = lambda: SimpleNamespace(
        showMessage=lambda msg, timeout=0: messages.append((msg, timeout))
    )

    manager.on_data_read_error('读取失败')

    assert messages == [('读取失败', 5000)]
    assert bar.visible == [False]


def test_on_indicator_calculate_error_shows_message(qapp):
    manager, window = _make_manager(_make_table())
    messages = []
    window.statusBar = lambda: SimpleNamespace(
        showMessage=lambda msg, timeout=0: messages.append((msg, timeout))
    )

    manager.on_indicator_calculate_error('计算失败')

    assert messages == [('计算失败', 5000)]


def test_on_indicator_calculate_progress(qapp):
    manager, window = _make_manager(_make_table())

    manager.on_indicator_calculate_progress(50, 100)


def test_on_data_read_completed_empty_frame():
    manager, window = _make_manager(_make_table())
    messages = []
    window.statusBar = lambda: SimpleNamespace(
        showMessage=lambda msg, timeout=0: messages.append((msg, timeout))
    )
    bar = _ProgressBar()
    window.market_progress_bar = bar

    manager.on_data_read_completed(SimpleNamespace(is_empty=lambda: True), '贵州茅台', '600519.SH')

    assert messages == [('贵州茅台(600519.SH) 没有数据', 5000)]
    assert bar.visible == [False]


def test_on_data_read_completed_empty_frame_without_bar():
    manager, window = _make_manager(_make_table())
    messages = []
    window.statusBar = lambda: SimpleNamespace(
        showMessage=lambda msg, timeout=0: messages.append((msg, timeout))
    )

    manager.on_data_read_completed(SimpleNamespace(is_empty=lambda: True), '贵州茅台', '600519.SH')

    assert messages == [('贵州茅台(600519.SH) 没有数据', 5000)]
    assert not hasattr(window, 'market_progress_bar')
