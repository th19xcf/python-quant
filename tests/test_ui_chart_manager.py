"""ChartManager 单元测试：数据准备、坐标轴、标签渲染与指标绘制（离屏）。"""

from types import SimpleNamespace

import numpy as np
import pandas as pd
import polars as pl
import pyqtgraph as pg
import pytest
from PySide6.QtCore import QPointF, QTimer
from PySide6.QtWidgets import QLabel, QVBoxLayout, QWidget

from src.ui.chart_components.chart_event_binder import ChartEventBinder
from src.ui.chart_components.chart_ui_builder import ChartUIBuilder
from src.ui.chart_manager import ChartManager

N = 6

ALL_COLUMNS = [
    'date', 'open', 'high', 'low', 'close', 'volume',
    'k', 'd', 'j', 'rsi14', 'macd', 'macd_signal', 'macd_hist',
    'wr1', 'wr2', 'mb', 'up', 'dn', 'pdi', 'ndi', 'adx', 'adxr',
    'trix', 'trma', 'br', 'ar', 'dma', 'ama', 'expma12', 'expma50',
    'bbi', 'hsl', 'hsl_ma5', 'hsl_ma10', 'lb', 'cyc5', 'cyc13',
    'cyc34', 'cyc_inf', 'cys', 'cys_ma5', 'vol_ma5', 'vol_ma10',
    'ma5', 'ma10', 'ma20', 'ma60', 'rsi',
]


def _df(n=N):
    from datetime import date, timedelta

    data = {'date': [date(2024, 1, 1) + timedelta(days=i) for i in range(n)]}
    for i, col in enumerate(ALL_COLUMNS[1:]):
        data[col] = [float(i + 1) + j * 0.1 for j in range(n)]
    return pl.DataFrame(data)


def _pandas_df(n=N):
    return _df(n).to_pandas()


def _make_manager():
    window = SimpleNamespace()
    return ChartManager(window), window


@pytest.fixture
def plot(qapp):
    widget = pg.PlotWidget()
    yield widget
    widget.close()


def _plot_items(widget):
    return widget.plotItem.items


def test_init():
    manager, window = _make_manager()

    assert manager.window is window


def test_empty_refresh_methods_removed():
    """空 pass 的刷新方法已删除，避免留下"调用后无任何效果"的假入口。"""
    assert not hasattr(ChartManager, 'refresh_kline_chart')
    assert not hasattr(ChartManager, 'refresh_indicator_charts')


def test_setup_chart_labels_builds_title_bar(qapp):
    manager, window = _make_manager()
    layout = QVBoxLayout()
    holder = QWidget()
    holder.setLayout(layout)
    window.chart_layout = layout
    window.chart_container = holder
    window.current_period = '日线'
    window.current_selected_window = 1
    window.create_indicator_menu_button = lambda idx: QWidget()

    manager._setup_chart_labels('贵州茅台', '600519.SH')

    assert window.chart_title_label.text() == '贵州茅台(600519.SH) 日线'
    assert 'MA5: --' in window.ma_values_label.text()
    assert window.menu_btn is not None
    assert layout.count() >= 1
    assert holder.findChildren(QWidget)


def test_setup_chart_labels_replaces_previous_labels(qapp):
    manager, window = _make_manager()
    layout = QVBoxLayout()
    holder = QWidget()
    holder.setLayout(layout)
    window.chart_layout = layout
    window.chart_container = holder
    window.current_selected_window = 1
    window.chart_title_label = QLabel('old')
    window.ma_values_label = QLabel('old')
    window.create_indicator_menu_button = lambda idx: QWidget()

    manager._setup_chart_labels('平安银行', '000001.SZ')

    assert '平安银行(000001.SZ)' in window.chart_title_label.text()
    assert layout.count() == 1


def test_prepare_chart_data_truncates_to_bar_count():
    manager, window = _make_manager()
    window.displayed_bar_count = 3
    dates, opens, highs, lows, closes, x, ohlc = manager._prepare_chart_data(_df())

    assert len(dates) == 3
    assert np.array_equal(x, np.arange(3))
    assert ohlc[0][0] == 0
    assert len(ohlc[0]) == 5


def test_prepare_chart_data_keeps_all_rows_by_default():
    manager, window = _make_manager()
    dates, opens, highs, lows, closes, x, ohlc = manager._prepare_chart_data(_df())

    assert len(dates) == N
    assert np.array_equal(x, np.arange(N))
    assert len(ohlc) == N


def test_setup_chart_axes_sets_ticks_and_y_range(plot):
    manager, window = _make_manager()
    df = _df(10)
    dates = df['date'].to_list()
    highs = df['high'].to_list()
    lows = df['low'].to_list()

    manager._setup_chart_axes(plot, dates, highs, lows)

    y_min, y_max = plot.viewRange()[1]
    # pyqtgraph 会在 setYRange 之外再叠加 padding，因此只断言范围覆盖数据
    assert y_min <= min(lows)
    assert y_max >= max(highs)
    assert plot.getAxis('bottom')._tickLevels is not None


def test_draw_indicator_curve(plot):
    manager, window = _make_manager()
    x = np.arange(4)
    manager._draw_indicator_curve(plot, x, [1.0, 2.0, 3.0, 4.0], 'w', 1, 'line')

    assert len(_plot_items(plot)) == 1
    assert _plot_items(plot)[0].name() == 'line'


def test_draw_indicator_histogram_with_color_list(plot):
    manager, window = _make_manager()
    x = np.arange(4)

    manager._draw_indicator_histogram(plot, x, [1.0, -1.0, 2.0, -2.0])

    bars = [item for item in _plot_items(plot) if isinstance(item, pg.BarGraphItem)]
    assert len(bars) == 1


def test_setup_indicator_axis_handles_negative_values(plot):
    manager, window = _make_manager()
    manager._setup_indicator_axis(plot, np.arange(4), [-1.0, 2.0, -3.0, 4.0])

    x_min, x_max = plot.viewRange()[0]
    y_min, y_max = plot.viewRange()[1]
    assert x_min <= 0
    assert x_max >= 3
    assert y_min <= -3.0
    assert y_max >= 4.0


@pytest.mark.parametrize(('name', 'columns', 'keyword'), [
    ('KDJ', ['k', 'd', 'j'], 'K: '),
    ('MACD', ['macd', 'macd_signal', 'macd_hist'], 'MACD'),
    ('MA', ['ma5', 'ma10', 'ma20', 'ma60'], 'MA5'),
    ('WR', ['wr1', 'wr2'], 'WR1'),
    ('BOLL', ['mb', 'up', 'dn'], 'MB'),
    ('DMI', ['pdi', 'ndi', 'adx', 'adxr'], 'PDI'),
    ('DMA', ['dma', 'ama'], 'AMA'),
])
def test_update_indicator_values(name, columns, keyword):
    manager, window = _make_manager()
    label = QLabel()
    manager._update_indicator_values(label, name, _pandas_df())

    assert keyword in label.text()


def test_update_indicator_values_legacy_wr_fallback():
    manager, window = _make_manager()
    label = QLabel()
    df = _pandas_df()[['wr1']].rename(columns={'wr1': 'wr'})

    manager._update_indicator_values(label, 'WR', df)

    assert 'WR:' in label.text()


@pytest.mark.parametrize(('name', 'df_columns'), [
    ('WR', ['date']),
    ('BOLL', ['date']),
    ('DMI', ['date']),
    ('DMA', ['date']),
    ('UNKNOWN', ['date']),
])
def test_update_indicator_values_missing_columns(name, df_columns):
    manager, window = _make_manager()
    label = QLabel('prev')
    df = _pandas_df()[df_columns]

    manager._update_indicator_values(label, name, df)

    assert label.text() == ''


def test_update_indicator_values_without_label():
    manager, window = _make_manager()
    manager._update_indicator_values(None, 'KDJ', _pandas_df())


@pytest.mark.parametrize(('name', 'columns', 'expected_names'), [
    ('KDJ', ['k', 'd', 'j'], {'K', 'D', 'J'}),
    ('RSI', ['rsi'], {'RSI'}),
    ('RSI', ['rsi14'], {'RSI'}),
    ('MA', ['ma5', 'ma10', 'ma20', 'ma60'], {'MA5', 'MA10', 'MA20', 'MA60'}),
    ('DMI', ['pdi', 'ndi', 'adx', 'adxr'], {'+DI', '-DI', 'ADX', 'ADXR'}),
])
def test_draw_technical_indicator(plot, name, columns, expected_names):
    manager, window = _make_manager()
    df = _df()[columns]
    x = np.arange(N)

    manager._draw_technical_indicator(plot, name, x, df)

    names = {
        item.name() for item in _plot_items(plot)
        if isinstance(item, pg.PlotDataItem)
    }
    assert names == expected_names


def test_draw_technical_indicator_macd_histogram_path(plot):
    manager, window = _make_manager()
    df = _df()[['macd', 'macd_signal', 'macd_hist']]

    manager._draw_technical_indicator(plot, 'MACD', np.arange(N), df)

    names = {
        item.name() for item in _plot_items(plot)
        if isinstance(item, pg.PlotDataItem)
    }
    assert names == {'DIF', 'DEA'}
    assert any(isinstance(item, pg.BarGraphItem) for item in _plot_items(plot))


def test_draw_technical_indicator_fallback_for_unknown_name(plot):
    manager, window = _make_manager()
    df = pl.DataFrame({'date': list(range(N)), 'close': [1.0] * N, 'foo': [2.0] * N})
    manager._draw_technical_indicator(plot, 'FOO', np.arange(N), df)

    names = {
        item.name() for item in _plot_items(plot)
        if isinstance(item, pg.PlotDataItem)
    }
    assert names == {'foo'}


def test_draw_technical_indicator_volume_histogram_path(plot):
    manager, window = _make_manager()
    df = _df()[['volume']]

    manager._draw_technical_indicator(plot, 'VOL', np.arange(N), df)

    assert any(isinstance(item, pg.BarGraphItem) for item in _plot_items(plot))


def test_draw_technical_indicator_pandas_uses_values(plot):
    manager, window = _make_manager()
    df = pd.DataFrame({'close': [1.0] * N, 'foo': [3.0] * N})
    manager._draw_technical_indicator(plot, 'FOO', np.arange(N), df)

    assert len(_plot_items(plot)) >= 1


@pytest.mark.parametrize(('name', 'keyword'), [
    ('KDJ', 'K: '),
    ('RSI', 'RSI14'),
    ('MACD', 'DIF'),
    ('WR', 'WR1'),
    ('BOLL', 'MB'),
    ('DMI', 'PDI'),
    ('TRIX', 'TRIX'),
    ('BRAR', 'BR'),
    ('DMA', 'AMA'),
    ('EXPMA', 'EXPMA12'),
    ('BBI', 'BBI(3,6,12,24)'),
    ('HSL', 'HSL'),
    ('LB', 'LB'),
    ('CYC', 'CYC5'),
    ('CYS', 'CYS'),
    ('default', 'K: '),
])
def test_label_renderer_full_rows(name, keyword):
    manager, window = _make_manager()

    assert keyword in manager._label_renderer(name)(_df())


@pytest.mark.parametrize(('name', 'keyword'), [
    ('KDJ', 'KDJ指标数据不可用'),
    ('RSI', 'RSI指标数据不可用'),
    ('MACD', 'MACD指标数据不可用'),
    ('WR', 'WR指标数据不可用'),
    ('BOLL', 'BOLL指标数据不可用'),
    ('DMI', 'DMI指标数据不可用'),
    ('TRIX', 'TRIX指标数据不可用'),
    ('BRAR', 'BRAR指标数据不可用'),
    ('DMA', 'DMA指标数据不可用'),
    ('EXPMA', 'EXPMA指标数据不可用'),
    ('BBI', 'BBI指标数据不可用'),
    ('HSL', 'HSL指标数据不可用'),
    ('LB', 'LB指标数据不可用'),
    ('CYC', 'CYC指标数据不可用'),
    ('CYS', 'CYS指标数据不可用'),
    ('default', '指标数据不可用'),
])
def test_label_renderer_missing_columns(name, keyword):
    manager, window = _make_manager()
    empty = pl.DataFrame({'date': []})

    assert keyword in manager._label_renderer(name)(empty)


@pytest.mark.parametrize(('value', 'expected'), [
    (12.0, '#FF0000'),
    (6.0, '#FFA500'),
    (3.0, '#00BFFF'),
])
def test_hsl_label_color(value, expected):
    manager, window = _make_manager()
    df = _df(2).with_columns(pl.lit(value).alias('hsl'))

    assert expected in manager._render_hsl_label(df)


@pytest.mark.parametrize(('value', 'expected'), [
    (2.5, '#FF0000'),
    (1.8, '#FFA500'),
    (1.2, '#00FF7F'),
    (0.6, '#00BFFF'),
])
def test_lb_label_color(value, expected):
    manager, window = _make_manager()
    df = _df(2).with_columns(pl.lit(value).alias('lb'))

    assert expected in manager._render_lb_label(df)


def test_cys_label_color_negative():
    manager, window = _make_manager()
    df = _df(2).with_columns(pl.lit(-1.5).alias('cys'))

    assert '#00FF00' in manager._render_cys_label(df)


def test_macd_label_color_negative():
    manager, window = _make_manager()
    df = _df(2).with_columns(pl.lit(-1.0).alias('macd_hist'))

    assert '#00FF00' in manager._render_macd_label(df)


def test_latest_value_variants():
    df = _df(3)
    assert ChartManager._latest_value(df, 'k') == pytest.approx(6.2)
    assert ChartManager._latest_value(df, 'missing') is None
    assert ChartManager._latest_value(pl.DataFrame({'k': []}), 'k') is None


def test_save_indicator_data_sets_all_series():
    manager, window = _make_manager()
    manager.save_indicator_data(_df())

    for attr, _fields in ChartManager._INDICATOR_SERIES_TABLE:
        assert hasattr(window, attr)

    assert window.current_kdj_data['k'].shape == (N,)
    assert window.current_volume_data['volume'].shape == (N,)


def test_save_indicator_data_missing_columns_uses_empty_arrays():
    manager, window = _make_manager()
    manager.save_indicator_data(pl.DataFrame({'date': [1, 2]}))

    assert window.current_kdj_data['k'].size == 0
    assert window.current_cys_data['cys'].size == 0


@pytest.mark.parametrize('name', [
    'KDJ', 'RSI', 'MACD', 'WR', 'BOLL', 'DMI', 'TRIX',
    'BRAR', 'DMA', 'EXPMA', 'BBI', 'HSL', 'LB', 'CYC', 'CYS', 'default',
])
def test_update_indicator_values_label_creates_label(name):
    manager, window = _make_manager()
    manager.update_indicator_values_label(name, _df())

    assert isinstance(window.kdj_values_label, QLabel)
    assert window.kdj_values_label.text() != ''
    assert window.current_kdj_data['k'].shape == (N,)


def test_plot_k_line_delegates_to_window():
    manager, window = _make_manager()
    window._plot_k_line_impl = lambda df, name, code: ('sentinel', df, name, code)
    df = _df()

    result = manager.plot_k_line(df, '名称', '600519.SH')

    assert result == ('sentinel', df, '名称', '600519.SH')


# ---------------------------------------------------------------------------
# ChartUIBuilder
# ---------------------------------------------------------------------------

LABEL_COLUMNS = [
    'k', 'd', 'j', 'rsi14', 'macd', 'macd_signal', 'macd_hist',
    'volume', 'vol_ma5', 'vol_ma10', 'wr1', 'wr2', 'mb', 'up', 'dn',
    'vr', 'mavr', 'ar', 'br', 'pdi', 'ndi', 'adx', 'adxr', 'trix', 'trma',
    'obv', 'dma', 'ama', 'asi', 'asi_sig', 'emv', 'cci', 'roc', 'mtm', 'psy',
    'mcst', 'mcst_ma', 'sar', 'swl', 'sws', 'cr', 'expma12', 'expma50',
    'bbi', 'hsl', 'hsl_ma5', 'hsl_ma10', 'lb', 'cyc5', 'cyc13', 'cyc34',
    'cyc_inf', 'cys', 'cys_ma5',
]


def _label_df(n=2, columns=None):
    """构造包含全部指标列的 Polars DataFrame（用于标签文本断言）。"""
    columns = LABEL_COLUMNS if columns is None else columns
    data = {col: [float(i + 1) + j * 0.1 for j in range(n)] for i, col in enumerate(columns)}
    return pl.DataFrame(data)


def _make_builder():
    main_window = SimpleNamespace(
        current_selected_window=1,
        create_indicator_menu_button=lambda index: QWidget(),
    )
    return ChartUIBuilder(main_window), main_window


def test_create_title_section_builds_labels(qapp):
    builder, window = _make_builder()
    layout = QVBoxLayout()
    holder = QWidget()
    holder.setLayout(layout)
    window.chart_layout = layout

    container = builder.create_title_section('贵州茅台', '600519.SH')

    assert window.chart_title_label.text() == '贵州茅台(600519.SH) 日线'
    assert 'MA5: --' in window.ma_values_label.text()
    assert isinstance(window.menu_btn, QWidget)
    assert window.title_ma_container is container
    assert layout.count() == 1
    assert layout.itemAt(0).widget() is container


def test_create_title_section_replaces_previous_container(qapp):
    builder, window = _make_builder()
    layout = QVBoxLayout()
    holder = QWidget()
    holder.setLayout(layout)
    window.chart_layout = layout

    builder.create_title_section('平安银行', '000001.SZ')
    builder.create_title_section('招商银行', '600036.SH')

    assert window.chart_title_label.text() == '招商银行(600036.SH) 日线'
    assert layout.count() == 1
    assert layout.itemAt(0).widget() is window.title_ma_container


def test_create_title_section_without_chart_layout(qapp):
    builder, window = _make_builder()

    container = builder.create_title_section('中国平安', '601318.SH')

    assert window.title_ma_container is container
    assert '中国平安(601318.SH)' in window.chart_title_label.text()


@pytest.mark.parametrize(('window_index', 'indicator', 'prefix', 'keyword'), [
    (2, 'VOL', 'volume', 'VOLUME'),
    (3, 'KDJ', 'kdj', 'K: '),
])
def test_create_indicator_label_bar_sets_references(
    qapp, window_index, indicator, prefix, keyword
):
    builder, window = _make_builder()

    container = builder.create_indicator_label_bar(window_index, indicator, _label_df())

    assert getattr(window, f'{prefix}_label_container') is container
    assert isinstance(getattr(window, f'{prefix}_values_label'), QLabel)
    assert keyword in getattr(window, f'{prefix}_values_label').text()
    assert getattr(window, f'{prefix}_menu_btn') is not None


def test_create_indicator_label_bar_other_window_keeps_no_reference(qapp):
    builder, window = _make_builder()

    container = builder.create_indicator_label_bar(0, 'KDJ', _label_df())

    assert container is not None
    assert not hasattr(window, 'kdj_label_container')


def test_create_indicator_label_bar_twice_cleans_previous(qapp):
    builder, window = _make_builder()

    builder.create_indicator_label_bar(2, 'VOL', _label_df())
    container = builder.create_indicator_label_bar(2, 'VOL', _label_df())

    assert window.volume_label_container is container
    assert 'VOLUME' in window.volume_values_label.text()


@pytest.mark.parametrize(('indicator', 'keyword'), [
    ('KDJ', 'K: '),
    ('RSI', 'RSI14'),
    ('MACD', 'DIF'),
    ('VOL', 'VOLUME'),
    ('WR', 'WR1'),
    ('BOLL', 'MB'),
    ('VR', 'VR: '),
    ('BRAR', 'AR: '),
    ('DMI', 'PDI'),
    ('TRIX', 'TRIX'),
    ('OBV', 'OBV'),
    ('ASI', 'ASI'),
    ('EMV', 'EMV'),
    ('CCI', 'CCI'),
    ('ROC', 'ROC'),
    ('MTM', 'MTM'),
    ('PSY', 'PSY'),
    ('MCST', 'MCST'),
    ('SAR', 'SAR(0.02,0.2)'),
    ('FSL', 'SWL'),
    ('CR', 'CR(26)'),
    ('DMA', 'DMA(10,50)'),
    ('EXPMA', 'EXPMA12'),
    ('BBI', 'BBI(3,6,12,24)'),
    ('HSL', 'HSL'),
    ('LB', 'LB'),
    ('CYC', 'CYC5'),
    ('CYS', 'CYS'),
])
def test_get_indicator_label_text_full_columns(indicator, keyword):
    builder, window = _make_builder()

    assert keyword in builder._get_indicator_label_text(indicator, _label_df())


@pytest.mark.parametrize(('indicator', 'keyword'), [
    ('KDJ', 'KDJ指标数据不可用'),
    ('RSI', 'RSI指标数据不可用'),
    ('MACD', 'MACD指标数据不可用'),
    ('VOL', '成交量数据不可用'),
    ('BOLL', 'BOLL指标数据不可用'),
    ('BRAR', 'BRAR指标数据不可用'),
    ('DMI', 'DMI指标数据不可用'),
    ('TRIX', 'TRIX指标数据不可用'),
    ('OBV', 'OBV指标数据不可用'),
    ('ASI', 'ASI指标数据不可用'),
    ('EMV', 'EMV指标数据不可用'),
    ('CCI', 'CCI指标数据不可用'),
    ('ROC', 'ROC指标数据不可用'),
    ('MTM', 'MTM指标数据不可用'),
    ('PSY', 'PSY指标数据不可用'),
    ('MCST', 'MCST指标数据不可用'),
    ('SAR', 'SAR指标数据不可用'),
    ('FSL', 'FSL指标数据不可用'),
    ('CR', 'CR指标数据不可用'),
    ('DMA', 'DMA指标数据不可用'),
    ('EXPMA', 'EXPMA指标数据不可用'),
    ('BBI', 'BBI指标数据不可用'),
    ('HSL', 'HSL指标数据不可用'),
    ('LB', 'LB指标数据不可用'),
    ('CYC', 'CYC指标数据不可用'),
    ('CYS', 'CYS指标数据不可用'),
])
def test_get_indicator_label_text_missing_columns(indicator, keyword):
    builder, window = _make_builder()
    empty = pl.DataFrame({'date': []})

    assert keyword in builder._get_indicator_label_text(indicator, empty)


@pytest.mark.parametrize('indicator', ['WR', 'VR', 'UNKNOWN'])
def test_get_indicator_label_text_empty_string_cases(indicator):
    builder, window = _make_builder()

    assert builder._get_indicator_label_text(indicator, _label_df(columns=['date'])) == ''


def test_get_indicator_label_text_optional_columns():
    builder, window = _make_builder()

    legacy_wr = builder._get_indicator_label_text('WR', _label_df(columns=['wr']))
    vr_only = builder._get_indicator_label_text('VR', _label_df(columns=['vr']))
    dmi_core = builder._get_indicator_label_text('DMI', _label_df(columns=['pdi', 'ndi']))
    trix_only = builder._get_indicator_label_text('TRIX', _label_df(columns=['trix']))
    asi_only = builder._get_indicator_label_text('ASI', _label_df(columns=['asi']))
    mcst_only = builder._get_indicator_label_text('MCST', _label_df(columns=['mcst']))
    hsl_only = builder._get_indicator_label_text('HSL', _label_df(columns=['hsl']))
    cyc_core = builder._get_indicator_label_text('CYC', _label_df(columns=['cyc5', 'cyc13']))
    cys_only = builder._get_indicator_label_text('CYS', _label_df(columns=['cys']))

    assert 'WR:' in legacy_wr
    assert 'MAVR' not in vr_only and 'VR:' in vr_only
    assert 'ADX' not in dmi_core and 'PDI' in dmi_core
    assert 'TRMA' not in trix_only and 'TRIX' in trix_only
    assert 'SIG' not in asi_only and 'ASI' in asi_only
    assert 'MCST' in mcst_only
    assert 'HSL' in hsl_only
    assert 'CYC5' in cyc_core
    assert 'CYS' in cys_only


def test_create_indicator_label_bar_optional_columns(qapp):
    builder, window = _make_builder()

    container = builder.create_indicator_label_bar(3, 'WR', _label_df(columns=['wr']))

    assert window.kdj_label_container is container
    assert 'WR:' in window.kdj_values_label.text()


@pytest.mark.parametrize('window_index', [2, 3])
def test_add_label_to_container_inserts_into_layout(qapp, window_index):
    builder, window = _make_builder()
    layout = QVBoxLayout()
    holder = QWidget()
    holder.setLayout(layout)
    prefix = 'volume' if window_index == 2 else 'kdj'
    setattr(window, f'{prefix}_container_layout', layout)

    container = builder.create_indicator_label_bar(window_index, 'KDJ', _label_df())
    builder.add_label_to_container(container, getattr(window, f'{prefix}_values_label'), window_index)

    assert layout.count() == 1
    assert layout.itemAt(0).widget() is container


def test_add_label_to_container_ignores_unknown_window(qapp):
    builder, window = _make_builder()
    layout = QVBoxLayout()
    holder = QWidget()
    holder.setLayout(layout)

    container = builder.create_indicator_label_bar(2, 'VOL', _label_df())
    builder.add_label_to_container(container, window.volume_values_label, 1)

    assert layout.count() == 0


# ---------------------------------------------------------------------------
# ChartEventBinder
# ---------------------------------------------------------------------------


class _MouseHandler:
    def __init__(self):
        self.moves = []
        self.clicks = []

    def handle_mouse_moved(self, pos, dates, opens, highs, lows, closes):
        self.moves.append(pos)

    def handle_mouse_clicked(self, event, dates, opens, highs, lows, closes):
        self.clicks.append(event)


def _make_binder():
    handler = _MouseHandler()
    window = SimpleNamespace(
        tech_plot_widget=pg.PlotWidget(),
        volume_plot_widget=pg.PlotWidget(),
        kdj_plot_widget=pg.PlotWidget(),
        event_handler_manager=SimpleNamespace(get_mouse_handler=lambda: handler),
        show_info_box=lambda: None,
    )
    return ChartEventBinder(window), window, handler


BIND_ARGS = ([1, 2, 3], [1.0, 1.0, 1.0], [2.0, 2.0, 2.0], [0.5, 0.5, 0.5], [1.5, 1.5, 1.5])


def test_bind_all_events_sets_info_timer_and_disables_menus(qapp):
    binder, window, handler = _make_binder()
    shown = []
    window.show_info_box = lambda: shown.append(True)

    binder.bind_all_events(*BIND_ARGS)

    assert isinstance(window.info_timer, QTimer)
    assert window.info_timer.isSingleShot()
    assert window.info_timer.interval() == 200
    for widget in (
        window.tech_plot_widget, window.volume_plot_widget, window.kdj_plot_widget
    ):
        assert widget.getViewBox().menuEnabled() is False

    window.info_timer.timeout.emit()
    assert shown == [True]


def test_bind_all_events_connects_mouse_handlers(qapp):
    binder, window, handler = _make_binder()

    binder.bind_all_events(*BIND_ARGS)

    window.tech_plot_widget.scene().sigMouseMoved.emit(QPointF(1, 2))
    window.volume_plot_widget.scene().sigMouseMoved.emit(QPointF(3, 4))
    window.kdj_plot_widget.scene().sigMouseMoved.emit(QPointF(5, 6))
    assert len(handler.moves) == 3

    click_event = object()
    window.tech_plot_widget.scene().sigMouseClicked.emit(click_event)
    window.volume_plot_widget.scene().sigMouseClicked.emit(click_event)
    window.kdj_plot_widget.scene().sigMouseClicked.emit(click_event)
    assert len(handler.clicks) == 3


def test_bind_all_events_syncs_x_range(qapp):
    binder, window, handler = _make_binder()
    calls = []
    window.dividend_marker_manager = SimpleNamespace(update_position=lambda: calls.append(True))

    binder.bind_all_events(*BIND_ARGS)

    window.tech_plot_widget.getViewBox().setXRange(2, 9, padding=0)

    x_min, x_max = window.volume_plot_widget.getViewBox().viewRange()[0]
    assert x_min <= 2.0 + 1e-6
    assert x_max >= 9.0 - 1e-6
    assert calls


def test_disable_default_context_menu_disables_viewbox(qapp):
    binder, window, handler = _make_binder()

    binder._disable_default_context_menu()

    assert window.tech_plot_widget.getViewBox().menuEnabled() is False
    assert window.volume_plot_widget.getViewBox().menuEnabled() is False


def test_setup_crosshair_creates_hidden_lines(qapp):
    binder, window, handler = _make_binder()

    lines = binder.setup_crosshair()

    assert len(lines) == 6
    assert all(isinstance(line, pg.InfiniteLine) for line in lines)
    assert all(not line.isVisible() for line in lines)
    assert len(window.tech_plot_widget.plotItem.items) == 2
    assert len(window.volume_plot_widget.plotItem.items) == 2
    assert len(window.kdj_plot_widget.plotItem.items) == 2


def test_setup_crosshair_removes_previous_lines(qapp):
    binder, window, handler = _make_binder()
    window.vline = pg.InfiniteLine(angle=90)
    window.tech_plot_widget.addItem(window.vline)
    window.hline = pg.InfiniteLine(angle=0)
    window.tech_plot_widget.addItem(window.hline)

    lines = binder.setup_crosshair()

    assert window.vline not in window.tech_plot_widget.plotItem.items
    assert len(lines) == 6
    assert len(window.tech_plot_widget.plotItem.items) == 2


def test_setup_info_text_adds_hidden_text_item(qapp):
    binder, window, handler = _make_binder()

    info_text = binder.setup_info_text()

    assert isinstance(info_text, pg.TextItem)
    assert info_text in window.tech_plot_widget.plotItem.items
    assert not info_text.isVisible()
