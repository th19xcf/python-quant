"""图表指标渲染 Mixin 单元测试（离屏 pyqtgraph，不依赖网络/数据库）。"""

from types import SimpleNamespace

import numpy as np
import polars as pl
import pyqtgraph as pg
import pytest

from src.ui.chart_components.indicator_renderer import IndicatorRenderer

N = 8


def _df(columns, n=N, all_nan=False):
    """构造测试用 Polars DataFrame。"""
    data = {}
    for i, col in enumerate(columns):
        if all_nan:
            data[col] = [float('nan')] * n
        else:
            data[col] = [float(i + 1) + j * 0.1 for j in range(n)]
    return pl.DataFrame(data)


def _host(show_ma_lines=True):
    mw = SimpleNamespace(
        moving_averages={},
        selected_ma=None,
        ma_points=[],
        show_ma_lines=show_ma_lines,
        ma_data={},
    )
    return IndicatorRenderer(mw), mw


@pytest.fixture
def plot(qapp):
    widget = pg.PlotWidget()
    yield widget
    widget.close()


def _user_items(widget):
    """仅统计通过 addItem/plot 添加到 PlotItem 的图形项（排除坐标轴等默认项）。"""
    return widget.plotItem.items


def _data_items(widget):
    """仅统计 plot() 产生的 PlotDataItem。"""
    return [item for item in _user_items(widget) if isinstance(item, pg.PlotDataItem)]


def _names(widget):
    return {item.name() for item in _data_items(widget)}


def _bar_graphs(widget):
    return [item for item in _user_items(widget) if isinstance(item, pg.BarGraphItem)]


def _curves(widget):
    return [item for item in _user_items(widget) if isinstance(item, pg.PlotCurveItem)]


def _assert_nothing_drawn(widget):
    assert _names(widget) == set()
    assert _curves(widget) == []
    assert _bar_graphs(widget) == []


# (方法名, 所需列, 正常数据下通过 plot(name=...) 生成的曲线名)
RENDER_CASES = [
    ('render_boll', ['mb', 'up', 'dn'], {'MB', 'UP', 'DN'}),
    ('render_dmi', ['pdi', 'ndi', 'adx', 'adxr'], {'PDI', 'NDI', 'ADX', 'ADXR'}),
    ('render_dma', ['dma', 'ama'], {'DMA', 'AMA'}),
    ('render_sar', ['sar'], {'SAR'}),
    ('render_expma', ['expma12', 'expma50'], {'EXPMA12', 'EXPMA50'}),
    ('render_bbi', ['bbi'], {'BBI'}),
    ('render_cyc', ['cyc5', 'cyc13', 'cyc34', 'cyc_inf'], {'CYC5', 'CYC13', 'CYC34', 'CYC∞'}),
    ('render_vr', ['vr', 'mavr'], {'VR', 'MAVR'}),
    ('render_obv', ['obv'], {'OBV'}),
    ('render_asi', ['asi', 'asi_sig'], {'ASI', 'ASI_SIG'}),
    ('render_emv', ['emv'], {'EMV'}),
    ('render_mcst', ['mcst', 'mcst_ma'], {'MCST', 'MCST_MA'}),
    ('render_kdj', ['k', 'd', 'j'], {'K', 'D', 'J'}),
    ('render_rsi', ['rsi14'], {'RSI14'}),
    ('render_wr', ['wr1', 'wr2'], {'WR1', 'WR2'}),
    ('render_brar', ['ar', 'br'], {'AR', 'BR'}),
    ('render_trix', ['trix', 'trma'], {'TRIX', 'TRMA'}),
    ('render_cci', ['cci'], {'CCI'}),
    ('render_roc', ['roc'], {'ROC'}),
    ('render_mtm', ['mtm'], {'MTM'}),
    ('render_psy', ['psy'], {'PSY'}),
    ('render_fsl', ['swl', 'sws'], {'SWL', 'SWS'}),
    ('render_cr', ['cr'], {'CR'}),
    ('render_abi', ['abi10'], {'ABI'}),
    ('render_adl', ['adl'], {'ADL'}),
    ('render_adr', ['adr10'], {'ADR'}),
    ('render_obos', ['obos10'], {'OBOS'}),
]


@pytest.mark.parametrize(('method', 'columns', 'expected'), RENDER_CASES)
def test_render_normal_columns(qapp, plot, method, columns, expected):
    renderer, mw = _host()
    df = _df(columns)
    x = np.arange(N)

    result = getattr(renderer, method)(plot, df, x)

    assert result is df
    assert _names(plot) == expected


@pytest.mark.parametrize(('method', 'columns', 'expected'), RENDER_CASES)
def test_render_missing_columns_is_noop(qapp, plot, method, columns, expected):
    renderer, mw = _host()
    df = _df(['close'])
    x = np.arange(N)

    result = getattr(renderer, method)(plot, df, x)

    assert result is df
    _assert_nothing_drawn(plot)


@pytest.mark.parametrize(('method', 'columns', 'expected'), RENDER_CASES)
def test_render_all_nan_draws_nothing(qapp, plot, method, columns, expected):
    renderer, mw = _host()
    df = _df(columns, all_nan=True)
    x = np.arange(N)

    result = getattr(renderer, method)(plot, df, x)

    assert result is df
    _assert_nothing_drawn(plot)


@pytest.mark.parametrize('method', [
    'render_ma_lines', 'render_boll', 'render_kdj', 'render_macd', 'render_rsi', 'render_wr',
    'render_volume',
])
def test_render_none_dataframe_is_noop(qapp, plot, method):
    renderer, mw = _host()

    result = getattr(renderer, method)(plot, None, np.arange(N))

    assert result is None
    _assert_nothing_drawn(plot)


def test_render_ma_lines_populates_state(qapp, plot):
    renderer, mw = _host()
    df = _df(['ma5', 'ma10', 'ma20', 'ma60'])
    x = np.arange(N)

    renderer.render_ma_lines(plot, df, x)

    assert _names(plot) == {'MA5', 'MA10', 'MA20', 'MA60'}
    assert set(mw.moving_averages) == {'MA5', 'MA10', 'MA20', 'MA60'}
    assert len(mw.moving_averages['MA5']['data'][1]) == N
    assert mw.moving_averages['MA60']['color'] == '#00FF00'
    assert len(mw.ma_data['MA5']) == N
    assert mw.ma_colors['MA20'] == 'red'
    assert mw.ma_points == []


def test_render_ma_lines_hidden_keeps_data_only(qapp, plot):
    renderer, mw = _host(show_ma_lines=False)
    df = _df(['ma5', 'ma10', 'ma20', 'ma60'])

    renderer.render_ma_lines(plot, df, np.arange(N))

    _assert_nothing_drawn(plot)
    assert mw.moving_averages == {}
    assert len(mw.ma_data['MA10']) == N


def test_render_ma_lines_empty_table(qapp, plot):
    renderer, mw = _host()
    df = _df(['ma5', 'ma10', 'ma20', 'ma60'], n=0)

    renderer.render_ma_lines(plot, df, np.arange(0))

    _assert_nothing_drawn(plot)
    assert mw.ma_data['MA5'].size == 0


def test_render_ma_lines_partial_nan_uses_valid_points(qapp, plot):
    renderer, mw = _host()
    ma5 = [float('nan')] * 4 + [1.0, 2.0, 3.0, 4.0]
    df = pl.DataFrame({'ma5': ma5, 'ma10': [5.0] * N})

    renderer.render_ma_lines(plot, df, np.arange(N))

    assert _names(plot) == {'MA5', 'MA10'}
    assert len(mw.moving_averages['MA5']['data'][0]) == 4
    assert len(mw.moving_averages['MA10']['data'][0]) == N


def test_render_volume_draws_bar_and_ma(qapp, plot):
    renderer, mw = _host()
    df = pl.DataFrame({
        'volume': [100.0, 200.0, 150.0, 300.0, 120.0, 260.0, 90.0, 180.0],
        'vol_ma5': [120.0, 150.0, 160.0, 180.0, 190.0, 200.0, 210.0, 220.0],
        'vol_ma10': [110.0] * N,
        'close': [10.0, 11.0, 10.5, 12.0, 11.5, 12.5, 12.0, 13.0],
        'open': [9.5, 10.0, 11.0, 10.4, 12.0, 11.4, 12.6, 11.9],
    })
    x = np.arange(N)

    renderer.render_volume(plot, df, x)

    assert len(_bar_graphs(plot)) == 1
    assert _bar_graphs(plot)[0].opts['height'].shape == (N,)
    assert len(_data_items(plot)) == 2
    assert mw.moving_averages == {}


def test_render_volume_missing_volume_column(qapp, plot):
    renderer, mw = _host()

    result = renderer.render_volume(plot, _df(['close']), np.arange(N))

    assert result is not None
    _assert_nothing_drawn(plot)
    assert _bar_graphs(plot) == []


def test_render_volume_empty_table(qapp, plot):
    renderer, mw = _host()
    df = pl.DataFrame({'volume': []})

    result = renderer.render_volume(plot, df, np.arange(0))

    assert result is df
    _assert_nothing_drawn(plot)
    assert _bar_graphs(plot) == []


def test_render_volume_without_open_close_uses_grey_bars(qapp, plot):
    renderer, mw = _host()
    df = pl.DataFrame({'volume': [100.0] * N})

    renderer.render_volume(plot, df, np.arange(N))

    bar = _bar_graphs(plot)[0]
    assert len(bar.opts['brushes']) == N


def test_render_hsl_draws_bars_and_ma(qapp, plot):
    renderer, mw = _host()
    df = pl.DataFrame({
        'hsl': [12.0, 8.0, 3.0, 15.0, 6.0, 2.0, 11.0, 4.0],
        'hsl_ma5': [5.0] * N,
        'hsl_ma10': [4.0] * N,
    })

    renderer.render_hsl(plot, df, np.arange(N))

    assert _names(plot) == {'MA5', 'MA10'}
    assert len(_curves(plot)) == 3


def test_render_lb_draws_colored_lines(qapp, plot):
    renderer, mw = _host()
    df = pl.DataFrame({'lb': [2.5, 1.8, 1.2, 0.6, 3.0, 2.2, 0.9, 1.1]})

    renderer.render_lb(plot, df, np.arange(N))

    assert _names(plot) == set()
    assert len(_curves(plot)) >= 1


def test_render_lb_missing_column(qapp, plot):
    renderer, mw = _host()

    assert renderer.render_lb(plot, _df(['close']), np.arange(N)) is not None
    _assert_nothing_drawn(plot)


def test_render_cys_draws_lines_and_ma(qapp, plot):
    renderer, mw = _host()
    df = pl.DataFrame({
        'cys': [1.0, -2.0, 3.0, -1.5, 0.5, 2.5, -0.5, 1.2],
        'cys_ma5': [0.5] * N,
    })

    renderer.render_cys(plot, df, np.arange(N))

    assert _names(plot) == {'MA5'}
    assert len(_curves(plot)) == 2


def test_render_cys_missing_column(qapp, plot):
    renderer, mw = _host()

    assert renderer.render_cys(plot, _df(['close']), np.arange(N)) is not None
    _assert_nothing_drawn(plot)


def test_render_vol_tdx_draws_two_color_groups(qapp, plot):
    renderer, mw = _host()
    df = pl.DataFrame({
        'vol_tdx': [10.0, 20.0, 15.0, 30.0, 25.0, 12.0, 18.0, 22.0],
        'close': [10.0, 11.0, 10.5, 12.0, 11.0, 10.8, 11.5, 12.4],
    })

    renderer.render_vol_tdx(plot, df, np.arange(N))

    assert _names(plot) == set()
    assert len(_curves(plot)) == 2


def test_render_vol_tdx_missing_column(qapp, plot):
    renderer, mw = _host()

    assert renderer.render_vol_tdx(plot, _df(['close']), np.arange(N)) is not None
    _assert_nothing_drawn(plot)


def test_render_macd_histogram_bars(qapp, plot):
    renderer, mw = _host()
    df = pl.DataFrame({
        'macd': [0.5, -0.2, 0.8, -0.4, 0.1, 0.3, -0.1, 0.6],
        'macd_signal': [0.4] * N,
        'macd_hist': [0.1, -0.1, 0.2, -0.3, 0.05, 0.1, -0.2, 0.15],
    })

    renderer.render_macd(plot, df, np.arange(N))

    assert _names(plot) == {'DIF', 'DEA'}
    assert len(_bar_graphs(plot)) == 1


def test_render_macd_without_hist_column(qapp, plot):
    renderer, mw = _host()
    df = pl.DataFrame({'macd': [0.5, -0.2, 0.8, -0.4], 'macd_signal': [0.4] * 4})

    renderer.render_macd(plot, df, np.arange(4))

    assert _names(plot) == {'DIF', 'DEA'}
    assert _bar_graphs(plot) == []


def test_render_sar_length_mismatch_pads(qapp, plot):
    renderer, mw = _host()
    df = pl.DataFrame({'sar': [10.0, 11.0, 12.0]})

    renderer.render_sar(plot, df, np.arange(5))

    item = plot.listDataItems()[0]
    assert item.xData.shape == (3,)
    assert item.yData.shape == (3,)


def test_render_wr_legacy_column(qapp, plot):
    renderer, mw = _host()
    df = pl.DataFrame({'wr': [float(v) for v in range(N)]})

    renderer.render_wr(plot, df, np.arange(N))

    assert _names(plot) == {'WR'}


def test_render_cr_reference_line_added(qapp, plot):
    renderer, mw = _host()
    lines_before = len(plot.items())
    renderer.render_cr(plot, _df(['cr']), np.arange(N))
    assert len(plot.listDataItems()) == 1
    assert len(plot.items()) > lines_before
