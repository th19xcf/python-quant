
"""IndicatorLabelManager 与共享指标展示表的一致性测试（无需 GUI 环境）"""

import polars as pl

from src.ui.indicator_labels import IndicatorLabelManager


class _FakeLabel:
    """最小化的 QLabel 替身，只记录最后一次 setText 的内容。"""

    def __init__(self):
        self.text = None

    def setText(self, text):
        self.text = text


class _FakeChartManager:
    def __init__(self):
        self.saved = None

    def save_indicator_data(self, df):
        self.saved = df


class _FakeMainWindow:
    def __init__(self):
        self.kdj_values_label = _FakeLabel()
        self.chart_manager = _FakeChartManager()


def _df(columns):
    return pl.DataFrame(columns)


def test_kdj_rendered_with_shared_format():
    mw = _FakeMainWindow()
    manager = IndicatorLabelManager(mw)

    manager.update_indicator_values_label(
        'KDJ', _df({'k': [1.0, 2.345], 'd': [3.0, 4.567], 'j': [5.0, 6.789]}))

    assert mw.kdj_values_label.text == (
        "<font color='white'>K: 2.35</font>  "
        "<font color='yellow'>D: 4.57</font>  "
        "<font color='magenta'>J: 6.79</font>"
    )


def test_rsi14_column_aliased_to_rsi():
    mw = _FakeMainWindow()
    manager = IndicatorLabelManager(mw)

    manager.update_indicator_values_label(
        'RSI', _df({'rsi14': [40.0, 55.5]}))

    assert mw.kdj_values_label.text == (
        "<font color='blue'>RSI14: 55.50</font>"
    )


def test_macd_hist_color_follows_sign():
    mw = _FakeMainWindow()
    manager = IndicatorLabelManager(mw)

    manager.update_indicator_values_label(
        'MACD',
        _df({'macd': [0.1], 'macd_signal': [0.05], 'macd_hist': [-0.01]}),
    )

    assert "#00FF00" in mw.kdj_values_label.text


def test_unknown_indicator_falls_back_to_kdj():
    mw = _FakeMainWindow()
    manager = IndicatorLabelManager(mw)

    manager.update_indicator_values_label(
        'VOL', _df({'volume': [1000], 'k': [1.0], 'd': [2.0], 'j': [3.0]}))

    assert 'K: 1.00' in mw.kdj_values_label.text


def test_unknown_indicator_without_kdj_shows_not_available():
    mw = _FakeMainWindow()
    manager = IndicatorLabelManager(mw)

    manager.update_indicator_values_label('VOL', _df({'volume': [1000]}))

    assert mw.kdj_values_label.text == "<font color='white'>指标数据不可用</font>"


def test_empty_dataframe_keeps_label_untouched():
    mw = _FakeMainWindow()
    manager = IndicatorLabelManager(mw)

    manager.update_indicator_values_label('KDJ', pl.DataFrame())

    assert mw.kdj_values_label.text is None


def test_indicator_data_is_saved_for_mouse_hover():
    mw = _FakeMainWindow()
    manager = IndicatorLabelManager(mw)
    df = _df({'k': [1.0], 'd': [2.0], 'j': [3.0]})

    manager.update_indicator_values_label('KDJ', df)

    assert mw.chart_manager.saved is df
