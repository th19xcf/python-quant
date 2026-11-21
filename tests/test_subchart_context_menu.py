"""副图右键菜单与指标参数：菜单结构、用法注释覆盖、参数透传与持久化。"""

from __future__ import annotations

import datetime

import numpy as np
import polars as pl
import pytest

from config.indicator_notes import INDICATOR_NOTES
from src.ui.chart_components.indicator_renderer import ensure_indicator_columns
from src.ui.indicator_parameters import (
    PARAM_SPECS,
    apply_overrides,
    build_calc_kwargs,
    load_overrides,
    reset_overrides,
    validate_param_value,
)
from src.ui.subchart_context_menu import (
    SUB_MENU_STRUCTURE,
    build_subchart_menu,
    menu_entries,
)

EXPECTED_TEXTS = [
    '选择副图指标',
    '临时叠加其它指标',
    '指标用法注释',
    '调整指标参数',
    '修改当前指标公式',
    '分享当前公式',
    '分享到指标平台',
    '删除当前指标',
]


class _FakeWindow:
    def __init__(self, indicators):
        self.window_indicators = indicators


def _ohlcv(n: int = 120) -> pl.DataFrame:
    rng = np.random.default_rng(7)
    close = 20 + np.cumsum(rng.normal(0, 0.2, n))
    dates = pl.date_range(datetime.date(2025, 1, 1), datetime.date(2025, 12, 31),
                          eager=True).head(n)
    return pl.DataFrame({
        'date': dates,
        'open': close,
        'high': close + 0.3,
        'low': close - 0.3,
        'close': close,
        'volume': np.full(n, 1e5),
    })


def test_subchart_menu_structure_matches_screenshot():
    assert [text for text, _ in SUB_MENU_STRUCTURE] == EXPECTED_TEXTS
    enabled = dict(menu_entries('MACD'))
    assert enabled['指标用法注释'] and enabled['调整指标参数']
    assert not any(enabled[text] for text in (
        '选择副图指标', '临时叠加其它指标', '修改当前指标公式',
        '分享当前公式', '分享到指标平台', '删除当前指标'))
    # 无副图指标时整组禁用
    assert all(not flag for _, flag in menu_entries(None))


def test_menu_only_for_subchart_windows(qapp):
    # 主图（窗口 1）不返回菜单，主图既有右键行为不受影响
    assert build_subchart_menu(_FakeWindow({1: 'MACD'}), 1) is None

    menu = build_subchart_menu(_FakeWindow({2: 'MACD', 3: None}), 2)
    assert menu is not None
    submenu = menu.actions()[0].menu()
    assert submenu.title() == '副图指标'
    assert [action.text() for action in submenu.actions()] == EXPECTED_TEXTS
    states = {action.text(): action.isEnabled() for action in submenu.actions()}
    assert states['指标用法注释'] and states['调整指标参数']
    assert not states['删除当前指标']

    empty_menu = build_subchart_menu(_FakeWindow({2: None}), 2)
    assert empty_menu.actions()[0].menu().isEnabled() is False


def test_menu_identical_for_all_subchart_indicators_and_windows(qapp):
    """窗口 2/3 内的任意指标共用同一容器入口，右键菜单结构完全一致。"""
    cases = [(2, 'MACD'), (2, 'RSI'), (2, 'BOLL'), (3, 'SUPERTREND'), (3, 'OBV')]
    baseline = None
    for window_num, indicator in cases:
        menu = build_subchart_menu(_FakeWindow({window_num: indicator}), window_num)
        assert menu is not None, f'窗口{window_num}/{indicator} 未弹出菜单'
        actions = menu.actions()[0].menu().actions()
        snapshot = [(action.text(), action.isEnabled()) for action in actions]
        assert [text for text, _ in snapshot] == EXPECTED_TEXTS
        states = dict(snapshot)
        assert states['指标用法注释'] and states['调整指标参数']
        assert not states['删除当前指标']
        if baseline is None:
            baseline = snapshot
        else:
            assert snapshot == baseline


def test_notes_cover_all_implemented_sub_indicators():
    from src.ui.main_window_indicator_bar_mixin import IMPLEMENTED_INDICATORS

    expected = {name.upper() for name in IMPLEMENTED_INDICATORS} - {'MA'}
    missing = expected - set(INDICATOR_NOTES)
    assert not missing, f'缺少用法注释: {sorted(missing)}'
    for name in expected:
        note = INDICATOR_NOTES[name]
        for field in ('meaning', 'params', 'reading', 'caveat'):
            assert note.get(field, '').strip(), f'{name}.{field} 为空'


def test_param_validation_rejects_illegal_values():
    for illegal in (0, -5, 1000):
        with pytest.raises(ValueError):
            validate_param_value('RSI', 'period', illegal)
    with pytest.raises(ValueError):
        validate_param_value('MACD', 'fast_period', -1)
    with pytest.raises(ValueError):
        validate_param_value('MACD', 'fast_period', 12.5)
    assert validate_param_value('RSI', 'period', 7) == 7
    assert validate_param_value('BOLL', 'std_dev', 2.5) == 2.5


def test_param_change_recomputes_macd_and_rsi(tmp_path, monkeypatch):
    monkeypatch.setenv('QUANT_INDICATOR_PARAMS_PATH', str(tmp_path / 'params.json'))
    df = _ohlcv()

    base_macd = ensure_indicator_columns(df, ['MACD'])
    apply_overrides('MACD', {'fast_period': 6, 'slow_period': 13, 'signal_period': 5})
    overrides = load_overrides()
    custom_macd = ensure_indicator_columns(
        df, ['MACD'], overrides=overrides, **build_calc_kwargs(overrides))

    assert not np.allclose(base_macd['macd'].to_numpy(), custom_macd['macd'].to_numpy())
    assert not np.allclose(base_macd['macd_signal'].to_numpy(),
                           custom_macd['macd_signal'].to_numpy())

    base_rsi = ensure_indicator_columns(df, ['RSI'])
    apply_overrides('RSI', {'period': 7})
    overrides = load_overrides()
    custom_rsi = ensure_indicator_columns(
        df, ['RSI'], overrides=overrides, **build_calc_kwargs(overrides))

    assert 'rsi14' in base_rsi.columns
    assert 'rsi7' in custom_rsi.columns
    assert not np.allclose(base_rsi['rsi14'].to_numpy(), custom_rsi['rsi7'].to_numpy())

    # 持久化：文件存在且恢复默认后清空
    assert (tmp_path / 'params.json').exists()
    reset_overrides('MACD')
    reset_overrides('RSI')
    assert load_overrides() == {}


def test_indicators_without_params_are_declared_explicitly():
    """未收录参数的指标在弹窗中显示「该指标无可调参数」，不给假输入框。"""
    from src.ui.main_window_indicator_bar_mixin import IMPLEMENTED_INDICATORS

    for name in ('OBV', 'CCI', 'SAR', 'VOL'):
        assert name in IMPLEMENTED_INDICATORS
        assert name not in PARAM_SPECS
