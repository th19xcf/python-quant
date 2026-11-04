"""筹码分布算法单测：箱位随可见区间重算、全区间覆盖全历史、A/B 衰减口径。"""

import inspect

import numpy as np
import polars as pl
import pytest

from src.tech_analysis.chip import (
    AUTO_FLOOR_BOUNDS,
    AUTO_FLOOR_MODE,
    DEFAULT_TURNOVER_FLOOR,
    DEFAULT_TURNOVER_FLOOR_MODE,
    TURNOVER_FLOOR_PRESETS,
    compute_chip_profile,
    decay_rates,
    resolve_turnover_floor,
)


def _make_frame(days: int = 80) -> pl.DataFrame:
    """构造两段价格区间的日线，保证筹码在不同价位都有分布。"""
    close = np.concatenate(
        [
            np.full(days // 2, 12.0),
            np.full(days - days // 2, 18.0),
        ]
    )
    volume = np.full(days, 1000.0)
    return pl.DataFrame(
        {
            'date': pl.date_range(
                start=pl.date(2026, 1, 1),
                end=pl.date(2026, 4, 11),
                interval='1d',
                eager=True,
            ).head(days),
            'open': close,
            'high': close * 1.02,
            'low': close * 0.98,
            'close': close,
            'volume': volume,
        }
    )


def test_visible_range_rebins_chip_boxes():
    """裁剪模式下箱边界收敛到可视价格区间，箱间距随之改变。"""
    frame = _make_frame()
    full = compute_chip_profile(frame, price_range=None)
    visible = (13.5, 14.5)
    cropped = compute_chip_profile(frame, price_range=visible)

    assert full.price_range == full.full_range
    low, high = cropped.price_range
    assert 13.5 - 1e-9 <= low <= 13.5 + 1e-9
    assert high == 14.5 or high < 14.5 + 1e-9
    assert cropped.edges.shape[0] == cropped.bins + 1
    # 同样的箱数摊到更窄的价格区间 -> 单箱价格跨度更小
    crop_step = cropped.edges[1] - cropped.edges[0]
    full_step = full.edges[1] - full.edges[0]
    assert crop_step < full_step

    # 换一个可见区间，箱位必须跟着重算
    other = compute_chip_profile(frame, price_range=(17.5, 18.5))
    assert 17.5 - 1e-9 <= other.price_range[0] <= 17.5 + 1e-9
    assert not np.allclose(other.edges, cropped.edges)


def test_full_range_covers_history():
    """全区间模式覆盖全历史价格范围，柱长合计约等于 100%。"""
    frame = _make_frame()
    full = compute_chip_profile(frame, price_range=None)
    expected = (float(frame['low'].min()), float(frame['high'].max()))
    assert np.isclose(full.price_range[0], expected[0])
    assert np.isclose(full.price_range[1], expected[1])
    assert np.isclose(full.shares.sum(), 100.0, atol=1e-6)

    cropped = compute_chip_profile(frame, price_range=(11.8, 12.2))
    # 裁剪模式下老筹码被裁掉，可见柱长合计小于全历史 100%
    assert cropped.shares.sum() < 100.0


def test_turn_missing_falls_back_to_proxy():
    """A 档在 turn 整列缺失/全空时逐行回退，结果与 B 档完全一致。"""
    frame = _make_frame()
    null_turn = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias('turn'))
    a = compute_chip_profile(null_turn, turnover_source='turn')
    b = compute_chip_profile(frame, turnover_source='proxy')
    assert a.turn_source_ratio == 0.0
    assert np.allclose(a.shares, b.shares)
    assert np.allclose(a.edges, b.edges)
    # 部分行有 turn 时只对缺失行回退，真实行占比正确
    mixed = frame.with_columns(
        pl.when(pl.int_range(0, frame.height) < 40)
        .then(pl.lit(3.0))
        .otherwise(None)
        .cast(pl.Float64)
        .alias('turn')
    )
    partial = compute_chip_profile(mixed, turnover_source='turn')
    assert partial.turn_source_ratio == pytest.approx(40 / 80)


def test_turn_floor_is_parameterized_and_zero_turn_never_decays():
    """下限三档可参数化；turn=0 一律不衰减，不被下限抬起。"""
    frame = _make_frame(days=5).with_columns(
        pl.Series('turn', [0.0, 0.1, 0.2, 5.0, 20.0])
    )
    d_raw, warmup, stats_raw = decay_rates(
        frame, turnover_source='turn', turnover_floor=None
    )
    d_02, _, stats_02 = decay_rates(
        frame, turnover_source='turn', turnover_floor=0.2
    )
    d_2, _, stats_2 = decay_rates(
        frame, turnover_source='turn', turnover_floor=2.0
    )

    # 零换手行三档都是 0，不允许被下限强撑
    assert d_raw[0] == d_02[0] == d_2[0] == 0.0
    assert np.allclose(d_raw, [0.0, 0.001, 0.002, 0.05, 0.10])
    assert np.allclose(d_02, [0.0, 0.002, 0.002, 0.05, 0.10])
    assert np.allclose(d_2, [0.0, 0.02, 0.02, 0.05, 0.10])
    assert warmup == 0

    # clamped_ratio 分母为有效 turn 行（5 行），区分下限抬起/上限压住
    assert stats_raw.clamped_low_ratio == 0.0
    assert stats_raw.clamped_high_ratio == pytest.approx(0.2)
    assert stats_02.clamped_low_ratio == pytest.approx(0.2)
    assert stats_02.clamped_high_ratio == pytest.approx(0.2)
    assert stats_02.clamped_ratio == pytest.approx(0.4)
    assert stats_2.clamped_low_ratio == pytest.approx(0.4)
    assert stats_2.clamped_high_ratio == pytest.approx(0.2)
    assert stats_2.clamped_ratio == pytest.approx(0.6)
    assert stats_02.turn_source_ratio == 1.0


def test_invalid_turnover_floor_raises():
    """下限越界立即报错，避免静默产生异常衰减。"""
    frame = _make_frame()
    with pytest.raises(ValueError, match='turnover_floor'):
        decay_rates(frame, turnover_source='turn', turnover_floor=20.0)
    with pytest.raises(ValueError, match='turnover_floor'):
        decay_rates(frame, turnover_source='turn', turnover_floor='unknown')


def test_default_floor_mode_is_auto_p10():
    """代码默认与计算默认参数必须同为 auto_p10，避免 UI 与实现不一致。"""
    assert DEFAULT_TURNOVER_FLOOR_MODE == 'auto_p10'
    assert TURNOVER_FLOOR_PRESETS[DEFAULT_TURNOVER_FLOOR_MODE] == AUTO_FLOOR_MODE
    assert DEFAULT_TURNOVER_FLOOR == AUTO_FLOOR_MODE
    default = inspect.signature(compute_chip_profile).parameters['turnover_floor'].default
    assert default == AUTO_FLOOR_MODE
    default_source = inspect.signature(decay_rates).parameters['turnover_floor'].default
    assert default_source == AUTO_FLOOR_MODE


def test_auto_p10_matches_real_p50_for_ultra_low_turnover():
    """工行型超低换手：auto p10 落到保护下界 0.02%，有效衰减中位等于真实 p50。"""
    turn = np.concatenate([
        np.full(15, 0.0175),
        np.full(34, 0.0400),
        np.full(51, 0.0714),
    ])
    frame = _make_frame(days=100).with_columns(pl.Series('turn', turn))
    d, _, stats = decay_rates(frame, 'turn', AUTO_FLOOR_MODE)
    assert stats.turnover_floor == AUTO_FLOOR_BOUNDS[0]
    assert np.median(d) == pytest.approx(np.median(turn[turn > 0]) / 100, rel=1e-6)
    assert np.median(d) == pytest.approx(0.000714, rel=1e-6)
    assert stats.clamped_low_ratio == pytest.approx(0.15)

    # ChipProfile 记录的是解析后的实际下限，而不是档位名称
    low_profile = compute_chip_profile(frame, turnover_floor=AUTO_FLOOR_MODE)
    assert low_profile.turnover_floor == AUTO_FLOOR_BOUNDS[0]
    mid_frame = _make_frame(days=100).with_columns(
        pl.Series('turn', np.full(100, 0.1))
    )
    mid_profile = compute_chip_profile(mid_frame, turnover_floor=AUTO_FLOOR_MODE)
    assert mid_profile.turnover_floor == pytest.approx(0.1)
    assert low_profile.turnover_floor != mid_profile.turnover_floor


def test_auto_p10_floor_uses_positive_turn_and_clips_bounds():
    """auto_p10 只用正换手率算 p10，并夹在 0.02%~0.2% 保护区间。"""
    low = np.array([0.0, 0.001, 0.002, 0.003, 0.004])
    assert resolve_turnover_floor(AUTO_FLOOR_MODE, low) == AUTO_FLOOR_BOUNDS[0]

    high = np.linspace(1.0, 100.0, 100)
    assert resolve_turnover_floor(AUTO_FLOOR_MODE, high) == AUTO_FLOOR_BOUNDS[1]

    middle = np.array([0.0, 0.05, 0.05, 0.05, 0.05, 0.05])
    assert resolve_turnover_floor(AUTO_FLOOR_MODE, middle) == pytest.approx(0.05)

    assert resolve_turnover_floor(AUTO_FLOOR_MODE, np.array([0.0, np.nan])) is None
    assert resolve_turnover_floor(0.05, low) == 0.05
    assert resolve_turnover_floor(None, low) is None

    frame = _make_frame(days=5).with_columns(
        pl.Series('turn', [0.0, 0.01, 0.2, 5.0, 20.0])
    )
    d, _, stats = decay_rates(frame, turnover_source='turn',
                              turnover_floor=AUTO_FLOOR_MODE)
    assert stats.turnover_floor is not None
    assert AUTO_FLOOR_BOUNDS[0] <= stats.turnover_floor <= AUTO_FLOOR_BOUNDS[1]
    # 零换手仍严格不衰减
    assert d[0] == 0.0


def test_proxy_source_ignores_turn_column():
    """B 档必须只用 vol/MA20，不得被已附加的 turn 列影响。"""
    frame = _make_frame()
    with_turn = frame.with_columns(pl.lit(8.0).alias('turn'))
    proxy_plain = compute_chip_profile(frame, turnover_source='proxy')
    proxy_with_turn = compute_chip_profile(with_turn, turnover_source='proxy')
    assert np.allclose(proxy_plain.shares, proxy_with_turn.shares)
    assert proxy_with_turn.turn_source_ratio == 0.0


def test_ab_switch_equivalent_when_turn_matches_proxy():
    """turn 取代理 d×100 时，A/B 两档应得到同一条筹码分布（切换不走样）。"""
    frame = _make_frame()
    proxy_d, _, _ = decay_rates(frame, turnover_source='proxy')
    frame_with_turn = frame.with_columns(
        pl.Series('turn', proxy_d * 100.0)
    )
    a = compute_chip_profile(frame_with_turn, turnover_source='turn')
    b = compute_chip_profile(frame, turnover_source='proxy')
    assert a.turn_source_ratio == 1.0
    assert np.allclose(a.shares, b.shares)


def test_unknown_turnover_source_raises():
    """未知口径立即报错，避免静默走错档位。"""
    frame = _make_frame()
    with pytest.raises(ValueError, match='turnover_source'):
        compute_chip_profile(frame, turnover_source='noop')
