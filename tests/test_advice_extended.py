#!/usr/bin/env python3

"""扩展因子集配置、缺失处理、软筛选与未来函数的单测。"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest

from src.advice.config import TIER_PRESETS, default_config
from src.advice.extended_factors import compute_extended_candidates
from src.advice.factor_eval import (
    EvalConfig,
    FactorMetrics,
    SimConfig,
    _walk_forward_windows,
    add_forward_returns,
    evaluate_factor,
    selection_weights,
)
from src.advice.factor_sets import (
    DISABLED_FACTORS,
    enabled_extended_columns,
)
from src.advice.fundamental_factors import (
    build_fundamentals,
    ttm_from_cumulative,
)
from src.advice.ic_monitor import rolling_ic_series, run_ic_monitor
from src.advice.scoring import attach_score
from src.advice.soft_filters import apply_soft_filter
from src.backtest.costs.cost_model import CostModel


def _synthetic_panel(days: int = 80, codes: int = 8) -> pl.DataFrame:
    rows = []
    base = date(2025, 1, 2)
    for code_index in range(codes):
        price = 10.0 + code_index
        for day in range(days):
            drift = np.sin(day / 7.0 + code_index) * 0.02
            price = max(1.0, price * (1.0 + drift))
            current = base + timedelta(days=day)
            rows.append({
                'date': current.isoformat(),
                'ts_code': f'{code_index:06d}.SZ',
                'open': price * 0.995,
                'high': price * 1.01,
                'low': price * 0.99,
                'close': price,
                'volume': 1_000_000 + code_index * 10_000 + day * 1_000,
                'amount': price * (1_000_000 + code_index * 10_000 + day * 1_000),
                'raw_open': price * 0.995,
                'raw_close': price,
                'raw_prev_close': price * 0.99,
                'history_rows': day + 1,
            })
    return pl.DataFrame(rows)


def test_extended_config_weights_and_size() -> None:
    config = default_config()
    assert config.factor_set == 'extended'
    for tier in config.tiers.values():
        total = sum(abs(spec.weight) for spec in tier.factors)
        assert abs(total - 1.0) < 1e-9
        assert all(spec.weight > 0 for spec in tier.factors)
    columns = enabled_extended_columns()
    assert len(columns) == len(set(columns))
    assert 12 <= len(columns) <= 50
    assert not (set(columns) & DISABLED_FACTORS)


def test_legacy_switch_keeps_original_factors() -> None:
    legacy = default_config('legacy', soft_filter_enabled=False)
    assert legacy.factor_set == 'legacy'
    assert list(legacy.tiers) == list(TIER_PRESETS)
    for key, tier in legacy.tiers.items():
        assert tier.factors == TIER_PRESETS[key].factors
    extended = default_config()
    ext_columns = {spec.column for spec in extended.tiers['mid'].factors}
    assert ext_columns - {spec.column for spec in TIER_PRESETS['mid'].factors}


def test_extended_columns_compute_without_error() -> None:
    panel = _synthetic_panel(days=300, codes=5)
    frame = compute_extended_candidates(panel)
    for column in enabled_extended_columns():
        assert column in frame.columns
    # 最新一日（窗口足够长）不应整列为空
    latest = frame.filter(pl.col('date') == frame['date'].max())
    for column in enabled_extended_columns():
        if column.startswith('ext_mcst') or column.startswith('ext_cyc'):
            continue
        assert latest[column].is_not_null().sum() > 0, column


def _frame_with_tier_columns(panel: pl.DataFrame, tier) -> pl.DataFrame:
    """给合成面板补齐档位所需因子列（确定性、随票/随日变化）。"""
    code_id = pl.col('ts_code').str.slice(0, 6).cast(pl.Int64)
    day_id = pl.col('date').str.slice(-2).cast(pl.Int64)
    return panel.with_columns([
        (code_id * 0.1 + day_id * 0.01 + index * 0.02).alias(spec.column)
        for index, spec in enumerate(tier.factors)
    ])


def test_reweight_missing_is_not_zero_and_short_history_masked() -> None:
    frame = _synthetic_panel(days=100, codes=6)
    tier = default_config().tiers['mid']
    frame = _frame_with_tier_columns(frame, tier)
    scored = attach_score(frame, tier, missing_policy='reweight',
                          min_history_rows=60)
    latest = scored.filter(pl.col('date') == frame['date'].max())
    assert latest['score'].is_not_null().sum() >= 1
    # 历史不足的票被置空，不参与打分
    short = scored.filter(pl.col('history_rows') < 60)
    assert short['score'].is_null().all()
    # 缺失不填 0：某列某票置空后，分数仍由其它因子归一得到
    target = frame['ts_code'][0]
    latest_date = frame['date'].max()
    column = tier.factors[0].column
    one = frame.with_columns(
        pl.when((pl.col('ts_code') == target) & (pl.col('date') == latest_date))
        .then(None).otherwise(pl.col(column)).alias(column))
    scored_one = attach_score(one, tier, missing_policy='reweight')
    last = scored_one.filter((pl.col('date') == latest_date)
                             & (pl.col('ts_code') == target))
    assert last['score'][0] is not None
    assert abs(last['score'][0]) > 1e-6


def test_attach_score_reports_missing_extended_columns() -> None:
    """扩展档位缺列时必须明确报错并列出列名，不得静默降级。"""
    frame = _synthetic_panel(days=20, codes=3)
    tier = default_config().tiers['mid']
    missing = [spec.column for spec in tier.factors
               if spec.column not in frame.columns]
    assert missing
    with pytest.raises(ValueError) as exc:
        attach_score(frame, tier, missing_policy='reweight')
    message = str(exc.value)
    assert '缺少因子列' in message
    for column in missing:
        assert column in message


def test_score_uses_no_future_data() -> None:
    frame = _synthetic_panel(days=80, codes=5)
    tier = default_config().tiers['mid']
    frame = _frame_with_tier_columns(frame, tier)
    original = attach_score(frame, tier, missing_policy='reweight',
                            min_history_rows=60)
    cutoff = frame['date'].unique().sort()[60]
    mutated = frame.with_columns(
        pl.when(pl.col('date') > cutoff)
        .then(pl.col('close') * 3.0)
        .otherwise(pl.col('close')).alias('close'))
    changed = attach_score(mutated, tier, missing_policy='reweight',
                           min_history_rows=60)
    before = original.filter(pl.col('date') <= cutoff).sort(['ts_code', 'date'])
    after = changed.filter(pl.col('date') <= cutoff).sort(['ts_code', 'date'])
    assert np.allclose(before['score'].to_numpy(), after['score'].to_numpy(),
                       equal_nan=True)


def test_soft_filter_thresholds_and_missing_fail() -> None:
    frame = _synthetic_panel(days=100, codes=10)
    # 构造可区分的动量/波动/RSI
    frame = frame.with_columns([
        pl.col('close').pct_change().over('ts_code').alias('__ret'),
    ]).with_columns([
        (pl.col('close') / pl.col('close').shift(60).over('ts_code') - 1.0)
        .alias('ma20_gap'),
        (pl.col('close') / pl.col('close').shift(60).over('ts_code') - 1.0)
        .alias('ma60_gap'),
        pl.col('__ret').rolling_mean(window_size=20, min_samples=20)
        .over('ts_code').alias('volume_momentum_20'),
        pl.col('__ret').abs().rolling_mean(window_size=14, min_samples=14)
        .over('ts_code').alias('atr_pct'),
        (pl.lit(50.0) + pl.col('__ret') * 1000).alias('rsi_14'),
    ]).drop('__ret')
    config = default_config().soft_filter
    filtered = apply_soft_filter(frame, config)
    latest = filtered.filter(pl.col('date') == filtered['date'].max())
    assert 'soft_pass' in latest.columns
    assert latest.filter(pl.col('soft_pass')).height > 0
    # 缺失条件 -> 不通过
    broken = latest.with_columns(pl.lit(None, dtype=pl.Float64).alias('rsi_14'))
    filtered_broken = apply_soft_filter(broken, config)
    assert filtered_broken['soft_pass'].sum() == 0


def test_weights_sum_to_one_and_direction_applied() -> None:
    config = default_config()
    for tier in config.tiers.values():
        weighted = sum(spec.weight * abs(spec.direction) for spec in tier.factors)
        assert weighted == pytest.approx(1.0)
        assert all(spec.weight > 0 for spec in tier.factors)


def test_walk_forward_windows_have_purge_and_embargo() -> None:
    """训练窗终点与测试窗起点之间必须留出 horizon 的标签隔离带。"""
    dates = [date(2020, 1, 1) + timedelta(days=index) for index in range(600)]
    for horizon, embargo in ((20, 20), (5, 5)):
        config = EvalConfig(horizon=horizon, embargo=embargo,
                            train_months=24, test_months=6)
        bounds = _walk_forward_windows(dates, config)
        assert bounds
        for _train_start, train_end, test_start, _test_end in bounds:
            gap = dates.index(test_start) - dates.index(train_end)
            # 训练标签占 [t+1, t+horizon]，因此 gap 至少 horizon + 2
            assert gap >= horizon + 2


def test_simulation_costs_come_from_backtest_cost_model() -> None:
    """评估侧成本必须复用回测内核默认参数，--stress 只翻倍滑点。"""
    baseline = SimConfig(top_k=20, hold_days=20).make_cost_model().params
    assert baseline == CostModel().params
    stressed = SimConfig(top_k=20, hold_days=20, stress=True).make_cost_model().params
    assert stressed['slippage'] == pytest.approx(baseline['slippage'] * 2.0)
    changed = {
        key for key in baseline
        if stressed[key] != baseline[key]
    }
    assert changed == {'slippage'}


def _statement_frame(rows: list[dict]) -> pl.DataFrame:
    schema = {'ts_code': pl.Utf8, 'end_date': pl.Date, 'ann_date': pl.Date,
              'revenue': pl.Float64, 'parent_net_profit': pl.Float64,
              'total_assets': pl.Float64, 'total_equity': pl.Float64}
    frame = pl.DataFrame(rows, schema_overrides=schema, orient='row')
    return frame


def test_ttm_from_cumulative_uses_year_to_date_math() -> None:
    """累计口径 TTM = 本期 + 上年年报 - 上年同期；Q4 即年报值。"""
    rows = []
    cumulative = {
        date(2022, 3, 31): 12.0, date(2022, 6, 30): 28.0,
        date(2022, 9, 30): 45.0, date(2022, 12, 31): 60.0,
        date(2023, 3, 31): 10.0, date(2023, 6, 30): 25.0,
        date(2023, 9, 30): 40.0,
    }
    for index, (end_date, value) in enumerate(sorted(cumulative.items())):
        rows.append({'ts_code': '000001.SZ', 'end_date': end_date,
                     'ann_date': end_date + timedelta(days=25),
                     'revenue': value * 10.0, 'parent_net_profit': value,
                     'total_assets': None, 'total_equity': None})
    frame = _statement_frame(rows)
    ttm = ttm_from_cumulative(frame, ('revenue', 'parent_net_profit'))
    by_end = {row['end_date']: row['parent_net_profit__ttm']
              for row in ttm.to_dicts()}
    assert by_end[date(2022, 12, 31)] == pytest.approx(60.0)
    assert by_end[date(2023, 3, 31)] == pytest.approx(10.0 + 60.0 - 12.0)
    assert by_end[date(2023, 6, 30)] == pytest.approx(25.0 + 60.0 - 28.0)
    assert by_end[date(2023, 9, 30)] == pytest.approx(40.0 + 60.0 - 45.0)


def test_fundamentals_respect_ann_date_and_cap_ratios() -> None:
    """财务可见性按 ann_date 生效；bp/ep 用总市值，换手用流通股本。"""
    dates = [date(2023, 10, 20) + timedelta(days=index) for index in range(20)]
    panel = pl.DataFrame({
        'ts_code': ['000001.SZ'] * len(dates),
        'date': dates,
        'volume': [10_000.0] * len(dates),
    })
    valuation = pl.DataFrame({
        'ts_code': ['000001.SZ'] * len(dates),
        'trade_date': dates,
        'total_mv': [1.0e10] * len(dates),
        'circ_mv': [8.0e9] * len(dates),
        'dv_ratio': [2.0] * len(dates),
    })
    shares = pl.DataFrame({
        'ts_code': ['000001.SZ'],
        'effective_date': [date(2020, 1, 1)],
        'float_share': [100_000_000],
    })
    income = _statement_frame([
        {'ts_code': '000001.SZ', 'end_date': date(2022, 12, 31),
         'ann_date': date(2023, 3, 20), 'revenue': 500.0,
         'parent_net_profit': 50.0, 'total_assets': None, 'total_equity': None},
        {'ts_code': '000001.SZ', 'end_date': date(2022, 9, 30),
         'ann_date': date(2022, 10, 25), 'revenue': 450.0,
         'parent_net_profit': 45.0, 'total_assets': None, 'total_equity': None},
        {'ts_code': '000001.SZ', 'end_date': date(2023, 9, 30),
         'ann_date': date(2023, 10, 26), 'revenue': 600.0,
         'parent_net_profit': 60.0, 'total_assets': None, 'total_equity': None},
    ])
    balance = _statement_frame([
        {'ts_code': '000001.SZ', 'end_date': date(2022, 12, 31),
         'ann_date': date(2023, 3, 20), 'revenue': None,
         'parent_net_profit': None, 'total_assets': 9.0e10,
         'total_equity': 1.0e9},
        {'ts_code': '000001.SZ', 'end_date': date(2022, 9, 30),
         'ann_date': date(2022, 10, 25), 'revenue': None,
         'parent_net_profit': None, 'total_assets': 8.0e10,
         'total_equity': 1.0e9},
        {'ts_code': '000001.SZ', 'end_date': date(2023, 9, 30),
         'ann_date': date(2023, 10, 26), 'revenue': None,
         'parent_net_profit': None, 'total_assets': 1.1e11,
         'total_equity': 2.0e9},
    ])

    out = build_fundamentals(panel, valuation, shares, income, balance)
    by_date = {row['date']: row for row in out.to_dicts()}

    before = by_date[date(2023, 10, 24)]   # 2023Q3 尚未公告
    assert before['ff_value_bp'] == pytest.approx(1.0e9 / 1.0e10)
    assert before['ff_value_ep'] == pytest.approx(50.0 / 1.0e10)
    assert before['ff_investment'] is None  # 缺上年同期总资产

    after = by_date[date(2023, 11, 5)]     # 2023Q3 已公告且换手窗口已满
    assert after['ff_value_bp'] == pytest.approx(2.0e9 / 1.0e10)
    assert after['ff_value_ep'] == pytest.approx((60.0 + 50.0 - 45.0) / 1.0e10)
    assert after['ff_value_ep'] != before['ff_value_ep']
    assert after['ff_investment'] == pytest.approx(1.1e11 / 8.0e10 - 1.0)
    assert after['ff_size'] == pytest.approx(np.log(8.0e9))
    assert after['ff_div_yield'] == pytest.approx(0.02)
    assert after['ext_turnover_20'] == pytest.approx(0.01)


def test_fundamentals_same_day_annual_and_q1_prefers_latest_period() -> None:
    """年报与一季报同日披露时，TTM 取最新报告期（Q1），而不是较早的年报。"""
    panel = pl.DataFrame({
        'ts_code': ['000001.SZ'],
        'date': [date(2023, 5, 10)],
        'volume': [10_000.0],
    })
    valuation = pl.DataFrame({
        'ts_code': ['000001.SZ'],
        'trade_date': [date(2023, 5, 10)],
        'total_mv': [1.0e10],
        'circ_mv': [8.0e9],
        'dv_ratio': [2.0],
    })
    shares = pl.DataFrame({
        'ts_code': ['000001.SZ'],
        'effective_date': [date(2020, 1, 1)],
        'float_share': [100_000_000],
    })
    income = _statement_frame([
        {'ts_code': '000001.SZ', 'end_date': date(2021, 12, 31),
         'ann_date': date(2022, 3, 30), 'revenue': 400.0,
         'parent_net_profit': 40.0, 'total_assets': None, 'total_equity': None},
        {'ts_code': '000001.SZ', 'end_date': date(2022, 3, 31),
         'ann_date': date(2022, 4, 28), 'revenue': 100.0,
         'parent_net_profit': 10.0, 'total_assets': None, 'total_equity': None},
        {'ts_code': '000001.SZ', 'end_date': date(2022, 12, 31),
         'ann_date': date(2023, 4, 28), 'revenue': 500.0,
         'parent_net_profit': 50.0, 'total_assets': None, 'total_equity': None},
        {'ts_code': '000001.SZ', 'end_date': date(2023, 3, 31),
         'ann_date': date(2023, 4, 28), 'revenue': 120.0,
         'parent_net_profit': 12.0, 'total_assets': None, 'total_equity': None},
    ])
    balance = _statement_frame([
        {'ts_code': '000001.SZ', 'end_date': date(2022, 12, 31),
         'ann_date': date(2023, 4, 28), 'revenue': None,
         'parent_net_profit': None, 'total_assets': 9.0e10,
         'total_equity': 1.0e9},
        {'ts_code': '000001.SZ', 'end_date': date(2023, 3, 31),
         'ann_date': date(2023, 4, 28), 'revenue': None,
         'parent_net_profit': None, 'total_assets': 9.5e10,
         'total_equity': 1.2e9},
    ])

    row = build_fundamentals(panel, valuation, shares, income, balance).to_dicts()[0]
    # 取 Q1 TTM：120 + 500 - 100 = 520；若误取年报则为 500
    assert row['ff_sales_yield'] == pytest.approx(520.0 / 1.0e10)
    assert row['ff_value_ep'] == pytest.approx((12.0 + 50.0 - 10.0) / 1.0e10)
    assert row['ff_value_bp'] == pytest.approx(1.2e9 / 1.0e10)


def _scored_panel(days: int = 140, codes: int = 40) -> pl.DataFrame:
    """分数 = -(未来 5 日收益)，刻意制造负 IC 以验证告警。"""
    rng = np.random.default_rng(7)
    base = date(2025, 1, 2)
    steps = rng.normal(0.0, 0.02, size=(codes, days))
    prices = 10.0 * np.cumprod(1.0 + steps, axis=1)
    forward = np.full((codes, days), np.nan)
    forward[:, :-5] = prices[:, 5:] / prices[:, :-5] - 1.0

    frame = pl.DataFrame({
        'date': [base + timedelta(days=day) for _code in range(codes)
                 for day in range(days)],
        'ts_code': [f'{code:06d}.SZ' for code in range(codes)
                    for _day in range(days)],
        'close': [float(prices[code, day])
                  for code in range(codes) for day in range(days)],
        'score': [-float(forward[code, day])
                  for code in range(codes) for day in range(days)],
    })
    return frame


def test_ic_monitor_alerts_on_consecutive_negative_windows(tmp_path) -> None:
    """连续 3 个滚动窗口 IC 为负时监控必须告警并落盘快照。"""
    scored = _scored_panel()
    windows, values = rolling_ic_series(scored, horizon=5, window_days=20)
    assert len(values) == 7
    assert all(value < 0 for value in values[-3:])

    snapshot = run_ic_monitor(scored, 'short', trade_date=date(2025, 5, 20),
                              horizon=5, window_days=20, out_dir=tmp_path)
    assert snapshot['status']['failed'] is True
    assert '连续' in snapshot['status']['reason'] or '负' in snapshot['status']['reason']
    assert (tmp_path / '20250520_short.json').exists()
    assert (tmp_path / 'history.jsonl').exists()


def test_ic_monitor_normal_series_does_not_alert(tmp_path) -> None:
    """正 IC 序列不应误报。"""
    scored = _scored_panel().with_columns((-pl.col('score')).alias('score'))
    snapshot = run_ic_monitor(scored, 'short', trade_date=date(2025, 5, 20),
                              horizon=5, window_days=20, out_dir=tmp_path)
    assert snapshot['status']['failed'] is False


def test_ic_monitor_ignores_trailing_partial_window(tmp_path) -> None:
    """尾部只剩 3 天的残窗不得参与判定，避免被单日噪声误触发告警。"""
    scored = _scored_panel(days=68)
    scored = scored.with_columns((-pl.col('score')).alias('score'))
    dates = sorted(scored['date'].unique().to_list())
    tail = dates[-8:-5]  # 最后 5 天标签尚未实现，倒数第 6~8 天是最后 3 个可算日
    scored = scored.with_columns(
        pl.when(pl.col('date').is_in(tail)).then(-pl.col('score'))
        .otherwise(pl.col('score')).alias('score'))

    windows, values = rolling_ic_series(scored, horizon=5, window_days=20)
    assert len(windows) == 4
    assert windows[-1]['days'] == 3
    assert len(values) == 3
    assert all(value > 0 for value in values)

    snapshot = run_ic_monitor(scored, 'short', trade_date=date(2025, 5, 20),
                              horizon=5, window_days=20, out_dir=tmp_path)
    assert snapshot['status']['failed'] is False
    assert snapshot['n_windows'] == 4
    assert snapshot['n_usable_windows'] == 3


def _ic_frame(sign: float, days: int = 12, codes: int = 30) -> pl.DataFrame:
    """构造与未来 5 日收益同向/反向的因子（sign=+1 正 IC，-1 负 IC）。"""
    frame = _scored_panel(days=days, codes=codes).select(['date', 'ts_code', 'close'])
    frame = add_forward_returns(frame, [5])
    return frame.with_columns((sign * pl.col('fwd_5')).alias('f'))


def _eval_item(frame: pl.DataFrame, direction: float) -> FactorMetrics:
    dates = sorted(frame['date'].unique().to_list())
    return evaluate_factor(frame, 'f', 'other', direction, 5, dates)


def test_selection_weights_follow_raw_ic_sign() -> None:
    """权重符号必须来自训练窗原始 IC：正 IC 取正、负 IC 取负。"""
    positive = _eval_item(_ic_frame(1.0), 1.0)
    negative = _eval_item(_ic_frame(-1.0), 1.0)
    assert positive.ic_mean > 0 and positive.ic_mean_raw > 0
    assert negative.ic_mean < 0 and negative.ic_mean_raw < 0
    assert selection_weights([positive], {}, new_share=0.7)['f'] == pytest.approx(0.7)
    assert selection_weights([negative], {}, new_share=0.7)['f'] == pytest.approx(-0.7)


def test_selection_weights_use_raw_sign_when_aligned_positive() -> None:
    """回归用例：对齐后 ic_mean 恒正、但原始 IC 为负时，权重必须为负号。"""
    item = _eval_item(_ic_frame(-1.0), -1.0)
    assert item.ic_mean > 0  # 已按登记方向对齐
    assert item.ic_mean_raw < 0
    assert selection_weights([item], {}, new_share=0.7)['f'] == pytest.approx(-0.7)


def test_default_tiers_exclude_observe_only_short() -> None:
    """默认档位集合不含 short；TIER_PRESETS 仍保留其配置供回放/复核。"""
    from src.advice.config import OBSERVE_ONLY_TIERS, default_output_tiers

    assert 'short' in TIER_PRESETS
    assert 'short' in OBSERVE_ONLY_TIERS
    assert default_output_tiers() == ['mid', 'long']
    assert default_output_tiers(default_config().tiers) == ['mid', 'long']


def test_cli_default_tiers_exclude_short() -> None:
    from src.advice.cli import build_parser

    assert build_parser().parse_args([]).tiers == ['mid', 'long']


def test_explicit_short_request_is_marked_observe_only() -> None:
    """显式请求 short 不报错：产出带 observe_only=True 的标记，数值原样保留。"""
    from src.advice.cli import mark_observe_only

    marked = mark_observe_only({
        'short': {'label': '短线', 'counts': {}},
        'mid': {'label': '中线', 'counts': {}},
    })
    assert marked['short']['observe_only'] is True
    assert marked['mid']['observe_only'] is False
    assert marked['short']['label'] == '短线'


def test_ui_segmented_control_hides_short() -> None:
    from src.ui.advice.widgets import SegmentedControl

    keys = [item[0] for item in SegmentedControl.ITEMS]
    assert keys == ['mid', 'long']
    assert 'short' not in keys


if __name__ == '__main__':
    raise SystemExit(pytest.main([__file__]))
