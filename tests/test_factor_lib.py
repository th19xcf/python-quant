"""因子库回归测试：量纲口径、T+1 收益、分层评估、冗余聚类与窗口算子。"""

from datetime import date, timedelta

import numpy as np
import pandas as pd
import polars as pl

from src.quant.factor_lib import data, evaluate, ops, redundancy, report
from src.quant.factor_lib.factors import load_all
from src.quant.factor_lib.registry import FactorSpec, all_specs, computable_specs


def _dates(start: date, rows: int) -> list[date]:
    return [start + timedelta(days=index) for index in range(rows)]


def _panel(stocks: int = 6, rows: int = 240) -> pl.DataFrame:
    """构造满足因子库列契约的合成面板（后复权价 = 原始价，量纲一致）。"""
    dates = _dates(date(2021, 1, 4), rows)
    rng = np.random.default_rng(20261103)
    frames = []
    for code in range(stocks):
        drift = 0.001 * (code + 1)
        noise = rng.normal(0.0, 0.01, rows)
        close = 10.0 * np.cumprod(1.0 + drift + noise)
        volume = np.full(rows, 1000.0)
        frames.append(pl.DataFrame({
            'date': dates,
            'ts_code': f'{code:06d}.SZ',
            'open': close,
            'high': close,
            'low': close,
            'close': close,
            'raw_open': close,
            'raw_close': close,
            'volume': volume,
            'amount': close * volume * 100.0,
            'hfq_factor': np.full(rows, 1.0),
            'next_tradable': np.full(rows, True),
        }))
    return pl.concat(frames).sort(['ts_code', 'date'])


def test_dimension_check_passes_on_consistent_units():
    panel = _panel()
    result = data.dimension_check(panel)
    assert result['passed'] is True
    assert abs(float(result['median']) - 1.0) < 0.01


def test_dimension_check_flags_volume_unit_change():
    panel = _panel().with_columns((pl.col('volume') * 100.0).alias('volume'))
    result = data.dimension_check(panel)
    assert result['passed'] is False
    assert float(result['median']) < 0.02


def test_forward_returns_starts_at_t_plus_one():
    panel = pl.DataFrame({
        'date': _dates(date(2024, 1, 2), 8),
        'ts_code': ['A'] * 8,
        'open': [10.0 + index for index in range(8)],
    })
    fwd = evaluate.forward_returns(panel, 2)
    # 持有 2 日：t+1 开盘买、t+3 开盘卖 → 13 / 11 - 1
    first = fwd.sort('date').row(0, named=True)['fwd']
    assert first is not None
    assert abs(first - (13.0 / 11.0 - 1.0)) < 1e-12


def test_staged_cross_section_rank_has_no_silent_null():
    panel = _panel(stocks=3, rows=30)
    staged = ops.stage(
        panel, _sig=ops.ts_mean(pl.col('close'), 5),
    )
    staged = ops.stage(staged, _rank=ops.rank_cs(pl.col('_sig')))
    out = ops.out(staged, pl.col('_rank'))
    # 前 4 行因窗口不足为 null，其余必须有值（否则是 polars 窗口嵌套静默 null）
    assert out.null_count() <= 4 * 3
    assert out.drop_nulls().len() > 60


def test_evaluate_factor_recovers_monotonic_signal():
    panel = _panel(stocks=6, rows=240)
    fwd = evaluate.forward_returns(panel, 2)
    values = panel.join(fwd, on=['date', 'ts_code'], how='left')
    spec = FactorSpec(
        name='sig', category='test', expression='fwd', dependencies='',
        direction='+', source='test', status='verified', lookback=3,
    )
    config = evaluate.EvalConfig(horizon=2, n_groups=3, min_names=6, min_periods=6)
    dates = evaluate.rebalance_dates(panel, 2)
    result = evaluate.evaluate_factor(
        values.with_columns(pl.col('fwd').alias('sig')), fwd, spec, config, dates,
        (0.0005, 0.001),
    )
    assert 'error' not in result
    assert float(result['mean_ic']) > 0.9
    assert float(result['q5_minus_q1']) > 0
    assert float(result['monotonicity']) >= 0.9


def test_clustering_keeps_highest_icir_per_cluster():
    corr = pd.DataFrame(
        [[1.0, 0.9, 0.0], [0.9, 1.0, 0.0], [0.0, 0.0, 1.0]],
        index=['high', 'low', 'solo'], columns=['high', 'low', 'solo'],
    )
    metrics = pl.DataFrame({
        'factor': ['high', 'low', 'solo'],
        'icir': [1.0, 0.5, 0.2],
    })
    clusters = redundancy.build_clusters(corr, metrics)
    assert len(clusters.kept) == 2
    assert 'high' in clusters.kept and 'solo' in clusters.kept
    assert 'low' in clusters.dropped


def test_registry_marks_unavailable_factors():
    load_all()
    specs = all_specs()
    assert len(specs) >= 150
    assert len(computable_specs()) >= 150
    unavailable = [spec for spec in specs if spec.status == 'unavailable']
    assert unavailable  # 市值/账面/行业数据缺失的因子必须显式登记为不可用


def test_headline_is_plain_conclusion_first():
    spec = FactorSpec(
        name='a101_042', category='alpha101', expression='x', dependencies='',
        direction='+', source='test', status='verified',
    )
    row = {'icir_annual': 1.6, 't_stat': 3.5, 'monotonicity': 0.9}
    text = report.build_headline([(spec, row)])
    assert not text.startswith(('a101', 'gtja', 'qlib', 'ff_', 'IC'))
    assert '赚钱' in text
    assert '噪音' in text
