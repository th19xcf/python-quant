#!/usr/bin/env python3

"""自学习第一阶段：切分、标签、权重学习器与评估器测试。"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest

from src.quant.learning.config import WalkForwardConfig
from src.quant.learning.evaluator import (
    ic_stats,
    quantile_analysis,
    selection_sets,
    simulate_long_only,
    turnover_from_sets,
)
from src.quant.learning.labels import (
    LABEL_COL,
    add_adjusted_prices,
    attach_forward_labels,
)
from src.quant.learning.splits import make_walk_forward_windows
from src.quant.learning.walk_forward import build_learning_report, prepare_panel
from src.quant.learning.weights import ICWeightLearner, RidgeWeightLearner


def _dates(count: int, start: date = date(2023, 1, 2)) -> list[date]:
    return [start + timedelta(days=index) for index in range(count)]


def _synthetic_panel(n_codes: int = 25, n_days: int = 240,
                     seed: int = 42) -> pl.DataFrame:
    """构造含未来收益可预测关系的合成面板。"""
    rng = np.random.default_rng(seed)
    days = _dates(n_days)
    rows: list[dict[str, object]] = []
    for code_index in range(n_codes):
        code = f'{600000 + code_index:06d}.SH'
        price = 10.0 + code_index
        latent = rng.normal(0, 1, size=n_days)
        lagged = np.roll(latent, 1)
        lagged[0] = 0.0
        for day_index, day in enumerate(days):
            price = max(1.0, price * (1 + 0.0005 * rng.normal()))
            rows.append({
                'ts_code': code,
                'date': day,
                'open': price * 0.999,
                'high': price * 1.01,
                'low': price * 0.99,
                'close': price,
                'pre_close': price * 0.999,
                'volume': 1000.0 + rng.integers(0, 500),
                'amount': 1_000_000.0 + rng.normal(0, 10_000),
                'hfq_factor': 1.0,
                'qfq_factor': 1.0,
                'momentum_1m': float(lagged[day_index]),
                'momentum_3m': float(lagged[day_index] * 0.5),
                'momentum_6m': float(lagged[day_index] * 0.3),
                'momentum_12m': float(lagged[day_index] * 0.2),
            })
    return pl.DataFrame(rows)


def test_walk_forward_embargo_and_purge() -> None:
    config = WalkForwardConfig(horizon=3, train_size=60, test_size=20,
                               embargo=5, min_train_dates=20,
                               min_test_dates=10)
    windows = make_walk_forward_windows(300, config)
    assert windows
    first = windows[0]
    assert first.test_start == first.train_end + 5
    # 训练标签窗口 [t+1, t+1+3] 不得跨入测试窗
    assert first.purged_train_end == min(
        first.test_start - config.horizon - 1, first.train_end)
    assert first.purged_train_end + 1 + config.horizon <= first.test_start + 1
    for previous, current in zip(windows[:-1], windows[1:], strict=True):
        assert current.test_start == previous.test_end


def test_embargo_must_cover_holding_period() -> None:
    with pytest.raises(ValueError):
        WalkForwardConfig(horizon=10, embargo=5)


def test_forward_label_uses_t_plus_one_open() -> None:
    opens = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    frame = pl.DataFrame({
        'ts_code': ['000001.SZ'] * len(opens),
        'date': _dates(len(opens)),
        'open_hfq': opens,
    })
    result = attach_forward_labels(frame, horizon=2)
    # t=0: 在 t+1 开盘价 2.0 建仓，t+3 开盘价 4.0 退出 → 4/2 - 1 = 1.0
    assert result['fwd_ret'][0] == pytest.approx(1.0)
    assert result['fwd_ret'][-1] is None


def test_ic_weight_learner_picks_predictive_factor() -> None:
    rng = np.random.default_rng(7)
    rows: list[dict[str, object]] = []
    for day_index in range(120):
        day = _dates(1, start=date(2023, 1, 2))[0] + timedelta(days=day_index)
        signal = rng.normal(size=30)
        noise = rng.normal(size=30)
        for code_index in range(30):
            rows.append({
                'date': day,
                'ts_code': f'{600000 + code_index:06d}.SH',
                'predictive__z': float(signal[code_index]),
                'noise__z': float(noise[code_index]),
                LABEL_COL: float(signal[code_index] * 0.01),
            })
    frame = pl.DataFrame(rows)
    model = ICWeightLearner(half_life=60).fit(
        frame, ['predictive__z', 'noise__z'])
    assert model.coefficients['predictive__z'] > 0
    assert abs(model.coefficients['predictive__z']) > abs(
        model.coefficients.get('noise__z', 0.0))


def test_ridge_learner_recovers_positive_loading() -> None:
    rng = np.random.default_rng(11)
    rows: list[dict[str, object]] = []
    for day_index in range(150):
        day = _dates(1, start=date(2023, 1, 2))[0] + timedelta(days=day_index)
        signal = rng.normal(size=40)
        for code_index in range(40):
            rows.append({
                'date': day,
                'ts_code': f'{600000 + code_index:06d}.SH',
                'signal__z': float(signal[code_index]),
                'noise__z': float(rng.normal()),
                LABEL_COL: float(signal[code_index] * 0.02),
            })
    frame = pl.DataFrame(rows)
    model = RidgeWeightLearner(ridge_lambda=1.0).fit(
        frame, ['signal__z', 'noise__z'])
    assert model.coefficients['signal__z'] > 0


def test_quantile_monotonicity_and_turnover() -> None:
    panel = _synthetic_panel(n_codes=20, n_days=12)
    frame = panel.select(['date', 'ts_code', 'momentum_1m']).rename(
        {'momentum_1m': 'score'})
    frame = frame.with_columns(
        (pl.col('score') * 0.01).alias(LABEL_COL))
    quantiles, monotonicity, long_short = quantile_analysis(
        frame, 'score', n_quantiles=5)
    assert len(quantiles) == 5
    assert monotonicity > 0.9
    assert long_short > 0
    sets = selection_sets(frame, 'score', top_fraction=0.2, rebalance=5)
    assert len(sets) >= 2
    assert 0.0 <= turnover_from_sets(sets) <= 1.0
    assert ic_stats(frame, 'score')['ic_mean'] > 0.9


def test_portfolio_simulation_executes_next_open() -> None:
    config = WalkForwardConfig(horizon=3, train_size=40, test_size=10,
                               embargo=3, min_train_dates=10, min_test_dates=5,
                               top_fraction=0.3)
    panel = add_adjusted_prices(_synthetic_panel(n_codes=12, n_days=60))
    frame = panel.select(['date', 'ts_code', 'momentum_1m']).rename(
        {'momentum_1m': 'score'})
    result = simulate_long_only(panel, frame, config)
    assert result['trades'] >= 1
    assert len(result['equity_curve']) > 0
    assert result['performance']['annual_return'] != 0 or result['total_return']


def test_prepare_panel_adds_regime_and_labels() -> None:
    config = WalkForwardConfig(horizon=5, train_size=80, test_size=30,
                               embargo=5, min_train_dates=20,
                               min_test_dates=10)
    panel = _synthetic_panel(n_codes=20, n_days=200)
    prepared, regimes = prepare_panel(
        panel, ['momentum_1m', 'momentum_3m'], config)
    assert LABEL_COL in prepared.columns
    assert 'regime' in prepared.columns
    assert 'momentum_1m__z' in prepared.columns
    assert regimes.height == prepared['date'].n_unique()
    assert prepared['close_hfq'].is_finite().all()


def test_learning_report_smoke() -> None:
    config = WalkForwardConfig(horizon=3, train_size=60, test_size=20,
                               embargo=4, min_train_dates=20,
                               min_test_dates=8, top_fraction=0.3)
    panel = _synthetic_panel(n_codes=20, n_days=220)
    adds = panel.with_columns([
        (pl.col('momentum_1m') * 0.4 + pl.col('momentum_3m')).alias(
            'momentum_1m'),
    ])
    report = build_learning_report(
        adds, ['momentum_1m', 'momentum_3m', 'momentum_6m', 'momentum_12m'],
        config, title='smoke')
    assert report.windows > 0
    assert report.weight_schemes
    assert report.param_schemes
    assert report.factor_metrics
    assert all(item.n_periods > 0 for item in report.weight_schemes)
