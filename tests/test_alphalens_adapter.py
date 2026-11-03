"""Alphalens 适配层单测：锁定 T+1 建仓口径、截面 IC 输出与边界行为。

全部使用构造数据，无网络依赖；未安装 analytics 可选组时自动跳过。
"""

from __future__ import annotations

import json
from datetime import date, timedelta

import numpy as np
import pandas as pd
import polars as pl
import pytest

pytest.importorskip('alphalens')

from src.alpha.evaluation.alphalens_adapter import (  # noqa: E402
    AlphalensConfig,
    build_alphalens_inputs,
    evaluate_factor_cross_section,
    run_alphalens_evaluation,
)
from src.alpha.evaluation.factor_evaluator import FactorEvaluator  # noqa: E402


def _panel(opens: dict[str, list[float]], factor_by_asset: dict[str, list[float]],
           dates: list[date]) -> pl.DataFrame:
    rows = []
    for code, prices in opens.items():
        factors = factor_by_asset[code]
        for day, price, factor in zip(dates, prices, factors, strict=True):
            rows.append({
                'ts_code': code,
                'date': day,
                'open': price,
                'close': price,
                'factor': factor,
            })
    return pl.DataFrame(rows)


@pytest.fixture()
def six_days() -> list[date]:
    start = date(2024, 1, 2)
    return [start + timedelta(days=i) for i in range(6)]


def test_t_plus_one_execution_lag_and_open_return(six_days):
    """信号 T 日产生 → 因子索引前移到 T+1，1 日收益用 T+1→T+2 的 open 计。"""
    opens = {
        'AAA.SZ': [10.0, 10.5, 11.0, 11.5, 12.0, 12.5],
        'BBB.SZ': [20.0, 21.5, 22.0, 22.5, 23.0, 24.0],
    }
    factors = {
        'AAA.SZ': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        'BBB.SZ': [2.0, 1.0, 4.0, 3.0, 6.0, 5.0],
    }
    panel = _panel(opens, factors, six_days)

    config = AlphalensConfig(factor_column='factor', periods=(1, 2), quantiles=2)
    factor, prices = build_alphalens_inputs(panel, config)

    original_dates = pd.DatetimeIndex(pd.to_datetime(six_days))
    expected_exec = original_dates[1:]
    actual_dates = pd.DatetimeIndex(factor.index.get_level_values('date')).unique()
    # 信号日 d0..d4 前移到 d1..d5；最后一日 d5 无后继交易日，被丢弃
    assert list(actual_dates) == list(expected_exec)

    from alphalens.utils import get_clean_factor_and_forward_returns
    factor_data = get_clean_factor_and_forward_returns(
        factor, prices, periods=(1, 2), quantiles=2, max_loss=1.0)

    # 执行日 2024-01-03 的 1D 收益必须 = open(01-04) / open(01-03) - 1
    exec_day = pd.Timestamp('2024-01-03')
    assert factor_data.loc[(exec_day, 'AAA.SZ'), '1D'] == pytest.approx(
        opens['AAA.SZ'][2] / opens['AAA.SZ'][1] - 1)
    assert factor_data.loc[(exec_day, 'BBB.SZ'), '1D'] == pytest.approx(
        opens['BBB.SZ'][2] / opens['BBB.SZ'][1] - 1)
    # 2D 收益 = open(T+3)/open(T+1) - 1
    assert factor_data.loc[(exec_day, 'AAA.SZ'), '2D'] == pytest.approx(
        opens['AAA.SZ'][3] / opens['AAA.SZ'][1] - 1)


def test_close_entry_uses_t_plus_one_close(six_days):
    opens = {
        'AAA.SZ': [10.0, 11.0, 12.0, 13.0, 14.0, 15.0],
        'BBB.SZ': [20.0, 22.0, 24.0, 26.0, 28.0, 30.0],
    }
    factors = {
        'AAA.SZ': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        'BBB.SZ': [2.0, 1.0, 4.0, 3.0, 6.0, 5.0],
    }
    panel = _panel(opens, factors, six_days)

    config = AlphalensConfig(factor_column='factor', periods=(1,), quantiles=2,
                             entry_price='close', max_loss=1.0)
    factor, prices = build_alphalens_inputs(panel, config)
    from alphalens.utils import get_clean_factor_and_forward_returns
    factor_data = get_clean_factor_and_forward_returns(
        factor, prices, periods=(1,), quantiles=2, max_loss=1.0)
    exec_day = pd.Timestamp('2024-01-03')
    assert factor_data.loc[(exec_day, 'AAA.SZ'), '1D'] == pytest.approx(12.0 / 11.0 - 1)


def test_empty_and_single_day_panels_raise():
    empty = pl.DataFrame(schema={'ts_code': pl.Utf8, 'date': pl.Date,
                                 'open': pl.Float64, 'close': pl.Float64,
                                 'factor': pl.Float64})
    config = AlphalensConfig(factor_column='factor', periods=(1,))
    with pytest.raises(ValueError, match='为空'):
        build_alphalens_inputs(empty, config)

    single = _panel(
        {'AAA.SZ': [10.0], 'BBB.SZ': [20.0]},
        {'AAA.SZ': [1.0], 'BBB.SZ': [2.0]},
        [date(2024, 1, 2)],
    )
    with pytest.raises(ValueError, match='至少 2 个交易日'):
        build_alphalens_inputs(single, config)


def test_duplicate_date_asset_is_deduplicated(six_days):
    opens = {'AAA.SZ': [10.0] * 6, 'BBB.SZ': [20.0] * 6}
    factors = {'AAA.SZ': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
               'BBB.SZ': [1.0] * 6}
    panel = _panel(opens, factors, six_days)
    # 追加一条同日同标的的重复记录（因子值不同，应保留最后一条）
    dup = pl.DataFrame([{'ts_code': 'AAA.SZ', 'date': six_days[2],
                         'open': 10.0, 'close': 10.0, 'factor': 99.0}])
    panel = pl.concat([panel, dup])

    config = AlphalensConfig(factor_column='factor', periods=(1,), quantiles=2)
    factor, prices = build_alphalens_inputs(panel, config)
    assert len(prices) == len(six_days)
    assert prices.loc[pd.Timestamp(six_days[2]), 'AAA.SZ'] == pytest.approx(10.0)
    assert len(factor.index) == 5 * 2  # 6 日 × 2 标的中最后一日被丢弃
    assert not factor.index.duplicated().any()


def test_missing_middle_date_is_forward_filled(six_days):
    opens = {
        'AAA.SZ': [10.0, 11.0, 12.0, 13.0, 14.0, 15.0],
        # BBB 缺失第三天（2024-01-04）
        'BBB.SZ': [20.0, 22.0, np.nan, 26.0, 28.0, 30.0],
    }
    factors = {
        'AAA.SZ': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        'BBB.SZ': [2.0, 1.0, 4.0, 3.0, 6.0, 5.0],
    }
    panel = _panel(opens, factors, six_days)
    panel = panel.filter(
        ~((pl.col('ts_code') == 'BBB.SZ')
          & (pl.col('date') == six_days[2])))

    config = AlphalensConfig(factor_column='factor', periods=(1,), quantiles=2,
                             max_loss=1.0)
    factor, prices = build_alphalens_inputs(panel, config)
    assert pd.isna(prices.loc[pd.Timestamp(six_days[2]), 'BBB.SZ'])
    metrics = evaluate_factor_cross_section(factor, prices, config)
    assert metrics['n_assets'] == 2
    assert metrics['n_dates'] >= 1


def test_run_evaluation_writes_html_and_json(six_days, tmp_path):
    opens = {
        f'{i}.SZ': [10.0 * (1.0 + 0.02 * (i + 1)) ** j for j in range(6)]
        for i in range(4)
    }
    factors = {
        f'{i}.SZ': [float((j + 1) * (i + 1)) for j in range(6)] for i in range(4)
    }
    panel = _panel(opens, factors, six_days)
    result = run_alphalens_evaluation(
        panel, 'factor', periods=(1, 2), quantiles=2, max_loss=1.0,
        output_dir=tmp_path, report_name='alphalens_test')

    assert set(result) >= {'ic_mean', 'icir', 'ic_decay', 'quantile_returns',
                           'long_short_return', 'turnover', 'tearsheet_path',
                           'json_path', 'entry_rule', 'elapsed_seconds'}
    assert result['entry_rule'].startswith('信号 T 日产生')
    assert len(result['periods']) == 2
    for period in result['periods']:
        assert np.isfinite(result['ic_mean'][period])
        std = result['ic_std'][period]
        if np.isfinite(std) and std > 0:
            assert result['icir'][period] == pytest.approx(
                result['ic_mean'][period] / std)
        else:
            # 截面 IC 无波动时 ICIR 数学上无定义，约定为 NaN
            assert np.isnan(result['icir'][period])

    html = tmp_path / 'alphalens_test.html'
    js = tmp_path / 'alphalens_test.json'
    assert html.exists() and html.stat().st_size > 1000
    assert 'data:image/png;base64,' in html.read_text(encoding='utf-8')
    payload = json.loads(js.read_text(encoding='utf-8'))
    assert payload['factor'] == 'factor'
    assert payload['price_adjustment'] == 'hfq'


def test_invalid_config_values_raise():
    with pytest.raises(ValueError, match='entry_price'):
        AlphalensConfig(factor_column='f', entry_price='vwap')  # type: ignore[arg-type]
    with pytest.raises(ValueError, match='periods'):
        AlphalensConfig(factor_column='f', periods=())
    with pytest.raises(ValueError, match='quantiles'):
        AlphalensConfig(factor_column='f', quantiles=1)


def test_ts_ic_alias_preserved():
    factor = pl.Series('factor', [1.0, 2.0, 3.0, 4.0])
    returns = pl.Series('returns', [0.01, 0.02, 0.015, 0.03])
    evaluator = FactorEvaluator()
    assert evaluator.calculate_ts_ic(factor, returns) == pytest.approx(
        evaluator.calculate_ic(factor, returns))
    assert evaluator.calculate_ts_rank_ic(factor, returns) == pytest.approx(
        evaluator.calculate_rank_ic(factor, returns))
