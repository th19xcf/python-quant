#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
横截面算子回归测试

覆盖：截面排序、去极值、行业/暴露中性化、截面 IC 与汇总。
关键断言都是"数学性质"而非快照，例如中性化后组内均值≈0、
残差与暴露相关系数≈0、Rank IC 在完美单调关系下为 1。
"""

import numpy as np
import polars as pl
import pytest

from src.alpha.cross_section.cross_sectional import (
    cross_sectional_ic,
    cross_sectional_rank,
    ic_summary,
    neutralize,
    winsorize,
)

DATES = ['2024-01-02', '2024-01-03', '2024-01-04']
N_STOCKS = 20


@pytest.fixture(scope='module')
def panel() -> pl.DataFrame:
    """3 个交易日 × 20 只标的，含极端值、行业与市值暴露。"""
    rng = np.random.default_rng(7)
    codes = [f'{i:06d}.SZ' for i in range(N_STOCKS) for _ in DATES]
    dates = DATES * N_STOCKS
    factors = [rng.normal(0, 1) for _ in range(N_STOCKS) for _ in DATES]
    industries = ['bank' if i % 2 == 0 else 'tech'
                  for i in range(N_STOCKS) for _ in DATES]
    mktcaps = [float(rng.uniform(1e9, 1e11)) for _ in codes]

    df = pl.DataFrame(
        {'ts_code': codes, 'date': dates, 'factor': factors,
         'industry': industries, 'mktcap': mktcaps})
    # 注入极端值，验证去极值确实生效
    return df.with_columns(
        pl.when((pl.col('ts_code') == '000000.SZ') & (pl.col('date') == DATES[0]))
          .then(1e6).otherwise(pl.col('factor')).alias('factor'))


class TestCrossSectionalRank:
    def test_rank_is_within_date(self, panel):
        out = cross_sectional_rank(panel, ['factor'], pct=False)
        for d in DATES:
            ranks = out.filter(pl.col('date') == d)['factor_cs_rank'].sort()
            # 每个截面恰好是 1..N 的全排列
            assert ranks.to_list() == list(range(1, N_STOCKS + 1))

    def test_pct_rank_in_unit_interval(self, panel):
        out = cross_sectional_rank(panel, ['factor'], pct=True)
        vals = out['factor_cs_rank']
        assert vals.min() > 0 and vals.max() <= 1.0

    def test_row_order_and_columns_preserved(self, panel):
        out = cross_sectional_rank(panel, ['factor'])
        assert out.select(panel.columns).equals(panel)

    def test_missing_column_raises(self, panel):
        with pytest.raises(ValueError, match='factor_x'):
            cross_sectional_rank(panel, ['factor_x'])


class TestWinsorize:
    def test_extreme_value_is_clipped(self, panel):
        out = winsorize(panel, ['factor'])
        first_day = out.filter(pl.col('date') == DATES[0])
        assert first_day['factor_win'].max() < 1e6

    def test_within_quantile_bounds(self, panel):
        out = winsorize(panel, ['factor'], lower=0.1, upper=0.9)
        for d in DATES:
            day = out.filter(pl.col('date') == d)
            values = np.asarray(day['factor'].to_list())
            lo = np.quantile(values, 0.1, method='linear')
            hi = np.quantile(values, 0.9, method='linear')
            assert day['factor_win'].min() >= lo
            assert day['factor_win'].max() <= hi

    def test_invalid_quantiles_raise(self, panel):
        with pytest.raises(ValueError, match='分位点非法'):
            winsorize(panel, ['factor'], lower=0.9, upper=0.1)


class TestNeutralize:
    def test_industry_residual_mean_is_zero(self, panel):
        out = neutralize(panel, ['factor'], industry_col='industry')
        grouped = (out.group_by(['date', 'industry'])
                      .agg(pl.col('factor_neu').mean().alias('m')))
        assert all(abs(v) < 1e-10 for v in grouped['m'].to_list())

    def test_exposure_residual_uncorrelated(self, panel):
        out = neutralize(panel, ['factor'], exposure_col='mktcap')
        for d in DATES:
            day = out.filter(pl.col('date') == d)
            corr = float(np.corrcoef(
                day['factor_neu'].to_numpy(), day['mktcap'].to_numpy())[0, 1])
            assert abs(corr) < 1e-9

    def test_requires_at_least_one_exposure(self, panel):
        with pytest.raises(ValueError, match='industry_col 或 exposure_col'):
            neutralize(panel, ['factor'])

    def test_industry_then_exposure_chaining(self, panel):
        out = (neutralize(panel, ['factor'], industry_col='industry')
               .pipe(lambda d: neutralize(d, ['factor_neu'], exposure_col='mktcap'))
               )
        assert 'factor_neu_neu' in out.columns
        # 顺序残差化后不再保证行业内均值为 0，但最后一步保证：
        # 每个截面内残差均值为 0、且与暴露不相关
        for d in DATES:
            day = out.filter(pl.col('date') == d)
            assert abs(day['factor_neu_neu'].mean()) < 1e-9
            corr = float(np.corrcoef(day['factor_neu_neu'].to_numpy(),
                                     day['mktcap'].to_numpy())[0, 1])
            assert abs(corr) < 1e-9


class TestCrossSectionalIC:
    def test_perfect_monotonic_ic_is_one(self):
        df = pl.DataFrame({
            'date': [DATES[0]] * 10,
            'factor': list(range(10)),
            'ret': [float(i) * 0.01 for i in range(10)],
        })
        ic = cross_sectional_ic(df, 'factor', 'ret')
        assert ic.height == 1
        assert ic['ic'][0] == pytest.approx(1.0)

    def test_inverse_relation_ic_negative(self):
        df = pl.DataFrame({
            'date': [DATES[0]] * 10,
            'factor': list(range(10)),
            'ret': [-float(i) * 0.01 for i in range(10)],
        })
        ic = cross_sectional_ic(df, 'factor', 'ret')
        assert ic['ic'][0] == pytest.approx(-1.0)

    def test_one_row_per_date_sorted(self, panel):
        out = panel.with_columns(
            (pl.col('factor') * 0.01).alias('fwd_ret')).filter(
            pl.col('date') != DATES[2])
        ic = cross_sectional_ic(out, 'factor', 'fwd_ret')
        assert ic.height == len(DATES) - 1
        assert ic['date'].to_list() == sorted(ic['date'].to_list())

    def test_ic_summary(self):
        ic = pl.DataFrame({'ic': [0.1, 0.2, -0.1, 0.3]})
        s = ic_summary(ic)
        assert s['n_periods'] == 4
        assert s['mean_ic'] == pytest.approx(0.125)
        assert s['positive_ratio'] == pytest.approx(0.75)

    def test_ic_summary_empty(self):
        s = ic_summary(pl.DataFrame({'ic': [None, None]}, schema={'ic': pl.Float64}))
        assert s == {'mean_ic': 0.0, 'ic_std': 0.0, 'ic_ir': 0.0,
                     'positive_ratio': 0.0, 'n_periods': 0}
