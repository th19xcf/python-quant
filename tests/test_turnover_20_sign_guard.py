"""守护：``new_factors.turnover_20`` 必须落盘正原值，禁止再次取负。

背景（G-096 / 2026-11-12 定案）：本列曾落盘为 ``-(20 日真实换手率均值)``，
与 ``src/advice/fundamental_factors.py`` 的 ``ext_turnover_20``（存正原值、
方向由 ``direction=-1`` 承载）精确互为相反数（ρ = -0.99999888）。若有人再把
两份 ``turnover_20`` 按列名直接合并，方向冲突会静默抵消信号。

两条断言：
1. 生产者源码不得对 ``_turn20`` 取负，且 ``build_turnover`` 必须声明「正原值」；
2. 落盘列与日常换手率（``factors/stock_turnover.parquet``，存正原值）的秩相关
   必须显著为正——取负后 ρ 会翻成 -1，本断言即变红。
"""
from __future__ import annotations

import re
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[1]
BUILDER = ROOT / 'tools' / 'research' / 'build_new_factors.py'
NEW_FACTORS = ROOT / 'data' / 'local_store' / 'meta' / 'research_inputs' / 'new_factors.parquet'
DAILY_TURNOVER = ROOT / 'data' / 'local_store' / 'factors' / 'stock_turnover.parquet'

WINDOW_START = '2024-01-01'


def test_producer_stores_positive_turnover() -> None:
    src = BUILDER.read_text(encoding='utf-8')
    negation = re.search(r"\(\s*-\s*pl\.col\(\s*['\"]_turn20['\"]\s*\)", src)
    assert negation is None, (
        'build_new_factors.py 又对 _turn20 取负了：turnover_20 必须落盘正原值，'
        '方向由元数据 direction=-1 表达（G-096）')
    assert '正原值' in src, 'build_turnover docstring 必须写明「正原值」约定'


@pytest.mark.skipif(not NEW_FACTORS.exists(), reason='new_factors.parquet 不存在')
@pytest.mark.skipif(not DAILY_TURNOVER.exists(), reason='stock_turnover.parquet 不存在')
def test_turnover_20_direction_is_positive() -> None:
    nf = (pl.read_parquet(NEW_FACTORS)
          .select('ts_code', 'date', 'turnover_20')
          .filter(pl.col('date') >= pl.date(2024, 1, 1)))
    daily = (pl.scan_parquet(DAILY_TURNOVER)
             .filter(pl.col('trade_date') >= pl.date(2023, 11, 1))
             .select('ts_code', pl.col('trade_date').alias('date'), 'turn')
             .collect()
             .sort(['ts_code', 'date'])
             # baostock turn 单位为百分比，换算成与 new_factors 同量纲的小数
             .with_columns(
                 (pl.col('turn').rolling_mean(20, min_samples=10).over('ts_code') / 100.0)
                 .alias('ref')))
    joined = (nf.join(daily, on=['ts_code', 'date'], how='inner')
              .drop_nulls(['turnover_20', 'ref']))
    assert joined.height > 50_000, f'可比行数过少: {joined.height}'

    rho = joined.select(
        pl.corr(pl.col('turnover_20').rank(), pl.col('ref').rank())).item()
    ratio = joined.select(
        (pl.col('turnover_20') / pl.col('ref')).median()).item()

    assert rho > 0.99, (
        f'turnover_20 与日常换手率秩相关 ρ={rho:.6f}，方向或口径已偏离正原值约定'
        f'（若 |ρ|>0.99 且为负，说明该列又被取负，见 G-096 缺陷）')
    assert 0.5 < ratio < 2.0, (
        f'turnover_20 / 日常换手率 中位比 = {ratio:.4f}，超出同量纲区间，'
        f'疑似缩放或取负错误（G-096）')
