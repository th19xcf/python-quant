#!/usr/bin/env python3

"""缺失值用例：窗口不足的因子必须保持 null，不参与打分（禁止 fill_null(0.0)）。"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from src.advice.config import TIER_PRESETS, FactorSpec, default_config
from src.advice.factor_panel import compute_factor_panel
from src.advice.scoring import attach_score

START = date(2026, 1, 1)


def _rows(code: str, days: int, base: float, offset: int = 0) -> list[dict]:
    rows = []
    for index in range(days):
        close = base + index * 0.05
        rows.append({
            'date': START + timedelta(days=offset + index),
            'ts_code': code,
            'open': close, 'high': close * 1.01, 'low': close * 0.99,
            'close': close, 'volume': 1.0e5, 'amount': 5.0e8,
            'raw_amount': 5.0e8, 'raw_close': close, 'raw_prev_close': close,
        })
    return rows


def _panel(days: int = 60, stocks: int = 3) -> pl.DataFrame:
    rows: list[dict] = []
    for index in range(stocks):
        rows.extend(_rows(f'00000{index}.SZ', days, base=10.0 + index))
    return pl.DataFrame(rows)


def test_factor_panel_keeps_warmup_missing_as_null() -> None:
    panel = compute_factor_panel(_panel(), extended=False)
    one = panel.filter(pl.col('ts_code') == '000000.SZ')

    # 窗口不足不再是 0：ATR(14) 前 14 行、20 日波动前 20 行、1 月动量前 20 行
    assert one['atr_14'].null_count() == 14
    assert one['volatility_20'].null_count() == 20
    assert one['momentum_1m'].null_count() == 20
    # 由缺失因子派生的列同样保留 null，且不存在"缺失被填成 0"的行
    assert one['atr_pct'].null_count() == 14
    assert (one['atr_14'] == 0).sum() == 0
    assert (one['momentum_1m'] == 0).sum() == 0


def test_missing_factor_is_excluded_instead_of_scored_as_extreme() -> None:
    """缺失不参与打分：单因子档位下，缺失票 score=null，不会因填 0 拿最高分。"""
    date_one = START
    rows = [
        {'date': date_one, 'ts_code': f'00000{i}.SZ', 'volume': 1.0e5,
         'atr_pct': value}
        for i, value in enumerate([0.01, 0.02, 0.03, 0.04])
    ]
    rows.append({'date': date_one, 'ts_code': '000009.SZ', 'volume': 1.0e5,
                 'atr_pct': None})
    frame = pl.DataFrame(rows)
    tier = default_config('legacy').tier('short').with_overrides(
        factors=(FactorSpec('atr_pct', 'ATR占比', 1.0, direction=-1.0),))
    scored = attach_score(frame, tier)
    row = scored.filter(pl.col('ts_code') == '000009.SZ').row(0, named=True)
    assert row['score'] is None
    assert row['score_percentile'] is None
    best = scored.filter(pl.col('ts_code') == '000000.SZ').row(0, named=True)
    assert best['score'] is not None and best['score'] > 0


def test_new_stock_without_history_gets_no_score() -> None:
    """历史不足的"新股"不再因为全缺失被填 0 而拿到最高加权。"""
    rows: list[dict] = []
    for index in range(4):
        rows.extend(_rows(f'00000{index}.SZ', 60, base=10.0 + index))
    # 第 5 只只有 10 个交易日，五个短线因子全部处于窗口不足状态
    rows.extend(_rows('000009.SZ', 10, base=20.0, offset=50))
    panel = pl.DataFrame(rows)

    factors = compute_factor_panel(panel, extended=False)
    new_stock = factors.filter(pl.col('ts_code') == '000009.SZ').sort('date')
    assert new_stock['atr_pct'].is_null().all()
    assert new_stock['momentum_1m'].is_null().all()

    scored = attach_score(factors, default_config('legacy').tier('short'),
                          min_history_rows=0)
    assert scored.filter(pl.col('ts_code') == '000009.SZ')['score'].is_null().all()
    last_date = scored['date'].max()
    seasoned = scored.filter((pl.col('date') == last_date)
                             & (pl.col('ts_code') != '000009.SZ'))
    assert seasoned['score'].null_count() == 0


def test_ret_20_is_duplicate_of_momentum_1m_and_not_double_weighted() -> None:
    """ret_20 与 momentum_1m 是同一个公式；当前档位权重里没有同时使用两者。"""
    panel = compute_factor_panel(_panel(days=80), extended=False)
    both = panel.drop_nulls(['ret_20', 'momentum_1m'])
    assert both.height > 0
    diff = (both['ret_20'] - both['momentum_1m']).abs().max()
    assert diff < 1e-12
    # 缺失位置也一致
    assert (panel['ret_20'].is_null() == panel['momentum_1m'].is_null()).all()

    for key, tier in TIER_PRESETS.items():
        columns = {spec.column for spec in tier.factors}
        assert not ({'ret_20', 'momentum_1m'} <= columns), (
            f'档位 {key} 同时使用 ret_20 与 momentum_1m，构成重复计权')


if __name__ == '__main__':
    raise SystemExit(pytest.main([__file__]))
