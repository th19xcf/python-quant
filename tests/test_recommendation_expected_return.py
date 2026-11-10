#!/usr/bin/env python3

"""推荐输出不得再出现 ``expected_return``（``score * 100`` 冒充收益预测）。"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from src.recommendation.stock_recommender import StockRecommender


def _panel() -> pl.DataFrame:
    rows = []
    start = date(2026, 9, 1)
    for code, base in (('600000.SH', 10.0), ('000001.SZ', 12.0)):
        for offset in range(30):
            rows.append({
                'stock_code': code,
                'date': start + timedelta(days=offset),
                'close': base + offset * 0.1,
                'volume': 1_000_000 + offset * 1_000,
                'stock_name': f'标的{code[:3]}',
                'industry': '银行',
            })
    return pl.DataFrame(rows)


def test_recommender_output_has_no_expected_return():
    results = StockRecommender().recommend_stocks(_panel(), top_n=2)
    assert len(results) == 2
    for row in results:
        assert 'expected_return' not in row
        # 收益维度只能显式给 score，不得换名继续冒充
        assert 'score' in row
