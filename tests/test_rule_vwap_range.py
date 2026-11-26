"""成交均价 [low, high] 规则（2026-11-26）。"""
from datetime import date

import polars as pl

from src.data.quality.rules import RuleContext, rule_vwap_out_of_range


def _frame(rows):
    return pl.DataFrame(rows, schema={'date': pl.Date, 'ts_code': pl.Utf8, 'high': pl.Float64,
                                      'low': pl.Float64, 'volume': pl.Float64,
                                      'amount': pl.Float64}, orient='row')


def test_flags_unit_error_and_passes_normal() -> None:
    d = date(2024, 1, 2)
    frame = _frame([
        (d, '600000.SH', 10.5, 10.0, 1000.0, 1000 * 100 * 10.2),   # 正常：均价 10.2
        (d, '600001.SH', 10.5, 10.0, 1000.0, 1000 * 10.2),         # 成交量错 100 倍：均价 0.102
        (d, '600002.SH', 10.5, 10.0, 1000.0, 1000 * 100 * 10.52),  # 越界 0.19%，在容差内
        (d, '600003.SH', 10.5, 10.0, 0.0, 0.0),                    # 停牌：跳过
    ])
    out = rule_vwap_out_of_range(frame, RuleContext())
    # 违规表不含 ts_code：只有成交量错 100 倍那行（均价 0.102）被抓出
    assert out.height == 1
    assert abs(out['value'][0] - 0.102) < 1e-9
    assert 'low=10.0' in out['detail'][0]


def test_skips_pre_1996() -> None:
    frame = _frame([(date(1993, 5, 1), '600601.SH', 10.5, 10.0, 1000.0, 1000.0)])
    assert rule_vwap_out_of_range(frame, RuleContext()).height == 0
