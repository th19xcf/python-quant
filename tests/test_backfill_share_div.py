"""送转字段缺失扫描与回补单测：确认/无法解释/误报、只填空值、幂等。"""

import sys
from datetime import date
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / 'tools'))

from backfill_share_div import apply_backfill, classify, scan  # noqa: E402


def _shares() -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': ['600000.SH', '600000.SH', '600001.SH', '600001.SH',
                    '600002.SH', '600002.SH', '600004.SH', '600004.SH'],
        'effective_date': [date(2020, 1, 1), date(2020, 6, 8),
                           date(2020, 1, 1), date(2020, 6, 8),
                           date(2020, 1, 1), date(2020, 6, 8),
                           date(2020, 1, 1), date(2020, 6, 8)],
        'total_share': [100_000_000, 120_000_000,
                        50_300_600, 80_028_254,
                        100_000_000, 120_000_000,
                        100_000_000, 120_000_000],
        'float_share': [80_000_000, 96_000_000,
                        50_300_600, 80_028_254,
                        80_000_000, 96_000_000,
                        80_000_000, 96_000_000],
        'change_reason': ['转增股上市', '转增股上市',
                          '上市前股本', '送股上市,转增股上市',
                          '配售A股上市', '配售A股上市',
                          '转增股上市', '转增股上市'],
    })


def _dividends() -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': ['600000.SH', '600001.SH', '600002.SH', '600004.SH',
                    '600005.SH'],
        'ex_date': [date(2020, 6, 8), date(2020, 6, 8), date(2020, 6, 8),
                    None, None],
        'record_date': [date(2020, 6, 1), date(2019, 12, 31),
                        date(2020, 6, 1), date(2020, 6, 8), None],
        'cash_div': [0.5, 0.5, 0.5, 0.5, 0.5],
        'share_div': [0.0, 0.0, 0.0, None, 0.0],
    })


def test_confirmed_detected_and_backfilled():
    frame = _dividends()
    decisions = scan(frame, _shares())
    hit = [d for d in decisions if d['ts_code'] == '600000.SH']
    assert len(hit) == 1
    assert hit[0]['category'] == 'confirmed'
    assert hit[0]['inferred_share_div'] == 0.2
    out, filled = apply_backfill(frame, decisions)
    row = out.filter(pl.col('ts_code') == '600000.SH').row(0, named=True)
    assert row['share_div'] == 0.2
    assert row['share_div_source'] == 'inferred_from_shares'
    # 600000 与 600004 同时命中确认缺失
    assert filled == 2


def test_off_grid_is_unresolved_and_not_backfilled():
    frame = _dividends()
    decisions = scan(frame, _shares())
    hit = [d for d in decisions if d['ts_code'] == '600001.SH']
    assert len(hit) == 1
    # 50,300,600 → 80,028,254，隐含 10 送 5.91，不在 0.05 档位
    assert hit[0]['category'] == 'unexplained'
    assert hit[0]['inferred_share_div'] is None
    out, filled = apply_backfill(frame, decisions)
    row = out.filter(pl.col('ts_code') == '600001.SH').row(0, named=True)
    assert row['share_div'] == 0.0
    assert row['share_div_source'] == 'orig'
    assert filled == 2  # 仅 600000/600004 被回补


def test_false_positive_from_other_event_not_backfilled():
    frame = _dividends()
    decisions = scan(frame, _shares())
    hit = [d for d in decisions if d['ts_code'] == '600002.SH']
    assert len(hit) == 1
    assert hit[0]['category'] == 'false_positive'
    assert hit[0]['inferred_share_div'] is None


def test_non_null_value_is_never_overwritten():
    frame = pl.DataFrame({
        'ts_code': ['600004.SH'],
        'ex_date': [date(2020, 6, 8)],
        'record_date': [date(2020, 6, 1)],
        'cash_div': [0.5],
        'share_div': [0.3],
    })
    decisions = [{'_row_id': 0, 'category': 'confirmed',
                  'inferred_share_div': 0.2}]
    out, filled = apply_backfill(frame, decisions)
    row = out.row(0, named=True)
    assert row['share_div'] == 0.3
    assert row['share_div_source'] == 'orig'
    assert filled == 0


def test_record_date_fallback_detects_missing_ex_date():
    frame = _dividends()
    decisions = scan(frame, _shares())
    hit = [d for d in decisions if d['ts_code'] == '600004.SH']
    assert len(hit) == 1
    assert hit[0]['category'] == 'confirmed'
    assert hit[0]['day'] == '2020-06-08'
    out, filled = apply_backfill(frame, decisions)
    row = out.filter(pl.col('ts_code') == '600004.SH').row(0, named=True)
    assert row['share_div'] == 0.2
    assert row['share_div_source'] == 'inferred_from_shares'
    assert filled == 2  # 600000 与 600004 同时命中确认缺失


def test_multiple_same_day_share_events_are_unresolved():
    shares = pl.concat([
        _shares().filter(pl.col('ts_code') == '600000.SH'),
        pl.DataFrame({
            'ts_code': ['600000.SH'],
            'effective_date': [date(2020, 6, 8)],
            'total_share': [120_000_000],
            'float_share': [96_000_000],
            'change_reason': ['增发A股上市'],
        }),
    ])
    # 同日两行股本事件（source 拼接重复）→ 无法解释
    frame = _dividends().filter(pl.col('ts_code') == '600000.SH')
    decisions = scan(frame, shares)
    assert len(decisions) == 1
    assert decisions[0]['category'] == 'unexplained'
    assert decisions[0]['inferred_share_div'] is None


def test_no_share_increase_is_ignored():
    shares = _shares().filter(pl.col('ts_code') == '600005.SH')
    frame = _dividends().filter(pl.col('ts_code') == '600005.SH')
    assert scan(frame, shares) == []


def test_apply_twice_is_idempotent():
    frame = _dividends()
    shares = _shares()
    decisions = scan(frame, shares)
    once, filled = apply_backfill(frame, decisions)
    assert filled == 2
    second = scan(once, shares)
    assert all(d['category'] != 'confirmed' for d in second)
    twice, filled_again = apply_backfill(once, second)
    assert filled_again == 0
    assert twice.equals(once)


def test_classify_grid_boundaries():
    # 0.05 档位接受，0.02 档位偏离 0.003 以内亦接受
    assert classify(100, 105, '送股上市')['category'] == 'confirmed'
    assert classify(100, 120, '转增股上市')['category'] == 'confirmed'
    # 1000 → 1187，隐含 10 送 1.87，非标准档位
    assert classify(1000, 1187, '送股上市')['category'] == 'unexplained'
    # 股本未增加不判定
    assert classify(100, 100, '送股上市') is None
