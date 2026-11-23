"""limit_board 涨跌停标记：档位解析、上市阶段、封板/可成交判定。

只用内存构造的中间态表调用 ``_apply_marks``，不读 data/local_store；
价格与可买可卖仍由 ``ExecutionRules`` 裁决（模块内部会抽样对拍，口径不一致会直接抛错）。
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src.core.market_rules import ExecutionRules
from src.data.limit_board import (
    _apply_marks,
    _board_expr,
    _limit_pct_map,
    _month_spans,
    _prepare,
)

_SCHEMA = {
    'date': pl.Date, 'ts_code': pl.Utf8,
    'open': pl.Float64, 'high': pl.Float64, 'low': pl.Float64,
    'close': pl.Float64, 'pre_close': pl.Float64, 'volume': pl.Float64,
    'board': pl.Utf8, 'list_date': pl.Date, 'listing_days': pl.Int32,
    'issue_price': pl.Float64, 'issue_price_is_zero': pl.Boolean,
    'st_status': pl.Utf8, 'st_rank': pl.Int8, 'era': pl.Int8,
}

_BASE = {
    'date': date(2024, 6, 3), 'ts_code': '600000.SH',
    'open': 10.0, 'high': 10.5, 'low': 9.8, 'close': 10.2,
    'pre_close': 10.0, 'volume': 1_000_000.0,
    'board': '沪市主板', 'list_date': date(2000, 1, 4), 'listing_days': 5000,
    'issue_price': None, 'issue_price_is_zero': None,
    'st_status': 'non_st', 'st_rank': 0, 'era': 1,
}


def _frame(*rows: dict) -> pl.DataFrame:
    return pl.DataFrame([{**_BASE, **row} for row in rows], schema=_SCHEMA)


def _mark(row: dict) -> dict:
    out = _apply_marks(_frame(row), {})
    assert out.height == 1
    return out.row(0, named=True)


def test_month_spans_clip_to_year_boundary_and_partial_months():
    spans = list(_month_spans(date(2023, 11, 15), date(2024, 1, 10)))
    assert [key for _, _, key in spans] == ['202311', '202312', '202401']
    assert spans[0][0] == date(2023, 11, 15) and spans[0][1] == date(2023, 11, 30)
    assert spans[1] == (date(2023, 12, 1), date(2023, 12, 31), '202312')
    assert spans[2] == (date(2024, 1, 1), date(2024, 1, 10), '202401')


def test_limit_pct_map_follows_board_and_st_rank():
    mapped = _limit_pct_map(['600000.SH', '300750.SZ', '688981.SH'])
    by_key = {(r['ts_code'], r['era'], r['st_rank']): r['pct']
              for r in mapped.iter_rows(named=True)}
    pre = date(2019, 1, 2)
    # 主板：普通 10%、ST 5%；改革前创业板同样 10%
    assert by_key[('600000.SH', 0, 0)] == 10
    assert by_key[('600000.SH', 0, 1)] == 5
    assert by_key[('300750.SZ', 0, 0)] == 10
    # 注册制后创业板 20%，科创板两段都是 20%
    assert by_key[('300750.SZ', 1, 0)] == 20
    assert by_key[('688981.SH', 0, 0)] == 20
    # 创业板的 20% 板块上 ST 不降档，仍按 20%（st_wide 分支由 limit_basis 标注）
    assert by_key[('300750.SZ', 1, 1)] == 20
    assert by_key[('688981.SH', 0, 1)] == 20
    assert ExecutionRules.resolve_limit_pct('600000.SH', is_st=True, trade_date=pre) == 0.05


def test_board_expr_falls_back_to_code_prefix_when_board_missing():
    frame = pl.DataFrame({
        'ts_code': ['600000.SH', '688001.SH', '300750.SZ', '920001.BJ', '830799.BJ'],
        'board': [None, None, None, None, None],
    })
    assert frame.with_columns(_board_expr())['board'].to_list() == [
        '主板', '科创板', '创业板', '北交所', '北交所']
    # stock_basic 有值时以其为准，不被代码段覆盖
    kept = pl.DataFrame({'ts_code': ['300750.SZ'], 'board': ['主板']})
    assert kept.with_columns(_board_expr())['board'].to_list() == ['主板']


def test_normal_day_limit_prices_and_touch_flags():
    row = _mark({})
    assert row['limit_basis'] == 'board'
    assert row['limit_pct'] == pytest.approx(0.10)
    assert row['limit_up_price'] == pytest.approx(11.00)
    assert row['limit_down_price'] == pytest.approx(9.00)
    assert row['prev_close_source'] == 'quote'
    assert row['listing_phase'] == 'normal'
    assert row['touch_up'] is False and row['close_at_up'] is False
    assert row['one_word_up'] is False
    assert row['can_buy'] is True and row['can_sell'] is True


def test_touch_limit_intraday_but_not_sealed():
    row = _mark({'high': 11.0, 'close': 10.8})
    assert row['touch_up'] is True
    assert row['close_at_up'] is False
    assert row['can_buy'] is True


def test_one_word_limit_up_blocks_buying():
    row = _mark({'open': 11.0, 'high': 11.0, 'low': 11.0, 'close': 11.0})
    assert row['one_word_up'] is True
    assert row['close_at_up'] is True and row['touch_up'] is True
    assert row['can_buy'] is False
    assert row['can_sell'] is True


def test_st_rows_use_five_percent_band_and_basis():
    row = _mark({'ts_code': '000001.SZ', 'board': '深市主板',
                 'st_status': 'st', 'st_rank': 1})
    assert row['limit_basis'] == 'st'
    assert row['limit_up_price'] == pytest.approx(10.50)
    assert row['limit_down_price'] == pytest.approx(9.50)


def test_unknown_st_status_keeps_board_band_but_flags_basis():
    row = _mark({'st_status': 'unknown', 'st_rank': 2})
    assert row['limit_basis'] == 'st_unknown'
    assert row['limit_up_price'] == pytest.approx(11.00)


def test_suspended_row_blocks_both_sides():
    row = _mark({'volume': 0.0})
    assert row['suspended'] is True
    assert row['can_buy'] is False and row['can_sell'] is False


def test_star_board_listing_exempt_leaves_prices_empty():
    row = _mark({'ts_code': '688001.SH', 'board': '科创板',
                 'list_date': date(2024, 1, 2), 'date': date(2024, 1, 4),
                 'listing_days': 3, 'pre_close': None})
    assert row['listing_phase'] == 'exempt'
    assert row['limit_basis'] == 'new_stock_exempt'
    assert row['limit_up_price'] is None and row['limit_down_price'] is None
    assert row['touch_up'] is None and row['close_at_up'] is None
    assert row['can_buy'] is True and row['can_sell'] is True


def test_legacy_first_day_before_2014_has_no_band():
    row = _mark({'date': date(2010, 1, 4), 'list_date': date(2010, 1, 4),
                 'listing_days': 1, 'pre_close': None, 'open': 5.0,
                 'high': 7.0, 'low': 4.0, 'close': 6.0})
    assert row['listing_phase'] == 'legacy'
    assert row['limit_basis'] == 'new_stock_legacy_no_limit'
    assert row['limit_up_price'] is None
    assert row['can_buy'] is True and row['can_sell'] is True


def test_legacy_first_day_with_issue_price_uses_44_36_band():
    row = _mark({'ts_code': '600003.SH', 'date': date(2016, 6, 1),
                 'list_date': date(2016, 6, 1), 'listing_days': 1,
                 'issue_price': 5.0, 'issue_price_is_zero': False,
                 'pre_close': None, 'open': 6.0, 'high': 7.2, 'low': 4.0,
                 'close': 7.2})
    assert row['listing_phase'] == 'legacy'
    assert row['limit_basis'] == 'new_stock_legacy_44_36'
    assert row['limit_pct'] is None            # 上下不对称，幅度列留空
    assert row['limit_up_price'] == pytest.approx(7.20)
    assert row['limit_down_price'] == pytest.approx(3.20)
    assert row['close_at_up'] is True
    assert row['can_buy'] is True              # 开盘 6.0 未触板，仍可买入


def _write_store(root: Path, *, with_new_share: bool) -> None:
    """最小列存：两日行情 + 交易日历 + 上市信息 + 逐日 ST 面板。"""
    (root / 'quotes' / 'stock').mkdir(parents=True)
    quotes = pl.DataFrame({
        'date': [date(2024, 6, 3), date(2024, 6, 4)] * 2,
        'ts_code': ['600000.SH'] * 2 + ['688001.SH'] * 2,
        'open': [10.0, 10.1, 20.0, 20.2],
        'high': [10.3, 10.4, 20.5, 20.8],
        'low': [9.9, 10.0, 19.8, 20.1],
        'close': [10.2, 10.3, 20.4, 20.6],
        'pre_close': [10.0, 10.2, 20.0, 20.4],
        'volume': [1e6, 1.1e6, 2e5, 2.1e5],
    }, schema_overrides={'date': pl.Date})
    quotes.write_parquet(root / 'quotes' / 'stock' / 'shard_202406.parquet')

    (root / 'meta' / 'breadth').mkdir(parents=True)
    pl.DataFrame({'trade_date': [date(2024, 6, 3), date(2024, 6, 4)]}
                 ).write_parquet(root / 'meta' / 'breadth' / 'breadth_daily.parquet')

    pl.DataFrame({
        'ts_code': ['600000.SH', '688001.SH'],
        'list_date': [date(2000, 1, 4), date(2024, 6, 3)],
        'board': ['沪市主板', '科创板'],
    }).write_parquet(root / 'meta' / 'stock_basic.parquet')

    (root / 'meta' / 'st' / 'daily').mkdir(parents=True)
    pl.DataFrame({
        'trade_date': [date(2024, 6, 4)],
        'ts_code': ['600000.SH'],
        'status': ['st'],
    }, schema_overrides={'trade_date': pl.Date}
    ).write_parquet(root / 'meta' / 'st' / 'daily' / 'st_daily_202406.parquet')

    if with_new_share:
        pl.DataFrame({
            'ts_code': ['688001.SH'],
            'issue_date': [date(2024, 6, 3)],
            'price': [20.0],
            'price_is_zero': [False],
        }, schema_overrides={'issue_date': pl.Date}
        ).write_parquet(root / 'meta' / 'new_share.parquet')


def test_prepare_maps_listing_days_st_status_and_new_share(tmp_path):
    _write_store(tmp_path, with_new_share=True)
    frame, counters = _prepare(tmp_path, date(2024, 6, 1), date(2024, 6, 30))

    rows = {(r['ts_code'], r['date']): r for r in frame.iter_rows(named=True)}
    # 上市日之前的日历索引 → 上市第 1/2 个交易日
    assert rows[('688001.SH', date(2024, 6, 3))]['listing_days'] == 1
    assert rows[('688001.SH', date(2024, 6, 4))]['listing_days'] == 2
    # 上市日不在交易日历覆盖范围内时 listing_days 留空（不猜）
    assert rows[('600000.SH', date(2024, 6, 4))]['listing_days'] is None
    assert counters['listing_days_missing'] == 2
    # ST 面板缺行的日期记 unknown（rank 2），有行且为 st 的记 st（rank 1）
    assert rows[('600000.SH', date(2024, 6, 3))]['st_status'] == 'unknown'
    assert rows[('600000.SH', date(2024, 6, 4))]['st_status'] == 'st'
    assert rows[('688001.SH', date(2024, 6, 4))]['st_status'] == 'unknown'
    # 2024 年属注册制后（era=1），发行价随 new_share 落进中间列
    assert {r['era'] for r in rows.values()} == {1}
    assert rows[('688001.SH', date(2024, 6, 3))]['issue_price'] == pytest.approx(20.0)
    assert counters['quote_rows'] == 4 and counters['quote_codes'] == 2
    assert counters['new_share_rows'] == 1
    assert counters['st_panel_missing'] == 3


def test_prepare_without_new_share_fills_null_issue_price(tmp_path):
    _write_store(tmp_path, with_new_share=False)
    frame, counters = _prepare(tmp_path, date(2024, 6, 1), date(2024, 6, 30))

    assert frame['issue_price'].null_count() == frame.height
    assert counters['new_share_rows'] == 0
    assert frame['listing_days'].null_count() == counters['listing_days_missing'] == 2

