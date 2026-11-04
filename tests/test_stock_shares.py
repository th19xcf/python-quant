"""股本事实表单测：点位边界、单位、缺失、选源、倒推。

运行：``pytest tests/test_stock_shares.py -q``
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl

from src.data.local_store.share_capital import (
    SOURCE_CNINFO,
    SOURCE_EM,
    ShareSnapshot,
    choose_share_series,
    derive_share_series,
    normalize_rows,
    shares_as_of,
    snapshot_error,
    stitch_legacy_series,
    write_shares,
)


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema={
        'ts_code': pl.Utf8,
        'effective_date': pl.Date,
        'total_share': pl.Int64,
        'float_share': pl.Int64,
        'free_float_share': pl.Int64,
        'change_reason': pl.Utf8,
        'source': pl.Utf8,
        'fetched_at': pl.Datetime('us'),
    })


def _write(root: Path, rows: list[dict[str, object]]) -> None:
    write_shares(_frame(rows), root)


# ----------------------------------------------------------------------
# 单位
# ----------------------------------------------------------------------
def test_cninfo_wan_to_gu() -> None:
    """巨潮单位是万股，入库必须是股（×10000 后取整）。"""
    frame = normalize_rows(
        [{'变动日期': '2024-03-29', '总股本': 462724.9686, '已流通股份': 389123.4567,
          '变动原因': '定期报告'}],
        ts_code='300750.SZ', date_key='变动日期', total_key='总股本',
        float_key='已流通股份', reason_key='变动原因', scale=10000.0,
        source=SOURCE_CNINFO, fetched_at=datetime(2026, 11, 4), today=date(2026, 11, 4))
    row = frame.to_dicts()[0]
    assert row['total_share'] == 4_627_249_686
    assert row['float_share'] == 3_891_234_567
    assert frame.schema['total_share'] == pl.Int64


def test_em_passthrough_is_gu() -> None:
    """东财源端已是股，scale=1 时不做二次放大。"""
    frame = normalize_rows(
        [{'变更日期': '2025-10-27', '总股本': 33_305_838_300, '已上市流通A股': 33_305_838_300}],
        ts_code='600000.SH', date_key='变更日期', total_key='总股本',
        float_key='已上市流通A股', scale=1.0, source=SOURCE_EM,
        fetched_at=datetime(2026, 11, 4), today=date(2026, 11, 4))
    assert frame.to_dicts()[0]['total_share'] == 33_305_838_300


def test_nonpositive_and_missing_total_become_none() -> None:
    """总股本缺失/非正的行直接丢弃，流通股本为空保留 NULL。"""
    frame = normalize_rows(
        [{'变动日期': '2024-01-02', '总股本': '-', '已流通股份': 1.0},
         {'变动日期': '2024-06-30', '总股本': 100.5, '已流通股份': None},
         {'变动日期': '2025-01-01', '总股本': 0, '已流通股份': 10}],
        ts_code='000001.SZ', date_key='变动日期', total_key='总股本',
        float_key='已流通股份', scale=10000.0, source=SOURCE_CNINFO,
        fetched_at=datetime(2026, 11, 4), today=date(2026, 11, 4))
    assert frame.height == 1
    row = frame.to_dicts()[0]
    assert row['total_share'] == 1_005_000
    assert row['float_share'] is None


# ----------------------------------------------------------------------
# 去未来行 / 同值折叠
# ----------------------------------------------------------------------
def test_future_effective_date_is_dropped() -> None:
    """生效日 > 采集日的行必须丢弃，保证不会提前看到未来股本。"""
    frame = normalize_rows(
        [{'变更日期': '2026-11-05', '总股本': 200, '已上市流通A股': 200},
         {'变更日期': '2026-11-03', '总股本': 100, '已上市流通A股': 100}],
        ts_code='600000.SH', date_key='变更日期', total_key='总股本',
        float_key='已上市流通A股', scale=1.0, source=SOURCE_EM,
        fetched_at=datetime(2026, 11, 4), today=date(2026, 11, 4))
    assert frame.height == 1
    assert frame.to_dicts()[0]['effective_date'] == date(2026, 11, 3)


def test_identical_steps_collapse_to_earliest() -> None:
    """定期报告快照与真实变动同值时，只保留最早生效那行。"""
    frame = normalize_rows(
        [{'变动日期': '2020-06-30', '总股本': 100, '已流通股份': 100},
         {'变动日期': '2020-09-30', '总股本': 100, '已流通股份': 100},
         {'变动日期': '2021-05-20', '总股本': 200, '已流通股份': 200}],
        ts_code='000001.SZ', date_key='变动日期', total_key='总股本',
        float_key='已流通股份', scale=1.0, source=SOURCE_CNINFO,
        fetched_at=datetime(2026, 11, 4), today=date(2026, 11, 4))
    assert [row['effective_date'] for row in frame.to_dicts()] == [
        date(2020, 6, 30), date(2021, 5, 20)]


# ----------------------------------------------------------------------
# 点时查询
# ----------------------------------------------------------------------
def test_as_of_boundary_effective_date(tmp_path: Path) -> None:
    """生效日前一天查旧股本，生效日当天查新股本（硬要求）。"""
    _write(tmp_path, [
        {'ts_code': '600000.SH', 'effective_date': date(2020, 1, 2),
         'total_share': 1000, 'float_share': 800, 'free_float_share': None,
         'change_reason': '首次', 'source': SOURCE_EM,
         'fetched_at': datetime(2026, 11, 4)},
        {'ts_code': '600000.SH', 'effective_date': date(2024, 6, 1),
         'total_share': 2000, 'float_share': 1500, 'free_float_share': None,
         'change_reason': '送转', 'source': SOURCE_EM,
         'fetched_at': datetime(2026, 11, 4)},
    ])

    before = shares_as_of('600000.SH', '2024-05-31', tmp_path)
    on_day = shares_as_of('600000.SH', '2024-06-01', tmp_path)
    assert isinstance(before, ShareSnapshot)
    assert isinstance(on_day, ShareSnapshot)
    assert before.total_share == 1000 and before.float_share == 800
    assert on_day.total_share == 2000 and on_day.float_share == 1500
    assert before.effective_date == date(2020, 1, 2)
    assert on_day.effective_date == date(2024, 6, 1)


def test_as_of_accepts_str_date_datetime(tmp_path: Path) -> None:
    """查询入参兼容 str / date / datetime（下游用法不统一）。"""
    _write(tmp_path, [
        {'ts_code': '000001.SZ', 'effective_date': date(2022, 1, 4),
         'total_share': 500, 'float_share': 500, 'free_float_share': None,
         'change_reason': None, 'source': SOURCE_EM,
         'fetched_at': datetime(2026, 11, 4)},
    ])
    assert shares_as_of('000001.SZ', '2022-01-04', tmp_path) is not None
    assert shares_as_of('000001.SZ', date(2022, 1, 4), tmp_path) is not None
    assert shares_as_of('000001.SZ', datetime(2022, 1, 4, 15, 0), tmp_path) is not None
    assert shares_as_of('000001.SZ', '2022-01-03', tmp_path) is None


def test_as_of_missing_code_and_before_first_row(tmp_path: Path) -> None:
    """缺失标的返回 None；早于首个生效日也返回 None（不向前臆造）。"""
    _write(tmp_path, [
        {'ts_code': '600000.SH', 'effective_date': date(2020, 1, 2),
         'total_share': 1000, 'float_share': 800, 'free_float_share': None,
         'change_reason': None, 'source': SOURCE_EM,
         'fetched_at': datetime(2026, 11, 4)},
    ])
    assert shares_as_of('000002.SZ', '2025-01-01', tmp_path) is None
    assert shares_as_of('600000.SH', '2019-12-31', tmp_path) is None
    assert shares_as_of('600000.SH', 'not-a-date', tmp_path) is None


def test_missing_float_is_none_not_zero(tmp_path: Path) -> None:
    """流通股本缺失必须是 None（不能拿 0 当股本，会算出无穷换手率）。"""
    _write(tmp_path, [
        {'ts_code': '920001.BJ', 'effective_date': date(2020, 7, 27),
         'total_share': 153_646_104, 'float_share': None, 'free_float_share': None,
         'change_reason': None, 'source': SOURCE_EM,
         'fetched_at': datetime(2026, 11, 4)},
    ])
    snap = shares_as_of('920001.BJ', '2026-11-04', tmp_path)
    assert snap is not None and snap.float_share is None


# ----------------------------------------------------------------------
# 选源
# ----------------------------------------------------------------------
def test_choose_prefers_em_when_matching_snapshot() -> None:
    """东财与快照一致时使用东财（A 股口径流通股本）。"""
    em = _frame([{'ts_code': '688981.SH', 'effective_date': date(2025, 1, 2),
                 'total_share': 8_000_000_000, 'float_share': 2_000_000_000,
                 'free_float_share': None, 'change_reason': None,
                 'source': SOURCE_EM, 'fetched_at': datetime(2026, 11, 4)}])
    cn = _frame([{'ts_code': '688981.SH', 'effective_date': date(2025, 1, 2),
                 'total_share': 8_000_000_000, 'float_share': 8_000_000_000,
                 'free_float_share': None, 'change_reason': None,
                 'source': SOURCE_CNINFO, 'fetched_at': datetime(2026, 11, 4)}])
    chosen = choose_share_series(em, cn, (8_000_000_000, 2_000_000_000))
    assert chosen.to_dicts()[0]['source'] == SOURCE_EM
    assert chosen.to_dicts()[0]['float_share'] == 2_000_000_000


def test_choose_falls_back_to_cninfo_when_em_stale() -> None:
    """东财末行落后于快照、巨潮更准时，必须切巨潮，否则送转会漏采。"""
    em = _frame([{'ts_code': '600683.SH', 'effective_date': date(2012, 4, 9),
                 'total_share': 700_000_000, 'float_share': 700_000_000,
                 'free_float_share': None, 'change_reason': None,
                 'source': SOURCE_EM, 'fetched_at': datetime(2026, 11, 4)}])
    cn = _frame([{'ts_code': '600683.SH', 'effective_date': date(2026, 6, 30),
                 'total_share': 740_777_600, 'float_share': 740_777_600,
                 'free_float_share': None, 'change_reason': None,
                 'source': SOURCE_CNINFO, 'fetched_at': datetime(2026, 11, 4)}])
    chosen = choose_share_series(em, cn, (740_777_597, 740_777_597))
    assert chosen.to_dicts()[-1]['source'] == SOURCE_CNINFO


def test_stitch_legacy_prefix_from_cninfo() -> None:
    """老票：锚点（两源同日一致）前用巨潮，锚点后用东财。"""
    em = _frame([
        {'ts_code': '600653.SH', 'effective_date': date(1990, 12, 19),
         'total_share': 50_000, 'float_share': None, 'free_float_share': None,
         'change_reason': '首发A股上市', 'source': SOURCE_EM,
         'fetched_at': datetime(2026, 11, 4)},
        {'ts_code': '600653.SH', 'effective_date': date(1993, 3, 15),
         'total_share': 17_000_000, 'float_share': 11_594_000,
         'free_float_share': None, 'change_reason': '送股上市',
         'source': SOURCE_EM, 'fetched_at': datetime(2026, 11, 4)},
        {'ts_code': '600653.SH', 'effective_date': date(2009, 7, 22),
         'total_share': 1_746_380_317, 'float_share': 1_746_380_317,
         'free_float_share': None, 'change_reason': '转增股上市',
         'source': SOURCE_EM, 'fetched_at': datetime(2026, 11, 4)},
    ])
    cn = _frame([
        {'ts_code': '600653.SH', 'effective_date': date(1990, 12, 19),
         'total_share': 1_000_000, 'float_share': 700_000,
         'free_float_share': None, 'change_reason': 'A股上市',
         'source': SOURCE_CNINFO, 'fetched_at': datetime(2026, 11, 4)},
        {'ts_code': '600653.SH', 'effective_date': date(1993, 3, 15),
         'total_share': 17_000_000, 'float_share': 11_594_000,
         'free_float_share': None, 'change_reason': '送股,转增',
         'source': SOURCE_CNINFO, 'fetched_at': datetime(2026, 11, 4)},
    ])
    merged, stitched = stitch_legacy_series(em, cn)
    assert stitched is True
    rows = merged.to_dicts()
    assert rows[0]['effective_date'] == date(1990, 12, 19)
    assert rows[0]['total_share'] == 1_000_000       # 巨潮官方口径
    assert rows[0]['source'] == SOURCE_CNINFO
    assert rows[-1]['total_share'] == 1_746_380_317  # 锚点后仍用东财
    assert rows[-1]['source'] == SOURCE_EM


def test_stitch_legacy_refuses_without_anchor() -> None:
    """流通口径不同（A+H/B）时找不到锚点，必须原样返回东财，不拼接。"""
    em = _frame([
        {'ts_code': '601633.SH', 'effective_date': date(2011, 9, 1),
         'total_share': 8_500_000_000, 'float_share': 6_200_000_000,
         'free_float_share': None, 'change_reason': None, 'source': SOURCE_EM,
         'fetched_at': datetime(2026, 11, 4)},
    ])
    cn = _frame([
        {'ts_code': '601633.SH', 'effective_date': date(2011, 9, 1),
         'total_share': 8_500_000_000, 'float_share': 8_500_000_000,
         'free_float_share': None, 'change_reason': None,
         'source': SOURCE_CNINFO, 'fetched_at': datetime(2026, 11, 4)},
    ])
    merged, stitched = stitch_legacy_series(em, cn)
    assert stitched is False
    assert merged.to_dicts()[0]['float_share'] == 6_200_000_000


def test_choose_empty_sources() -> None:
    """两源皆空返回空帧，交给倒推兜底。"""
    empty = _frame([])
    assert choose_share_series(empty, empty, None).is_empty()


def test_snapshot_error_ignores_uncomparable() -> None:
    """快照没有流通股本时不拿它参与比较，避免误判。"""
    em = _frame([{'ts_code': '600000.SH', 'effective_date': date(2025, 1, 2),
                 'total_share': 1000, 'float_share': 800, 'free_float_share': None,
                 'change_reason': None, 'source': SOURCE_EM,
                 'fetched_at': datetime(2026, 11, 4)}])
    assert snapshot_error(em, None) is None
    assert snapshot_error(em, (1000, None)) == 0.0
    assert snapshot_error(em, (2000, 800)) == 0.5


# ----------------------------------------------------------------------
# 倒推
# ----------------------------------------------------------------------
def test_derive_chain_returns_current_total() -> None:
    """倒推链末行必须等于当前总股本，中间每步按 (1+share_div) 等比放大。"""
    frame = derive_share_series(
        '000001.SZ', list_date=date(1991, 4, 3), current_total=19_405_918_198,
        current_float=19_405_918_198,
        dividend_events=[(date(1994, 7, 11), 0.5), (date(2021, 5, 20), 0.1)],
        fetched_at=datetime(2026, 11, 4), today=date(2026, 11, 4))
    rows = frame.to_dicts()
    assert rows[-1]['total_share'] == 19_405_918_198
    # 2021-05-20 之前的一档 = 当前 / 1.1，1994-07-11 之前 = 当前 / (1.1*1.5)
    assert rows[-2]['total_share'] == round(19_405_918_198 / 1.1)
    assert rows[0]['total_share'] == round(19_405_918_198 / 1.65)
    assert rows[0]['effective_date'] == date(1991, 4, 3)


def test_derive_skips_events_before_listing() -> None:
    """上市前的送转不参与倒推（否则会凭空压低首行股本）。"""
    frame = derive_share_series(
        '300750.SZ', list_date=date(2018, 6, 11), current_total=1000,
        current_float=900,
        dividend_events=[(date(2010, 1, 1), 1.0), (date(2020, 6, 1), 0.25)],
        fetched_at=datetime(2026, 11, 4), today=date(2026, 11, 4))
    rows = frame.to_dicts()
    assert rows[0]['effective_date'] == date(2018, 6, 11)
    assert rows[0]['total_share'] == round(1000 / 1.25)
    assert rows[-1]['total_share'] == 1000
    assert rows[-1]['float_share'] == 900
