#!/usr/bin/env python3

"""洁净集准入函数测试：结构性/时效性缺陷、ST as-of、档位差异。"""

from __future__ import annotations

import os
import time
from datetime import date
from pathlib import Path

import numpy as np
import pytest

from src.data.quality.clean_set import (
    FUND_DIVIDEND_PENDING_ENV,
    FUND_DIVIDEND_PENDING_SCOPE_ENV,
    CodeInfo,
    EligibilityEngine,
    _apply_scope_with_etf_codes,
    _as_date,
    _code_type,
    _load_fund_etf_codes,
    _resolve_fund_pending_scope,
)


@pytest.fixture(autouse=True)
def _isolate_fund_snapshot(monkeypatch, tmp_path):
    """单测默认不读真实 meta/fund_basic.parquet，避免环境耦合。"""
    monkeypatch.setattr(
        'src.data.quality.clean_set.FUND_BASIC_SNAPSHOT',
        tmp_path / 'missing_fund_basic.parquet')


def make_code(
    ts_code: str,
    *,
    data_type: str = 'stock',
    start: date | None = date(2018, 1, 2),
    end: date | None = date(2026, 11, 3),
    quotes: list[date] | None = None,
    factors: list[date] | None = None,
    **attrs: object,
) -> CodeInfo:
    """构造单标的索引；默认因子日期与行情日期一致。"""
    quote_list = quotes or []
    factor_list = quote_list if factors is None else factors
    info = CodeInfo(
        ts_code=ts_code,
        data_type=data_type,
        rows=len(quote_list),
        start=start,
        end=end,
    )
    info.start_ord = start.toordinal() if start else None
    info.end_ord = end.toordinal() if end else None
    info.quote_dates = np.array(sorted(d.toordinal() for d in quote_list), dtype=np.int64)
    info.factor_dates = np.array(sorted(d.toordinal() for d in factor_list), dtype=np.int64)
    info.factor_rows = len(factor_list)
    for key, value in attrs.items():
        setattr(info, key, value)
    return info


def make_engine(
    codes: list[CodeInfo],
    *,
    calendar: list[date] | None = None,
    as_of: date = date(2026, 11, 3),
    suspected_st: set[str] | None = None,
    dup_exdiv_codes: set[str] | None = None,
) -> EligibilityEngine:
    """不加载真实数据，直接组装判定引擎。"""
    engine = object.__new__(EligibilityEngine)
    engine.root = Path('data/local_store')
    engine.exclude_st = True
    engine.include_suspected_st = False
    engine.codes = {info.ts_code: info for info in codes}
    engine.calendar = calendar or [as_of]
    engine.calendar_ord = np.array(
        [d.toordinal() for d in engine.calendar], dtype=np.int64
    )
    engine.as_of = as_of
    engine.suspected_st = suspected_st or set()
    engine.st_default = (date(2005, 1, 1).toordinal(), as_of.toordinal())
    engine.dup_exdiv_codes = dup_exdiv_codes or set()
    engine.null_exdiv_dup_codes = set()
    return engine


def test_structural_defect_is_permanent() -> None:
    """结构性缺陷在任何日期都剔除，但 full 不受影响。"""
    info = make_code(
        '000001.SZ',
        quotes=[date(2018, 6, 1), date(2026, 11, 3)],
        structural=['structural_duplicate_exdiv_key'],
    )
    engine = make_engine([info])

    for day in ('2018-06-01', '2026-11-03'):
        ok, reasons = engine.eligible('000001.SZ', day, 'standard')
        assert ok is False
        assert reasons == ['structural_duplicate_exdiv_key']
        ok, reasons = engine.eligible('000001.SZ', day, 'strict')
        assert ok is False
        assert 'structural_duplicate_exdiv_key' in reasons
        assert engine.eligible('000001.SZ', day, 'full') == (True, [])


def test_temporal_stop_does_not_leak_backward() -> None:
    """2019 年停更的标的，2018 年仍然可用。"""
    info = make_code(
        '000002.SZ',
        end=date(2019, 6, 28),
        quotes=[date(2018, 6, 29), date(2019, 6, 28)],
    )
    engine = make_engine([info])

    assert engine.eligible('000002.SZ', '2018-06-29', 'strict') == (True, [])
    ok, reasons = engine.eligible('000002.SZ', '2019-07-01', 'strict')
    assert ok is False
    assert reasons == ['temporal_source_stopped']


def test_factor_tail_only_affects_later_dates() -> None:
    """因子末端滞后只影响 date > 因子末端的区间。"""
    info = make_code(
        '000003.SZ',
        quotes=[date(2026, 9, 30), date(2026, 11, 3)],
        factors=[date(2026, 9, 30)],
    )
    engine = make_engine([info])

    assert engine.eligible('000003.SZ', '2026-09-30', 'strict') == (True, [])
    assert engine.eligible('000003.SZ', '2026-11-03', 'standard') == (True, [])
    ok, reasons = engine.eligible('000003.SZ', '2026-11-03', 'strict')
    assert ok is False
    assert reasons == ['temporal_factor_tail']


def test_st_interval_is_asof_and_recovers_after_lifting() -> None:
    """ST 区间命中当日不可选，摘帽后自动回归。"""
    info = make_code(
        '600001.SH',
        start=date(2024, 1, 2),
        end=date(2025, 12, 31),
        quotes=[date(2024, 6, 3), date(2025, 6, 2)],
        st_intervals=[
            (date(2024, 5, 6).toordinal(), date(2025, 3, 6).toordinal())
        ],
    )
    engine = make_engine([info])

    ok, reasons = engine.eligible('600001.SH', '2024-06-03', 'standard')
    assert ok is False
    assert reasons == ['st_confirmed']
    assert engine.eligible('600001.SH', '2025-06-02', 'strict') == (True, [])


def test_suspected_st_is_marked_but_not_admitted_by_default() -> None:
    """疑似 ST 默认不参与 strict，开关打开后才剔除。"""
    info = make_code('600002.SH', quotes=[date(2024, 6, 3)])
    engine = make_engine([info], suspected_st={'600002.SH'})

    assert engine.eligible('600002.SH', '2024-06-03', 'strict') == (True, [])
    ok, reasons = engine.eligible(
        '600002.SH', '2024-06-03', 'strict', include_suspected_st=True
    )
    assert ok is False
    assert reasons == ['st_suspected']
    assert engine.eligible_set(
        '2024-06-03', 'strict', include_suspected_st=True
    ) == []


def test_gap_and_out_of_range_dates_are_unavailable() -> None:
    """缺口日、未上市日、断更后的日期都返回不可用。"""
    info = make_code(
        '600003.SH',
        start=date(2018, 1, 2),
        end=date(2018, 1, 5),
        quotes=[date(2018, 1, 2), date(2018, 1, 5)],
    )
    engine = make_engine([info])

    ok, reasons = engine.eligible('600003.SH', '2018-01-03', 'strict')
    assert ok is False
    assert reasons == ['temporal_gap_or_suspension']
    ok, reasons = engine.eligible('600003.SH', '2017-12-29', 'strict')
    assert ok is False
    assert reasons == ['temporal_not_listed']
    ok, reasons = engine.eligible('600003.SH', '2018-01-08', 'strict')
    assert ok is False
    assert reasons == ['temporal_source_stopped']


def test_fund_is_pending_only_in_strict() -> None:
    """基金在 standard 可用，strict 因分红口径 pending 被剔。"""
    info = make_code(
        '510300.SH',
        data_type='fund',
        start=date(2020, 1, 2),
        quotes=[date(2024, 6, 3)],
    )
    engine = make_engine([info])

    assert engine.eligible('510300.SH', '2024-06-03', 'standard') == (True, [])
    ok, reasons = engine.eligible('510300.SH', '2024-06-03', 'strict')
    assert ok is False
    assert reasons == ['pending_fund_dividend']


def test_fund_pending_switch_opens_strict() -> None:
    """一键开关打开后，基金不再被 pending 剔除，standard 口径不变。"""
    info = make_code(
        '510300.SH',
        data_type='fund',
        start=date(2020, 1, 2),
        quotes=[date(2024, 6, 3)],
    )
    engine = make_engine([info])
    engine.close_fund_dividend_pending = True

    assert engine.eligible('510300.SH', '2024-06-03', 'standard') == (True, [])
    assert engine.eligible('510300.SH', '2024-06-03', 'strict') == (True, [])
    assert engine.eligible_set('2024-06-03', 'strict', data_type='fund') == [
        '510300.SH'
    ]
    assert engine.layer_at('510300.SH', '2024-06-03') == 'remaining'
    assert engine._layer_hits('510300.SH', '2024-06-03') == set()


def test_fund_pending_switch_in_timeline() -> None:
    """时间序列的 strict 基金只数随开关变化。"""
    calendar = [date(2018, 1, 2), date(2018, 1, 31)]
    fund = make_code(
        '510700.SH',
        data_type='fund',
        quotes=[date(2018, 1, 31)],
    )
    engine = make_engine([fund], calendar=calendar, as_of=date(2018, 1, 31))

    assert engine.timeline('2018-01-31', '2018-01-31')[0]['strict_fund'] == 0
    engine.close_fund_dividend_pending = True
    assert engine.timeline('2018-01-31', '2018-01-31')[0]['strict_fund'] == 1


def test_close_fund_pending_flag_sources(monkeypatch) -> None:
    """开关优先级：显式传参 > 环境变量 > 默认 etf。"""
    monkeypatch.delenv(FUND_DIVIDEND_PENDING_ENV, raising=False)
    monkeypatch.delenv(FUND_DIVIDEND_PENDING_SCOPE_ENV, raising=False)
    assert _resolve_fund_pending_scope() == 'etf'
    assert _resolve_fund_pending_scope(None, True) == 'all'
    assert _resolve_fund_pending_scope(None, False) == 'none'
    assert _resolve_fund_pending_scope('etf', False) == 'etf'
    monkeypatch.setenv(FUND_DIVIDEND_PENDING_ENV, '1')
    assert _resolve_fund_pending_scope() == 'all'
    monkeypatch.setenv(FUND_DIVIDEND_PENDING_SCOPE_ENV, 'etf')
    assert _resolve_fund_pending_scope() == 'etf'
    assert _resolve_fund_pending_scope('none') == 'none'
    with pytest.raises(ValueError):
        _resolve_fund_pending_scope('bogus')


def test_fund_pending_scope_none_etf_all() -> None:
    """三档取值：none 全剔、etf 只放 fund_basic 名单、all 全放。"""
    etf = make_code('510300.SH', data_type='fund', quotes=[date(2024, 6, 3)])
    lof = make_code('161907.SZ', data_type='fund', quotes=[date(2024, 6, 3)])
    engine = make_engine([etf, lof])
    engine.etf_codes = {'510300.SH'}

    for scope, etf_ok, lof_ok in (
        ('none', False, False), ('etf', True, False), ('all', True, True)
    ):
        engine.close_fund_dividend_pending_scope = scope
        assert engine.eligible('510300.SH', '2024-06-03', 'strict') == (
            etf_ok, [] if etf_ok else ['pending_fund_dividend'])
        assert engine.eligible('161907.SZ', '2024-06-03', 'strict') == (
            lof_ok, [] if lof_ok else ['pending_fund_dividend'])

    engine.close_fund_dividend_pending_scope = 'etf'
    assert engine.layer_at('510300.SH', '2024-06-03') == 'remaining'
    assert engine.layer_at('161907.SZ', '2024-06-03') == 'pending'


def test_fund_pending_legacy_bool_maps_to_scope() -> None:
    """旧布尔参数/属性兼容映射：True→all，False→none。"""
    engine = make_engine(
        [make_code('510300.SH', data_type='fund', quotes=[date(2024, 6, 3)])]
    )
    engine.close_fund_dividend_pending = True
    assert engine.close_fund_dividend_pending_scope == 'all'
    assert engine.close_fund_dividend_pending is True
    engine.close_fund_dividend_pending = False
    assert engine.close_fund_dividend_pending_scope == 'none'
    assert engine.close_fund_dividend_pending is False


def test_fund_pending_scope_degrades_without_etf_codes(
    monkeypatch, tmp_path
) -> None:
    """fund_basic 不可用且无缓存时，etf 范围降级为不放开。"""
    def boom() -> set[str]:
        raise RuntimeError('db down')

    monkeypatch.setattr(
        'src.data.quality.clean_set.FUND_ETF_CODES_CACHE',
        tmp_path / 'missing.txt')
    monkeypatch.setattr(
        'src.data.quality.clean_set._fund_etf_codes_from_db', boom)
    assert _load_fund_etf_codes() is None
    assert _apply_scope_with_etf_codes('etf', None) == 'none'
    assert _apply_scope_with_etf_codes('etf', {'510300.SH'}) == 'etf'
    assert _apply_scope_with_etf_codes('all', None) == 'all'

    monkeypatch.setattr(
        'src.data.quality.clean_set._fund_etf_codes_from_db',
        lambda: {'510300.SH'})
    assert _load_fund_etf_codes() == {'510300.SH'}


def test_rights_and_jump_mismatch_are_strict_only() -> None:
    """配股存疑与跳变不匹配只影响 strict。"""
    info = make_code(
        '600004.SH',
        quotes=[date(2024, 6, 3)],
        rights_start=date(2020, 1, 2).toordinal(),
        mismatch_start=date(2021, 1, 4).toordinal(),
    )
    engine = make_engine([info])

    assert engine.eligible('600004.SH', '2024-06-03', 'standard') == (True, [])
    ok, reasons = engine.eligible('600004.SH', '2024-06-03', 'strict')
    assert ok is False
    assert reasons == [
        'structural_rights_no_second_source',
        'structural_event_jump_mismatch',
    ]


def test_unknown_code_and_invalid_tier() -> None:
    """未知标的返回 unknown_code；非法档位报错。"""
    engine = make_engine([make_code('600005.SH', quotes=[date(2024, 6, 3)])])

    assert engine.eligible('999999.SH', '2024-06-03', 'strict') == (
        False,
        ['unknown_code'],
    )
    try:
        engine.eligible('600005.SH', '2024-06-03', 'bogus')
    except ValueError as exc:
        assert '未知档位' in str(exc)
    else:
        raise AssertionError('非法档位应抛 ValueError')


def test_eligible_set_filters_by_type_and_st() -> None:
    """批量入口按类型过滤，并正确叠加 ST 开关。"""
    stock = make_code('600006.SH', quotes=[date(2024, 6, 3)])
    index = make_code('000300.SH', data_type='index', quotes=[date(2024, 6, 3)])
    fund = make_code('510500.SH', data_type='fund', quotes=[date(2024, 6, 3)])
    engine = make_engine([stock, index, fund])

    assert engine.eligible_set('2024-06-03', 'full', data_type='stock') == [
        '600006.SH'
    ]
    assert engine.eligible_set('2024-06-03', 'strict', data_type='index') == [
        '000300.SH'
    ]
    assert engine.eligible_set('2024-06-03', 'strict', data_type='fund') == []


def test_layer_priority_and_hit_intersection() -> None:
    """一票命中多层时按缺陷 > ST > 除权 > pending 归类。"""
    defect = make_code(
        '000010.SZ',
        quotes=[date(2024, 6, 3)],
        structural=['structural_content_inconsistent'],
        st_intervals=[(date(2020, 1, 1).toordinal(), date(2025, 1, 1).toordinal())],
        rights_start=date(2020, 1, 2).toordinal(),
    )
    st_only = make_code(
        '000011.SZ',
        quotes=[date(2024, 6, 3)],
        st_intervals=[(date(2020, 1, 1).toordinal(), date(2025, 1, 1).toordinal())],
        rights_start=date(2020, 1, 2).toordinal(),
    )
    div_only = make_code(
        '000012.SZ',
        quotes=[date(2024, 6, 3)],
        rights_start=date(2020, 1, 2).toordinal(),
    )
    fund = make_code('510600.SH', data_type='fund', quotes=[date(2024, 6, 3)])
    clean = make_code('600007.SH', quotes=[date(2024, 6, 3)])
    engine = make_engine([defect, st_only, div_only, fund, clean])

    assert engine.layer_at('000010.SZ', '2024-06-03') == 'data_defect'
    assert engine.layer_at('000011.SZ', '2024-06-03') == 'st'
    assert engine.layer_at('000012.SZ', '2024-06-03') == 'div_doubt'
    assert engine.layer_at('510600.SH', '2024-06-03') == 'pending'
    assert engine.layer_at('600007.SH', '2024-06-03') == 'remaining'
    assert engine._layer_hits('000010.SZ', '2024-06-03') == {
        'data_defect', 'st', 'div_doubt'
    }


def test_month_end_dates_and_timeline() -> None:
    """月末采样与时间序列按三档输出。"""
    calendar = [date(2018, 1, 2), date(2018, 1, 31), date(2018, 2, 28)]
    stock = make_code('600008.SH', quotes=[date(2018, 1, 31), date(2018, 2, 28)])
    fund = make_code(
        '510700.SH',
        data_type='fund',
        quotes=[date(2018, 1, 31), date(2018, 2, 28)],
    )
    engine = make_engine(
        [stock, fund], calendar=calendar, as_of=date(2018, 2, 28)
    )

    assert engine.month_end_dates('2018-01-01', '2018-02-28') == [
        date(2018, 1, 31), date(2018, 2, 28)
    ]
    rows = engine.timeline('2018-01-31', '2018-02-28')
    assert [row['date'] for row in rows] == ['2018-01-31', '2018-02-28']
    assert rows[0]['full_stock'] == 1
    assert rows[0]['standard_stock'] == 1
    assert rows[0]['strict_stock'] == 1
    assert rows[0]['full_fund'] == 1
    assert rows[0]['strict_fund'] == 0


def test_summary_at_groups_views_layers_and_reasons() -> None:
    """静态汇总覆盖三档、分层、交集与年份分布。"""
    defect = make_code(
        '000020.SZ',
        quotes=[date(2024, 6, 3)],
        structural=['structural_duplicate_exdiv_key'],
        rights_start=date(2020, 1, 2).toordinal(),
    )
    st_only = make_code(
        '000021.SZ',
        quotes=[date(2024, 6, 3)],
        st_intervals=[(date(2020, 1, 1).toordinal(), date(2025, 1, 1).toordinal())],
    )
    fund = make_code('510800.SH', data_type='fund', quotes=[date(2024, 6, 3)])
    clean = make_code('600009.SH', quotes=[date(2024, 6, 3)])
    engine = make_engine([defect, st_only, fund, clean])
    summary = engine.summary_at('2024-06-03')

    assert summary['as_of'] == '2024-06-03'
    assert summary['universe']['codes'] == 4
    assert summary['layers']['data_defect']['codes'] == 1
    assert summary['layers']['st']['codes'] == 1
    assert summary['layers']['pending']['codes'] == 1
    assert summary['layers']['remaining']['codes'] == 1
    assert sum(summary['intersection_matrix'].values()) == 4
    assert summary['views']['strict']['codes'] == 1
    assert summary['strict_year_distribution']['2015-2019']['strict'] == 1
    assert summary['strict_year_distribution']['2015-2019']['full'] == 3
    assert 0.0 <= summary['strict_rows_pct'] <= 1.0


def test_finalize_structural_marks_hard_defects() -> None:
    """结构性汇总：无行情、内容、因子、重复键。"""
    no_local = make_code('000030.SZ', start=None, end=None, quotes=[])
    no_local.start_ord = None
    no_local.end_ord = None
    content = make_code(
        '000031.SZ', quotes=[date(2024, 6, 3)], invalid_ohlc=1
    )
    factor_bad = make_code(
        '000032.SZ', quotes=[date(2024, 6, 3)], factor_rows=0
    )
    irreversible = make_code(
        '000033.SZ', quotes=[date(2024, 6, 3)], factor_ratio_dev=1e-6
    )
    duplicate = make_code('000034.SZ', quotes=[date(2024, 6, 3)])
    index = make_code(
        '000035.SH', data_type='index', quotes=[date(2024, 6, 3)], factor_rows=0
    )
    engine = make_engine(
        [no_local, content, factor_bad, irreversible, duplicate, index],
        dup_exdiv_codes={'000034.SZ'},
    )

    engine._finalize_structural()

    assert 'structural_no_local_quote' in no_local.structural
    assert 'structural_content_inconsistent' in content.structural
    assert 'structural_factor_bad' in factor_bad.structural
    assert 'structural_factor_irreversible' in irreversible.structural
    assert 'structural_duplicate_exdiv_key' in duplicate.structural
    assert 'structural_factor_bad' not in index.structural


def test_date_and_code_helpers() -> None:
    """日期解析与代码类型推断。"""
    assert _as_date('2024-06-03') == date(2024, 6, 3)
    assert _as_date('20240603') == date(2024, 6, 3)
    assert _as_date(date(2024, 6, 3)) == date(2024, 6, 3)
    assert _as_date(date(2024, 6, 3).toordinal()) == date(2024, 6, 3)
    assert _code_type('000300.SH') == 'index'
    assert _code_type('510300.SH') == 'fund'
    assert _code_type('600000.SH') == 'stock'


def test_fund_etf_snapshot_preferred(monkeypatch, tmp_path) -> None:
    """存在本地 fund_basic.parquet 时只读快照，不连远程库。"""
    import polars as pl

    snapshot = tmp_path / 'fund_basic.parquet'
    pl.DataFrame({
        'ts_code': ['510300.SH', '161907.SZ'],
        'fund_type': ['ETF', 'LOF'],
    }).write_parquet(snapshot)
    monkeypatch.setattr(
        'src.data.quality.clean_set.FUND_BASIC_SNAPSHOT', snapshot)

    def boom() -> set[str]:
        raise AssertionError('快照存在时不应连接远程库')

    monkeypatch.setattr(
        'src.data.quality.clean_set._fund_etf_codes_from_db', boom)
    assert _load_fund_etf_codes() == {'510300.SH'}


def _write_dividend(root: Path, rows: list[dict]) -> None:
    import polars as pl

    target = root / 'factors'
    target.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(target / 'stock_dividend.parquet')


def test_same_day_complementary_rows_are_not_duplicate(tmp_path) -> None:
    """同日「配股」与「送股」来自两个源各记一半，属互补行，不是重复键。"""
    root = tmp_path / 'store'
    _write_dividend(root, [
        {'ts_code': '000002.SZ', 'ex_date': date(1991, 6, 8), 'cash_div': 0.0,
         'share_div': 0.0, 'rights_issue_ratio': 0.5, 'rights_issue_price': 4.4},
        {'ts_code': '000002.SZ', 'ex_date': date(1991, 6, 8), 'cash_div': 0.0,
         'share_div': 0.2, 'rights_issue_ratio': 0.0, 'rights_issue_price': None},
    ])
    engine = object.__new__(EligibilityEngine)
    engine.root = root
    engine._load_dividend_flags()
    assert engine.dup_exdiv_codes == set()


def test_same_day_overlapping_rows_are_duplicate(tmp_path) -> None:
    """同日在两个源都记了现金分红 → 权益字段重叠，仍是重复键。"""
    root = tmp_path / 'store'
    _write_dividend(root, [
        {'ts_code': '600000.SH', 'ex_date': date(2001, 6, 8), 'cash_div': 0.2,
         'share_div': 0.0, 'rights_issue_ratio': 0.0, 'rights_issue_price': None},
        {'ts_code': '600000.SH', 'ex_date': date(2001, 6, 8), 'cash_div': 0.2,
         'share_div': 0.0, 'rights_issue_ratio': 0.0, 'rights_issue_price': None},
    ])
    engine = object.__new__(EligibilityEngine)
    engine.root = root
    engine._load_dividend_flags()
    assert engine.dup_exdiv_codes == {'600000.SH'}


def test_neutral_rights_event_is_not_jump_mismatch(tmp_path) -> None:
    """配股价≈市价时理论因子为 1，不应算「事件与跳变不匹配」。"""
    import polars as pl

    root = tmp_path / 'store'
    qdir = root / 'quotes' / 'stock' / 'sh'
    qdir.mkdir(parents=True, exist_ok=True)
    days = [date(2020, 1, 2), date(2020, 1, 3), date(2020, 1, 6)]
    pl.DataFrame({
        'date': days,
        'close': [10.0, 10.0, 10.0],
    }).write_parquet(qdir / '600000.SH.parquet')

    engine = object.__new__(EligibilityEngine)
    engine.root = root
    info = make_code('600000.SH', quotes=days, market='sh')
    events = pl.DataFrame([
        {'ex_date': days[1], 'cash_div': 0.0, 'share_div': 0.0,
         'rights_issue_ratio': 0.3, 'rights_issue_price': 10.0,
         'no_ex_rights': False},
        {'ex_date': days[2], 'cash_div': 1.0, 'share_div': 0.0,
         'rights_issue_ratio': 0.0, 'rights_issue_price': None,
         'no_ex_rights': False},
    ])
    missing = np.array([info.factor_dates[1], info.factor_dates[2]], dtype=np.int64)
    kept = engine._drop_neutral_missing(
        info, info.factor_dates, missing, events)
    # 配股日被剔除，现金分红日仍要求有跳变
    assert kept.tolist() == [info.factor_dates[2]]


def test_fund_etf_cache_fresh_skips_db(monkeypatch, tmp_path) -> None:
    """缓存未过期时直接返回，不连远程库。"""
    cache = tmp_path / 'fund_etf_codes.txt'
    cache.write_text('510300.SH\n159915.SZ\n', encoding='utf-8')
    monkeypatch.setattr(
        'src.data.quality.clean_set.FUND_ETF_CODES_CACHE', cache)

    def boom() -> set[str]:
        raise AssertionError('缓存有效时不应连接远程库')

    monkeypatch.setattr(
        'src.data.quality.clean_set._fund_etf_codes_from_db', boom)
    assert _load_fund_etf_codes() == {'510300.SH', '159915.SZ'}


def test_fund_etf_cache_expired_refreshes_and_rewrites(
    monkeypatch, tmp_path
) -> None:
    """缓存过期后连库刷新，并把新名单回写本地。"""
    cache = tmp_path / 'fund_etf_codes.txt'
    cache.write_text('510300.SH\n', encoding='utf-8')
    stale = time.time() - 30 * 86400
    os.utime(cache, (stale, stale))
    monkeypatch.setattr(
        'src.data.quality.clean_set.FUND_ETF_CODES_CACHE', cache)
    monkeypatch.setattr(
        'src.data.quality.clean_set._fund_etf_codes_from_db',
        lambda: {'510300.SH', '588000.SH'})

    assert _load_fund_etf_codes() == {'510300.SH', '588000.SH'}
    assert cache.read_text(encoding='utf-8').split() == [
        '510300.SH', '588000.SH']


def test_fund_etf_cache_expired_falls_back_on_db_error(
    monkeypatch, tmp_path
) -> None:
    """缓存过期且远程失败时沿用旧名单，不降级为 none。"""
    cache = tmp_path / 'fund_etf_codes.txt'
    cache.write_text('510300.SH\n', encoding='utf-8')
    stale = time.time() - 30 * 86400
    os.utime(cache, (stale, stale))
    monkeypatch.setattr(
        'src.data.quality.clean_set.FUND_ETF_CODES_CACHE', cache)

    def boom() -> set[str]:
        raise ConnectionError('offline')

    monkeypatch.setattr(
        'src.data.quality.clean_set._fund_etf_codes_from_db', boom)
    assert _load_fund_etf_codes() == {'510300.SH'}
