#!/usr/bin/env python3

"""不可用标的排除清单：幂等生成、is_tradable 边界、空清单。"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from src.data.quality.tradable_universe import (
    EXCLUSION_SCHEMA,
    EXCLUSIONS_FILE,
    PRICE_JUMPS_FILE,
    SUMMARY_FILE,
    build_exclusions,
    exclusion_reasons,
    is_tradable,
    load_exclusions,
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _make_store(root: Path) -> None:
    (root / 'meta').mkdir(parents=True)
    (root / 'factors').mkdir(parents=True)
    (root / 'quotes' / 'stock' / 'sh').mkdir(parents=True)
    (root / 'quotes' / 'index' / 'sh').mkdir(parents=True)
    (root / 'quotes' / 'fund' / 'sh').mkdir(parents=True)
    (root / 'quotes' / 'fund' / 'sz').mkdir(parents=True)

    dates = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7),
             date(2026, 1, 8), date(2026, 1, 9), date(2026, 1, 10)]
    di_rows = {
        'ts_code': ['600000.SH', '000001.SH', '510000.SH', '159041.SZ'],
        'data_type': ['stock', 'index', 'fund', 'fund'],
        'rows': [len(dates)] * 4,
        'start': [dates[0]] * 4,
        'end': [dates[-1]] * 4,
    }
    pl.DataFrame(di_rows).write_parquet(root / 'meta' / 'daily_index.parquet')

    frame = pl.DataFrame({
        'date': dates,
        'open': [10.0, 10.2, 10.1, 10.3, 10.4, 10.5],
        'high': [10.0, 10.2, 10.1, 10.3, 10.4, 10.5],
        'low': [10.0, 10.2, 10.1, 10.3, 10.4, 10.5],
        'close': [10.0, 10.2, 10.1, 10.3, 10.4, 10.5],
        'pre_close': [None, 10.0, 10.2, 10.1, 10.3, 10.4],
        'qfq_factor': [1.0] * len(dates),
        'hfq_factor': [1.0] * len(dates),
    })
    frame.write_parquet(root / 'quotes' / 'stock' / 'sh' / '600000.SH.parquet')
    pl.DataFrame({'date': dates,
                  'open': [3000.0] * len(dates), 'high': [3000.0] * len(dates),
                  'low': [3000.0] * len(dates), 'close': [3000.0] * len(dates),
                  'pre_close': [None] + [3000.0] * (len(dates) - 1),
                  'qfq_factor': [1.0] * len(dates),
                  'hfq_factor': [1.0] * len(dates)}).write_parquet(
        root / 'quotes' / 'index' / 'sh' / '000001.SH.parquet')
    pl.DataFrame({'date': dates,
                  'open': [1.0] * len(dates), 'high': [1.0] * len(dates),
                  'low': [1.0] * len(dates), 'close': [1.0] * len(dates),
                  'pre_close': [None] + [1.0] * (len(dates) - 1),
                  'qfq_factor': [1.0] * len(dates),
                  'hfq_factor': [1.0] * len(dates)}).write_parquet(
        root / 'quotes' / 'fund' / 'sh' / '510000.SH.parquet')

    jump_dates = dates + [date(2026, 1, 12)]
    jump_close = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 3.0]
    pl.DataFrame({
        'date': jump_dates,
        'open': jump_close,
        'high': jump_close,
        'low': jump_close,
        'close': jump_close,
        'pre_close': [None] + jump_close[:-1],
        'qfq_factor': [1.0] * len(jump_dates),
        'hfq_factor': [1.0] * len(jump_dates),
    }).write_parquet(root / 'quotes' / 'fund' / 'sz' / '159041.SZ.parquet')

    pl.DataFrame({
        'ts_code': ['159041.SZ'],
        'ex_date': [date(2020, 1, 1)],
        'report_date': ['2020-01-01'],
        'share_div': [0.0],
    }).write_parquet(root / 'factors' / 'stock_dividend.parquet')


BASIC_TABLES = {
    'stock': ['600000.SH', '600001.SH'],
    'index': ['000001.SH'],
    'fund': ['510000.SH', '159041.SZ'],
}


def test_build_is_idempotent(tmp_path) -> None:
    store = tmp_path / 'store'
    _make_store(store)
    out = tmp_path / 'universe'
    build_exclusions(store, basic_tables=BASIC_TABLES, output_dir=out,
                     as_of=date(2026, 2, 1))
    first = {name: _sha(out / name) for name in
             (EXCLUSIONS_FILE, SUMMARY_FILE, PRICE_JUMPS_FILE)}
    build_exclusions(store, basic_tables=BASIC_TABLES, output_dir=out,
                     as_of=date(2026, 2, 1))
    second = {name: _sha(out / name) for name in first}
    assert first == second

    summary = json.loads((out / SUMMARY_FILE).read_text(encoding='utf-8'))
    reasons = {key: value['codes'] for key, value in summary['reasons'].items()}
    assert reasons['placeholder_price'] == 1
    assert reasons['stale_feed'] == 4
    assert reasons['no_quotes'] == 1
    assert reasons['bad_price_jump'] == 1
    assert summary['price_jumps']['by_label']['data_error']['rows'] == 1
    # 2026-11-10 起对外口径只保留 strict，旧键 healthy_by_audit_definition 不再输出
    coverage = summary['coverage_after_exclusion']
    assert 'strict' in coverage
    assert 'healthy_by_audit_definition' not in coverage
    assert set(coverage) == {'block_only', 'all_reasons', 'strict'}


def test_is_tradable_boundaries(tmp_path) -> None:
    store = tmp_path / 'store'
    _make_store(store)
    out = tmp_path / 'universe'
    build_exclusions(store, basic_tables=BASIC_TABLES, output_dir=out,
                     as_of=date(2026, 2, 1))
    exclusions = load_exclusions(out)

    # 停更标的：as_of 早于最后交易日时不算 stale
    assert is_tradable('600000.SH', date(2026, 1, 6), exclusions=exclusions)
    assert not is_tradable('600000.SH', date(2026, 2, 1), exclusions=exclusions)
    # 占位行情永久不可用
    assert not is_tradable('510000.SH', date(2026, 1, 6), exclusions=exclusions)
    # 基础表有、本地无行情
    assert not is_tradable('600001.SH', date(2026, 1, 6), exclusions=exclusions)
    # 跳变错误只在当天不可用
    assert not is_tradable('159041.SZ', date(2026, 1, 12), exclusions=exclusions)
    assert is_tradable('159041.SZ', date(2026, 1, 9), exclusions=exclusions)
    # 严格口径把历史缺口也算上
    reasons = exclusion_reasons('600000.SH', date(2026, 1, 6), exclusions=exclusions)
    assert all(item['reason_code'] != 'stale_feed' for item in reasons)


def test_empty_manifest_is_safe(tmp_path) -> None:
    empty = tmp_path / 'empty'
    frame = load_exclusions(empty)
    assert frame.height == 0
    assert set(frame.columns) == set(EXCLUSION_SCHEMA)
    assert is_tradable('600000.SH', date(2026, 1, 6), root=empty)
    assert exclusion_reasons('600000.SH', root=empty) == []


@pytest.mark.parametrize('ts_code', ['600000.SH', '510000.SH', '600001.SH'])
def test_empty_manifest_unknown_codes(tmp_path, ts_code: str) -> None:
    assert is_tradable(ts_code, root=tmp_path / 'missing')


# ----------------------------------------------------------------------
# bad_ohlc：逐行登记、不整只排除，占比过高才停用
# ----------------------------------------------------------------------
LEGAL_OHLC = {'open': 1000.0, 'high': 1010.0, 'low': 990.0, 'close': 1000.0}


def _ohlc_store(tmp_path: Path, rows: list[dict]) -> Path:
    store = tmp_path / 'ohlc_store'
    (store / 'meta').mkdir(parents=True)
    dates = [item['date'] for item in rows]
    pl.DataFrame({
        'ts_code': ['000010.SH'], 'data_type': ['index'],
        'rows': [len(dates)], 'start': [dates[0]], 'end': [dates[-1]],
    }).write_parquet(store / 'meta' / 'daily_index.parquet')
    path = store / 'quotes' / 'index' / 'sh' / '000010.SH.parquet'
    path.parent.mkdir(parents=True)
    pl.DataFrame({
        'date': dates,
        'open': [item['open'] for item in rows],
        'high': [item['high'] for item in rows],
        'low': [item['low'] for item in rows],
        'close': [item['close'] for item in rows],
        'pre_close': [None] + [rows[i - 1]['close'] for i in range(1, len(rows))],
        'qfq_factor': [1.0] * len(rows),
        'hfq_factor': [1.0] * len(rows),
    }).write_parquet(path)
    return store


@pytest.mark.parametrize(
    ('expected_label', 'bad_row'),
    [
        ('high<low', {'open': 1000.0, 'high': 990.0, 'low': 1010.0, 'close': 1000.0}),
        ('high<max(open,close)', {'open': 1000.0, 'high': 1005.0,
                                  'low': 990.0, 'close': 1010.0}),
        ('low>min(open,close)', {'open': 1000.0, 'high': 1010.0,
                                 'low': 995.0, 'close': 990.0}),
        ('open<=0', {'open': 0.0, 'high': 1000.0, 'low': 990.0, 'close': 990.0}),
        ('close<=0', {'open': 1000.0, 'high': 1000.0, 'low': 990.0, 'close': 0.0}),
    ],
)
def test_bad_ohlc_flags_each_type(tmp_path, expected_label: str, bad_row: dict) -> None:
    rows = [
        {'date': date(2026, 1, 5), **LEGAL_OHLC},
        {'date': date(2026, 1, 6), **bad_row},
        {'date': date(2026, 1, 7), **LEGAL_OHLC},
    ]
    out = tmp_path / 'universe'
    build_exclusions(_ohlc_store(tmp_path, rows), output_dir=out, as_of=date(2026, 1, 7))
    bad = load_exclusions(out).filter(pl.col('reason_code') == 'bad_ohlc')
    row_level = bad.filter(pl.col('expected_rows') == 1)
    assert row_level.height == 1
    assert row_level['last_trade_date'][0] == date(2026, 1, 6)
    assert expected_label in row_level['reason_text'][0]
    # 1/3 = 33% > 20%，会额外登记一条整只停用汇总行
    assert bad.height == 2


def test_bad_ohlc_does_not_flag_legal_rows(tmp_path) -> None:
    rows = [
        {'date': date(2026, 1, 5), **LEGAL_OHLC},
        {'date': date(2026, 1, 6), 'open': 1000.0, 'high': 1000.0,
         'low': 1000.0, 'close': 1000.0},
        {'date': date(2026, 1, 7), 'open': 1010.0, 'high': 1010.0,
         'low': 990.0, 'close': 990.0},
    ]
    out = tmp_path / 'universe'
    build_exclusions(_ohlc_store(tmp_path, rows), output_dir=out, as_of=date(2026, 1, 7))
    bad = load_exclusions(out).filter(pl.col('reason_code') == 'bad_ohlc')
    assert bad.height == 0


def test_bad_ohlc_row_does_not_exclude_whole_code(tmp_path) -> None:
    rows = [{'date': date(2026, 1, 5), **LEGAL_OHLC}]
    rows += [{'date': date(2026, 1, 6 + i), **LEGAL_OHLC} for i in range(5)]
    rows.append({'date': date(2026, 1, 11), 'open': 1000.0, 'high': 990.0,
                 'low': 1010.0, 'close': 1000.0})
    out = tmp_path / 'universe'
    build_exclusions(_ohlc_store(tmp_path, rows), output_dir=out, as_of=date(2026, 1, 11))
    exclusions = load_exclusions(out)
    bad = exclusions.filter(pl.col('reason_code') == 'bad_ohlc')
    assert bad.height == 1
    reasons = exclusion_reasons('000010.SH', date(2026, 1, 11), exclusions=exclusions)
    assert [item['reason_code'] for item in reasons] == ['bad_ohlc']
    assert is_tradable('000010.SH', date(2026, 1, 11), exclusions=exclusions)
    assert is_tradable('000010.SH', date(2026, 1, 5), exclusions=exclusions)


def test_bad_ohlc_high_share_excludes_code_and_is_idempotent(tmp_path) -> None:
    rows = [{'date': date(2026, 1, 5 + i), **LEGAL_OHLC} for i in range(7)]
    for i in range(3):
        rows.append({'date': date(2026, 1, 12 + i), 'open': 1000.0,
                     'high': 990.0, 'low': 1010.0, 'close': 1000.0})
    store = _ohlc_store(tmp_path, rows)
    out = tmp_path / 'universe'
    build_exclusions(store, output_dir=out, as_of=date(2026, 1, 14))
    first = _sha(out / EXCLUSIONS_FILE)
    build_exclusions(store, output_dir=out, as_of=date(2026, 1, 14))
    assert _sha(out / EXCLUSIONS_FILE) == first

    exclusions = load_exclusions(out)
    bad = exclusions.filter(pl.col('reason_code') == 'bad_ohlc')
    # 3 行级 + 1 条整只停用汇总行（3/10 = 30% > 20%）
    assert bad.height == 4
    aggregate = bad.filter(pl.col('expected_rows') > 1)
    assert aggregate.height == 1
    assert aggregate['expected_rows'][0] == 10
    assert aggregate['missing_rows'][0] == 3
    assert is_tradable('000010.SH', date(2026, 1, 5), exclusions=exclusions) is False


# ----------------------------------------------------------------------
# strict 口径：通达信自编指数（880*/881*）的 internal_gap 只记录、不剔除
# ----------------------------------------------------------------------
def _exclusion_frame(items: list[dict]) -> pl.DataFrame:
    base = {
        'reason_text': '',
        'last_trade_date': None,
        'expected_rows': 0,
        'actual_rows': 0,
        'missing_rows': 0,
        'severity': 'warn',
        'detected_at': date(2026, 2, 1),
    }
    return pl.DataFrame([{**base, **item} for item in items], schema=EXCLUSION_SCHEMA)


def test_strict_exempts_synthetic_index_internal_gap() -> None:
    frame = _exclusion_frame([
        {'ts_code': '880001.SH', 'asset_class': 'index', 'reason_code': 'internal_gap'},
        {'ts_code': '881001.SH', 'asset_class': 'index', 'reason_code': 'internal_gap'},
        {'ts_code': '000001.SH', 'asset_class': 'index', 'reason_code': 'internal_gap'},
        {'ts_code': '399001.SZ', 'asset_class': 'index', 'reason_code': 'internal_gap'},
        {'ts_code': '600000.SH', 'asset_class': 'stock', 'reason_code': 'internal_gap'},
    ])
    # 880/881 自编指数：strict 下不再因 internal_gap 被剔除
    for code in ('880001.SH', '881001.SH'):
        assert is_tradable(code, date(2026, 2, 1), exclusions=frame)
        assert is_tradable(code, date(2026, 2, 1), strict=True, exclusions=frame)
    # 真实指数与个股的 internal_gap 照旧生效
    for code in ('000001.SH', '399001.SZ', '600000.SH'):
        assert is_tradable(code, date(2026, 2, 1), exclusions=frame)
        assert not is_tradable(code, date(2026, 2, 1), strict=True, exclusions=frame)


def test_strict_keeps_other_warnings_on_synthetic_index() -> None:
    frame = _exclusion_frame([
        {'ts_code': '880001.SH', 'asset_class': 'index', 'reason_code': 'internal_gap'},
        {'ts_code': '880001.SH', 'asset_class': 'index', 'reason_code': 'no_basic_info'},
    ])
    assert is_tradable('880001.SH', date(2026, 2, 1), exclusions=frame)
    assert not is_tradable('880001.SH', date(2026, 2, 1), strict=True, exclusions=frame)


def test_synthetic_index_prefix_requires_index_asset_class() -> None:
    # 同前缀但非指数（异常数据）不享受豁免
    frame = _exclusion_frame([
        {'ts_code': '880001.SH', 'asset_class': 'stock', 'reason_code': 'internal_gap'},
    ])
    assert not is_tradable('880001.SH', date(2026, 2, 1), strict=True, exclusions=frame)


# ----------------------------------------------------------------------
# TDX 对账：只有交易日历内的 bar 才构成「本地缺口」
# ----------------------------------------------------------------------
def _write_tdx_day(path: Path, days: list[date]) -> None:
    """按通达信 32 字节 lday 布局写日期字段（其余字段留零）。"""
    from src.data.quality.tradable_universe import _DAY_DTYPE  # noqa: PLC0415

    raw = np.zeros(len(days), dtype=_DAY_DTYPE)
    raw['date'] = [int(d.strftime('%Y%m%d')) for d in days]
    path.parent.mkdir(parents=True, exist_ok=True)
    raw.tofile(path)


def _patch_calendar(monkeypatch, days: list[date]) -> None:
    import src.data.quality.tradable_universe as tu  # noqa: PLC0415

    calendar = np.array(
        [np.datetime64(d.isoformat(), 'D').astype(np.int64) for d in days],
        dtype=np.int64,
    )
    monkeypatch.setattr(tu, '_benchmark_calendar', lambda _root: calendar)


def _tdx_store(tmp_path: Path, local_days: list[date]) -> Path:
    """最小列存：单只 600000.SH 的行情 + daily_index。"""
    store = tmp_path / 'store'
    (store / 'meta').mkdir(parents=True)
    pl.DataFrame({
        'ts_code': ['600000.SH'], 'data_type': ['stock'],
        'rows': [len(local_days)], 'start': [local_days[0]], 'end': [local_days[-1]],
    }).write_parquet(store / 'meta' / 'daily_index.parquet')
    closes = [10.0] * len(local_days)
    path = store / 'quotes' / 'stock' / 'sh' / '600000.SH.parquet'
    path.parent.mkdir(parents=True)
    pl.DataFrame({
        'date': local_days, 'open': closes, 'high': closes, 'low': closes,
        'close': closes, 'pre_close': [None] + closes[:-1],
        'qfq_factor': [1.0] * len(local_days), 'hfq_factor': [1.0] * len(local_days),
    }).write_parquet(path)
    return store


#: 2026-01-05(一) ~ 01-09(五) + 01-12(一)：跨一个周末的 6 个交易日
TRADING = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7),
           date(2026, 1, 8), date(2026, 1, 9), date(2026, 1, 12)]
#: 2026-01-10(六) / 01-11(日)
WEEKEND = [date(2026, 1, 10), date(2026, 1, 11)]


def test_tdx_bars_on_non_trading_days_are_not_internal_gap(
        tmp_path, monkeypatch) -> None:
    """TDX 写在周末（非交易日）的 bar 不得计成「本地缺口」。

    现实成因：通达信早期日期错位，1993-05-01（周六）/05-02（周日）写出 bar，
    同时漏掉 05-03、05-06；不过滤会把 000001.SZ、000002.SZ 误判 internal_gap，
    再经 strict 口径移出可交易池。这里把两根周末 bar 放在本地区间内部，
    与真实形态一致（本地 04-30 与 05-03 之间恰好夹着那两根）。
    """
    store = _tdx_store(tmp_path, TRADING)
    tdx_root = tmp_path / 'tdx'
    _write_tdx_day(tdx_root / 'sh' / 'lday' / 'sh600000.day',
                   [d for d in TRADING if d < WEEKEND[0]] + WEEKEND
                   + [d for d in TRADING if d > WEEKEND[1]])
    _patch_calendar(monkeypatch, TRADING)

    out = tmp_path / 'universe'
    build_exclusions(store, tdx_root=tdx_root, output_dir=out, as_of=date(2026, 2, 1))
    exclusions = load_exclusions(out)
    gaps = exclusions.filter((pl.col('reason_code') == 'internal_gap')
                             & (pl.col('ts_code') == '600000.SH'))
    assert gaps.height == 0
    assert is_tradable('600000.SH', date(2026, 1, 6), strict=True,
                       exclusions=exclusions)


def test_tdx_missing_real_trading_day_still_flagged(
        tmp_path, monkeypatch) -> None:
    """日历内的真实缺口仍必须登记（防止过滤把真缺口一起吞掉）。"""
    local_days = [d for d in TRADING if d != date(2026, 1, 7)]
    store = _tdx_store(tmp_path, local_days)
    tdx_root = tmp_path / 'tdx'
    _write_tdx_day(tdx_root / 'sh' / 'lday' / 'sh600000.day', TRADING)
    _patch_calendar(monkeypatch, TRADING)

    out = tmp_path / 'universe'
    build_exclusions(store, tdx_root=tdx_root, output_dir=out, as_of=date(2026, 2, 1))
    exclusions = load_exclusions(out)
    gaps = exclusions.filter((pl.col('reason_code') == 'internal_gap')
                             & (pl.col('ts_code') == '600000.SH'))
    assert gaps.height == 1
    assert gaps['missing_rows'][0] == 1
    assert gaps['expected_rows'][0] == 6
    assert gaps['last_trade_date'][0] == date(2026, 1, 12)


# ----------------------------------------------------------------------
# manifest 漏登记：必须报错，不得静默跳过
# ----------------------------------------------------------------------
def test_manifest_missing_registration_raises(tmp_path) -> None:
    """写盘产物未在 manifest 登记时必须报错（DG-3）。

    回归点：``refresh_manifest_files`` 默认 ``required=False``，找不到登记条目时
    ``continue`` 静默放过，``universe_exclusions.json`` 因此长期进不了 ``files``。
    这里只登记两张 parquet、故意漏掉 JSON 汇总，断言构建直接失败。
    """
    store = tmp_path / 'store'
    _make_store(store)
    out = store / 'meta' / 'universe'
    out.mkdir(parents=True)
    (out / 'manifest.json').write_text(json.dumps({
        'dataset': 'test',
        'files': [
            {'path': f'meta/universe/{EXCLUSIONS_FILE}',
             'rows': 0, 'bytes': 0, 'sha256': ''},
            {'path': f'meta/universe/{PRICE_JUMPS_FILE}',
             'rows': 0, 'bytes': 0, 'sha256': ''},
        ],
    }, ensure_ascii=False), encoding='utf-8')

    with pytest.raises(RuntimeError, match=SUMMARY_FILE):
        build_exclusions(store, basic_tables=BASIC_TABLES, output_dir=out,
                         as_of=date(2026, 2, 1))
