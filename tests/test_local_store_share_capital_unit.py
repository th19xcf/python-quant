"""share_capital 模块单测：读取缓存、单位换算、选源拼接、倒推与批量采集。

全部使用临时目录 + 合成 parquet + 注入 fake ``akshare``，不访问网络、不读
``data/`` 正式表。重点覆盖生效日边界（``effective_date <= as_of``）、
万股→股换算、两源锚点拼接与断点续跑的 chunk_status。
"""

from __future__ import annotations

import json
import os
import sys
import types
from datetime import date, datetime

import pandas as pd
import polars as pl
import pytest

import src.data.local_store.share_capital as share_module
import src.utils.exception_handler as exc_module
from src.data.local_store.share_capital import (
    SHARE_COLUMNS,
    SHARE_SCHEMA,
    SOURCE_CNINFO,
    SOURCE_DERIVED,
    SOURCE_EM,
    ShareSnapshot,
    _as_date,
    _close,
    _collapse_steps,
    _float,
    _frame_from_rows,
    _load_json,
    _normalize_frame,
    _opt_int,
    _text,
    _to_shares,
    _write_failed_csv,
    _write_json,
    _write_part,
    assemble_shares,
    choose_share_series,
    collect_one,
    collect_shares,
    coverage_report,
    default_work_dir,
    derive_share_series,
    fetch_cninfo_history,
    fetch_current_info,
    fetch_em_history,
    fetch_market_snapshot,
    load_dividend_events,
    normalize_rows,
    read_shares,
    shares_as_of,
    shares_frame,
    shares_path,
    snapshot_error,
    stitch_legacy_series,
    write_shares,
)
from src.utils.exceptions import DataSourceConnectionError

_TODAY = date(2026, 11, 12)
_FETCHED = datetime(2026, 11, 12, 9, 0)


def _row(ts_code: str, effective: date, total: int,
         float_share: int | None = None,
         free: int | None = None) -> dict:
    return {
        'ts_code': ts_code,
        'effective_date': effective,
        'total_share': total,
        'float_share': float_share,
        'free_float_share': free,
        'change_reason': None,
        'source': 'test',
        'fetched_at': _FETCHED,
    }


def _frame(rows: list[dict]) -> pl.DataFrame:
    return _frame_from_rows(rows)


def _install_akshare(monkeypatch, **attrs) -> types.SimpleNamespace:
    fake = types.SimpleNamespace(**attrs)
    monkeypatch.setitem(sys.modules, 'akshare', fake)
    return fake


# ----------------------------------------------------------------------
# 路径、读取与点时查询
# ----------------------------------------------------------------------
def test_paths_and_default_work_dir(tmp_path):
    assert shares_path(tmp_path) == (
        tmp_path / 'meta' / 'factors' / 'stock_shares.parquet')
    assert default_work_dir().parts[-2:] == ('tmp', 'stock_shares')


def test_read_shares_cache_and_lru(tmp_path):
    root = tmp_path / 'root'
    assert read_shares(root=root) is None
    assert read_shares('600000.SH', root) is None

    write_shares(_frame([
        _row('600000.SH', date(2020, 1, 1), 100, 80),
        _row('600000.SH', date(2021, 1, 1), 200, 160),
        _row('000001.SZ', date(2021, 1, 1), 300, 240),
    ]), root, write_meta=False)

    all_rows = read_shares(root=root)
    assert all_rows is not None and all_rows.height == 3
    one = read_shares('600000.SH', root)
    assert one is not None
    assert one['effective_date'].to_list() == [date(2020, 1, 1), date(2021, 1, 1)]
    assert one.columns == list(SHARE_COLUMNS)
    # 缓存命中
    assert read_shares('600000.SH', root).equals(one)
    # 未知代码缓存 None
    assert read_shares('999999.SH', root) is None
    assert read_shares('999999.SH', root) is None
    # 只取指定列（换独立文件避开按 ts_code 缓存）
    subset_root = tmp_path / 'subset'
    write_shares(_frame([_row('600000.SH', date(2020, 1, 1), 100)]),
                 subset_root, write_meta=False)
    subset = read_shares('600000.SH', subset_root,
                         columns=['ts_code', 'effective_date', 'total_share'])
    assert subset is not None
    assert subset.columns == ['ts_code', 'effective_date', 'total_share']

    # mtime 变化 → 失效重读
    path = shares_path(root)
    future = path.stat().st_mtime_ns + 10 ** 9
    os.utime(path, ns=(future, future))
    assert read_shares('600000.SH', root) is not None

    # LRU 上限
    many = [_row(f'{i:06d}.SZ', date(2021, 1, 1), 100 + i) for i in range(140)]
    other_root = tmp_path / 'many'
    write_shares(_frame(many), other_root, write_meta=False)
    for row in many:
        read_shares(row['ts_code'], other_root)
    assert len(share_module._SHARES_CACHE) <= share_module._LRU_MAX


def test_read_shares_stat_oserror(tmp_path, monkeypatch):
    root = tmp_path / 'root'
    write_shares(_frame([_row('600000.SH', date(2020, 1, 1), 100)]), root,
                 write_meta=False)

    class BadPath:
        def exists(self):
            return True

        def stat(self):
            raise OSError('gone')

    monkeypatch.setattr(
        share_module, 'shares_path', lambda *_a, **_k: BadPath())
    assert read_shares('600000.SH', root) is None


def test_shares_frame_and_as_of(tmp_path):
    root = tmp_path / 'root'
    write_shares(_frame([
        _row('600000.SH', date(2020, 1, 1), 100, 80, 60),
        _row('600000.SH', date(2021, 1, 1), 200, 160),
    ]), root, write_meta=False)
    assert shares_frame('600000', root) is not None
    assert shares_frame('999999', root) is None

    before = shares_as_of('600000.SH', '2019-12-31', root)
    assert before is None

    on_first = shares_as_of('600000.SH', date(2020, 1, 1), root)
    assert on_first == ShareSnapshot(
        '600000.SH', date(2020, 1, 1), 100, 80, 60)

    # 生效日边界：前一天仍取旧值，当天即见新值
    assert shares_as_of('600000.SH', '2020-12-31', root).total_share == 100
    latest = shares_as_of('600000.SH', '2021-01-01', root)
    assert latest.total_share == 200
    assert latest.free_float_share is None

    assert shares_as_of('600000.SH', 'not-a-date', root) is None
    assert shares_as_of('000001.SZ', '2021-01-01', root) is None


def test_write_shares_fills_schema_and_writes_meta(tmp_path):
    root = tmp_path / 'root'
    raw = pl.DataFrame({
        'ts_code': ['600000.SH'],
        'effective_date': [date(2020, 1, 1)],
        'total_share': [100.7],
        'float_share': [80.2],
        'source': ['test'],
    })
    path = write_shares(raw, root)
    frame = pl.read_parquet(path)
    assert frame.columns == list(SHARE_COLUMNS)
    assert frame.schema['total_share'] == pl.Int64
    assert frame['total_share'][0] == 100
    assert frame['free_float_share'][0] is None
    meta = json.loads(
        path.with_name(share_module.SHARES_META_FILENAME).read_text(encoding='utf-8'))
    assert meta['rows'] == 1
    assert meta['codes'] == 1
    assert meta['sources'][0]['source'] == 'test'


def test_write_meta_empty_frame(tmp_path):
    root = tmp_path / 'root'
    path = write_shares(_frame([]), root)
    meta = json.loads(
        path.with_name(share_module.SHARES_META_FILENAME).read_text(encoding='utf-8'))
    assert meta['rows'] == 0 and meta['codes'] == 0
    assert meta['sources'] == []


# ----------------------------------------------------------------------
# 单位与清洗
# ----------------------------------------------------------------------
def test_opt_int_and_float_and_text():
    assert _opt_int(None) is None
    assert _opt_int('bad') is None
    assert _opt_int(float('nan')) is None
    assert _opt_int(12.9) == 12
    assert _float(None) is None
    assert _float('x') is None
    assert _float(float('inf')) is None
    assert _float('1.5') == 1.5
    assert _text(None) is None
    assert _text(' nan ') is None
    assert _text(' 送股 ') == '送股'


def test_as_date_variants():
    assert _as_date('2024/01/02') == date(2024, 1, 2)
    assert _as_date('') is None
    assert _as_date('nan') is None
    assert _as_date('-') is None
    assert _as_date(None) is None
    assert _as_date(datetime(2024, 1, 2, 3)) == date(2024, 1, 2)


def test_to_shares_and_frame_from_rows():
    assert _to_shares(None) is None
    assert _to_shares('bad') is None
    assert _to_shares(float('nan')) is None
    assert _to_shares(0) is None
    assert _to_shares(-1) is None
    assert _to_shares(1.5) == 2
    assert _to_shares(1.2345, scale=10000) == 12345
    assert _frame_from_rows([]).schema == SHARE_SCHEMA


def test_normalize_frame_and_collapse():
    frame = _normalize_frame(pl.DataFrame({
        'ts_code': ['A'],
        'total_share': [1.2],
    }))
    assert frame.columns == list(SHARE_COLUMNS)
    assert frame.schema['total_share'] == pl.Int64

    rows = [
        {'total_share': 100, 'float_share': 80},
        {'total_share': 100, 'float_share': 80},
        {'total_share': 100, 'float_share': 90},
    ]
    assert len(_collapse_steps(rows)) == 2


def test_normalize_rows_filters_and_dedupes():
    raw = [
        {'变更日期': '2020/01/01', '总股本': 100, '已上市流通A股': 80, '变动原因': '送股'},
        {'变更日期': '2020/01/01', '总股本': 120, '已上市流通A股': 90,
         '变动原因': 'null'},
        {'变更日期': '2020/01/01', '总股本': 90, '已上市流通A股': 70},   # 同日较小值不覆盖
        {'变更日期': '2027/01/01', '总股本': 999},                      # 未来行丢弃
        {'变更日期': 'bad', '总股本': 1},                               # 日期无效
        {'变更日期': '2021/01/01', '总股本': 0},                        # 非正股本
        {'变更日期': '2022/01/01', '总股本': 200, '已上市流通A股': 150,
         '变动原因': '增发'},
    ]
    out = normalize_rows(
        raw, ts_code='600000.SH', date_key='变更日期', total_key='总股本',
        float_key='已上市流通A股', reason_key='变动原因', scale=1.0,
        source=SOURCE_EM, fetched_at=_FETCHED, today=_TODAY)
    assert out['effective_date'].to_list() == [
        date(2020, 1, 1), date(2022, 1, 1)]
    assert out['total_share'].to_list() == [120, 200]
    assert out['change_reason'][0] == 'null'
    assert out['change_reason'][1] == '增发'


def test_normalize_rows_scale_and_no_reason_key():
    raw = [{'变动日期': '2020-01-01', '总股本': 1.5, '已流通股份': 1.2}]
    out = normalize_rows(
        raw, ts_code='600000.SH', date_key='变动日期', total_key='总股本',
        float_key='已流通股份', scale=10000.0, source=SOURCE_CNINFO,
        fetched_at=_FETCHED, today=_TODAY)
    assert out['total_share'][0] == 15000
    assert out['float_share'][0] == 12000
    assert out['change_reason'][0] is None


# ----------------------------------------------------------------------
# 选源与拼接
# ----------------------------------------------------------------------
def test_snapshot_error_paths():
    assert snapshot_error(_frame([]), (100, 80)) is None
    assert snapshot_error(_frame([_row('A', date(2020, 1, 1), 100)]), None) is None
    series = _frame([_row('A', date(2020, 1, 1), 100, 80)])
    assert snapshot_error(series, (110, 80)) == pytest.approx(1 / 11)
    assert snapshot_error(series, (None, 80)) == 0.0
    assert snapshot_error(_frame([_row('A', date(2020, 1, 1), 100, None)]),
                          (100, 80)) == 0.0


def test_close_branches():
    assert _close(None, 100, 0.02) is False
    assert _close(0, 100, 0.02) is False
    assert _close(101, 100, 0.02) is True
    assert _close(103, 100, 0.02) is False


def test_stitch_legacy_series():
    empty = _frame([])
    em = _frame([
        _row('600000.SH', date(1994, 1, 1), 100, 80),
        _row('600000.SH', date(1996, 1, 1), 200, 160),
    ])
    assert stitch_legacy_series(empty, em)[1] is False
    assert stitch_legacy_series(em, empty)[1] is False

    # 无同日一致值 → 不拼接
    cn_diff = _frame([_row('600000.SH', date(1996, 1, 1), 999, 999)])
    stitched, used = stitch_legacy_series(em, cn_diff)
    assert used is False
    assert stitched.equals(em.sort('effective_date'))

    # 锚点 1996-01-01：前缀取巨潮、后缀取东财
    cn = _frame([
        _row('600000.SH', date(1993, 1, 1), 50, 40),
        _row('600000.SH', date(1996, 1, 1), 200, 160),
    ])
    stitched, used = stitch_legacy_series(em, cn)
    assert used is True
    assert stitched['effective_date'].to_list() == [
        date(1993, 1, 1), date(1996, 1, 1)]
    assert stitched['source'].to_list() == ['test', 'test']
    assert stitched['total_share'].to_list() == [50, 200]


def test_choose_share_series_branches():
    em = _frame([_row('A', date(2020, 1, 1), 100, 80)])
    cn = _frame([_row('A', date(2020, 1, 1), 200, 160)])
    assert choose_share_series(_frame([]), cn).equals(cn)
    assert choose_share_series(em, _frame([])).equals(em)
    # 快照缺失 → 用东财
    assert choose_share_series(em, cn, None).equals(em)
    # 东财偏差过大、巨潮更接近 → 用巨潮
    assert choose_share_series(em, cn, (200, 160)).equals(cn)
    # 东财已在容差内 → 仍用东财
    assert choose_share_series(em, cn, (101, 80)).equals(em)


# ----------------------------------------------------------------------
# 倒推
# ----------------------------------------------------------------------
def test_derive_share_series_branches():
    assert derive_share_series(
        'A', list_date=date(2020, 1, 1), current_total=0, current_float=None,
        dividend_events=(), fetched_at=_FETCHED, today=_TODAY).is_empty()

    out = derive_share_series(
        'A', list_date=date(2020, 1, 1), current_total=200, current_float=100,
        dividend_events=[
            (date(2021, 6, 1), 1.0),        # 10 送 10
            (date(2022, 6, 1), 0.0),        # 比例 <= 0 丢弃
            (date(2019, 6, 1), 0.5),        # 早于上市日丢弃
            (date(2027, 6, 1), 0.5),        # 未来事件丢弃
        ], fetched_at=_FETCHED, today=_TODAY)
    assert out['effective_date'].to_list() == [
        date(2020, 1, 1), date(2021, 6, 1)]
    assert out['total_share'].to_list() == [100, 200]
    assert out['float_share'].to_list() == [50, 100]
    assert out['source'].unique().to_list() == [SOURCE_DERIVED]

    # 无流通股本 → float 全 None；最后一行已等于当前值时不重复追加
    none_float = derive_share_series(
        'A', list_date=date(2020, 1, 1), current_total=100, current_float=None,
        dividend_events=(), fetched_at=_FETCHED, today=_TODAY)
    assert none_float['float_share'].to_list() == [None]
    assert none_float['effective_date'].to_list() == [date(2020, 1, 1)]


# ----------------------------------------------------------------------
# 远程抓取（fake akshare）
# ----------------------------------------------------------------------
def test_fetch_em_history_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(exc_module.time, 'sleep', lambda _s: None)
    monkeypatch.setattr(share_module.time, 'sleep', lambda _s: None)
    good = pd.DataFrame({
        '变更日期': ['2020-01-01'],
        '总股本': [100],
        '已上市流通A股': [80],
        '变动原因': ['送股'],
    })
    _install_akshare(monkeypatch, stock_zh_a_gbjg_em=lambda **_k: good)
    out = fetch_em_history('600000.SH', fetched_at=_FETCHED, today=_TODAY,
                           throttle=0.0)
    assert out.height == 1
    assert out['source'][0] == SOURCE_EM

    _install_akshare(monkeypatch, stock_zh_a_gbjg_em=lambda **_k: None)
    assert fetch_em_history(
        '600000.SH', fetched_at=_FETCHED, today=_TODAY, throttle=0.0).is_empty()

    _install_akshare(
        monkeypatch,
        stock_zh_a_gbjg_em=lambda **_k: pd.DataFrame({'x': [1]}))
    assert fetch_em_history(
        '600000.SH', fetched_at=_FETCHED, today=_TODAY, throttle=0.0).is_empty()

    def boom(**_k):
        raise ValueError('down')

    _install_akshare(monkeypatch, stock_zh_a_gbjg_em=boom)
    with pytest.raises(DataSourceConnectionError, match='股本结构'):
        fetch_em_history('600000.SH', fetched_at=_FETCHED, today=_TODAY,
                         throttle=0.0)


def test_fetch_cninfo_history_paths(monkeypatch):
    monkeypatch.setattr(exc_module.time, 'sleep', lambda _s: None)
    monkeypatch.setattr(share_module.time, 'sleep', lambda _s: None)
    captured: dict = {}

    def query(**kwargs):
        captured.update(kwargs)
        return pd.DataFrame({
            '变动日期': ['2020-01-01'],
            '总股本': [1.5],
            '已流通股份': [1.2],
            '变动原因': ['增发'],
        })

    _install_akshare(monkeypatch, stock_share_change_cninfo=query)
    out = fetch_cninfo_history('600000.SH', fetched_at=_FETCHED,
                               today=_TODAY, throttle=0.0)
    assert captured['symbol'] == '600000'
    assert captured['end_date'] == '20261112'
    assert out['total_share'][0] == 15000
    assert out['source'][0] == SOURCE_CNINFO

    def key_error(**_k):
        raise KeyError('公告日期')

    _install_akshare(monkeypatch, stock_share_change_cninfo=key_error)
    assert fetch_cninfo_history(
        '600000.SH', fetched_at=_FETCHED, today=_TODAY,
        throttle=0.0).is_empty()

    def boom(**_k):
        raise RuntimeError('down')

    _install_akshare(monkeypatch, stock_share_change_cninfo=boom)
    with pytest.raises(DataSourceConnectionError, match='股本变动'):
        fetch_cninfo_history('600000.SH', fetched_at=_FETCHED, today=_TODAY,
                             throttle=0.0)

    _install_akshare(monkeypatch, stock_share_change_cninfo=lambda **_k: None)
    assert fetch_cninfo_history(
        '600000.SH', fetched_at=_FETCHED, today=_TODAY,
        throttle=0.0).is_empty()

    _install_akshare(
        monkeypatch,
        stock_share_change_cninfo=lambda **_k: pd.DataFrame({'x': [1]}))
    assert fetch_cninfo_history(
        '600000.SH', fetched_at=_FETCHED, today=_TODAY,
        throttle=0.0).is_empty()


def test_fetch_current_info_paths(monkeypatch):
    monkeypatch.setattr(exc_module.time, 'sleep', lambda _s: None)
    monkeypatch.setattr(share_module.time, 'sleep', lambda _s: None)
    good = pd.DataFrame({'item': ['总股本', '流通股'], 'value': [100, 80]})
    _install_akshare(monkeypatch, stock_individual_info_em=lambda **_k: good)
    assert fetch_current_info('600000.SH', throttle=0.0) == (100, 80)

    _install_akshare(monkeypatch, stock_individual_info_em=lambda **_k: None)
    assert fetch_current_info('600000.SH', throttle=0.0) is None

    _install_akshare(
        monkeypatch,
        stock_individual_info_em=lambda **_k: pd.DataFrame(
            {'item': ['总股本'], 'value': [0]}))
    assert fetch_current_info('600000.SH', throttle=0.0) is None

    def boom(**_k):
        raise RuntimeError('down')

    _install_akshare(monkeypatch, stock_individual_info_em=boom)
    with pytest.raises(DataSourceConnectionError, match='个股信息'):
        fetch_current_info('600000.SH', throttle=0.0)


def test_fetch_market_snapshot_cache_and_filters(tmp_path, monkeypatch):
    spot = pd.DataFrame({
        '代码': ['600000', '000001', '999999', '600001', '600002'],
        '最新价': [10.0, 0.0, 5.0, 5.0, 5.0],
        '总市值': [1e10, 1e9, None, 5e9, -1.0],
        '流通市值': [8e9, None, 1e9, None, 2e9],
    })
    calls = {'n': 0}

    def stock_zh_a_spot_em():
        calls['n'] += 1
        return spot

    _install_akshare(monkeypatch, stock_zh_a_spot_em=stock_zh_a_spot_em)
    cache_dir = tmp_path / 'cache'
    out = fetch_market_snapshot(cache_dir=cache_dir)
    # 000001 最新价为 0、999999 缺总市值、600002 总市值为负 → 均跳过；
    # 600001 流通市值缺失 → 保留总股本、流通股本为 None
    assert set(out) == {'600000.SH', '600001.SH'}
    assert out['600000.SH'] == (1_000_000_000, 800_000_000)
    assert out['600001.SH'] == (1_000_000_000, None)
    assert (cache_dir / f'spot_{date.today():%Y%m%d}.parquet').exists()
    assert calls['n'] == 1

    # 命中缓存不再请求
    _install_akshare(
        monkeypatch,
        stock_zh_a_spot_em=lambda: (_ for _ in ()).throw(AssertionError('no')))
    assert fetch_market_snapshot(cache_dir=cache_dir) == out
    # refresh 强制重抓
    _install_akshare(monkeypatch, stock_zh_a_spot_em=stock_zh_a_spot_em)
    fetch_market_snapshot(cache_dir=cache_dir, refresh=True)
    assert calls['n'] == 2

    # 源端返回空 → {}
    _install_akshare(monkeypatch, stock_zh_a_spot_em=lambda: None)
    assert fetch_market_snapshot(cache_dir=tmp_path / 'empty',
                                 refresh=True) == {}


# ----------------------------------------------------------------------
# 单票采集
# ----------------------------------------------------------------------
def test_collect_one_em_ok(monkeypatch):
    em = _frame([_row('A', date(2020, 1, 1), 100, 80)])
    monkeypatch.setattr(share_module, 'fetch_em_history', lambda *_a, **_k: em)
    series, status_name = collect_one(
        'A', fetched_at=_FETCHED, today=_TODAY, throttle=0.0)
    assert status_name == 'ok'
    assert series.equals(em)


def test_collect_one_switches_to_cninfo(monkeypatch):
    em = _frame([_row('A', date(2020, 1, 1), 100, 80)])
    cn = _frame([_row('A', date(2020, 1, 1), 200, 160)])
    monkeypatch.setattr(share_module, 'fetch_em_history', lambda *_a, **_k: em)
    monkeypatch.setattr(share_module, 'fetch_cninfo_history', lambda *_a, **_k: cn)
    series, status_name = collect_one(
        'A', fetched_at=_FETCHED, today=_TODAY, snapshot=(200, 160),
        throttle=0.0)
    assert status_name == 'ok'
    assert series['total_share'].to_list() == [200]


def test_collect_one_stitches_legacy(monkeypatch):
    em = _frame([
        _row('A', date(1994, 1, 1), 100, 80),
        _row('A', date(1996, 1, 1), 200, 160),
    ])
    cn = _frame([
        _row('A', date(1993, 1, 1), 50, 40),
        _row('A', date(1996, 1, 1), 200, 160),
    ])
    monkeypatch.setattr(share_module, 'fetch_em_history', lambda *_a, **_k: em)
    monkeypatch.setattr(share_module, 'fetch_cninfo_history', lambda *_a, **_k: cn)
    series, status_name = collect_one(
        'A', fetched_at=_FETCHED, today=_TODAY,
        list_date=date(1993, 1, 1), throttle=0.0)
    assert status_name == 'ok'
    assert series['total_share'].to_list() == [50, 200]


def test_collect_one_cninfo_error_falls_back(monkeypatch):
    em = _frame([_row('A', date(1994, 1, 1), 100, 80)])

    def boom(*_a, **_k):
        raise DataSourceConnectionError(source='cninfo', message='down')

    monkeypatch.setattr(share_module, 'fetch_em_history', lambda *_a, **_k: em)
    monkeypatch.setattr(share_module, 'fetch_cninfo_history', boom)
    series, status_name = collect_one(
        'A', fetched_at=_FETCHED, today=_TODAY,
        list_date=date(1993, 1, 1), throttle=0.0)
    assert status_name == 'ok'
    assert series.equals(em)


def test_collect_one_derive_and_empty(monkeypatch):
    empty = _frame([])
    monkeypatch.setattr(share_module, 'fetch_em_history', lambda *_a, **_k: empty)
    monkeypatch.setattr(share_module, 'fetch_cninfo_history', lambda *_a, **_k: empty)

    series, status_name = collect_one(
        'A', fetched_at=_FETCHED, today=_TODAY,
        list_date=date(2020, 1, 1), allow_derive=False, throttle=0.0)
    assert status_name == 'empty' and series.is_empty()

    monkeypatch.setattr(share_module, 'fetch_current_info', lambda *_a, **_k: None)
    series, status_name = collect_one(
        'A', fetched_at=_FETCHED, today=_TODAY, throttle=0.0)
    assert status_name == 'empty'

    monkeypatch.setattr(
        share_module, 'fetch_current_info', lambda *_a, **_k: (100, 80))
    series, status_name = collect_one(
        'A', fetched_at=_FETCHED, today=_TODAY,
        list_date=date(2020, 1, 1), dividend_events=[(date(2021, 1, 1), 0.5)],
        throttle=0.0)
    assert status_name == 'derived'
    # 倒推：100 / 1.5 = 66.7 → 67 股
    assert series['total_share'].to_list() == [67, 100]


# ----------------------------------------------------------------------
# 批量与合并
# ----------------------------------------------------------------------
def test_load_dividend_events(tmp_path):
    assert load_dividend_events(tmp_path) == {}
    path = tmp_path / 'factors' / 'stock_dividend.parquet'
    path.parent.mkdir(parents=True)
    pl.DataFrame({
        'ts_code': ['A', 'A', 'A', 'B'],
        'ex_date': [date(2021, 1, 1), date(2020, 1, 1), date(2022, 1, 1),
                    date(2021, 5, 1)],
        'share_div': [0.3, 0.2, None, 0.0],
    }).write_parquet(path)
    # date 列是 Date 而非字符串，_as_date 也能处理
    events = load_dividend_events(tmp_path)
    assert events['A'] == [(date(2020, 1, 1), 0.2), (date(2021, 1, 1), 0.3)]
    assert 'B' not in events


def test_write_helpers(tmp_path):
    part = tmp_path / 'parts' / 'part_00000.parquet'
    part.parent.mkdir()
    _write_part(part, _frame([_row('A', date(2020, 1, 1), 100)]))
    assert pl.read_parquet(part).columns == list(SHARE_COLUMNS)

    assert _load_json(tmp_path / 'none.json') == {}
    _write_json(tmp_path / 'x.json', {'a': 1})
    assert _load_json(tmp_path / 'x.json') == {'a': 1}
    (tmp_path / 'bad.json').write_text('{bad', encoding='utf-8')
    assert _load_json(tmp_path / 'bad.json') == {}

    _write_failed_csv(tmp_path / 'failed.csv', [
        {'ts_code': 'A', 'error': 'boom', 'stage': 'fetch'}])
    lines = (tmp_path / 'failed.csv').read_text(encoding='utf-8').splitlines()
    assert lines == ['ts_code,error,stage', 'A,boom,fetch']
    _write_failed_csv(tmp_path / 'empty.csv', [])
    assert (tmp_path / 'empty.csv').read_text(
        encoding='utf-8').splitlines() == ['ts_code,error,stage']


def test_collect_shares_chunks_resume_and_failures(tmp_path, monkeypatch):
    work = tmp_path / 'work'

    def fake_task(ts_code, *_a, **_k):
        if ts_code == 'B':
            raise RuntimeError('down')
        return _frame([_row(ts_code, date(2020, 1, 1), 100, 80)]), 'ok'

    monkeypatch.setattr(share_module, '_collect_task', fake_task)
    out = collect_shares(
        ['A', 'B', 'C'], work_dir=work, snapshot={}, dividend_events={},
        workers=2, chunk_size=2, throttle=0.0)
    assert out['target'] == 3
    assert out['ok'] == 2
    assert out['failed'] == 1
    assert out['rows'] == 2
    assert out['failed_list'][0]['ts_code'] == 'B'
    progress = json.loads(
        (work / 'progress.json').read_text(encoding='utf-8'))
    assert progress['chunk_status']['00000'] == 'partial'
    assert progress['chunk_status']['00001'] == 'ok'

    # 第二块完成且 part 存在 → 续跑只重试 partial 块
    calls: list[str] = []

    def ok_task(ts_code, *_a, **_k):
        calls.append(ts_code)
        return _frame([_row(ts_code, date(2020, 1, 1), 100, 80)]), 'ok'

    monkeypatch.setattr(share_module, '_collect_task', ok_task)
    second = collect_shares(
        ['A', 'B', 'C'], work_dir=work, snapshot={}, dividend_events={},
        workers=1, chunk_size=2, throttle=0.0)
    assert calls == ['A', 'B']
    assert second['ok'] == 2
    assert second['failed'] == 0

    # refresh 清空进度后全量重跑
    calls.clear()
    collect_shares(
        ['A', 'B', 'C'], work_dir=work, snapshot={}, dividend_events={},
        workers=1, chunk_size=2, throttle=0.0, refresh=True)
    assert calls == ['A', 'B', 'C']

    # 参数签名不一致 → 忽略旧进度
    calls.clear()
    collect_shares(
        ['A', 'B', 'C'], work_dir=work, snapshot={}, dividend_events={},
        workers=1, chunk_size=3, throttle=0.0)
    assert calls == ['A', 'B', 'C']

    # limit 截断
    calls.clear()
    collect_shares(
        ['A', 'B', 'C'], work_dir=work, snapshot={}, dividend_events={},
        workers=1, chunk_size=2, throttle=0.0, limit=1, refresh=True)
    assert calls == ['A']


def test_collect_task_wrapper(monkeypatch):
    captured: dict = {}

    def fake_collect(code, **kwargs):
        captured.update(code=code, **kwargs)
        return _frame([]), 'empty'

    monkeypatch.setattr(share_module, 'collect_one', fake_collect)
    frame, status_name = share_module._collect_task(
        'A', _TODAY, _FETCHED, {'A': (100, 80)}, date(2020, 1, 1),
        0.0, True, {'A': [(date(2021, 1, 1), 0.5)]})
    assert status_name == 'empty'
    assert captured['snapshot'] == (100, 80)
    assert captured['dividend_events'] == [(date(2021, 1, 1), 0.5)]


def test_assemble_shares(tmp_path):
    work = tmp_path / 'work'
    with pytest.raises(FileNotFoundError, match='没有可合并的分块'):
        assemble_shares(tmp_path / 'root', work)

    parts = work / 'parts'
    parts.mkdir(parents=True)
    _write_part(parts / 'part_00000.parquet', _frame([
        _row('A', date(2020, 1, 1), 100),
        _row('A', date(2021, 1, 1), 110),
    ]))
    _write_part(parts / 'part_00001.parquet', _frame([
        _row('A', date(2020, 1, 1), 999,
             ),
    ]))
    out = assemble_shares(tmp_path / 'root', work)
    assert out['parts'] == 2
    assert out['rows'] == 2
    assert out['codes'] == 1
    frame = pl.read_parquet(out['path'])
    # 同键 keep='last'（part_00001 覆盖 part_00000）
    assert frame.filter(pl.col('effective_date') == date(2020, 1, 1))[
        'total_share'][0] == 999


def test_coverage_report(tmp_path):
    root = tmp_path / 'root'
    empty = coverage_report(['A', 'B'], root)
    assert empty == {'universe': 2, 'covered': 0, 'missing': ['A', 'B'],
                     'rows': 0}
    write_shares(_frame([
        _row('600000.SH', date(2020, 1, 1), 100),
        _row('000001.SZ', date(2020, 1, 1), 200),
    ]), root)
    out = coverage_report(['600000.SH', '000001.SZ', '999999.SH'], root)
    assert out == {'universe': 3, 'covered': 2, 'missing': ['999999.SH'],
                   'rows': 2}
