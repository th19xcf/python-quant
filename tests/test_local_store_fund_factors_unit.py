"""fund_factors 模块单测：事件标准化、抓取编排、因子重建与三项校验。

全部用临时目录 + 合成 parquet + 注入 fake ``akshare``，不访问网络、不读
``data/`` 正式表。重点覆盖除权日对齐、份额折算、断点参数与校验器可失败性。
"""

from __future__ import annotations

import json
import sys
import types
from datetime import date, datetime, timedelta

import pandas as pd
import polars as pl
import pytest

import src.data.local_store.fund_factors as fund_module
from src.data.local_store.fund_factors import (
    FUND_DIVIDEND_FILENAME,
    FUND_FACTOR_FILENAME,
    FUND_META_FILENAME,
    FUND_SHARE_CHANGE_FILENAME,
    _backup_to_csv,
    _call_with_retry,
    _compute_code_factors,
    _fetch_one_share_change,
    _next_trade_date,
    _normalize_dividends,
    _parse_years,
    build_fund_store,
    fetch_dividend_events,
    fetch_share_change_events,
    main,
    prepare_events_for_code,
    rebuild_fund_factors,
    verify_coverage,
    verify_gap_digestion,
    verify_reversibility,
)
from src.data.local_store.store import LocalFactorStore, LocalQuoteStore

_DATES = [date(2024, 1, 2) + timedelta(days=i) for i in range(5)]


def _fund_frame(closes: list[float] | None = None) -> pl.DataFrame:
    closes = closes or [10.0, 10.2, 10.4, 10.6, 10.8]
    return pl.DataFrame({
        'date': _DATES,
        'ts_code': ['510880.SH'] * len(_DATES),
        'open': closes,
        'high': [c + 0.1 for c in closes],
        'low': [c - 0.1 for c in closes],
        'close': closes,
        'pre_close': [closes[0], *closes[:-1]],
        'volume': [1000.0] * len(_DATES),
        'amount': [1e4] * len(_DATES),
        'pct_chg': [0.0] * len(_DATES),
        'change': [0.0] * len(_DATES),
        'qfq_factor': [1.0] * len(_DATES),
        'hfq_factor': [1.0] * len(_DATES),
    })


def _write_quotes(root, codes: list[str], closes: list[float] | None = None) -> None:
    store = LocalQuoteStore(root, enabled=True)
    for code in codes:
        store.write_daily('fund', code, _fund_frame(closes).with_columns(
            pl.lit(code).alias('ts_code')))
    store.flush_meta()


def _dividend_frame(rows: list[tuple[str, date, float]]) -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': [r[0] for r in rows],
        'ex_date': [r[1] for r in rows],
        'record_date': [r[1] for r in rows],
        'cash_amount': [r[2] for r in rows],
        'source': ['test'] * len(rows),
        'fetched_at': [datetime(2026, 11, 4)] * len(rows),
    }, schema={
        'ts_code': pl.Utf8, 'ex_date': pl.Date, 'record_date': pl.Date,
        'cash_amount': pl.Float64, 'source': pl.Utf8, 'fetched_at': pl.Datetime,
    })


def _split_frame(rows: list[tuple[str, date, float]]) -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': [r[0] for r in rows],
        'ex_date': [r[1] for r in rows],
        'split_type': ['份额折算'] * len(rows),
        'share_ratio': [r[2] for r in rows],
        'source': ['test'] * len(rows),
        'fetched_at': [datetime(2026, 11, 4)] * len(rows),
    }, schema={
        'ts_code': pl.Utf8, 'ex_date': pl.Date, 'split_type': pl.Utf8,
        'share_ratio': pl.Float64, 'source': pl.Utf8, 'fetched_at': pl.Datetime,
    })


def _write_factor_snapshot(root, code: str, dates: list[date],
                           qfq: list[float],
                           hfq: list[float] | None = None) -> None:
    path = root / 'factors'
    path.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'ts_code': [code] * len(dates),
        'trade_date': dates,
        'qfq_factor': qfq,
        'hfq_factor': hfq if hfq is not None else [1.0] * len(dates),
    }).write_parquet(path / FUND_FACTOR_FILENAME)


def _install_akshare(monkeypatch, **attrs) -> types.SimpleNamespace:
    fake = types.SimpleNamespace(**attrs)
    monkeypatch.setitem(sys.modules, 'akshare', fake)
    return fake


# ----------------------------------------------------------------------
# 工具函数
# ----------------------------------------------------------------------
def test_next_trade_date_boundaries():
    assert _next_trade_date(_DATES, date(2024, 1, 1)) == date(2024, 1, 2)
    # 本身是交易日时取严格下一日，不是当日
    assert _next_trade_date(_DATES, date(2024, 1, 2)) == date(2024, 1, 3)
    assert _next_trade_date(_DATES, _DATES[-1]) is None
    assert _next_trade_date(_DATES, date(2030, 1, 1)) is None
    assert _next_trade_date([], date(2024, 1, 2)) is None


def test_call_with_retry_success_and_exhaustion(monkeypatch):
    monkeypatch.setattr(fund_module.time, 'sleep', lambda _s: None)
    calls = {'n': 0}

    def ok():
        calls['n'] += 1
        return 'value'

    assert _call_with_retry(ok, attempts=3) == 'value'
    assert calls['n'] == 1

    def flaky():
        calls['n'] += 1
        if calls['n'] < 3:
            raise RuntimeError('boom')
        return 'recovered'

    assert _call_with_retry(flaky, attempts=3) == 'recovered'
    assert calls['n'] == 3

    def always_fail():
        raise ValueError('always')

    assert _call_with_retry(always_fail, attempts=2, label='x') is None


def test_parse_years_variants():
    assert _parse_years(None) is None
    assert _parse_years('') is None
    assert _parse_years('2020-2022') == [2020, 2021, 2022]
    assert _parse_years('2020, 2022,') == [2020, 2022]


# ----------------------------------------------------------------------
# 事件标准化与抓取
# ----------------------------------------------------------------------
def test_normalize_dividends_dedupes_and_filters():
    raw = pl.DataFrame({
        '基金代码': ['510880', '510880', None, '159915'],
        '除息日期': ['2024-01-03', '2024-01-03', '2024-02-01', '2024-03-01'],
        '权益登记日': ['2024-01-02', '2024-01-02', '2024-01-31', '2024-02-29'],
        '分红': [0.1, 0.2, 0.3, -0.5],
    })
    out = _normalize_dividends(raw, datetime(2026, 11, 4))
    # 同日重复取最大金额；空代码与负分红被剔除
    assert out.height == 1
    assert out['ts_code'][0] == '510880.SH'
    assert out['cash_amount'][0] == pytest.approx(0.2)
    assert out.columns == list(fund_module.DIVIDEND_COLUMNS)


def test_normalize_dividends_bad_dates_dropped():
    raw = pl.DataFrame({
        '基金代码': ['161907', '161907'],
        '除息日期': ['not-a-date', '2024-01-03'],
        '权益登记日': ['2024-01-02', '2024-01-02'],
        '分红': [0.1, 0.0],
    })
    out = _normalize_dividends(raw, datetime(2026, 11, 4))
    assert out.is_empty()


def test_fetch_dividend_events_cache_and_empty(tmp_path, monkeypatch):
    calls: list[str] = []

    def fund_fh_em(year=None):
        calls.append(str(year))
        return pd.DataFrame({
            '基金代码': ['510050'],
            '除息日期': ['2024-01-03'],
            '权益登记日': ['2024-01-02'],
            '分红': [0.15],
        })

    _install_akshare(monkeypatch, fund_fh_em=fund_fh_em)
    cache = tmp_path / 'cache'
    first = fetch_dividend_events([2024], cache_dir=cache)
    assert calls == ['2024']
    assert first.height == 1
    assert (cache / 'fh_2024.parquet').exists()

    # 命中缓存不再请求远端
    second = fetch_dividend_events([2024], cache_dir=cache)
    assert calls == ['2024']
    assert second.height == 1

    # refresh=True 重新抓取
    fetch_dividend_events([2024], cache_dir=cache, refresh=True)
    assert calls == ['2024', '2024']

    # 源端返回空 → 空帧但 schema 完整
    _install_akshare(monkeypatch, fund_fh_em=lambda year=None: None)
    empty = fetch_dividend_events([2025], cache_dir=tmp_path / 'empty')
    assert empty.is_empty()
    assert empty.columns == list(fund_module.DIVIDEND_COLUMNS)


def test_fetch_share_change_events_parses_and_caches(tmp_path, monkeypatch):
    monkeypatch.setattr(fund_module.time, 'sleep', lambda _s: None)
    good = pd.DataFrame({
        '拆分折算日': ['2024-01-03', '2024-02-01', 'bad'],
        '拆分折算比例': ['1:0.5', '0.8', '1:2'],
        '拆分类型': ['份额折算', '份额分拆', 'x'],
    })

    def fund_open_fund_info_em(symbol=None, indicator=None):
        if symbol == '510880':
            return good
        raise RuntimeError('missing')

    _install_akshare(monkeypatch, fund_open_fund_info_em=fund_open_fund_info_em)
    cache_path = tmp_path / 'splits.json'
    out = fetch_share_change_events(
        ['510880.SH', '510880.SH', '159915.SZ'],
        cache_path=cache_path, max_workers=2)
    # 510880 两行有效事件；159915 抓取失败按无事件处理
    assert out.height == 2
    assert out['ts_code'].unique().to_list() == ['510880.SH']
    assert set(out['share_ratio'].to_list()) == {0.5, 0.8}
    cache = json.loads(cache_path.read_text(encoding='utf-8'))
    assert isinstance(cache['510880'], list)
    assert 'error' in cache['159915']

    # 二次调用全部命中缓存，不再触发远端
    def explode(**_kwargs):
        raise AssertionError('不应再抓取')

    _install_akshare(monkeypatch, fund_open_fund_info_em=explode)
    cached = fetch_share_change_events(
        ['510880.SH'], cache_path=cache_path)
    assert cached.height == 2


def test_fetch_share_change_events_corrupt_cache_and_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(fund_module.time, 'sleep', lambda _s: None)
    cache_path = tmp_path / 'splits.json'
    cache_path.write_text('{not-json', encoding='utf-8')
    _install_akshare(
        monkeypatch,
        fund_open_fund_info_em=lambda **_k: pd.DataFrame())
    out = fetch_share_change_events(['510880.SH'], cache_path=cache_path)
    assert out.is_empty()
    assert out.columns == list(fund_module.SHARE_CHANGE_COLUMNS)


def test_fetch_one_share_change_success_and_failure(monkeypatch):
    monkeypatch.setattr(fund_module.time, 'sleep', lambda _s: None)
    _install_akshare(
        monkeypatch,
        fund_open_fund_info_em=lambda **_k: pd.DataFrame({'a': [1]}))
    sym, rows, elapsed = _fetch_one_share_change('510880', {})
    assert sym == '510880'
    assert rows == [{'a': 1}]
    assert elapsed >= 0

    attempts = {'n': 0}

    def boom(**_k):
        attempts['n'] += 1
        raise ValueError('down')

    _install_akshare(monkeypatch, fund_open_fund_info_em=boom)
    sym, rows, _ = _fetch_one_share_change('159915', {})
    assert sym == '159915'
    assert 'ValueError: down' in rows['error']
    assert attempts['n'] == 3


# ----------------------------------------------------------------------
# 事件合成
# ----------------------------------------------------------------------
def test_prepare_events_empty_trade_dates():
    out = prepare_events_for_code('510880.SH', [], None, None)
    assert out.is_empty()
    assert out.columns == [
        'ts_code', 'ex_date', 'cash_div', 'share_div',
        'rights_issue_ratio', 'rights_issue_price']


def test_prepare_events_skips_invalid_and_out_of_range():
    dividends = _dividend_frame([
        ('510880.SH', date(2024, 1, 3), 0.0),      # 现金 <= 0
        ('510880.SH', date(2024, 2, 1), 0.1),      # 晚于最后一根 K 线
        ('510880.SH', date(2024, 1, 4), 0.05),     # 有效
    ])
    splits = _split_frame([
        ('510880.SH', date(2024, 1, 2), 0.0),      # 比例 <= 0
        ('510880.SH', date(2024, 1, 5), 0.5),      # 次日不存在 → 跳过
        ('510880.SH', date(2024, 1, 3), 2.0),      # 有效，次日 1-4
    ])
    events = prepare_events_for_code('510880.SH', _DATES, dividends, splits)
    assert events.height == 2
    by_date = {row['ex_date']: row for row in events.to_dicts()}
    assert by_date[date(2024, 1, 4)]['cash_div'] == pytest.approx(0.05)
    assert by_date[date(2024, 1, 4)]['share_div'] == pytest.approx(1.0)
    assert by_date[date(2024, 1, 4)]['rights_issue_ratio'] == 0.0


def test_prepare_events_skips_split_for_quote_adjusted_code():
    splits = _split_frame([('159930.SZ', date(2024, 1, 3), 2.0)])
    events = prepare_events_for_code('159930.SZ', _DATES, None, splits)
    assert events.is_empty()


def test_prepare_events_non_etf_dividend_maps_to_next_session():
    dividends = _dividend_frame([('161907.SZ', date(2024, 1, 3), 0.2)])
    events = prepare_events_for_code('161907.SZ', _DATES, dividends, None)
    assert events['ex_date'][0] == date(2024, 1, 4)


def test_compute_code_factors_paths(tmp_path):
    root = tmp_path / 'store'
    _write_quotes(root, ['510880.SH'])
    store = LocalQuoteStore(root, enabled=True)
    assert _compute_code_factors('999999.SH', store, None, None) is None

    plain = _compute_code_factors('510880.SH', store, None, None)
    assert plain is not None
    assert plain['qfq_factor'].to_list() == [1.0] * 5

    dividends = _dividend_frame([('510880.SH', date(2024, 1, 4), 0.5)])
    adjusted = _compute_code_factors('510880.SH', store, dividends, None)
    assert adjusted is not None
    # 除息日之前的价格需要乘上未来事件因子（<1），事件当日及之后为 1
    assert adjusted['qfq_factor'][0] < 1.0
    assert adjusted['qfq_factor'][-1] == pytest.approx(1.0)

    # 缺 close 列的行不行：直接写一个只有 date 的帧
    path = store.quote_path('fund', '510880.SH')
    pl.DataFrame({'date': _DATES}).write_parquet(path)
    LocalQuoteStore(root, enabled=True)
    assert _compute_code_factors(
        '510880.SH', LocalQuoteStore(root, enabled=True), None, None) is None


# ----------------------------------------------------------------------
# 重建与落盘
# ----------------------------------------------------------------------
def test_rebuild_fund_factors_dry_run_and_filters(tmp_path):
    root = tmp_path / 'store'
    _write_quotes(root, ['510880.SH', '159915.SZ'])
    out = rebuild_fund_factors(root, pl.DataFrame(), pl.DataFrame(), dry_run=True)
    assert out['fund_codes'] == 2
    assert out['rows'] == 10
    assert out['dry_run'] is True

    filtered = rebuild_fund_factors(
        root, pl.DataFrame(), pl.DataFrame(),
        codes=['510880.SH'], dry_run=True)
    assert filtered['fund_codes'] == 1


def test_rebuild_fund_factors_success_writes_snapshot(tmp_path, monkeypatch):
    root = tmp_path / 'store'
    _write_quotes(root, ['510880.SH'])
    monkeypatch.setattr(
        fund_module, 'refresh_quote_factor_columns',
        lambda *_a, **_k: {'mismatch_rows': 0})
    dividends = _dividend_frame([('510880.SH', date(2024, 1, 4), 0.5)])
    out = rebuild_fund_factors(root, dividends, pl.DataFrame(), dry_run=False)

    assert out['factor_codes'] == 1
    assert out['failed_codes'] == 0
    assert out['dry_run'] is False
    assert (root / 'factors' / FUND_FACTOR_FILENAME).exists()
    sidecar = (root / 'factors' / fund_module.FUND_COVERED_SIDECAR).read_text(
        encoding='utf-8')
    assert sidecar.strip() == '510880.SH'
    meta = json.loads(
        (root / 'factors' / FUND_META_FILENAME).read_text(encoding='utf-8'))
    assert meta['factor_rows'] == 5
    assert meta['quote_factor_refresh'] == {'mismatch_rows': 0}


def test_rebuild_fund_factors_reports_failures(tmp_path, monkeypatch):
    root = tmp_path / 'store'
    _write_quotes(root, ['510880.SH', '159915.SZ'])

    def boom(*_a, **_k):
        raise ValueError('bad frame')

    monkeypatch.setattr(fund_module, '_compute_code_factors', boom)
    with pytest.raises(RuntimeError, match='没有可写入的标的'):
        rebuild_fund_factors(root, pl.DataFrame(), pl.DataFrame(), dry_run=False)


def test_rebuild_fund_factors_quote_refresh_error_is_non_fatal(tmp_path, monkeypatch):
    root = tmp_path / 'store'
    _write_quotes(root, ['510880.SH'])

    def boom(*_a, **_k):
        raise RuntimeError('copy failed')

    monkeypatch.setattr(fund_module, 'refresh_quote_factor_columns', boom)
    out = rebuild_fund_factors(root, pl.DataFrame(), pl.DataFrame(), dry_run=False)
    assert out['quote_factor_refresh'] is None
    assert (root / 'factors' / FUND_FACTOR_FILENAME).exists()


def test_backup_to_csv(tmp_path):
    log_dir = tmp_path / 'logs'
    missing = tmp_path / 'nope.parquet'
    _backup_to_csv(missing, log_dir)
    assert not log_dir.exists()

    path = tmp_path / 'fund_adj_factor.parquet'
    pl.DataFrame({'a': [1, 2]}).write_parquet(path)
    _backup_to_csv(path, log_dir)
    assert (log_dir / 'fund_adj_factor.csv').exists()


def test_build_fund_store_requires_quotes(tmp_path):
    with pytest.raises(RuntimeError, match='没有基金行情'):
        build_fund_store(tmp_path / 'store', skip_fetch=True, dry_run=True)


def test_build_fund_store_rejects_unknown_codes(tmp_path):
    root = tmp_path / 'store'
    _write_quotes(root, ['510880.SH'])
    with pytest.raises(RuntimeError, match='指定的基金代码不在本地行情中'):
        build_fund_store(root, codes=['159915.SZ'], skip_fetch=True, dry_run=True)


def test_build_fund_store_dry_run_and_full(tmp_path, monkeypatch):
    root = tmp_path / 'store'
    _write_quotes(root, ['510880.SH'])
    factors = root / 'factors'
    factors.mkdir(parents=True, exist_ok=True)
    _dividend_frame([('510880.SH', date(2024, 1, 4), 0.5)]).write_parquet(
        factors / FUND_DIVIDEND_FILENAME)
    _split_frame([('510880.SH', date(2024, 1, 2), 1.0)]).write_parquet(
        factors / FUND_SHARE_CHANGE_FILENAME)
    monkeypatch.setattr(
        fund_module, 'refresh_quote_factor_columns',
        lambda *_a, **_k: {'mismatch_rows': 0})

    dry = build_fund_store(
        root, skip_fetch=True, dry_run=True, log_dir=tmp_path / 'logs')
    assert dry['dry_run'] is True
    assert dry['dividend_codes_all'] == 1
    assert dry['share_change_codes_all'] == 1

    # dry-run 不写因子快照
    assert not (factors / FUND_FACTOR_FILENAME).exists()

    # 已有旧因子快照 → 走 CSV 备份
    pl.DataFrame({
        'ts_code': ['510880.SH'], 'trade_date': [_DATES[0]],
        'qfq_factor': [1.0], 'hfq_factor': [1.0],
    }).write_parquet(factors / FUND_FACTOR_FILENAME)
    full = build_fund_store(
        root, skip_fetch=True, dry_run=False, log_dir=tmp_path / 'logs')
    assert full['factor_codes'] == 1
    assert (tmp_path / 'logs' / 'fund_adj_factor.csv').exists()
    assert (tmp_path / 'logs' / 'fund_dividend_new.csv').exists()
    meta = json.loads((factors / FUND_META_FILENAME).read_text(encoding='utf-8'))
    assert meta['dividend_rows'] == 1


# ----------------------------------------------------------------------
# 三项校验（含「校验器能失败」的回归）
# ----------------------------------------------------------------------
def test_verify_reversibility_passes_on_consistent_snapshot(tmp_path):
    root = tmp_path / 'store'
    _write_quotes(root, ['510880.SH'])
    _write_factor_snapshot(root, '510880.SH', _DATES, [1.0] * 5)
    out = verify_reversibility(root, ['510880.SH'])
    assert out['codes'] == 1
    assert out['rows'] == 5
    assert out['max_relative_error'] == 0.0
    assert out['passed'] is True


def test_verify_reversibility_detects_corrupted_factor(tmp_path):
    """复查缺陷 1：旧实现 close*qfq/qfq 恒真，篡改因子也必须判失败。"""
    root = tmp_path / 'store'
    _write_quotes(root, ['510880.SH'])
    qfq = [1.0, 1.01, 1.0, 1.0, 1.0]
    _write_factor_snapshot(root, '510880.SH', _DATES, qfq)
    out = verify_reversibility(root, ['510880.SH'])
    assert out['max_relative_error'] == pytest.approx(0.01, rel=1e-6)
    assert out['passed'] is False


def test_verify_reversibility_skips_missing_data(tmp_path):
    root = tmp_path / 'store'
    _write_quotes(root, ['510880.SH'])
    _write_factor_snapshot(root, '510880.SH', _DATES, [1.0] * 5)
    out = verify_reversibility(root, ['999999.SH', '510880.SH'])
    assert out['codes'] == 1
    assert out['rows'] == 5


def test_verify_gap_digestion_empty_schema_is_uniform(tmp_path):
    """复查缺陷 2：空事件与正常路径必须返回同一结构。"""
    dividends = _dividend_frame([('510880.SH', date(2024, 1, 4), 0.5)])
    empty = verify_gap_digestion(tmp_path / 'store', pl.DataFrame())
    assert empty == {
        'events_checked': 0, 'before_over_1pct': 0,
        'after_over_2pct': 0, 'samples': []}
    # 分红表非空但因子快照缺失 → 仍走同一结构
    no_factor = verify_gap_digestion(tmp_path / 'store', dividends)
    assert set(no_factor) == set(empty)
    assert no_factor['events_checked'] == 0


def test_verify_gap_digestion_counts_events(tmp_path):
    root = tmp_path / 'store'
    _write_quotes(root, ['510880.SH'], closes=[10.0, 10.0, 5.5, 5.5, 5.5])
    # hfq 在除息日跳到 1/0.55，复权后涨跌幅应回到 0
    _write_factor_snapshot(
        root, '510880.SH', _DATES,
        qfq=[0.55, 0.55, 1.0, 1.0, 1.0],
        hfq=[1.0, 1.0, 1 / 0.55, 1 / 0.55, 1 / 0.55])
    dividends = _dividend_frame([('510880.SH', date(2024, 1, 4), 4.5)])
    out = verify_gap_digestion(root, dividends)
    assert out['events_checked'] == 1
    assert out['before_over_1pct'] == 1
    assert out['after_over_2pct'] == 0
    assert len(out['samples']) == 1
    assert out['samples'][0]['ts_code'] == '510880.SH'


def test_verify_coverage_counts_missing(tmp_path):
    root = tmp_path / 'store'
    _write_quotes(root, ['510880.SH', '159915.SZ'])
    _write_factor_snapshot(root, '510880.SH', _DATES, [1.0] * 5)
    out = verify_coverage(root)
    assert out['quote_codes'] == 2
    assert out['factor_codes'] == 1
    assert out['missing_factor'] == ['159915.SZ']
    assert out['missing_factor_count'] == 1
    assert out['extra_factor_count'] == 0

    empty_root = tmp_path / 'empty'
    empty_root.mkdir()
    assert verify_coverage(empty_root) == {
        'quote_codes': 0, 'factor_codes': 0, 'missing_factor': [],
        'missing_factor_count': 0, 'extra_factor_count': 0}


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def test_main_dry_run_and_verify(tmp_path, monkeypatch):
    recorded: dict[str, object] = {}
    monkeypatch.setattr(
        fund_module, 'build_fund_store',
        lambda *a, **kw: recorded.update(args=a, kwargs=kw) or {'dry_run': True})
    assert main(['--store-root', str(tmp_path), '--years', '2020-2021',
                 '--codes', '510880.SH', '--dry-run']) == 0
    assert recorded['kwargs']['years'] == [2020, 2021]
    assert recorded['kwargs']['codes'] == ['510880.SH']

    calls: list[str] = []
    monkeypatch.setattr(
        fund_module, 'build_fund_store',
        lambda *a, **kw: {'dry_run': False})
    monkeypatch.setattr(
        fund_module, 'verify_reversibility',
        lambda *a, **kw: calls.append('reversibility') or {'passed': True})
    monkeypatch.setattr(
        fund_module, 'verify_gap_digestion',
        lambda *a, **kw: calls.append('gap') or {'events_checked': 0})
    monkeypatch.setattr(
        fund_module, 'verify_coverage',
        lambda *a, **kw: calls.append('coverage') or {})
    assert main(['--store-root', str(tmp_path), '--verify']) == 0
    assert calls == ['reversibility', 'gap', 'coverage']


def test_factor_store_reads_fund_snapshot(tmp_path):
    root = tmp_path / 'store'
    _write_quotes(root, ['510880.SH'])
    _write_factor_snapshot(root, '510880.SH', _DATES, [1.0] * 5)
    store = LocalFactorStore(root, enabled=True)
    frame = store.factors('510880.SH')
    assert frame is not None
    assert frame.height == 5
