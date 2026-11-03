"""腾讯客户端与验收工具的单测（全部离线：网络与 TDX 读取均打桩）。"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from argparse import ArgumentTypeError
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

import src.data.tencent.__main__ as cli
import src.data.tencent.client as client_module
import src.data.tencent.daily_fill as fill_module
from src.data.tencent.client import (
    MAX_MINUTE_BARS,
    RateLimiter,
    TencentClient,
    TencentError,
    normalize_minute_volume,
    parse_daily_rows,
    parse_minute_rows,
    to_tencent_symbol,
)
from src.data.tencent.daily_fill import (
    QuoteCoverage,
    _estimated_count,
    _sanitize,
    resolve_latest_trade_date,
    sample_stock_codes,
    verify_daily,
)


# ----------------------------------------------------------------------
# 基础工具
# ----------------------------------------------------------------------
def test_to_tencent_symbol_maps_markets() -> None:
    assert to_tencent_symbol('600000.SH') == 'sh600000'
    assert to_tencent_symbol('000001.SZ') == 'sz000001'
    assert to_tencent_symbol('430047.BJ') == 'bj430047'


def test_rate_limiter_rejects_negative_interval() -> None:
    with pytest.raises(ValueError):
        RateLimiter(min_interval=-1)


def test_rate_limiter_retries_then_succeeds(monkeypatch) -> None:
    monkeypatch.setattr(client_module.time, 'sleep', lambda _seconds: None)
    calls: list[int] = []

    def flaky() -> str:
        calls.append(1)
        if len(calls) == 1:
            raise TencentError('第一次失败')
        return 'ok'

    limiter = RateLimiter(min_interval=0.0, max_retries=2, backoff_base=0)
    assert limiter.call(flaky) == 'ok'
    assert limiter.retries == 1


def test_rate_limiter_raises_last_error(monkeypatch) -> None:
    monkeypatch.setattr(client_module.time, 'sleep', lambda _seconds: None)

    def always_fail() -> str:
        raise TencentError('持续失败')

    limiter = RateLimiter(min_interval=0.0, max_retries=1, backoff_base=0)
    with pytest.raises(TencentError, match='持续失败'):
        limiter.call(always_fail)
    assert limiter.retries == 1


class _Response:
    def __init__(self, body: str) -> None:
        self.body = body

    def read(self) -> bytes:
        return self.body.encode('utf-8')

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *args: object) -> bool:
        return False


def _patch_urlopen(monkeypatch, body: str) -> None:
    monkeypatch.setattr(
        urllib.request, 'urlopen',
        lambda request, timeout=0: _Response(body))


def test_raw_get_rejects_html(monkeypatch) -> None:
    _patch_urlopen(monkeypatch, '  <html>risk control</html>')
    with pytest.raises(TencentError, match='HTML'):
        TencentClient(min_interval=0)._raw_get('https://example.com')


def test_raw_get_rejects_non_json(monkeypatch) -> None:
    _patch_urlopen(monkeypatch, 'not-json')
    with pytest.raises(TencentError, match='不是 JSON'):
        TencentClient(min_interval=0)._raw_get('https://example.com')


def test_raw_get_rejects_non_dict(monkeypatch) -> None:
    _patch_urlopen(monkeypatch, '[1, 2]')
    with pytest.raises(TencentError, match='结构异常'):
        TencentClient(min_interval=0)._raw_get('https://example.com')


def test_raw_get_returns_payload(monkeypatch) -> None:
    _patch_urlopen(monkeypatch, json.dumps({'code': 0}))
    assert TencentClient(min_interval=0)._raw_get('https://example.com') == {'code': 0}


def test_consecutive_failures_counted_and_reset(monkeypatch) -> None:
    client = TencentClient(min_interval=0, max_retries=0)

    def boom(_url: str) -> dict:
        raise TencentError('boom')

    monkeypatch.setattr(client, '_raw_get', boom)
    for _ in range(2):
        with pytest.raises(TencentError):
            client._get('https://example.com')
    assert client.consecutive_failures() == 2
    monkeypatch.setattr(client, '_raw_get', lambda _url: {'code': 0})
    client._get('https://example.com')
    assert client.consecutive_failures() == 0


# ----------------------------------------------------------------------
# 日线 / 分钟取数（打桩 _get）
# ----------------------------------------------------------------------
def test_fetch_daily_business_error() -> None:
    client = TencentClient(min_interval=0)
    client._get = lambda url: {'code': 1001, 'msg': 'symbol not found'}  # type: ignore[method-assign]
    with pytest.raises(TencentError, match='业务错误'):
        client.fetch_daily('600000.SH')


@pytest.mark.parametrize('payload', [
    {'code': 0, 'data': None},
    {'code': 0, 'data': {}},
    {'code': 0, 'data': {'sh600000': {}}},
    {'code': 0, 'data': {'sh600000': {'day': []}}},
])
def test_fetch_daily_empty_shapes(payload) -> None:
    client = TencentClient(min_interval=0)
    client._get = lambda url: payload  # type: ignore[method-assign]
    frame = client.fetch_daily('600000.SH')
    assert frame.is_empty()
    assert frame.columns == ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']


def test_fetch_minute_rejects_unknown_freq() -> None:
    with pytest.raises(ValueError, match='不支持的分钟频率'):
        TencentClient(min_interval=0).fetch_minute('600000.SH', freq='m3')


def test_fetch_minute_business_error() -> None:
    client = TencentClient(min_interval=0)
    client._get = lambda url: {'code': 1, 'msg': 'bad'}  # type: ignore[method-assign]
    with pytest.raises(TencentError, match='业务错误'):
        client.fetch_minute('600000.SH', 'm1')


@pytest.mark.parametrize('payload', [
    {'code': 0, 'data': None},
    {'code': 0, 'data': {}},
    {'code': 0, 'data': {'sh600000': {}}},
    {'code': 0, 'data': {'sh600000': {'m1': []}}},
])
def test_fetch_minute_empty_shapes(payload) -> None:
    client = TencentClient(min_interval=0)
    client._get = lambda url: payload  # type: ignore[method-assign]
    frame = client.fetch_minute('600000.SH', 'm1')
    assert frame.is_empty()
    assert frame.columns == [
        'datetime', 'open', 'high', 'low', 'close', 'volume', 'amount']


def test_fetch_minute_clamps_count_to_hard_limit(monkeypatch) -> None:
    captured: list[str] = []
    client = TencentClient(min_interval=0)

    def fake_get(url: str) -> dict:
        captured.append(url)
        return {'code': 0, 'data': {'sh600000': {'m1': [
            ['202611021330', '7.0', '7.0', '7.01', '6.99', '1200', {}, '']]}}}

    monkeypatch.setattr(client, '_get', fake_get)
    frame = client.fetch_minute('600000.SH', 'm1', count=5000)
    assert frame.height == 1
    assert f',{MAX_MINUTE_BARS}' in captured[0]


def test_parse_rows_skip_invalid_shapes() -> None:
    daily = parse_daily_rows(
        [['2026-10-19'], 'bad', ['2026-10-19', '1', '2', '3', '4', '']])
    assert daily.is_empty()
    minute = parse_minute_rows([['202611021330', '1', '2', '3', '4'],
                                ['202611021330', '1', '1.0', '1.0', '1.0', '5', {}, '']])
    assert minute.height == 1


def test_parse_rows_skip_non_numeric_values_and_empty_lists() -> None:
    assert parse_daily_rows([['2026-10-19', 'abc', '1', '2', '3', '4']]).is_empty()
    assert parse_minute_rows([['202611021330', 'abc', '1', '2', '3', '4']]).is_empty()
    assert parse_minute_rows([]).is_empty()


def test_normalize_daily_volume_edges() -> None:
    empty = pl.DataFrame(schema={'date': pl.Date, 'open': pl.Float64,
                                 'high': pl.Float64, 'low': pl.Float64,
                                 'close': pl.Float64, 'volume': pl.Float64,
                                 'amount': pl.Float64})
    assert client_module.normalize_daily_volume(empty, '688190.SH').is_empty()

    zero_volume = pl.DataFrame({
        'date': [date(2026, 11, 2)], 'open': [10.0], 'high': [10.0],
        'low': [10.0], 'close': [10.0], 'volume': [0.0], 'amount': [100.0],
    }, schema_overrides={'date': pl.Date})
    assert client_module.normalize_daily_volume(
        zero_volume, '688190.SH')['volume'][0] == 0.0

    ambiguous = zero_volume.with_columns(
        pl.lit(1.0).alias('volume'), pl.lit(100.0).alias('amount'))
    assert client_module.normalize_daily_volume(
        ambiguous, '688190.SH')['volume'][0] == 1.0


def test_normalize_minute_volume_converts_star_to_hands() -> None:
    """科创板分钟成交量以「股」返回时换算为手（600000 等保持原值）。"""
    frame = pl.DataFrame({
        'datetime': [datetime(2026, 11, 2, 9, 35)],
        'open': [60.0], 'high': [60.1], 'low': [59.9], 'close': [60.0],
        'volume': [1200.0], 'amount': [None],
    }, schema_overrides={'amount': pl.Float64})
    assert normalize_minute_volume(frame, '688190.SH')['volume'][0] == 12.0
    assert normalize_minute_volume(frame, '600000.SH')['volume'][0] == 1200.0
    assert normalize_minute_volume(pl.DataFrame(), '688190.SH').is_empty()


def test_fetch_daily_parses_valid_payload() -> None:
    client = TencentClient(min_interval=0)
    client._get = lambda url: {'code': 0, 'data': {'sh600000': {'day': [  # type: ignore[method-assign]
        ['2026-11-02', '10.0', '10.1', '10.2', '9.9', '1234', {}, '1.2', '100.5', '0', '0']]}}}
    frame = client.fetch_daily('600000.SH')
    assert frame.height == 1
    assert frame['amount'][0] == pytest.approx(1_005_000.0)


# ----------------------------------------------------------------------
# 补数辅助函数
# ----------------------------------------------------------------------
def _daily_frame(dates: list[date], closes: list[float]) -> pl.DataFrame:
    return pl.DataFrame({
        'date': dates, 'open': closes, 'high': closes, 'low': closes,
        'close': closes, 'volume': [100.0] * len(dates),
        'amount': [1000.0] * len(dates),
    }, schema_overrides={'date': pl.Date})


def test_resolve_latest_trade_date_drops_intraday_bar() -> None:
    class Client:
        def fetch_daily(self, code: str, count: int = 10) -> pl.DataFrame:
            return _daily_frame([date(2026, 10, 30), date(2026, 11, 2), date(2026, 11, 3)],
                                [10.0, 10.1, 10.2])

    resolved = resolve_latest_trade_date(
        Client(), now=datetime(2026, 11, 3, 10, 0))
    assert resolved == date(2026, 11, 2)


def test_resolve_latest_trade_date_after_close_keeps_today() -> None:
    class Client:
        def fetch_daily(self, code: str, count: int = 10) -> pl.DataFrame:
            return _daily_frame([date(2026, 10, 30), date(2026, 11, 2), date(2026, 11, 3)],
                                [10.0, 10.1, 10.2])

    assert resolve_latest_trade_date(
        Client(), now=datetime(2026, 11, 3, 15, 30)) == date(2026, 11, 3)


def test_resolve_latest_trade_date_raises_without_data() -> None:
    class Client:
        def fetch_daily(self, code: str, count: int = 10) -> pl.DataFrame:
            raise TencentError('没数据')

    with pytest.raises(TencentError, match='无法从腾讯日线推断最新交易日'):
        resolve_latest_trade_date(Client())


def test_estimated_count_clamped() -> None:
    assert _estimated_count(date(2026, 11, 2), date(2026, 11, 2)) == 12
    assert _estimated_count(date(2020, 1, 1), date(2026, 11, 2)) == 800


def test_sanitize_drops_invalid_rows() -> None:
    frame = pl.DataFrame({
        'open': [1.0, -1.0, 1.0], 'high': [1.0, 1.0, 1.0],
        'low': [1.0, 1.0, 1.0], 'close': [1.0, 1.0, 1.0],
        'volume': [10.0, 10.0, -5.0],
    })
    assert _sanitize(frame).height == 1


def test_sample_stock_codes_is_deterministic_and_stock_only() -> None:
    coverages = {}
    for code in ('600000.SH', '600519.SH', '000001.SZ', '300750.SZ', '600036.SH'):
        coverages[('stock', code)] = QuoteCoverage(
            ts_code=code, data_type='stock', rows=100,
            start=date(2024, 1, 1), end=date(2026, 10, 31), file_size=1)
    coverages[('fund', '510050.SH')] = QuoteCoverage(
        ts_code='510050.SH', data_type='fund', rows=100,
        start=date(2024, 1, 1), end=date(2026, 10, 31), file_size=1)
    coverages[('index', '000300.SH')] = QuoteCoverage(
        ts_code='000300.SH', data_type='index', rows=100,
        start=date(2024, 1, 1), end=date(2026, 10, 31), file_size=1)
    first = sample_stock_codes(coverages, count=5)
    second = sample_stock_codes(coverages, count=5)
    assert first == second
    assert all(code.endswith(('.SH', '.SZ')) for code in first)
    assert '000300.SH' not in first
    assert '510050.SH' not in first
    assert len(first) == 5  # 4 只核心 + 1 只固定种子随机补充


def test_resolve_latest_trade_date_skips_empty_frames() -> None:
    class Client:
        def fetch_daily(self, code: str, count: int = 10) -> pl.DataFrame:
            return pl.DataFrame()

    with pytest.raises(TencentError):
        resolve_latest_trade_date(Client())


# ----------------------------------------------------------------------
# 验收报告
# ----------------------------------------------------------------------
def test_verify_daily_statuses(tmp_path, monkeypatch) -> None:
    good = _daily_frame([date(2026, 10, 30), date(2026, 11, 2)], [10.0, 10.1])

    class Client:
        def fetch_daily(self, code: str, count: int = 800) -> pl.DataFrame:
            if code == '000002.SZ':
                raise TencentError('源端失败')
            return good

    def fake_tdx(tdx_path: Path | None, ts_code: str) -> pl.DataFrame:
        if ts_code == '000003.SZ':
            return pl.DataFrame()
        return good

    monkeypatch.setattr(fill_module, '_tdx_frame', fake_tdx)
    store = SimpleNamespace(root=tmp_path)
    report = verify_daily(
        SimpleNamespace(data=SimpleNamespace(tdx_data_path='')),
        store, Client(),
        codes=['600000.SH', '000002.SZ', '000003.SZ'])
    statuses = {item['ts_code']: item['status'] for item in report['results']}
    assert statuses['600000.SH'] == 'ok'
    assert statuses['000002.SZ'] == 'source_error'
    assert statuses['000003.SZ'] == 'tdx_missing'
    assert report['fields']['close']['max_abs'] == pytest.approx(0.0)


def test_verify_daily_date_filters_and_no_overlap(tmp_path, monkeypatch) -> None:
    good = _daily_frame([date(2026, 10, 30), date(2026, 11, 2)], [10.0, 10.1])
    monkeypatch.setattr(fill_module, '_tdx_frame', lambda _path, _code: good)

    class Client:
        def fetch_daily(self, code: str, count: int = 800) -> pl.DataFrame:
            return good

    store = SimpleNamespace(root=tmp_path)
    report = verify_daily(
        SimpleNamespace(data=SimpleNamespace(tdx_data_path='')), store, Client(),
        codes=['600000.SH'], start=date(2026, 11, 1), end=date(2026, 11, 2))
    assert report['overlap_rows'] == 1
    assert report['results'][0]['status'] == 'ok'

    no_overlap = verify_daily(
        SimpleNamespace(data=SimpleNamespace(tdx_data_path='')), store, Client(),
        codes=['600000.SH'], end=date(2026, 10, 20))
    assert no_overlap['results'][0]['status'] == 'no_overlap'


def test_verify_daily_falls_back_to_raw_code(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(fill_module, '_tdx_frame', lambda _path, _code: pl.DataFrame())
    report = verify_daily(
        SimpleNamespace(data=SimpleNamespace(tdx_data_path='')),
        SimpleNamespace(root=tmp_path), SimpleNamespace(),
        codes=['not-a-code'])
    assert report['results'][0]['status'] == 'tdx_missing'
    assert report['results'][0]['ts_code'] == 'NOT-A-CODE'


def test_compare_with_tdx_empty_and_missing_columns() -> None:
    empty = pl.DataFrame()
    assert fill_module.compare_with_tdx(empty, empty) == {'overlap_rows': 0, 'fields': {}}
    left = _daily_frame([date(2026, 11, 2)], [10.0])
    right = _daily_frame([date(2026, 10, 30)], [10.0])
    assert fill_module.compare_with_tdx(left, right) == {'overlap_rows': 0, 'fields': {}}
    partial = right.select(['date', 'open'])
    comparison = fill_module.compare_with_tdx(left, partial)
    assert comparison['overlap_rows'] == 0  # 日期不重叠
    comparison = fill_module.compare_with_tdx(left, left.select(['date', 'open']))
    assert set(comparison['fields']) == {'open'}


def test_read_meta_rows_missing_and_corrupt(tmp_path) -> None:
    from src.data.local_store.store import LocalQuoteStore

    store = LocalQuoteStore(tmp_path, enabled=True)
    assert fill_module.read_meta_rows(store) == {}
    store.meta_path.parent.mkdir(parents=True, exist_ok=True)
    store.meta_path.write_bytes(b'not-a-parquet')
    assert fill_module.read_meta_rows(store) == {}


def test_tdx_frame_none_and_missing(tmp_path) -> None:
    assert fill_module._tdx_frame(None, '600000.SH').is_empty()
    assert fill_module._tdx_frame(tmp_path, '600000.SH').is_empty()
    assert fill_module._tdx_mtime(None, '600000.SH') is None
    assert fill_module._tdx_mtime(tmp_path, '600000.SH') is None


def test_refresh_meta_with_empty_coverages(tmp_path) -> None:
    from src.data.local_store.store import LocalQuoteStore

    store = LocalQuoteStore(tmp_path, enabled=True)
    assert fill_module.refresh_meta(store, {}, {}) == 0


def _local_kline() -> pl.DataFrame:
    return pl.DataFrame({
        'date': [date(2026, 10, 16), date(2026, 10, 19)],
        'ts_code': ['600000.SH', '600000.SH'],
        'open': [9.8, 9.95], 'high': [9.95, 10.05], 'low': [9.75, 9.9],
        'close': [9.9, 10.0], 'pre_close': [9.8, 9.9],
        'volume': [600000.0, 550000.0], 'amount': [588000000.0, 550000000.0],
        'pct_chg': [1.02, 1.01], 'change': [0.1, 0.1],
        'qfq_factor': [1.0, 1.0], 'hfq_factor': [12.77, 12.77],
    }, schema_overrides={'date': pl.Date}).select(
        list(fill_module._EVIDENCE_KLINE_COLUMNS))


def _remote_kline() -> pl.DataFrame:
    return pl.DataFrame({
        'date': [date(2026, 11, 2)],
        'open': [10.0], 'high': [10.1], 'low': [9.9], 'close': [10.05],
        'volume': [500000.0], 'amount': [500000000.0],
    }, schema_overrides={'date': pl.Date})


class _FillClient:
    def __init__(self, *, fail_codes: tuple[str, ...] = (),
                 failures: int = 0) -> None:
        self.fail_codes = fail_codes
        self.failures = failures
        self.calls: list[str] = []

    def fetch_daily(self, ts_code: str, count: int = 800) -> pl.DataFrame:
        self.calls.append(ts_code)
        if ts_code in self.fail_codes:
            raise TencentError('boom')
        return _remote_kline()

    def consecutive_failures(self) -> int:
        return self.failures


def _fill_config() -> SimpleNamespace:
    return SimpleNamespace(data=SimpleNamespace(tdx_data_path=''))


def test_fill_daily_gap_records_failure(tmp_path) -> None:
    from src.data.local_store.store import LocalQuoteStore

    store = LocalQuoteStore(tmp_path, enabled=True)
    store.write_daily('stock', '600000.SH', _local_kline(), full=True)
    stats = fill_module.fill_daily_gap(
        _fill_config(), store, _FillClient(fail_codes=('600000.SH',)),
        latest_trade_date=date(2026, 11, 2), guard_sample_size=0)
    assert stats['failed_symbols'] == 1
    assert stats['rows_added'] == 0


def test_fill_daily_gap_aborts_when_repeated_failures(tmp_path) -> None:
    from src.data.local_store.store import LocalQuoteStore

    store = LocalQuoteStore(tmp_path, enabled=True)
    for code in ('600000.SH', '600001.SH'):
        store.write_daily('stock', code, _local_kline(), full=True)
    stats = fill_module.fill_daily_gap(
        _fill_config(), store, _FillClient(failures=fill_module._MAX_CONSECUTIVE_FAILURES),
        latest_trade_date=date(2026, 11, 2), guard_sample_size=0)
    assert stats['failed_symbols'] == 0


def test_make_store_uses_resolved_root(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(cli, 'resolve_store_root', lambda config: tmp_path)
    store = cli._make_store(SimpleNamespace())
    assert store.root == tmp_path
    assert store.enabled is True


# ----------------------------------------------------------------------
# CLI（联网闸门 / 参数解析 / 报告落盘）
# ----------------------------------------------------------------------
def test_parse_date_accepts_two_formats_and_rejects_garbage() -> None:
    assert cli._parse_date('2026-11-02') == date(2026, 11, 2)
    assert cli._parse_date('20261102') == date(2026, 11, 2)
    with pytest.raises(ArgumentTypeError):
        cli._parse_date('11/02/2026')


def test_parse_types_expands_all_and_rejects_unknown() -> None:
    assert cli._parse_types('all') == ('stock', 'index', 'fund')
    assert cli._parse_types('') == ('stock', 'index')
    assert cli._parse_types('stock,index') == ('stock', 'index')
    with pytest.raises(ArgumentTypeError):
        cli._parse_types('stock,unknown')


def test_split_codes_and_require_remote() -> None:
    assert cli._split_codes(None) is None
    assert cli._split_codes('600000.SH, 000001.SZ') == ['600000.SH', '000001.SZ']
    assert cli._require_remote(True) is True
    assert cli._require_remote(False) is False


def test_write_report_creates_parent_dir(tmp_path) -> None:
    target = tmp_path / 'nested' / 'report.json'
    cli._write_report(str(target), {'rows': 1})
    assert json.loads(target.read_text(encoding='utf-8')) == {'rows': 1}
    cli._write_report(None, {'rows': 1})  # 无路径时静默


def test_main_refuses_network_without_flag() -> None:
    assert cli.main(['fill-daily']) == 2
    assert cli.main(['verify-daily']) == 2
    assert cli.main(['minute', '--codes', '600000.SH']) == 2


def test_cmd_fill_daily_returns_zero_and_writes_report(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(cli, '_make_store', lambda config: SimpleNamespace())
    monkeypatch.setattr(
        cli, 'fill_daily_gap',
        lambda *args, **kwargs: {'filled_symbols': 1, 'rows_added': 10})
    report = tmp_path / 'fill.json'
    code = cli.main(['fill-daily', '--allow-remote', '--dry-run',
                     '--latest-trade-date', '2026-11-02', '--report', str(report)])
    assert code == 0
    assert json.loads(report.read_text(encoding='utf-8'))['rows_added'] == 10


def test_cmd_fill_daily_returns_three_on_guard_failure(monkeypatch) -> None:
    monkeypatch.setattr(cli, '_make_store', lambda config: SimpleNamespace())
    monkeypatch.setattr(
        cli, 'fill_daily_gap',
        lambda *args, **kwargs: {'aborted': 'guard_failed'})
    assert cli.main(['fill-daily', '--allow-remote', '--latest-trade-date',
                     '2026-11-02']) == 3


def test_cmd_verify_daily_reports_and_returns_one_on_mismatch(monkeypatch) -> None:
    monkeypatch.setattr(cli, '_make_store', lambda config: SimpleNamespace())
    result = {
        'codes': 1, 'overlap_rows': 1,
        'fields': {'close': {'max_abs': 0.5, 'max_rel': 1.0, 'diff_rows': 1}},
        'results': [{'ts_code': '600000.SH', 'status': 'price_mismatch'}],
        'elapsed_s': 0.1,
    }
    monkeypatch.setattr(cli, 'verify_daily', lambda *args, **kwargs: result)
    assert cli.main(['verify-daily', '--allow-remote',
                     '--codes', '600000.SH']) == 1


def test_cmd_minute_writes_summary(monkeypatch, tmp_path) -> None:
    summary = {'total_rows': 320, 'items': [], 'failures': [],
               'amount_available': False, 'can_ingest_stock_minute': False}
    monkeypatch.setattr(cli, 'fetch_minute_batch', lambda *args, **kwargs: summary)
    monkeypatch.setattr(cli, 'format_minute_brief', lambda payload: 'm1 320 根')
    monkeypatch.setattr(
        cli, 'write_summary_csv', lambda path, payload: Path(path))
    report = tmp_path / 'minute.json'
    summary_csv = tmp_path / 'summary.csv'
    assert cli.main(['minute', '--allow-remote', '--codes', '600000.SH',
                     '--freq', 'm1', '--output-dir', str(tmp_path),
                     '--report', str(report),
                     '--summary-csv', str(summary_csv)]) == 0
    assert json.loads(report.read_text(encoding='utf-8'))['total_rows'] == 320
    assert summary_csv.exists() is False  # write_summary_csv 被打桩
