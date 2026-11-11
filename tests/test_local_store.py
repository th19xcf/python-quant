"""本地列存读取路由回归测试。

覆盖：
1. 覆盖度/新鲜度判定（full 与增量片段、源文件更新后回退）
2. DataFetcher 本地列存命中且远程关闭时零公网访问
3. 数据库"非空即返回"遮蔽修正（库内最新日期落后本地时判残缺）
4. 因子/分红读取本地优先、不再访问远程
5. get_stock_batch 与逐票 get_stock_data 数值等价
6. 增量构建：只重建落后/内容变化的标的、max_date 截断、盘中守卫
7. 元数据增量写入不丢其他条目、rescan_meta 可从 parquet 重建
8. 目标日校验分类（源缺目标日不计为本地漏建）
"""

import contextlib
import json
import struct
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest
from loguru import logger

import src.data.local_store.build as build_module
from src.data.adj_factor_calculator import AdjFactorCalculator
from src.data.data_cache import global_data_cache
from src.data.data_manager import DataManager
from src.data.local_store import factor_rebuild as factor_rebuild_module
from src.data.local_store import metrics as route_metrics
from src.data.local_store import reset_local_stores
from src.data.local_store.build import (
    build,
    refresh_factors,
    validate_remote_overwrite,
    verify_incremental_target,
)
from src.data.local_store.factor_rebuild import rebuild_factors_from_local
from src.data.local_store.session_guard import filter_intraday_rows
from src.data.local_store.store import (
    LocalFactorStore,
    LocalQuoteStore,
    factor_lru_capacity,
    refresh_quote_factor_columns,
    verify_quote_factor_consistency,
)
from src.data.managers.data_fetcher import DataFetcher

_RECS = [
    ('20240102', 10.0, 10.5, 9.8, 10.0, 100, 1000),
    ('20240103', 10.0, 10.8, 9.9, 10.5, 120, 1260),
]


def _write_day_file(root, market, code, records) -> Path:
    path = Path(root) / market / 'lday' / f'{market}{code}.day'
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'wb') as f:
        for day, open_, high, low, close, volume, amount in records:
            f.write(struct.pack(
                '<IIIIIfII',
                int(day), int(open_ * 100), int(high * 100), int(low * 100),
                int(close * 100), float(amount), int(volume * 100), 0,
            ))
    return path


def _build_config(tdx_root, store_root) -> SimpleNamespace:
    return SimpleNamespace(data=SimpleNamespace(
        tdx_data_path=str(tdx_root),
        local_store_path=str(store_root),
        local_store_enabled=True,
        remote_database_fallback=False,
    ))


def make_frame(rows: int = 5, start: date = date(2024, 1, 2)) -> pl.DataFrame:
    """构造最小行情帧（本地列存/复权出口所需列）。"""
    dates = [start + timedelta(days=i) for i in range(rows)]
    closes = [10.0 + i for i in range(rows)]
    return pl.DataFrame({
        'date': dates,
        'open': closes,
        'high': [c + 0.5 for c in closes],
        'low': [c - 0.5 for c in closes],
        'close': closes,
        'volume': [1000 + i for i in range(rows)],
        'amount': [10000.0 for _ in range(rows)],
        'qfq_factor': [1.0 for _ in range(rows)],
        'hfq_factor': [1.0 for _ in range(rows)],
    })


def make_config(store_root, *, remote: bool = False, tdx: str = '') -> SimpleNamespace:
    return SimpleNamespace(data=SimpleNamespace(
        local_store_enabled=True,
        local_store_path=str(store_root),
        remote_database_fallback=remote,
        tdx_data_path=tdx,
        allow_remote_sources=False,
        max_workers=4,
        retry_max_retries=1,
        retry_delay=0.1,
        retry_backoff_factor=1.0,
        retry_max_delay=1.0,
    ))


class ExplodingEngineDB:
    """任何远程访问都会炸掉的假数据库，用于断言本地路径零公网。"""

    def is_connected(self) -> bool:
        return True

    @property
    def engine(self):
        raise AssertionError('本地路径不应访问远程数据库引擎')


@pytest.fixture(autouse=True)
def _reset_stores(monkeypatch):
    reset_local_stores()
    monkeypatch.setattr(global_data_cache, '_cache', {}, raising=False)
    route_metrics.reset()
    yield
    reset_local_stores()


def test_covers_full_and_incremental(tmp_path):
    store = LocalQuoteStore(tmp_path)
    frame = make_frame()
    store.write_daily('stock', '600000.SH', frame, full=True, source_mtime_ns=123)
    store.flush_meta()

    reloaded = LocalQuoteStore(tmp_path)
    assert reloaded.covers('stock', '600000.SH', '2024-01-01', '2024-01-10')
    # 整文件构建允许请求早于存量起点（前缀已完整）
    assert reloaded.covers('stock', '600000.SH', '2010-01-01', '2024-01-10')

    tail = make_frame(rows=3, start=date(2024, 3, 1))
    store.write_daily('stock', '000001.SZ', tail, full=False, source_mtime_ns=456)
    store.flush_meta()
    reloaded = LocalQuoteStore(tmp_path)
    # 增量片段：请求早于存量起点时必须回退，不得当成完整前缀
    assert not reloaded.covers('stock', '000001.SZ', '2024-01-01', '2024-03-10')
    assert reloaded.covers('stock', '000001.SZ', '2024-03-01', '2024-03-10')


def test_is_stale_uses_source_mtime(tmp_path):
    store = LocalQuoteStore(tmp_path)
    store.write_daily('stock', '600000.SH', make_frame(), full=True, source_mtime_ns=1)
    store.flush_meta()

    source = tmp_path / 'sh600000.day'
    source.write_bytes(b'\x00' * 32)
    assert LocalQuoteStore(tmp_path).is_stale('stock', '600000.SH', source)
    assert not LocalQuoteStore(tmp_path).is_stale('stock', '600000.SH', None)


def test_fetcher_hits_local_store_without_remote(tmp_path):
    store = LocalQuoteStore(tmp_path)
    store.write_daily('stock', '600000.SH', make_frame(), full=True, source_mtime_ns=1)
    store.flush_meta()
    reset_local_stores()

    fetcher = DataFetcher(make_config(tmp_path, remote=False),
                          db_manager=ExplodingEngineDB())
    out = fetcher.get_stock_data('600000.SH', '2024-01-01', '2024-01-10')

    assert out.height == 5
    assert 'raw_close' in out.columns  # hfq 出口
    assert route_metrics.snapshot()['counters'].get('local_store:hit', 0) >= 1


def test_db_partial_detection_does_not_short_circuit(tmp_path):
    store = LocalQuoteStore(tmp_path)
    store.write_daily('stock', '600000.SH', make_frame(rows=12, start=date(2024, 1, 2)),
                      full=True, source_mtime_ns=1)
    store.flush_meta()
    reset_local_stores()

    fetcher = DataFetcher(make_config(tmp_path), db_manager=None)
    partial = pl.DataFrame({
        'date': [date(2024, 1, 2), date(2024, 1, 3)],
        'close': [10.0, 11.0],
    })
    assert fetcher._db_result_is_partial('stock', '600000.SH', '2024-01-01', '2024-01-20', partial)
    # 覆盖到本地末端后不再判残缺
    full = pl.DataFrame({
        'date': [date(2024, 1, 2), date(2024, 1, 13)],
        'close': [10.0, 21.0],
    })
    assert not fetcher._db_result_is_partial(
        'stock', '600000.SH', '2024-01-01', '2024-01-20', full)


def test_dividend_events_use_local_snapshot_only(tmp_path):
    factor_dir = tmp_path / 'factors'
    factor_dir.mkdir(parents=True)
    pl.DataFrame({
        'ts_code': ['600000.SH'] * 3,
        'ex_date': [date(2023, 6, 1), date(2024, 6, 1), date(2025, 6, 1)],
        'cash_div': [0.5, 0.6, 0.7],
        'share_div': [0.0, 0.0, 0.0],
        'rights_issue_ratio': [0.0, 0.0, 0.0],
        'rights_issue_price': [0.0, 0.0, 0.0],
    }).write_parquet(factor_dir / 'stock_dividend.parquet')
    reset_local_stores()

    fetcher = DataFetcher(make_config(tmp_path), db_manager=ExplodingEngineDB())
    events = fetcher._get_dividend_events('600000.SH', date(2024, 1, 1))

    assert events.height == 2
    assert events['ex_date'].min() == date(2024, 6, 1)


def test_data_manager_dividend_prefers_local(tmp_path):
    factor_dir = tmp_path / 'factors'
    factor_dir.mkdir(parents=True)
    pl.DataFrame({
        'ts_code': ['600000.SH'],
        'symbol': ['600000'],
        'name': ['浦发银行'],
        'dividend_year': ['2023'],
        'report_date': [date(2023, 12, 31)],
        'record_date': [date(2024, 5, 20)],
        'ex_date': [date(2024, 5, 21)],
        'pay_date': [date(2024, 5, 28)],
        'cash_div': [0.5],
        'share_div': [0.0],
        'total_div': [0.5],
        'rights_issue_price': [0.0],
        'rights_issue_ratio': [0.0],
    }).write_parquet(factor_dir / 'stock_dividend.parquet')
    reset_local_stores()

    manager = object.__new__(DataManager)
    manager.config = make_config(tmp_path)
    manager.db_manager = ExplodingEngineDB()

    out = manager.get_stock_dividend('600000.SH')
    assert out.height == 1
    assert out['ex_date'][0] == date(2024, 5, 21)


def test_get_stock_batch_matches_single_fetch(tmp_path):
    store = LocalQuoteStore(tmp_path)
    for code in ('600000.SH', '000001.SZ'):
        store.write_daily('stock', code, make_frame(rows=6, start=date(2024, 1, 2)),
                          full=True, source_mtime_ns=1)
    store.flush_meta()
    reset_local_stores()

    fetcher = DataFetcher(make_config(tmp_path), db_manager=ExplodingEngineDB())
    batch = fetcher.get_stock_batch(
        ['600000.SH', '000001.SZ'], '2024-01-01', '2024-01-10',
        adjustment_type='hfq')
    assert set(batch) == {'600000.SH', '000001.SZ'}

    global_data_cache.clear()
    for code in batch:
        single = fetcher.get_stock_data(code, '2024-01-01', '2024-01-10',
                                        adjustment_type='hfq')
        assert single.equals(batch[code]), f'{code} 批量与逐票结果不一致'


def test_incremental_only_rebuilds_stale_symbols(tmp_path):
    """只重建尾部落后的标的，已最新的标的直接跳过。"""
    tdx = tmp_path / 'tdx'
    _write_day_file(tdx, 'sh', '600000', _RECS)
    _write_day_file(tdx, 'sz', '000001', _RECS[:1])
    store_root = tmp_path / 'store'
    store = LocalQuoteStore(store_root)
    factor_store = LocalFactorStore(store_root)
    cfg = _build_config(tdx, store_root)

    full = build(cfg, store, factor_store, data_type='stock')
    assert full['symbols'] == 2

    # 000001 源端前进一天，600000 不变
    _write_day_file(tdx, 'sz', '000001', _RECS)
    stats = build(cfg, store, factor_store, data_type='stock', incremental=True)

    assert stats['symbols'] == 1
    assert stats['skipped_uptodate'] == 1
    assert store.coverage('stock', '000001.SZ').end == date(2024, 1, 3)


def test_incremental_detects_same_date_content_change(tmp_path):
    """日期相同但末行内容变化时仍触发重建（不依赖 mtime）。"""
    tdx = tmp_path / 'tdx'
    _write_day_file(tdx, 'sh', '600000', _RECS)
    store_root = tmp_path / 'store'
    store = LocalQuoteStore(store_root)
    factor_store = LocalFactorStore(store_root)
    cfg = _build_config(tdx, store_root)
    build(cfg, store, factor_store, data_type='stock')

    modified = [*_RECS[:-1], ('20240103', 10.0, 10.8, 9.9, 10.5, 120, 9999)]
    _write_day_file(tdx, 'sh', '600000', modified)
    stats = build(cfg, store, factor_store, data_type='stock', incremental=True)

    assert stats['symbols'] == 1
    last = store.read_file('stock', '600000.SH').sort('date').tail(1).to_dicts()[0]
    assert last['amount'] == pytest.approx(9999.0)


def test_build_max_date_truncates_intraday_row(tmp_path):
    """max_date 只写入不晚于目标日的记录，避免盘中行入库。"""
    tdx = tmp_path / 'tdx'
    records = [*_RECS, ('20240104', 10.5, 11.0, 10.4, 10.8, 130, 1404)]
    _write_day_file(tdx, 'sh', '600000', records)
    store_root = tmp_path / 'store'
    store = LocalQuoteStore(store_root)
    factor_store = LocalFactorStore(store_root)
    cfg = _build_config(tdx, store_root)

    build(cfg, store, factor_store, data_type='stock',
          max_date=date(2024, 1, 3))

    assert store.coverage('stock', '600000.SH').end == date(2024, 1, 3)


def test_filter_intraday_rows_drops_today_before_close():
    """盘中守卫：15:00 前丢弃当日行，收盘后保留，force 强制保留。"""
    frame = pl.DataFrame({'date': [date(2024, 1, 2), date(2024, 1, 3)]})

    assert filter_intraday_rows(frame, moment=datetime(2024, 1, 3, 10, 0)).height == 1
    assert filter_intraday_rows(frame, moment=datetime(2024, 1, 3, 15, 1)).height == 2
    assert filter_intraday_rows(
        frame, moment=datetime(2024, 1, 3, 10, 0), force=True).height == 2


def test_incremental_write_keeps_other_meta_entries(tmp_path):
    """增量写单只不应清空元数据里其他标的（历史回归）。"""
    store = LocalQuoteStore(tmp_path)
    store.write_daily(
        'stock', '600000.SH', make_frame(rows=3), full=True, source_mtime_ns=1)
    store.flush_meta()

    # 新实例模拟下一次进程启动：内存元数据为空，只写另一只标的
    other = LocalQuoteStore(tmp_path)
    other.write_daily(
        'stock', '000001.SZ', make_frame(rows=3), full=True, source_mtime_ns=2)
    other.flush_meta()

    meta = LocalQuoteStore(tmp_path)._load_meta()
    assert ('stock', '600000.SH') in meta
    assert ('stock', '000001.SZ') in meta


def test_rescan_meta_rebuilds_from_parquet(tmp_path):
    """元数据丢失后可从行情 Parquet 重建覆盖度。"""
    store = LocalQuoteStore(tmp_path)
    store.write_daily(
        'stock', '600000.SH', make_frame(rows=3, start=date(2024, 1, 2)),
        full=True, source_mtime_ns=1)
    store.flush_meta()
    LocalQuoteStore(tmp_path).meta_path.unlink()

    fresh = LocalQuoteStore(tmp_path)
    result = fresh.rescan_meta()

    assert result['total'] >= 1
    cov = fresh.coverage('stock', '600000.SH')
    assert cov is not None
    assert cov.rows == 3
    assert cov.end == date(2024, 1, 4)


def test_verify_target_classifies_source_missing_target(tmp_path):
    """源文件缺目标日时归为 source_stale，而不是本地漏建。"""
    tdx = tmp_path / 'tdx'
    _write_day_file(tdx, 'sh', '600000', _RECS)
    store_root = tmp_path / 'store'
    store = LocalQuoteStore(store_root)
    factor_store = LocalFactorStore(store_root)
    cfg = _build_config(tdx, store_root)
    build(cfg, store, factor_store, data_type='stock')

    result = verify_incremental_target(
        cfg, store, target_date=date(2024, 1, 4), data_type='stock')

    assert result['source_stale'] == 1
    assert result['local_behind'] == 0
    assert result['source_stale_samples'][0]['reason'] == 'source_lagging'


def test_verify_target_flags_local_behind_when_source_has_target(tmp_path):
    """源有目标日、本地没有时才算本地漏建并告警。"""
    tdx = tmp_path / 'tdx'
    _write_day_file(tdx, 'sh', '600000', _RECS)
    store_root = tmp_path / 'store'
    store = LocalQuoteStore(store_root)
    store.write_daily(
        'stock', '600000.SH', make_frame(rows=2, start=date(2024, 1, 2)),
        full=True, source_mtime_ns=1)
    store.flush_meta()
    cfg = _build_config(tdx, store_root)

    # 源端有 2024-01-03，本地只到 01-03，目标日设为 01-03 时不算落后；
    # 把目标日设为源端不含的 01-04 时归 source_stale，这里改为直接构造
    # 本地落后一天且源含目标日的场景：本地只到 01-02。
    store.write_daily(
        'stock', '600000.SH', make_frame(rows=1, start=date(2024, 1, 2)),
        full=True, source_mtime_ns=1)
    store.flush_meta()

    result = verify_incremental_target(
        cfg, store, target_date=date(2024, 1, 3), data_type='stock')

    assert result['local_behind'] == 1
    assert result['source_stale'] == 0


def test_rebuild_factors_matches_dividend_path(tmp_path):
    """全市场因子补齐：结果与「读分红现算」逐行一致，且只补缺失标的。"""
    quote_store = LocalQuoteStore(tmp_path)
    for code in ('600000.SH', '000001.SZ'):
        quote_store.write_daily('stock', code, make_frame(rows=6, start=date(2024, 1, 2)),
                                full=True, source_mtime_ns=1)
    quote_store.flush_meta()

    factor_dir = tmp_path / 'factors'
    factor_dir.mkdir(parents=True)
    pl.DataFrame({
        'ts_code': ['600000.SH'],
        'ex_date': [date(2024, 1, 4)],
        'cash_div': [0.5],
        'share_div': [0.0],
        'rights_issue_ratio': [0.0],
        'rights_issue_price': [0.0],
    }).write_parquet(factor_dir / 'stock_dividend.parquet')
    factor_store = LocalFactorStore(tmp_path)

    result = rebuild_factors_from_local(quote_store, factor_store, chunk_size=1)
    assert result['codes_added'] == 2
    assert result['factor_codes'] == 2

    snapshot = pl.read_parquet(factor_dir / 'stock_adj_factor.parquet')
    prices = quote_store.read_file('stock', '600000.SH').select([
        pl.col('date').alias('trade_date'), pl.col('close').cast(pl.Float64)])
    expected = AdjFactorCalculator.calculate_factors(
        prices, factor_store.dividends('600000.SH'))
    actual = snapshot.filter(pl.col('ts_code') == '600000.SH').select(
        ['trade_date', 'qfq_factor', 'hfq_factor'])
    joined = actual.join(expected.select(['trade_date', 'qfq_factor', 'hfq_factor']),
                         on='trade_date', suffix='_calc')
    assert (joined['qfq_factor'] - joined['qfq_factor_calc']).abs().max() == 0.0
    assert (joined['hfq_factor'] - joined['hfq_factor_calc']).abs().max() == 0.0

    flat = snapshot.filter(pl.col('ts_code') == '000001.SZ')
    assert flat['qfq_factor'].max() == 1.0 and flat['hfq_factor'].max() == 1.0

    # 幂等：再次只补缺失时为 0；显式重算单只时其余标的仍保留
    again = rebuild_factors_from_local(quote_store, factor_store)
    assert again['codes_added'] == 0
    updated = rebuild_factors_from_local(
        quote_store, factor_store, codes=['600000.SH'], include_existing=True)
    assert updated['factor_codes'] == 2


def test_factor_alignment_runs_when_flush_meta_fails(tmp_path, monkeypatch):
    """flush_meta 抛错时因子副本对齐仍必须执行（2026-11-11 16:25 事故回归）。"""
    tdx = tmp_path / 'tdx'
    _write_day_file(tdx, 'sh', '600000', _RECS)
    store_root = tmp_path / 'store'
    store = LocalQuoteStore(store_root)
    factor_store = LocalFactorStore(store_root)
    cfg = _build_config(tdx, store_root)

    aligned: list[dict] = []

    def boom(self):  # noqa: ANN001
        raise RuntimeError('could not append value: datetime dtype conflict')

    monkeypatch.setattr(LocalQuoteStore, 'flush_meta', boom)
    monkeypatch.setattr(
        build_module, '_align_written_factor_copies',
        lambda qs, fs, written: aligned.append(dict(written)) or {})

    stats = build(cfg, store, factor_store, data_type='stock')

    assert stats['meta_error'] and 'dtype conflict' in stats['meta_error']
    assert aligned, 'flush_meta 失败后 _align_written_factor_copies 未被调用'
    assert aligned[0].get('stock') == ['600000.SH']


def test_full_build_snapshots_quotes_dir_before_overwrite(tmp_path):
    """全量重建前把现有 quotes/ 移入 _backup，不就地覆盖（DG-11）。"""
    tdx = tmp_path / 'tdx'
    _write_day_file(tdx, 'sh', '600000', _RECS)
    store_root = tmp_path / 'store'
    store = LocalQuoteStore(store_root)
    factor_store = LocalFactorStore(store_root)
    cfg = _build_config(tdx, store_root)

    # 首次全量：quotes/ 尚不存在，不应产生空快照
    build(cfg, store, factor_store, data_type='stock')
    assert not list((store_root / '_backup').glob('quotes_*'))

    modified = [*_RECS, ('20240104', 10.5, 11.0, 10.4, 10.8, 130, 1404)]
    _write_day_file(tdx, 'sh', '600000', modified)
    build(cfg, store, factor_store, data_type='stock')

    snaps = sorted((store_root / '_backup').glob('quotes_*'))
    assert len(snaps) == 1
    assert (snaps[0] / 'stock' / 'sh' / '600000.SH.parquet').exists()
    assert store.coverage('stock', '600000.SH').end == date(2024, 1, 4)


def test_resolve_incremental_defaults_to_incremental():
    """裸 build 默认增量；只有显式 --all 才进全量。"""
    assert build_module._resolve_incremental(
        all_=False, codes=None, since=None, incremental=False) is True
    assert build_module._resolve_incremental(
        all_=True, codes=None, since=None, incremental=False) is False
    assert build_module._resolve_incremental(
        all_=False, codes=['600000.SH'], since=None, incremental=False) is False
    assert build_module._resolve_incremental(
        all_=False, codes=None, since=date(2024, 1, 1), incremental=False) is False
    with pytest.raises(ValueError):
        build_module._resolve_incremental(
            all_=True, codes=['600000.SH'], since=None, incremental=False)


def _rebuild_fixture(store_root: Path, codes: tuple[str, ...]
                     ) -> tuple[LocalQuoteStore, LocalFactorStore]:
    """构造若干标的的本地行情列存，返回（行情库, 因子库）。"""
    quote_store = LocalQuoteStore(store_root)
    for code in codes:
        quote_store.write_daily('stock', code, make_frame(rows=4, start=date(2024, 1, 2)),
                                full=True, source_mtime_ns=1)
    quote_store.flush_meta()
    return quote_store, LocalFactorStore(store_root)


def _rebuild_dir(store_root: Path) -> Path:
    return Path(store_root) / 'factors' / 'rebuild'


def _manifest(store_root: Path) -> dict:
    return json.loads((_rebuild_dir(store_root) / 'manifest.json').read_text(encoding='utf-8'))


def test_rebuild_resumes_when_parts_complete_but_manifest_unfinished(tmp_path, monkeypatch):
    """合并完成、manifest 尚未记 completed 时中断：重启必须续跑，不得重采。

    这条用例直接锁住收尾顺序（先 manifest 后删分块）：若把顺序改回「先清
    分块再写 manifest」，中断后分块已不存在，重启只能全量重采，用例失败。
    """
    quote_store, factor_store = _rebuild_fixture(
        tmp_path, ('600000.SH', '000001.SZ', '600519.SH', '000002.SZ'))
    real_write_manifest = factor_rebuild_module._write_manifest

    def _fail_on_completed(path, payload):
        if payload.get('status') == 'completed':
            raise OSError('模拟：manifest 收尾前进程中断')
        return real_write_manifest(path, payload)

    monkeypatch.setattr(factor_rebuild_module, '_write_manifest', _fail_on_completed)
    with pytest.raises(OSError):
        rebuild_factors_from_local(quote_store, factor_store, chunk_size=2)

    parts = sorted(_rebuild_dir(tmp_path).glob('part_*.parquet'))
    assert len(parts) == 2, '中断时不得提前删掉分块'
    state = _manifest(tmp_path)
    assert state['status'] == 'running' and state['completed_parts'] == [0, 1]

    calls: list[int] = []

    def _forbid_recompute(index, *args, **kwargs):
        calls.append(index)
        raise AssertionError(f'分块 {index} 已落盘，不应重采')

    monkeypatch.setattr(factor_rebuild_module, '_write_manifest', real_write_manifest)
    monkeypatch.setattr(factor_rebuild_module, '_process_chunk', _forbid_recompute)
    result = rebuild_factors_from_local(
        quote_store, factor_store, chunk_size=2, include_existing=True)

    assert calls == []
    assert result['factor_codes'] == 4
    snapshot = pl.read_parquet(tmp_path / 'factors' / 'stock_adj_factor.parquet')
    assert snapshot['ts_code'].n_unique() == 4
    assert _manifest(tmp_path)['status'] == 'completed'
    assert list(_rebuild_dir(tmp_path).glob('part_*.parquet')) == []


def test_rebuild_resumes_from_recorded_parts_after_mid_loop_crash(tmp_path, monkeypatch):
    """分块 0 已落盘、分块 1 计算中断：重启只补分块 1。"""
    quote_store, factor_store = _rebuild_fixture(
        tmp_path, ('600000.SH', '000001.SZ', '600519.SH', '000002.SZ'))
    real_process = factor_rebuild_module._process_chunk

    def _explode_on_second(index, *args, **kwargs):
        if index == 1:
            raise RuntimeError('模拟：分块 1 计算中断')
        return real_process(index, *args, **kwargs)

    monkeypatch.setattr(factor_rebuild_module, '_process_chunk', _explode_on_second)
    with pytest.raises(RuntimeError):
        rebuild_factors_from_local(quote_store, factor_store, chunk_size=2)
    assert _manifest(tmp_path)['completed_parts'] == [0]

    calls: list[int] = []

    def _counting(index, *args, **kwargs):
        calls.append(index)
        return real_process(index, *args, **kwargs)

    monkeypatch.setattr(factor_rebuild_module, '_process_chunk', _counting)
    result = rebuild_factors_from_local(
        quote_store, factor_store, chunk_size=2, include_existing=True)

    assert calls == [1]
    assert result['factor_codes'] == 4
    snapshot = pl.read_parquet(tmp_path / 'factors' / 'stock_adj_factor.parquet')
    assert snapshot['ts_code'].n_unique() == 4


def test_rebuild_does_not_resume_across_chunk_size_change(tmp_path, monkeypatch):
    """分块粒度变了必须重采：旧下标不能复用，否则会漏掉新布局里的标的。"""
    codes = ('600000.SH', '000001.SZ', '600519.SH', '000002.SZ', '600036.SH')
    quote_store, factor_store = _rebuild_fixture(tmp_path, codes)
    real_process = factor_rebuild_module._process_chunk

    def _explode_on_second(index, *args, **kwargs):
        if index == 1:
            raise RuntimeError('模拟：分块 1 计算中断')
        return real_process(index, *args, **kwargs)

    monkeypatch.setattr(factor_rebuild_module, '_process_chunk', _explode_on_second)
    with pytest.raises(RuntimeError):
        rebuild_factors_from_local(quote_store, factor_store, chunk_size=2)
    assert len(list(_rebuild_dir(tmp_path).glob('part_*.parquet'))) == 1

    calls: list[int] = []

    def _counting(index, *args, **kwargs):
        calls.append(index)
        return real_process(index, *args, **kwargs)

    monkeypatch.setattr(factor_rebuild_module, '_process_chunk', _counting)
    rebuild_factors_from_local(
        quote_store, factor_store, chunk_size=3, include_existing=True)

    assert calls == [0, 1]
    snapshot = pl.read_parquet(tmp_path / 'factors' / 'stock_adj_factor.parquet')
    assert snapshot['ts_code'].n_unique() == len(codes)


def _write_factor_snapshot(store_root: Path, code: str, dates: list[date],
                           qfq: list[float], hfq: list[float]) -> None:
    factor_dir = Path(store_root) / 'factors'
    factor_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'ts_code': [code] * len(dates),
        'trade_date': dates,
        'qfq_factor': qfq,
        'hfq_factor': hfq,
    }).write_parquet(factor_dir / 'stock_adj_factor.parquet')


def test_read_daily_uses_factor_snapshot_instead_of_stale_copy(tmp_path):
    """行情内嵌因子副本过期时，读出口必须以因子快照为准。"""
    store = LocalQuoteStore(tmp_path)
    frame = make_frame(5).with_columns(pl.lit('600000.SH').alias('ts_code'))
    store.write_daily('stock', '600000.SH', frame)
    store.flush_meta()
    _write_factor_snapshot(tmp_path, '600000.SH', frame['date'].to_list(),
                           [0.9, 0.9, 0.9, 1.0, 1.0],
                           [1.0, 1.0, 1.003, 1.003, 1.003])

    reader = LocalQuoteStore(tmp_path)
    sliced = reader.read_daily('stock', '600000.SH',
                               date(2024, 1, 2), date(2024, 1, 3))
    assert sliced is not None
    assert sliced['qfq_factor'].to_list() == [0.9, 0.9]
    assert sliced['hfq_factor'].to_list() == [1.0, 1.0]

    # 默认读文件仍是原始副本（供扫描/增量合并使用），显式要求时才覆盖
    assert reader.read_file('stock', '600000.SH')['qfq_factor'].to_list() == [1.0] * 5
    fixed = reader.read_file('stock', '600000.SH', with_factors=True)
    assert fixed['qfq_factor'].to_list() == [0.9, 0.9, 0.9, 1.0, 1.0]


def test_factor_lru_skips_stat_on_hit_and_capacity_is_bounded(tmp_path, monkeypatch):
    """因子 LRU 命中路径不再逐次 stat()，容量按可用内存自适应且有上下限。"""
    _write_factor_snapshot(tmp_path, '600000.SH', [date(2024, 1, 2), date(2024, 1, 3)],
                           [1.0, 1.0], [1.0, 1.0])
    factor_store = LocalFactorStore(tmp_path)

    stats: list[Path] = []
    real_stat = Path.stat

    def counting_stat(self, *args, **kwargs):
        stats.append(self)
        return real_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, 'stat', counting_stat)
    first = factor_store.factors('600000.SH')
    after_first = len(stats)
    second = factor_store.factors('600000.SH')

    assert after_first >= 1  # 首次读取必须探一次 mtime
    assert len(stats) == after_first  # TTL 内命中不再 stat
    assert first is not None and first.equals(second)

    capacity = factor_lru_capacity()
    assert 128 <= capacity <= 8192


def test_factor_invalidate_picks_up_rebuilt_snapshot(tmp_path):
    """同进程重建快照后 invalidate() 必须立刻看到新值（不等 TTL）。"""
    _write_factor_snapshot(tmp_path, '600000.SH', [date(2024, 1, 2)],
                           [0.5], [1.0])
    factor_store = LocalFactorStore(tmp_path)
    assert factor_store.factors('600000.SH')['qfq_factor'].to_list() == [0.5]

    _write_factor_snapshot(tmp_path, '600000.SH', [date(2024, 1, 2)],
                           [0.8], [1.0])
    factor_store.invalidate()
    assert factor_store.factors('600000.SH')['qfq_factor'].to_list() == [0.8]


def test_factor_mtime_ttl_fallback_sees_cross_process_rebuild(tmp_path, monkeypatch):
    """跨进程重建（无人调用 invalidate）在 TTL 到期后也必须被感知。"""
    _write_factor_snapshot(tmp_path, '600000.SH', [date(2024, 1, 2)],
                           [0.5], [1.0])
    factor_store = LocalFactorStore(tmp_path)
    assert factor_store.factors('600000.SH')['qfq_factor'].to_list() == [0.5]

    monkeypatch.setattr('src.data.local_store.store._FACTOR_MTIME_TTL', 0.0)
    _write_factor_snapshot(tmp_path, '600000.SH', [date(2024, 1, 2)],
                           [0.8], [1.0])
    assert factor_store.factors('600000.SH')['qfq_factor'].to_list() == [0.8]


def test_sync_quote_factors_aligns_copy_then_verify_is_zero(tmp_path):
    """副本对齐后全量核对不一致只数必须为 0，且 dry-run 不改文件。"""
    store = LocalQuoteStore(tmp_path)
    frame = make_frame(5).with_columns(pl.lit('600000.SH').alias('ts_code'))
    store.write_daily('stock', '600000.SH', frame)
    store.flush_meta()
    _write_factor_snapshot(tmp_path, '600000.SH', frame['date'].to_list(),
                           [0.9, 0.9, 0.9, 1.0, 1.0],
                           [1.0, 1.0, 1.003, 1.003, 1.003])
    factor_store = LocalFactorStore(tmp_path)

    dry = refresh_quote_factor_columns(store, factor_store, dry_run=True)
    assert dry['changed_codes'] == 1
    assert dry['changed_rows'] == 6
    assert store.read_file('stock', '600000.SH')['qfq_factor'].to_list() == [1.0] * 5

    done = refresh_quote_factor_columns(store, factor_store)
    assert done['changed_codes'] == 1
    assert store.read_file('stock', '600000.SH')['qfq_factor'].to_list() == [
        0.9, 0.9, 0.9, 1.0, 1.0]

    stats = verify_quote_factor_consistency(store, factor_store)
    assert stats['stock']['mismatch_codes'] == 0
    assert stats['stock']['mismatch_rows'] == 0
    assert stats['index']['mismatch_codes'] == 0
    assert stats['fund']['mismatch_codes'] == 0


# ----------------------------------------------------------------------
# refresh-factors：本地快照为唯一权威源，默认禁止被远程库反向覆盖
# ----------------------------------------------------------------------
def _write_dividend_snapshot(store_root: Path, rows: list[tuple[str, date, float]]) -> Path:
    factor_dir = Path(store_root) / 'factors'
    factor_dir.mkdir(parents=True, exist_ok=True)
    path = factor_dir / 'stock_dividend.parquet'
    pl.DataFrame({
        'ts_code': [r[0] for r in rows],
        'ex_date': [r[1] for r in rows],
        'cash_div': [r[2] for r in rows],
        'share_div': [0.0 for _ in rows],
        'rights_issue_ratio': [0.0 for _ in rows],
        'rights_issue_price': [0.0 for _ in rows],
    }).write_parquet(path)
    return path


class _FakeDBConnection:
    """占位连接对象：read_database 已被替换，不需要真实游标。"""


class _FakeDBEngine:
    def connect(self):
        return contextlib.nullcontext(_FakeDBConnection())


class _FakeDatabaseManager:
    """拦截 refresh_factors 的数据库访问，断言全程不产生公网请求。"""

    def __init__(self, config=None):
        self.engine = _FakeDBEngine()
        self.disconnected = False

    def connect(self) -> bool:
        return True

    def disconnect(self) -> None:
        self.disconnected = True


def _patch_remote_refresh(monkeypatch, factors: pl.DataFrame, dividends: pl.DataFrame) -> None:
    """把 refresh_factors 的数据库读取替换成给定帧（按 SQL 关键字分流）。"""
    import src.database.db_manager as db_module

    monkeypatch.setattr(db_module, 'DatabaseManager', _FakeDatabaseManager)
    monkeypatch.setattr(
        pl, 'read_database',
        lambda sql, conn: factors if 'stock_adj_factor' in sql else dividends)


def _inject_parquet_write_crash(monkeypatch, *, fail_on_call: int) -> None:
    """让第 N 次 parquet 写入先落半截字节再抛异常，模拟写盘中断。"""
    calls = {'count': 0}
    original = pl.DataFrame.write_parquet

    def _crashing(self, file, *args, **kwargs):
        calls['count'] += 1
        if calls['count'] == fail_on_call:
            target = Path(file)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b'PARTIAL-PARQUET')
            raise OSError(f'注入写盘中断（第 {calls["count"]} 次写入）')
        return original(self, file, *args, **kwargs)

    monkeypatch.setattr(pl.DataFrame, 'write_parquet', _crashing)


def test_refresh_factors_refuses_without_explicit_switches(tmp_path):
    """默认拒绝覆盖：缺 allow-remote / 缺 force-overwrite / 口令错误都中止。"""
    factor_store = LocalFactorStore(tmp_path)

    with pytest.raises(PermissionError, match='allow-remote'):
        refresh_factors(make_config(tmp_path), factor_store)
    with pytest.raises(PermissionError, match='默认禁止'):
        refresh_factors(make_config(tmp_path), factor_store, allow_remote=True)
    with pytest.raises(PermissionError, match='二次确认'):
        refresh_factors(make_config(tmp_path), factor_store,
                        allow_remote=True, force_overwrite=True, confirm='yes')

    # 拒绝路径不得创建任何快照文件
    assert not (tmp_path / 'factors').exists()


def test_refresh_factors_aborts_when_remote_regresses(tmp_path, monkeypatch):
    """危险开关下差异超阈值中止：快照文件字节不变、不写 meta。"""
    _write_dividend_snapshot(tmp_path, [
        ('600000.SH', date(2024, 1, 4), 0.5),
        ('600000.SH', date(2025, 1, 4), 0.6),
    ])
    _write_factor_snapshot(tmp_path, '600000.SH', [date(2024, 1, 2)], [0.9], [1.1])
    factor_path = tmp_path / 'factors' / 'stock_adj_factor.parquet'
    dividend_path = tmp_path / 'factors' / 'stock_dividend.parquet'
    before = (factor_path.read_bytes(), dividend_path.read_bytes())

    # 远程：少一条分红事件、最新除权日倒退、因子末值恒 1.0
    remote_dividends = pl.DataFrame({
        'ts_code': ['600000.SH'],
        'ex_date': [date(2024, 1, 4)],
        'cash_div': [0.5],
    })
    remote_factors = pl.DataFrame({
        'ts_code': ['600000.SH'],
        'trade_date': [date(2024, 1, 2)],
        'qfq_factor': [1.0],
        'hfq_factor': [1.0],
    })
    _patch_remote_refresh(monkeypatch, remote_factors, remote_dividends)
    config = make_config(tmp_path)
    factor_store = LocalFactorStore(tmp_path)

    with pytest.raises(RuntimeError, match='存在回退'):
        refresh_factors(config, factor_store,
                        allow_remote=True, force_overwrite=True,
                        confirm='OVERWRITE-LOCAL-SNAPSHOT')

    assert (factor_path.read_bytes(), dividend_path.read_bytes()) == before
    assert not (tmp_path / 'factors' / 'meta.json').exists()

    # dry-run 只报告 would_abort，同样不落盘
    planned = refresh_factors(config, factor_store,
                              allow_remote=True, force_overwrite=True,
                              confirm='OVERWRITE-LOCAL-SNAPSHOT', dry_run=True)
    assert planned['would_abort'] is True
    assert planned['validation']['ok'] is False
    assert planned['validation']['event_drop_rows'] == 1
    assert planned['validation']['event_drop_codes'] == 1
    assert planned['validation']['latest_date_regression_codes'] == 1
    assert planned['validation']['factor_mismatch_codes'] == 1
    assert (factor_path.read_bytes(), dividend_path.read_bytes()) == before


def test_refresh_factors_cold_start_writes_snapshot(tmp_path, monkeypatch):
    """首次冷启动（无快照）在 force + confirm 下允许写入远程数据。"""
    factors = pl.DataFrame({
        'ts_code': ['600000.SH'],
        'trade_date': [date(2024, 1, 2)],
        'qfq_factor': [0.9],
        'hfq_factor': [1.0],
    })
    dividends = pl.DataFrame({
        'ts_code': ['600000.SH'],
        'ex_date': [date(2024, 1, 4)],
        'cash_div': [0.5],
        'share_div': [0.0],
        'rights_issue_ratio': [0.0],
        'rights_issue_price': [0.0],
    })
    _patch_remote_refresh(monkeypatch, factors, dividends)

    meta = refresh_factors(make_config(tmp_path), LocalFactorStore(tmp_path),
                           allow_remote=True, force_overwrite=True,
                           confirm='OVERWRITE-LOCAL-SNAPSHOT')

    assert meta['source'] == 'remote_mysql'
    assert meta['forced_overwrite'] is True
    assert meta['validation']['ok'] is True
    written = pl.read_parquet(tmp_path / 'factors' / 'stock_adj_factor.parquet')
    assert written.height == 1


def test_dividend_fix_columns_survive_remote_refresh(tmp_path, monkeypatch):
    """远程 refresh 全量重写分红快照后，cash_div_fix / share_div_fix 必须保留且生效。

    守护两件事：
    1. 结构上 ``_DIVIDEND_COLUMNS``（refresh 的 SELECT 列清单）必须覆盖两个
       fix 列，且不得出现模型里不存在的列（否则 SELECT 直接报错或静默漏列）；
    2. 行为上刷新后的快照仍带 fix 列，且 ``AdjFactorCalculator`` 会用 fix
       覆盖原值（因子与修正前不同），避免一次日常 refresh 把人工修正抹掉。
    """
    from src.data.local_store.build import _DIVIDEND_COLUMNS
    from src.database.models.stock import StockDividend

    model_columns = set(StockDividend.__table__.columns.keys())
    assert {'cash_div_fix', 'share_div_fix'} <= set(_DIVIDEND_COLUMNS)
    assert set(_DIVIDEND_COLUMNS) <= model_columns

    # 刷新前快照：只有原值列（模拟修正后又被远程全量重写的旧态）
    _write_dividend_snapshot(tmp_path, [('600000.SH', date(2024, 1, 4), 0.46)])
    _patch_remote_refresh(
        monkeypatch,
        pl.DataFrame({'ts_code': [], 'trade_date': [], 'qfq_factor': [],
                      'hfq_factor': []}),
        pl.DataFrame({
            'ts_code': ['600000.SH'],
            'symbol': ['600000'],
            'name': ['浦发银行'],
            'dividend_year': ['2016'],
            'report_date': [date(2017, 3, 30)],
            'record_date': [date(2017, 7, 7)],
            'ex_date': [date(2024, 1, 4)],
            'pay_date': [date(2024, 1, 10)],
            'cash_div': [0.46],
            'share_div': [0.0],
            'total_div': [None],
            'rights_issue_price': [0.0],
            'rights_issue_ratio': [0.0],
            'cash_div_fix': [2.97],
            'share_div_fix': [None],
        }),
    )

    refresh_factors(make_config(tmp_path), LocalFactorStore(tmp_path),
                    allow_remote=True, force_overwrite=True,
                    confirm='OVERWRITE-LOCAL-SNAPSHOT')

    written = pl.read_parquet(tmp_path / 'factors' / 'stock_dividend.parquet')
    assert {'cash_div_fix', 'share_div_fix'} <= set(written.columns)
    assert written['cash_div'].to_list() == [0.46]
    assert written['cash_div_fix'].to_list() == [2.97]

    prices = pl.DataFrame({
        'ts_code': ['600000.SH', '600000.SH'],
        'trade_date': [date(2024, 1, 3), date(2024, 1, 4)],
        'close': [20.0, 17.0],
    })
    fixed = AdjFactorCalculator.calculate_factors(prices, written)
    # 用原值 0.46 算：factor=(20-0.46)/20=0.977；用 fix 2.97 算：0.8515
    assert abs(fixed['qfq_factor'][0] - 0.8515) < 1e-9
    assert abs(fixed['qfq_factor'][1] - 1.0) < 1e-12
    raw = AdjFactorCalculator.calculate_factors(
        prices, written.drop(['cash_div_fix', 'share_div_fix']))
    assert abs(raw['qfq_factor'][0] - 0.977) < 1e-9


def test_refresh_factors_crash_on_second_file_keeps_others_intact(
        tmp_path, monkeypatch):
    """三文件写入在第二个中断：旧分红表与旧 meta 不得被半截写入改坏。"""
    _write_dividend_snapshot(tmp_path, [
        ('600000.SH', date(2024, 1, 4), 0.5),
        ('600000.SH', date(2025, 1, 4), 0.6),
    ])
    _write_factor_snapshot(tmp_path, '600000.SH', [date(2024, 1, 2)], [0.9], [1.1])
    factor_path = tmp_path / 'factors' / 'stock_adj_factor.parquet'
    dividend_path = tmp_path / 'factors' / 'stock_dividend.parquet'
    meta_path = tmp_path / 'factors' / 'meta.json'
    meta_path.write_text('{"source": "previous"}', encoding='utf-8')
    dividend_before = dividend_path.read_bytes()
    meta_before = meta_path.read_bytes()

    # 远程数据与本地快照一致（校验通过），确保能走到写盘阶段
    _patch_remote_refresh(
        monkeypatch,
        pl.DataFrame({
            'ts_code': ['600000.SH'], 'trade_date': [date(2024, 1, 2)],
            'qfq_factor': [0.9], 'hfq_factor': [1.1],
        }),
        pl.DataFrame({
            'ts_code': ['600000.SH', '600000.SH'],
            'ex_date': [date(2024, 1, 4), date(2025, 1, 4)],
            'cash_div': [0.5, 0.6], 'share_div': [0.0, 0.0],
            'rights_issue_ratio': [0.0, 0.0],
            'rights_issue_price': [0.0, 0.0],
        }),
    )
    _inject_parquet_write_crash(monkeypatch, fail_on_call=2)

    with pytest.raises(OSError, match='注入写盘中断'):
        refresh_factors(make_config(tmp_path), LocalFactorStore(tmp_path),
                        allow_remote=True, force_overwrite=True,
                        confirm='OVERWRITE-LOCAL-SNAPSHOT')

    # 第二个文件（分红）保持原字节：不得留下半截 parquet
    assert dividend_path.read_bytes() == dividend_before
    # 第三个文件（meta.json）还没轮到写，保持旧内容
    assert meta_path.read_bytes() == meta_before
    # 第一个文件已完整落盘（单文件原子），应是可读的新快照
    assert pl.read_parquet(factor_path).height == 1
    assert [p.name for p in (tmp_path / 'factors').iterdir()
            if p.name.endswith('.tmp')] == []


def test_validate_remote_overwrite_skips_when_snapshot_absent(tmp_path):
    """无快照时不判回退（冷启动不应被阈值拦住）。"""
    stats = validate_remote_overwrite(
        LocalFactorStore(tmp_path),
        pl.DataFrame({
            'ts_code': ['600000.SH'], 'trade_date': [date(2024, 1, 2)],
            'qfq_factor': [1.0], 'hfq_factor': [1.0],
        }),
        pl.DataFrame({
            'ts_code': ['600000.SH'], 'ex_date': [date(2024, 1, 4)],
        }),
    )
    assert stats['ok'] is True
    assert stats['snapshot_factor_rows'] == 0
    assert stats['snapshot_dividend_rows'] == 0


def test_overlay_warns_when_factor_snapshot_missing(tmp_path):
    """快照缺票时 overlay 必须告警，不能静默沿用内嵌副本。"""
    store = LocalQuoteStore(tmp_path)
    frame = make_frame(3).with_columns(pl.lit('600000.SH').alias('ts_code'))
    store.write_daily('stock', '600000.SH', frame)
    store.flush_meta()

    reader = LocalQuoteStore(tmp_path)
    messages: list[str] = []
    handler_id = logger.add(lambda m: messages.append(str(m)), level='WARNING')
    try:
        fixed = reader.read_file('stock', '600000.SH', with_factors=True)
    finally:
        logger.remove(handler_id)

    assert fixed is not None
    # 缺票时保留内嵌副本（本测试里副本恰为 1.0），但必须留下告警
    assert fixed['qfq_factor'].to_list() == [1.0] * 3
    assert any('因子快照缺失' in m for m in messages)


def _add_backfilled_row(store, kind, code, day) -> None:
    """模拟历史缺口回补：向本地单票文件追加一条源端当前没有的行。"""
    frame = store.read_file(kind, code)
    extra = frame.head(1).with_columns(pl.lit(day).cast(frame.schema['date']).alias('date'))
    merged = (pl.concat([frame, extra], how='vertical_relaxed')
              .unique(subset=['date'], keep='last').sort('date'))
    store.write_daily(kind, code, merged, full=True, source_mtime_ns=1)
    store.flush_meta()


def test_full_rebuild_keeps_backfilled_superset_rows(tmp_path):
    """全量重建不得用较短源端整只覆盖本地已回补的超集行。"""
    tdx = tmp_path / 'tdx'
    store_root = tmp_path / 'store'
    _write_day_file(tdx, 'sh', '600000', _RECS)
    store = LocalQuoteStore(store_root)
    factor_store = LocalFactorStore(store_root)
    cfg = _build_config(tdx, store_root)

    build(cfg, store, factor_store, data_type='stock')
    assert store.read_file('stock', '600000.SH').height == 2

    _add_backfilled_row(store, 'stock', '600000.SH', date(2023, 12, 29))
    assert store.read_file('stock', '600000.SH').height == 3

    # 源端仍只有 2 行，再次全量重建后补入行必须原样保留
    build(cfg, store, factor_store, data_type='stock')
    frame = store.read_file('stock', '600000.SH')
    assert frame.height == 3
    assert frame['date'].to_list() == [
        date(2023, 12, 29), date(2024, 1, 2), date(2024, 1, 3)]


def test_incremental_rebuild_keeps_backfilled_superset_rows(tmp_path):
    """增量重建（源端前进）同样不得裁掉本地更早的补入行。"""
    tdx = tmp_path / 'tdx'
    store_root = tmp_path / 'store'
    _write_day_file(tdx, 'sh', '600000', _RECS[:1])
    store = LocalQuoteStore(store_root)
    factor_store = LocalFactorStore(store_root)
    cfg = _build_config(tdx, store_root)

    build(cfg, store, factor_store, data_type='stock')
    _add_backfilled_row(store, 'stock', '600000.SH', date(2023, 12, 29))

    # 源端前进一天（2024-01-03），触发增量重建
    _write_day_file(tdx, 'sh', '600000', _RECS)
    stats = build(cfg, store, factor_store, data_type='stock', incremental=True)
    assert stats['symbols'] == 1

    frame = store.read_file('stock', '600000.SH')
    assert frame.height == 3
    assert frame['date'].to_list() == [
        date(2023, 12, 29), date(2024, 1, 2), date(2024, 1, 3)]

