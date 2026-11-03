"""本地列存读取路由回归测试。

覆盖：
1. 覆盖度/新鲜度判定（full 与增量片段、源文件更新后回退）
2. DataFetcher 本地列存命中且远程关闭时零公网访问
3. 数据库"非空即返回"遮蔽修正（库内最新日期落后本地时判残缺）
4. 因子/分红读取本地优先、不再访问远程
5. get_stock_batch 与逐票 get_stock_data 数值等价
"""

from datetime import date, timedelta
from types import SimpleNamespace

import polars as pl
import pytest

from src.data.adj_factor_calculator import AdjFactorCalculator
from src.data.data_cache import global_data_cache
from src.data.data_manager import DataManager
from src.data.local_store import metrics as route_metrics
from src.data.local_store import reset_local_stores
from src.data.local_store.factor_rebuild import rebuild_factors_from_local
from src.data.local_store.store import LocalFactorStore, LocalQuoteStore
from src.data.managers.data_fetcher import DataFetcher


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
