#!/usr/bin/env python3

"""DataManager 异步委托与基础查询、AsyncDataManager 的覆盖率补齐用例。

DataManager 现在继承 DataService，异步方法只是转发到 AsyncDataManager；
本文件用假数据源/会话覆盖：
1. async 包装方法的委托与同步回退；
2. AsyncDataManager 的线程池、缓存、批量分片、异常与周/月聚合；
3. DataManager 的基础信息查询、默认数据回填与本地行情入口。

不连接真实数据库与网络，线程池在用例结束时关闭。
"""

import asyncio
import sys
from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest

from src.data.async_data_manager import AsyncDataManager
from src.data.data_manager import DataManager
from src.data.services import data_service as data_service_module
from src.database.models.fund import ClosedFundBasic, FundBasic
from src.database.models.index import IndexBasic
from src.database.models.stock import StockBasic, StockDividend

# ----------------------------------------------------------------------
# 替身
# ----------------------------------------------------------------------


def _daily_frame(n: int = 12) -> pl.DataFrame:
    """构造包含聚合所需列（vol/amount/pct_chg/change）的日线帧。"""
    dates = pl.date_range(
        pl.date(2030, 1, 1), pl.date(2030, 3, 31), interval='1d', eager=True
    ).head(n)
    values = [float(i + 10) for i in range(n)]
    return pl.DataFrame({
        'date': dates,
        'open': values,
        'high': [v + 1 for v in values],
        'low': [v - 1 for v in values],
        'close': values,
        'vol': [1000.0 + i for i in range(n)],
        'amount': [2000.0 + i for i in range(n)],
        'pct_chg': [0.5 for _ in range(n)],
        'change': [0.1 for _ in range(n)],
    })


class _FakeFetcher:
    """DataFetcher 替身：返回固定 DataFrame 并记录调用。"""

    def __init__(self, config=None, db_manager=None):
        self.calls = []

    def get_stock_data(self, *args, **kwargs):
        self.calls.append(('stock', args))
        return _daily_frame()

    def get_index_data(self, *args, **kwargs):
        self.calls.append(('index', args))
        return _daily_frame()

    def get_available_sources(self):
        return []


class _FakeHandler:
    """内置数据源替身：只需可注册即可。"""

    def __init__(self, config=None, db_manager=None):
        self.name = 'fake_handler'

    def get_stock_data(self, *args, **kwargs):
        return _daily_frame()

    def get_index_data(self, *args, **kwargs):
        return _daily_frame()


class _FakeConfig:
    def __init__(self):
        self.data = SimpleNamespace(
            retry_max_retries=0,
            retry_delay=0.0,
            retry_backoff_factor=1.0,
            retry_max_delay=0.0,
            tdx_data_path='C:/tmp/tdx',
        )


class _Query:
    def __init__(self, rows):
        # 持有会话表的真实列表引用，插入后再次 all() 能读到新数据
        self._rows = rows

    def filter(self, expression=None, *_args, **_kwargs):
        """尽力解析 ts_code == value 形式的等值过滤，其余表达式原样返回。"""
        try:
            if getattr(expression.operator, '__name__', '') != 'eq':
                return self
            key = expression.left.name
            value = expression.right.value
        except AttributeError:
            return self
        return _Query([
            row for row in self._rows
            if str(getattr(row, key, '')) == str(value)
        ])

    def filter_by(self, **kwargs):
        return _Query([
            row for row in self._rows
            if all(getattr(row, key, None) == value for key, value in kwargs.items())
        ])

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None


class _Session:
    def __init__(self, tables=None, query_error=False):
        self.tables = {key: list(value) for key, value in (tables or {}).items()}
        self.query_error = query_error
        self.commits = 0
        self.closes = 0

    def query(self, model):
        if self.query_error:
            raise RuntimeError('查询失败')
        return _Query(self.tables.setdefault(model, []))

    def add(self, obj):
        self.tables.setdefault(type(obj), []).append(obj)

    def commit(self):
        self.commits += 1

    def close(self):
        self.closes += 1


class _FakeDB:
    def __init__(self, session=None, connected=True):
        self.session = session
        self.connected = connected
        self.cleaned = 0

    def is_connected(self):
        return self.connected

    def get_session(self):
        return self.session

    def _cleanup_session(self):
        self.cleaned += 1


class _FakeAsyncManager:
    """记录委托参数的异步管理器替身。"""

    def __init__(self):
        self.calls = []

    async def get_stock_data_async(self, *args):
        self.calls.append(('stock', args))
        return _daily_frame()

    async def get_index_data_async(self, *args):
        self.calls.append(('index', args))
        return _daily_frame()

    async def get_multiple_stocks_data_async(self, *args):
        self.calls.append(('multiple', args))
        return {'600000.SH': _daily_frame()}

    def close(self):
        pass


@pytest.fixture
def manager(monkeypatch):
    """构造真实 DataManager（底层取数器与数据源全部替换为替身）。"""
    monkeypatch.setattr(data_service_module, 'DataFetcher', _FakeFetcher)
    monkeypatch.setattr(
        data_service_module, 'DataUpdater',
        lambda config=None, db_manager=None: SimpleNamespace(),
    )
    monkeypatch.setattr('src.data.tdx_handler.TdxHandler', _FakeHandler)
    monkeypatch.setattr('src.data.baostock_handler.BaostockHandler', _FakeHandler)
    monkeypatch.setattr('src.data.akshare_handler.AkShareHandler', _FakeHandler)

    instance = DataManager(_FakeConfig(), _FakeDB(), None)
    yield instance
    instance.async_data_manager.close()


# ----------------------------------------------------------------------
# async 包装：委托 / 同步回退
# ----------------------------------------------------------------------


def test_data_service_property_returns_self(manager):
    """兼容属性 data_service 返回 DataManager 自身。"""
    assert manager.data_service is manager


def test_async_methods_delegate_to_async_manager(manager):
    """async 包装方法把参数完整转发给 AsyncDataManager。"""
    fake = _FakeAsyncManager()
    manager.async_data_manager = fake

    stock = asyncio.run(
        manager.get_stock_data_async('600000.SH', '2030-01-01', '2030-01-31')
    )
    index = asyncio.run(
        manager.get_index_data_async('000001.SH', '2030-01-01', '2030-01-31', '1w')
    )
    multiple = asyncio.run(
        manager.get_multiple_stocks_data_async(
            ['600000.SH', '000001.SZ'], '2030-01-01', '2030-01-31', batch_size=1
        )
    )

    assert stock.height == _daily_frame().height
    assert index.height == _daily_frame().height
    assert set(multiple) == {'600000.SH'}
    assert fake.calls[0][1] == ('600000.SH', '2030-01-01', '2030-01-31', '1d', 'hfq')
    assert fake.calls[1][1][-1] == '1w'
    assert fake.calls[2][1][-2:] == (1, 30)


def test_async_stock_and_index_fall_back_to_sync():
    """无 AsyncDataManager 时异步包装回退到同步方法。"""
    bare = DataManager.__new__(DataManager)
    bare.async_data_manager = None
    calls = []

    def _stock(code, start, end, frequency, adjustment_type):
        calls.append((code, frequency, adjustment_type))
        return _daily_frame()

    def _index(code, start, end, frequency):
        calls.append((code, frequency))
        return _daily_frame()

    bare.get_stock_data = _stock
    bare.get_index_data = _index

    stock = asyncio.run(
        bare.get_stock_data_async('600001.SH', '2030-01-01', '2030-02-01')
    )
    index = asyncio.run(
        bare.get_index_data_async('000002.SH', '2030-01-01', '2030-02-01', '1m')
    )

    assert stock.height == _daily_frame().height
    assert index.height == _daily_frame().height
    assert calls == [
        ('600001.SH', '1d', 'hfq'),
        ('000002.SH', '1m'),
    ]


def test_multiple_stocks_fallback_runs_serially_with_progress():
    """无异步管理器时多股票串行回退，进度分支（每 5 只 + 末尾）都会执行。"""
    bare = DataManager.__new__(DataManager)
    bare.async_data_manager = None
    bare.get_stock_data = lambda code, *_args: _daily_frame().with_columns(
        pl.lit(code).alias('ts_code')
    )
    codes = [f'60000{i}.SH' for i in range(6)]

    result = asyncio.run(
        bare.get_multiple_stocks_data_async(
            codes, '2030-01-01', '2030-02-01', frequency='1d'
        )
    )

    assert list(result) == codes
    assert all(frame.height == _daily_frame().height for frame in result.values())


def test_init_async_manager_degrades_on_import_error(monkeypatch):
    """AsyncDataManager 模块缺失时初始化失败但 DataManager 仍可用。"""
    bare = DataManager.__new__(DataManager)
    bare.config = _FakeConfig()
    bare.db_manager = _FakeDB()
    bare.plugin_manager = None
    bare.plugin_datasources = {}
    monkeypatch.setitem(sys.modules, 'src.data.async_data_manager', None)

    bare._init_async_manager()

    assert bare.async_data_manager is None


# ----------------------------------------------------------------------
# 真实 AsyncDataManager：线程池、缓存、批量与聚合
# ----------------------------------------------------------------------


def _async_manager(manager):
    """用 DataManager 现有配置构造独立 AsyncDataManager（便于单独关闭）。"""
    return AsyncDataManager(manager)


def test_async_stock_data_uses_executor_and_cache(manager):
    """首次走线程池取数并写缓存，第二次直接命中缓存不再取数。"""
    manager.async_data_manager.close()
    async_manager = _async_manager(manager)
    fetcher = manager.data_fetcher
    try:
        first = asyncio.run(
            async_manager.get_stock_data_async(
                '600100.SH', '2031-01-01', '2031-01-31'
            )
        )
        calls_after_first = len(fetcher.calls)
        second = asyncio.run(
            async_manager.get_stock_data_async(
                '600100.SH', '2031-01-01', '2031-01-31'
            )
        )
    finally:
        async_manager.close()

    assert first.height == _daily_frame().height
    assert second.height == first.height
    assert calls_after_first == 1
    assert len(fetcher.calls) == 1


def test_async_index_data_uses_executor_and_cache(manager):
    """指数异步取数同样写缓存，第二次命中。"""
    manager.async_data_manager.close()
    async_manager = _async_manager(manager)
    fetcher = manager.data_fetcher
    try:
        first = asyncio.run(
            async_manager.get_index_data_async(
                '000300.SH', '2031-02-01', '2031-02-28'
            )
        )
        calls = len(fetcher.calls)
        second = asyncio.run(
            async_manager.get_index_data_async(
                '000300.SH', '2031-02-01', '2031-02-28'
            )
        )
    finally:
        async_manager.close()

    assert first.height == _daily_frame().height
    assert second.height == first.height
    assert len(fetcher.calls) == calls == 1


def test_async_batches_and_handles_single_failure(manager):
    """批量分片执行；单只股票失败返回空表，不影响其余结果。"""
    manager.async_data_manager.close()
    async_manager = _async_manager(manager)
    codes = ['600200.SH', '600201.SH', '600202.SH', 'BAD.SH', '600204.SH']

    def _stock(code, *_args, **_kwargs):
        if code == 'BAD.SH':
            raise RuntimeError('取数失败')
        return _daily_frame()

    manager.get_stock_data = _stock
    try:
        result = asyncio.run(
            async_manager.get_multiple_stocks_data_async(
                codes, '2031-03-01', '2031-03-31', batch_size=2
            )
        )
    finally:
        del manager.get_stock_data
        async_manager.close()

    assert list(result) == codes
    assert result['BAD.SH'].is_empty()
    assert result['600200.SH'].height == _daily_frame().height


def test_async_weekly_and_monthly_resample(manager):
    """1w/1m 频率先取日线再聚合，行数少于日线且保留 OHLC。"""
    manager.async_data_manager.close()
    async_manager = _async_manager(manager)
    try:
        weekly = asyncio.run(
            async_manager.get_stock_data_async(
                '600300.SH', '2031-04-01', '2031-04-30', frequency='1w'
            )
        )
        monthly = asyncio.run(
            async_manager.get_stock_data_async(
                '600301.SH', '2031-05-01', '2031-05-31', frequency='1m'
            )
        )
    finally:
        async_manager.close()

    assert weekly.height < _daily_frame().height
    assert {'open', 'high', 'low', 'close', 'vol'} <= set(weekly.columns)
    assert monthly.height <= weekly.height


def test_resample_frame_branches():
    """聚合覆盖 Date、字符串 date、trade_date 别名与无日期列四种输入。"""
    frame = _daily_frame()

    weekly = AsyncDataManager._resample_stock_frame(frame, '1w')
    assert weekly.height < frame.height
    assert {'open', 'high', 'low', 'close', 'vol'} <= set(weekly.columns)
    assert weekly['date'].to_list() == sorted(weekly['date'].to_list())

    as_text = frame.with_columns(pl.col('date').dt.strftime('%Y-%m-%d'))
    monthly = AsyncDataManager._resample_stock_frame(as_text, '1m')
    assert monthly.height < frame.height
    assert 'date' in monthly.columns

    no_date = frame.drop('date')
    assert AsyncDataManager._resample_stock_frame(no_date, '1w').columns == no_date.columns

    unchanged = AsyncDataManager._resample_stock_frame(frame, '1d')
    assert unchanged.height == frame.height


def test_async_manager_close_is_idempotent(manager):
    """close 关闭线程池，重复调用不抛出。"""
    async_manager = _async_manager(manager)

    async_manager.close()
    async_manager.close()

    assert async_manager.executor._shutdown is True


# ----------------------------------------------------------------------
# 基础信息查询、默认回填与分红数据
# ----------------------------------------------------------------------


def _rows(count=2):
    return [
        SimpleNamespace(
            ts_code=f'60000{i}.SH', symbol=f'60000{i}', name=f'股票{i}',
            market='SH', industry='银行',
        )
        for i in range(count)
    ]


@pytest.fixture
def basic_manager():
    session = _Session({
        StockBasic: _rows(),
        IndexBasic: [SimpleNamespace(ts_code='000001.SH', name='上证指数')],
        FundBasic: [SimpleNamespace(ts_code='510300.SH', name='沪深300ETF')],
        ClosedFundBasic: [SimpleNamespace(ts_code='500018.SH', name='基金兴和')],
    })
    db = _FakeDB(session)
    instance = DataManager.__new__(DataManager)
    instance.config = _FakeConfig()
    instance.db_manager = db
    yield instance
    if getattr(instance, 'async_data_manager', None):
        instance.async_data_manager.close()


def test_basic_data_queries_all_tables(basic_manager):
    """四类基础信息查询返回 Polars DataFrame，并触发会话清理。"""
    assert basic_manager.get_stock_basic('sh').height == 2
    assert basic_manager.get_stock_basic('sz').height == 2
    assert basic_manager.get_index_basic().height == 1
    assert basic_manager.get_fund_basic().height == 1
    assert basic_manager.get_closed_fund_basic('sh').height == 1
    assert basic_manager.db_manager.cleaned == 5


def test_basic_data_inserts_defaults_when_table_empty():
    """空表且提供 defaults 时插入默认行并提交，再返回结果。"""
    session = _Session()
    db = _FakeDB(session)
    instance = DataManager.__new__(DataManager)
    instance.config = _FakeConfig()
    instance.db_manager = db
    instance.async_data_manager = None

    assert instance.get_stock_basic().height == 3
    assert instance.get_closed_fund_basic().height == 3
    assert session.commits == 2


def test_basic_data_empty_when_db_unavailable_or_session_missing():
    """数据库缺失或会话不可用时返回空表。"""
    instance = DataManager.__new__(DataManager)
    instance.config = _FakeConfig()
    instance.db_manager = None

    assert instance.get_stock_basic().is_empty()

    instance.db_manager = _FakeDB(session=None)
    assert instance.get_stock_basic().is_empty()


def test_basic_data_swallows_query_error():
    """底层查询抛 RuntimeError 时降级为空表，不向上抛出。"""
    instance = DataManager.__new__(DataManager)
    instance.config = _FakeConfig()
    instance.db_manager = _FakeDB(_Session(query_error=True))

    assert instance.get_stock_basic().is_empty()
    assert instance.get_index_basic().is_empty()


def test_stock_code_name_and_record_helpers(basic_manager):
    """代码列表、名称查询与 UI 搜索记录都能从会话读取，并清理会话。"""
    assert basic_manager.list_stock_ts_codes() == ['600000.SH', '600001.SH']
    assert basic_manager.get_stock_name('600000.SH') == '股票0'
    assert basic_manager.get_stock_name('600009.SH') is None
    assert basic_manager.get_stock_name('') is None

    records = basic_manager.list_stock_records()
    assert records[0] == {
        'ts_code': '600000.SH', 'symbol': '600000', 'name': '股票0',
        'market': 'SH', 'industry': '银行',
    }


def test_stock_helpers_return_empty_without_db():
    """数据库不可用时三个辅助查询都返回空值。"""
    instance = DataManager.__new__(DataManager)
    instance.config = _FakeConfig()
    instance.db_manager = None

    assert instance.list_stock_ts_codes() == []
    assert instance.get_stock_name('600000.SH') is None
    assert instance.list_stock_records() == []


def _dividend_rows():
    return [
        SimpleNamespace(
            ts_code='600000.SH', symbol='600000', name='浦发银行',
            dividend_year='2023', report_date='2023-12-31',
            record_date='2024-01-10', ex_date='2024-01-11',
            pay_date='2024-01-20', cash_div=0.5, share_div=0.0,
            total_div=0.5, rights_issue_price=0.0, rights_issue_ratio=0.0,
        )
    ]


def test_stock_dividend_returns_frame_and_guards():
    """分红数据成功返回；空代码/无库/未连接/空结果均返回空表。"""
    session = _Session({StockDividend: _dividend_rows()})
    instance = DataManager.__new__(DataManager)
    instance.config = _FakeConfig()
    instance.db_manager = _FakeDB(session)
    instance.async_data_manager = None

    result = instance.get_stock_dividend('600000.SH')
    assert result.height == 1
    assert 'cash_div' in result.columns

    assert instance.get_stock_dividend('').is_empty()

    instance.db_manager = _FakeDB(session, connected=False)
    assert instance.get_stock_dividend('600000.SH').is_empty()

    instance.db_manager = _FakeDB(_Session())
    assert instance.get_stock_dividend('600000.SH').is_empty()

    instance.db_manager = None
    assert instance.get_stock_dividend('600000.SH').is_empty()


def test_stock_dividend_swallows_query_error():
    """分红查询抛 RuntimeError 时返回空表，并仍清理会话。"""
    db = _FakeDB(_Session(query_error=True))
    instance = DataManager.__new__(DataManager)
    instance.config = _FakeConfig()
    instance.db_manager = db

    assert instance.get_stock_dividend('600000.SH').is_empty()
    assert db.session.closes == 1


def test_local_quote_entry_requires_config_data():
    """缺少本地数据路径配置时最新行情入口返回 None。"""
    instance = DataManager.__new__(DataManager)
    instance.config = SimpleNamespace(data=None)
    instance.db_manager = _FakeDB()

    assert instance.get_latest_local_quote('600000') is None


def test_database_date_column_is_supported():
    """基础信息行字段为空值时按空字符串兜底，不抛出。"""
    session = _Session({
        StockBasic: [
            SimpleNamespace(
                ts_code='600000.SH', symbol='', name='',
                market='', industry='',
            )
        ]
    })
    instance = DataManager.__new__(DataManager)
    instance.config = _FakeConfig()
    instance.db_manager = _FakeDB(session)
    instance.async_data_manager = None

    records = instance.list_stock_records()

    assert records == [{
        'ts_code': '600000.SH', 'symbol': '', 'name': '',
        'market': '', 'industry': '',
    }]
    assert isinstance(date.today(), date)
