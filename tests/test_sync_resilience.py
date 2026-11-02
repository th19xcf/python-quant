#!/usr/bin/env python3

"""
同步业务线韧性用例：幂等、回滚与降级。

P1-9：sync_stock / sync_index / sync_fund 的覆盖率此前分别只有 34% / 32% / 12%，
缺口集中在三条语义：

1. 幂等 —— 同一份输入重复执行不得重复写入，也不得把已有记录反复计为更新；
2. 回滚 —— 提交失败必须 rollback 并把本批计入失败清单，不残留半成品；
3. 降级 —— 外部数据源全部失败时必须终止（或用空映射降级），绝不写占位数据。

本文件用最小会话/数据源替身覆盖这三类语义，不连接真实数据库与网络。
"""

import sys
import types
from datetime import date
from types import SimpleNamespace

import pandas as pd
import polars as pl
import pytest

from src.data.services import sync_fund, sync_index, sync_stock
from src.data.services.sync_index import IndexSyncContext
from src.data.services.sync_stock import StockBatchContext
from src.database.models.fund import FundBasic
from src.database.models.index import IndexBasic
from src.database.models.stock import StockBasic
from src.utils.exceptions import DataSaveError, DataSourceException

# ----------------------------------------------------------------------
# 最小替身
# ----------------------------------------------------------------------

class _FakeQuery:
    """查询替身：忽略过滤条件，直接返回内存表中的行。"""

    def __init__(self, rows, scalar_value=None):
        self._rows = rows
        self._scalar_value = scalar_value

    def filter(self, *_args, **_kwargs):
        return self

    def all(self):
        return list(self._rows)

    def scalar(self):
        return self._scalar_value


class _FakeSession:
    """按模型类持有内存行的最小会话替身。"""

    def __init__(self, stock_rows=None, fund_rows=None, index_rows=None,
                 scalar_value=None):
        self._tables = {
            StockBasic: list(stock_rows or []),
            FundBasic: list(fund_rows or []),
            IndexBasic: list(index_rows or []),
        }
        self._scalar_value = scalar_value
        self.commits = 0
        self.rollbacks = 0
        self.active = True

    @property
    def is_active(self):
        return self.active

    def query(self, model):
        return _FakeQuery(self._tables.get(model, []), self._scalar_value)

    def add(self, obj):
        self._tables[type(obj)].append(obj)

    def add_all(self, objs):
        for obj in objs:
            self.add(obj)

    def delete(self, obj):
        self._tables[type(obj)].remove(obj)

    def rollback(self):
        self.rollbacks += 1
        self.active = True

    def commit(self):
        if not self.active:
            raise RuntimeError('会话处于 requires-rollback 状态')
        self.commits += 1


def _index_result():
    return {
        'total_tdx_indexes': 0, 'updated_indexes': 0, 'inserted_indexes': 0,
        'deleted_indexes': 0, 'unchanged_indexes': 0, 'fallback_named_indexes': 0,
        'filled_blank_fields': 0, 'failed_indexes': [],
    }


# ----------------------------------------------------------------------
# sync_stock：批量写入回滚 + 新增幂等 + 外部源全失败终止
# ----------------------------------------------------------------------

def test_flush_stock_batch_rolls_back_and_counts_failed():
    """批量提交失败必须回滚，并把本批全部计入失败清单。"""
    session = _FakeSession()

    def _failing_commit():
        session.active = False
        raise RuntimeError('批量提交失败')

    session.commit = _failing_commit
    result = {'inserted_stocks': 2, 'failed_stocks': []}
    pending = [
        ('600000.SH', SimpleNamespace(ts_code='600000.SH')),
        ('000001.SZ', SimpleNamespace(ts_code='000001.SZ')),
    ]

    sync_stock._flush_stock_batch(session, pending, result)

    assert session.rollbacks == 1
    assert result['inserted_stocks'] == 0
    assert result['failed_stocks'] == ['600000.SH', '000001.SZ']


def test_stock_insert_is_idempotent_across_reruns(monkeypatch):
    """同一批新股票重复处理：已存在的代码跳过，不重复写入 stock_basic。"""
    session = _FakeSession()
    merged_basic = {
        '600000.SH': {'name': '浦发银行', 'symbol': '600000'},
        '000001.SZ': {'name': '平安银行', 'symbol': '000001'},
    }

    def _stock_info(ts_code, _merged, _baostock, _akshare):
        return dict(merged_basic[ts_code], ts_code=ts_code)

    monkeypatch.setattr(sync_stock, 'get_stock_info_from_apis', _stock_info)

    result = {'inserted_stocks': 0, 'failed_stocks': []}
    existing_codes = {'000001.SZ'}
    ctx = StockBatchContext(
        ['600000.SH', '000001.SZ'], merged_basic, existing_codes,
        session, None, None, result,
    )
    sync_stock._process_single_stock_batch(ctx)

    assert result['inserted_stocks'] == 1
    assert [row.ts_code for row in session.query(StockBasic).all()] == ['600000.SH']

    # 第二次执行：existing_codes 已含两只，新增计数不再增长、无重复行
    rerun_result = {'inserted_stocks': 0, 'failed_stocks': []}
    ctx = StockBatchContext(
        ['600000.SH', '000001.SZ'], merged_basic, ctx.existing_codes,
        session, None, None, rerun_result,
    )
    sync_stock._process_single_stock_batch(ctx)

    assert rerun_result['inserted_stocks'] == 0
    assert len(session.query(StockBasic).all()) == 1


def test_prepare_stock_sync_terminates_when_all_external_sources_fail(monkeypatch):
    """Baostock/AkShare 均不可用时必须终止，不写占位数据。"""
    class _FakeTdxHandler:
        def __init__(self, _config, _db_manager):
            pass

        def get_stock_list(self):
            return ['600000.SH', '000001.SZ', '510300.SH']

    monkeypatch.setattr('src.data.tdx_handler.TdxHandler', _FakeTdxHandler)
    monkeypatch.setattr('src.data.baostock_handler.BaostockHandler', lambda *a: None)
    monkeypatch.setattr('src.data.akshare_handler.AkShareHandler', lambda *a: None)
    monkeypatch.setattr(
        sync_stock, '_fetch_external_stock_basic', lambda *a: (None, None, {})
    )

    result = {
        'total_tdx_stocks': 0, 'existing_stocks': 0, 'new_stocks': 0,
        'deleted_non_stock_rows': 0, 'failed_stocks': [],
        'inserted_stocks': 0, 'updated_stocks': 0,
        'filled_blank_fields': 0, 'refreshed_fields': 0,
    }
    ctx = sync_stock._prepare_stock_sync(
        SimpleNamespace(), None, _FakeSession(), result
    )

    assert ctx is None
    # 通达信代码已过滤掉基金（510300.SH），个股计数正确
    assert result['total_tdx_stocks'] == 2


# ----------------------------------------------------------------------
# sync_index：重复同步幂等 + 清理过期记录 + 外部源全失败终止
# ----------------------------------------------------------------------

def test_index_sync_is_idempotent_and_prunes_stale_rows():
    """重复同步同一份指数：第二次全部 unchanged，且过期记录被清理。"""
    stale = IndexBasic(ts_code='999999.SH', name='旧指数', market='SH')
    session = _FakeSession(index_rows=[stale])
    external_map = {
        '000001.SH': {
            'name': '上证指数',
            'list_date': date(1991, 7, 15),
            'base_date': date(1990, 12, 19),
        },
    }

    first = _index_result()
    ctx = IndexSyncContext(
        ['000001.SH'], {'000001.SH'}, external_map,
        {row.ts_code: row for row in session.query(IndexBasic).all()},
    )
    sync_index._run_index_sync(ctx, session, first)

    assert first['inserted_indexes'] == 1
    assert first['deleted_indexes'] == 1
    assert first['failed_indexes'] == []
    assert [row.ts_code for row in session.query(IndexBasic).all()] == ['000001.SH']
    assert session.query(IndexBasic).all()[0].name == '上证指数'

    second = _index_result()
    ctx = IndexSyncContext(
        ['000001.SH'], {'000001.SH'}, external_map,
        {row.ts_code: row for row in session.query(IndexBasic).all()},
    )
    sync_index._run_index_sync(ctx, session, second)

    assert second['inserted_indexes'] == 0
    assert second['updated_indexes'] == 0
    assert second['deleted_indexes'] == 0
    assert second['unchanged_indexes'] == 1
    assert second['filled_blank_fields'] == 0


def test_prepare_index_sync_terminates_without_external_info(monkeypatch):
    """外部指数信息全为空时必须终止，避免写入 "指数XXXXXX" 占位名。"""
    class _FakeTdxHandler:
        def __init__(self, _config, _db_manager):
            pass

        def get_index_list(self):
            return ['000001.SH', '399001.SZ', '600000.SH']

    monkeypatch.setattr('src.data.tdx_handler.TdxHandler', _FakeTdxHandler)
    monkeypatch.setattr(sync_index, '_collect_external_index_name_map', lambda _c: {})

    result = _index_result()
    ctx = sync_index._prepare_index_sync(
        SimpleNamespace(), None, _FakeSession(), result
    )

    assert ctx is None
    assert result['total_tdx_indexes'] == 2


def test_prepare_index_sync_terminates_without_tdx_index_codes(monkeypatch):
    """通达信数据源没有指数代码时直接跳过同步。"""
    class _FakeTdxHandler:
        def __init__(self, _config, _db_manager):
            pass

        def get_index_list(self):
            return ['600000.SH', '000001.SZ']

    monkeypatch.setattr('src.data.tdx_handler.TdxHandler', _FakeTdxHandler)

    result = _index_result()
    ctx = sync_index._prepare_index_sync(
        SimpleNamespace(), None, _FakeSession(), result
    )

    assert ctx is None
    assert result['total_tdx_indexes'] == 0


# ----------------------------------------------------------------------
# sync_fund：迁移幂等 + 提交失败回滚 + AkShare 降级
# ----------------------------------------------------------------------

def test_fund_migration_is_idempotent():
    """同一基金记录迁移两次：只新增一次，第二次不再计为更新。"""
    stock = StockBasic(
        ts_code='510300.SH', symbol='510300', name='沪深300ETF', market='SH',
    )
    session = _FakeSession()
    existing_fund_map = {}
    result = {'inserted_funds': 0, 'updated_funds': 0}

    sync_fund._migrate_fund_stock_row(stock, existing_fund_map, session, result)
    assert result['inserted_funds'] == 1
    assert len(session.query(FundBasic).all()) == 1

    sync_fund._migrate_fund_stock_row(stock, existing_fund_map, session, result)
    assert result['inserted_funds'] == 1
    assert result['updated_funds'] == 0
    assert len(session.query(FundBasic).all()) == 1


def test_etf_enrichment_does_not_overwrite_existing_fields():
    """AkShare ETF 补全只填空字段，不覆盖已有名称/类型。"""
    session = _FakeSession()
    existing = FundBasic(
        ts_code='510300.SH', symbol='510300', name='沪深300ETF',
        fund_type='ETF', market='SH', status='L',
    )
    session.add(existing)
    existing_fund_map = {'510300.SH': existing}
    result = {'inserted_funds': 0, 'updated_funds': 0, 'akshare_enriched': 0}

    sync_fund._merge_etf_row(
        {'基金代码': '510300', '基金简称': '沪深300ETF'},
        existing_fund_map, session, result,
    )

    assert result == {'inserted_funds': 0, 'updated_funds': 0, 'akshare_enriched': 0}
    assert existing.name == '沪深300ETF'


def test_fund_date_backfill_uses_cache(tmp_path):
    """同一基金的日期回填只读取一次通达信文件，且退市基金补 delist_date。"""
    config = SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))
    fund = SimpleNamespace(
        ts_code='510300.SH', status='D', list_date=None, delist_date=None,
    )
    result = {'list_date_filled': 0, 'delist_date_filled': 0}
    cache = {fund.ts_code: (date(2012, 5, 28), date(2020, 1, 2))}

    assert sync_fund._fill_fund_date_row(fund, config, cache, result) is True
    assert fund.list_date == date(2012, 5, 28)
    assert fund.delist_date == date(2020, 1, 2)
    assert result == {'list_date_filled': 1, 'delist_date_filled': 1}

    # 已填满：不再有改动，也不触发任何文件读取
    assert sync_fund._fill_fund_date_row(fund, config, cache, result) is False


def test_enrich_funds_degrades_when_akshare_unavailable(monkeypatch):
    """AkShare 不可用时只告警降级，不抛出、不中断基金迁移。"""
    class _BrokenHandler:
        def __init__(self, _config, _db_manager):
            pass

        def update_etf_basic(self):
            raise DataSourceException('akshare', '接口不可用')

    monkeypatch.setattr('src.data.akshare_handler.AkShareHandler', _BrokenHandler)

    session = _FakeSession()
    result = {'inserted_funds': 0, 'updated_funds': 0, 'akshare_enriched': 0}
    sync_fund._enrich_funds_from_akshare(
        SimpleNamespace(), {}, session, result
    )

    assert result == {'inserted_funds': 0, 'updated_funds': 0, 'akshare_enriched': 0}


def test_fund_migration_rolls_back_on_commit_failure(monkeypatch, tmp_path):
    """最终提交失败必须回滚，并计入失败数，不残留迁移结果。"""
    stock = StockBasic(
        ts_code='510300.SH', symbol='510300', name='沪深300ETF', market='SH',
    )
    session = _FakeSession(stock_rows=[stock])

    def _failing_commit():
        raise DataSaveError('提交失败')

    session.commit = _failing_commit
    monkeypatch.setattr(sync_fund, '_enrich_funds_from_akshare', lambda *a: None)

    config = SimpleNamespace(data=SimpleNamespace(tdx_data_path=str(tmp_path)))
    db_manager = SimpleNamespace(get_session=lambda: session)

    result = sync_fund.sync_stock_fund_to_fund_basic(config, db_manager)

    assert result['failed'] == 1
    assert session.rollbacks == 1


# ----------------------------------------------------------------------
# sync_stock：主流程（外部信息合并 → 行业补全 → 新增/回填 → 提交）
# ----------------------------------------------------------------------

def _stock_result():
    return {
        'total_tdx_stocks': 0, 'existing_stocks': 0, 'new_stocks': 0,
        'deleted_non_stock_rows': 0, 'failed_stocks': [],
        'inserted_stocks': 0, 'updated_stocks': 0,
        'filled_blank_fields': 0, 'refreshed_fields': 0,
    }


def test_fetch_external_stock_basic_degrades_on_baostock_failure(monkeypatch):
    """Baostock 失败时降级为 AkShare 数据，行业映射为空。"""

    class _BrokenBaostock:
        def update_stock_basic(self):
            raise DataSourceException('baostock', '登录失败')

        def _ensure_baostock_login(self):
            return False

    akshare_frame = pl.DataFrame(
        {'code': ['600000'], 'code_name': ['浦发银行']}
    )

    class _OkAkshare:
        def update_stock_basic(self):
            return akshare_frame

    monkeypatch.setitem(sys.modules, 'baostock', types.ModuleType('baostock'))

    baostock_basic, akshare_basic, industry_map = (
        sync_stock._fetch_external_stock_basic(_BrokenBaostock(), _OkAkshare())
    )

    assert baostock_basic is None
    assert akshare_basic.height == 1
    assert industry_map == {}


def test_fetch_external_stock_basic_degrades_on_akshare_failure(monkeypatch):
    """AkShare 失败时保留 Baostock 数据，不抛出。"""
    baostock_frame = pl.DataFrame({'code': ['sh.600000']})

    class _OkBaostock:
        def update_stock_basic(self):
            return baostock_frame

        def _ensure_baostock_login(self):
            return False

    class _BrokenAkshare:
        def update_stock_basic(self):
            raise DataSourceException('akshare', '接口失败')

    monkeypatch.setitem(sys.modules, 'baostock', types.ModuleType('baostock'))

    baostock_basic, akshare_basic, industry_map = (
        sync_stock._fetch_external_stock_basic(_OkBaostock(), _BrokenAkshare())
    )

    assert baostock_basic.height == 1
    assert akshare_basic is None
    assert industry_map == {}


class _FakeResultSet:
    """最小化的 baostock ResultData：只实现 next()/get_row_data() 协议。

    故意不提供 get_data()：pandas 2.0 起已移除 DataFrame.append，baostock 的
    get_data() 在多页结果上必然 AttributeError，生产代码必须走逐行拉取。
    """

    error_code = '0'

    def __init__(self, frame: pd.DataFrame):
        self.fields = list(frame.columns)
        self._rows = frame.astype(object).values.tolist()
        self._cursor = 0

    def next(self):
        if self._cursor >= len(self._rows):
            return False
        self._cursor += 1
        return True

    def get_row_data(self):
        return self._rows[self._cursor - 1]


def test_fetch_baostock_industry_map_filters_index_and_fund(monkeypatch):
    """行业映射只保留个股，指数/基金/空行业均被过滤。"""
    raw = pd.DataFrame({
        'code': ['sh.600000', 'sh.000001', 'sz.000001', 'sh.510300',
                 'sh.688001', 'sh.600009'],
        'industry': ['银行', '指数', '银行', 'ETF', '科技', ''],
    })

    fake_baostock = types.ModuleType('baostock')
    fake_baostock.query_stock_industry = lambda: _FakeResultSet(raw)
    monkeypatch.setitem(sys.modules, 'baostock', fake_baostock)

    class _Handler:
        def _ensure_baostock_login(self):
            return True

    mapping = sync_stock._fetch_baostock_industry_map(_Handler())

    assert mapping == {
        '600000.SH': '银行',
        '000001.SZ': '银行',
        '688001.SH': '科技',
    }


def test_fetch_baostock_industry_map_returns_empty_on_login_or_empty_data(
    monkeypatch,
):
    """未登录或接口返回空表时返回空映射。"""

    class _NotLoggedIn:
        def _ensure_baostock_login(self):
            return False

    fake_baostock = types.ModuleType('baostock')
    monkeypatch.setitem(sys.modules, 'baostock', fake_baostock)

    assert sync_stock._fetch_baostock_industry_map(_NotLoggedIn()) == {}

    empty = pd.DataFrame({'code': [], 'industry': []})
    fake_baostock.query_stock_industry = lambda: _FakeResultSet(empty)

    class _LoggedIn:
        def _ensure_baostock_login(self):
            return True

    assert sync_stock._fetch_baostock_industry_map(_LoggedIn()) == {}


def test_collect_akshare_board_names_handles_missing_input():
    """板块列表缺失、空表或缺名称列时返回 None；正常时去空去重。"""
    assert sync_stock._collect_akshare_board_names(None) is None
    assert sync_stock._collect_akshare_board_names(pd.DataFrame()) is None
    assert sync_stock._collect_akshare_board_names(pd.DataFrame({'foo': [1]})) is None

    names = sync_stock._collect_akshare_board_names(
        pd.DataFrame({'板块名称': ['银行', '', '']})
    )
    assert names == ['银行']


def test_fetch_akshare_industry_map_skips_broken_boards(monkeypatch):
    """逐板块取成分：空板块/缺代码列/接口异常均跳过，其余正常入映射。"""
    boards = pd.DataFrame({'板块名称': ['银行', '半导体', '空板块', '无代码列']})
    constituents = {
        '银行': pd.DataFrame({'代码': ['600000', '000001', '000001.SH']}),
        '空板块': pd.DataFrame(),
        '无代码列': pd.DataFrame({'名称': ['x']}),
    }

    def _cons(symbol):
        if symbol == '半导体':
            raise DataSourceException('akshare', '板块接口失败')
        return constituents[symbol]

    monkeypatch.setattr('akshare.stock_board_industry_name_em', lambda: boards)
    monkeypatch.setattr('akshare.stock_board_industry_cons_em', _cons)

    mapping = sync_stock._fetch_akshare_industry_map()

    assert mapping == {'600000.SH': '银行', '000001.SZ': '银行'}


def test_fetch_akshare_industry_map_degrades_on_list_failure(monkeypatch):
    """板块列表接口异常时降级为空映射。"""
    def _boom():
        raise DataSourceException('akshare', '列表接口失败')

    monkeypatch.setattr('akshare.stock_board_industry_name_em', _boom)

    assert sync_stock._fetch_akshare_industry_map() == {}


def test_get_stock_info_from_apis_handles_bad_date_and_symbol_fallback():
    """命中 ts_code 时解析日期，非法日期置空；未命中按 symbol 回退；均无则 None。"""
    merged = {
        '600000.SH': {'symbol': '600000', 'name': '浦发银行', 'list_date': '不是日期'},
    }
    info = sync_stock.get_stock_info_from_apis('600000.SH', merged, None, None)
    assert info['ts_code'] == '600000.SH'
    assert info['list_date'] is None

    by_symbol = {'sh.600000': {'symbol': '600000', 'name': '浦发银行'}}
    fallback = sync_stock.get_stock_info_from_apis(
        '600000.SH', by_symbol, None, None
    )
    assert fallback['ts_code'] == '600000.SH'
    assert fallback['name'] == '浦发银行'

    assert sync_stock.get_stock_info_from_apis('600001.SH', {}, None, None) is None


def test_merge_stock_basic_merges_sources_and_skips_existing():
    """Baostock 先入、AkShare 补全字段；已存在代码不进入合并结果。"""
    baostock_df = pl.DataFrame({
        'code': ['sh.600000', 'sz.000001', 'sz.000002'],
        'code_name': ['浦发银行', '平安银行', '万科A'],
        'industry': ['银行', '银行', '房地产'],
        'ipoDate': ['1999-11-10', '1991-04-03', '1991-01-29'],
        'outDate': ['', '', ''],
    })
    akshare_df = pl.DataFrame({
        '代码': ['600000', '000001', '000002'],
        '名称': ['浦发银行', '平安银行', '万科A'],
        '所属行业': ['银行', '银行', '房地产'],
    })

    merged = sync_stock.merge_stock_basic(
        baostock_df, akshare_df, {'000002.SZ'}
    )

    assert '000002.SZ' not in merged
    assert merged['600000.SH']['name'] == '浦发银行'
    assert merged['000001.SZ']['industry'] == '银行'


def test_prune_non_stock_rows_removes_index_and_fund_rows():
    """清理 stock_basic 中的指数/基金行，并统计存量股票数。"""
    session = _FakeSession(stock_rows=[
        StockBasic(ts_code='600000.SH', name='浦发银行', market='SH'),
        StockBasic(ts_code='000001.SH', name='上证指数', market='SH'),
        StockBasic(ts_code='510300.SH', name='沪深300ETF', market='SH'),
    ])
    result = _stock_result()

    existing_codes, non_stock_codes = sync_stock._prune_non_stock_rows(
        session, result
    )

    assert existing_codes == {'600000.SH'}
    assert non_stock_codes == {'000001.SH', '510300.SH'}
    assert result['deleted_non_stock_rows'] == 2
    assert result['existing_stocks'] == 1
    assert [row.ts_code for row in session.query(StockBasic).all()] == ['600000.SH']


def test_apply_baostock_industry_only_fills_missing_or_unknown():
    """行业映射仅在缺失或占位值时覆盖，已有有效行业不被改写。"""
    merged = {
        '600000.SH': {'industry': ''},
        '000001.SZ': {'industry': '未知'},
        '688001.SH': {'industry': '电子'},
    }

    sync_stock._apply_baostock_industry(merged, {})
    sync_stock._apply_baostock_industry(merged, {
        '600000.SH': '银行',
        '000001.SZ': '银行',
        '688001.SH': '半导体',
        '300750.SZ': '电池',
    })

    assert merged['600000.SH']['industry'] == '银行'
    assert merged['000001.SZ']['industry'] == '银行'
    assert merged['688001.SH']['industry'] == '电子'
    assert merged['300750.SZ']['industry'] == '电池'
    assert merged['300750.SZ']['symbol'] == '300750'


def test_akshare_industry_fallback_skipped_when_industries_complete(monkeypatch):
    """所有股票都有行业时不触发 AkShare 兜底调用。"""
    def _should_not_run():
        raise AssertionError('不应触发 AkShare 兜底')

    monkeypatch.setattr(sync_stock, '_fetch_akshare_industry_map', _should_not_run)
    merged = {'600000.SH': {'industry': '银行'}}

    assert sync_stock._apply_akshare_industry_fallback(merged, ['600000.SH']) == 0


def test_akshare_industry_fallback_fills_only_missing_industry(monkeypatch):
    """兜底映射补充缺失行业与缺失标的，已有非空行业保持不变。"""
    monkeypatch.setattr(
        sync_stock,
        '_fetch_akshare_industry_map',
        lambda: {'600000.SH': '银行', '000001.SZ': '银行', '300750.SZ': '电池'},
    )
    merged = {
        '600000.SH': {'industry': ''},
        '000001.SZ': {'industry': '未知'},
    }

    filled = sync_stock._apply_akshare_industry_fallback(
        merged, ['600000.SH', '000001.SZ']
    )

    assert filled == 3
    assert merged['600000.SH']['industry'] == '银行'
    assert merged['000001.SZ']['industry'] == '未知'
    assert merged['300750.SZ']['industry'] == '电池'


def test_backfill_stock_row_fills_blank_fields(monkeypatch):
    """空白行按外部信息与 ts_code 推断填充文本/日期/状态字段。"""
    row = StockBasic(ts_code='600000.SH', symbol='600000')
    session = _FakeSession(stock_rows=[row])
    result = {'filled_blank_fields': 0, 'refreshed_fields': 0}
    monkeypatch.setattr(
        sync_stock, '_extract_tdx_first_last_trade_date',
        lambda _config, _code: (date(1999, 11, 10), date(2024, 1, 1)),
    )

    changed = sync_stock._backfill_stock_row(
        row,
        {
            'name': '浦发银行', 'area': '上海', 'industry': '银行',
            'market': '', 'list_date': '1999-11-10', 'status': 'L',
        },
        session, SimpleNamespace(), result,
    )

    assert changed is True
    assert row.name == '浦发银行'
    assert row.area == '上海'
    assert row.industry == '银行'
    assert row.market == '主板'
    assert row.list_date == date(1999, 11, 10)
    assert row.status == 'L'
    assert result['filled_blank_fields'] == 6


def test_backfill_stock_row_refreshes_changed_fields():
    """已有非空字段变化计为刷新，空退市日期计为回填。"""
    row = StockBasic(
        ts_code='000001.SZ', symbol='000001', name='股票000001',
        area='旧地区', industry='旧行业', market='SZ', status='P',
        list_date=date(1991, 4, 3),
    )
    result = {'filled_blank_fields': 0, 'refreshed_fields': 0}

    changed = sync_stock._backfill_stock_row(
        row,
        {
            'name': '平安银行', 'area': '深圳', 'industry': '银行',
            'market': '主板', 'list_date': '1991-04-03',
            'delist_date': '2020-01-01', 'status': 'D',
        },
        _FakeSession(), SimpleNamespace(), result,
    )

    assert changed is True
    assert row.name == '平安银行'
    assert row.area == '深圳'
    assert row.industry == '银行'
    assert row.market == '主板'
    assert row.status == 'D'
    assert row.list_date == date(1991, 4, 3)
    assert row.delist_date == date(2020, 1, 1)
    assert result['filled_blank_fields'] == 1
    assert result['refreshed_fields'] == 5


def test_backfill_list_date_falls_back_to_stock_daily(monkeypatch):
    """外部与通达信均无日期时，回退到 stock_daily 最早交易日。"""
    row = StockBasic(ts_code='600000.SH', symbol='600000')
    session = _FakeSession(stock_rows=[row], scalar_value=date(2010, 1, 4))
    result = {'filled_blank_fields': 0, 'refreshed_fields': 0}
    monkeypatch.setattr(
        sync_stock, '_extract_tdx_first_last_trade_date', lambda _c, _code: (None, None)
    )

    changed = sync_stock._backfill_list_date(
        row, {'list_date': None}, session, SimpleNamespace(), result
    )

    assert changed is True
    assert row.list_date == date(2010, 1, 4)
    assert result['filled_blank_fields'] == 1


def test_backfill_list_date_swallows_stock_daily_query_errors(monkeypatch):
    """stock_daily 查询失败只降级为无改动，不向上抛出。"""
    class _BrokenSession:
        def query(self, *_args, **_kwargs):
            raise RuntimeError('查询失败')

    row = StockBasic(ts_code='600000.SH', symbol='600000')
    result = {'filled_blank_fields': 0, 'refreshed_fields': 0}
    monkeypatch.setattr(
        sync_stock, '_extract_tdx_first_last_trade_date', lambda _c, _code: (None, None)
    )

    changed = sync_stock._backfill_list_date(
        row, {'list_date': None}, _BrokenSession(), SimpleNamespace(), result
    )

    assert changed is False
    assert row.list_date is None


def test_backfill_status_fills_refreshes_and_keeps():
    """状态空值回填、变化刷新、相同值无改动。"""
    row = StockBasic(ts_code='600000.SH', status=None)
    result = {'filled_blank_fields': 0, 'refreshed_fields': 0}

    assert sync_stock._backfill_status(row, {'status': 'L'}, result) is True
    assert row.status == 'L'
    assert result['filled_blank_fields'] == 1

    assert sync_stock._backfill_status(row, {'status': 'D'}, result) is True
    assert row.status == 'D'
    assert result['refreshed_fields'] == 1

    assert sync_stock._backfill_status(row, {'status': 'D'}, result) is False


def test_finalize_database_commit_rolls_back_on_failure():
    """最终提交失败回滚；成功时正常提交。"""
    session = _FakeSession()

    def _failing_commit():
        raise RuntimeError('提交失败')

    session.commit = _failing_commit
    result = {
        'inserted_stocks': 0, 'updated_stocks': 0,
        'filled_blank_fields': 0, 'refreshed_fields': 0,
    }

    sync_stock._finalize_database_commit(session, result)
    assert session.rollbacks == 1

    session.active = True
    session.commit = lambda: None
    sync_stock._finalize_database_commit(session, result)
    assert session.commits == 0


def test_log_sync_result_reports_failed_stocks():
    """统计日志覆盖有失败清单的分支。"""
    result = _stock_result()
    result['failed_stocks'] = ['600000.SH', '000001.SZ']

    sync_stock._log_sync_result(result)  # 不应抛出


def test_prepare_stock_sync_returns_none_without_tdx_stocks(monkeypatch):
    """通达信只有指数/基金时终止同步。"""
    class _IndexOnlyTdx:
        def __init__(self, _config, _db_manager):
            pass

        def get_stock_list(self):
            return ['000001.SH', '510300.SH']

    monkeypatch.setattr('src.data.tdx_handler.TdxHandler', _IndexOnlyTdx)
    result = _stock_result()

    assert sync_stock._prepare_stock_sync(
        SimpleNamespace(), None, _FakeSession(), result
    ) is None
    assert result['total_tdx_stocks'] == 0


def test_prepare_stock_sync_builds_merged_context(monkeypatch):
    """准备阶段完成代码过滤、非股票清理与多源基本信息合并。"""
    class _Tdx:
        def __init__(self, _config, _db_manager):
            pass

        def get_stock_list(self):
            return ['600000.SH', '000001.SZ', '510300.SH']

    monkeypatch.setattr('src.data.tdx_handler.TdxHandler', _Tdx)
    monkeypatch.setattr(
        'src.data.baostock_handler.BaostockHandler', lambda *_a: 'baostock'
    )
    monkeypatch.setattr(
        'src.data.akshare_handler.AkShareHandler', lambda *_a: 'akshare'
    )

    baostock_df = pl.DataFrame({
        'code': ['sh.600000'], 'code_name': ['浦发银行'],
        'industry': ['银行'], 'ipoDate': ['1999-11-10'],
    })
    akshare_df = pl.DataFrame({
        '代码': ['000001'], '名称': ['平安银行'], '所属行业': ['银行'],
    })
    monkeypatch.setattr(
        sync_stock, '_fetch_external_stock_basic',
        lambda *_a: (baostock_df, akshare_df, {}),
    )
    monkeypatch.setattr(sync_stock, '_fetch_akshare_industry_map', lambda: {})

    session = _FakeSession(
        stock_rows=[StockBasic(ts_code='600000.SH', name='浦发银行')]
    )
    result = _stock_result()

    ctx = sync_stock._prepare_stock_sync(
        SimpleNamespace(), None, session, result
    )

    assert ctx is not None
    assert ctx.existing_codes == {'600000.SH'}
    assert ctx.new_codes == ['000001.SZ']
    assert set(ctx.merged_basic) >= {'600000.SH', '000001.SZ'}
    assert result['total_tdx_stocks'] == 2


def test_run_stock_sync_inserts_new_and_backfills_existing():
    """执行阶段：新增缺失股票，同时回填存量股票空白字段并最终提交。"""
    existing_row = StockBasic(
        ts_code='600000.SH', symbol='600000', name='股票600000', status=None,
    )
    session = _FakeSession(stock_rows=[existing_row])
    merged = {
        '600000.SH': {
            'symbol': '600000', 'name': '浦发银行', 'industry': '银行',
            'area': '上海', 'market': '主板', 'list_date': '1999-11-10',
            'status': 'L',
        },
        '000001.SZ': {
            'symbol': '000001', 'name': '平安银行', 'industry': '银行',
            'area': '深圳', 'market': '主板', 'list_date': '1991-04-03',
            'status': 'L',
        },
    }
    result = _stock_result()
    ctx = sync_stock.StockSyncContext(
        config=SimpleNamespace(), db_manager=None, result=result,
        session=session, baostock_handler=None, akshare_handler=None,
        existing_codes={'600000.SH'}, new_codes=['000001.SZ'],
        tdx_stock_codes=['600000.SH', '000001.SZ'], merged_basic=merged,
    )

    sync_stock._run_stock_sync(ctx, session, SimpleNamespace(), result)

    assert result['inserted_stocks'] == 1
    assert result['updated_stocks'] == 1
    assert session.commits >= 2
    assert {row.ts_code for row in session.query(StockBasic).all()} == {
        '600000.SH', '000001.SZ',
    }


def test_sync_tdx_stock_to_database_runs_full_flow(monkeypatch):
    """端到端编排：准备 → 新增 → 回填 → 提交，全部走流程骨架。"""
    class _Tdx:
        def __init__(self, _config, _db_manager):
            pass

        def get_stock_list(self):
            return ['600000.SH', '000001.SZ']

    monkeypatch.setattr('src.data.tdx_handler.TdxHandler', _Tdx)
    monkeypatch.setattr(
        'src.data.baostock_handler.BaostockHandler', lambda *_a: object()
    )
    monkeypatch.setattr(
        'src.data.akshare_handler.AkShareHandler', lambda *_a: object()
    )

    baostock_df = pl.DataFrame({
        'code': ['sh.600000', 'sz.000001'],
        'code_name': ['浦发银行', '平安银行'],
        'industry': ['银行', '银行'],
        'ipoDate': ['1999-11-10', '1991-04-03'],
    })
    monkeypatch.setattr(
        sync_stock, '_fetch_external_stock_basic',
        lambda *_a: (baostock_df, None, {}),
    )
    monkeypatch.setattr(sync_stock, '_fetch_akshare_industry_map', lambda: {})

    session = _FakeSession()
    result = sync_stock.sync_tdx_stock_to_database(
        SimpleNamespace(), SimpleNamespace(get_session=lambda: session)
    )

    assert result['inserted_stocks'] == 2
    assert result['failed_stocks'] == []
    assert session.commits >= 2
    assert {row.ts_code for row in session.query(StockBasic).all()} == {
        '600000.SH', '000001.SZ',
    }


def test_sync_tdx_stock_to_database_rolls_back_on_prepare_error(monkeypatch):
    """准备阶段抛出数据源异常时回滚会话，不写入任何记录。"""
    class _Tdx:
        def __init__(self, _config, _db_manager):
            pass

        def get_stock_list(self):
            return ['600000.SH']

    monkeypatch.setattr('src.data.tdx_handler.TdxHandler', _Tdx)
    monkeypatch.setattr(
        'src.data.baostock_handler.BaostockHandler', lambda *_a: object()
    )
    monkeypatch.setattr(
        'src.data.akshare_handler.AkShareHandler', lambda *_a: object()
    )

    def _explode(*_args):
        raise DataSourceException('baostock', '网络中断')

    monkeypatch.setattr(sync_stock, '_fetch_external_stock_basic', _explode)

    session = _FakeSession()
    result = sync_stock.sync_tdx_stock_to_database(
        SimpleNamespace(), SimpleNamespace(get_session=lambda: session)
    )

    assert session.rollbacks == 1
    assert result['inserted_stocks'] == 0
    assert session.query(StockBasic).all() == []
