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

from datetime import date
from types import SimpleNamespace

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

    def __init__(self, rows):
        self._rows = rows

    def filter(self, *_args, **_kwargs):
        return self

    def all(self):
        return list(self._rows)


class _FakeSession:
    """按模型类持有内存行的最小会话替身。"""

    def __init__(self, stock_rows=None, fund_rows=None, index_rows=None):
        self._tables = {
            StockBasic: list(stock_rows or []),
            FundBasic: list(fund_rows or []),
            IndexBasic: list(index_rows or []),
        }
        self.commits = 0
        self.rollbacks = 0
        self.active = True

    @property
    def is_active(self):
        return self.active

    def query(self, model):
        return _FakeQuery(self._tables[model])

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

        def get_stock_list(self):
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

        def get_stock_list(self):
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
