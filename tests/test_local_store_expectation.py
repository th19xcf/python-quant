#!/usr/bin/env python3

"""本地列存缺失分级的回归测试。

核心断言：**理应存在却读不到必须报错并给出路径**，而「未上市 / 已退市 /
区间停牌 / 尚未构建」这类正常空数据必须放行（只留日志）。
"""

from datetime import date

import polars as pl
import pytest
from loguru import logger

from src.data.local_store.expectation import (
    MissingLocalDataError,
    check_quote_file,
    explain_gap,
)
from src.data.local_store.store import LocalFactorStore, LocalQuoteStore

CODE = '600000.SH'


def _write_daily_index(root, *, rows=5, start=date(2024, 1, 2), end=date(2024, 1, 5),
                       data_type='stock', code=CODE) -> None:
    (root / 'meta').mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'ts_code': [code],
        'data_type': [data_type],
        'rows': [rows],
        'start': [start],
        'end': [end],
    }).write_parquet(root / 'meta' / 'daily_index.parquet')


def _write_stock_basic(root, *, list_date=date(1991, 4, 3), delist_date=None,
                       code=CODE) -> None:
    (root / 'meta').mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'ts_code': [code],
        'list_date': [list_date],
        'delist_date': [delist_date],
    }).write_parquet(root / 'meta' / 'stock_basic.parquet')


def _write_quote(root, *, dates, code=CODE, data_type='stock') -> None:
    path = root / 'quotes' / data_type / 'sh' / f'{code}.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'date': dates,
        'open': [10.0] * len(dates),
        'high': [10.5] * len(dates),
        'low': [9.5] * len(dates),
        'close': [10.0] * len(dates),
        'volume': [1_000_000.0] * len(dates),
    }).write_parquet(path)


# ---------------------------------------------------------------- 路径写错即报错

def test_read_daily_raises_when_store_root_is_wrong(tmp_path):
    """根目录写错：带日期的读取路径必须抛错并指出实际路径。"""
    wrong = tmp_path / 'typo_local_store'
    store = LocalQuoteStore(wrong)

    with pytest.raises(MissingLocalDataError) as excinfo:
        store.read_daily('stock', CODE, date(2024, 1, 2), date(2024, 1, 5))

    assert str(wrong) in str(excinfo.value)
    assert 'local_store_path' in str(excinfo.value)


def test_read_file_warns_when_store_root_is_wrong(tmp_path):
    """根目录写错：整份读取不抛错（首次构建根目录本就未创建），但必须留痕。

    这是构建/采集语义与读取语义的边界：``read_file`` 无法从文件系统区分
    「首次构建」和「路径拼错」，抛错会让新库构建整体失败；因此改为每次根目录
    只 ``warn`` 一次，读取链路（``read_daily``）则直接抛错。
    """
    wrong = tmp_path / 'typo_local_store'
    store = LocalQuoteStore(wrong)
    records: list[str] = []
    handler_id = logger.add(lambda msg: records.append(str(msg)), level='WARNING')
    try:
        assert store.read_file('stock', CODE) is None
        assert store.read_file('stock', CODE) is None
    finally:
        logger.remove(handler_id)

    hits = [line for line in records if str(wrong) in line]
    assert len(hits) == 1, f'同一根目录应只告警一次，实际 {len(hits)} 条'
    assert 'local_store_path' in hits[0]


def test_read_file_raises_when_coverage_declared_but_file_missing(tmp_path):
    """根目录在、daily_index 声明有覆盖、文件不在：搬迁漏改，必须报错。"""
    store = LocalQuoteStore(tmp_path)
    _write_daily_index(tmp_path)

    with pytest.raises(MissingLocalDataError) as excinfo:
        store.read_file('stock', CODE)

    assert f'{CODE}.parquet' in str(excinfo.value)


def test_read_daily_raises_when_coverage_declared_but_file_missing(tmp_path):
    """daily_index 声明有 5 行、上市期内却读不到文件 → 报错（不再 warn 后返回 None）。"""
    _write_daily_index(tmp_path)
    _write_stock_basic(tmp_path)
    (tmp_path / 'quotes' / 'stock').mkdir(parents=True, exist_ok=True)
    store = LocalQuoteStore(tmp_path)

    with pytest.raises(MissingLocalDataError) as excinfo:
        store.read_daily('stock', CODE, date(2024, 1, 2), date(2024, 1, 5))

    assert f'{CODE}.parquet' in str(excinfo.value)


def test_missing_dividend_snapshot_is_visible_to_consumers(tmp_path):
    """快照整体缺失：读取层返回 None 但留痕，且 ``has_dividend_snapshot`` 为假。

    致命与否交给消费方判断（``DataManager.get_dividends`` / 回测撮合层会抛错），
    构建流程中途调用则不被误伤。
    """
    store = LocalFactorStore(tmp_path)

    assert store.has_dividend_snapshot() is False
    assert store.dividends(CODE) is None


# ---------------------------------------------------------------- 预期为空必须放行

def test_read_daily_returns_none_for_unlisted_code(tmp_path):
    """请求区间整段早于上市日：允许空值，不抛错。"""
    _write_daily_index(tmp_path, start=date(2024, 2, 1), end=date(2024, 2, 5))
    _write_stock_basic(tmp_path, list_date=date(2024, 3, 1))
    (tmp_path / 'quotes' / 'stock').mkdir(parents=True, exist_ok=True)
    store = LocalQuoteStore(tmp_path)

    assert store.read_daily('stock', CODE, date(2024, 1, 2), date(2024, 1, 5)) is None


def test_read_daily_returns_none_for_delisted_code(tmp_path):
    """请求区间整段晚于退市日：允许空值，不抛错。"""
    _write_daily_index(tmp_path, start=date(2024, 2, 1), end=date(2024, 2, 5))
    _write_stock_basic(tmp_path, list_date=date(2000, 1, 4), delist_date=date(2020, 6, 30))
    (tmp_path / 'quotes' / 'stock').mkdir(parents=True, exist_ok=True)
    store = LocalQuoteStore(tmp_path)

    assert store.read_daily('stock', CODE, date(2024, 1, 2), date(2024, 1, 5)) is None


def test_read_daily_raises_for_listed_code_without_coverage(tmp_path):
    """上市期内、区间带日期但 daily_index 无记录：按口径报错。

    刻意不放行：带日期的读取是「回溯/分析」语义，样本悄悄缺一只就会得到
    系统性偏低的结论；批量取数走 ``read_file``（不带日期）才允许按未构建返回 None。
    """
    _write_daily_index(tmp_path, code='000001.SZ')
    _write_stock_basic(tmp_path, list_date=date(1991, 4, 3))
    (tmp_path / 'quotes' / 'stock').mkdir(parents=True, exist_ok=True)
    store = LocalQuoteStore(tmp_path)

    with pytest.raises(MissingLocalDataError):
        store.read_daily('stock', CODE, date(2024, 1, 2), date(2024, 1, 5))


def test_read_file_returns_none_for_code_not_built(tmp_path):
    """整份读取（不带日期）在未构建时返回 None，供增量采集 / 首次构建使用。"""
    _write_daily_index(tmp_path, code='000001.SZ')
    _write_stock_basic(tmp_path, list_date=date(1991, 4, 3))
    (tmp_path / 'quotes' / 'stock').mkdir(parents=True, exist_ok=True)
    store = LocalQuoteStore(tmp_path)

    assert store.read_file('stock', CODE) is None


def test_read_daily_returns_none_when_range_has_no_rows(tmp_path):
    """文件在、覆盖度也说得通，但区间内一行没有（停牌/缺口）：返回 None 不抛错。"""
    _write_daily_index(tmp_path, start=date(2024, 1, 2), end=date(2024, 1, 31))
    _write_stock_basic(tmp_path, list_date=date(1991, 4, 3))
    _write_quote(tmp_path, dates=[date(2024, 1, 5)])
    store = LocalQuoteStore(tmp_path)

    assert store.read_daily('stock', CODE, date(2024, 1, 10), date(2024, 1, 12)) is None


def test_read_daily_returns_frame_when_file_exists(tmp_path):
    """正常路径：文件存在时按区间切片返回，不被新断言误伤。"""
    _write_daily_index(tmp_path, start=date(2024, 1, 2), end=date(2024, 1, 5))
    _write_stock_basic(tmp_path, list_date=date(1991, 4, 3))
    _write_quote(tmp_path, dates=[date(2024, 1, 2), date(2024, 1, 3)])
    store = LocalQuoteStore(tmp_path)

    frame = store.read_daily('stock', CODE, date(2024, 1, 2), date(2024, 1, 3))

    assert frame is not None
    assert frame.height == 2


def test_dividends_returns_none_for_code_without_records(tmp_path):
    """快照存在但该标的无分红记录：正常语义，返回 None 而不是抛错。"""
    factors = tmp_path / 'factors'
    factors.mkdir(parents=True)
    pl.DataFrame({
        'ts_code': ['000001.SZ'],
        'ex_date': [date(2024, 1, 2)],
        'cash_div': [0.1],
        'share_div': [0.0],
    }).write_parquet(factors / 'stock_dividend.parquet')
    store = LocalFactorStore(tmp_path)

    assert store.dividends(CODE) is None


def test_disabled_store_keeps_returning_empty(tmp_path):
    """显式关闭本地列存时保持旧语义：不抛错、返回空（供回退链路使用）。"""
    store = LocalFactorStore(tmp_path, enabled=False)

    assert store.dividends(CODE) is None
    assert store.factors(CODE) is None


# ---------------------------------------------------------------- 判定函数本身

def test_explain_gap_prefers_listing_window_over_coverage(tmp_path):
    """已退市优先于覆盖度：即便 daily_index 残留记录，也判为预期为空。"""
    _write_daily_index(tmp_path, rows=5, start=date(2024, 1, 2), end=date(2024, 1, 5))
    _write_stock_basic(tmp_path, list_date=date(2000, 1, 4), delist_date=date(2020, 6, 30))

    reason = explain_gap(tmp_path, 'stock', CODE, date(2024, 1, 2), date(2024, 1, 5))

    assert reason is not None
    assert '已退市' in reason


def test_check_quote_file_raises_with_path_in_message(tmp_path):
    """报错报文必须带文件路径，便于直接定位是哪个常量拼错。"""
    _write_daily_index(tmp_path)
    _write_stock_basic(tmp_path)
    (tmp_path / 'quotes' / 'stock').mkdir(parents=True, exist_ok=True)
    target = tmp_path / 'quotes' / 'stock' / 'sh' / f'{CODE}.parquet'

    with pytest.raises(MissingLocalDataError) as excinfo:
        check_quote_file(target, store_root=tmp_path, data_type='stock',
                         ts_code=CODE, start=date(2024, 1, 2), end=date(2024, 1, 5))

    assert str(target) in str(excinfo.value)
