#!/usr/bin/env python3

"""大势型指标（ABI/ADL/ADR/OBOS）依赖注入测试。

计算层不再自行构造 DatabaseManager：调用方需注入 db_manager 或直接传入
market_breadth 数据；缺依赖时显式失败/返回 None，禁止用全 0 补齐。
"""

import datetime

import numpy as np
import polars as pl
import pytest

from src.tech_analysis.indicators import market_breadth as mb
from src.tech_analysis.indicators.market_breadth import (
    MarketBreadthUnavailableError,
    calculate_abi,
    calculate_adl,
    calculate_adr,
    calculate_market_breadth_indicators,
    calculate_obos,
    get_market_breadth_from_db,
)


def _ohlcv(n: int = 40) -> pl.DataFrame:
    """构造带日期的 OHLCV 主帧。"""
    dates = pl.date_range(
        pl.date(2024, 1, 1), pl.date(2024, 12, 31), interval='1d', eager=True
    ).head(n)
    close = 100.0 + np.arange(n, dtype=np.float64)
    return pl.DataFrame(
        {
            'date': dates,
            'open': close,
            'high': close + 1.0,
            'low': close - 1.0,
            'close': close,
            'volume': np.full(n, 100_000.0),
        }
    )


def _breadth_rows(n: int = 40) -> list[tuple]:
    """构造与主帧日期对齐的涨跌家数行（tuple 形式，模拟数据库 Result）。"""
    start = datetime.date(2024, 1, 1)
    return [
        (
            start + datetime.timedelta(days=i),
            10 + i,
            5 + i,
            0,
            15 + 2 * i,
        )
        for i in range(n)
    ]


def _breadth_frame(n: int = 40) -> pl.DataFrame:
    """构造可直接注入的涨跌家数 DataFrame。"""
    dates = pl.date_range(
        pl.date(2024, 1, 1), pl.date(2024, 12, 31), interval='1d', eager=True
    ).head(n)
    idx = np.arange(n, dtype=np.int64)
    return pl.DataFrame(
        {
            'trade_date': dates,
            'up_count': idx + 10,
            'down_count': idx + 5,
            'flat_count': np.zeros(n, dtype=np.int64),
            'total_count': idx * 2 + 15,
        }
    )


class _FakeResult:
    """模拟 SQLAlchemy Result.all()。"""

    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


class _FakeSession:
    """模拟数据库会话，记录 close 调用。"""

    def __init__(self, rows):
        self._rows = rows
        self.closed = False

    def execute(self, _query):
        return _FakeResult(self._rows)

    def close(self):
        self.closed = True


class _FakeDbManager:
    """模拟 DatabaseManager：可返回会话、None 或抛错。"""

    def __init__(self, rows=None, session=None, error=None):
        self.session = session if session is not None else _FakeSession(rows or [])
        self.error = error

    def get_session(self):
        if self.error is not None:
            raise self.error
        return self.session


# ----------------------------------------------------------------------
# 取数层：缺 db_manager 返回 None，注入后可取数
# ----------------------------------------------------------------------


def test_get_market_breadth_without_db_manager_returns_none():
    """未注入 db_manager 时不再自行 new DatabaseManager，返回 None。"""
    assert get_market_breadth_from_db('2024-01-01', '2024-02-01') is None


def test_get_market_breadth_with_injected_manager_returns_frame():
    """注入 db_manager 后按 row orient 构造带列名的 DataFrame。"""
    rows = _breadth_rows(5)
    manager = _FakeDbManager(rows=rows)

    result = get_market_breadth_from_db('2024-01-01', '2024-02-01', db_manager=manager)

    assert result is not None
    assert result.columns == [
        'trade_date', 'up_count', 'down_count', 'flat_count', 'total_count'
    ]
    assert result.height == 5
    assert manager.session.closed is True


def test_get_market_breadth_with_missing_session_returns_none():
    """db_manager 未返回会话时返回 None，不抛错。"""
    manager = _FakeDbManager(session=None)
    manager.session = None

    assert get_market_breadth_from_db('2024-01-01', '2024-02-01', db_manager=manager) is None


def test_get_market_breadth_returns_none_for_empty_rows():
    """数据库无数据时返回 None（不再伪造全 0）。"""
    assert get_market_breadth_from_db(
        '2024-01-01', '2024-02-01', db_manager=_FakeDbManager(rows=[])
    ) is None


def test_get_market_breadth_swallows_db_errors():
    """取数异常记录 error 后返回 None，交由调用方显式失败。"""
    manager = _FakeDbManager(error=RuntimeError('db down'))

    assert get_market_breadth_from_db('2024-01-01', '2024-02-01', db_manager=manager) is None


# ----------------------------------------------------------------------
# 计算层：缺依赖显式失败，禁止全 0 补齐
# ----------------------------------------------------------------------


def test_calculate_abi_without_dependency_raises():
    """单指标计算缺涨跌家数且无 db_manager 时显式报错。"""
    with pytest.raises(MarketBreadthUnavailableError):
        calculate_abi(_ohlcv().lazy())


def test_calculate_breadth_indicators_without_dependency_return_none():
    """批量聚合缺依赖时返回 None，不产生全 0 指标列。"""
    assert calculate_market_breadth_indicators(_ohlcv().lazy(), ['abi', 'adl']) is None


@pytest.mark.parametrize(
    ('func', 'column', 'expected_nulls'),
    [
        (calculate_abi, 'abi10', 0),
        (calculate_adl, 'adl', 0),
        (calculate_adr, 'adr10', 9),
        (calculate_obos, 'obos10', 9),
    ],
)
def test_breadth_functions_with_injected_frame(func, column, expected_nulls):
    """直接注入 market_breadth 后产出对应指标列，且值来自真实涨跌家数。"""
    result = func(_ohlcv().lazy(), market_breadth=_breadth_frame()).collect()

    assert column in result.columns
    # ADR/OBOS 为 10 日滚动均值，前 9 行无完整窗口属预期
    assert result[column].null_count() == expected_nulls
    assert result['up_count'].to_list() == _breadth_frame()['up_count'].to_list()


def test_calculate_abi_value_matches_manual_formula():
    """ABI = |up-down|/(up+down)*100，逐行与手算一致。"""
    breadth = _breadth_frame()
    result = calculate_abi(_ohlcv().lazy(), market_breadth=breadth).collect()

    expected = (
        (breadth['up_count'] - breadth['down_count']).abs()
        / (breadth['up_count'] + breadth['down_count'])
        * 100
    )
    np.testing.assert_allclose(
        result['abi10'].to_numpy(), expected.to_numpy(), rtol=1e-5, atol=1e-5
    )


def test_calculate_abi_with_injected_db_manager():
    """注入 db_manager 时由计算层内部取数，同样不伪造全 0。"""
    manager = _FakeDbManager(rows=_breadth_rows())
    result = calculate_abi(_ohlcv().lazy(), db_manager=manager).collect()

    assert 'abi10' in result.columns
    assert result['abi10'].null_count() == 0


def test_ensure_columns_passthrough_when_present():
    """主帧已有 up_count/down_count 时直接复用，不触发取数。"""
    frame = _ohlcv().with_columns(
        pl.Series('up_count', np.arange(40, dtype=np.int64)),
        pl.Series('down_count', np.arange(40, dtype=np.int64)),
    )
    result = calculate_abi(frame.lazy()).collect()

    assert 'abi10' in result.columns
    assert result['up_count'].to_list() == frame['up_count'].to_list()


def test_multiple_polars_skips_breadth_without_dependency():
    """默认批量入口缺依赖时跳过大势型指标，其他指标照常计算。"""
    from src.tech_analysis.indicator_calculator import (
        calculate_multiple_indicators_polars,
    )

    result = calculate_multiple_indicators_polars(_ohlcv(), ['ma', 'abi'])

    assert 'ma5' in result.columns
    assert 'abi10' not in result.columns


def test_multiple_polars_uses_injected_market_breadth():
    """默认批量入口注入涨跌家数后正常产出大势型指标。"""
    from src.tech_analysis.indicator_calculator import (
        calculate_multiple_indicators_polars,
    )

    result = calculate_multiple_indicators_polars(
        _ohlcv(), ['ma', 'abi'], market_breadth=_breadth_frame()
    )

    assert 'ma5' in result.columns
    assert 'abi10' in result.columns


def test_indicator_manager_forwards_injected_market_breadth():
    """IndicatorManager 单指标入口把 market_breadth 透传给大势型指标。"""
    from src.tech_analysis.indicator_manager import IndicatorManager

    manager = IndicatorManager(cache_enabled=False)
    result = manager.calculate_indicator(
        _ohlcv(), 'abi', market_breadth=_breadth_frame()
    )

    assert 'abi10' in result.columns
