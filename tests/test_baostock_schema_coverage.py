#!/usr/bin/env python3

"""Baostock schema 与数据源基类纯函数的覆盖率补齐用例。

覆盖 minute_row_to_record 的两种时间格式与空值回退、daily_row_to_record
与复权因子计算，以及 DataSourceBase 的参数校验/归一化/K 线质量校验分支。
全部离线，不依赖 baostock 安装。
"""

from datetime import date, datetime

import polars as pl
import pytest

from src.data.sources import baostock_schema as schema
from src.data.sources.base import (
    KLINE_STANDARD_COLUMNS,
    REQUIRED_SOURCE_METHODS,
    DataSourceBase,
)

# ----------------------------------------------------------------------
# baostock_schema
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ('raw_time', 'expected'),
    [
        ('20240102103000000', datetime(2024, 1, 2, 10, 30, 0)),
        (' 20240102103000 ', datetime(2024, 1, 2, 10, 30, 0)),
        ('2024-01-02T10:30:00', datetime(2024, 1, 2, 10, 30, 0)),
        ('2024-01-02 10:30:00', datetime(2024, 1, 2, 10, 30, 0)),
    ],
)
def test_minute_row_parses_numeric_and_iso_time(raw_time, expected):
    """分钟线时间支持数字串与 ISO 字符串两种上游格式。"""
    row = {
        'time': raw_time, 'open': 10.5, 'high': 11.0,
        'low': 10.0, 'close': 10.8, 'volume': 100.0, 'amount': 200.0,
    }

    record = schema.minute_row_to_record(row, '600000.SH')

    assert record['ts_code'] == '600000.SH'
    assert record['trade_time'] == expected
    assert record['close'] == 10.8
    # 成交量契约：上游 volume 为「股」，此处统一除以 100 归一为「手」（1 手=100 股）
    assert record['vol'] == 1.0


def test_minute_row_falls_back_to_zero_for_empty_values():
    """分钟线数值为空/None 时统一回退为 0.0，不抛出。"""
    row = {
        'time': '2024-01-02T10:30:00', 'open': None, 'high': '',
        'low': 0, 'close': None, 'volume': '', 'amount': None,
    }

    record = schema.minute_row_to_record(row, '000001.SZ')

    assert record['open'] == 0.0
    assert record['high'] == 0.0
    assert record['low'] == 0.0
    assert record['close'] == 0.0
    assert record['vol'] == 0.0
    assert record['amount'] == 0.0


def test_daily_row_converts_types_and_computes_change():
    """日线行转换出标准字段，change 由收盘价减前收计算。"""
    row = {
        'date': '2024-01-02', 'open': 9.9, 'high': 10.5, 'low': 9.8,
        'close': 10.2, 'preclose': 10.0, 'pctChg': 2.0,
        'volume': 1000.0, 'amount': 5000.0,
    }

    record = schema.daily_row_to_record(row, '600000.SH')

    assert record['trade_date'] == date(2024, 1, 2)
    assert record['pre_close'] == 10.0
    assert record['change'] == pytest.approx(0.2)
    assert record['pct_chg'] == 2.0


def test_daily_row_falls_back_to_zero_for_empty_values():
    """日线空数值回退 0.0，change 退化为 0。"""
    row = {
        'date': '2024-01-03', 'open': '', 'high': None, 'low': '',
        'close': None, 'preclose': None, 'pctChg': '',
        'volume': None, 'amount': '',
    }

    record = schema.daily_row_to_record(row, '600000.SH')

    assert record['close'] == 0.0
    assert record['change'] == 0.0
    assert record['vol'] == 0.0


def test_daily_row_keeps_turn_as_percent_without_conversion():
    """turn 原样保留上游百分数（0.1979 表示 0.1979%），缺键时按 0.0。

    实测依据：600000.SH ``volume / (turn/100)`` 稳定反推出流通股本，
    若按小数解释会小两个数量级；因此这里断言不做任何 ×100/÷100 换算。
    """
    row = {
        'date': '2024-01-02', 'open': 9.9, 'high': 10.5, 'low': 9.8,
        'close': 10.2, 'preclose': 10.0, 'pctChg': 2.0,
        'volume': 1000.0, 'amount': 5000.0, 'turn': '0.1979',
    }

    record = schema.daily_row_to_record(row, '600000.SH')
    assert record['turn'] == pytest.approx(0.1979)
    # 成交量仍按「股→手」收口，与 turn 无关
    assert record['vol'] == pytest.approx(10.0)

    without_turn = {k: v for k, v in row.items() if k != 'turn'}
    assert schema.daily_row_to_record(without_turn, '600000.SH')['turn'] == 0.0

    suspended = {**row, 'turn': ''}
    assert schema.daily_row_to_record(suspended, '600000.SH')['turn'] == 0.0


def test_compute_daily_factors_success():
    """复权因子：qfq 按区间末锚点归一，hfq 为原始比值。"""
    raw = pl.DataFrame({'date': [1, 2, 3], 'close': [10.0, 11.0, 12.0]})
    hfq = pl.DataFrame({'date': [1, 2, 3], 'close_hfq': [20.0, 22.0, 24.0]})

    qfq, hfq_factor = schema.compute_daily_factors(raw, hfq)

    assert qfq is not None and hfq_factor is not None
    assert qfq['qfq_factor'].to_list()[-1] == pytest.approx(1.0)
    assert hfq_factor['hfq_factor'].to_list() == [2.0, 2.0, 2.0]


def test_compute_daily_factors_returns_none_for_empty_or_invalid():
    """无交集/价格非正/锚点非正时返回 (None, None)。"""
    raw = pl.DataFrame({'date': [1, 2], 'close': [10.0, 0.0]})
    hfq = pl.DataFrame({'date': [3], 'close_hfq': [24.0]})

    assert schema.compute_daily_factors(raw, hfq) == (None, None)

    zero_anchor = pl.DataFrame({'date': [1], 'close': [10.0]})
    zero_hfq = pl.DataFrame({'date': [1], 'close_hfq': [0.0]})
    assert schema.compute_daily_factors(zero_anchor, zero_hfq) == (None, None)


# ----------------------------------------------------------------------
# DataSourceBase：纯函数与参数校验
# ----------------------------------------------------------------------


class _StubSource(DataSourceBase):
    """可实例化的最小数据源子类，用于验证初始化契约。"""

    def __init__(self, config=None, fail=False):
        self._fail = fail
        super().__init__(config)

    def _initialize(self):
        if self._fail:
            raise RuntimeError('初始化失败')

    def get_stock_data(self, *_args, **_kwargs):
        return pl.DataFrame()

    def get_index_data(self, *_args, **_kwargs):
        return pl.DataFrame()


class _IncompleteSource(DataSourceBase):
    """未覆盖契约方法的子类，missing_contract_methods 应返回全部方法名。"""

    initialize = lambda self: None  # noqa: E731


def test_initialize_marks_available_and_failure_falls_back():
    """初始化成功置 True；抛异常时置 False 且不中断构造。"""
    ok = _StubSource()
    broken = _StubSource(fail=True)

    assert ok.is_available() is True
    assert broken.is_available() is False


def test_missing_contract_methods_detects_unimplemented():
    """完整实现返回空列表；未实现返回全部契约方法名。"""
    assert _StubSource.missing_contract_methods() == []
    assert _IncompleteSource.missing_contract_methods() == list(REQUIRED_SOURCE_METHODS)


def test_default_code_and_date_and_frequency_helpers():
    """默认代码、日期与频率参数校验分支。"""
    source = _StubSource()
    source.default_stock_codes = ['600000.SH']
    source.default_index_codes = ['000001.SH']

    assert source._get_default_codes('stock') == ['600000.SH']
    assert source._get_default_codes('index') == ['000001.SH']
    assert source._get_default_codes('fund') == []

    start, end = source._process_dates('', '', date_format='%Y%m%d')
    assert len(start) == 8 and len(end) == 8
    given = source._process_dates('2024-01-01', '2024-01-31')
    assert given == ('2024-01-01', '2024-01-31')

    assert source._validate_frequency('5') == '5'
    source.supported_frequencies = {'5', '15'}
    assert source._validate_frequency('15') == '15'
    assert source._validate_frequency('60') == '5'


def test_normalize_kline_empty_returns_input():
    """空表直接返回，不做任何列处理。"""
    empty = pl.DataFrame()

    assert DataSourceBase._normalize_kline(empty, {}) is empty


def test_normalize_kline_maps_dates_volume_and_quality():
    """列映射/日期解析/成交量单位/K 线质量校验在归一化收口点一次性完成。"""
    raw = pl.DataFrame({
        '日期': ['20240102', '20240103', '20240104'],
        '开盘': [10.0, 0.0, 0.0],
        '最高': [11.0, 14.0, 0.0],
        '最低': [9.0, 14.0, 0.0],
        '收盘': [10.5, 14.0, 0.0],
        '昨收': [10.0, 14.0, 0.0],
        '成交量': [1000.0, 1000.0, 0.0],
        '成交额': [5000.0, 5000.0, 0.0],
        '涨跌幅': [99.0, 0.0, 99.0],
        '涨跌额': [7.0, 0.0, 7.0],
    })
    column_map = {
        'date': '日期', 'open': '开盘', 'high': '最高', 'low': '最低',
        'close': '收盘', 'pre_close': '昨收', 'volume': '成交量',
        'amount': '成交额', 'pct_chg': '涨跌幅', 'change': '涨跌额',
    }

    result = DataSourceBase._normalize_kline(
        raw, column_map, ts_code='600000.SH',
        volume_unit='share', date_format='%Y%m%d', include_factors=False,
    )

    assert result.height == 2  # 非法 OHLC 且非停牌被剔除，停牌行保留
    assert list(result.columns) == [
        c for c in KLINE_STANDARD_COLUMNS if c in result.columns
    ]
    assert result['ts_code'].to_list() == ['600000.SH', '600000.SH']
    assert result['date'].to_list() == [date(2024, 1, 2), date(2024, 1, 4)]
    assert result['volume'].to_list() == pytest.approx([10.0, 0.0])
    assert 'qfq_factor' not in result.columns

    good = result.row(0, named=True)
    assert good['change'] == pytest.approx(0.5)
    assert good['pct_chg'] == pytest.approx(5.0)

    suspended = result.row(1, named=True)
    assert suspended['change'] == pytest.approx(7.0)
    assert suspended['pct_chg'] == pytest.approx(99.0)


def test_normalize_kline_fills_factors_and_recomputes():
    """include_factors=True 补齐因子列；合法前收时重算涨跌幅。"""
    raw = pl.DataFrame({
        'date': ['2024-01-02'],
        'open': [10.0], 'high': [11.0], 'low': [9.5], 'close': [10.5],
        'pre_close': [10.0], 'volume': [1000.0], 'amount': [5000.0],
        'pct_chg': [99.0], 'change': [7.0],
    })

    result = DataSourceBase._normalize_kline(raw, {}, date_format='%Y-%m-%d')

    row = result.row(0, named=True)
    assert result['qfq_factor'].to_list() == [1.0]
    assert result['hfq_factor'].to_list() == [1.0]
    assert row['change'] == pytest.approx(0.5)
    assert row['pct_chg'] == pytest.approx(5.0)
    assert result['date'].to_list() == [date(2024, 1, 2)]


def test_quality_validation_short_circuits_and_suspended_rows():
    """空表/缺 OHLC 列原样返回；停牌行保留并计数。"""
    empty = pl.DataFrame()
    assert DataSourceBase._validate_kline_quality(empty) is empty

    only_close = pl.DataFrame({'close': [1.0]})
    assert DataSourceBase._validate_kline_quality(only_close).columns == ['close']

    suspended = pl.DataFrame({
        'date': [date(2024, 1, 2)], 'open': [0.0], 'high': [0.0],
        'low': [0.0], 'close': [0.0], 'volume': [-1.0],
        'pre_close': [1.0], 'pct_chg': [0.0], 'change': [0.0],
    })

    result = DataSourceBase._validate_kline_quality(suspended)

    assert result.height == 1


def test_default_stub_methods_return_none():
    """基类未实现的数据方法返回 None 或空表，不抛出异常。"""
    source = _StubSource()

    assert source.get_stock_basic() is None
    assert source.get_index_basic() is None
    assert source.get_fund_basic() is None
    assert source.get_closed_fund_basic() is None
    assert source.get_stock_dividend('600000.SH') is None
    assert source.get_fund_data('510300.SH', '2024-01-01', '2024-01-31') is None
    assert source.update_stock_basic() is None
