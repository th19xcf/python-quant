"""ST 时间维表与按日期判定 ST 状态的回归测试。"""

from datetime import date

import numpy as np
import pandas as pd
import polars as pl
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.backtest.engine.backtest_engine import BacktestEngine
from src.backtest.engine.execution_core import EventBacktestRunner
from src.backtest.strategies.base_strategy import BaseStrategy
from src.data.services.st_status_service import StStatusService, normalize_ts_code
from src.database.models.stock import StockStatusHistory


class _IndexSignalStrategy(BaseStrategy):
    """在指定 bar 索引发出固定信号"""

    def __init__(self, signals):
        super().__init__(name='索引信号策略')
        self.signals = dict(signals)

    def generate_signal(self, data, index):
        return self.signals.get(index, 'hold')


def _make_df(closes):
    days = len(closes)
    dates = pd.date_range('2023-01-02', periods=days, freq='B')
    closes = np.asarray(closes, dtype=float)
    return pl.DataFrame({
        'date': dates,
        'open': closes,
        'high': closes * 1.01,
        'low': closes * 0.99,
        'close': closes,
        'volume': np.full(days, 1_000_000.0),
    })


def _interval(start, end=date(2024, 1, 10), is_st=True):
    return [{'start_date': start, 'end_date': end, 'is_st': is_st}]


def test_st_lookup_respects_interval_boundaries():
    """区间首尾都命中，区间外（含退市前后）返回未知 None"""
    value = _interval(date(2024, 1, 1), date(2024, 1, 10))
    resolver = EventBacktestRunner._resolve_st_for_date
    assert resolver(value, date(2023, 12, 31)) is None
    assert resolver(value, date(2024, 1, 1)) is True
    assert resolver(value, date(2024, 1, 10)) is True
    assert resolver(value, date(2024, 1, 11)) is None
    # 字符串日期也应被归一化
    assert resolver(value, '2024-01-05') is True


def test_st_lookup_open_ended_interval_and_non_st():
    """end_date=None 表示持续有效；is_st=False 是明确非 ST"""
    value = [{'start_date': date(2024, 5, 1), 'end_date': None, 'is_st': False}]
    resolver = EventBacktestRunner._resolve_st_for_date
    assert resolver(value, date(2024, 4, 30)) is None
    assert resolver(value, date(2024, 5, 1)) is False
    assert resolver(value, date(2025, 1, 1)) is False


def test_st_status_unknown_only_for_missing_or_empty():
    """静态三态与区间列表的未知判定"""
    st_map, unknown = EventBacktestRunner._resolve_st_status(
        ['A', 'B', 'C'], {'A': True, 'B': False, 'C': []})
    assert unknown is True
    assert st_map['A'] is True and st_map['B'] is False

    st_map, unknown = EventBacktestRunner._resolve_st_status(
        ['A', 'B'], {'A': True, 'B': _interval(date(2024, 1, 1))})
    assert unknown is False


def test_engine_uses_interval_cover_to_clear_st_unknown():
    """区间覆盖全部交易日的明确非 ST → st_unknown=False；部分覆盖 → True"""
    df = _make_df([10.0, 10.0, 10.0])

    engine = BacktestEngine(df)
    engine.set_strategy(_IndexSignalStrategy({}))
    full = engine.run_backtest(
        ts_code='600000.SH',
        is_st=[{'start_date': date(2023, 1, 1), 'end_date': date(2023, 1, 6), 'is_st': False}],
    )
    assert full['st_unknown'] is False

    engine = BacktestEngine(df)
    engine.set_strategy(_IndexSignalStrategy({}))
    partial = engine.run_backtest(
        ts_code='600000.SH',
        is_st=[{'start_date': date(2023, 1, 3), 'end_date': date(2023, 1, 6), 'is_st': False}],
    )
    assert partial['st_unknown'] is True


def test_normalize_ts_code_variants():
    assert normalize_ts_code('600000') == '600000.SH'
    assert normalize_ts_code('000001') == '000001.SZ'
    assert normalize_ts_code('300001') == '300001.SZ'
    assert normalize_ts_code('830001') == '830001.BJ'
    assert normalize_ts_code('sh.600000') == '600000.SH'
    assert normalize_ts_code('600000.SH') == '600000.SH'
    assert normalize_ts_code('') is None


@pytest.fixture
def service(tmp_path):
    engine = create_engine(f'sqlite:///{tmp_path / "st.db"}')
    StockStatusHistory.__table__.create(engine)
    session = sessionmaker(bind=engine)()
    yield StStatusService(session=session)
    session.close()


def test_refresh_current_writes_and_closes_intervals(service, monkeypatch):
    """当前名单写入开放区间；名单移除后区间被闭合"""
    monkeypatch.setattr(
        StStatusService, '_fetch_current_st_codes',
        staticmethod(lambda: ['600000.SH', '000001.SZ']),
    )

    stats = service.refresh_current(as_of=date(2024, 1, 15),
                                   coverage_start=date(2023, 1, 1))
    assert stats['ok'] is True and stats['mode'] == 'current_only'
    assert stats['opened'] == 2 and stats['closed'] == 0

    intervals = service.get_status_intervals(['600000.SH', '000001.SZ'])
    assert intervals['600000.SH'][0]['start_date'] == date(2023, 1, 1)
    assert intervals['600000.SH'][0]['end_date'] is None
    assert intervals['600000.SH'][0]['is_st'] is True

    # 000001.SZ 摘帽：第二次刷新只保留 600000.SH
    monkeypatch.setattr(
        StStatusService, '_fetch_current_st_codes',
        staticmethod(lambda: ['600000.SH']),
    )
    stats = service.refresh_current(as_of=date(2024, 2, 1))
    assert stats['closed'] == 1

    intervals = service.get_status_intervals(['600000.SH', '000001.SZ'])
    assert intervals['000001.SZ'][0]['end_date'] == date(2024, 1, 31)
    assert intervals['600000.SH'][0]['end_date'] is None


def test_get_status_intervals_filters_by_range(service, monkeypatch):
    """查询区间与维表无交集时不返回，回测按未知处理"""
    monkeypatch.setattr(
        StStatusService, '_fetch_current_st_codes',
        staticmethod(lambda: ['600000.SH']),
    )
    service.refresh_current(as_of=date(2024, 1, 15), coverage_start=date(2024, 1, 1))

    assert '600000.SH' in service.get_status_intervals(['600000.SH'])
    # 区间 [2024-01-01, None) 与 2023 年无交集
    assert service.get_status_intervals(
        ['600000.SH'], date(2023, 1, 1), date(2023, 12, 31)) == {}
    # 未知标的不出现在结果中
    assert service.get_status_intervals(['999999.SZ']) == {}


def test_refresh_failure_keeps_existing_data(service, monkeypatch):
    """数据源失败时返回 ok=False，不清空已有维表"""
    def _boom():
        raise RuntimeError('网络不可用')

    monkeypatch.setattr(
        StStatusService, '_fetch_current_st_codes', staticmethod(_boom),
    )
    stats = service.refresh_current(as_of=date(2024, 1, 15))
    assert stats['ok'] is False
    assert '网络不可用' in stats['error']
