#!/usr/bin/env python3

"""扩展数据契约产物的构建器回归测试（停复牌 / 行业 PIT / 指数成分）。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from src.data.quality.extended_builders import (
    build_index_membership,
    build_industry_pit,
    build_suspension,
    main,
)
from src.data.quality.extended_contracts import check_contracts
from src.data.trading_calendar import reset_calendar_cache

# 连续 6 个工作日（2026-11-02 周一 ~ 11-09 周一，跳过周末）
TRADING_DAYS = (
    date(2026, 11, 2), date(2026, 11, 3), date(2026, 11, 4),
    date(2026, 11, 5), date(2026, 11, 6), date(2026, 11, 9),
)


@pytest.fixture(autouse=True)
def _clear_calendar_cache() -> None:
    reset_calendar_cache()
    yield
    reset_calendar_cache()


def _write_index(root: Path, days: tuple[date, ...] = TRADING_DAYS) -> None:
    path = root / 'quotes' / 'index' / 'sh' / '000001.SH.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'date': list(days)}, schema={'date': pl.Date}).write_parquet(path)


def _write_quotes(root: Path, ts_code: str, days: tuple[date, ...],
                  data_type: str = 'stock') -> None:
    market = ts_code.rsplit('.', 1)[-1].lower()
    path = root / 'quotes' / data_type / market / f'{ts_code}.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'date': list(days)}, schema={'date': pl.Date}).write_parquet(path)


def _write_daily_index(root: Path, entries: list[tuple[str, str, str, int]]) -> None:
    """entries: [(ts_code, data_type, market, rows)]"""
    rows = [{
        'ts_code': code, 'data_type': kind, 'market': market, 'rows': rows,
        'start': TRADING_DAYS[0], 'end': TRADING_DAYS[-1],
    } for code, kind, market, rows in entries]
    path = root / 'meta' / 'daily_index.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


def _write_stale(root: Path, codes: list[str]) -> None:
    path = root / 'meta' / 'universe' / 'stale_status.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'ts_code': codes, 'action': ['exclude'] * len(codes),
        'is_stale': [True] * len(codes),
    }).write_parquet(path)


def test_suspension_derives_missing_trading_days(tmp_path: Path) -> None:
    _write_index(tmp_path)
    # 600000.SH 缺少 11-04（周三）一个交易日
    _write_quotes(tmp_path, '600000.SH',
                  tuple(d for d in TRADING_DAYS if d != date(2026, 11, 4)))
    _write_quotes(tmp_path, '600001.SH', TRADING_DAYS)
    _write_daily_index(tmp_path, [
        ('600000.SH', 'stock', 'sh', 5),
        ('600001.SH', 'stock', 'sh', 6),
    ])

    summary = build_suspension(tmp_path, as_of=date(2026, 11, 9), commit=True)
    assert summary['codes_covered'] == 2
    assert summary['suspended_rows'] == 1

    frame = pl.read_parquet(
        tmp_path / 'meta' / 'suspension' / 'suspension_daily.parquet')
    assert frame.height == 1
    row = frame.row(0, named=True)
    assert row['ts_code'] == '600000.SH'
    assert row['trade_date'] == date(2026, 11, 4)
    assert row['is_suspended'] is True
    assert row['run_len'] == 1
    assert row['confidence'] == 'high'

    coverage = json.loads(
        (tmp_path / 'meta' / 'suspension' / 'coverage.json').read_text('utf-8'))
    assert coverage['complete'] is True
    assert coverage['method'] == 'derived_from_quote_gap'
    status = {item['key']: item for item in check_contracts(
        tmp_path, as_of=date(2026, 11, 9))}['suspension']
    assert status['status'] == 'ready'


def test_suspension_marks_stale_codes_low_confidence(tmp_path: Path) -> None:
    _write_index(tmp_path)
    _write_quotes(tmp_path, '600002.SH',
                  tuple(d for d in TRADING_DAYS if d != date(2026, 11, 5)))
    _write_daily_index(tmp_path, [('600002.SH', 'stock', 'sh', 5)])
    _write_stale(tmp_path, ['600002.SH'])

    build_suspension(tmp_path, as_of=date(2026, 11, 9), commit=True)
    frame = pl.read_parquet(
        tmp_path / 'meta' / 'suspension' / 'suspension_daily.parquet')
    assert frame['confidence'].to_list() == ['low']
    coverage = json.loads(
        (tmp_path / 'meta' / 'suspension' / 'coverage.json').read_text('utf-8'))
    assert coverage['low_confidence_rows'] == 1
    assert any('confidence=high' in caveat for caveat in coverage['caveats'])


def _write_industry_snapshot(root: Path, day: date, rows: list[tuple[str, str]]) -> None:
    path = root / 'meta' / 'industry' / f'industry_{day:%Y%m%d}.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [{'code': code, 'industry_l1': 'TDX 金融' if l1.startswith('T01') else 'TDX 信息',
          'industry_l2': f'二级{l1[:5]}', 'industry_code': l1,
          'source': 'tdx_hq_cache', 'as_of': day}
         for code, l1 in rows]
    ).write_parquet(path)


def test_industry_pit_splits_on_change_points(tmp_path: Path) -> None:
    day1, day2 = date(2026, 11, 3), date(2026, 11, 5)
    # 600000.SH 的二级行业从 T010101 切到 T010102；600001.SH 不变
    _write_industry_snapshot(tmp_path, day1, [
        ('600000.SH', 'T010101'), ('600001.SH', 'T020201')])
    _write_industry_snapshot(tmp_path, day2, [
        ('600000.SH', 'T010201'), ('600001.SH', 'T020201')])

    summary = build_industry_pit(tmp_path, commit=True)
    assert summary['snapshots'] == 2
    assert summary['codes'] == 2

    frame = pl.read_parquet(tmp_path / 'meta' / 'industry_pit' / 'industry_pit.parquet')
    l2 = frame.filter((pl.col('ts_code') == '600000.SH') & (pl.col('level') == 'L2'))
    assert l2.sort('effective_date')['industry_code'].to_list() == ['T0101', 'T0102']
    assert l2.sort('effective_date')['end_date'].to_list() == [day2, None]
    # L1 未变 -> 只有一段开口区间
    l1 = frame.filter((pl.col('ts_code') == '600000.SH') & (pl.col('level') == 'L1'))
    assert l1.height == 1
    assert l1['end_date'].to_list() == [None]

    coverage = json.loads(
        (tmp_path / 'meta' / 'industry_pit' / 'coverage.json').read_text('utf-8'))
    assert coverage['complete'] is False
    assert '不得用当前行业回填' in coverage['incomplete_reason']


def _write_index_weight(root: Path,
                        panels: dict[str, dict[str, list[str]]]) -> None:
    """写指数成分月度快照：``{指数: {期末日: [成分...]}}``。"""
    rows = [{'index_code': index, 'ts_code': code, 'trade_date': day,
             'weight': 1.0, 'source': 'tushare.index_weight', 'fetched_at': ''}
            for index, periods in panels.items()
            for day, codes in periods.items() for code in codes]
    path = root / 'meta' / 'index_members' / 'index_weight_monthly.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema={
        'index_code': pl.String, 'ts_code': pl.String, 'trade_date': pl.String,
        'weight': pl.Float64, 'source': pl.String,
        'fetched_at': pl.String}).write_parquet(path)


def test_membership_monthly_diff_marks_entry_and_exit(tmp_path: Path) -> None:
    day1, day2 = date(2026, 1, 30), date(2026, 2, 27)
    _write_index_weight(tmp_path, {'000300.SH': {
        '20260130': ['600000.SH', '600001.SH'],
        # 600001 退出、600002 新进
        '20260227': ['600000.SH', '600002.SH'],
    }})

    summary = build_index_membership(tmp_path, allow_remote=False, commit=True)
    assert summary['indices'] == 1
    assert summary['intervals'] == 3
    assert summary['index_codes'] == ['000300']

    frame = pl.read_parquet(
        tmp_path / 'meta' / 'index_members' / 'membership_intervals.parquet')
    closed = frame.filter(pl.col('ts_code') == '600001.SH').row(0, named=True)
    assert closed['out_date'] == day2
    assert closed['out_date_known'] is True
    # 首期就在册 -> 真实纳入日不晚于该期（左截断）
    assert closed['in_date_known'] is False

    opened = frame.filter(pl.col('ts_code') == '600000.SH').row(0, named=True)
    assert opened['out_date'] is None
    assert opened['in_date'] == day1
    assert opened['in_date_known'] is False

    entered = frame.filter(pl.col('ts_code') == '600002.SH').row(0, named=True)
    assert entered['in_date'] == day2
    assert entered['in_date_known'] is True
    assert entered['out_date'] is None

    coverage = json.loads(
        (tmp_path / 'meta' / 'index_members' / 'coverage.json').read_text('utf-8'))
    assert coverage['complete'] is True
    assert coverage['resolution'] == 'monthly'


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
    _write_index(tmp_path)
    _write_quotes(tmp_path, '600003.SH',
                  tuple(d for d in TRADING_DAYS if d != date(2026, 11, 6)))
    _write_daily_index(tmp_path, [('600003.SH', 'stock', 'sh', 5)])

    summary = build_suspension(tmp_path, as_of=date(2026, 11, 9), commit=False)
    assert summary['committed'] is False
    assert summary['suspended_rows'] == 1
    assert not (tmp_path / 'meta' / 'suspension').exists()


def test_remote_fetch_break_records_skipped_and_stops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """第 2 只指数失败即停：记下被跳过的指数与续跑起点，后续指数不再请求。"""
    _write_index_weight(tmp_path, {'000300.SH': {'20260130': ['600000.SH']}})
    calls: list[str] = []

    def _fake_fetch(symbol: str):
        calls.append(symbol)
        if symbol == '000905':
            return None
        frame = pl.DataFrame(
            [{'index_code': symbol, 'ts_code': '600000.SH',
              'snapshot_date': date(2026, 1, 30), 'source': 'csindex'}],
            schema={'index_code': pl.String, 'ts_code': pl.String,
                    'snapshot_date': pl.Date, 'source': pl.String})
        return frame, date(2026, 1, 30)

    monkeypatch.setattr(
        'src.data.quality.extended_builders._fetch_membership_snapshot',
        _fake_fetch)
    monkeypatch.setattr(
        'src.data.quality.extended_builders.time.sleep', lambda _seconds: None)

    summary = build_index_membership(tmp_path, allow_remote=True, commit=True)
    # 第 2 只失败后立即停手，第 3/4 只根本没有被请求
    assert calls == ['000300', '000905']
    assert summary['remote_failed'] == '000905.SH'
    assert summary['remote_skipped'] == ['000852.SH', '000016.SH']

    progress_path = (tmp_path / 'meta' / 'index_membership'
                     / 'fetch_progress.json')
    progress = json.loads(progress_path.read_text('utf-8'))
    assert progress['completed'] == ['000300.SH']
    assert progress['failed'] == '000905.SH'
    assert progress['skipped'] == ['000852.SH', '000016.SH']
    assert progress['next_index'] == '000905.SH'
    assert progress['complete'] is False

    # 再跑一次：按续跑记录从失败那只开始，不再重抓已完成的 000300
    calls.clear()
    again = build_index_membership(tmp_path, allow_remote=True, commit=True)
    assert calls == ['000905']
    assert again['resume_from'] == '000905.SH'


def test_cli_reports_missing_inputs(tmp_path: Path) -> None:

    with pytest.raises(FileNotFoundError):
        build_suspension(tmp_path, as_of=date(2026, 11, 9))
    with pytest.raises(FileNotFoundError):
        build_industry_pit(tmp_path)
    # 无成分月度快照 -> 明确报缺源，而不是产出空表冒充就绪
    with pytest.raises(FileNotFoundError):
        main(['membership', '--root', str(tmp_path), '--json'])
