"""G-067：逐码候选清单落盘，且落盘行与内存 ``candidate_signals`` 逐条相等。"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools.advice_stateful_backtest import (
    CANDIDATE_COLUMNS,
    _read_chunk_candidates,
    _write_csv,
    candidate_reason,
    candidate_records_for_day,
    verify_candidate_rows,
)


def _tier_config(soft: bool = True) -> tuple[SimpleNamespace, SimpleNamespace]:
    tier = SimpleNamespace(buy_percentile=0.95)
    config = SimpleNamespace(soft_filter=SimpleNamespace(enabled=soft))
    return tier, config


def test_candidate_records_one_to_one_with_signals(tmp_path: Path) -> None:
    """内存候选 N 条 → 落盘 N 行，字段逐条相等。"""
    tier, config = _tier_config()
    candidates = [('600519.SH', 0.9912345), ('000001.SZ', 0.9501)]
    day = date(2026, 11, 4)
    stats = {'candidate_signals': 0}
    stats['candidate_signals'] += len(candidates)   # 与 run_stateful 同段计数

    records = candidate_records_for_day(
        candidates, day, candidate_reason(tier, config))
    assert len(records) == stats['candidate_signals']
    assert records[0]['reason'] == 'pct>=0.95;soft_pass;eligibility_ok;entry_allowed'
    for (code, pct), row in zip(candidates, records, strict=True):
        assert row['ts_code'] == code
        assert row['trade_date'] == day
        assert row['score_percentile'] == round(float(pct), 6)

    path = tmp_path / 'candidates.csv'
    _write_csv(path, records, CANDIDATE_COLUMNS)
    back = _read_chunk_candidates(path)
    assert len(back) == stats['candidate_signals']
    for row, got in zip(records, back, strict=True):
        assert got['ts_code'] == row['ts_code']
        assert str(got['trade_date'])[:10] == row['trade_date'].isoformat()
        assert abs(float(got['score_percentile'])
                   - row['score_percentile']) < 1e-12
        assert got['reason'] == row['reason']
    # 落盘行数与该片计数一致 → 合并期对拍通过
    verify_candidate_rows('2026', len(back), stats['candidate_signals'])


def test_empty_chunk_writes_header_and_reads_back_as_zero(tmp_path: Path) -> None:
    """零候选分片：只写表头，读回 0 行，对拍仍成立。"""
    path = tmp_path / 'candidates.csv'
    _write_csv(path, [], CANDIDATE_COLUMNS)
    assert path.exists()
    assert _read_chunk_candidates(path) == []
    verify_candidate_rows('empty', 0, 0)


def test_row_count_mismatch_aborts(tmp_path: Path) -> None:
    """DG-31 反例：少写一行/多算一个信号必须被对拍拦住，不能空过。"""
    tier, config = _tier_config()
    records = candidate_records_for_day(
        [('600519.SH', 0.99)], date(2026, 11, 4), candidate_reason(tier, config))
    path = tmp_path / 'candidates.csv'
    _write_csv(path, records, CANDIDATE_COLUMNS)
    recorded = len(_read_chunk_candidates(path))
    # 内存说 2 个信号、磁盘只有 1 行 → 必须中止
    with pytest.raises(SystemExit, match='不一致'):
        verify_candidate_rows('2026', recorded, 2)
    # 反之磁盘比内存多也必须中止
    with pytest.raises(SystemExit, match='不一致'):
        verify_candidate_rows('2026', recorded + 1, 1)


def test_reason_switches_with_soft_filter() -> None:
    """命中理由随软筛开关变化，避免把 soft_pass 写死在理由里。"""
    on = candidate_reason(*_tier_config(soft=True))
    off = candidate_reason(*_tier_config(soft=False))
    assert 'soft_pass' in on
    assert 'soft_pass' not in off
    assert off == 'pct>=0.95;eligibility_ok;entry_allowed'
