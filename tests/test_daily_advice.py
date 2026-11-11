#!/usr/bin/env python3

"""每日入口（tools/daily_advice.py）：数据 stale / 数据不足 / 门禁拦截三种情形。"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pytest

from src.data.universe_filter import (
    RULES_VERSION,
    UniverseFilterConfig,
    UniverseFilterResult,
)
from tools.daily_advice import (
    EXIT_NO_DATA,
    EXIT_OK,
    EXIT_STALE,
    main,
)

AS_OF = date(2026, 11, 9)


@dataclass
class FakeRow:
    ts_code: str
    name: str
    score: float
    score_percentile: float
    direction: str
    disposition: str = 'normal'
    risk_notes: list[str] = field(default_factory=list)
    top_factors: list[tuple[str, float]] = field(default_factory=list)
    horizon: str = 'mid'
    validity_days: int = 20
    defer_until: object = None
    defer_reason: str = ''


@dataclass
class FakeAdvice:
    label: str
    candidates: int
    turnover: float
    filter_stats: dict
    notes: list[str]
    rows: list[FakeRow]


@dataclass
class FakeResult:
    signal_date: date
    trade_date: date
    earliest_exec_date: date
    exec_status: str
    exec_date_basis: str
    equity: float
    tiers: dict


def _fresh(blocking: bool = False) -> dict:
    return {
        'reference': AS_OF.isoformat(), 'label': 'A股日线行情',
        'latest': '2026-11-06' if not blocking else '2026-10-20',
        'severity': 'critical' if blocking else 'ok',
        'lag_days': 5 if blocking else 1,
        'reasons': ['行情落后 5 个自然日'] if blocking else [],
        'alerts': [], 'blocking': blocking,
    }


def _universe(kept: list[str]) -> UniverseFilterResult:
    dropped = {'st': ['600900.SH'] if kept else [], 'board': ['920001.BJ']}
    return UniverseFilterResult(as_of=AS_OF, input_codes=kept + ['600900.SH'],
                                kept=list(kept), dropped=dropped,
                                config=UniverseFilterConfig())


def _engine(blocked: bool, rows: list[FakeRow]):
    advice = FakeAdvice(label='中期', candidates=10, turnover=0.1,
                        filter_stats={'ic_gate_blocked': blocked},
                        notes=['IC监控(20日): 连续3窗 RankIC<0'] if blocked else [],
                        rows=rows)
    result = FakeResult(signal_date=AS_OF, trade_date=AS_OF,
                        earliest_exec_date=AS_OF, exec_status='待次日开盘',
                        exec_date_basis='T+1 开盘', equity=1_000_000.0,
                        tiers={'mid': advice})
    return lambda config: type('E', (), {'generate': lambda self, **kw: result})()


def _row(code: str, direction: str) -> FakeRow:
    return FakeRow(ts_code=code, name=code, score=0.8, score_percentile=0.9,
                   direction=direction,
                   top_factors=[('momentum_20', 1.2), ('roe', 0.4)])


def test_stale_data_blocks_output(tmp_path: Path) -> None:
    out = tmp_path / 'reports'
    code = main(['--as-of', AS_OF.isoformat(), '--out-root', str(out),
                 '--store-root', str(tmp_path / 'store')],
                freshness_probe=lambda *_: _fresh(blocking=True),
                engine_factory=_engine(False, [_row('600000.SH', 'BUY')]),
                universe_builder=lambda *_: (AS_OF, _universe(['600000.SH'])))
    assert code == EXIT_STALE
    assert not list(out.rglob('daily_advice.md'))


def test_stale_can_be_overridden(tmp_path: Path) -> None:
    out = tmp_path / 'reports'
    code = main(['--as-of', AS_OF.isoformat(), '--out-root', str(out),
                 '--store-root', str(tmp_path / 'store'), '--allow-stale'],
                freshness_probe=lambda *_: _fresh(blocking=True),
                engine_factory=_engine(False, [_row('600000.SH', 'BUY')]),
                universe_builder=lambda *_: (AS_OF, _universe(['600000.SH'])))
    assert code == EXIT_OK
    markdown = (out / '20261109' / 'daily_advice.md').read_text(encoding='utf-8')
    assert '[STALE]' in markdown


def test_no_universe_left_reports_insufficient_data(tmp_path: Path) -> None:
    code = main(['--as-of', AS_OF.isoformat(), '--out-root', str(tmp_path / 'reports'),
                 '--store-root', str(tmp_path / 'store')],
                freshness_probe=lambda *_: _fresh(),
                engine_factory=_engine(False, [_row('600000.SH', 'BUY')]),
                universe_builder=lambda *_: (AS_OF, _universe([])))
    assert code == EXIT_NO_DATA


def test_ic_gate_blocked_is_visible_in_output(tmp_path: Path) -> None:
    out = tmp_path / 'reports'
    code = main(['--as-of', AS_OF.isoformat(), '--out-root', str(out),
                 '--store-root', str(tmp_path / 'store')],
                freshness_probe=lambda *_: _fresh(),
                engine_factory=_engine(True, [_row('600000.SH', 'HOLD')]),
                universe_builder=lambda *_: (AS_OF, _universe(['600000.SH'])))
    assert code == EXIT_OK
    markdown = (out / '20261109' / 'daily_advice.md').read_text(encoding='utf-8')
    assert 'IC门禁' in markdown
    assert '选池规则版本' in markdown
    payload = json.loads((out / '20261109' / 'daily_advice.json').read_text(encoding='utf-8'))
    assert payload['universe_filter']['rule_counts']['st'] == 1
    assert payload['tiers']['mid']['ic_gate_blocked'] is True


def test_empty_panel_reports_insufficient_data(tmp_path: Path) -> None:
    code = main(['--as-of', AS_OF.isoformat(), '--out-root', str(tmp_path / 'reports'),
                 '--store-root', str(tmp_path / 'store')],
                freshness_probe=lambda *_: _fresh(),
                engine_factory=_engine(False, []),
                universe_builder=lambda *_: (AS_OF, _universe(['600000.SH'])))
    assert code == EXIT_NO_DATA


def test_available_counts_series_written_and_idempotent(tmp_path: Path) -> None:
    """可买只数序列：按 (信号日, 档位) 落 CSV，重复跑同一日不重复追加。"""
    out = tmp_path / 'reports'
    stats = {'ic_gate_blocked': 0, 'funnel_panel_rows': 4965,
             'funnel_soft_pass': 241, 'funnel_score_pass': 498,
             'funnel_candidates': 0, 'funnel_buys': 0, 'funnel_holds': 15,
             'funnel_sells': 0, 'funnel_avoid': 10}
    advice = FakeAdvice(label='中期', candidates=0, turnover=0.0,
                        filter_stats=stats, notes=[],
                        rows=[_row('600000.SH', 'HOLD')])
    result = FakeResult(signal_date=AS_OF, trade_date=AS_OF,
                        earliest_exec_date=AS_OF, exec_status='待次日开盘',
                        exec_date_basis='T+1 开盘', equity=1_000_000.0,
                        tiers={'mid': advice})
    def factory(config):  # noqa: ARG001
        return type('E', (), {'generate': lambda self, **kw: result})()
    for _ in range(2):
        code = main(['--as-of', AS_OF.isoformat(), '--out-root', str(out),
                     '--store-root', str(tmp_path / 'store')],
                    freshness_probe=lambda *_: _fresh(),
                    engine_factory=factory,
                    universe_builder=lambda *_: (AS_OF, _universe(['600000.SH'])))
        assert code == EXIT_OK
    rows = list(csv.DictReader((out / 'available_counts.csv').open(
        encoding='utf-8', newline='')))
    assert len(rows) == 1
    row = rows[0]
    assert row['signal_date'] == '2026-11-09' and row['tier'] == 'mid'
    assert row['universe_kept'] == '1'
    assert row['score_pass'] == '498'  # noqa: S105
    assert row['all_gates_pass'] == '0'  # noqa: S105
    assert row['new'] == '0' and row['hold'] == '15'
    assert row['rules_version'] == RULES_VERSION


if __name__ == '__main__':
    raise SystemExit(pytest.main([__file__]))
