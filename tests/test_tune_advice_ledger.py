"""`tools/tune_advice.py` 的 Bonferroni 分母口径用例。

背景（2026-11-11）：上一版把分母改成「已用 tuning 次数」，已用 3 次时算出
`α=0.05/3=0.01667`、`t≈2.39`，低于寻优路线图声明的 N=20、`α*=0.0025`、`t*≈3.10`。
分母取已用次数等于边做边降门槛（p-hacking），必须固定为事先声明的计划预算 N=20。

覆盖三个场景：
1. 已用 3 次：算出的 α 仍对应 N=20；
2. 已用 25 次（超预算）：告警，分母仍是 20，不自动涨到 25；
3. `_append_ledger` 的已用次数只计 tuning，verify/diagnostic 不计。
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from tools import tune_advice as T


def _write_ledger(path: Path, purposes: list[str]) -> None:
    rows: list[dict[str, str]] = []
    for idx, purpose in enumerate(purposes):
        row = dict.fromkeys(T.LEDGER_COLUMNS, '')
        row['experiment_id'] = f'exp{idx:02d}'
        row['label'] = f'label{idx:02d}'
        row['hold'] = '20'
        row['purpose'] = purpose
        rows.append(row)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=T.LEDGER_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def test_planned_budget_matches_roadmap_declaration() -> None:
    assert T.PLANNED_TRIALS == 20
    assert T.PLANNED_ALPHA == pytest.approx(0.0025)
    assert T.PLANNED_T_CRIT == pytest.approx(3.10)


def test_three_used_keeps_denominator_at_planned_budget(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str]) -> None:
    ledger = tmp_path / 'experiments.csv'
    _write_ledger(ledger, ['baseline', 'tuning', 'tuning', 'tuning', 'verify'])
    monkeypatch.setattr(T, 'LEDGER_PATH', ledger)
    monkeypatch.setattr(T, 'LEDGER_DIR', tmp_path)

    assert T.cmd_ledger(None) == 0  # type: ignore[arg-type]
    out = capsys.readouterr().out
    assert '计划预算 N=20' in out
    assert 'α*=0.0025' in out
    assert 't*≈3.10' in out
    assert '已用 tuning 次数 3' in out
    assert '不参与门槛计算' in out
    assert '0.05/3' not in out


def test_over_budget_warns_but_denominator_stays(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str]) -> None:
    ledger = tmp_path / 'experiments.csv'
    _write_ledger(ledger, ['tuning'] * 25)
    monkeypatch.setattr(T, 'LEDGER_PATH', ledger)
    monkeypatch.setattr(T, 'LEDGER_DIR', tmp_path)

    assert T.cmd_ledger(None) == 0  # type: ignore[arg-type]
    out = capsys.readouterr().out
    assert '已用 tuning 次数 25' in out
    assert '超过计划预算 N=20' in out
    assert '计划预算 N=20' in out
    assert 'α*=0.0025' in out
    assert '0.05/25' not in out


def test_append_ledger_counts_only_tuning(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ledger = tmp_path / 'experiments.csv'
    _write_ledger(ledger, ['tuning', 'verify', 'diagnostic'])
    monkeypatch.setattr(T, 'LEDGER_PATH', ledger)
    monkeypatch.setattr(T, 'LEDGER_DIR', tmp_path)

    used = T._append_ledger({'experiment_id': 'new', 'purpose': 'tuning'})
    assert used == 2
    with ledger.open('r', encoding='utf-8', newline='') as handle:
        rows = list(csv.DictReader(handle))
    assert rows[-1]['cumulative_attempts'] == '2'
