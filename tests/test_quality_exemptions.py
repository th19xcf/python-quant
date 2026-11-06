#!/usr/bin/env python3

"""已知违规基线：精确豁免、过期失效、新增回归仍阻断门禁。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import polars as pl
import pytest

from src.data.quality.checker import QualityChecker
from src.data.quality.exemptions import (
    ExemptionIndex,
    ExemptionRecord,
    build_baseline,
    normalize_trade_date,
    read_exemptions,
    write_exemptions,
)


@dataclass
class _Violation:
    """duck-typed 违规对象，避免依赖 QualityViolation 构造细节。"""

    rule_id: str
    severity: str
    ts_code: str
    trade_date: date | None


class _FakeSource:
    def __init__(self, frames: dict[str, dict[str, pl.DataFrame]],
                 name: str = 'local') -> None:
        self.name = name
        self._frames = frames

    def codes(self, data_type: str) -> list[str]:
        return sorted(self._frames.get(data_type, {}))

    def load(self, data_type: str, ts_code: str) -> pl.DataFrame | None:
        return self._frames.get(data_type, {}).get(ts_code)


def _frame(day: date) -> pl.DataFrame:
    """一行契约自洽行情，仅 volume/amount 为空（复现退市股尾部瑕疵）。"""
    return pl.DataFrame({
        'date': pl.Series([day], dtype=pl.Date),
        'open': pl.Series([10.0], dtype=pl.Float64),
        'high': pl.Series([10.5], dtype=pl.Float64),
        'low': pl.Series([9.5], dtype=pl.Float64),
        'close': pl.Series([10.0], dtype=pl.Float64),
        'pre_close': pl.Series([10.0], dtype=pl.Float64),
        'change': pl.Series([0.0], dtype=pl.Float64),
        'pct_chg': pl.Series([0.0], dtype=pl.Float64),
        'volume': pl.Series([None], dtype=pl.Float64),
        'amount': pl.Series([None], dtype=pl.Float64),
        'qfq_factor': pl.Series([1.0], dtype=pl.Float64),
    })


def test_normalize_trade_date() -> None:
    assert normalize_trade_date(date(2024, 1, 2)) == '2024-01-02'
    assert normalize_trade_date('2024-01-02') == '2024-01-02'
    assert normalize_trade_date(None) == '*'
    assert normalize_trade_date('') == '*'


def test_build_baseline_only_errors_and_deduplicates() -> None:
    violations = [
        _Violation('volume_invalid', 'error', '600000.SH', date(2024, 1, 2)),
        _Violation('volume_invalid', 'error', '600000.SH', date(2024, 1, 2)),
        _Violation('amount_dimension', 'warning', '600000.SH', date(2024, 1, 3)),
    ]
    records = build_baseline(violations, reason='历史源端瑕疵')
    assert len(records) == 1
    assert records[0].key == ('volume_invalid', '600000.SH', '2024-01-02')
    assert records[0].reason == '历史源端瑕疵'


def test_build_baseline_requires_reason() -> None:
    with pytest.raises(ValueError, match='reason'):
        build_baseline([], reason='   ')


def test_read_write_roundtrip(tmp_path) -> None:
    path = tmp_path / 'nested' / 'exemptions.csv'
    records = [
        ExemptionRecord('volume_invalid', '600000.SH', '2024-01-02', 'r', 't'),
        ExemptionRecord('required_columns', '600000.SH', '2024-01-02', 'r', 't'),
        ExemptionRecord('volume_invalid', '600000.SH', '2024-01-02', 'r', 't'),
    ]
    assert write_exemptions(records, path) == 2
    loaded = read_exemptions(path)
    assert len(loaded) == 2
    assert {item.key for item in loaded} == {item.key for item in records}
    assert not path.with_name(path.name + '.tmp').exists()
    assert read_exemptions(tmp_path / 'missing.csv') == ()


def test_index_is_precise_and_honours_expiry(tmp_path) -> None:
    path = tmp_path / 'exemptions.csv'
    expired = (date.today() - timedelta(days=1)).isoformat()
    write_exemptions([
        ExemptionRecord('volume_invalid', '600000.SH', '2024-01-02', 'r', 't'),
        ExemptionRecord('volume_invalid', '600000.SH', '2024-01-03',
                        'r', 't', expires=expired),
    ], path)
    index = ExemptionIndex(path)
    assert index.size == 1
    assert index.is_exempt('volume_invalid', '600000.sh', date(2024, 1, 2))
    assert not index.is_exempt('volume_invalid', '600000.SH', date(2024, 1, 3))
    assert not index.is_exempt('volume_invalid', '600000.SH', date(2024, 1, 4))
    assert not index.is_exempt('low_gt_open_close', '600000.SH', date(2024, 1, 2))
    assert not index.is_exempt('volume_invalid', '000001.SZ', date(2024, 1, 2))


def test_exempt_errors_do_not_hide_new_regression(tmp_path, monkeypatch) -> None:
    path = tmp_path / 'exemptions.csv'
    write_exemptions([
        ExemptionRecord('volume_invalid', '600000.SH', '2024-01-02', 'r', 't'),
    ], path)
    index = ExemptionIndex(path)
    monkeypatch.setattr('src.data.quality.checker.is_exempt', index.is_exempt)

    known = QualityChecker(
        source=_FakeSource({'stock': {'600000.SH': _frame(date(2024, 1, 2))}}),
        latest_trade_date=date(2024, 2, 1),
    ).run(data_types=('stock',), write_report=False)
    errors = [item for item in known.violations if item.severity == 'error']
    assert errors, '构造的 frame 必须产生 error 级违规'
    assert known.exempt_errors == 1
    assert known.actionable_errors == len(errors) - 1
    assert sum(1 for item in errors if item.exempt) == 1
    assert known.by_severity.get('error') == len(errors), '违规仍须完整记录'

    regression = QualityChecker(
        source=_FakeSource({'stock': {'600000.SH': _frame(date(2024, 1, 3))}}),
        latest_trade_date=date(2024, 2, 1),
    ).run(data_types=('stock',), write_report=False)
    assert regression.exempt_errors == 0, '新日期不得被历史基线豁免'
    assert regression.actionable_errors == len(
        [item for item in regression.violations if item.severity == 'error']
    )
    assert regression.actionable_errors > 0
