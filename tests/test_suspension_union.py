"""停牌并集（缺口型 + 占位 bar 型）与准入判据的回归测试。"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.data.quality.clean_set import CodeInfo, EligibilityEngine  # noqa: E402
from src.data.quality.suspension_union import load_suspension_union  # noqa: E402


def _write_store(root: Path, gap: pd.DataFrame, implied: pd.DataFrame) -> Path:
    meta = root / 'meta' / 'suspension'
    meta.mkdir(parents=True, exist_ok=True)
    gap.to_parquet(meta / 'suspension_daily.parquet', index=False)
    implied.to_parquet(meta / 'implied_suspension.parquet', index=False)
    return root


def _frame(pairs: list[tuple[str, str]]) -> pd.DataFrame:
    return pd.DataFrame({'ts_code': [p[0] for p in pairs],
                         'trade_date': pd.to_datetime([p[1] for p in pairs])})


def test_placeholder_bar_day_is_suspended(tmp_path: Path) -> None:
    """① 占位 bar 日期（既有停牌表未登记）应判为停牌。"""
    store = _write_store(
        tmp_path,
        _frame([('300280.SZ', '2022-06-06')]),
        _frame([('300280.SZ', '2022-06-07')]),
    )
    union = load_suspension_union(store)
    assert union.is_suspended('300280.SZ', date(2022, 6, 7)) is True

    engine = object.__new__(EligibilityEngine)
    info = CodeInfo(ts_code='300280.SZ', data_type='stock', rows=2,
                    start=date(2022, 6, 6), end=date(2022, 6, 8),
                    start_ord=date(2022, 6, 6).toordinal(),
                    end_ord=date(2022, 6, 8).toordinal(),
                    quote_dates=np.array([date(2022, 6, 6).toordinal(),
                                          date(2022, 6, 7).toordinal()], dtype=np.int64),
                    date_suspended={date(2022, 6, 7).toordinal()})
    reasons = engine._availability_reasons(info, date(2022, 6, 7).toordinal())
    assert reasons == ['temporal_gap_or_suspension']


def test_normal_day_unaffected(tmp_path: Path) -> None:
    """② 正常交易日不受影响，判据语义不变。"""
    store = _write_store(
        tmp_path,
        _frame([('300280.SZ', '2022-06-06')]),
        _frame([('300280.SZ', '2022-06-07')]),
    )
    union = load_suspension_union(store)
    assert union.is_suspended('300280.SZ', date(2022, 6, 8)) is False
    assert union.is_suspended('000001.SZ', date(2022, 6, 7)) is False
    assert union.is_suspended('300280.SZ', None) is False

    engine = object.__new__(EligibilityEngine)
    normal = date(2022, 6, 8).toordinal()
    info = CodeInfo(ts_code='300280.SZ', data_type='stock', rows=3,
                    start=date(2022, 6, 6), end=date(2022, 6, 8),
                    start_ord=date(2022, 6, 6).toordinal(),
                    end_ord=date(2022, 6, 8).toordinal(),
                    quote_dates=np.array([date(2022, 6, 6).toordinal(),
                                          date(2022, 6, 7).toordinal(), normal],
                                         dtype=np.int64),
                    date_suspended={date(2022, 6, 7).toordinal()})
    assert engine._availability_reasons(info, normal) == []


def test_union_does_not_double_count(tmp_path: Path) -> None:
    """③ 同一 (ts_code, trade_date) 同时出现在两表时只计一次，来源标 both。"""
    store = _write_store(
        tmp_path,
        _frame([('300280.SZ', '2022-06-06'), ('300280.SZ', '2022-06-07')]),
        _frame([('300280.SZ', '2022-06-07'), ('300280.SZ', '2022-06-09')]),
    )
    union = load_suspension_union(store)
    assert len(union.frame) == 3
    assert union.frame.duplicated(['ts_code', 'trade_date']).sum() == 0
    assert union.dates_for('300280.SZ') == {date(2022, 6, 6), date(2022, 6, 7),
                                            date(2022, 6, 9)}
    row = union.frame[union.frame['trade_date'] == date(2022, 6, 7)].iloc[0]
    assert row['source'] == 'both'
    assert union.stats == {'rows': 3, 'codes': 1, 'gap_only': 1,
                           'implied_bar_only': 1, 'both': 1}


def test_missing_files_are_tolerated(tmp_path: Path) -> None:
    """缺表时不抛错，并集为空（不阻断准入索引构建）。"""
    union = load_suspension_union(tmp_path)
    assert len(union.frame) == 0
    assert union.is_suspended('300280.SZ', date(2022, 6, 7)) is False


def test_rebutted_gap_row_is_not_suspended(tmp_path: Path) -> None:
    """is_suspended=false 的留档行（缺口候选被真实 bar 推翻）不进停牌并集。"""
    gap = pd.DataFrame({
        'ts_code': ['300280.SZ', '300280.SZ'],
        'trade_date': pd.to_datetime(['2022-06-06', '2022-06-07']),
        'is_suspended': [True, False],
    })
    store = _write_store(tmp_path, gap, _frame([]))
    union = load_suspension_union(store)
    assert union.is_suspended('300280.SZ', date(2022, 6, 6)) is True
    assert union.is_suspended('300280.SZ', date(2022, 6, 7)) is False
    assert len(union.frame) == 1
    assert union.stats['rows'] == 1
