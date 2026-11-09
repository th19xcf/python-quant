"""停牌并集：把「缺口型停牌」与「占位 bar 型停牌」合成单一可交易判据。

两套来源
--------
- ``meta/suspension/suspension_daily.parquet``：由「交易日历 − 实际有 bar」反推，
  只登记**没有 K 线**的停牌日（``derived_from_quote_gap``）。
- ``meta/suspension/implied_suspension.parquet``：源端在停牌期仍写出 K 线，
  但 ``volume/amount`` 为 NULL、四价相等，属**占位 bar**；这类日期不在上一张表里。

``load_suspension_union`` 把两者按键求并集，重复键只保留一条（来源记为
``both``），因此不会重复计数。判定语义不变：只补上原先漏掉的日子。
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_STORE = REPO_ROOT / 'data' / 'local_store'

SUSPENSION_FILES = (
    ('gap', 'suspension_daily.parquet'),
    ('implied_bar', 'implied_suspension.parquet'),
)


class SuspensionUnion:
    """``(ts_code, trade_date)`` 停牌并集，提供 O(1) 判定。"""

    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame
        self._codes: dict[str, set[date]] = {}
        for code, day in zip(frame['ts_code'].astype(str), frame['trade_date'],
                             strict=True):
            self._codes.setdefault(code, set()).add(day)

    def is_suspended(self, ts_code: str, trade_date: date | str | None) -> bool:
        """该标的在该日是否停牌（两套来源任一命中即返回 True）。"""
        if trade_date is None or not ts_code:
            return False
        day = trade_date if isinstance(trade_date, date) else date.fromisoformat(str(trade_date)[:10])
        return day in self._codes.get(str(ts_code), ())

    def dates_for(self, ts_code: str) -> set[date]:
        """该标的的全部停牌日；无记录返回空集合。"""
        return set(self._codes.get(str(ts_code), ()))

    @property
    def stats(self) -> dict[str, int]:
        """行数、标的数与各来源命中数，便于日志与核对。"""
        src = self.frame['source']
        return {
            'rows': int(len(self.frame)),
            'codes': int(self.frame['ts_code'].nunique()),
            'gap_only': int((src == 'gap').sum()),
            'implied_bar_only': int((src == 'implied_bar').sum()),
            'both': int((src == 'both').sum()),
        }


def _read(path: Path) -> pd.DataFrame | None:
    if not path.is_file():
        return None
    df = pd.read_parquet(path, columns=['ts_code', 'trade_date'])
    df['ts_code'] = df['ts_code'].astype(str)
    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
    return df.drop_duplicates(['ts_code', 'trade_date'])


def load_suspension_union(store_root: str | Path | None = None) -> SuspensionUnion:
    """读取两套停牌来源并求并集；缺失的文件按空表处理，不抛错。

    返回的 ``frame`` 列：``ts_code``、``trade_date``、``source``
    （``gap`` / ``implied_bar`` / ``both``）。
    """
    store = Path(store_root) if store_root is not None else DEFAULT_STORE
    parts: list[pd.DataFrame] = []
    for label, name in SUSPENSION_FILES:
        df = _read(store / 'meta' / 'suspension' / name)
        if df is not None:
            parts.append(df.assign(source=label))
    if not parts:
        merged = pd.DataFrame({'ts_code': pd.Series(dtype=str),
                               'trade_date': pd.Series(dtype=object),
                               'source': pd.Series(dtype=str)})
        return SuspensionUnion(merged)
    all_rows = pd.concat(parts, ignore_index=True)
    merged = (all_rows.groupby(['ts_code', 'trade_date'], as_index=False)['source']
              .agg(lambda s: s.iloc[0] if s.nunique() == 1 else 'both'))
    merged = merged.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)
    return SuspensionUnion(merged)
