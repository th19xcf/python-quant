"""停牌并集：把「缺口型停牌」与「占位 bar 型停牌」合成单一可交易判据。

两套来源
--------
- ``meta/suspension/suspension_daily.parquet``：由「交易日历 − 实际有 bar」反推，
  只登记**没有 K 线**的停牌日（``derived_from_quote_gap``）。2026-11-12 起该表
  允许保留 ``is_suspended=false`` 的留档行（缺口候选被真实 bar 推翻），并集只采纳
  ``is_suspended != False`` 的行。
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

#: 隔离行清单：与 ``implied_suspension`` 同源同批行（源端 volume/amount 双 NULL），
#: 但语义是「读侧剔除」而不是「判为停牌」，因此是独立正式路径，不是重复登记。
QUARANTINE_FILE = ('meta', 'anomalies', 'quarantine_rows.parquet')


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
    import pyarrow.parquet as pq  # noqa: PLC0415 - 仅在此处需要探测列

    columns = ['ts_code', 'trade_date']
    if 'is_suspended' in pq.read_schema(path).names:
        columns.append('is_suspended')
    df = pd.read_parquet(path, columns=columns)
    if 'is_suspended' in df.columns:
        # 只采纳判定为停牌的行；is_suspended=false 是「缺口候选被真实 bar 推翻」的
        # 留档行（2026-11-12 起），不得进入停牌并集。空值按停牌处理（历史行为不变）。
        df = df[df['is_suspended'].fillna(True).astype(bool)]
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


def verify_quarantine_alignment(store_root: str | Path | None = None) -> dict[str, int]:
    """校验 ``implied_suspension`` 与 ``quarantine_rows`` 的键集相等。

    两表同源（源端 ``volume``/``amount`` 双 NULL 的行）但语义不同：前者进停牌并集
    做可交易性判定，后者进行情读侧做坏行剔除。二者由不同脚本各自产出、互不联动，
    任何一侧单独重放都会让键集分叉——坏行被当成停牌，或坏行回流行情读侧，两种都
    不报错，只会让回测样本静默偏移。故此处显式校验，键集不等即抛错。

    Args:
        store_root: 列存根目录，默认 ``data/local_store``。

    Returns:
        ``{'implied_rows', 'quarantine_rows', 'only_implied', 'only_quarantine'}``，
        键集相等时后两项均为 0。

    Raises:
        ValueError: 键集不等，或两表只有一侧存在（无法校验）。
    """
    store = Path(store_root) if store_root is not None else DEFAULT_STORE
    implied = _read(store / 'meta' / 'suspension' / 'implied_suspension.parquet')
    quarantine = _read(store.joinpath(*QUARANTINE_FILE))
    if (implied is None) != (quarantine is None):
        present = 'implied_suspension' if implied is not None else 'quarantine_rows'
        raise ValueError(f'隔离台账对齐校验失败：只有 {present} 存在，另一侧缺失')
    if implied is None or quarantine is None:
        return {'implied_rows': 0, 'quarantine_rows': 0,
                'only_implied': 0, 'only_quarantine': 0}

    implied_keys = set(zip(implied['ts_code'], implied['trade_date'], strict=True))
    quarantine_keys = set(zip(quarantine['ts_code'], quarantine['trade_date'], strict=True))
    only_implied = implied_keys - quarantine_keys
    only_quarantine = quarantine_keys - implied_keys
    if only_implied or only_quarantine:
        raise ValueError(
            '隔离台账对齐校验失败：implied_suspension 与 quarantine_rows 键集不等 '
            f'(implied {len(implied_keys)} 行 / quarantine {len(quarantine_keys)} 行；'
            f'仅 implied {len(only_implied)} 行，仅 quarantine {len(only_quarantine)} 行)。'
            f'样例 仅 implied={sorted(only_implied)[:5]}；'
            f'仅 quarantine={sorted(only_quarantine)[:5]}'
        )
    return {'implied_rows': len(implied_keys), 'quarantine_rows': len(quarantine_keys),
            'only_implied': 0, 'only_quarantine': 0}

