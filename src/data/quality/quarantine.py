#!/usr/bin/env python3

r"""行情行级隔离清单（读侧硬过滤）。

契约巡检里有一批**源端无法修复**的坏行：退市/停更个股尾部的
``volume`` / ``amount`` 全为 null（TDX 对无成交日不落值），OHLC 仍有效。
这些行既不能编造成交量，也不该整行删掉丢掉历史价格，因此由
``tools/repair_quote_anomalies.py`` 精确登记到
``meta/anomalies/quarantine_rows.parquet``，在读侧按
``(ts_code, trade_date)`` 剔除，保证任何下游（含单票回测，组合回测的
PIT 停更池只挡得住池级剔除）都拿不到这些行。

清单不存在时 **fail-open**（原样返回），避免台账缺失拖垮取数；调用方
可通过 ``quarantine_stats()`` 观察是否降级。
"""

from __future__ import annotations

from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any

import polars as pl
from polars.exceptions import PolarsError

from src.data.local_store.paths import default_store_root

QUARANTINE_FILE = 'meta/anomalies/quarantine_rows.parquet'
PLACEHOLDER_FILE = 'meta/anomalies/placeholder_quote_codes.csv'


def quarantine_path(root: str | Path | None = None) -> Path:
    base = Path(root) if root else default_store_root()
    return base / QUARANTINE_FILE


def placeholder_codes(root: str | Path | None = None) -> frozenset[str]:
    """整只价格恒为 1.0 的占位桩基金（池级无条件剔除）。

    判据是**整只全序列**都是 ``close=1.0, volume=0, amount=0``。通达信对
    已退市/转型/长期无成交的 LOF 用 1.0 表示「该日无行情」，价格序列无意义，
    任何收益率都是假的，必须整只挡在回测与推荐之外。

    只取 ``full=True`` 的标的：另有约 135 只基金**仅头部**是占位桩（上市初期
    无净值）、后期有真实行情，整只剔除会误伤真实数据，这些只做行级过滤，
    见 :func:`placeholder_ranges`。

    清单缺失时返回空集（fail-open），不阻断取数。
    """
    path = (Path(root) if root else default_store_root()) / PLACEHOLDER_FILE
    if not path.is_file():
        return frozenset()
    try:
        frame = pl.read_csv(path)
    except (OSError, PolarsError):
        return frozenset()
    if 'full' not in frame.columns:
        # 旧版清单没有 full 列，只登记过「全序列」与「尾部」，按原语义读
        return frozenset(str(c) for c in frame['ts_code'].to_list())
    full = frame.filter(pl.col('full').fill_null(False))
    return frozenset(str(c) for c in full['ts_code'].to_list())


def placeholder_ranges(
    root: str | Path | None = None,
) -> frozenset[tuple[str, date]]:
    """占位桩的**行级**清单：返回 ``(ts_code, trade_date)`` 集合。

    覆盖全部 232 只（含仅头部/中段为占位桩的标的），供读侧逐行剔除，
    不牵连同标的的真实行情。
    """
    path = (Path(root) if root else default_store_root()) / PLACEHOLDER_FILE
    if not path.is_file():
        return frozenset()
    try:
        frame = pl.read_csv(path)
    except (OSError, PolarsError):
        return frozenset()
    out: set[tuple[str, date]] = set()
    for row in frame.iter_rows(named=True):
        code = str(row['ts_code'])
        raw_start, raw_end = row.get('start'), row.get('end')
        if raw_start is None or raw_end is None:
            continue
        start = date.fromisoformat(str(raw_start)[:10])
        end = date.fromisoformat(str(raw_end)[:10])
        span = (end - start).days
        if span < 0:
            continue
        # 区间内逐日登记；跨度为交易日，直接按日历日展开会在长假处多出，
        # 但调用方以行情行是否存在为准，多出的日期不会命中任何行
        for offset in range(span + 1):
            out.add((code, start + timedelta(days=offset)))
    return frozenset(out)


@lru_cache(maxsize=4)
def _index(path: str, mtime_ns: int) -> dict[str, frozenset[date]]:
    """按 path+mtime 缓存，台账更新后自动失效。"""
    frame = pl.read_parquet(path, columns=['ts_code', 'trade_date'])
    out: dict[str, set[date]] = {}
    for row in frame.iter_rows(named=True):
        raw = row['trade_date']
        d = raw if isinstance(raw, date) else date.fromisoformat(str(raw)[:10])
        out.setdefault(str(row['ts_code']), set()).add(d)
    return {code: frozenset(dates) for code, dates in out.items()}


def quarantine_stats(root: str | Path | None = None) -> dict[str, Any]:
    """返回台账状态，供每日门禁/诊断使用。"""
    path = quarantine_path(root)
    if not path.is_file():
        return {'available': False, 'rows': 0, 'codes': 0,
                'reason': f'missing: {path}'}
    rows = _index(str(path), path.stat().st_mtime_ns)
    return {'available': True,
            'rows': sum(len(v) for v in rows.values()),
            'codes': len(rows), 'path': str(path)}


def drop_quarantined(frame: pl.DataFrame, ts_code: str,
                     root: str | Path | None = None) -> pl.DataFrame:
    """剔除该标的被隔离的交易日；台账缺失或无 date 列时原样返回。"""
    if frame is None or frame.is_empty() or 'date' not in frame.columns:
        return frame
    path = quarantine_path(root)
    if not path.is_file():
        return frame
    dates = _index(str(path), path.stat().st_mtime_ns).get(ts_code)
    if not dates:
        return frame
    return frame.filter(~pl.col('date').is_in(list(dates)))
