"""基金行情「非成交价」行级门控（只读）。

背景
----
``data/local_store/quotes/fund/**`` 里存在两类不能当成交价用的行：
上市前或无场内交易期间的**净值序列**（``close`` 为净值×100/×1000，``volume=0``），
以及源端填的**占位桩**（``close`` 恒为 1.0）。另有折算日残留行（价格停在被折算前的量级）。
行本身按数据红线不删除，只在读取侧过滤。

侧车
----
- ``meta/anomalies/placeholder_quote_codes.csv``：区间清单（``ts_code,start,end,rows,total,full``）
- ``meta/anomalies/fund_pseudo_registry.json``：逐区间类别（``nav_series`` / ``placeholder`` /
  ``converted_leftover``），由 ``2026-11-12/gap-rootcause/work/apply_b_registry.py`` 生成

两个文件任意一个缺失、或某区间在 JSON 里没有类别，一律抛 :class:`PseudoRegistryError`，
**不允许静默放行**（DG-27 的反面：不能把「未接线」当「无数据」，也不能把「查不到清单」当「都是真数据」）。

用法::

    from src.data.quality.fund_price_basis import filter_tradable, is_pseudo
    frame = filter_tradable(frame, root, '160311.SZ')      # 默认开启
    frame = filter_tradable(frame, root, '160311.SZ', enabled=False)  # 复现旧行为
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import polars as pl

KINDS = ('nav_series', 'placeholder', 'converted_leftover')
CSV_NAME = 'placeholder_quote_codes.csv'
JSON_NAME = 'fund_pseudo_registry.json'


class PseudoRegistryError(RuntimeError):
    """侧车清单缺失或与 JSON 类别不同步。"""


@dataclass(frozen=True)
class PseudoSpan:
    ts_code: str
    start: date
    end: date
    kind: str
    rows: int

    def contains(self, day: date) -> bool:
        return self.start <= day <= self.end


_CACHE: dict[str, dict[str, list[PseudoSpan]]] = {}


def registry_paths(root: Path | str) -> tuple[Path, Path]:
    base = Path(root) / 'meta' / 'anomalies'
    return base / CSV_NAME, base / JSON_NAME


def clear_cache() -> None:
    """测试或清单更新后重置缓存。"""
    _CACHE.clear()


def load_spans(root: Path | str) -> dict[str, list[PseudoSpan]]:
    key = str(Path(root).resolve())
    if key in _CACHE:
        return _CACHE[key]
    csv_path, json_path = registry_paths(root)
    for path in (csv_path, json_path):
        if not path.is_file():
            raise PseudoRegistryError(f'缺少基金伪数据侧车清单: {path}')
    kinds = json.loads(json_path.read_text(encoding='utf-8')).get('kinds') or {}
    spans: dict[str, list[PseudoSpan]] = {}
    with csv_path.open(encoding='utf-8', newline='') as handle:
        for row in csv.DictReader(handle):
            span_key = '|'.join([row['ts_code'], row['start'], row['end']])
            kind = kinds.get(span_key)
            if kind not in KINDS:
                raise PseudoRegistryError(f'区间 {span_key} 缺少合法类别（实际 {kind!r}）')
            spans.setdefault(row['ts_code'], []).append(PseudoSpan(
                ts_code=row['ts_code'], start=date.fromisoformat(row['start']),
                end=date.fromisoformat(row['end']), kind=kind, rows=int(row['rows'])))
    _CACHE[key] = spans
    return spans


def spans_for(root: Path | str, ts_code: str,
              kinds: tuple[str, ...] | None = None) -> list[PseudoSpan]:
    found = load_spans(root).get(ts_code, [])
    return [s for s in found if kinds is None or s.kind in kinds]


def is_pseudo(root: Path | str, ts_code: str, day: date,
              kinds: tuple[str, ...] | None = None) -> bool:
    return any(s.contains(day) for s in spans_for(root, ts_code, kinds))


def first_tradable_day(root: Path | str, ts_code: str) -> date | None:
    """首个不在任何伪数据区间内的交易日；没有侧车记录时返回 None。"""
    spans = spans_for(root, ts_code)
    if not spans:
        return None
    return max(s.end for s in spans) + timedelta(days=1)


def filter_tradable(frame: pl.DataFrame, root: Path | str, ts_code: str,
                    kinds: tuple[str, ...] | None = None,
                    enabled: bool = True) -> pl.DataFrame:
    """剔除伪数据区间内的行；``enabled=False`` 原样返回（复现旧行为）。"""
    if not enabled:
        return frame
    spans = spans_for(root, ts_code, kinds)
    if not spans or 'date' not in frame.columns:
        return frame
    keep = pl.lit(True)
    for span in spans:
        keep = keep & ~pl.col('date').is_between(span.start, span.end)
    return frame.filter(keep)
