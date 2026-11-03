#!/usr/bin/env python3

"""腾讯分钟 K 线取数（显式 CLI，只读，不写库）。

硬限制与契约（实测 2026-11-03）：

- 每档 **最多 320 根**，不支持翻页回溯；m1 约 1.3 个交易日，m5 约 20 个交易日。
- 字段：时间 / open / close / high / low / volume，第 7 列为空对象 → **无成交额**。
- volume 单位为手（m5 全天 48 根加总与日线成交量逐日对拍确认）；科创板
  （688/689）接口返回「股」，已在 :mod:`src.data.tencent.client` 内按
  100 股/手换算后再返回。

因此：

1. **禁止用 ``close * volume`` 反推成交额**（本项目明确要求不伪造）；
2. ``stock_minute`` 若要求成交额，则腾讯分钟线**不能入库**；本工具只输出
   取数结果与字段结论，不写数据库；
3. 若未来接入其它「量单位=股」的分钟源，必须走
   :func:`normalize_volume` 显式换算（已用测试锁定）。
"""

from __future__ import annotations

import csv
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, cast

import polars as pl
from loguru import logger

from src.data.tencent.client import (
    BARS_PER_DAY,
    MAX_MINUTE_BARS,
    MINUTE_FREQUENCIES,
    TencentClient,
    TencentError,
)
from src.utils.code_converter import to_ts_code

#: 项目 ``stock_minute`` 表契约：成交量单位为「手」
TARGET_VOLUME_UNIT = 'hand'
_VOLUME_UNIT_DIVISOR = {'hand': 1.0, 'share': 100.0}


@dataclass(frozen=True)
class MinuteFetchResult:
    ts_code: str
    freq: str
    rows: int
    first: datetime | None
    last: datetime | None
    span_days_est: float
    has_amount: bool
    volume_unit: str
    elapsed_s: float
    csv_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            'ts_code': self.ts_code, 'freq': self.freq, 'rows': self.rows,
            'first': self.first.isoformat(sep=' ') if self.first else None,
            'last': self.last.isoformat(sep=' ') if self.last else None,
            'span_days_est': round(self.span_days_est, 2),
            'has_amount': self.has_amount, 'volume_unit': self.volume_unit,
            'elapsed_s': round(self.elapsed_s, 3), 'csv_path': self.csv_path,
        }


def normalize_volume(frame: pl.DataFrame, source_unit: str = 'hand') -> pl.DataFrame:
    """把成交量显式换算到项目契约（手）。

    Args:
        source_unit: ``hand``（原样）或 ``share``（股，按 100 股=1 手换算）

    Raises:
        ValueError: 未知单位（不做静默猜测）
    """
    if source_unit not in _VOLUME_UNIT_DIVISOR:
        raise ValueError(f'未知成交量单位: {source_unit!r}（可选 hand/share）')
    divisor = _VOLUME_UNIT_DIVISOR[source_unit]
    if divisor == 1.0 or frame.is_empty():
        return frame
    return frame.with_columns((pl.col('volume') / divisor).alias('volume'))


def _estimate_span_days(rows: int, freq: str) -> float:
    bars = BARS_PER_DAY.get(freq)
    return (rows / bars) if bars else 0.0


def fetch_minute_batch(client: TencentClient, codes: Iterable[str], freq: str = 'm1',
                       count: int = MAX_MINUTE_BARS, *,
                       source_unit: str = 'hand',
                       output_dir: Path | str | None = None) -> dict[str, Any]:
    """批量取分钟线并汇总字段契约（不写数据库）。

    Returns:
        汇总字典：每只标的的行数/时间范围/估算覆盖天数/成交额是否可用，
        以及 ``can_ingest_stock_minute`` 的结论与原因。
    """
    if freq not in MINUTE_FREQUENCIES:
        raise ValueError(f'不支持的分钟频率: {freq!r}（可选 {MINUTE_FREQUENCIES}）')
    count = max(1, min(int(count), MAX_MINUTE_BARS))
    if output_dir is not None:
        Path(output_dir).mkdir(parents=True, exist_ok=True)

    started = time.perf_counter()
    items: list[MinuteFetchResult] = []
    failures: list[dict[str, str]] = []
    for raw_code in codes:
        try:
            ts_code = to_ts_code(raw_code)
        except (TypeError, ValueError) as exc:
            failures.append({'code': str(raw_code), 'error': str(exc)})
            continue
        begin = time.perf_counter()
        try:
            frame = client.fetch_minute(ts_code, freq, count)
        except (TencentError, OSError) as exc:
            failures.append({'code': ts_code, 'error': str(exc)[:160]})
            logger.warning(f'[tencent-minute] {ts_code} 取数失败: {exc}')
            continue
        frame = normalize_volume(frame, source_unit)
        elapsed = time.perf_counter() - begin
        csv_path = None
        if output_dir is not None and not frame.is_empty():
            csv_path = str(Path(output_dir) / f'{ts_code}_{freq}.csv')
            frame.write_csv(csv_path)
        items.append(MinuteFetchResult(
            ts_code=ts_code, freq=freq, rows=frame.height,
            first=cast(datetime | None, frame['datetime'].min())
            if not frame.is_empty() else None,
            last=cast(datetime | None, frame['datetime'].max())
            if not frame.is_empty() else None,
            span_days_est=_estimate_span_days(frame.height, freq),
            has_amount=False,
            volume_unit=TARGET_VOLUME_UNIT,
            elapsed_s=elapsed, csv_path=csv_path,
        ))

    has_amount = any(item.has_amount for item in items)
    return {
        'freq': freq,
        'count_requested': count,
        'max_bars_hard_limit': MAX_MINUTE_BARS,
        'items': [item.to_dict() for item in items],
        'failures': failures,
        'total_rows': sum(item.rows for item in items),
        'volume_unit': TARGET_VOLUME_UNIT,
        'amount_available': has_amount,
        'can_ingest_stock_minute': has_amount,
        'ingest_reason': (
            '腾讯分钟接口无成交额（第7列为空对象），按「不伪造」原则不写 stock_minute；'
            '如需入库请改用带 amount 的分钟源'
            if not has_amount else '字段含成交额，可按项目契约入库'
        ),
        'elapsed_s': round(time.perf_counter() - started, 3),
    }


def write_summary_csv(path: Path | str, summary: dict[str, Any]) -> str:
    """把汇总结果落成 CSV（只包含每只标的的行数与时间范围）。"""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fields = ['ts_code', 'freq', 'rows', 'first', 'last', 'span_days_est',
              'has_amount', 'volume_unit', 'elapsed_s', 'csv_path']
    with target.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for item in summary.get('items', []):
            writer.writerow({key: item.get(key) for key in fields})
    return str(target)


def format_minute_brief(summary: dict[str, Any]) -> str:
    """CLI 收尾用的单行结论。"""
    rows = ', '.join(
        f'{item["ts_code"]}={item["rows"]}根({item["first"]}→{item["last"]})'
        for item in summary.get('items', []))
    return (
        f'{summary["freq"]} 每档上限 {summary["max_bars_hard_limit"]} 根；{rows}；'
        f'成交量单位={summary["volume_unit"]}，成交额可用={summary["amount_available"]}；'
        f'能否入库={summary["can_ingest_stock_minute"]}（{summary["ingest_reason"]}）')
