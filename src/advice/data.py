#!/usr/bin/env python3

"""本地列存行情加载器。

读路径只碰本地 Parquet（``data/local_store/quotes/stock``），不发起远程查询：
- 直接使用 ``LocalQuoteStore.read_file``，绕开覆盖度元数据，避免并行构建期间
  ``meta/daily_index.parquet`` 尚未 flush 时误判为无数据；
- 原始价保留在 ``raw_*`` 列，供 ``ExecutionRules`` 做涨跌停/停牌判定；
- ``open/high/low/close`` 统一为后复权价（后复权 PIT 安全，适合因子与回测），
  ``ref_price`` 使用原始收盘价（人工下单看到的价格）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Iterable

import polars as pl
from loguru import logger

from src.data.local_store.store import LocalQuoteStore, resolve_store_root

_PANEL_COLUMNS = (
    'date', 'ts_code', 'open', 'high', 'low', 'close', 'volume', 'amount',
    'raw_open', 'raw_high', 'raw_low', 'raw_close', 'raw_prev_close',
    'raw_volume', 'raw_amount', 'hfq_factor', 'qfq_factor',
)


@dataclass
class LoadReport:
    """加载结果的可观测信息。"""

    requested: int = 0
    loaded: int = 0
    rows: int = 0
    missing: list[str] = field(default_factory=list)
    skipped_short: list[str] = field(default_factory=list)
    #: 缺复权因子/前收列、被 1.0 或 shift(1) 兜底的标的（价格基准不可信）
    degraded: list[str] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        return {
            'requested': self.requested,
            'loaded': self.loaded,
            'rows': self.rows,
            'missing': len(self.missing),
            'missing_examples': self.missing[:5],
            'skipped_short': len(self.skipped_short),
            'degraded': len(self.degraded),
            'degraded_examples': self.degraded[:5],
        }


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(text[:10] if '-' in text else text, fmt).date()
        except ValueError:
            continue
    return None


def _normalize_frame(frame: pl.DataFrame, ts_code: str) -> pl.DataFrame:
    """统一列名/复权口径，输出加载器的标准列。"""
    if 'date' not in frame.columns:
        return pl.DataFrame()
    frame = frame.with_columns(pl.col('date').cast(pl.Date, strict=False))
    for column in ('open', 'high', 'low', 'close', 'volume', 'amount'):
        if column not in frame.columns:
            return pl.DataFrame()

    for factor_column in ('hfq_factor', 'qfq_factor'):
        if factor_column not in frame.columns:
            frame = frame.with_columns(pl.lit(1.0).alias(factor_column))
        frame = frame.with_columns(
            pl.col(factor_column).cast(pl.Float64, strict=False).fill_null(1.0))

    if 'pre_close' not in frame.columns:
        frame = frame.with_columns(pl.col('close').shift(1).alias('pre_close'))

    return frame.select([
        pl.col('date'),
        pl.lit(ts_code).alias('ts_code'),
        (pl.col('open') * pl.col('hfq_factor')).alias('open'),
        (pl.col('high') * pl.col('hfq_factor')).alias('high'),
        (pl.col('low') * pl.col('hfq_factor')).alias('low'),
        (pl.col('close') * pl.col('hfq_factor')).alias('close'),
        pl.col('volume').cast(pl.Float64, strict=False).alias('volume'),
        pl.col('amount').cast(pl.Float64, strict=False).alias('amount'),
        pl.col('open').cast(pl.Float64, strict=False).alias('raw_open'),
        pl.col('high').cast(pl.Float64, strict=False).alias('raw_high'),
        pl.col('low').cast(pl.Float64, strict=False).alias('raw_low'),
        pl.col('close').cast(pl.Float64, strict=False).alias('raw_close'),
        pl.col('pre_close').cast(pl.Float64, strict=False).alias('raw_prev_close'),
        pl.col('volume').cast(pl.Float64, strict=False).alias('raw_volume'),
        pl.col('amount').cast(pl.Float64, strict=False).alias('raw_amount'),
        pl.col('hfq_factor').cast(pl.Float64, strict=False),
        pl.col('qfq_factor').cast(pl.Float64, strict=False),
    ])


class LocalPanelLoader:
    """本地日线面板加载器（多标的一次拼装）。"""

    def __init__(self, config: Any, store: LocalQuoteStore | None = None,
                 min_rows: int = 60) -> None:
        self.config = config
        self.root = resolve_store_root(config)
        self.store = store or LocalQuoteStore(self.root, enabled=True)
        self.min_rows = int(min_rows)

    def default_start(self, as_of: date, trading_days: int) -> date:
        """按交易日数估算自然日回溯起点（含节假日冗余）。"""
        return as_of - timedelta(days=int(trading_days * 1.7) + 30)

    def load(self, codes: Iterable[str], start: Any, end: Any) -> tuple[pl.DataFrame, LoadReport]:
        """加载多标的日线；缺文件/历史过短的标的跳过并统计。"""
        start_dt = _as_date(start)
        end_dt = _as_date(end)
        report = LoadReport()
        codes = list(dict.fromkeys(str(code).upper() for code in codes if code))
        report.requested = len(codes)
        frames: list[pl.DataFrame] = []

        for code in codes:
            raw = self.store.read_file('stock', code, with_factors=True)
            if raw is None or raw.is_empty():
                report.missing.append(code)
                continue
            if any(column not in raw.columns
                   for column in ('hfq_factor', 'qfq_factor', 'pre_close')):
                report.degraded.append(code)
            frame = _normalize_frame(raw, code)
            if frame.is_empty():
                report.missing.append(code)
                continue
            if start_dt is not None:
                frame = frame.filter(pl.col('date') >= start_dt)
            if end_dt is not None:
                frame = frame.filter(pl.col('date') <= end_dt)
            if frame.height < self.min_rows:
                report.skipped_short.append(code)
                continue
            frames.append(frame)

        if report.degraded:
            logger.warning(
                f'[advice] {len(report.degraded)} 只标的缺少复权因子/前收列，'
                f'已用 1.0 或 shift(1) 兜底，价格基准可能失真；'
                f'示例 {report.degraded[:5]}')
        if not frames:
            logger.warning(
                f"[advice] 本地列存没有可用行情：requested={report.requested}, "
                f"missing={len(report.missing)}")
            return pl.DataFrame({col: [] for col in _PANEL_COLUMNS}), report

        panel = pl.concat(frames, how='vertical_relaxed').sort(['ts_code', 'date'])
        report.loaded = len(frames)
        report.rows = panel.height
        logger.info(
            f"[advice] 本地面板加载完成: {report.loaded}/{report.requested} 标的, "
            f"{report.rows} 行, 缺文件 {len(report.missing)}")
        return panel, report


def panel_trading_dates(panel: pl.DataFrame) -> list[date]:
    """从面板提取升序交易日列表（全市场并集）。"""
    if panel.is_empty() or 'date' not in panel.columns:
        return []
    return [value for value in panel['date'].unique().sort().to_list()
            if isinstance(value, date)]
