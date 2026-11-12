#!/usr/bin/env python3

"""推荐引擎测试用的合成本地列存构造器（非测试模块）。"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl


def business_dates(end: date, count: int) -> list[date]:
    dates: list[date] = []
    cursor = end
    while len(dates) < count:
        if cursor.weekday() < 5:
            dates.append(cursor)
        cursor -= timedelta(days=1)
    return list(reversed(dates))


def write_stock(root: Path, ts_code: str, dates: list[date], base: float,
                trend: float = 0.02, wave: float = 0.0,
                amount: float = 5.0e8, volume: float = 1.0e5,
                limit_up_last: bool = False) -> None:
    """写入单标的 Parquet（原始价 + hfq_factor=1）。"""
    closes: list[float] = []
    for index in range(len(dates)):
        value = base * (1.0 + trend * index / 10.0)
        if wave:
            value *= 1.0 + wave * ((index % 7) - 3) / 3.0
        closes.append(value)
    if limit_up_last and len(closes) > 1:
        # 涨跌停价按交易所规则取整到分，合成数据必须与之一致
        closes[-1] = round(closes[-2] * 1.1, 2)

    opens = [closes[0]] + closes[:-1]
    if limit_up_last:
        opens[-1] = closes[-1]
    highs = [max(open_, close) * 1.01
             for open_, close in zip(opens, closes, strict=False)]
    lows = [min(open_, close) * 0.99
            for open_, close in zip(opens, closes, strict=False)]
    prev = [None] + closes[:-1]
    frame = pl.DataFrame({
        'date': dates,
        'open': opens,
        'high': highs,
        'low': lows,
        'close': closes,
        'pre_close': [value if value is not None else closes[0] for value in prev],
        'volume': [volume] * len(dates),
        'amount': [amount] * len(dates),
        'hfq_factor': [1.0] * len(dates),
        'qfq_factor': [1.0] * len(dates),
    })
    market = ts_code.rsplit('.', 1)[-1].lower()
    path = root / 'quotes' / 'stock' / market / f'{ts_code}.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path)


def build_store(root: Path, codes: list[str], end: date, days: int = 320,
                with_limit_up: str | None = None) -> Path:
    dates = business_dates(end, days)
    for index, code in enumerate(codes):
        write_stock(
            root, code, dates, base=8.0 + index * 1.5,
            trend=0.03 + index * 0.01, wave=0.02 if index % 2 else 0.0,
            limit_up_last=(code == with_limit_up),
        )
    write_st_snapshot(root, codes, end)
    return root


def write_st_snapshot(root: Path, codes: list[str], day: date) -> None:
    """写入全量 non_st 快照（并入单表 history），供合成测试通过 fail-closed 的 ST 检查。"""
    path = root / 'meta' / 'st_status' / 'st_status_history.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame({
        'code': codes,
        'name': codes,
        'status': ['non_st'] * len(codes),
        'source': ['test'] * len(codes),
        'reason': [''] * len(codes),
        'as_of': [day] * len(codes),
        'is_st': [False] * len(codes),
    })
    if path.is_file():
        frame = pl.concat([pl.read_parquet(path).filter(pl.col('as_of') != day), frame],
                          how='vertical')
    frame.sort(['as_of', 'code']).write_parquet(path)
