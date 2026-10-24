#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
通达信 .day 日线文件解析（纯函数实现）

不依赖 config / 数据库 / 线程池，供数据层与 UI 展示层共用，
避免同一套 32 字节定长记录解析逻辑在多处重复。
"""

import struct
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import polars as pl

RECORD_SIZE = 32


@dataclass(frozen=True)
class QuoteSnapshot:
    """最新一根日线行情快照（价格为元，成交量为手，成交额为元）。"""

    date: str
    open: float
    high: float
    low: float
    close: float
    preclose: float
    volume: int
    amount: float
    change: float
    pct_chg: float
    amplitude: float


def resolve_market(code: str, ts_code: Optional[str] = None) -> str:
    """
    推断通达信市场目录（sh/sz/bj）。

    Args:
        code: 不带市场前缀的证券代码，如 510300
        ts_code: 可选的标准代码，如 510300.SH，优先使用其后缀

    Returns:
        str: 'sh' / 'sz' / 'bj'
    """
    if ts_code and '.' in ts_code:
        suffix = ts_code.split('.')[-1].upper()
        if suffix in {'SH', 'SZ', 'BJ'}:
            return suffix.lower()

    if code.startswith('5'):
        return 'sh'
    if code.startswith(('1', '0', '3')):
        return 'sz'
    if code.startswith(('8', '9')):
        return 'bj'
    return 'sz'


def build_day_file_path(tdx_data_path, market: str, code: str) -> Path:
    """构造通达信日线文件路径。"""
    return Path(tdx_data_path) / market / 'lday' / f'{market}{code}.day'


def _decode_record(record: bytes) -> Dict:
    """
    解析单条 32 字节日线记录。

    字段布局（经真实 .day 文件校验）：
        0-4   date     uint32   YYYYMMDD
        4-8   open     uint32   价格 × 100
        8-12  high     uint32   价格 × 100
        12-16 low      uint32   价格 × 100
        16-20 close    uint32   价格 × 100
        20-24 amount   float32  成交额（元）
        24-28 volume   uint32   成交量（股）
        28-32 reserved

    输出统一为：成交额（元）、成交量（手，即股 / 100）。
    """
    return {
        'date': datetime.strptime(str(struct.unpack('<I', record[0:4])[0]), '%Y%m%d').date(),
        'open': struct.unpack('<I', record[4:8])[0] / 100,
        'high': struct.unpack('<I', record[8:12])[0] / 100,
        'low': struct.unpack('<I', record[12:16])[0] / 100,
        'close': struct.unpack('<I', record[16:20])[0] / 100,
        'volume': struct.unpack('<I', record[24:28])[0] / 100,
        'amount': struct.unpack('<f', record[20:24])[0],
    }


def read_recent_records(file_path, count: Optional[int] = None) -> List[Dict]:
    """
    读取文件末尾的若干条日线记录。

    Args:
        file_path: .day 文件路径
        count: 读取最近 count 条；None 表示全部

    Returns:
        list: 按时间升序排列的记录字典列表
    """
    path = Path(file_path)
    record_count = path.stat().st_size // RECORD_SIZE
    if record_count == 0:
        return []

    start = 0 if count is None else max(0, record_count - count)
    with open(path, 'rb') as f:
        f.seek(start * RECORD_SIZE)
        content = f.read()

    return [
        _decode_record(content[i * RECORD_SIZE:(i + 1) * RECORD_SIZE])
        for i in range(len(content) // RECORD_SIZE)
    ]


def parse_day_file(file_path, max_days: Optional[int] = None) -> pl.DataFrame:
    """
    解析通达信日线数据文件。

    Args:
        file_path: 日线数据文件路径
        max_days: 只解析最近 max_days 条记录，None 表示全部

    Returns:
        polars.DataFrame: date/open/high/low/close/volume（手）/amount（元）
    """
    records = read_recent_records(file_path, max_days)
    return pl.DataFrame(records) if records else pl.DataFrame()


def read_latest_quote(file_path) -> Optional[QuoteSnapshot]:
    """
    读取最新一根日线行情，并基于前一根收盘价计算涨跌/振幅。

    Args:
        file_path: .day 文件路径

    Returns:
        QuoteSnapshot: 行情快照；文件不存在、为空或解析失败时返回 None
    """
    try:
        records = read_recent_records(file_path, 2)
    except (OSError, ValueError):
        return None

    if not records:
        return None

    latest = records[-1]
    prev = records[-2] if len(records) >= 2 else None
    preclose = prev['close'] if prev else latest['close']
    close = latest['close']
    change = close - preclose
    pct_chg = (change / preclose) * 100 if preclose != 0 else 0.0
    amplitude = ((latest['high'] - latest['low']) / preclose) * 100 if preclose > 0 else 0.0

    return QuoteSnapshot(
        date=latest['date'].strftime('%Y-%m-%d'),
        open=latest['open'],
        high=latest['high'],
        low=latest['low'],
        close=close,
        preclose=preclose,
        volume=latest['volume'],
        amount=latest['amount'],
        change=change,
        pct_chg=pct_chg,
        amplitude=amplitude,
    )
