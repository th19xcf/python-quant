#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
通达信 .day 日线文件解析（纯函数实现）

不依赖 config / 数据库 / 线程池，供数据层与 UI 展示层共用，
避免同一套 32 字节定长记录解析逻辑在多处重复。
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import polars as pl

RECORD_SIZE = 32

# 32 字节定长记录的 numpy 结构化布局（与真实 .day 文件逐字段一致）
_DAY_DTYPE = np.dtype([
    ('date', '<u4'),      # YYYYMMDD
    ('open', '<u4'),      # 价格 × 100
    ('high', '<u4'),      # 价格 × 100
    ('low', '<u4'),       # 价格 × 100
    ('close', '<u4'),     # 价格 × 100
    ('amount', '<f4'),    # 成交额（元）
    ('volume', '<u4'),    # 成交量（个股/基金=股，指数=手）
    ('reserved', '<u4'),
])


def is_index_file(file_path) -> bool:
    """
    按文件名判断是否为指数 .day 文件。

    通达信指数代码规则：sh000xxx（上证系列）、sz399xxx（深证系列）。
    指数记录的成交量字段单位为「手」，个股/基金为「股」，
    两者共用 lday 目录与 32 字节布局，必须区分处理。
    """
    name = Path(file_path).stem.lower()
    if name.startswith('sh'):
        return name[2:].startswith('000')
    if name.startswith('sz'):
        return name[2:].startswith('399')
    return False


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


def _load_content(path: Path, count: Optional[int]) -> bytes:
    """定位并读入末尾 count 条记录的原始字节（count=None 表示全部）。"""
    record_count = path.stat().st_size // RECORD_SIZE
    start = 0 if count is None else max(0, record_count - count)
    with open(path, 'rb') as f:
        f.seek(start * RECORD_SIZE)
        return f.read()


def _decode_records(content: bytes, volume_in_hand: bool) -> Dict[str, Any]:
    """
    向量化解码整段 32 字节定长记录。

    与旧版逐记录 struct.unpack + strptime 的输出语义完全一致：
    日期为 datetime.date、价格除以 100、成交额为 float32 原值宽化到 float64、
    成交量个股/基金除以 100（股→手）、指数原样（手）。
    """
    arr = np.frombuffer(content, dtype=_DAY_DTYPE)
    # pd.to_datetime 定格式走 C 层解析，校验规则与 strptime 一致
    dates = pd.to_datetime(arr['date'].astype(str), format='%Y%m%d').date
    volume = arr['volume'].astype(np.int64) if volume_in_hand else arr['volume'] / 100.0
    return {
        'date': dates,
        'open': arr['open'] / 100.0,
        'high': arr['high'] / 100.0,
        'low': arr['low'] / 100.0,
        'close': arr['close'] / 100.0,
        'volume': volume,
        'amount': arr['amount'].astype(np.float64),
    }


def read_recent_records(file_path, count: Optional[int] = None,
                        volume_in_hand: Optional[bool] = None) -> List[Dict]:
    """
    读取文件末尾的若干条日线记录。

    Args:
        file_path: .day 文件路径
        count: 读取最近 count 条；None 表示全部
        volume_in_hand: 成交量字段是否以手为单位；
            None 时按文件名自动判断（sh000*/sz399* 指数为手，其余为股）

    Returns:
        list: 按时间升序排列的记录字典列表
    """
    path = Path(file_path)
    record_count = path.stat().st_size // RECORD_SIZE
    if record_count == 0:
        return []

    if volume_in_hand is None:
        volume_in_hand = is_index_file(path)

    content = _load_content(path, count)
    cols = _decode_records(content, volume_in_hand)
    keys = tuple(cols.keys())
    return [dict(zip(keys, row)) for row in zip(*(cols[k] for k in keys))]


def parse_day_file(file_path, max_days: Optional[int] = None) -> pl.DataFrame:
    """
    解析通达信日线数据文件。

    Args:
        file_path: 日线数据文件路径
        max_days: 只解析最近 max_days 条记录，None 表示全部

    Returns:
        polars.DataFrame: date/open/high/low/close/volume（手）/amount（元）
    """
    path = Path(file_path)
    record_count = path.stat().st_size // RECORD_SIZE
    if record_count == 0:
        return pl.DataFrame()

    # 批量解析：numpy frombuffer 一次解码，直接按列构建 DataFrame，避免逐记录建字典
    cols = _decode_records(_load_content(path, max_days), is_index_file(path))
    return pl.DataFrame({
        'date': cols['date'].tolist(),
        'open': cols['open'],
        'high': cols['high'],
        'low': cols['low'],
        'close': cols['close'],
        'volume': cols['volume'],
        'amount': cols['amount'],
    })


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
