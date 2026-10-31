#!/usr/bin/env python3

"""
Baostock 上游接口契约：字段串、列映射与行→记录转换收口于此。

handler 层不再出现上游字段字面量（preclose/pctChg 等）；
任何上游变更只改本文件，与 akshare_schema 对齐。
"""

from datetime import datetime
from typing import Tuple

import polars as pl

# Baostock 查询字段串（集中声明，避免多处字面量漂移）
DAILY_FIELDS = (
    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,"
    "tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"
)
DAILY_FIELDS_BASIC = "date,code,open,high,low,close,preclose,volume,amount,pctChg"
MINUTE_FIELDS = "date,time,code,open,high,low,close,volume,amount"

# Baostock 上游列名 -> 标准列名（供 _normalize_kline 使用）
BAOSTOCK_KLINE_COLUMN_MAP = {
    'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close',
    'pre_close': 'preclose', 'volume': 'volume', 'amount': 'amount',
    'pct_chg': 'pctChg',
}


def compute_daily_factors(raw_df: pl.DataFrame, hfq_df: pl.DataFrame
                          ) -> Tuple[pl.DataFrame | None, pl.DataFrame | None]:
    """纯计算：由原始价与后复权价求每日 qfq_factor / hfq_factor。"""
    raw = raw_df.select([
        pl.col('date'),
        pl.col('close').cast(pl.Float64).alias('close_raw'),
    ])
    joined = (raw.join(hfq_df, on='date', how='inner')
                 .filter((pl.col('close_raw') > 0)
                         & pl.col('close_hfq').is_not_null())
                 .sort('date'))
    if joined.is_empty():
        return None, None

    factor = joined['close_hfq'] / joined['close_raw']
    anchor = factor[-1]  # 区间末锚点，与 baostock 前复权口径一致
    if anchor is None or anchor <= 0:
        return None, None

    hfq_factor = pl.DataFrame({'date': joined['date'], 'hfq_factor': factor})
    qfq_factor = pl.DataFrame({'date': joined['date'], 'qfq_factor': factor / anchor})
    return qfq_factor, hfq_factor


def daily_row_to_record(row, ts_code):
    """Baostock 日线行 -> 待入库记录；缺失数值统一按 0.0 处理"""
    close_val = float(row['close']) if row['close'] else 0.0
    pre_close_val = float(row['preclose']) if row['preclose'] else 0.0
    return {
        'ts_code': ts_code,
        'trade_date': datetime.strptime(row['date'], "%Y-%m-%d").date(),
        'open': float(row['open']) if row['open'] else 0.0,
        'high': float(row['high']) if row['high'] else 0.0,
        'low': float(row['low']) if row['low'] else 0.0,
        'close': close_val,
        'pre_close': pre_close_val,
        'change': close_val - pre_close_val,
        'pct_chg': float(row['pctChg']) if row['pctChg'] else 0.0,
        'vol': float(row['volume']) if row['volume'] else 0.0,
        'amount': float(row['amount']) if row['amount'] else 0.0,
    }


def minute_row_to_record(row, ts_code):
    """Baostock 分钟线行 -> 待入库记录"""
    return {
        'ts_code': ts_code,
        'trade_date': datetime.strptime(row['date'], "%Y-%m-%d").date(),
        'trade_time': row['time'],
        'open': float(row['open']) if row['open'] else 0.0,
        'high': float(row['high']) if row['high'] else 0.0,
        'low': float(row['low']) if row['low'] else 0.0,
        'close': float(row['close']) if row['close'] else 0.0,
        'vol': float(row['volume']) if row['volume'] else 0.0,
        'amount': float(row['amount']) if row['amount'] else 0.0,
    }
