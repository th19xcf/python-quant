#!/usr/bin/env python3

"""
Baostock 上游接口契约：字段串、列映射与行→记录转换收口于此。

handler 层不再出现上游字段字面量（preclose/pctChg 等）；
任何上游变更只改本文件，与 akshare_schema 对齐。
"""

from datetime import datetime
from typing import Any, Tuple

import pandas as pd
import polars as pl
from loguru import logger

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


def result_to_frame(result: Any) -> pd.DataFrame:
    """把 Baostock 结果集逐页拉全并拼成 pandas DataFrame。

    baostock 0.9.4 的 ResultData.get_data() 内部仍在使用 pandas 已在 2.0 移除的
    DataFrame.append，多页结果（如 query_stock_basic 全市场、跨年日线）必然抛
    AttributeError；这里改走官方推荐的 next()/get_row_data() 逐行拉取，与 pandas
    版本无关。空结果按 fields 建空表，保持调用方 `df.empty` 语义不变。
    """
    if result.error_code != '0':
        # Baostock 把网络失败收敛成 error_code，若不显式告警，
        # 上层只会看到"数据为空"，无法区分网络中断与真的没有数据
        logger.warning(
            f"Baostock 返回错误({result.error_code}): {getattr(result, 'error_msg', '')}")
    rows = []
    while result.error_code == '0' and result.next():
        rows.append(result.get_row_data())
    return pd.DataFrame(rows, columns=result.fields)


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


def _parse_minute_time(raw_time) -> datetime:
    """解析 Baostock 分钟线时间：``YYYYMMDDHHMMSSmmm`` 或 ISO 字符串。"""
    text = str(raw_time).strip()
    if text.isdigit():
        return datetime.strptime(text, "%Y%m%d%H%M%S%f")
    return datetime.fromisoformat(text)


def minute_row_to_record(row, ts_code):
    """Baostock 分钟线行 -> 待入库记录（与 StockMinute 列一致）。"""
    return {
        'ts_code': ts_code,
        'trade_time': _parse_minute_time(row['time']),
        'open': float(row['open']) if row['open'] else 0.0,
        'high': float(row['high']) if row['high'] else 0.0,
        'low': float(row['low']) if row['low'] else 0.0,
        'close': float(row['close']) if row['close'] else 0.0,
        'vol': float(row['volume']) if row['volume'] else 0.0,
        'amount': float(row['amount']) if row['amount'] else 0.0,
    }
