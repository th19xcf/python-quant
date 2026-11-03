#!/usr/bin/env python3

"""
通达信 .day 日线文件解析（纯函数实现）

不依赖 config / 数据库 / 线程池，供数据层与 UI 展示层共用，
避免同一套 32 字节定长记录解析逻辑在多处重复。
"""

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import polars as pl

from src.utils.code_converter import is_fund_code

RECORD_SIZE = 32
# 价格字段缩放：个股/指数整数单位为「分」（÷100），基金（ETF/LOF/封闭式）
# 报价保留 3 位小数，整数单位为「厘」（÷1000）。用 510050 上市首日
# （原始 876）与成交额反算验证：÷100 得 8.76 元与 amount 相差 10 倍，
# ÷1000 得 0.876 元且 amount/(price*volume) ≈ 1，故基金必须 ÷1000。
STOCK_PRICE_DIVISOR = 100.0
FUND_PRICE_DIVISOR = 1000.0

# 32 字节定长记录的 numpy 结构化布局（与真实 .day 文件逐字段一致）
_DAY_DTYPE = np.dtype([
    ('date', '<u4'),      # YYYYMMDD
    ('open', '<u4'),      # 价格 × 100（个股/指数）或 × 1000（基金，3 位小数）
    ('high', '<u4'),      # 同上
    ('low', '<u4'),       # 同上
    ('close', '<u4'),     # 同上
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


def is_fund_file(file_path) -> bool:
    """按文件名判断是否为基金（ETF/LOF/封闭式/REITs）的 .day 文件。

    基金与个股共用 lday 目录与 32 字节布局，但价格缩放不同（基金 ÷1000），
    代码段判定统一委托 ``code_converter.is_fund_code``，避免前缀规则分叉。
    """
    name = Path(file_path).stem.lower()
    if len(name) != 8 or name[:2] not in ('sh', 'sz', 'bj'):
        return False
    return is_fund_code(name[2:], name[:2])


def price_divisor(file_path) -> float:
    """返回该 .day 文件价格字段的缩放因子（个股/指数 100，基金 1000）。"""
    return FUND_PRICE_DIVISOR if is_fund_file(file_path) else STOCK_PRICE_DIVISOR


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


def resolve_market(code: str, ts_code: str | None = None) -> str:
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


def _load_content(path: Path, count: int | None) -> bytes:
    """定位并读入末尾 count 条记录的原始字节（count=None 表示全部）。"""
    record_count = path.stat().st_size // RECORD_SIZE
    start = 0 if count is None else max(0, record_count - count)
    with open(path, 'rb') as f:
        f.seek(start * RECORD_SIZE)
        return f.read()


# 少量记录（如只取末尾 2 条快照）走直接构造 date 的轻量路径，
# 规避 pandas 每次调用的固定开销；大数组仍用向量化解析。
_SMALL_DECODE_THRESHOLD = 8


def _decode_dates_small(values) -> np.ndarray:
    """把 YYYYMMDD 整数序列逐个构造成 datetime.date 对象数组。"""
    out = np.empty(len(values), dtype=object)
    for i, raw in enumerate(values):
        text = f"{int(raw):08d}"
        out[i] = date(int(text[:4]), int(text[4:6]), int(text[6:]))
    return out


def _decode_records(content: bytes, volume_in_hand: bool,
                    price_div: float = STOCK_PRICE_DIVISOR) -> Dict[str, Any]:
    """
    向量化解码整段 32 字节定长记录。

    与旧版逐记录 struct.unpack + strptime 的输出语义完全一致：
    日期为 datetime.date、价格按 ``price_div`` 缩放（个股/指数 100、基金 1000）、
    成交额为 float32 原值宽化到 float64、
    成交量个股/基金除以 100（股/份→手）、指数原样（手）；
    基金极少数「手」单位行按成交额反算单独识别。
    """
    arr = np.frombuffer(content, dtype=_DAY_DTYPE)
    if len(arr) <= _SMALL_DECODE_THRESHOLD:
        dates = _decode_dates_small(arr['date'])
    else:
        # 大数组走 pd.to_datetime C 层向量化解析，校验规则与 strptime 一致
        dates = pd.to_datetime(arr['date'].astype(str), format='%Y%m%d').date
    price = {
        key: arr[key] / price_div
        for key in ('open', 'high', 'low', 'close')
    }
    amount = arr['amount'].astype(np.float64)
    raw_vol = arr['volume'].astype(np.float64)

    if price_div == FUND_PRICE_DIVISOR:
        # 基金成交量绝大多数按「份」存储（÷100 得到手）；极少数巨量行
        # （全量 281 万行中约 0.37 万行、66 个标的）因份值超过 uint32 上限
        # 被通达信改写为「手」。用成交额反算识别：amount/(close*raw) ≈ 100
        # 说明原值已是手，直接保留；≈1 说明是份，除以 100。阈值取 50 以
        # 拉开两个真实簇（1 与 100）之间的判定余量。
        with np.errstate(divide='ignore', invalid='ignore'):
            implied = np.where(
                (price['close'] > 0) & (raw_vol > 0),
                amount / (price['close'] * raw_vol), 0.0)
        volume = np.where(implied > 50.0, raw_vol, raw_vol / 100.0)
    else:
        volume = raw_vol.astype(np.int64) if volume_in_hand else raw_vol / 100.0

    return {
        'date': dates,
        'open': price['open'],
        'high': price['high'],
        'low': price['low'],
        'close': price['close'],
        'volume': volume,
        'amount': amount,
    }


def read_recent_records(file_path, count: int | None = None,
                        volume_in_hand: bool | None = None) -> List[Dict]:
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
    cols = _decode_records(content, volume_in_hand, price_divisor(path))
    keys = tuple(cols.keys())
    return [dict(zip(keys, row, strict=False)) for row in zip(*(cols[k] for k in keys), strict=False)]


def parse_day_file(file_path, max_days: int | None = None) -> pl.DataFrame:
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
    cols = _decode_records(_load_content(path, max_days), is_index_file(path),
                           price_divisor(path))
    return pl.DataFrame({
        'date': cols['date'].tolist(),
        'open': cols['open'],
        'high': cols['high'],
        'low': cols['low'],
        'close': cols['close'],
        'volume': cols['volume'],
        'amount': cols['amount'],
    })


def _decode_record_date(record: bytes) -> date | None:
    """解码单条记录 @0 的 YYYYMMDD 日期，非法值返回 None。"""
    try:
        day_value = int(np.frombuffer(record, dtype=_DAY_DTYPE, count=1)['date'][0])
        text = str(day_value)
        if len(text) != 8:
            return None
        return datetime.strptime(text, '%Y%m%d').date()
    except (ValueError, TypeError):
        return None


def read_first_last_trade_date(file_path) -> tuple[date | None, date | None]:
    """
    读取 .day 文件首末两条记录的交易日。

    只读首尾各 32 字节，不加载全文件，供增量同步判断文件覆盖范围。

    Args:
        file_path: .day 文件路径

    Returns:
        tuple: (首个交易日, 最后一个交易日)；文件缺失、为空或解析失败时为 (None, None)
    """
    path = Path(file_path)
    try:
        record_count = path.stat().st_size // RECORD_SIZE
        if record_count == 0:
            return None, None

        with open(path, 'rb') as f:
            first_record = f.read(RECORD_SIZE)
            f.seek((record_count - 1) * RECORD_SIZE)
            last_record = f.read(RECORD_SIZE)

        return _decode_record_date(first_record), _decode_record_date(last_record)
    except (OSError, ValueError):
        return None, None


def read_latest_quote(file_path) -> QuoteSnapshot | None:
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
