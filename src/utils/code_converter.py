#!/usr/bin/env python3

"""
证券代码格式统一转换工具（收口点）

标准格式（ts_code）：600000.SH / 000001.SZ / 920001.BJ
各数据源方言：
- baostock: sh.600000 / sz.000001 / bj.920001
- akshare:  600000（纯数字）
- tdx 文件: sh600000.day（市场小写前缀 + 6 位数字）

市场推断规则（纯数字代码）：
- 92 开头        -> BJ（北交所新股）
- 4 / 8 开头     -> BJ（北交所/新三板）
- 6 / 9 开头     -> SH（沪市股票 / 沪市B股）
- 5 开头         -> SH（沪市基金/ETF，如 51xxxx、58xxxx）
- 其余           -> SZ（深市股票/ETF，如 0/1/2/3 开头）

特判：920 开头 6 位代码即使显式带 .SZ 后缀也判为 BJ
（北交所新股历史上曾被错误标注 .SZ 后缀，保持兼容）。
"""

from typing import Tuple

MARKET_SH = 'SH'
MARKET_SZ = 'SZ'
MARKET_BJ = 'BJ'

_VALID_MARKETS = (MARKET_SH, MARKET_SZ, MARKET_BJ)
_TDX_PREFIXES = ('sh', 'sz', 'bj')


def infer_market(symbol: str) -> str:
    """
    纯数字 6 位代码推断市场

    Args:
        symbol: 6 位数字代码，如 '600000'

    Returns:
        str: 'SH' / 'SZ' / 'BJ'

    Raises:
        ValueError: 代码不是 6 位数字
    """
    if not symbol or len(symbol) != 6 or not symbol.isdigit():
        raise ValueError(f"无效的证券代码: {symbol!r}（需为 6 位数字）")

    if symbol.startswith('92'):
        return MARKET_BJ
    if symbol.startswith(('4', '8')):
        return MARKET_BJ
    if symbol.startswith(('6', '9', '5')):
        return MARKET_SH
    return MARKET_SZ


def parse_code(code: str) -> Tuple[str, str]:
    """
    解析任意格式的证券代码为 (symbol, market)

    支持格式：
    - 600000.SH / 600000.sh（标准格式，大小写不限）
    - sh.600000 / SH.600000（baostock 格式）
    - sh600000（tdx 文件名风格）
    - 600000（纯数字，按规则推断市场）

    Args:
        code: 任意格式的证券代码

    Returns:
        Tuple[str, str]: (symbol, market)，如 ('600000', 'SH')

    Raises:
        ValueError: 无法识别的格式
    """
    if not code or not isinstance(code, str):
        raise ValueError(f"无效的证券代码: {code!r}")

    code = code.strip()

    if '.' in code:
        if code.count('.') != 1:
            raise ValueError(f"无效的证券代码格式: {code}")
        left, right = code.split('.')

        # sh.600000 格式（市场前缀在前）
        if left.lower() in _TDX_PREFIXES:
            market, symbol = left.upper(), right
        # 600000.SH 格式（市场后缀在后）
        elif right.upper() in _VALID_MARKETS:
            symbol, market = left, right.upper()
            # 特判：920 开头北交所新股即使标注 .SZ 也判为 BJ
            if symbol.startswith('92') and len(symbol) == 6 and market == MARKET_SZ:
                market = MARKET_BJ
        else:
            raise ValueError(f"无效的证券代码格式: {code}")
    elif code[:2].lower() in _TDX_PREFIXES and len(code) == 8:
        # sh600000 格式（tdx 文件名风格）
        market, symbol = code[:2].upper(), code[2:]
    else:
        # 纯数字格式，推断市场
        symbol = code
        market = infer_market(symbol)

    if not symbol or not symbol.isdigit() or len(symbol) != 6:
        raise ValueError(f"无效的证券代码: {code!r}（数字部分需为 6 位）")

    return symbol, market


def to_ts_code(code: str) -> str:
    """任意格式 -> 标准 ts_code（如 '600000.SH'）"""
    symbol, market = parse_code(code)
    return f"{symbol}.{market}"


def to_symbol(code: str) -> str:
    """任意格式 -> 纯数字代码（如 '600000'，akshare 接口使用）"""
    symbol, _ = parse_code(code)
    return symbol


def to_baostock_code(code: str) -> str:
    """任意格式 -> baostock 格式（如 'sh.600000'）"""
    symbol, market = parse_code(code)
    return f"{market.lower()}.{symbol}"


def to_tdx_market(code: str) -> str:
    """任意格式 -> tdx 小写市场标识（如 'sh'，用于拼接数据目录/文件名）"""
    _, market = parse_code(code)
    return market.lower()


def to_tdx_file_stem(code: str) -> str:
    """任意格式 -> tdx 文件名主体（如 'sh600000'，对应 sh600000.day）"""
    symbol, market = parse_code(code)
    return f"{market.lower()}{symbol}"
