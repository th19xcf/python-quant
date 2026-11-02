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


def is_index_code(code: str | None) -> bool:
    """判断任意格式的证券代码是否为指数（单一实现，供全项目复用）。

    支持的指数代码规则：
    - 沪市：000xxx（上证系列）、880xxx / 881xxx（通达信板块指数）
    - 深市：399xxx（深证系列）
    - 北交所：884xxx / 899xxx

    兼容 ts_code（000001.SH）、baostock（sh.000001）、tdx（sh000001）。
    纯数字代码因存在「000001.SZ 个股 / 000001.SH 指数」的同号歧义，
    一律返回 False，由调用方先补全市场后缀再判定。
    """
    if not code:
        return False

    try:
        symbol, market = parse_code(code)
    except (TypeError, ValueError):
        return False

    market = market.upper()
    if market == MARKET_SH:
        return symbol.startswith(('000', '880', '881'))
    if market == MARKET_SZ:
        return symbol.startswith('399')
    if market == MARKET_BJ:
        return symbol.startswith(('884', '899'))
    return False


# 指数纯数字前缀 -> 市场（000001 是上证指数而非平安银行，不能用 infer_market）
_INDEX_PREFIX_MARKETS = (
    (('000', '880', '881'), MARKET_SH),
    (('399',), MARKET_SZ),
    (('884', '899'), MARKET_BJ),
)

# 基金代码前缀 -> 市场（单一实现，TDX 文件扫描与同步流程共用）
# SH: 50/501/508（沪市封闭式/ETF）、51/52/56（560-563）/58（588）、520/526
# SZ: 15/158/16（LOF）/18/180
_FUND_PREFIX_MARKETS = {
    MARKET_SH: ('50', '501', '508', '51', '52', '56', '560', '561', '562', '563',
                '58', '520', '526'),
    MARKET_SZ: ('15', '158', '16', '18', '180'),
}


def to_index_ts_code(code: str | None) -> str:
    """任意格式的指数代码 -> 标准 ts_code（如 '000001.SH'）。

    与 `to_ts_code` 的差别：纯数字代码按指数前缀规则推断市场，
    避免 000001 被个股规则推断为 000001.SZ。
    """
    if not code:
        raise ValueError(f"无效的指数代码: {code!r}")

    text = str(code).strip()
    if not (text.isdigit() and len(text) == 6):
        # 已带市场标识（000001.SH / sh.000001 / sh880001）时委托通用转换
        return to_ts_code(text)

    for prefixes, market in _INDEX_PREFIX_MARKETS:
        if text.startswith(prefixes):
            return f"{text}.{market}"
    raise ValueError(f"无法识别的指数代码前缀: {code!r}")


def is_fund_code(code: str | None, market: str | None = None) -> bool:
    """判断证券代码是否为基金（ETF / LOF / 封闭式基金）。

    代码前缀是主判据；`market` 可显式指定（通达信文件名扫描时已知市场），
    未指定时按统一规则推断。
    """
    if not code:
        return False

    try:
        symbol, inferred_market = parse_code(str(code))
    except (TypeError, ValueError):
        return False

    mkt = (market or inferred_market).upper()
    return symbol.startswith(_FUND_PREFIX_MARKETS.get(mkt, ()))
