#!/usr/bin/env python3

"""
除权除息数据服务

股票代码格式化、分红日期过滤原为主窗口绘制 Mixin 中的业务逻辑，
下沉为纯函数与服务类，UI 层仅负责把结果交给标记管理器渲染。
"""

from src.utils.code_converter import parse_code
from src.utils.logger import logger

# 分红数据输出列（按可用列裁剪）
_DIVIDEND_COLUMNS = ['ex_date', 'record_date', 'pay_date', 'cash_div', 'share_div']


def format_stock_code(stock_code: str) -> str:
    """
    格式化为标准 ts_code（如 600519.SH）。

    委托统一代码转换收口点，支持 600519.SH / sh600519 / sh.600519 / 600519；
    纯数字代码按 code_converter.infer_market 推断市场（含北交所与沪市基金）。
    """
    symbol, market = parse_code(stock_code)
    return f"{symbol}.{market}"


_SKIP = object()  # 无法识别的日期值：丢弃（与原实现一致）


def _to_date(value):
    """按原实现规则转换 K 线日期值；无法识别时返回 _SKIP。"""
    if hasattr(value, 'date') and callable(value.date):
        return value.date()
    if hasattr(value, 'strftime'):
        return value
    if hasattr(value, 'astype'):
        try:
            item = value.astype('datetime64[D]').item()
            return item.date() if hasattr(item, 'date') else item
        except (OSError, RuntimeError, ValueError):
            return value
    return _SKIP


def filter_dividends_by_dates(dividend_data, kline_dates):
    """过滤出除权日落在 K 线日期范围内的分红记录。"""
    if len(kline_dates) == 0:
        return []

    kline_date_set = set()
    for d in kline_dates:
        converted = _to_date(d)
        if converted is not _SKIP:
            kline_date_set.add(converted)

    filtered = []
    for div in dividend_data:
        ex_date = div.get('ex_date')
        if ex_date:
            if hasattr(ex_date, 'date') and callable(ex_date.date):
                ex_date = ex_date.date()
            if ex_date in kline_date_set:
                filtered.append(div)
    return filtered


class DividendService:
    """分红配股数据查询服务。"""

    def __init__(self, data_manager=None):
        self._data_manager = data_manager

    def get_dividends_in_range(self, stock_code: str, kline_dates) -> list:
        """
        获取指定股票在 K 线日期范围内的分红记录。

        Returns:
            list[dict]: 分红记录；无数据或查询失败返回空列表
        """
        if not self._data_manager:
            return []

        try:
            ts_code = format_stock_code(stock_code)
            df = self._data_manager.get_stock_dividend(ts_code)
            if df is None or df.is_empty():
                return []

            available = [c for c in _DIVIDEND_COLUMNS if c in df.columns]
            dividend_data = df.select(available).to_dicts()
            return filter_dividends_by_dates(dividend_data, kline_dates)
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"获取分红数据失败: {e}")
            return []
