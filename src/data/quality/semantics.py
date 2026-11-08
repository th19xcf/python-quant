#!/usr/bin/env python3

"""通达信「统计指标」序列的语义识别。

通达信把两类完全不同的东西都编进 ``880xxx`` 代码段：

1. **板块价格指数**（如 880301 煤炭）：``open/high/low/close`` 是点位，
   与个股同语义，可以算收益率、套用价格类契约；
2. **涨跌/停板家数统计**（如 880005 涨跌家数）：四个价格列装的其实是
   家数，实测映射为

   - ``open``   = 下跌家数
   - ``high``   = 该板块当日总家数
   - ``low``    = 平盘家数
   - ``close``  = 上涨家数

   2026-11-06 用全市场个股自算得 上涨 757 / 下跌 4740 / 平盘 74，
   对应 880005 的 close=758 / open=4741 / low=74，误差 1 家，映射成立。

第 2 类序列的家数本来就会逐日剧烈变动（涨跌家数在 700~4800 之间摆动是常态），
套用「前收等于上一交易日收盘」「单日涨跌幅有限」这类价格规则会 100% 误报。
本模块只做**识别**，不改数据、不做单位换算。
"""

from __future__ import annotations

from typing import Final

#: 涨跌家数 / 停板家数统计指标：代码 -> (名称, 统计含义)
STATISTIC_SERIES: Final[dict[str, tuple[str, str]]] = {
    '880005.SH': ('涨跌家数', '全市场 上涨/下跌/平盘 家数'),
    '880006.SH': ('停板家数', '全市场 涨停/跌停/停牌 家数'),
    '880015.SH': ('主板涨跌', '主板 上涨/下跌/平盘 家数'),
    '880016.SH': ('主板停板', '主板 涨停/跌停 家数'),
    '880025.SH': ('中小涨跌', '中小板 上涨/下跌/平盘 家数'),
    '880026.SH': ('中小停板', '中小板 涨停/跌停 家数'),
    '880035.SH': ('创业涨跌', '创业板 上涨/下跌/平盘 家数'),
    '880036.SH': ('创业停板', '创业板 涨停/跌停 家数'),
    '880045.SH': ('科创涨跌', '科创板 上涨/下跌/平盘 家数'),
    '880046.SH': ('科创停板', '科创板 涨停/跌停 家数'),
    '880075.SH': ('北证涨跌', '北证 上涨/下跌/平盘 家数'),
    '880076.SH': ('北证停板', '北证 涨停/跌停 家数'),
}

#: 代码集合，便于快速判定
STATISTIC_SERIES_CODES: Final[frozenset[str]] = frozenset(STATISTIC_SERIES)


def is_statistic_series(ts_code: str) -> bool:
    """该代码是否属于涨跌/停板家数统计指标（而非价格指数）。"""
    return ts_code.upper() in STATISTIC_SERIES_CODES


def describe(ts_code: str) -> str | None:
    """返回统计指标的「名称(含义)」，非统计指标返回 None。"""
    item = STATISTIC_SERIES.get(ts_code.upper())
    if item is None:
        return None
    name, meaning = item
    return f'{name}({meaning})'
