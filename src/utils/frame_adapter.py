#!/usr/bin/env python3

"""
DataFrame 双栈适配层（收口点）

项目内部统一使用 Polars；pandas 只允许出现在两处边界：
1. 外部调用方传入 pandas DataFrame（本模块负责 pandas -> Polars）；
2. legacy 插件 API（plugin_base.calculate 只接受 pandas，本模块负责
   Polars -> pandas 以及插件返回值的 pandas -> Polars 回转）。

除本模块与 legacy 插件适配层外，业务代码不应直接调用
pl.from_pandas / DataFrame.to_pandas。
"""

from typing import Union

import pandas as pd
import polars as pl


def to_polars(data: Union[pl.DataFrame, pd.DataFrame]) -> pl.DataFrame:
    """
    把输入数据统一转为 Polars DataFrame

    Args:
        data: Polars DataFrame 或 pandas DataFrame

    Returns:
        pl.DataFrame: Polars DataFrame；输入已是 Polars 时原样返回
    """
    if isinstance(data, pd.DataFrame):
        return pl.from_pandas(data)
    return data


def to_output(data: pl.DataFrame, return_polars: bool = True) -> Union[pl.DataFrame, pd.DataFrame]:
    """
    按调用方要求返回 Polars 或 pandas

    Args:
        data: Polars DataFrame
        return_polars: True 返回 Polars（默认），False 转为 pandas

    Returns:
        Union[pl.DataFrame, pd.DataFrame]: 指定格式的结果
    """
    if return_polars:
        return data
    return data.to_pandas()
