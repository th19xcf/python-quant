#!/usr/bin/env python3

"""
策略基类，为具体策略实现提供统一的接口
"""

from abc import ABC, abstractmethod
from typing import Any, Dict

import polars as pl


class BaseStrategy(ABC):
    """
    策略基类，所有具体策略都应继承此类

    双接口约定：
        - generate_signal(data, index)：逐行接口，每根 K 线调用一次，
          服务少量需要跨行状态的策略（保留兼容）；
        - generate_signals_vectorized(df)：向量化接口，整段数据一次性
          产出信号列，回测引擎在主循环前批量调用。子类覆写它即可用
          纯 polars 表达式消除 N_days × N_symbols 次 Python 函数调用。
    """

    def __init__(self, name: str):
        """
        初始化策略

        Args:
            name: 策略名称
        """
        self.name = name
        self.params = {}

    def set_params(self, params: Dict[str, Any]):
        """
        设置策略参数

        Args:
            params: 策略参数
        """
        self.params.update(params)

    @abstractmethod
    def generate_signal(self, data: Dict[str, Any], index: int) -> str:
        """
        生成交易信号

        Args:
            data: 当前数据
            index: 当前数据索引

        Returns:
            str: 交易信号，'buy'、'sell'或'hold'
        """
        pass

    def generate_signals_vectorized(self, df: pl.DataFrame) -> pl.Series:
        """
        一次性生成整段数据的信号序列（向量化接口）

        默认兜底实现：批量导出各列后逐行构造行 dict 调用 generate_signal，
        与旧逐行接口语义完全一致。子类应覆写本方法，用纯 polars 表达式
        （pl.when 链式判定）实现，消除逐行函数调用与行 dict 组装开销。

        Args:
            df: 单标的行情数据（含引擎预计算的指标列），按日期升序

        Returns:
            pl.Series: 与 df 行序对齐的信号列，取值 'buy'/'sell'/'hold'
        """
        cols = {name: df[name].to_list() for name in df.columns}
        signals = [
            self.generate_signal({name: col[i] for name, col in cols.items()}, i)
            for i in range(df.height)
        ]
        return pl.Series('signal', signals, dtype=pl.Utf8)
