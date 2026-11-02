#!/usr/bin/env python3

"""
业务逻辑层接口定义
统一使用Polars DataFrame作为数据格式
"""

from typing import Any, List, Protocol

import polars as pl


class ITechnicalAnalyzer(Protocol):
    """技术分析器契约（结构化子类型）

    实现类无需继承本类（TechnicalAnalyzer 由 mixin 组合出全部方法），
    mypy 按方法签名做结构化一致性校验。
    """

    def calculate_indicator(self, indicator_type: str, *args: Any, **kwargs: Any) -> Any:
        """计算单个技术指标

        实际实现（ParallelIndicatorMixin）以 self.data 为输入，按指标类型
        分派到具体计算器，data 不作为显式参数——契约按实现签名维护，避免
        契约与实际调用方式漂移。

        Args:
            indicator_type: 指标类型，如：'ma'、'macd'、'kdj'等
            *args: 位置参数
            **kwargs: 指标计算参数（如 windows）

        Returns:
            Any: 计算结果，默认 Polars DataFrame；return_polars=False 时为 pandas
        """
        raise NotImplementedError("子类必须实现此方法")

    def calculate_all_indicators(self, data: pl.DataFrame, indicator_types: List[str] | None = None, **params: Any) -> pl.DataFrame:
        """计算多个技术指标

        Args:
            data: 股票数据（Polars DataFrame）
            indicator_types: 指标类型列表，默认计算所有指标
            **params: 指标计算参数

        Returns:
            pl.DataFrame: 包含所有计算结果的数据
        """
        raise NotImplementedError("子类必须实现此方法")

    def get_supported_indicators(self) -> List[str]:
        """获取支持的技术指标列表

        Returns:
            List[str]: 支持的技术指标列表
        """
        raise NotImplementedError("子类必须实现此方法")

    def is_indicator_supported(self, indicator_type: str) -> bool:
        """检查是否支持指定的技术指标

        Args:
            indicator_type: 指标类型

        Returns:
            bool: 是否支持
        """
        raise NotImplementedError("子类必须实现此方法")

    def clear_calculation_cache(self, indicator_type: str | None = None) -> bool:
        """清除指标计算缓存

        Args:
            indicator_type: 指标类型，None表示清除所有指标缓存

        Returns:
            bool: 清除是否成功
        """
        raise NotImplementedError("子类必须实现此方法")

