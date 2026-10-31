#!/usr/bin/env python3

"""
业务逻辑层接口定义
统一使用Polars DataFrame作为数据格式
"""

from typing import Any, Dict, List, Protocol

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

    def calculate_all_indicators(self, data: pl.DataFrame, indicator_types: List[str] | None = None, **params) -> pl.DataFrame:
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


class IStrategyAnalyzer:
    """策略分析器接口，定义策略分析和回测方法

    预留契约：当前无实现方与调用方（全仓仅本文件引用），保留供未来扩展，
    不要把它当作已生效的约束。
    """

    def backtest(self, data: pl.DataFrame, strategy: Any, **params) -> Dict[str, Any]:
        """回测策略

        Args:
            data: 股票数据（Polars DataFrame）
            strategy: 策略对象或策略名称
            **params: 回测参数

        Returns:
            Dict[str, Any]: 回测结果，包含收益率、最大回撤等指标
        """
        raise NotImplementedError("子类必须实现此方法")

    def evaluate_performance(self, backtest_results: Dict[str, Any]) -> Dict[str, Any]:
        """评估策略绩效

        Args:
            backtest_results: 回测结果

        Returns:
            Dict[str, Any]: 绩效评估结果
        """
        raise NotImplementedError("子类必须实现此方法")

    def optimize_parameters(self, data: pl.DataFrame, strategy: Any, param_space: Dict[str, List[Any]]) -> Dict[str, Any]:
        """优化策略参数

        Args:
            data: 股票数据（Polars DataFrame）
            strategy: 策略对象或策略名称
            param_space: 参数空间，如：{'fast_period': [5, 10, 15], 'slow_period': [20, 25, 30]}

        Returns:
            Dict[str, Any]: 优化结果，包含最优参数和对应的绩效指标
        """
        raise NotImplementedError("子类必须实现此方法")

    def get_supported_strategies(self) -> List[str]:
        """获取支持的策略列表

        Returns:
            List[str]: 支持的策略列表
        """
        raise NotImplementedError("子类必须实现此方法")


class ISignalGenerator:
    """信号生成器接口，定义买卖信号生成方法

    预留契约：当前无实现方与调用方（全仓仅本文件引用），保留供未来扩展，
    不要把它当作已生效的约束。
    """

    def generate_signals(self, data: pl.DataFrame, indicators: List[str] | None = None, **params) -> pl.DataFrame:
        """生成买卖信号

        Args:
            data: 股票数据，包含技术指标（Polars DataFrame）
            indicators: 用于生成信号的指标列表
            **params: 信号生成参数

        Returns:
            pl.DataFrame: 包含信号的数据
        """
        raise NotImplementedError("子类必须实现此方法")

    def get_signal_rules(self) -> List[Dict[str, Any]]:
        """获取信号生成规则

        Returns:
            List[Dict[str, Any]]: 信号生成规则列表
        """
        raise NotImplementedError("子类必须实现此方法")

    def validate_signals(self, signals_data: pl.DataFrame) -> Dict[str, Any]:
        """验证信号有效性

        Args:
            signals_data: 包含信号的数据（Polars DataFrame）

        Returns:
            Dict[str, Any]: 验证结果
        """
        raise NotImplementedError("子类必须实现此方法")


class IFactorAnalyzer:
    """因子分析器接口，定义因子计算和分析方法

    预留契约：当前无实现方与调用方（全仓仅本文件引用），保留供未来扩展，
    不要把它当作已生效的约束。
    """

    def calculate_factor(self, data: pl.DataFrame, factor_type: str, **params) -> pl.DataFrame:
        """计算因子值

        Args:
            data: 股票数据（Polars DataFrame）
            factor_type: 因子类型
            **params: 因子计算参数

        Returns:
            pl.DataFrame: 包含因子值的数据
        """
        raise NotImplementedError("子类必须实现此方法")

    def evaluate_factor(self, factor_data: pl.DataFrame) -> Dict[str, Any]:
        """评估因子有效性

        Args:
            factor_data: 包含因子值的数据（Polars DataFrame）

        Returns:
            Dict[str, Any]: 因子评估结果
        """
        raise NotImplementedError("子类必须实现此方法")

    def get_supported_factors(self) -> List[str]:
        """获取支持的因子列表

        Returns:
            List[str]: 支持的因子列表
        """
        raise NotImplementedError("子类必须实现此方法")

