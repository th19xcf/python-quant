#!/usr/bin/env python3

"""
Alpha因子计算器，用于计算各种Alpha因子

横截面（多标的）计算统一走模块级入口 calculate_factors_cross_sectional：
全表一次性求值，分区由 Polars 的 .over(code_col) 在引擎内部完成，
因子层与指标公式层不做逐组 Python 循环。每个分组仍是因子类设计所针对的
单标的序列，因此结果与逐标的调用完全一致；公式层也不引入双行为模式。
"""

import time
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import polars as pl
from loguru import logger

from src.alpha.factors.factor_registry import FACTOR_REGISTRY

# 保留这些 import：触发 @register_factor 装饰器注册到 FACTOR_REGISTRY
from src.alpha.factors.price_momentum import PriceMomentumFactors  # noqa: F401
from src.alpha.factors.risk import RiskFactors  # noqa: F401
from src.alpha.factors.technical import TechnicalFactors  # noqa: F401
from src.alpha.factors.volatility import VolatilityFactors  # noqa: F401
from src.alpha.factors.volume import VolumeFactors  # noqa: F401
from src.utils.frame_adapter import to_polars

# FactorEvaluator 可能依赖 scipy，缺失时允许降级
try:
    from src.alpha.evaluation.factor_evaluator import FactorEvaluator
except Exception as _exc:  # noqa: BLE001  # pragma: no cover - 仅在 scipy 缺失时触发
    logger.warning(f"FactorEvaluator 加载失败（可能是 scipy 未安装），评估相关方法将跳过：{_exc}")
    FactorEvaluator = None


def _next_period_returns(close: np.ndarray) -> pl.Series:
    """向量化计算下一期收益率序列（长度为 len(close) - 1）。"""
    prev = close[:-1]
    nxt = close[1:]
    with np.errstate(divide='ignore', invalid='ignore'):
        values = (nxt - prev) / prev
    return pl.Series('returns', values)


class AlphaCalculator:
    """
    Alpha因子计算器类，提供各种Alpha因子的计算功能

    因子扩展方式：
      1. 新建 src/alpha/factors/xxx.py，定义因子类并加 @register_factor('xxx')
      2. 在顶部 import 该文件（触发装饰器注册）
      3. 调用 calculate_factors(['xxx']) 即可，无需改本文件代码
    """

    def __init__(self, data, partition_by: str | None = None):
        if isinstance(data, pd.DataFrame):
            self.pl_df = to_polars(data)
        elif hasattr(data, 'to_pandas'):
            self.pl_df = data
        else:
            self.pl_df = pl.DataFrame(data)

        # partition_by: 传标的代码列名时，因子按该列分组求值（单查询计划，无逐组循环）
        self.partition_by = partition_by
        self.evaluator = FactorEvaluator() if FactorEvaluator is not None else None
        self.factor_results = {}

    def calculate_factors(self, factor_types: List[str], **params) -> pl.DataFrame:
        """
        计算指定类型的因子，并把因子列合并回返回 DataFrame

        因子分发走 FACTOR_REGISTRY（装饰器注册），不再硬编码 if/elif。
        """
        for factor_type in factor_types:
            try:
                try:
                    factor_cls = FACTOR_REGISTRY[factor_type]
                except KeyError:
                    available = ', '.join(FACTOR_REGISTRY.names()) or '(空)'
                    logger.warning(
                        f"未注册的因子类型: {factor_type}，可用: {available}")
                    continue

                result = factor_cls(self.pl_df,
                                    partition_by=self.partition_by).calculate(**params)
                self.factor_results[factor_type] = result
                logger.info(f"成功计算因子: {factor_type}")
            # 兜底理由：单个因子计算失败不中断其他因子
            except Exception as e:  # noqa: BLE001
                logger.error(f"计算因子 {factor_type} 失败: {e}")

        # 把各因子类返回的列合并回主表，保证返回值包含所有因子
        if self.factor_results:
            merged = self.pl_df
            for factor_type, factor_df in self.factor_results.items():
                if factor_df is None:
                    continue
                # 因子类可能返回新 DataFrame 或在内部把列加到 self.pl_df
                if isinstance(factor_df, pl.DataFrame):
                    new_cols = [c for c in factor_df.columns if c not in merged.columns]
                    if new_cols:
                        merged = merged.with_columns([factor_df[c] for c in new_cols])
            self.pl_df = merged

        return self.pl_df

    def evaluate_factors(self, factor_names: List[str], target_returns: pl.Series | None = None) -> Dict[str, Any]:
        """
        评估因子有效性

        Args:
            factor_names: 因子名称列表
            target_returns: 目标收益率序列

        Returns:
            Dict[str, Any]: 评估结果
        """
        if target_returns is None:
            # 计算下一期收益率作为目标
            target_returns = _next_period_returns(self.pl_df['close'].to_numpy())
            # 截取数据以匹配目标收益率
            data = self.pl_df.slice(0, len(target_returns))
        else:
            data = self.pl_df

        evaluation_results = {}

        for factor_name in factor_names:
            if factor_name in data.columns:
                factor_values = data[factor_name]
                ic = self.evaluator.calculate_ic(factor_values, target_returns)
                rank_ic = self.evaluator.calculate_rank_ic(factor_values, target_returns)

                evaluation_results[factor_name] = {
                    'ic': ic,
                    'rank_ic': rank_ic
                }
                logger.info(f"因子 {factor_name} 评估完成: IC={ic:.4f}, Rank IC={rank_ic:.4f}")
            else:
                logger.warning(f"因子 {factor_name} 不存在")

        return evaluation_results

    def analyze_factor_correlation(self, factor_names: List[str]) -> Dict[str, Any]:
        """
        分析因子相关性

        Args:
            factor_names: 因子名称列表

        Returns:
            Dict[str, Any]: 相关性分析结果
        """
        # 提取因子数据
        factor_data = []
        valid_factors = []

        for factor_name in factor_names:
            if factor_name in self.pl_df.columns:
                factor_data.append(self.pl_df[factor_name].to_numpy())
                valid_factors.append(factor_name)
            else:
                logger.warning(f"因子 {factor_name} 不存在")

        if not valid_factors:
            return {}

        # 计算相关性矩阵
        correlation_matrix = np.corrcoef(factor_data)

        # 构建相关性字典
        correlation_dict = {}
        for i, factor1 in enumerate(valid_factors):
            correlation_dict[factor1] = {}
            for j, factor2 in enumerate(valid_factors):
                correlation_dict[factor1][factor2] = correlation_matrix[i, j]

        return correlation_dict

    def optimize_factor_weights(self, factor_names: List[str], target_returns: pl.Series | None = None) -> Dict[str, float]:
        """
        优化因子权重

        Args:
            factor_names: 因子名称列表
            target_returns: 目标收益率序列

        Returns:
            Dict[str, float]: 优化后的因子权重
        """
        if target_returns is None:
            # 计算下一期收益率作为目标
            target_returns = _next_period_returns(self.pl_df['close'].to_numpy())
            # 截取数据以匹配目标收益率
            data = self.pl_df.slice(0, len(target_returns))
        else:
            data = self.pl_df

        # 提取因子数据
        factor_data = []
        valid_factors = []

        for factor_name in factor_names:
            if factor_name in data.columns:
                factor_data.append(data[factor_name].to_numpy())
                valid_factors.append(factor_name)
            else:
                logger.warning(f"因子 {factor_name} 不存在")

        if not valid_factors:
            return {}

        # 计算因子与目标收益率的相关性
        correlations = []
        for factor_values in factor_data:
            corr = np.corrcoef(factor_values, target_returns.to_numpy())[0, 1]
            correlations.append(abs(corr))

        # 基于相关性计算权重
        total_corr = sum(correlations)
        weights = {}
        for i, factor_name in enumerate(valid_factors):
            weights[factor_name] = correlations[i] / total_corr if total_corr > 0 else 1.0 / len(valid_factors)

        return weights

    def get_factor_results(self) -> Dict[str, Any]:
        """
        获取因子计算结果

        Returns:
            Dict[str, Any]: 因子计算结果
        """
        return self.factor_results


def calculate_factors_cross_sectional(data,
                                      factor_types: List[str],
                                      code_col: str = 'ts_code',
                                      **params) -> pl.DataFrame:
    """
    横截面（多标的）因子计算统一入口。

    实现方式：全表一次性求值，分区由 Polars 的 .over(code_col) 在引擎内部完成，
    不再有逐标的的 Python 循环，也没有进程派发与结果合并开销。
    结果与逐标的循环调用 AlphaCalculator bit 级一致（已由回归测试覆盖）。

    输入数据需保证同一标的内部按时间升序，且行序不做改动（输出与输入同序）。

    Args:
        data: Polars/pandas DataFrame，必须包含 code_col 列
        factor_types: 按需计算的因子类型子集（注册表中的名字）
        code_col: 标的代码列名，默认 'ts_code'（与数据库口径一致）
        **params: 透传给因子类 calculate() 的参数

    Returns:
        pl.DataFrame: 附加上因子列的完整数据，行序与输入一致

    Raises:
        ValueError: 输入缺少 code_col 列
    """
    df = to_polars(data)
    if not factor_types or df.is_empty():
        return df
    if code_col not in df.columns:
        raise ValueError(
            f"横截面计算缺少标的代码列 '{code_col}'，可用列: {df.columns}")

    t0 = time.perf_counter()
    result = AlphaCalculator(df, partition_by=code_col).calculate_factors(
        factor_types, **params)
    logger.info(
        f"横截面因子计算完成: {df[code_col].n_unique()} 标的 / {df.height} 行 × "
        f"{len(factor_types)} 类因子，耗时 {time.perf_counter() - t0:.2f}s")
    return result
