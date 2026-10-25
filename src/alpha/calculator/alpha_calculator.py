#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Alpha因子计算器，用于计算各种Alpha因子

横截面（多标的）计算统一走模块级入口 calculate_factors_cross_sectional：
按标的列物理分区 → 进程池分块并行 → 行序还原。因子类与指标公式层不做任何
分组感知改造——每个分区就是因子类设计所针对、且被现有测试覆盖的单标的
时间序列，正确性由构造保证，避免 .over() 给公式层引入双行为模式。
"""

import math
import os
import time
from concurrent.futures import ProcessPoolExecutor
from typing import List, Dict, Optional, Any
import polars as pl
import pandas as pd
import numpy as np
from loguru import logger

# 保留这些 import：触发 @register_factor 装饰器注册到 FACTOR_REGISTRY
from src.alpha.factors.price_momentum import PriceMomentumFactors  # noqa: F401
from src.alpha.factors.volatility import VolatilityFactors          # noqa: F401
from src.alpha.factors.volume import VolumeFactors                  # noqa: F401
from src.alpha.factors.technical import TechnicalFactors            # noqa: F401
from src.alpha.factors.risk import RiskFactors                      # noqa: F401
from src.alpha.factors.factor_registry import FACTOR_REGISTRY

# FactorEvaluator 可能依赖 scipy，缺失时允许降级
try:
    from src.alpha.evaluation.factor_evaluator import FactorEvaluator
except Exception as _exc:  # pragma: no cover - 仅在 scipy 缺失时触发
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

    def __init__(self, data):
        if isinstance(data, pd.DataFrame):
            self.pl_df = pl.from_pandas(data)
        elif hasattr(data, 'to_pandas'):
            self.pl_df = data
        else:
            self.pl_df = pl.DataFrame(data)

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

                result = factor_cls(self.pl_df).calculate(**params)
                self.factor_results[factor_type] = result
                logger.info(f"成功计算因子: {factor_type}")
            except Exception as e:
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
    
    def evaluate_factors(self, factor_names: List[str], target_returns: Optional[pl.Series] = None) -> Dict[str, Any]:
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
    
    def optimize_factor_weights(self, factor_names: List[str], target_returns: Optional[pl.Series] = None) -> Dict[str, float]:
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


_ROW_IDX_COL = '__cs_row_idx'


def _compute_partitions(partitions: List[pl.DataFrame],
                        factor_types: List[str],
                        params: Dict[str, Any]) -> Optional[pl.DataFrame]:
    """
    对若干单标的分区依次计算因子并纵向合并。

    模块级函数，供 ProcessPoolExecutor pickle 调用；子进程通过 import 本模块
    完成因子注册，与主进程同一代码路径。

    Args:
        partitions: 单标的分区列表（每个分区恰好一只标的）
        factor_types: 因子类型列表
        params: 透传给因子类 calculate() 的参数

    Returns:
        合并后的 DataFrame；空输入返回 None
    """
    results = []
    for part in partitions:
        results.append(AlphaCalculator(part).calculate_factors(factor_types, **params))
    if not results:
        return None
    if len(results) == 1:
        return results[0]
    # vertical_relaxed：分区间列数/类型可能存在细微差异，按列名对齐
    return pl.concat(results, how='vertical_relaxed')


def calculate_factors_cross_sectional(data,
                                      factor_types: List[str],
                                      code_col: str = 'ts_code',
                                      n_workers: Optional[int] = None,
                                      chunks_per_worker: int = 4,
                                      **params) -> pl.DataFrame:
    """
    横截面（多标的）因子计算统一入口。

    实现方式：按 code_col 物理分区为单标的序列 → 分块派发到进程池 →
    合并并按输入行序还原。每个分区走与单标的完全相同的因子计算路径，
    因此横截面结果与逐标的循环调用 AlphaCalculator 完全一致。

    输入数据需保证同一标的内部按时间升序（与单标的路径同一前置条件）。

    Args:
        data: Polars/pandas DataFrame，必须包含 code_col 列
        factor_types: 按需计算的因子类型子集（注册表中的名字）
        code_col: 标的代码列名，默认 'ts_code'（与数据库口径一致）
        n_workers: 进程池大小；1 为进程内串行。
            None 时自适应：总行数 ≥ 20 万用满 CPU 核数（不超过分区数），
            否则串行——Windows spawn 固定开销约 1.5s，小规模并行反而更慢
        chunks_per_worker: 每 worker 的 chunk 数，用于摊薄进程派发与 pickle 开销
        **params: 透传给因子类 calculate() 的参数

    Returns:
        pl.DataFrame: 附加上因子列的完整数据，行序与输入一致

    Raises:
        ValueError: 输入缺少 code_col 列
    """
    if isinstance(data, pd.DataFrame):
        df = pl.from_pandas(data)
    else:
        df = data
    if not factor_types or df.is_empty():
        return df
    if code_col not in df.columns:
        raise ValueError(
            f"横截面计算缺少标的代码列 '{code_col}'，可用列: {df.columns}")

    t0 = time.perf_counter()
    # 记录原始行序，最终还原（允许输入乱序/非连续）
    df = df.with_row_index(_ROW_IDX_COL)
    partitions = df.partition_by(code_col, maintain_order=True)

    workers = n_workers
    if workers is None:
        # 自适应：spawn 固定开销约 1.5s，仅在大批量时启用进程池
        workers = (os.cpu_count() or 1) if df.height >= 200_000 else 1
    workers = max(1, min(workers, len(partitions)))

    if workers == 1:
        merged = _compute_partitions(partitions, factor_types, params)
    else:
        # 连续切块：chunk 数 = workers × chunks_per_worker，且不超过分区数
        n_chunks = min(len(partitions), workers * max(1, chunks_per_worker))
        chunk_size = math.ceil(len(partitions) / n_chunks)
        chunks = [partitions[i:i + chunk_size]
                  for i in range(0, len(partitions), chunk_size)]
        with ProcessPoolExecutor(max_workers=workers) as pool:
            parts = list(pool.map(_compute_partitions,
                                  chunks,
                                  [factor_types] * len(chunks),
                                  [params] * len(chunks)))
        parts = [p for p in parts if p is not None]
        merged = (parts[0] if len(parts) == 1
                  else pl.concat(parts, how='vertical_relaxed'))

    result = (merged.sort(_ROW_IDX_COL).drop(_ROW_IDX_COL)
              if merged is not None else df.drop(_ROW_IDX_COL))
    logger.info(
        f"横截面因子计算完成: {len(partitions)} 标的 × {len(factor_types)} 类因子，"
        f"workers={workers}，耗时 {time.perf_counter() - t0:.2f}s")
    return result
