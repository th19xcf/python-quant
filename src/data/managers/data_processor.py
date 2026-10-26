#!/usr/bin/env python3

"""
数据处理模块

负责数据预处理、清洗、采样等操作

注意：异常值删除会破坏时间序列连续性，回测场景请谨慎使用 remove_outliers=True。
      fill_missing_values 的 backward 和 mean 原实现存在前视偏差，已改为安全版本。
"""

from typing import Any, Dict, Union

import polars as pl
from loguru import logger

from src.utils.memory_optimizer import MemoryOptimizer

# 可安全处理异常值检测的 OHLCV 列名
_OhlcvCols = ('open', 'high', 'low', 'close', 'volume', 'amount')


class DataProcessor:
    """
    数据处理模块，负责数据预处理、清洗、采样等操作
    """

    def __init__(self, config):
        self.config = config

    # ------------------------------------------------------------------
    # 通用预处理
    # ------------------------------------------------------------------
    def preprocess_data(self, data: Union[pl.DataFrame, pl.LazyFrame]) -> Union[pl.DataFrame, pl.LazyFrame]:
        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']
        if not all(col in data.columns for col in required_columns):
            logger.warning(f"数据缺少必要列，当前列: {data.columns}")
            return data
        if 'date' in data.columns:
            data = data.sort('date')
        data = data.unique(subset=['date'])
        return data

    def sample_data(self, data: Union[pl.DataFrame, pl.LazyFrame],
                    target_points: int = 1000, strategy: str = 'adaptive') -> Union[pl.DataFrame, pl.LazyFrame]:
        is_lazy = isinstance(data, pl.LazyFrame)
        if is_lazy:
            return data.nth_sample(target_points)
        if len(data) <= target_points:
            return data
        step = max(1, len(data) // target_points)
        if strategy == 'uniform':
            return data[::step]
        # adaptive / 其他都走简单均匀采样（真实自适应按波动率调整，此处默认）
        return data[::step]

    def convert_data_type(self, data: Union[pl.DataFrame, pl.LazyFrame],
                          target_type: str = 'float32') -> Union[pl.DataFrame, pl.LazyFrame]:
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount', 'pct_chg']
        dtype = pl.Float32 if target_type == 'float32' else pl.Float64
        exprs = [pl.col(c).cast(dtype) for c in numeric_columns if c in data.columns]
        if exprs:
            data = data.with_columns(exprs)
        return data

    # ------------------------------------------------------------------
    # 数据清洗：缺失值 + 异常值
    # ------------------------------------------------------------------
    def clean_data(self, data: Union[pl.DataFrame, pl.LazyFrame],
                   remove_outliers: bool = True, outlier_method: str = 'iqr') -> Union[pl.DataFrame, pl.LazyFrame]:
        data = data.drop_nulls()
        if 'volume' in data.columns:
            data = data.filter(pl.col('volume') > 0)
        if remove_outliers:
            # 注意：异常值删除会整行剔除，破坏时间序列连续性，
            # 且对跨多年的价格做 IQR 会把正常高低点当作异常。
            # 若用于回测，请改用标记（is_outlier 列）或缩尾。
            data = self._detect_and_remove_outliers(data, method=outlier_method)
        return data

    def _detect_and_remove_outliers(self, data: Union[pl.DataFrame, pl.LazyFrame],
                                    method: str = 'iqr', threshold: float = 3.0) -> Union[pl.DataFrame, pl.LazyFrame]:
        """检测并移除异常值，纯 Polars 表达式实现，DataFrame/LazyFrame 通用。

        修复：
          - 原实现逐列 to_numpy()[0][0]：LazyFrame 没有 to_numpy 必然崩溃；
          - 原 IQR 后一列基于前一列过滤后的数据计算分位数，结果依赖列顺序；
          - 原 zscore 中 std 为 None（全空列）时 `std > 0` 抛 TypeError。
        现在各列统计量基于原始数据独立计算，一次 filter 完成，Lazy/DataFrame 都能跑。
        """
        cols = [c for c in _OhlcvCols if c in data.columns]
        if not cols:
            return data

        conds: list[pl.Expr] = []
        for c in cols:
            x = pl.col(c)
            if method == 'iqr':
                q1, q3 = x.quantile(0.25), x.quantile(0.75)
                iqr = q3 - q1
                conds.append(x.is_between(q1 - threshold * iqr, q3 + threshold * iqr))
            elif method == 'zscore':
                std = x.std()
                mean = x.mean()
                # std 为 0 或 null 时不过滤该列（全常数列 / 全空列）
                conds.append(
                    pl.when(std.fill_null(0) > 0)
                    .then((x - mean).abs() <= threshold * std)
                    .otherwise(True)
                )
            else:
                logger.warning(f"不支持的异常值检测方法: {method}，跳过列 {c}")

        if conds:
            data = data.filter(pl.all_horizontal(conds))
        return data

    # ------------------------------------------------------------------
    # 缺失值填充（全部为纯 Polars expr，Lazy/DataFrame 通用）
    # ------------------------------------------------------------------
    def fill_missing_values(self, data: Union[pl.DataFrame, pl.LazyFrame],
                            method: str = 'forward') -> Union[pl.DataFrame, pl.LazyFrame]:
        """填充缺失值。

        修复：
          - backward：原实现 `fill_null(strategy='backward')` 用未来值填过去，回测会引入前视偏差。
            改为 forward，即历史可用值。
          - mean：原实现 `data.select(...).to_numpy()[0][0]` 取的是**全样本**均值，
            在回测中同样用到了未来数据。改为滚动累积均值，只包含截至当前行的已知数据。
          - forward：Polars 原生 expr，本身安全。
        """
        cols = [c for c in _OhlcvCols if c in data.columns]
        if not cols:
            return data

        if method == 'forward':
            exprs = [pl.col(c).fill_null(strategy='forward') for c in cols]
        elif method == 'backward':
            # 警告后降级为 forward，避免前视偏差
            logger.warning(
                "fill_missing_values(method='backward') 会使用未来数据，在回测中引入前视偏差，"
                "已自动降级为 forward。如需真正后向填充，请自行确认时序隔离。"
            )
            exprs = [pl.col(c).fill_null(strategy='forward') for c in cols]
        elif method == 'mean':
            # 滚动累积均值（只包含历史数据），不引入前视
            exprs = [
                pl.col(c).fill_null(
                    pl.col(c).cum_sum() / pl.col(c).is_not_null().cum_sum().clip_min(1)
                )
                for c in cols
            ]
        else:
            logger.warning(f"不支持的填充方法: {method}，保持原样")
            return data

        return data.with_columns(exprs)

    # ------------------------------------------------------------------
    # 数据质量检查（辅助）
    # ------------------------------------------------------------------
    def check_data_quality(self, data: pl.DataFrame) -> Dict[str, Any]:
        """检查数据质量（仅接受 DataFrame，LazyFrame 需先 .collect()）"""
        if isinstance(data, pl.LazyFrame):
            logger.warning("check_data_quality 收到 LazyFrame，自动 collect()")
            data = data.collect()

        report: Dict[str, Any] = {
            'total_rows': data.height,
            'null_values': {},
            'zero_volume_rows': 0,
        }

        # 一次 select 算完所有列的 null_count（不再循环 to_numpy）
        null_exprs = [pl.col(c).is_null().sum().alias(c) for c in data.columns]
        null_row = data.select(null_exprs).to_dicts()[0]
        report['null_values'] = null_row

        if 'volume' in data.columns:
            report['zero_volume_rows'] = data.filter(pl.col('volume') == 0).height

        return report

    # ------------------------------------------------------------------
    # 频率转换
    # ------------------------------------------------------------------
    def convert_frequency(self, df: pl.DataFrame, frequency: str) -> pl.DataFrame:
        if df.is_empty():
            return df
        try:
            lazy_df = df.lazy()
            if 'trade_date' in df.columns:
                lazy_df = lazy_df.with_columns(pl.col('trade_date').alias('date'))
            elif 'date' not in df.columns:
                logger.error("DataFrame中没有日期列")
                return df

            schema_d = lazy_df.collect_schema()
            date_type = schema_d['date']
            if date_type == pl.String:
                lazy_df = lazy_df.with_columns(pl.col('date').str.strptime(pl.Date, "%Y-%m-%d"))
            elif date_type == pl.Datetime:
                lazy_df = lazy_df.with_columns(pl.col('date').dt.date())

            if frequency == '1w':
                lazy_df = lazy_df.with_columns(
                    pl.col('date').dt.week().alias('week'),
                    pl.col('date').dt.year().alias('year'),
                )
                group_cols = ['year', 'week']
            elif frequency == '1m':
                lazy_df = lazy_df.with_columns(
                    pl.col('date').dt.month().alias('month'),
                    pl.col('date').dt.year().alias('year'),
                )
                group_cols = ['year', 'month']
            else:
                return df

            agg = [
                pl.col('date').first(),
                pl.col('open').first(),
                pl.col('high').max(),
                pl.col('low').min(),
                pl.col('close').last(),
                pl.col('volume').sum(),
                pl.col('amount').sum(),
            ]
            for extra in ('pct_chg', 'change'):
                if extra in df.columns:
                    agg.append(pl.col(extra).sum())

            result = (
                lazy_df
                .group_by(group_cols)
                .agg(agg)
                .sort('date')
                .with_columns(pl.col('date').dt.strftime("%Y-%m-%d"))
                .collect()
            )
            optimized = MemoryOptimizer.optimize_dataframe(result, enable_sparse=True)
            logger.info(f"将日线数据转换为{frequency}数据，从{df.height}条转换为{optimized.height}条")
            return optimized
        except (ValueError, TypeError) as e:
            logger.exception(f"转换数据频率失败: {e}")
            return df

    def optimize_memory(self, data: pl.DataFrame, enable_sparse: bool = True) -> pl.DataFrame:
        return MemoryOptimizer.optimize_dataframe(data, enable_sparse=enable_sparse)
