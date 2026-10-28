#!/usr/bin/env python3

"""
技术分析器，用于计算各种技术指标
"""

# 第三方库导入
from typing import List

import pandas as pd
import polars as pl
from loguru import logger

# 内部模块导入
from src.api.business_api import ITechnicalAnalyzer

from .analyzer.cache_ops import IndicatorCacheMixin
from .analyzer.parallel_ops import ParallelIndicatorMixin
from .analyzer.plugin_ops import PluginIndicatorMixin
from .indicator_cache import global_indicator_cache
from .indicator_calculator import (
    calculate_boll_polars,
    calculate_kdj_polars,
    calculate_ma_polars,
    calculate_macd_polars,
    calculate_rsi_polars,
    calculate_vol_ma_polars,
    calculate_wr_polars,
    preprocess_data_polars,
    sample_data_polars,
)


class TechnicalAnalyzer(ParallelIndicatorMixin, PluginIndicatorMixin, IndicatorCacheMixin, ITechnicalAnalyzer):
    """
    技术分析器类，提供各种技术指标的计算方法
    实现了ITechnicalAnalyzer接口
    """

    def __init__(self, data, plugin_manager=None):
        """
        初始化技术分析器

        Args:
            data: 股票数据，可以是Polars DataFrame、LazyFrame或Pandas DataFrame
            plugin_manager: 插件管理器实例，用于加载和使用指标插件
        """
        # 保存Polars DataFrame或LazyFrame作为主要数据结构
        self.pl_df = None
        self.is_lazy = False

        if hasattr(data, 'collect'):
            # 输入是Polars LazyFrame
            self.pl_df = data
            self.is_lazy = True
        elif hasattr(data, 'to_pandas'):
            # 输入是Polars DataFrame
            self.pl_df = data
            self.is_lazy = False
        elif isinstance(data, pd.DataFrame):
            # 输入是Pandas DataFrame，转换为Polars
            self.pl_df = pl.from_pandas(data)
            self.is_lazy = False
        else:
            # 输入是其他格式，转换为Polars
            self.pl_df = pl.DataFrame(data)
            self.is_lazy = False

        # 使用Polars进行数据预处理
        self._preprocess_data_polars()

        # 保存插件管理器实例
        self.plugin_manager = plugin_manager

        # 添加指标计算状态跟踪
        self.calculated_indicators = {
            'ma': set(),  # 已计算的MA窗口
            'macd': False,
            'rsi': set(),  # 已计算的RSI窗口
            'kdj': set(),  # 已计算的KDJ窗口
            'vol_ma': set(),  # 已计算的成交量MA窗口
            'boll': set(),  # 已计算的Boll窗口
            'wr': set(),  # 已计算的WR窗口
            'dmi': set(),  # 已计算的DMI窗口
            'cci': set(),  # 已计算的CCI窗口
            'roc': set(),  # 已计算的ROC窗口
            'mtm': set(),  # 已计算的MTM窗口
            'obv': False,  # 已计算的OBV
            'vr': set(),  # 已计算的VR窗口
            'psy': set(),  # 已计算的PSY窗口
            'trix': set(),  # 已计算的TRIX窗口
            'brar': set(),  # 已计算的BRAR窗口
            'asi': set(),  # 已计算的ASI窗口
            'emv': set(),  # 已计算的EMV窗口
            'mcst': set(),  # 已计算的MCST窗口
            'plugin': {}  # 已计算的插件指标，键为插件名称，值为已计算参数的集合
        }

        # 添加缓存机制，避免重复计算
        self._calculate_cache = {}
        # 使用Polars原生方法计算数据哈希
        self._data_hash = self._calculate_polars_data_hash()

        # 初始化指标映射，便于统一管理
        self.indicator_mapping = {
            'ma': self.calculate_ma,
            'macd': self.calculate_macd,
            'rsi': self.calculate_rsi,
            'kdj': self.calculate_kdj,
            'vol_ma': self.calculate_vol_ma,
            'boll': self.calculate_boll,
            'wr': self.calculate_wr,
            'dmi': self.calculate_indicator_parallel,
            'cci': self.calculate_indicator_parallel,
            'roc': self.calculate_indicator_parallel,
            'mtm': self.calculate_indicator_parallel,
            'obv': self.calculate_indicator_parallel,
            'vr': self.calculate_indicator_parallel,
            'psy': self.calculate_indicator_parallel,
            'trix': self.calculate_indicator_parallel,
            'brar': self.calculate_indicator_parallel,
            'asi': self.calculate_indicator_parallel,
            'emv': self.calculate_indicator_parallel,
            'mcst': self.calculate_indicator_parallel,
            'dma': self.calculate_indicator_parallel,
            'fsl': self.calculate_indicator_parallel,
            'sar': self.calculate_indicator_parallel,
            'vol_tdx': self.calculate_indicator_parallel,
            'cr': self.calculate_indicator_parallel,
            # 新增指标
            'expma': self.calculate_indicator_parallel,
            'bbi': self.calculate_indicator_parallel,
            'hsl': self.calculate_indicator_parallel,
            'lb': self.calculate_indicator_parallel,
            'cyc': self.calculate_indicator_parallel,
            'cys': self.calculate_indicator_parallel,
            # 大势型指标
            'abi': self.calculate_indicator_parallel,
            'adl': self.calculate_indicator_parallel,
            'adr': self.calculate_indicator_parallel,
            'obos': self.calculate_indicator_parallel
        }

        # 初始化插件指标映射
        self._init_plugin_indicator_mapping()

    def _calculate_polars_data_hash(self):
        """
        使用轻量级方法计算数据哈希，优化性能

        通过行数、首尾行数据和关键列的简单统计量生成哈希，
        避免计算大量统计量和字符串转换开销

        Returns:
            int: 唯一的数据哈希值
        """
        df = self.pl_df

        if self.is_lazy:
            # 对于LazyFrame，使用列名和数据类型生成哈希
            schema = df.schema
            hash_components = [str(schema)]
            return hash(tuple(hash_components))
        else:
            # 对于DataFrame，使用更高效的哈希生成方法
            row_count = df.height

            if row_count == 0:
                return 0

            key_cols = ['open', 'high', 'low', 'close', 'volume']

            # 只使用行数和首尾行数据，减少计算开销
            hash_components = [row_count]

            # 只获取必要的列，减少内存使用
            key_df = df.select(key_cols)

            # 获取首行数据
            first_row = key_df.slice(0, 1)
            if not first_row.is_empty():
                first_values = tuple(first_row.row(0))
                hash_components.append(hash(first_values))

            # 获取末行数据
            last_row = key_df.slice(-1, 1)
            if not last_row.is_empty():
                last_values = tuple(last_row.row(0))
                hash_components.append(hash(last_values))

            return hash(tuple(hash_components))

    def _preprocess_data_polars(self):
        """
        使用Polars进行数据预处理
        - 检查必要列是否存在
        - 转换为数值类型
        - 处理缺失值
        """
        if self.is_lazy:
            # 对于LazyFrame，我们不检查列是否存在，避免触发计算
            # 列检查会在实际执行时由Polars自动处理
            pass
        else:
            # 对于DataFrame，使用常规预处理
            self.pl_df = preprocess_data_polars(self.pl_df)

    def _calculate_window_based_indicator(self, indicator_type, calc_func, windows):
        """
        通用窗口指标计算方法

        Args:
            indicator_type: 指标类型
            calc_func: 具体的计算函数
            windows: 窗口列表

        Returns:
            pl.DataFrame: 包含计算结果的Polars DataFrame
        """
        # 确保windows是列表
        if not isinstance(windows, list):
            windows = [windows]

        # 只计算尚未计算过的窗口
        windows_to_calculate = [w for w in windows if w not in self.calculated_indicators[indicator_type]]

        if windows_to_calculate:
            # 尝试从全局缓存获取结果
            cached_result = None
            try:
                # 生成缓存键
                params = {'windows': windows_to_calculate}
                cached_result = global_indicator_cache.get(self.pl_df, indicator_type, **params)
            except (ValueError, TypeError) as e:
                logger.debug(f"从缓存获取{indicator_type}失败: {e}")

            if cached_result is not None:
                # 缓存命中，使用缓存结果
                logger.info(f"{indicator_type}缓存命中，避免重复计算")
                # 只合并主表尚未存在的列，避免重复列（_right）膨胀
                new_columns = [
                    col for col in cached_result.columns if col not in self.pl_df.columns
                ]
                if new_columns:
                    if self.is_lazy:
                        self.pl_df = self.pl_df.join(
                            cached_result.lazy().select(new_columns), on=['date'], how='left'
                        )
                    else:
                        self.pl_df = self.pl_df.join(
                            cached_result.select(new_columns), on=['date'], how='left'
                        )
            else:
                # 缓存未命中，执行计算
                # 调用具体的计算函数
                self.pl_df = calc_func(self.pl_df, windows_to_calculate)

                # 检查返回类型，更新is_lazy状态
                if hasattr(self.pl_df, 'collect'):
                    self.is_lazy = True
                else:
                    self.is_lazy = False

                # 将计算结果存入缓存
                try:
                    params = {'windows': windows_to_calculate}
                    # 如果是LazyFrame，先执行计算再缓存
                    cache_data = self.pl_df.collect() if self.is_lazy else self.pl_df
                    global_indicator_cache.set(cache_data, cache_data, indicator_type, **params)
                    logger.debug(f"{indicator_type}计算结果已缓存")
                except (ValueError, TypeError) as e:
                    logger.debug(f"缓存{indicator_type}结果失败: {e}")

            # 更新计算状态
            self.calculated_indicators[indicator_type].update(windows_to_calculate)

        # 执行惰性计算并返回Polars DataFrame
        if self.is_lazy:
            self.pl_df = self.pl_df.collect()
            self.is_lazy = False
        return self.pl_df

    def _calculate_simple_indicator(self, indicator_type, calc_func, **kwargs):
        """
        通用非窗口指标计算方法

        Args:
            indicator_type: 指标类型
            calc_func: 具体的计算函数
            **kwargs: 计算参数

        Returns:
            pl.DataFrame: 包含计算结果的Polars DataFrame
        """
        # 检查是否已经计算过该指标
        if not self.calculated_indicators[indicator_type]:
            # 尝试从全局缓存获取结果
            cached_result = None
            try:
                cached_result = global_indicator_cache.get(self.pl_df, indicator_type, **kwargs)
            except (ValueError, TypeError) as e:
                logger.debug(f"从缓存获取{indicator_type}失败: {e}")

            if cached_result is not None:
                # 缓存命中，使用缓存结果
                logger.info(f"{indicator_type}缓存命中，避免重复计算")
                # 只合并主表尚未存在的列，避免重复列（_right）膨胀
                new_columns = [
                    col for col in cached_result.columns if col not in self.pl_df.columns
                ]
                if new_columns:
                    if self.is_lazy:
                        self.pl_df = self.pl_df.join(
                            cached_result.lazy().select(new_columns), on=['date'], how='left'
                        )
                    else:
                        self.pl_df = self.pl_df.join(
                            cached_result.select(new_columns), on=['date'], how='left'
                        )
            else:
                # 缓存未命中，执行计算
                # 调用具体的计算函数
                self.pl_df = calc_func(self.pl_df, **kwargs)

                # 检查返回类型，更新is_lazy状态
                if hasattr(self.pl_df, 'collect'):
                    self.is_lazy = True
                else:
                    self.is_lazy = False

                # 将计算结果存入缓存
                try:
                    # 如果是LazyFrame，先执行计算再缓存
                    cache_data = self.pl_df.collect() if self.is_lazy else self.pl_df
                    global_indicator_cache.set(cache_data, cache_data, indicator_type, **kwargs)
                    logger.debug(f"{indicator_type}计算结果已缓存")
                except (ValueError, TypeError) as e:
                    logger.debug(f"缓存{indicator_type}结果失败: {e}")

            # 更新计算状态
            self.calculated_indicators[indicator_type] = True

        # 执行惰性计算并返回Polars DataFrame
        if self.is_lazy:
            self.pl_df = self.pl_df.collect()
            self.is_lazy = False
        return self.pl_df

    def calculate_macd(self, fast_period=12, slow_period=26, signal_period=9):
        """
        计算MACD指标

        Args:
            fast_period: 快速EMA周期
            slow_period: 慢速EMA周期
            signal_period: 信号线EMA周期

        Returns:
            pd.DataFrame: 包含MACD指标的DataFrame
        """
        return self._calculate_simple_indicator('macd', calculate_macd_polars, fast_period=fast_period, slow_period=slow_period, signal_period=signal_period)

    def calculate_kdj(self, windows=14):
        """
        计算KDJ指标

        Args:
            windows: KDJ计算窗口，支持单个窗口或窗口列表

        Returns:
            pd.DataFrame: 包含KDJ指标的DataFrame
        """
        return self._calculate_window_based_indicator('kdj', calculate_kdj_polars, windows)

    def calculate_boll(self, windows=None, std_dev=2.0):
        """
        计算Boll指标（布林带）

        Args:
            windows: Boll计算窗口，支持单个窗口或窗口列表
            std_dev: 标准差倍数，默认为2.0

        Returns:
            pl.DataFrame: 包含Boll指标的Polars DataFrame
        """
        # 确保windows是列表
        if windows is None:
            windows = [20]
        if not isinstance(windows, list):
            windows = [windows]

        # 只计算尚未计算过的窗口
        windows_to_calculate = [w for w in windows if w not in self.calculated_indicators['boll']]

        if windows_to_calculate:
            # 计算Boll指标
            self.pl_df = calculate_boll_polars(self.pl_df, windows_to_calculate, std_dev)

            # 更新计算状态
            self.calculated_indicators['boll'].update(windows_to_calculate)

        # 执行惰性计算并返回Polars DataFrame
        if self.is_lazy:
            self.pl_df = self.pl_df.collect()
            self.is_lazy = False
        return self.pl_df

    def calculate_wr(self, windows=None):
        """
        计算WR指标（威廉指标）

        Args:
            windows: WR计算窗口，支持单个窗口或窗口列表，默认[10, 6]（通达信风格）

        Returns:
            pd.DataFrame: 包含WR指标的DataFrame
        """
        # 通达信默认使用WR10和WR6
        if windows is None:
            windows = [10, 6]

        return self._calculate_window_based_indicator('wr', calculate_wr_polars, windows)

    def sample_data(self, target_points=1000, strategy='uniform', return_polars=True):
        """
        对数据进行采样，减少数据量，提高图表渲染速度

        Args:
            target_points: 目标采样点数
            strategy: 采样策略，可选值：'uniform'（均匀采样）、'time_weighted'（时间加权采样）
            return_polars: 是否返回Polars DataFrame

        Returns:
            pl.DataFrame或pd.DataFrame: 采样后的数据
        """
        current_data = self.get_data(return_polars=True)
        sampled_data = sample_data_polars(current_data, target_points, strategy)
        return sampled_data if return_polars else sampled_data.to_pandas()

    def get_data(self, return_polars=True, sample=False, sample_params=None):
        """
        获取包含所有计算指标的数据

        Args:
            return_polars: 是否返回Polars DataFrame，默认返回Polars DataFrame
            sample: 是否对数据进行采样
            sample_params: 采样参数，字典类型，包含target_points和strategy

        Returns:
            pl.DataFrame或pd.DataFrame: 包含所有指标的数据
        """
        # 执行惰性计算
        if self.is_lazy:
            self.pl_df = self.pl_df.collect()
            self.is_lazy = False

        data = self.pl_df if return_polars else self.pl_df.to_pandas()

        if sample:
            sample_params = sample_params or {}
            target_points = sample_params.get('target_points', 1000)
            strategy = sample_params.get('strategy', 'uniform')
            data = self.sample_data(target_points=target_points, strategy=strategy, return_polars=return_polars)

        return data

    def calculate_ma(self, windows=None):
        """
        计算移动平均线

        Args:
            windows: 移动平均窗口列表

        Returns:
            pd.DataFrame: 包含移动平均线的DataFrame
        """
        if windows is None:
            windows = [5, 10, 20, 60]
        return self._calculate_window_based_indicator('ma', calculate_ma_polars, windows)

    def calculate_rsi(self, windows=14):
        """
        计算RSI指标

        Args:
            windows: RSI计算窗口，支持单个窗口或窗口列表

        Returns:
            pd.DataFrame: 包含RSI指标的DataFrame
        """
        return self._calculate_window_based_indicator('rsi', calculate_rsi_polars, windows)

    def calculate_vol_ma(self, windows=None):
        """
        计算成交量移动平均线

        Args:
            windows: 移动平均窗口列表

        Returns:
            pd.DataFrame: 包含成交量移动平均线的DataFrame
        """
        if windows is None:
            windows = [5, 10]
        return self._calculate_window_based_indicator('vol_ma', calculate_vol_ma_polars, windows)

    def get_supported_indicators(self) -> List[str]:
        """
        获取支持的技术指标列表

        Returns:
            List[str]: 支持的技术指标列表
        """
        # 内置指标
        builtin_indicators = ['ma', 'rsi', 'kdj', 'vol_ma', 'wr', 'macd', 'dmi', 'cci', 'roc', 'mtm', 'obv', 'vr', 'psy', 'trix', 'brar', 'asi', 'emv', 'mcst', 'abi', 'adl', 'adr', 'obos']

        # 插件指标
        plugin_indicators = self.get_available_plugin_indicators()

        return builtin_indicators + plugin_indicators

    def is_indicator_supported(self, indicator_type: str) -> bool:
        """
        检查是否支持指定的技术指标

        Args:
            indicator_type: 指标类型

        Returns:
            bool: 是否支持
        """
        supported_indicators = self.get_supported_indicators()
        return indicator_type in supported_indicators
