#!/usr/bin/env python3

"""
并行与批量指标计算 Mixin
"""

from typing import List, Union

import pandas as pd
import polars as pl
from loguru import logger

from src.utils.event_bus import EventType, publish

from ..gpu_acceleration import calculate_with_gpu, is_gpu_available
from ..incremental_calculator import global_incremental_calculator
from ..indicator_cache import global_indicator_cache
from ..indicator_calculator import calculate_multiple_indicators_polars
from ..indicator_manager import global_indicator_manager


class ParallelIndicatorMixin:
    """并行与批量指标计算混入类"""

    def calculate_indicator_parallel(self, indicator_type, *args, **kwargs):
        """
        计算特定类型的指标，利用Polars内置并行计算能力

        Args:
            indicator_type: 指标类型，如'ma', 'macd', 'rsi', 'kdj', 'vol_ma'或插件名称
            *args: 传递给指标计算方法的位置参数
            **kwargs: 传递给指标计算方法的关键字参数

        Returns:
            pd.DataFrame: 包含计算指标的DataFrame
        """
        # 检查指标类型是否支持多窗口计算
        if indicator_type in ['ma', 'rsi', 'kdj', 'vol_ma', 'dmi', 'cci', 'roc', 'mtm', 'vr', 'psy', 'trix', 'brar', 'asi', 'emv', 'mcst', 'expma', 'cyc', 'hsl', 'lb']:
            # 对于支持多窗口的指标
            windows = kwargs.get('windows', [14])
            if not isinstance(windows, list):
                windows = [windows]

            # 只计算尚未计算过的窗口
            windows_to_calculate = [w for w in windows if w not in self.calculated_indicators[indicator_type]]

            if windows_to_calculate:
                # 统一走批量函数惰性路径：单计划、单 collect，避免单指标包装
                # 函数 eager 路径的重复执行与内存压缩/打印开销；各指标的窗口
                # 参数名需与 get_indicator_params 的口径一致
                windows_param = {
                    'ma': 'windows',
                    'rsi': 'rsi_windows',
                    'kdj': 'kdj_windows',
                    'vol_ma': 'vol_ma_windows',
                }.get(indicator_type, 'windows')
                lazy_df = calculate_multiple_indicators_polars(
                    self.pl_df.lazy(), [indicator_type], **{windows_param: windows_to_calculate})
                self.pl_df = lazy_df.collect()

                # 更新计算状态
                self.calculated_indicators[indicator_type].update(windows_to_calculate)
        elif indicator_type in ['macd', 'obv']:
            # 对于不支持多窗口的指标，直接使用批量计算函数
            lazy_df = self.pl_df.lazy()
            lazy_df = calculate_multiple_indicators_polars(lazy_df, [indicator_type])
            self.pl_df = lazy_df.collect()

            # 更新计算状态
            self.calculated_indicators[indicator_type] = True


        else:
            # 对于插件指标，调用常规计算方法
            self.calculate_indicator(indicator_type, *args, **kwargs)

        # 返回Polars DataFrame
        if self.is_lazy:
            self.pl_df = self.pl_df.collect()
            self.is_lazy = False
        return self.pl_df

    def calculate_indicators_parallel(self, indicator_types, *args, **kwargs):
        """
        计算多个指标类型，利用多线程并行计算能力

        Args:
            indicator_types: 指标类型列表，如['ma', 'macd', 'rsi', 'kdj', 'vol_ma']或插件名称列表
            *args: 传递给指标计算方法的位置参数
            **kwargs: 传递给指标计算方法的关键字参数

        Returns:
            pd.DataFrame: 包含所有计算指标的DataFrame
        """
        # 分离内置指标和插件指标
        builtin_indicators = []
        plugin_indicators = []

        for indicator_type in indicator_types:
            if self.plugin_manager and indicator_type in self.plugin_manager.get_available_indicator_plugins():
                plugin_indicators.append(indicator_type)
            else:
                builtin_indicators.append(indicator_type)

        # 分离已缓存和未缓存的指标
        cached_indicators = []
        uncached_indicators = []

        for indicator_type in builtin_indicators:
            try:
                cached_result = global_indicator_cache.get(self.pl_df, indicator_type, **kwargs)
                if cached_result is not None:
                    cached_indicators.append(indicator_type)
                    # 合并缓存结果到主DataFrame
                    self.pl_df = self.pl_df.join(cached_result, on=['date'], how='left')
                    logger.info(f"{indicator_type}缓存命中，避免重复计算")
                else:
                    uncached_indicators.append(indicator_type)
            except (ValueError, TypeError) as e:
                logger.debug(f"检查{indicator_type}缓存失败: {e}")
                uncached_indicators.append(indicator_type)

        # 更新已缓存指标的计算状态
        for indicator_type in cached_indicators:
            if indicator_type in self.calculated_indicators:
                if isinstance(self.calculated_indicators[indicator_type], set):
                    windows = kwargs.get(f'{indicator_type}_windows', kwargs.get('windows', [14]))
                    if not isinstance(windows, list):
                        windows = [windows]
                    self.calculated_indicators[indicator_type].update(windows)
                else:
                    self.calculated_indicators[indicator_type] = True

        # 批量计算未缓存的内置指标
        if uncached_indicators:
            try:
                # 使用指标管理器批量计算内置指标
                result_df = global_indicator_manager.calculate_indicators(
                    self.pl_df, uncached_indicators, return_polars=True, **kwargs
                )

                # 更新内部DataFrame
                self.pl_df = result_df

                # 将计算结果存入缓存
                for indicator_type in uncached_indicators:
                    try:
                        indicator_kwargs = {k: v for k, v in kwargs.items() if not k.endswith('_windows')}
                        if f'{indicator_type}_windows' in kwargs:
                            indicator_kwargs['windows'] = kwargs[f'{indicator_type}_windows']
                        elif 'windows' in kwargs:
                            indicator_kwargs['windows'] = kwargs['windows']
                        global_indicator_cache.set(self.pl_df, self.pl_df, indicator_type, **indicator_kwargs)
                        logger.debug(f"{indicator_type}计算结果已缓存")
                    except (ValueError, TypeError) as e:
                        logger.debug(f"缓存{indicator_type}结果失败: {e}")

                # 更新计算状态
                for indicator_type in uncached_indicators:
                    if indicator_type in self.calculated_indicators:
                        if isinstance(self.calculated_indicators[indicator_type], set):
                            windows = kwargs.get(f'{indicator_type}_windows', kwargs.get('windows', [14]))
                            if not isinstance(windows, list):
                                windows = [windows]
                            self.calculated_indicators[indicator_type].update(windows)
                        else:
                            self.calculated_indicators[indicator_type] = True
            except (ValueError, TypeError, RuntimeError) as e:
                logger.error(f"计算内置指标失败: {e}")

        # 并行计算插件指标
        if plugin_indicators:
            # 导入必要的模块
            import concurrent.futures
            import os

            # 根据CPU核心数确定并行度
            cpu_count = os.cpu_count() or 4
            max_workers = min(cpu_count, len(plugin_indicators))

            # 过滤掉已经计算过的插件指标
            plugins_to_calculate = []
            for plugin_name in plugin_indicators:
                # 生成参数哈希
                params_hash = hash(tuple(sorted(kwargs.items())))
                if plugin_name not in self.calculated_indicators['plugin'] or params_hash not in self.calculated_indicators['plugin'][plugin_name]:
                    plugins_to_calculate.append(plugin_name)

            if plugins_to_calculate:
                # 使用线程池并行计算插件指标
                results = []

                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # 提交所有插件指标计算任务
                    future_to_plugin = {}
                    for plugin_name in plugins_to_calculate:
                        future = executor.submit(
                            self.calculate_plugin_indicator,
                            plugin_name,
                            **kwargs
                        )
                        future_to_plugin[future] = plugin_name

                    # 收集计算结果
                    for future in concurrent.futures.as_completed(future_to_plugin):
                        plugin_name = future_to_plugin[future]
                        try:
                            future.result()
                            results.append(plugin_name)
                        except Exception as e:
                            logger.error(f"计算插件指标{plugin_name}失败: {e}")

                logger.info(f"并行计算完成，成功计算{len(results)}个插件指标")
            else:
                logger.info("所有插件指标已计算，无需重复计算")



        # 返回Polars DataFrame
        if self.is_lazy:
            self.pl_df = self.pl_df.collect()
            self.is_lazy = False
        return self.pl_df

    def calculate_all_indicators(self, data: Union[pl.DataFrame, pd.DataFrame] | None = None, indicator_types: List[str] | None = None, **params) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        计算多个技术指标
        使用多指标批量计算框架，充分利用Polars的Lazy API和并行计算能力

        Args:
            data: 股票数据，None表示使用现有数据
            indicator_types: 指标类型列表，默认计算所有指标
            **params: 指标计算参数

        Returns:
            pl.DataFrame或pd.DataFrame: 包含所有计算结果的数据
        """
        # 保存复权列，避免在指标计算过程中丢失
        adj_columns = ['qfq_factor', 'hfq_factor']
        adj_data = {}

        # 如果传入了新数据，更新内部数据
        if data is not None:
            if hasattr(data, 'to_pandas'):
                # 输入是Polars DataFrame
                self.pl_df = data
            elif isinstance(data, pd.DataFrame):
                # 输入是Pandas DataFrame，转换为Polars
                self.pl_df = pl.from_pandas(data)
            else:
                # 输入是其他格式，转换为Polars
                self.pl_df = pl.DataFrame(data)

            # 保存复权列数据
            for col in adj_columns:
                if col in self.pl_df.columns:
                    adj_data[col] = self.pl_df[col]

            # 使用Polars进行数据预处理
            self._preprocess_data_polars()

            # 重新计算数据哈希
            self._data_hash = self._calculate_polars_data_hash()

            # 使相关缓存失效
            try:
                # 使所有指标缓存失效
                global_indicator_cache.invalidate(self.pl_df)
                logger.debug("数据更新，所有指标缓存已失效")
            except (ValueError, TypeError) as e:
                logger.error(f"失效失败: {e}")


        # 1. 确定要计算的指标类型
        if indicator_types is None:
            # 默认计算所有指标
            indicator_types = ['ma', 'rsi', 'kdj', 'vol_ma', 'wr', 'macd', 'dmi', 'cci', 'roc', 'mtm', 'obv', 'vr', 'psy', 'trix', 'brar', 'asi', 'emv', 'mcst']

        # 2. 准备需要计算的内置指标类型
        builtin_indicators = [ind for ind in indicator_types if ind in ['ma', 'rsi', 'kdj', 'vol_ma', 'wr', 'macd', 'dmi', 'cci', 'roc', 'mtm', 'obv', 'vr', 'psy', 'trix', 'brar', 'asi', 'emv', 'mcst', 'abi', 'adl', 'adr', 'obos']]

        # 3. 使用新的批量计算函数进行内置指标计算
        success = True
        error_message = ""

        if builtin_indicators:
            try:
                # 使用新的批量计算函数，将所有指标计算合并到单个查询计划
                lazy_df = self.pl_df.lazy()
                lazy_df = calculate_multiple_indicators_polars(lazy_df, builtin_indicators, **params)

                # 执行计算并更新主DataFrame
                self.pl_df = lazy_df.collect()

                # 4. 更新计算状态
                # 使用字典映射替代长if-elif链，减少重复代码
                indicator_update_map = {
                    'ma': {'param_key': 'windows', 'default': [5, 10, 20, 60], 'update_type': 'windows'},
                    'rsi': {'param_key': 'rsi_windows', 'default': [14], 'update_type': 'windows'},
                    'kdj': {'param_key': 'kdj_windows', 'default': [14], 'update_type': 'windows'},
                    'vol_ma': {'param_key': 'vol_ma_windows', 'default': [5, 10], 'update_type': 'windows'},
                    'wr': {'param_key': 'wr_windows', 'default': [10, 6], 'update_type': 'windows'},
                    'macd': {'update_type': 'boolean'},
                    'dmi': {'param_key': 'windows', 'default': [14], 'update_type': 'windows'},
                    'cci': {'param_key': 'windows', 'default': [14], 'update_type': 'windows'},
                    'roc': {'param_key': 'windows', 'default': [12], 'update_type': 'windows'},
                    'mtm': {'param_key': 'windows', 'default': [12], 'update_type': 'windows'},
                    'obv': {'update_type': 'boolean'},
                    'vr': {'param_key': 'windows', 'default': [24], 'update_type': 'windows'},
                    'psy': {'param_key': 'windows', 'default': [12], 'update_type': 'windows'},
                    'trix': {'param_key': 'windows', 'default': [12], 'update_type': 'windows'},
                    'brar': {'param_key': 'windows', 'default': [26], 'update_type': 'windows'},
                    'asi': {'param_key': 'windows', 'default': [14], 'update_type': 'windows'},
                    'emv': {'param_key': 'windows', 'default': [14], 'update_type': 'windows'},
                    'mcst': {'param_key': 'windows', 'default': [12], 'update_type': 'windows'}
                }

                for indicator in builtin_indicators:
                    if indicator in indicator_update_map:
                        update_info = indicator_update_map[indicator]
                        if update_info['update_type'] == 'windows':
                            windows = params.get(update_info['param_key'], update_info['default'])
                            self.calculated_indicators[indicator].update(windows)
                        elif update_info['update_type'] == 'boolean':
                            self.calculated_indicators[indicator] = True

                # 5. 清除转换缓存，因为数据已更新
                self._pandas_cache = None
                self._pandas_cache_hash = None
            except (ValueError, TypeError, RuntimeError) as e:
                success = False
                error_message = f"计算内置指标失败: {str(e)}"
                logger.error(error_message)
                # 发布错误事件
                publish(
                    EventType.INDICATOR_ERROR,
                    data_type='stock',
                    indicators=builtin_indicators,
                    error=error_message
                )

        # 6. 计算插件指标
        plugin_indicators = self.get_available_plugin_indicators()
        for plugin_name in plugin_indicators:
            if indicator_types is None or plugin_name in indicator_types:
                try:
                    # 直接调用插件计算
                    self.calculate_plugin_indicator(plugin_name, **params)
                except (ValueError, TypeError, RuntimeError) as e:
                    logger.error(f"计算插件指标{plugin_name}时发生错误: {str(e)}")

        # 7. 恢复复权列到结果中
        if adj_data:
            try:
                # 构建需要添加的列表达式列表
                columns_to_add = []
                for col, values in adj_data.items():
                    if col not in self.pl_df.columns:
                        columns_to_add.append(values.alias(col))

                # 一次性添加所有缺失的复权列
                if columns_to_add:
                    self.pl_df = self.pl_df.with_columns(columns_to_add)
                    logger.debug(f"恢复复权列: {list(adj_data.keys())}")
            except (ValueError, TypeError) as e:
                logger.warning(f"恢复复权列时出错: {e}")

        # 8. 发布指标计算完成事件
        publish(
            EventType.INDICATOR_CALCULATED,
            data_type='stock',
            indicators=indicator_types,
            calculated_indicators=self.calculated_indicators.copy(),
            success=success,
            error=error_message if not success else None
        )

        # 返回结果，根据参数决定返回类型（默认返回Polars，避免全量to_pandas转换开销）
        return_polars = params.get('return_polars', True)
        return self.pl_df if return_polars else self.pl_df.to_pandas()

    def calculate_indicator(self, indicator_type, *args, **kwargs):
        """
        计算特定类型的指标

        Args:
            indicator_type: 指标类型，如'ma', 'macd', 'rsi', 'kdj', 'vol_ma'或插件名称
            *args: 传递给指标计算方法的位置参数
            **kwargs: 传递给指标计算方法的关键字参数

        Returns:
            pd.DataFrame: 包含计算指标的DataFrame
        """
        # 检查是否为插件指标
        if self.plugin_manager and indicator_type in self.plugin_manager.get_available_indicator_plugins():
            # 调用插件指标计算方法
            return self.calculate_plugin_indicator(indicator_type, **kwargs)

        # 尝试从全局缓存获取结果
        cached_result = None
        try:
            cached_result = global_indicator_cache.get(self.pl_df, indicator_type, **kwargs)
        except (ValueError, TypeError) as e:
            logger.debug(f"{indicator_type}失败: {e}")

        if cached_result is not None:
            # 缓存命中，使用缓存结果
            logger.info(f"{indicator_type}缓存命中，避免重复计算")
            # 合并缓存结果到主DataFrame
            self.pl_df = self.pl_df.join(cached_result, on=['date'], how='left')

            # 更新计算状态
            if indicator_type in self.calculated_indicators:
                if isinstance(self.calculated_indicators[indicator_type], set):
                    windows = kwargs.get('windows', [14])
                    if not isinstance(windows, list):
                        windows = [windows]
                    self.calculated_indicators[indicator_type].update(windows)
                else:
                    self.calculated_indicators[indicator_type] = True

            # 返回Polars DataFrame
            if self.is_lazy:
                self.pl_df = self.pl_df.collect()
                self.is_lazy = False
            return self.pl_df

        # 检查是否可以使用GPU加速计算
        if is_gpu_available():
            try:
                # 准备GPU计算所需的数据
                df = self.pl_df.collect() if self.is_lazy else self.pl_df
                data = {
                    'close': df['close'].to_numpy(),
                    'high': df['high'].to_numpy() if 'high' in df.columns else None,
                    'low': df['low'].to_numpy() if 'low' in df.columns else None
                }

                # 使用GPU加速计算
                logger.info(f"使用GPU加速计算{indicator_type}")
                gpu_result = calculate_with_gpu(indicator_type, data, **kwargs)

                if gpu_result:
                    # GPU计算成功，将结果合并到DataFrame
                    for col_name, values in gpu_result.items():
                        self.pl_df = self.pl_df.with_columns(
                            pl.Series(col_name, values).alias(col_name)
                        )

                    # 将计算结果存入缓存
                    try:
                        global_indicator_cache.set(self.pl_df, self.pl_df, indicator_type, **kwargs)
                        logger.debug(f"{indicator_type}GPU计算结果已缓存")
                    except (ValueError, TypeError) as e:
                        logger.warning(f"{indicator_type}GPU计算结果失败: {e}")


                    # 更新计算状态
                    if indicator_type in self.calculated_indicators:
                        if isinstance(self.calculated_indicators[indicator_type], set):
                            windows = kwargs.get('windows', [14])
                            if not isinstance(windows, list):
                                windows = [windows]
                            self.calculated_indicators[indicator_type].update(windows)
                        else:
                            self.calculated_indicators[indicator_type] = True

                    # 返回Polars DataFrame
                    if self.is_lazy:
                        self.pl_df = self.pl_df.collect()
                        self.is_lazy = False
                    return self.pl_df
            except (RuntimeError, ValueError) as e:
                logger.warning(f"{indicator_type}失败，使用常规计算: {e}")


        # 检查是否可以使用增量计算
        incremental_data = kwargs.get('incremental_data')
        if incremental_data is not None and global_incremental_calculator.is_supported(indicator_type):
            try:
                # 使用增量计算
                logger.info(f"使用增量计算{indicator_type}")
                result_df = global_incremental_calculator.incremental_calculate(
                    indicator_type, self.pl_df, incremental_data, **kwargs
                )

                # 合并增量计算结果
                # 确保列数匹配
                if result_df.columns != self.pl_df.columns:
                    # 调整列顺序和缺失列
                    for col in self.pl_df.columns:
                        if col not in result_df.columns:
                            result_df = result_df.with_columns(pl.Series([None] * len(result_df)).alias(col))
                    # 按照历史数据的列顺序重新排列
                    result_df = result_df.select(self.pl_df.columns)
                self.pl_df = self.pl_df.vstack(result_df)

                # 将计算结果存入缓存
                try:
                    global_indicator_cache.set(self.pl_df, self.pl_df, indicator_type, **kwargs)
                    logger.debug(f"{indicator_type}增量计算结果已缓存")
                except (ValueError, TypeError) as e:
                    logger.debug(f"{indicator_type}增量计算结果失败: {e}")

                # 更新计算状态
                if indicator_type in self.calculated_indicators:
                    if isinstance(self.calculated_indicators[indicator_type], set):
                        windows = kwargs.get('windows', [14])
                        if not isinstance(windows, list):
                            windows = [windows]
                        self.calculated_indicators[indicator_type].update(windows)
                    else:
                        self.calculated_indicators[indicator_type] = True

                # 返回Polars DataFrame
                if self.is_lazy:
                    self.pl_df = self.pl_df.collect()
                    self.is_lazy = False
                return self.pl_df
            except (RuntimeError, ValueError) as e:
                logger.warning(f"{indicator_type}失败，使用常规计算: {e}")


        # 缓存未命中且无法GPU/增量计算，使用指标管理器计算内置指标
        try:
            # 移除incremental_data参数，因为指标计算函数不支持
            kwargs_copy = kwargs.copy()
            kwargs_copy.pop('incremental_data', None)
            # 使用指标管理器计算指标
            result_df = global_indicator_manager.calculate_indicator(
                self.pl_df, indicator_type, return_polars=True, **kwargs_copy
            )

            # 更新内部DataFrame
            self.pl_df = result_df

            # 将计算结果存入缓存
            try:
                global_indicator_cache.set(self.pl_df, self.pl_df, indicator_type, **kwargs)
                logger.debug(f"{indicator_type}计算结果已缓存")
            except (ValueError, TypeError) as e:
                logger.debug(f"{indicator_type}结果失败: {e}")

            # 更新计算状态
            if indicator_type in self.calculated_indicators:
                if isinstance(self.calculated_indicators[indicator_type], set):
                    windows = kwargs.get('windows', [14])
                    if not isinstance(windows, list):
                        windows = [windows]
                    self.calculated_indicators[indicator_type].update(windows)
                else:
                    self.calculated_indicators[indicator_type] = True

            # 返回Polars DataFrame
            if self.is_lazy:
                self.pl_df = self.pl_df.collect()
                self.is_lazy = False
            return self.pl_df
        except ValueError as e:
            raise ValueError(f"不支持的指标类型: {indicator_type}") from e
