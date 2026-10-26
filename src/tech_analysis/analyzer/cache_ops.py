#!/usr/bin/env python3

"""
指标计算状态与缓存操作 Mixin
"""

from typing import Any, Dict

from loguru import logger

from ..indicator_cache import global_indicator_cache


class IndicatorCacheMixin:
    """指标计算状态与缓存操作混入类"""

    def _generate_cache_key(self, indicator_type, *args, **kwargs):
        """
        生成唯一的缓存键，基于数据哈希和指标参数

        Args:
            indicator_type: 指标类型
            *args: 位置参数
            **kwargs: 关键字参数

        Returns:
            int: 唯一的缓存键
        """
        # 优化缓存键生成，使用更高效的哈希组合
        # 对kwargs进行排序，确保相同参数不同顺序生成相同的键
        sorted_kwargs = tuple(sorted(kwargs.items()))
        # 组合所有参数生成唯一哈希
        return hash((self._data_hash, indicator_type, args, sorted_kwargs))

    def is_indicator_calculated(self, indicator_type, window=None):
        """
        检查特定指标是否已经计算

        Args:
            indicator_type: 指标类型，如'ma', 'macd', 'rsi', 'kdj', 'vol_ma'或插件名称
            window: 对于需要窗口的指标，指定窗口大小

        Returns:
            bool: 如果指标已计算返回True，否则返回False
        """
        # 检查指标类型是否存在
        if indicator_type not in self.calculated_indicators:
            return False

        # 获取指标计算状态
        status = self.calculated_indicators[indicator_type]

        # 根据状态类型判断
        if isinstance(status, set):
            # 窗口型指标，需要检查窗口是否已计算
            return window is not None and window in status
        else:
            # 非窗口型指标，直接返回状态值
            return status

    def reset_calculation(self, indicator_type=None, window=None):
        """
        重置指标计算状态，可选择重置特定指标或所有指标

        Args:
            indicator_type: 要重置的指标类型，None表示重置所有指标
            window: 对于需要窗口的指标，指定要重置的窗口，None表示重置该类型的所有窗口
        """
        def _reset_all_indicators():
            """重置所有指标"""
            # 重新初始化计算状态字典
            self.calculated_indicators = {
                'ma': set(),
                'macd': False,
                'rsi': set(),
                'kdj': set(),
                'vol_ma': set(),
                'boll': set(),
                'wr': set(),
                'dmi': set(),
                'cci': set(),
                'roc': set(),
                'mtm': set(),
                'obv': False,
                'vr': set(),
                'psy': set(),
                'trix': set(),
                'brar': set(),
                'asi': set(),
                'emv': set(),
                'mcst': set(),
                'plugin': {}
            }
            # 重置缓存
            self._calculate_cache.clear()
            # 重置插件指标映射
            self._init_plugin_indicator_mapping()
            # 重置数据哈希
            self._data_hash = self._calculate_polars_data_hash()

            # 重置Polars DataFrame，只保留原始列
            original_columns = ['open', 'high', 'low', 'close', 'volume']
            self.pl_df = self.pl_df.select(original_columns)



        def _reset_plugin_indicator(plugin_name):
            """重置特定插件指标"""
            if plugin_name in self.calculated_indicators['plugin']:
                del self.calculated_indicators['plugin'][plugin_name]
                # 从缓存中删除
                # 注意：这里无法直接删除全局缓存中的条目，因为缓存键包含数据哈希
                # 但我们可以清除本地计算缓存

        def _reset_window_based_indicator(ind_type, win):
            """重置基于窗口的指标"""
            columns_to_drop = []

            if win:
                # 重置特定窗口
                self.calculated_indicators[ind_type].discard(win)
                # 准备删除的列名
                if ind_type == 'vol_ma':
                    columns_to_drop = [f'vol_ma{win}']
                elif ind_type == 'boll':
                    columns_to_drop = [f'mb{win}', f'up{win}', f'dn{win}']
                else:
                    columns_to_drop = [f'{ind_type}{win}']
                # 从缓存中删除
                cache_key = self._generate_cache_key(ind_type, win)
                self._calculate_cache.pop(cache_key, None)
            else:
                # 重置该类型的所有窗口
                windows = list(self.calculated_indicators[ind_type])
                for w in windows:
                    if ind_type == 'vol_ma':
                        columns_to_drop.append(f'vol_ma{w}')
                    elif ind_type == 'boll':
                        columns_to_drop.extend([f'mb{w}', f'up{w}', f'dn{w}'])
                    else:
                        columns_to_drop.append(f'{ind_type}{w}')
                    # 从缓存中删除
                    cache_key = self._generate_cache_key(ind_type, w)
                    self._calculate_cache.pop(cache_key, None)
                # 清空该类型的所有窗口
                self.calculated_indicators[ind_type].clear()
                # 删除默认列名
                if ind_type == 'boll':
                    columns_to_drop.extend(['mb', 'up', 'dn'])
                elif ind_type == 'wr':
                    columns_to_drop.extend(['wr'])

            # 统一删除需要删除的列
            if columns_to_drop:
                columns_to_drop_existing = [col for col in columns_to_drop if col in self.pl_df.columns]
                if columns_to_drop_existing:
                    self.pl_df = self.pl_df.drop(columns_to_drop_existing)

        def _reset_simple_indicator(ind_type):
            """重置简单指标"""
            self.calculated_indicators[ind_type] = False
            # 准备删除的列名
            if ind_type == 'macd':
                columns_to_drop = ['macd', 'macd_signal', 'macd_hist']
            elif ind_type == 'obv':
                columns_to_drop = ['obv']
            else:
                columns_to_drop = [ind_type]

            # 从缓存中删除
            cache_key = self._generate_cache_key(ind_type)
            self._calculate_cache.pop(cache_key, None)

            # 统一删除需要删除的列
            columns_to_drop_existing = [col for col in columns_to_drop if col in self.pl_df.columns]
            if columns_to_drop_existing:
                self.pl_df = self.pl_df.drop(columns_to_drop_existing)

        # 主逻辑
        if not indicator_type:
            # 重置所有指标
            _reset_all_indicators()
        elif self.plugin_manager and indicator_type in self.plugin_manager.get_available_indicator_plugins():
            # 重置特定插件指标
            _reset_plugin_indicator(indicator_type)
        elif indicator_type in self.calculated_indicators:
            if indicator_type in ['ma', 'rsi', 'kdj', 'vol_ma', 'boll', 'wr', 'dmi', 'cci', 'roc', 'mtm', 'vr', 'psy', 'trix', 'brar', 'asi', 'emv', 'mcst']:
                # 重置基于窗口的指标
                _reset_window_based_indicator(indicator_type, window)
            elif indicator_type in ['macd', 'obv']:
                # 重置简单指标
                _reset_simple_indicator(indicator_type)
            elif indicator_type == 'plugin':
                # 重置所有插件指标
                self.calculated_indicators['plugin'].clear()
                # 从缓存中删除所有插件指标
                for cache_key in list(self._calculate_cache.keys()):
                    if cache_key.startswith('plugin_'):
                        del self._calculate_cache[cache_key]

    def get_calculated_indicators(self):
        """
        获取已计算的指标信息

        Returns:
            dict: 包含已计算指标类型和窗口的字典
        """
        return self.calculated_indicators.copy()

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        获取所有指标的缓存统计信息

        Returns:
            Dict[str, Any]: 详细的缓存统计信息
        """
        return global_indicator_cache.get_cache_info()

    def clear_calculation_cache(self, indicator_type: str | None = None) -> bool:
        """
        清除指标计算缓存

        Args:
            indicator_type: 指标类型，None表示清除所有指标缓存

        Returns:
            bool: 清除是否成功
        """
        try:
            if indicator_type:
                # 清除特定指标的缓存
                if indicator_type in self._calculate_cache:
                    del self._calculate_cache[indicator_type]
                # 重置计算状态
                self.reset_calculation(indicator_type)
            else:
                # 清除所有缓存
                self._calculate_cache.clear()
                # 重置所有计算状态
                self.reset_calculation()
            return True
        except (OSError, RuntimeError) as e:
            logger.error(f"清除缓存失败: {str(e)}")
            return False
