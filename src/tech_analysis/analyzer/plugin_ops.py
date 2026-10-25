#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
插件指标计算 Mixin
"""

from typing import Any, Dict, Optional

import polars as pl
import pandas as pd
from loguru import logger

from ..indicator_cache import global_indicator_cache


class PluginIndicatorMixin:
    """插件指标计算混入类"""
    
    def _init_plugin_indicator_mapping(self):
        """
        初始化插件指标映射
        """
        if not self.plugin_manager:
            return
        
        # 获取所有可用的指标插件
        available_indicators = self.plugin_manager.get_available_indicator_plugins()
        
        # 为每个指标插件创建对应的计算方法
        for plugin_name in available_indicators:
            # 动态添加指标映射
            self.indicator_mapping[plugin_name] = lambda *args, plugin_name=plugin_name, **kwargs: self.calculate_plugin_indicator(plugin_name, *args, **kwargs)

    def get_available_plugin_indicators(self):
        """
        获取可用的插件指标列表
        
        Returns:
            list: 可用插件指标名称列表
        """
        if not self.plugin_manager:
            return []
        
        return list(self.plugin_manager.get_available_indicator_plugins().keys())

    def _plugin_required_columns(self, plugin):
        """
        计算遗留pandas插件所需的输入列集合（基列 + 插件声明列）。
        插件未实现 get_required_columns 或声明为空时回退为全部列，保证兼容。

        Returns:
            list: 存在于当前数据中的列名列表
        """
        try:
            required = plugin.get_required_columns()
        except Exception:
            required = None
        if not required:
            return self.pl_df.columns
        base = ['date', 'open', 'high', 'low', 'close', 'volume']
        cols = [c for c in dict.fromkeys(base + list(required)) if c in self.pl_df.columns]
        return cols or self.pl_df.columns

    def _build_shared_legacy_source(self, plugin_names):
        """
        为批量遗留pandas插件构建一份裁剪后的共享Polars源帧。
        仅当批次中存在遗留插件且所有遗留插件都成功声明所需列时返回裁剪帧，
        否则返回None（各插件回退为按自身声明单独裁剪）。

        Returns:
            pl.DataFrame or None
        """
        if not self.plugin_manager:
            return None
        try:
            indicator_plugins = self.plugin_manager.get_available_indicator_plugins()
        except Exception:
            return None
        union_cols = []
        legacy_found = False
        for name in plugin_names:
            plugin = indicator_plugins.get(name)
            if plugin is None:
                return None
            if hasattr(plugin, 'supports_polars') and plugin.supports_polars():
                continue
            legacy_found = True
            try:
                required = plugin.get_required_columns()
            except Exception:
                return None
            if not required:
                return None
            union_cols.extend(required)
        if not legacy_found:
            return None
        base = ['date', 'open', 'high', 'low', 'close', 'volume']
        cols = [c for c in dict.fromkeys(base + union_cols) if c in self.pl_df.columns]
        if not cols or len(cols) >= len(self.pl_df.columns):
            return None
        try:
            return self.pl_df.select(cols)
        except Exception as e:
            logger.debug(f"构建遗留插件共享源帧失败，回退逐插件裁剪: {e}")
            return None

    def calculate_plugin_indicator(self, plugin_name, **kwargs):
        """
        计算插件指标
        
        Args:
            plugin_name: 插件名称
            **kwargs: 传递给插件calculate方法的参数
            
        Returns:
            pd.DataFrame: 包含插件指标的DataFrame
        """
        if not self.plugin_manager:
            raise ValueError("插件管理器未初始化")
        
        # 内部参数：批量入口共享的裁剪后源帧（不进入参数哈希与插件调用）
        shared_source_df = kwargs.pop('_source_df', None)

        # 获取指标插件实例
        indicator_plugins = self.plugin_manager.get_available_indicator_plugins()
        if plugin_name not in indicator_plugins:
            raise ValueError(f"指标插件{plugin_name}不存在或未启用")
        
        plugin = indicator_plugins[plugin_name]
        
        # 检查插件指标是否已经计算
        # 生成参数哈希，处理不可哈希类型
        def make_hashable(obj):
            if isinstance(obj, list):
                return tuple(obj)
            elif isinstance(obj, dict):
                return tuple(sorted((k, make_hashable(v)) for k, v in obj.items()))
            elif isinstance(obj, set):
                return frozenset(obj)
            return obj
        
        # 生成参数哈希
        hashable_kwargs = {k: make_hashable(v) for k, v in kwargs.items()}
        params_hash = hash(tuple(sorted(hashable_kwargs.items())))
        
        if plugin_name in self.calculated_indicators['plugin'] and params_hash in self.calculated_indicators['plugin'][plugin_name]:
            # 返回Polars DataFrame
            if self.is_lazy:
                self.pl_df = self.pl_df.collect()
                self.is_lazy = False
            return self.pl_df
        
        # 尝试从全局缓存获取结果
        cached_result = None
        try:
            cached_result = global_indicator_cache.get(self.pl_df, plugin_name, **kwargs)
        except (ValueError, TypeError) as e:
            logger.debug(f"{plugin_name}失败: {e}")
        
        if cached_result is not None:
                # 缓存命中，使用缓存结果
                logger.info(f"插件指标{plugin_name}缓存命中，避免重复计算")
                # 合并缓存结果到主DataFrame
                new_columns = [col for col in cached_result.columns if col not in self.pl_df.columns]
                if new_columns:
                    self.pl_df = self.pl_df.join(cached_result, on=['date'], how='left')
                
                # 更新计算状态
                if plugin_name not in self.calculated_indicators['plugin']:
                    self.calculated_indicators['plugin'][plugin_name] = set()
                self.calculated_indicators['plugin'][plugin_name].add(params_hash)
                
                # 返回Polars DataFrame
                if self.is_lazy:
                    self.pl_df = self.pl_df.collect()
                    self.is_lazy = False
                return self.pl_df
        
        # 检查是否可以使用增量计算
        incremental_data = kwargs.get('incremental_data', None)
        if incremental_data is not None:
            # 检查插件是否支持增量计算
            if hasattr(plugin, 'supports_incremental') and plugin.supports_incremental():
                try:
                    # 使用增量计算
                    logger.info(f"使用增量计算插件指标{plugin_name}")
                    if hasattr(plugin, 'incremental_calculate'):
                        # 调用插件的增量计算方法
                        result_df = plugin.incremental_calculate(self.pl_df, incremental_data, **kwargs)
                    else:
                        # 尝试使用通用增量计算（仅适用于简单指标）
                        # 注意：这是一个降级方案，可能不适用于所有插件
                        logger.warning(f"插件{plugin_name}支持增量计算但未实现incremental_calculate方法")
                        # 回退到全量计算
                        raise NotImplementedError("插件未实现incremental_calculate方法")
                    
                    # 合并增量计算结果
                    self.pl_df = self.pl_df.vstack(result_df)
                    
                    # 将计算结果存入缓存
                    try:
                        # 只缓存新增的列
                        new_columns = [col for col in result_df.columns if col not in ['date', 'open', 'high', 'low', 'close', 'volume']]
                        if new_columns:
                            cache_result = result_df.select(['date'] + new_columns)
                            global_indicator_cache.set(self.pl_df, cache_result, plugin_name, **kwargs)
                            logger.debug(f"插件指标{plugin_name}增量计算结果已缓存")
                    except (ValueError, TypeError) as e:
                        logger.debug(f"插件指标{plugin_name}增量计算结果失败: {e}")
                    
                    # 更新计算状态
                    if plugin_name not in self.calculated_indicators['plugin']:
                        self.calculated_indicators['plugin'][plugin_name] = set()
                    self.calculated_indicators['plugin'][plugin_name].add(params_hash)
                    
                    # 返回Polars DataFrame
                    if self.is_lazy:
                        self.pl_df = self.pl_df.collect()
                        self.is_lazy = False
                    return self.pl_df
                except (RuntimeError, ValueError) as e:
                    logger.warning(f"插件指标{plugin_name}失败，使用常规计算: {e}")

        
        try:
            # 检查插件是否支持polars
            if hasattr(plugin, 'supports_polars') and plugin.supports_polars():
                # 直接传递polars DataFrame给插件
                result_pl = plugin.calculate_polars(self.pl_df, **kwargs)
                
                # 合并结果到主Polars DataFrame，只添加新列
                if result_pl is not None and hasattr(result_pl, 'columns'):
                    new_columns = [col for col in result_pl.columns if col not in self.pl_df.columns]
                    if new_columns:
                        self.pl_df = self.pl_df.with_columns(
                            *[result_pl[col].alias(col) for col in new_columns]
                        )
                    
                    # 将计算结果存入缓存（只缓存新增的列，减少缓存大小）
                    try:
                        # 只缓存新增的列，减少缓存大小
                        cache_result = result_pl.select(['date'] + new_columns)
                        global_indicator_cache.set(self.pl_df, cache_result, plugin_name, **kwargs)
                        logger.debug(f"插件指标{plugin_name}计算结果已缓存")
                    except (ValueError, TypeError) as e:
                        logger.debug(f"插件指标{plugin_name}结果失败: {e}")
                    
                    # 更新计算状态
                    if plugin_name not in self.calculated_indicators['plugin']:
                        self.calculated_indicators['plugin'][plugin_name] = set()
                    self.calculated_indicators['plugin'][plugin_name].add(params_hash)
            else:
                # 旧插件，使用pandas DataFrame
                # 优先使用批量入口共享的裁剪后源帧；否则按插件声明的所需列裁剪，
                # 避免对全量列（含已计算指标列）做 to_pandas 全量转换
                if shared_source_df is not None:
                    df_pd = shared_source_df.to_pandas()
                else:
                    df_pd = self.pl_df.select(self._plugin_required_columns(plugin)).to_pandas()

                # 调用插件的calculate方法
                result_df = plugin.calculate(df_pd, **kwargs)

                # 将结果转换回Polars并合并
                if result_df is not None and isinstance(result_df, pd.DataFrame):
                    # 仅回转插件声明的输出列（+date），避免输入列重复转回Polars
                    try:
                        output_cols = plugin.get_output_columns()
                    except Exception:
                        output_cols = None
                    if output_cols:
                        keep = [c for c in ['date'] + list(output_cols) if c in result_df.columns]
                        if keep:
                            result_df = result_df[keep]
                    # 转换为Polars
                    result_pl = pl.from_pandas(result_df)
                    # 合并到主Polars DataFrame，只添加新列
                    new_columns = [col for col in result_pl.columns if col not in self.pl_df.columns]
                    if new_columns:
                        self.pl_df = self.pl_df.with_columns(
                            *[result_pl[col].alias(col) for col in new_columns]
                        )
                    
                    # 将计算结果存入缓存（只缓存新增的列，减少缓存大小）
                    try:
                        # 只缓存新增的列，减少缓存大小
                        cache_result = result_pl.select(['date'] + new_columns)
                        global_indicator_cache.set(self.pl_df, cache_result, plugin_name, **kwargs)
                        logger.debug(f"插件指标{plugin_name}计算结果已缓存")
                    except (ValueError, TypeError) as e:
                        logger.debug(f"插件指标{plugin_name}结果失败: {e}")
                    
                    # 更新计算状态
                    if plugin_name not in self.calculated_indicators['plugin']:
                        self.calculated_indicators['plugin'][plugin_name] = set()
                    self.calculated_indicators['plugin'][plugin_name].add(params_hash)
        except (ValueError, TypeError, RuntimeError) as e:
            raise RuntimeError(f"计算插件指标{plugin_name}失败: {str(e)}")

        # 返回Polars DataFrame
        if self.is_lazy:
            self.pl_df = self.pl_df.collect()
            self.is_lazy = False
        return self.pl_df

    def calculate_plugin_indicators_parallel(self, plugin_names, *args, **kwargs):
        """
        计算多个插件指标，利用多线程并行计算能力
        
        Args:
            plugin_names: 插件名称列表
            *args: 传递给插件计算方法的位置参数
            **kwargs: 传递给插件计算方法的关键字参数
        
        Returns:
            pd.DataFrame: 包含所有计算结果的DataFrame
        """
        if not plugin_names:
            plugin_names = self.get_available_plugin_indicators()
        
        # 检查是否有可用的插件指标
        if not plugin_names:
            # 返回Polars DataFrame
            if self.is_lazy:
                self.pl_df = self.pl_df.collect()
                self.is_lazy = False
            return self.pl_df
        
        # 导入必要的模块
        import concurrent.futures
        import os
        
        # 根据CPU核心数确定并行度
        cpu_count = os.cpu_count() or 4
        max_workers = min(cpu_count, len(plugin_names))
        
        # 过滤掉已经计算过的插件指标
        plugins_to_calculate = []
        for plugin_name in plugin_names:
            # 生成参数哈希
            params_hash = hash(tuple(sorted(kwargs.items())))
            if plugin_name not in self.calculated_indicators['plugin'] or params_hash not in self.calculated_indicators['plugin'][plugin_name]:
                plugins_to_calculate.append(plugin_name)
        
        if plugins_to_calculate:
            # 为遗留pandas插件预建一份裁剪后的共享Polars源帧（Polars DataFrame 不可变，
            # 线程间共享读取安全）；各线程仅对该小子集做 to_pandas，
            # 避免每个插件都对全量列重复转换。任一插件未声明所需列则放弃共享。
            shared_source_df = self._build_shared_legacy_source(plugins_to_calculate)

            # 使用线程池并行计算插件指标
            results = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有插件指标计算任务
                future_to_plugin = {}
                for plugin_name in plugins_to_calculate:
                    submit_kwargs = dict(kwargs)
                    if shared_source_df is not None:
                        submit_kwargs['_source_df'] = shared_source_df
                    future = executor.submit(
                        self.calculate_plugin_indicator,
                        plugin_name,
                        *args,
                        **submit_kwargs
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

    def invalidate_plugin_cache(self, plugin_name: Optional[str] = None):
        """
        使插件指标缓存失效
        
        Args:
            plugin_name: 可选，指定要失效的插件名称，None表示失效所有插件指标
        """
        try:
            if plugin_name:
                # 使指定插件的缓存失效
                global_indicator_cache.clear(plugin_name)
                logger.info(f"插件{plugin_name}的缓存已失效")
            else:
                # 使所有插件指标的缓存失效
                # 这里我们需要获取所有插件名称并逐个清除
                if self.plugin_manager:
                    plugin_names = self.plugin_manager.get_available_indicator_plugins().keys()
                    for name in plugin_names:
                        global_indicator_cache.clear(name)
                    logger.info(f"所有插件指标的缓存已失效")
        except (ValueError, TypeError) as e:
            logger.debug(f"失败: {e}")

    def get_plugin_cache_stats(self, plugin_name: Optional[str] = None) -> Dict[str, Any]:
        """
        获取插件指标的缓存统计信息
        
        Args:
            plugin_name: 可选，指定要查看的插件名称，None表示查看所有插件指标
        
        Returns:
            Dict[str, Any]: 缓存统计信息
        """
        try:
            cache_info = global_indicator_cache.get_cache_info()
            
            # 过滤出插件指标的缓存条目
            plugin_entries = []
            for entry in cache_info['entries']:
                key = entry['key']
                # 检查是否是插件指标缓存
                if self.plugin_manager:
                    plugin_names = self.plugin_manager.get_available_indicator_plugins().keys()
                    for p_name in plugin_names:
                        if key.startswith(f"{p_name}_"):
                            if plugin_name is None or p_name == plugin_name:
                                plugin_entries.append(entry)
                            break
            
            # 计算插件指标的缓存统计
            plugin_stats = {
                'total_entries': len(plugin_entries),
                'total_hits': sum(entry['hit_count'] for entry in plugin_entries),
                'total_size': sum(entry['size'] for entry in plugin_entries),
                'entries': plugin_entries
            }
            
            return plugin_stats
        except (ValueError, TypeError) as e:
            logger.debug(f"失败: {e}")
            return {'error': str(e)}
