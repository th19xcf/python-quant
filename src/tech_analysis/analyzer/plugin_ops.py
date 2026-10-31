#!/usr/bin/env python3

"""
插件指标计算 Mixin
"""

from typing import Any, Dict

import pandas as pd
import polars as pl
from loguru import logger

from src.utils.frame_adapter import to_output, to_polars

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
        # 兜底理由：插件为外部扩展代码，异常类型不可预知
        except Exception:  # noqa: BLE001
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
        # 兜底理由：插件为外部扩展代码，异常类型不可预知
        except Exception:  # noqa: BLE001
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
            # 兜底理由：插件为外部扩展代码，异常类型不可预知
            except Exception:  # noqa: BLE001
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
        # 兜底理由：列裁剪失败回退逐插件裁剪
        except Exception as e:  # noqa: BLE001
            logger.debug(f"构建遗留插件共享源帧失败，回退逐插件裁剪: {e}")
            return None

    def calculate_plugin_indicator(self, plugin_name, **kwargs):
        """
        计算插件指标

        Args:
            plugin_name: 插件名称
            **kwargs: 传递给插件calculate方法的参数

        Returns:
            pl.DataFrame: 包含插件指标的DataFrame
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

        # 生成参数哈希（处理不可哈希类型）
        params_hash = self._plugin_params_hash(kwargs)

        # 已计算过：直接返回
        if self._plugin_cache_hit(plugin_name, params_hash):
            return self._finalize_polars()

        # 尝试从全局缓存获取结果
        cached_result = None
        try:
            cached_result = global_indicator_cache.get(self.pl_df, plugin_name, **kwargs)
        except (ValueError, TypeError) as e:
            logger.debug(f"{plugin_name}失败: {e}")

        if cached_result is not None:
            self._apply_cached_result(plugin_name, cached_result, params_hash)
            return self._finalize_polars()

        # 检查是否可以使用增量计算
        if self._calculate_plugin_incremental(
            plugin, plugin_name, kwargs.get('incremental_data'), kwargs, params_hash
        ):
            return self._finalize_polars()

        self._calculate_with_plugin(plugin, plugin_name, shared_source_df, kwargs, params_hash)
        return self._finalize_polars()

    @staticmethod
    def _make_hashable(obj):
        """把 list/dict/set 递归转成可哈希结构。"""
        if isinstance(obj, list):
            return tuple(obj)
        if isinstance(obj, dict):
            return tuple(sorted((k, PluginIndicatorMixin._make_hashable(v)) for k, v in obj.items()))
        if isinstance(obj, set):
            return frozenset(obj)
        return obj

    @classmethod
    def _plugin_params_hash(cls, kwargs) -> int:
        """根据插件参数生成哈希，用于判断同一指标是否已计算过。"""
        hashable_kwargs = {k: cls._make_hashable(v) for k, v in kwargs.items()}
        return hash(tuple(sorted(hashable_kwargs.items())))

    def _plugin_cache_hit(self, plugin_name: str, params_hash: int) -> bool:
        """当前参数组合是否已在 calculated_indicators 中登记。"""
        plugin_state = self.calculated_indicators['plugin']
        return plugin_name in plugin_state and params_hash in plugin_state[plugin_name]

    def _mark_plugin_calculated(self, plugin_name: str, params_hash: int) -> None:
        """登记插件指标的计算状态。"""
        plugin_state = self.calculated_indicators['plugin']
        if plugin_name not in plugin_state:
            plugin_state[plugin_name] = set()
        plugin_state[plugin_name].add(params_hash)

    def _finalize_polars(self):
        """若当前帧仍是 LazyFrame，先 collect 再返回。"""
        if self.is_lazy:
            self.pl_df = self.pl_df.collect()
            self.is_lazy = False
        return self.pl_df

    def _apply_cached_result(self, plugin_name: str, cached_result, params_hash: int) -> None:
        """把全局缓存命中的结果按 date 左连接进主帧。"""
        logger.info(f"插件指标{plugin_name}缓存命中，避免重复计算")
        new_columns = [col for col in cached_result.columns if col not in self.pl_df.columns]
        if new_columns:
            self.pl_df = self.pl_df.join(cached_result, on=['date'], how='left')
        self._mark_plugin_calculated(plugin_name, params_hash)

    def _calculate_plugin_incremental(
        self, plugin, plugin_name: str, incremental_data, kwargs, params_hash: int
    ) -> bool:
        """尝试增量计算；成功返回 True，不支持或失败返回 False（调用方回退全量）。"""
        if incremental_data is None:
            return False
        if not (hasattr(plugin, 'supports_incremental') and plugin.supports_incremental()):
            return False
        try:
            logger.info(f"使用增量计算插件指标{plugin_name}")
            if hasattr(plugin, 'incremental_calculate'):
                result_df = plugin.incremental_calculate(self.pl_df, incremental_data, **kwargs)
            else:
                # 尝试使用通用增量计算（仅适用于简单指标）
                # 注意：这是一个降级方案，可能不适用于所有插件
                logger.warning(f"插件{plugin_name}支持增量计算但未实现incremental_calculate方法")
                # 回退到全量计算
                raise NotImplementedError("插件未实现incremental_calculate方法")

            self.pl_df = self.pl_df.vstack(result_df)
            self._cache_plugin_result(plugin_name, result_df, kwargs)
            self._mark_plugin_calculated(plugin_name, params_hash)
            return True
        except (RuntimeError, ValueError) as e:
            logger.warning(f"插件指标{plugin_name}失败，使用常规计算: {e}")
            return False

    def _calculate_with_plugin(
        self, plugin, plugin_name: str, shared_source_df, kwargs, params_hash: int
    ) -> None:
        """走插件全量计算（优先 Polars，回退遗留 pandas）。"""
        try:
            if hasattr(plugin, 'supports_polars') and plugin.supports_polars():
                result_pl = plugin.calculate_polars(self.pl_df, **kwargs)
                if result_pl is not None and hasattr(result_pl, 'columns'):
                    self._merge_polars_result(plugin_name, result_pl, kwargs, params_hash)
            else:
                self._calculate_legacy_pandas(
                    plugin, plugin_name, shared_source_df, kwargs, params_hash
                )
        except (ValueError, TypeError, RuntimeError) as e:
            raise RuntimeError(f"计算插件指标{plugin_name}失败: {str(e)}") from e

    def _calculate_legacy_pandas(
        self, plugin, plugin_name: str, shared_source_df, kwargs, params_hash: int
    ) -> None:
        """遗留 pandas 插件：裁剪输入列 → 计算 → 按输出列回转 Polars。"""
        # 优先使用批量入口共享的裁剪后源帧；否则按插件声明的所需列裁剪，
        # 避免对全量列（含已计算指标列）做 to_pandas 全量转换
        if shared_source_df is not None:
            df_pd = to_output(shared_source_df, return_polars=False)
        else:
            df_pd = to_output(
                self.pl_df.select(self._plugin_required_columns(plugin)), return_polars=False
            )

        result_df = plugin.calculate(df_pd, **kwargs)
        if result_df is None or not isinstance(result_df, pd.DataFrame):
            return

        # 仅回转插件声明的输出列（+date），避免输入列重复转回Polars
        try:
            output_cols = plugin.get_output_columns()
        # 兜底理由：插件为外部扩展代码，异常类型不可预知
        except Exception:  # noqa: BLE001
            output_cols = None
        if output_cols:
            keep = [c for c in ['date'] + list(output_cols) if c in result_df.columns]
            if keep:
                result_df = result_df[keep]
        self._merge_polars_result(plugin_name, to_polars(result_df), kwargs, params_hash)

    def _merge_polars_result(self, plugin_name: str, result_pl, kwargs, params_hash: int) -> None:
        """把插件结果的新增列并入主帧，并写入缓存与计算状态。"""
        new_columns = [col for col in result_pl.columns if col not in self.pl_df.columns]
        if new_columns:
            self.pl_df = self.pl_df.with_columns(
                *[result_pl[col].alias(col) for col in new_columns]
            )
        self._cache_plugin_result(plugin_name, result_pl, kwargs)
        self._mark_plugin_calculated(plugin_name, params_hash)

    def _cache_plugin_result(self, plugin_name: str, result_pl, kwargs) -> None:
        """把结果中新增的列写入全局指标缓存（失败仅记录日志）。"""
        try:
            new_columns = [
                col for col in result_pl.columns
                if col not in ['date', 'open', 'high', 'low', 'close', 'volume']
            ]
            if new_columns:
                cache_result = result_pl.select(['date'] + new_columns)
                global_indicator_cache.set(
                    self.pl_df, cache_result, plugin_name, **kwargs
                )
                logger.debug(f"插件指标{plugin_name}计算结果已缓存")
        except (ValueError, TypeError) as e:
            logger.debug(f"插件指标{plugin_name}结果失败: {e}")

    def _select_plugins_to_calculate(self, plugin_names, kwargs):
        """过滤掉已经计算过的插件指标。"""
        plugins_to_calculate = []
        for plugin_name in plugin_names:
            # 生成参数哈希
            params_hash = hash(tuple(sorted(kwargs.items())))
            if plugin_name not in self.calculated_indicators['plugin'] or params_hash not in self.calculated_indicators['plugin'][plugin_name]:
                plugins_to_calculate.append(plugin_name)
        return plugins_to_calculate

    def _run_plugin_calculations(self, plugins_to_calculate, shared_source_df, args, kwargs, max_workers):
        """线程池并行计算插件指标，逐笔记录失败但不影响其余。"""
        import concurrent.futures

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
                # 兜底理由：单个插件指标计算失败不影响其余
                except Exception as e:  # noqa: BLE001
                    logger.error(f"计算插件指标{plugin_name}失败: {e}")

        logger.info(f"并行计算完成，成功计算{len(results)}个插件指标")

    def calculate_plugin_indicators_parallel(self, plugin_names, *args, **kwargs):
        """
        计算多个插件指标，利用多线程并行计算能力

        Args:
            plugin_names: 插件名称列表
            *args: 传递给插件计算方法的位置参数
            **kwargs: 传递给插件计算方法的关键字参数

        Returns:
            pl.DataFrame: 包含所有计算结果的DataFrame
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
        import os

        # 根据CPU核心数确定并行度
        cpu_count = os.cpu_count() or 4
        max_workers = min(cpu_count, len(plugin_names))

        # 过滤掉已经计算过的插件指标
        plugins_to_calculate = self._select_plugins_to_calculate(plugin_names, kwargs)

        if plugins_to_calculate:
            # 为遗留pandas插件预建一份裁剪后的共享Polars源帧（Polars DataFrame 不可变，
            # 线程间共享读取安全）；各线程仅对该小子集做 to_pandas，
            # 避免每个插件都对全量列重复转换。任一插件未声明所需列则放弃共享。
            shared_source_df = self._build_shared_legacy_source(plugins_to_calculate)
            self._run_plugin_calculations(
                plugins_to_calculate, shared_source_df, args, kwargs, max_workers
            )
        else:
            logger.info("所有插件指标已计算，无需重复计算")

        # 返回Polars DataFrame
        if self.is_lazy:
            self.pl_df = self.pl_df.collect()
            self.is_lazy = False
        return self.pl_df

    def invalidate_plugin_cache(self, plugin_name: str | None = None):
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
                    logger.info("所有插件指标的缓存已失效")
        except (ValueError, TypeError) as e:
            logger.debug(f"失败: {e}")

    def get_plugin_cache_stats(self, plugin_name: str | None = None) -> Dict[str, Any]:
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
