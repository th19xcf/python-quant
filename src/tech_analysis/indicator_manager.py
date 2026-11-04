#!/usr/bin/env python3

"""
指标管理器模块，集成指标注册中心和缓存机制
提供统一的指标计算和管理接口
"""

import re
import threading
from typing import Any, Callable, Dict, List, Union

import pandas as pd
import polars as pl
from loguru import logger

from src.tech_analysis.indicator_cache import global_indicator_cache
from src.tech_analysis.indicator_calculator import (
    calculate_asi_polars,
    calculate_boll_polars,
    calculate_brar_polars,
    calculate_cci_polars,
    calculate_cr_polars,
    calculate_dma_polars,
    calculate_dmi_polars,
    calculate_emv_polars,
    calculate_fsl_polars,
    calculate_kdj_polars,
    calculate_ma_polars,
    calculate_macd_polars,
    calculate_mcst_polars,
    calculate_mtm_polars,
    calculate_multiple_indicators_polars,
    calculate_obv_polars,
    calculate_psy_polars,
    calculate_roc_polars,
    calculate_rsi_polars,
    calculate_sar_polars,
    calculate_trix_polars,
    calculate_vol_ma_polars,
    calculate_vr_polars,
    calculate_wr_polars,
    preprocess_data_polars,
)
from src.tech_analysis.indicator_registry import (
    global_indicator_registry,
    register_indicator,
)
from src.tech_analysis.indicators.cost import calculate_cyc, calculate_cys
from src.tech_analysis.indicators.market_breadth import (
    calculate_abi,
    calculate_adl,
    calculate_adr,
    calculate_obos,
)
from src.tech_analysis.indicators.trend import calculate_bbi, calculate_expma
from src.tech_analysis.indicators.volume import calculate_hsl, calculate_lb
from src.utils.frame_adapter import to_output, to_polars


def _calculate_hsl_with_turnover(df: pl.DataFrame, **kwargs) -> pl.DataFrame:
    """HSL：按「逐日股本 > 静态股本 > baostock turn」分级取真实换手率。

    真实值来自 ``turn`` 列（百分数）；``ts_code`` 由调用方透传，用于按票取表。
    """
    turn_frame = kwargs.get("turn_frame")
    ts_code = kwargs.get("ts_code")
    lazy = df.lazy()
    if turn_frame is None and ts_code:
        try:
            from src.data.local_store.turnover import attach_turn
            lazy = attach_turn(lazy, ts_code)
        except (ImportError, OSError, ValueError, TypeError,
                pl.exceptions.PolarsError) as exc:
            logger.debug(f"附加换手率失败({ts_code}): {exc}")
    return calculate_hsl(lazy, kwargs.get("float_share"), turn_frame).collect()


# 各指标产出列的真实命名规则（fullmatch 匹配，避免前缀误伤）
# 例如 dmi 的列是 pdi_14/ndi_14/adx_14/adxr_14，而 ma 的前缀会误吞 macd_* 列
_INDICATOR_COLUMN_PATTERNS: Dict[str, tuple[str, ...]] = {
    'ma': (r'ma', r'ma\d+'),
    'vol_ma': (r'vol_ma', r'vol_ma\d+'),
    'macd': ('macd', 'macd_signal', 'macd_hist'),
    'rsi': (r'rsi', r'rsi\d+'),
    'kdj': (r'k', r'd', r'j', r'k\d+', r'd\d+', r'j\d+'),
    'wr': (r'wr', r'wr\d+', 'wr2'),
    'boll': ('mb', 'up', 'dn', r'mb\d+', r'up\d+', r'dn\d+'),
    'dmi': (
        'pdi', 'ndi', 'adx', 'adxr',
        r'pdi_\d+', r'ndi_\d+', r'adx_\d+', r'adxr_\d+', r'dx_\d+',
    ),
    'cci': (r'cci', r'cci\d+'),
    'roc': (r'roc', r'roc\d+'),
    'mtm': (r'mtm', r'mtm\d+'),
    'obv': ('obv',),
    'vr': (r'vr', r'vr\d+'),
    'psy': (r'psy', r'psy\d+'),
    'trix': ('trix', 'trma', r'trix\d+', r'trma\d+'),
    'brar': ('ar', 'br', r'ar\d+', r'br\d+'),
    'asi': ('asi', 'asi_sig'),
    'emv': ('emv', r'emv\d+'),
    'mcst': ('mcst', r'mcst_ma\d+'),
    'dma': ('dma', 'ama'),
    'fsl': ('swl', 'sws'),
    'sar': ('sar',),
    'cr': ('cr', r'cr\d+'),
    'expma': ('expma', r'expma\d+'),
    'bbi': ('bbi',),
    'hsl': ('hsl', r'hsl_ma\d+', 'hsl_source'),
    'lb': ('lb',),
    'cyc': ('cyc', 'cyc_inf', r'cyc\d+'),
    'cys': ('cys', r'cys_ma\d+'),
    'abi': (r'abi\d+',),
    'adl': ('adl',),
    'adr': (r'adr\d+',),
    'obos': (r'obos\d+',),
}


def _extract_indicator_columns(columns: List[str], indicator_type: str) -> List[str]:
    """按指标声明列名抽取结果列（未知指标返回空列表，不写缓存）。"""
    patterns = _INDICATOR_COLUMN_PATTERNS.get(indicator_type)
    if not patterns:
        return []
    return [col for col in columns if any(re.fullmatch(p, col) for p in patterns)]


# 内置指标声明表：名称、计算函数、默认参数、描述、分类
# 集中声明后由 _register_builtin_indicators 统一注册，避免 300+ 行重复的 register_indicator 调用
_BUILTIN_INDICATORS: tuple[Dict[str, Any], ...] = (
    {
        "name": "ma",
        "calculate_func": calculate_ma_polars,
        "params": {"windows": [5, 10, 20, 60]},
        "description": "移动平均线",
        "category": "趋势指标",
    },
    {
        "name": "vol_ma",
        "calculate_func": calculate_vol_ma_polars,
        "params": {"windows": [5, 10]},
        "description": "成交量移动平均线",
        "category": "成交量指标",
    },
    {
        "name": "macd",
        "calculate_func": calculate_macd_polars,
        "params": {"fast_period": 12, "slow_period": 26, "signal_period": 9},
        "description": "平滑异同移动平均线",
        "category": "趋势指标",
    },
    {
        "name": "rsi",
        "calculate_func": calculate_rsi_polars,
        "params": {"windows": [14]},
        "description": "相对强弱指标",
        "category": "震荡指标",
    },
    {
        "name": "kdj",
        "calculate_func": calculate_kdj_polars,
        "params": {"windows": [14]},
        "description": "随机指标",
        "category": "震荡指标",
    },
    {
        "name": "wr",
        "calculate_func": calculate_wr_polars,
        "params": {"windows": [10, 6]},
        "description": "威廉指标",
        "category": "震荡指标",
    },
    {
        "name": "boll",
        "calculate_func": calculate_boll_polars,
        "params": {"windows": [20], "std_dev": 2.0},
        "description": "布林带",
        "category": "趋势指标",
    },
    {
        "name": "dmi",
        "calculate_func": calculate_dmi_polars,
        "params": {"windows": [14]},
        "description": "趋向指标",
        "category": "趋势指标",
    },
    {
        "name": "cci",
        "calculate_func": calculate_cci_polars,
        "params": {"windows": [14]},
        "description": "商品通道指标",
        "category": "震荡指标",
    },
    {
        "name": "roc",
        "calculate_func": calculate_roc_polars,
        "params": {"windows": [12]},
        "description": "变化率指标",
        "category": "震荡指标",
    },
    {
        "name": "mtm",
        "calculate_func": calculate_mtm_polars,
        "params": {"windows": [12]},
        "description": "动量指标",
        "category": "震荡指标",
    },
    {
        "name": "obv",
        "calculate_func": calculate_obv_polars,
        "params": {},
        "description": "能量潮",
        "category": "成交量指标",
    },
    {
        "name": "vr",
        "calculate_func": calculate_vr_polars,
        "params": {"windows": [26]},
        "description": "成交量比率",
        "category": "成交量指标",
    },
    {
        "name": "psy",
        "calculate_func": calculate_psy_polars,
        "params": {"windows": [12]},
        "description": "心理线",
        "category": "情绪指标",
    },
    {
        "name": "trix",
        "calculate_func": calculate_trix_polars,
        "params": {"windows": [12], "signal_period": 9},
        "description": "三重指数平滑",
        "category": "趋势指标",
    },
    {
        "name": "brar",
        "calculate_func": calculate_brar_polars,
        "params": {"windows": [26]},
        "description": "情绪指标",
        "category": "情绪指标",
    },
    {
        "name": "asi",
        "calculate_func": calculate_asi_polars,
        "params": {"signal_period": 20},
        "description": "振动升降指标",
        "category": "趋势指标",
    },
    {
        "name": "emv",
        "calculate_func": calculate_emv_polars,
        "params": {"windows": [14], "constant": 100000000},
        "description": "简易波动指标",
        "category": "趋势指标",
    },
    {
        "name": "mcst",
        "calculate_func": calculate_mcst_polars,
        "params": {"windows": [12]},
        "description": "市场成本",
        "category": "成本指标",
    },
    {
        "name": "dma",
        "calculate_func": calculate_dma_polars,
        "params": {"short_period": 10, "long_period": 50, "signal_period": 10},
        "description": "平均线差",
        "category": "趋势指标",
    },
    {
        "name": "fsl",
        "calculate_func": calculate_fsl_polars,
        "params": {},
        "description": "分水岭指标",
        "category": "趋势指标",
    },
    {
        "name": "sar",
        "calculate_func": calculate_sar_polars,
        "params": {"af_step": 0.02, "max_af": 0.2},
        "description": "抛物线转向指标",
        "category": "趋势指标",
    },
    {
        "name": "cr",
        "calculate_func": calculate_cr_polars,
        "params": {"windows": [26]},
        "description": "能量指标",
        "category": "能量指标",
    },
    {
        "name": "expma",
        "calculate_func": lambda df, **kwargs: calculate_expma(
            df.lazy(), kwargs.get("windows", [12, 50])
        ).collect(),
        "params": {"windows": [12, 50]},
        "description": "指数平均线",
        "category": "趋势指标",
    },
    {
        "name": "bbi",
        "calculate_func": lambda df, **kwargs: calculate_bbi(df.lazy()).collect(),
        "params": {},
        "description": "多空指数",
        "category": "趋势指标",
    },
    {
        "name": "hsl",
        "calculate_func": _calculate_hsl_with_turnover,
        "params": {"float_share": None, "ts_code": None},
        "description": "换手率",
        "category": "成交量指标",
    },
    {
        "name": "lb",
        "calculate_func": lambda df, **kwargs: calculate_lb(
            df.lazy(), kwargs.get("period", 5)
        ).collect(),
        "params": {"period": 5},
        "description": "量比",
        "category": "成交量指标",
    },
    {
        "name": "cyc",
        "calculate_func": lambda df, **kwargs: calculate_cyc(
            df.lazy(), kwargs.get("windows", [5, 13, 34])
        ).collect(),
        "params": {"windows": [5, 13, 34]},
        "description": "成本均线",
        "category": "成本指标",
    },
    {
        "name": "cys",
        "calculate_func": lambda df, **kwargs: calculate_cys(
            df.lazy(), kwargs.get("cyc_window", 13)
        ).collect(),
        "params": {"cyc_window": 13},
        "description": "市场盈亏",
        "category": "成本指标",
    },
    {
        "name": "abi",
        "calculate_func": lambda df, **kwargs: calculate_abi(
            df.lazy(),
            kwargs.get("period", 10),
            db_manager=kwargs.get("db_manager"),
            market_breadth=kwargs.get("market_breadth"),
        ).collect(),
        "params": {"period": 10},
        "description": "绝对广量指标",
        "category": "大势型",
    },
    {
        "name": "adl",
        "calculate_func": lambda df, **kwargs: calculate_adl(
            df.lazy(),
            db_manager=kwargs.get("db_manager"),
            market_breadth=kwargs.get("market_breadth"),
        ).collect(),
        "params": {},
        "description": "腾落指标",
        "category": "大势型",
    },
    {
        "name": "adr",
        "calculate_func": lambda df, **kwargs: calculate_adr(
            df.lazy(),
            kwargs.get("period", 10),
            db_manager=kwargs.get("db_manager"),
            market_breadth=kwargs.get("market_breadth"),
        ).collect(),
        "params": {"period": 10},
        "description": "涨跌比率",
        "category": "大势型",
    },
    {
        "name": "obos",
        "calculate_func": lambda df, **kwargs: calculate_obos(
            df.lazy(),
            kwargs.get("period", 10),
            db_manager=kwargs.get("db_manager"),
            market_breadth=kwargs.get("market_breadth"),
        ).collect(),
        "params": {"period": 10},
        "description": "超买超卖指标",
        "category": "大势型",
    },
)


# 内置指标注册状态：导入本模块不再触发注册（50+ 指标的注册耗时约 1.2s，
# 会直接算进首屏启动时间）。注册由 ensure_indicators_registered() 惰性触发，
# 或在启动阶段用 prewarm_indicators_async() 放到后台线程预热。
_register_lock = threading.Lock()
_indicators_registered = False


def ensure_indicators_registered() -> None:
    """
    确保内置指标已注册（幂等、线程安全）。

    首次调用会执行全部内置指标的注册，之后直接返回。
    """
    global _indicators_registered
    if _indicators_registered:
        return

    with _register_lock:
        if _indicators_registered:
            return
        for spec in _BUILTIN_INDICATORS:
            # params 复制一份，避免多个管理器实例共享同一份可变默认参数
            register_indicator(
                name=spec["name"],
                calculate_func=spec["calculate_func"],
                dependencies=[],
                params=dict(spec["params"]),
                description=spec["description"],
                category=spec["category"],
            )
        _indicators_registered = True
        logger.info(f"已注册{len(global_indicator_registry.get_supported_indicators())}个内置指标")


def prewarm_indicators_async() -> threading.Thread:
    """
    在后台线程预热指标注册，避免阻塞首屏。

    Returns:
        threading.Thread: 已启动的预热线程（daemon）
    """
    thread = threading.Thread(target=ensure_indicators_registered, name='indicator-prewarm', daemon=True)
    thread.start()
    return thread


class IndicatorManager:
    """
    指标管理器，集成指标注册中心和缓存机制
    提供统一的指标计算和管理接口
    """

    def __init__(self, cache_enabled: bool = True, cache_max_size: int = 1000, cache_default_ttl: int = 3600):
        """
        初始化指标管理器

        注意：构造函数不再注册内置指标（见 ensure_indicators_registered），
        具体接口在首次调用时惰性完成注册。

        Args:
            cache_enabled: 是否启用缓存
            cache_max_size: 缓存最大条目数
            cache_default_ttl: 默认缓存过期时间（秒）
        """
        # 缓存配置
        self._cache_enabled = cache_enabled
        if self._cache_enabled:
            # 可以通过global_indicator_cache访问全局缓存实例
            logger.info(f"指标缓存已启用，最大大小: {cache_max_size}, 默认TTL: {cache_default_ttl}秒")
        else:
            logger.info("指标缓存已禁用")

    def _ensure_ready(self) -> None:
        """惰性注册内置指标（幂等，首次调用有一次性注册开销）"""
        ensure_indicators_registered()

    def calculate_indicator(self, data: Union[pl.DataFrame, pd.DataFrame], indicator_type: str,
                          return_polars: bool = True, **params) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        计算单个指标

        Args:
            data: 输入数据
            indicator_type: 指标类型
            return_polars: 是否返回Polars DataFrame（默认True，避免不必要的格式转换开销）
            **params: 指标计算参数

        Returns:
            Union[pl.DataFrame, pd.DataFrame]: 包含计算结果的数据
        """
        self._ensure_ready()

        # 转换输入数据为Polars DataFrame
        pl_data = self._to_polars(data)

        # 预处理数据
        pl_data = preprocess_data_polars(pl_data)

        # 检查指标是否支持
        if not global_indicator_registry.is_indicator_supported(indicator_type):
            raise ValueError(f"不支持的指标类型: {indicator_type}")

        # 计算指标
        if self._cache_enabled:
            # 尝试从缓存获取结果
            cached_result = global_indicator_cache.get(pl_data, indicator_type, **params)
            if cached_result is not None:
                logger.debug(f"从缓存获取指标{indicator_type}计算结果")
                return self._to_result_format(cached_result, return_polars)

        # 执行计算
        result_df = global_indicator_registry.calculate_indicators(pl_data, [indicator_type], **params)

        # 保存到缓存
        if self._cache_enabled:
            global_indicator_cache.set(pl_data, result_df, indicator_type, **params)

        return self._to_result_format(result_df, return_polars)

    def _calculate_indicators_with_cache(self, pl_data, indicator_types, params):
        """带缓存的批量指标计算：命中缓存直接合并，未命中统一批量计算。"""
        # 尝试从缓存获取每个指标的结果
        cached_results = []
        uncached_indicators = []

        for indicator_type in indicator_types:
            cached_result = global_indicator_cache.get(pl_data, indicator_type, **params)
            if cached_result is not None:
                cached_results.append((indicator_type, cached_result))
                logger.debug(f"从缓存获取指标{indicator_type}计算结果")
            else:
                uncached_indicators.append(indicator_type)

        # 如果所有指标都在缓存中，直接合并结果
        if not uncached_indicators:
            # 合并缓存结果
            result_df = pl_data
            for _, cached_df in cached_results:
                # 只合并新的列（排除时间列和已存在的列）
                new_columns = [col for col in cached_df.columns if col not in result_df.columns and col != cached_df.columns[0]]
                if new_columns:
                    result_df = result_df.join(
                        cached_df.select([cached_df.columns[0]] + new_columns),
                        on=result_df.columns[0],
                        how='left'
                    )
            return result_df

        # 单标的多指标：单次批量调用，由 Polars 在单个查询计划内并行；
        # 外层再按组拆线程池只会重复计算共享中间列并超额订阅线程
        result_df = calculate_multiple_indicators_polars(pl_data, uncached_indicators, **params)

        # 保存到缓存
        for indicator_type in uncached_indicators:
            # 提取该指标的结果列（按声明列名，不再用前缀匹配）
            indicator_cols = _extract_indicator_columns(result_df.columns, indicator_type)
            if indicator_cols:
                indicator_result = result_df.select([result_df.columns[0]] + indicator_cols)
                global_indicator_cache.set(pl_data, indicator_result, indicator_type, **params)

        # 合并缓存结果和新计算结果
        for indicator_type, cached_df in cached_results:
            # 只合并新的列（排除时间列和已存在的列）
            new_columns = [col for col in cached_df.columns if col not in result_df.columns and col != cached_df.columns[0]]
            if new_columns:
                # 确保不会重复添加列
                result_df = result_df.join(
                    cached_df.select([cached_df.columns[0]] + new_columns),
                    on=result_df.columns[0],
                    how='left'
                )

        return result_df

    def calculate_indicators(self, data: Union[pl.DataFrame, pd.DataFrame], indicator_types: List[str],
                           return_polars: bool = True, parallel: bool = False, max_workers: int | None = None, **params) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        批量计算多个指标

        Args:
            data: 输入数据
            indicator_types: 指标类型列表
            return_polars: 是否返回Polars DataFrame（默认True，避免不必要的格式转换开销）
            parallel: 是否使用并行计算
            max_workers: 并行计算的最大工作线程数，None表示使用所有可用核心
            **params: 指标计算参数

        Returns:
            Union[pl.DataFrame, pd.DataFrame]: 包含计算结果的数据
        """
        self._ensure_ready()

        # 转换输入数据为Polars DataFrame
        pl_data = self._to_polars(data)

        # 预处理数据
        pl_data = preprocess_data_polars(pl_data)

        # 检查所有指标是否支持
        unsupported_indicators = [ind for ind in indicator_types
                                 if not global_indicator_registry.is_indicator_supported(ind)]
        if unsupported_indicators:
            raise ValueError(f"不支持的指标类型: {', '.join(unsupported_indicators)}")

        # 检查是否启用缓存
        if self._cache_enabled:
            result_df = self._calculate_indicators_with_cache(pl_data, indicator_types, params)
        else:
            # 缓存禁用，直接计算所有指标（同上：单次批量调用）
            result_df = calculate_multiple_indicators_polars(pl_data, indicator_types, **params)

        return self._to_result_format(result_df, return_polars)

    def calculate_all_indicators(self, data: Union[pl.DataFrame, pd.DataFrame],
                               return_polars: bool = True, parallel: bool = False, max_workers: int | None = None, **params) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        计算所有支持的指标

        Args:
            data: 输入数据
            return_polars: 是否返回Polars DataFrame（默认True，避免不必要的格式转换开销）
            parallel: 是否使用并行计算
            max_workers: 并行计算的最大工作线程数，None表示使用所有可用核心
            **params: 指标计算参数

        Returns:
            Union[pl.DataFrame, pd.DataFrame]: 包含所有指标计算结果的数据
        """
        self._ensure_ready()

        # 获取所有支持的指标
        all_indicators = global_indicator_registry.get_supported_indicators()

        # 使用批量计算
        return self.calculate_indicators(data, all_indicators, return_polars, parallel, max_workers, **params)

    def get_supported_indicators(self) -> Dict[str, Any]:
        """
        获取支持的指标列表

        Returns:
            Dict[str, Any]: 支持的指标信息字典
        """
        self._ensure_ready()
        indicators = global_indicator_registry.get_all_indicators()

        result = {}
        for name, config in indicators.items():
            result[name] = {
                'description': config.description,
                'category': config.category,
                'params': config.params
            }

        return result

    def get_indicators_by_category(self, category: str) -> Dict[str, Any]:
        """
        根据分类获取指标列表

        Args:
            category: 指标分类

        Returns:
            Dict[str, Any]: 指定分类的指标信息字典
        """
        self._ensure_ready()
        indicators = global_indicator_registry.get_indicators_by_category(category)

        result = {}
        for name, config in indicators.items():
            result[name] = {
                'description': config.description,
                'category': config.category,
                'params': config.params
            }

        return result

    def is_indicator_supported(self, indicator_type: str) -> bool:
        """
        检查指标是否支持

        Args:
            indicator_type: 指标类型

        Returns:
            bool: 是否支持该指标
        """
        self._ensure_ready()
        return global_indicator_registry.is_indicator_supported(indicator_type)

    def clear_cache(self, indicator_type: str | None = None):
        """
        清除缓存

        Args:
            indicator_type: 可选，指定要清除的指标类型，None表示清除所有
        """
        if self._cache_enabled:
            global_indicator_cache.clear(indicator_type)
        else:
            logger.warning("缓存已禁用，无法清除")

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        获取缓存统计信息

        Returns:
            Dict[str, Any]: 缓存统计信息
        """
        if self._cache_enabled:
            return global_indicator_cache.get_stats()
        else:
            return {
                'enabled': False,
                'size': 0,
                'max_size': 0,
                'hits': 0,
                'misses': 0,
                'hit_rate': 0.0,
                'evictions': 0,
                'total_requests': 0
            }

    def _to_polars(self, data: Union[pl.DataFrame, pd.DataFrame]) -> pl.DataFrame:
        """
        将输入数据转换为Polars DataFrame

        Args:
            data: 输入数据

        Returns:
            pl.DataFrame: 转换后的Polars DataFrame
        """
        if isinstance(data, pl.DataFrame):
            return data
        elif isinstance(data, pd.DataFrame):
            return to_polars(data)
        else:
            return pl.DataFrame(data)

    def _to_result_format(self, data: pl.DataFrame, return_polars: bool) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        将结果转换为指定格式

        Args:
            data: Polars DataFrame
            return_polars: 是否返回Polars DataFrame

        Returns:
            Union[pl.DataFrame, pd.DataFrame]: 转换后的结果
        """
        return to_output(data, return_polars)

    def register_custom_indicator(self, name: str, calculate_func: Callable, dependencies: List[str] | None = None,
                               params: Dict[str, Any] | None = None, description: str = "",
                               category: str = "自定义指标"):
        """
        注册自定义指标

        Args:
            name: 指标名称
            calculate_func: 指标计算函数
            dependencies: 依赖的其他指标列表
            params: 指标计算参数
            description: 指标描述
            category: 指标分类
        """
        from src.tech_analysis.indicator_registry import IndicatorConfig

        config = IndicatorConfig(
            name=name,
            calculate_func=calculate_func,
            dependencies=dependencies or [],
            params=params or {},
            description=description,
            category=category
        )

        global_indicator_registry.register_indicator(config)
        logger.info(f"自定义指标{name}注册成功")


# 创建全局指标管理器实例
global_indicator_manager = IndicatorManager()
