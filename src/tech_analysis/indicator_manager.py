#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
指标管理器模块，集成指标注册中心和缓存机制
提供统一的指标计算和管理接口
"""

from typing import List, Dict, Any, Optional, Union
import polars as pl
import pandas as pd
from loguru import logger

from src.tech_analysis.indicator_registry import global_indicator_registry, register_indicator
from src.tech_analysis.indicator_cache import global_indicator_cache, cached_calculation
from src.tech_analysis.indicator_calculator import (
    calculate_ma_polars,
    calculate_vol_ma_polars,
    calculate_macd_polars,
    calculate_rsi_polars,
    calculate_kdj_polars,
    calculate_wr_polars,
    calculate_boll_polars,
    calculate_dmi_polars,
    calculate_cci_polars,
    calculate_roc_polars,
    calculate_mtm_polars,
    calculate_obv_polars,
    calculate_vr_polars,
    calculate_psy_polars,
    calculate_trix_polars,
    calculate_brar_polars,
    calculate_asi_polars,
    calculate_emv_polars,
    calculate_mcst_polars,
    calculate_multiple_indicators_polars,
    preprocess_data_polars
)


class IndicatorManager:
    """
    指标管理器，集成指标注册中心和缓存机制
    提供统一的指标计算和管理接口
    """
    
    def __init__(self, cache_enabled: bool = True, cache_max_size: int = 1000, cache_default_ttl: int = 3600):
        """
        初始化指标管理器
        
        Args:
            cache_enabled: 是否启用缓存
            cache_max_size: 缓存最大条目数
            cache_default_ttl: 默认缓存过期时间（秒）
        """
        # 注册所有内置指标
        self._register_builtin_indicators()
        
        # 缓存配置
        self._cache_enabled = cache_enabled
        if self._cache_enabled:
            # 可以通过global_indicator_cache访问全局缓存实例
            logger.info(f"指标缓存已启用，最大大小: {cache_max_size}, 默认TTL: {cache_default_ttl}秒")
        else:
            logger.info("指标缓存已禁用")
    
    def _register_builtin_indicators(self):
        """
        注册所有内置指标
        """
        # 1. MA指标
        register_indicator(
            name='ma',
            calculate_func=calculate_ma_polars,
            dependencies=[],
            params={'windows': [5, 10, 20, 60]},
            description='移动平均线',
            category='趋势指标'
        )
        
        # 2. VOL_MA指标
        register_indicator(
            name='vol_ma',
            calculate_func=calculate_vol_ma_polars,
            dependencies=[],
            params={'windows': [5, 10]},
            description='成交量移动平均线',
            category='成交量指标'
        )
        
        # 3. MACD指标
        register_indicator(
            name='macd',
            calculate_func=calculate_macd_polars,
            dependencies=[],
            params={'fast_period': 12, 'slow_period': 26, 'signal_period': 9},
            description='平滑异同移动平均线',
            category='趋势指标'
        )
        
        # 4. RSI指标
        register_indicator(
            name='rsi',
            calculate_func=calculate_rsi_polars,
            dependencies=[],
            params={'windows': [14]},
            description='相对强弱指标',
            category='震荡指标'
        )
        
        # 5. KDJ指标
        register_indicator(
            name='kdj',
            calculate_func=calculate_kdj_polars,
            dependencies=[],
            params={'windows': [14]},
            description='随机指标',
            category='震荡指标'
        )
        
        # 6. WR指标
        register_indicator(
            name='wr',
            calculate_func=calculate_wr_polars,
            dependencies=[],
            params={'windows': [10, 6]},
            description='威廉指标',
            category='震荡指标'
        )
        
        # 7. BOLL指标
        register_indicator(
            name='boll',
            calculate_func=calculate_boll_polars,
            dependencies=[],
            params={'windows': [20], 'std_dev': 2.0},
            description='布林带',
            category='趋势指标'
        )
        
        # 8. DMI指标
        register_indicator(
            name='dmi',
            calculate_func=calculate_dmi_polars,
            dependencies=[],
            params={'windows': [14]},
            description='趋向指标',
            category='趋势指标'
        )
        
        # 9. CCI指标
        register_indicator(
            name='cci',
            calculate_func=calculate_cci_polars,
            dependencies=[],
            params={'windows': [14]},
            description='商品通道指标',
            category='震荡指标'
        )
        
        # 10. ROC指标
        register_indicator(
            name='roc',
            calculate_func=calculate_roc_polars,
            dependencies=[],
            params={'windows': [12]},
            description='变化率指标',
            category='震荡指标'
        )
        
        # 11. MTM指标
        register_indicator(
            name='mtm',
            calculate_func=calculate_mtm_polars,
            dependencies=[],
            params={'windows': [12]},
            description='动量指标',
            category='震荡指标'
        )
        
        # 12. OBV指标
        register_indicator(
            name='obv',
            calculate_func=calculate_obv_polars,
            dependencies=[],
            params={},
            description='能量潮',
            category='成交量指标'
        )
        
        # 13. VR指标
        register_indicator(
            name='vr',
            calculate_func=calculate_vr_polars,
            dependencies=[],
            params={'windows': [26]},
            description='成交量比率',
            category='成交量指标'
        )
        
        # 14. PSY指标
        register_indicator(
            name='psy',
            calculate_func=calculate_psy_polars,
            dependencies=[],
            params={'windows': [12]},
            description='心理线',
            category='情绪指标'
        )
        
        # 15. TRIX指标
        register_indicator(
            name='trix',
            calculate_func=calculate_trix_polars,
            dependencies=[],
            params={'windows': [12], 'signal_period': 9},
            description='三重指数平滑',
            category='趋势指标'
        )
        
        # 16. BRAR指标
        register_indicator(
            name='brar',
            calculate_func=calculate_brar_polars,
            dependencies=[],
            params={'windows': [26]},
            description='情绪指标',
            category='情绪指标'
        )
        
        # 17. ASI指标
        register_indicator(
            name='asi',
            calculate_func=calculate_asi_polars,
            dependencies=[],
            params={'signal_period': 20},
            description='振动升降指标',
            category='趋势指标'
        )
        
        # 18. EMV指标
        register_indicator(
            name='emv',
            calculate_func=calculate_emv_polars,
            dependencies=[],
            params={'windows': [14], 'constant': 100000000},
            description='简易波动指标',
            category='趋势指标'
        )
        
        # 19. MCST指标
        register_indicator(
            name='mcst',
            calculate_func=calculate_mcst_polars,
            dependencies=[],
            params={'windows': [12]},
            description='市场成本',
            category='成本指标'
        )
        
        logger.info(f"已注册{len(global_indicator_registry.get_supported_indicators())}个内置指标")
    
    def calculate_indicator(self, data: Union[pl.DataFrame, pd.DataFrame], indicator_type: str, 
                          return_polars: bool = False, **params) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        计算单个指标
        
        Args:
            data: 输入数据
            indicator_type: 指标类型
            return_polars: 是否返回Polars DataFrame
            **params: 指标计算参数
        
        Returns:
            Union[pl.DataFrame, pd.DataFrame]: 包含计算结果的数据
        """
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
    
    def calculate_indicators(self, data: Union[pl.DataFrame, pd.DataFrame], indicator_types: List[str], 
                           return_polars: bool = False, **params) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        批量计算多个指标
        
        Args:
            data: 输入数据
            indicator_types: 指标类型列表
            return_polars: 是否返回Polars DataFrame
            **params: 指标计算参数
        
        Returns:
            Union[pl.DataFrame, pd.DataFrame]: 包含计算结果的数据
        """
        # 转换输入数据为Polars DataFrame
        pl_data = self._to_polars(data)
        
        # 预处理数据
        pl_data = preprocess_data_polars(pl_data)
        
        # 检查所有指标是否支持
        unsupported_indicators = [ind for ind in indicator_types 
                                 if not global_indicator_registry.is_indicator_supported(ind)]
        if unsupported_indicators:
            raise ValueError(f"不支持的指标类型: {', '.join(unsupported_indicators)}")
        
        # 使用优化的批量计算函数
        result_df = calculate_multiple_indicators_polars(pl_data, indicator_types, **params)
        
        return self._to_result_format(result_df, return_polars)
    
    def calculate_all_indicators(self, data: Union[pl.DataFrame, pd.DataFrame], 
                               return_polars: bool = False, **params) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        计算所有支持的指标
        
        Args:
            data: 输入数据
            return_polars: 是否返回Polars DataFrame
            **params: 指标计算参数
        
        Returns:
            Union[pl.DataFrame, pd.DataFrame]: 包含所有指标计算结果的数据
        """
        # 获取所有支持的指标
        all_indicators = global_indicator_registry.get_supported_indicators()
        
        # 使用批量计算
        return self.calculate_indicators(data, all_indicators, return_polars, **params)
    
    def get_supported_indicators(self) -> Dict[str, Any]:
        """
        获取支持的指标列表
        
        Returns:
            Dict[str, Any]: 支持的指标信息字典
        """
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
        return global_indicator_registry.is_indicator_supported(indicator_type)
    
    def clear_cache(self, indicator_type: Optional[str] = None):
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
            return pl.from_pandas(data)
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
        if return_polars:
            return data
        else:
            return data.to_pandas()
    
    def register_custom_indicator(self, name: str, calculate_func: callable, dependencies: List[str] = None, 
                               params: Dict[str, Any] = None, description: str = "", 
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
