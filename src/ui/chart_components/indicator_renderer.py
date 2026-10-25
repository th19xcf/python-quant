#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
指标渲染类
负责各类技术指标的绘制和渲染
"""

import pyqtgraph as pg
import numpy as np
from typing import Any, Dict, Tuple, Optional
from loguru import logger

from .renderers.trend import TrendRenderMixin
from .renderers.volume import VolumeRenderMixin
from .renderers.oscillator import OscillatorRenderMixin


# UI指标名 → (指标类型, 期望输出列)
# 全部使用 calculate_multiple_indicators_polars 的默认参数，与原 render_* 兜底路径的
# 有效参数一致（MACD 12/26/9、BOLL [20]/2.0、EXPMA [12,50]、CYC [5,13,34]、
# CYS cyc_window=13、LB period=5、ABI/ADR/OBOS period=10 等均为框架默认值）。
# 注意：'VOL' 直接绘制 volume 列无需计算；'VOL-TDX' 在批量计算框架中无实现，保持现状跳过。
INDICATOR_CALC_MAP = {
    'KDJ': ('kdj', ['k', 'd']),
    'MACD': ('macd', ['macd', 'macd_signal']),
    'RSI': ('rsi', ['rsi14']),
    'WR': ('wr', ['wr1', 'wr2']),
    'BOLL': ('boll', ['mb', 'up', 'dn']),
    'VR': ('vr', ['vr']),
    'BRAR': ('brar', ['ar', 'br']),
    'DMI': ('dmi', ['pdi', 'ndi']),
    'TRIX': ('trix', ['trix']),
    'OBV': ('obv', ['obv']),
    'ASI': ('asi', ['asi']),
    'EMV': ('emv', ['emv']),
    'CCI': ('cci', ['cci']),
    'ROC': ('roc', ['roc']),
    'MTM': ('mtm', ['mtm']),
    'PSY': ('psy', ['psy']),
    'MCST': ('mcst', ['mcst']),
    'DMA': ('dma', ['dma', 'ama']),
    'FSL': ('fsl', ['swl', 'sws']),
    'SAR': ('sar', ['sar']),
    'CR': ('cr', ['cr']),
    'EXPMA': ('expma', ['expma12', 'expma50']),
    'BBI': ('bbi', ['bbi']),
    'HSL': ('hsl', ['hsl']),
    'LB': ('lb', ['lb']),
    'CYC': ('cyc', ['cyc5', 'cyc13']),
    'CYS': ('cys', ['cys']),
    'ABI': ('abi', ['abi10']),
    'ADL': ('adl', ['adl']),
    'ADR': ('adr', ['adr10']),
    'OBOS': ('obos', ['obos10']),
}


def ensure_indicator_columns(df: Any, indicator_names: Any) -> Any:
    """
    渲染前一次性批量补齐所选指标的缺失列（单个 Polars 查询计划）。

    只计算缺失列对应的指标类型，避免逐指标重复构造分析器、重复预处理
    以及 Pandas↔Polars 往返转换。

    Args:
        df: Polars DataFrame
        indicator_names: UI 指标名称列表（如 ['MACD', 'KDJ', 'SAR']）

    Returns:
        pl.DataFrame: 补齐指标列后的数据（无缺失时原样返回）
    """
    if df is None or not hasattr(df, 'columns'):
        return df

    missing_types = []
    for name in indicator_names:
        if not name:
            continue
        entry = INDICATOR_CALC_MAP.get(str(name).upper())
        if entry is None:
            continue
        indicator_type, expected_cols = entry
        if indicator_type in missing_types:
            continue
        if any(col not in df.columns for col in expected_cols):
            missing_types.append(indicator_type)

    if not missing_types:
        return df

    from src.tech_analysis.indicator_calculator import calculate_multiple_indicators_polars
    logger.info(f"渲染前批量补齐指标列: {missing_types}")
    result = calculate_multiple_indicators_polars(df.lazy(), missing_types)
    return result.collect() if hasattr(result, 'collect') else result


class IndicatorRenderer(TrendRenderMixin, VolumeRenderMixin, OscillatorRenderMixin):
    """
    指标渲染类
    负责绘制各类技术指标
    """
    
    def __init__(self, main_window: Any):
        """
        初始化指标渲染器
        
        Args:
            main_window: 主窗口实例
        """
        self.main_window = main_window
    
    def render_indicator(
        self, 
        plot_widget: Any, 
        indicator_name: str, 
        x: np.ndarray, 
        df: Any
    ) -> Any:
        """
        根据指标名称渲染相应的指标
        
        Args:
            plot_widget: 图表控件
            indicator_name: 指标名称
            x: x轴坐标
            df: 数据
            
        Returns:
            Any: 处理后的数据
        """
        # 检查df是否为None
        if df is None:
            logger.warning(f"render_indicator: df is None, skipping indicator {indicator_name}")
            return df
        
        renderers = {
            'VOL': self.render_volume,
            'KDJ': self.render_kdj,
            'MACD': self.render_macd,
            'RSI': self.render_rsi,
            'WR': self.render_wr,
            'BOLL': self.render_boll,
            'VR': self.render_vr,
            'BRAR': self.render_brar,
            'DMI': self.render_dmi,
            'TRIX': self.render_trix,
            'OBV': self.render_obv,
            'ASI': self.render_asi,
            'EMV': self.render_emv,
            'CCI': self.render_cci,
            'ROC': self.render_roc,
            'MTM': self.render_mtm,
            'PSY': self.render_psy,
            'MCST': self.render_mcst,
            'DMA': self.render_dma,
            'FSL': self.render_fsl,
            'SAR': self.render_sar,
            'VOL-TDX': self.render_vol_tdx,
            'CR': self.render_cr,
            # 新增指标
            'EXPMA': self.render_expma,
            'BBI': self.render_bbi,
            'HSL': self.render_hsl,
            'LB': self.render_lb,
            'CYC': self.render_cyc,
            'CYS': self.render_cys,
            # 大势型指标
            'ABI': self.render_abi,
            'ADL': self.render_adl,
            'ADR': self.render_adr,
            'OBOS': self.render_obos,
        }
        
        renderer = renderers.get(indicator_name)
        if renderer:
            df = renderer(plot_widget, df, x)
        else:
            logger.warning(f"未知的指标类型: {indicator_name}")

        return df
