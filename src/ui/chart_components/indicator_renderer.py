#!/usr/bin/env python3

"""
指标渲染类
负责各类技术指标的绘制和渲染
"""

from typing import Any

import numpy as np
from loguru import logger

from src.utils.dataframe_columns import resolve_date_column

from .renderers.oscillator import OscillatorRenderMixin
from .renderers.trend import TrendRenderMixin
from .renderers.volume import VolumeRenderMixin

# UI指标名 → (指标类型, 期望输出列)
# 全部使用 calculate_multiple_indicators_polars 的默认参数，与原 render_* 兜底路径的
# 有效参数一致（MACD 12/26/9、BOLL [20]/2.0、EXPMA [12,50]、CYC [5,13,34]、
# CYS cyc_window=13、LB period=5、ABI/ADR/OBOS period=10 等均为框架默认值）。
# 注意：'VOL' 直接绘制 volume 列无需计算；'VOL-TDX' 已随指标注册表注销，UI 侧不再提供该指标。
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
    # 路径依赖指标：批量入口不实现，由 ensure_indicator_columns 单独补算
    'SUPERTREND': ('supertrend', ['supertrend', 'st_lower', 'st_upper', 'st_dir']),
}


def ensure_indicator_columns(df: Any, indicator_names: Any,
                             overrides: dict[str, dict[str, float]] | None = None,
                             **calc_params: Any) -> Any:
    """
    渲染前一次性批量补齐所选指标的缺失列（单个 Polars 查询计划）。

    只计算缺失列对应的指标类型，避免逐指标重复构造分析器、重复预处理
    以及 Pandas↔Polars 往返转换。

    Args:
        df: Polars DataFrame
        indicator_names: UI 指标名称列表（如 ['MACD', 'KDJ', 'SAR']），
            overrides 为 UI 侧自定义参数，calc_params 为透传 kwargs，
            未传时全部走计算函数默认值。

    Returns:
        pl.DataFrame: 补齐指标列后的数据（无缺失时原样返回）
    """
    if df is None or not hasattr(df, 'columns'):
        return df

    overrides = overrides or {}
    missing_types = []
    need_supertrend = False
    for name in indicator_names:
        if not name:
            continue
        upper_name = str(name).upper()
        entry = INDICATOR_CALC_MAP.get(upper_name)
        if entry is None:
            continue
        indicator_type, default_cols = entry
        expected_cols = default_cols
        if upper_name in overrides:
            from src.ui.indicator_parameters import expected_columns

            expected_cols = expected_columns(upper_name, overrides)
        if any(col not in df.columns for col in expected_cols):
            if indicator_type == 'supertrend':
                # 路径依赖递推不进 Lazy 批量管道（见 indicators/trend.py），单独补算
                need_supertrend = True
                continue
            if indicator_type in missing_types:
                continue
            missing_types.append(indicator_type)

    if not missing_types and not need_supertrend:
        return df

    kwargs = dict(calc_params)
    if any(t in missing_types for t in ('abi', 'adl', 'adr', 'obos')):
        breadth = _load_breadth_for_frame(df)
        if breadth is not None:
            kwargs['market_breadth'] = breadth

    result = df
    if missing_types:
        from src.tech_analysis.indicator_calculator import (
            calculate_multiple_indicators_polars,
        )

        logger.info(f"渲染前批量补齐指标列: {missing_types}")
        result = calculate_multiple_indicators_polars(result.lazy(), missing_types, **kwargs)
        result = result.collect() if hasattr(result, 'collect') else result
    if need_supertrend:
        from src.tech_analysis.indicator_calculator import calculate_supertrend_polars

        logger.info("渲染前补算 Supertrend 四列（UI 侧参数，默认 ATR10/3）")
        st_period = int(calc_params.get('supertrend_period', 10))
        st_multiplier = float(calc_params.get('supertrend_multiplier', 3.0))
        result = calculate_supertrend_polars(result, period=st_period,
                                             multiplier=st_multiplier)
    return result


def overlay_y_range(df: Any, overlay_names: list[str]) -> tuple[float, float] | None:
    """主图叠加统一 Y 轴范围：K 线高低点 + 已开启叠加指标的有效值。

    与 chart_manager._setup_chart_axes 同口径（低点 *0.99、高点 *1.01），
    先取叠加值并集再套同一比例，避免 SAR/BOLL/Supertrend 互相覆盖轴范围。
    """
    if df is None or 'high' not in df.columns or 'low' not in df.columns:
        return None
    values = [np.asarray(df['low'], dtype=float), np.asarray(df['high'], dtype=float)]

    def _add_columns(columns: tuple[str, ...]) -> None:
        for col in columns:
            if col in df.columns:
                values.append(np.asarray(df[col], dtype=float))

    for name in overlay_names:
        if name == 'SAR':
            _add_columns(('sar',))
        elif name == 'BOLL':
            _add_columns(('up', 'dn', 'mb'))
        elif name == 'SUPERTREND':
            _add_columns(('st_upper', 'st_lower', 'supertrend'))

    merged = [arr[~np.isnan(arr)] for arr in values]
    merged = [arr for arr in merged if arr.size]
    if not merged:
        return None
    all_vals = np.concatenate(merged)
    return float(np.min(all_vals)) * 0.99, float(np.max(all_vals)) * 1.01


def _load_breadth_for_frame(df: Any) -> Any:
    """按主帧日期范围取 breadth 快照，并向前留 10 个交易日做 ADR/OBOS 预热。"""
    date_col = resolve_date_column(df.columns)
    if date_col is None:
        logger.warning("主帧缺少日期列，无法加载涨跌家数")
        return None
    try:
        dates = df[date_col]
        start = dates.min()
        end = dates.max()
        from src.data.services.breadth_service import get_breadth

        return get_breadth(start, end, warmup=10)
    except Exception as exc:  # 快照缺失时保留「跳过大势型指标」行为，不拖垮其它指标
        logger.warning(f"加载本地涨跌家数快照失败，跳过大势型指标: {exc}")
        return None


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
            # 路径依赖趋势指标（副图）
            'SUPERTREND': self.render_supertrend,
        }

        renderer = renderers.get(indicator_name)
        if renderer:
            df = renderer(plot_widget, df, x)
        else:
            logger.warning(f"未知的指标类型: {indicator_name}")

        return df
