#!/usr/bin/env python3

"""
图表数据准备类
负责K线图数据的预处理、复权计算、MA计算等
"""

from typing import Tuple

import numpy as np
import polars as pl
from loguru import logger

from src.tech_analysis.indicators.trend import (
    calculate_bbi as _calculate_bbi_lazy,
)
from src.tech_analysis.indicators.trend import (
    calculate_dma as _calculate_dma_lazy,
)

# 指标计算工具（绕开 IndicatorRegistry 直接用 LazyFrame 版本，原因见 trends.py 头部说明：
# DMA/BBI 未注册进 indicator_calculator，indicators/trend.py 是唯一实现）
from src.tech_analysis.indicators.trend import (
    calculate_ma as _calculate_ma_lazy,
)


class ChartDataPreparer:
    """
    图表数据准备类
    负责K线图数据的预处理和计算
    """

    def __init__(self):
        """初始化数据准备器"""
        pass

    def prepare_kline_data(
        self,
        df: pl.DataFrame,
        bar_count: int = 100,
        adjustment_type: str = 'qfq'
    ) -> pl.DataFrame:
        """
        准备K线图数据

        复权统一在 DataFetcher._apply_adjustment 出口处理：open/high/low/close
        列已经是请求的复权类型（qfq 或 hfq 或 none），此处不再二次应用。

        Args:
            df: 数据（DataFetcher 出口已含复权后的 open/high/low/close + qfq_*/hfq_* 完整套列）
            bar_count: 显示的柱体数量
            adjustment_type: 复权类型（仅保留签名用于调试；实际复权在 DataFetcher 已完成）

        Returns:
            pl.DataFrame: 处理后的数据
        """
        try:
            df_pl = df

            # 应用复权（此处为历史兜底保留：DataFetcher 出口已处理过，
            # 正常情况下是原地赋值空操作。若 DataFetcher 未来改动或
            # 某些调用方绕过 DataFetcher 直接给原始数据，这里会兜底）
            df_pl = self._apply_adjustment(df_pl, adjustment_type)

            # 计算移动平均线
            df_pl = self._calculate_moving_averages(df_pl)

            # 计算DMA指标（需要在截取数据前计算，以确保前部数据也有有效值）
            df_pl = self._calculate_dma(df_pl)

            # 计算BBI指标（需要在截取数据前计算，BBI需要24日数据）
            df_pl = self._calculate_bbi(df_pl)

            # 截取显示数据
            df_pl = self._truncate_data(df_pl, bar_count)

            logger.debug(f"数据准备完成，共 {len(df_pl)} 条记录")
            return df_pl

        except (ValueError, TypeError) as e:
            logger.exception(f"准备K线图数据失败: {e}")
            raise

    def _apply_adjustment(self, df: pl.DataFrame, adjustment_type: str) -> pl.DataFrame:
        """
        历史兜底：将 qfq_*/hfq_* 列写入 open/high/low/close

        正常情况下 DataFetcher._apply_adjustment 出口已经把 open/high/low/close
        写成请求的复权类型，此处的 df 已是正确状态——本方法相当于原地赋值空操作。

        保留原因：少数调用方可能绕过 DataFetcher 直接给原始行情（不含复权列），
        此时 qfq_*/hfq_* 可能缺失；本方法按 adjustment_type 做最后一次机会的兜底。
        """
        prefix = {'qfq': 'qfq_', 'hfq': 'hfq_'}.get(adjustment_type)
        if prefix is None:
            return df

        price_cols = [col for col in ('open', 'high', 'low', 'close') if f'{prefix}{col}' in df.columns]
        if not price_cols:
            return df

        exprs = [pl.col(f'{prefix}{col}').alias(col) for col in price_cols]
        return df.with_columns(exprs)

    def _calculate_moving_averages(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        计算移动平均线

        Args:
            df: 数据（已应用复权）

        Returns:
            pl.DataFrame: 包含MA的数据
        """
        try:
            logger.debug(f"计算MA前的数据列: {df.columns}")
            logger.debug(f"计算MA前的数据行数: {len(df)}")
            # 注意：此时close列已经是复权后的价格，MA计算会基于复权价格
            df = _calculate_ma_lazy(df.lazy(), [5, 10, 20, 60]).collect()

            logger.debug(f"计算MA后的数据列: {df.columns}")

            # 检查MA列是否存在且有有效值
            for col in ['ma5', 'ma10', 'ma20', 'ma60']:
                if col in df.columns:
                    non_null_count = df[col].drop_nulls().count()
                    logger.debug(f"{col} 非空值数量: {non_null_count}")

            return df

        except (ValueError, TypeError) as e:
            logger.warning(f"计算MA失败: {e}")
            return df

    def _calculate_dma(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        计算DMA指标（平均线差指标）
        DMA = MA(CLOSE, 10) - MA(CLOSE, 50)
        AMA = MA(DMA, 10)

        需要在截取数据前计算，以确保显示区域的前部也有有效值

        Args:
            df: 数据（已应用复权）

        Returns:
            pl.DataFrame: 包含DMA和AMA的数据
        """
        try:
            logger.debug(f"计算DMA前的数据行数: {len(df)}")
            df = _calculate_dma_lazy(df.lazy(), short_period=10, long_period=50, signal_period=10).collect()

            logger.debug(f"计算DMA后的数据列: {df.columns}")

            # 检查DMA列是否存在且有有效值
            if 'dma' in df.columns:
                non_null_count = df['dma'].drop_nulls().count()
                logger.debug(f"dma 非空值数量: {non_null_count}")
            if 'ama' in df.columns:
                non_null_count = df['ama'].drop_nulls().count()
                logger.debug(f"ama 非空值数量: {non_null_count}")

            return df

        except (ValueError, TypeError) as e:
            logger.warning(f"计算DMA失败: {e}")
            return df

    def _calculate_bbi(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        计算BBI指标（多空指数）
        BBI = (3日MA + 6日MA + 12日MA + 24日MA) / 4

        由于BBI需要24日数据才能计算出有效值，
        需要在截取数据前计算，以确保显示区域的前部也有有效值

        Args:
            df: 数据（已应用复权）

        Returns:
            pl.DataFrame: 包含BBI的数据
        """
        try:
            logger.debug(f"计算BBI前的数据行数: {len(df)}")
            df = _calculate_bbi_lazy(df.lazy()).collect()

            logger.debug(f"计算BBI后的数据列: {df.columns}")

            # 检查BBI列是否存在且有有效值
            if 'bbi' in df.columns:
                non_null_count = df['bbi'].drop_nulls().count()
                logger.debug(f"bbi 非空值数量: {non_null_count}")

            return df

        except (ValueError, TypeError) as e:
            logger.warning(f"计算BBI失败: {e}")
            return df

    def _truncate_data(self, df: pl.DataFrame, bar_count: int) -> pl.DataFrame:
        """
        截取显示数据

        由于DMA(10,50)指标需要50+10=60条数据才能计算出有效的AMA值，
        BBI指标需要24日数据才能计算出有效值，
        我们在完整数据集上计算这些指标后，再截取显示区域的数据。
        这样显示区域的前部也会有有效的指标值。

        Args:
            df: 完整数据（已计算所有指标）
            bar_count: 目标显示数量

        Returns:
            pl.DataFrame: 截取后的数据
        """
        if bar_count < len(df):
            # 直接截取最后bar_count条数据
            # 由于指标已经在完整数据集上计算完成，截取后的数据会有完整的指标值
            df = df.tail(bar_count)

        return df

    def extract_price_data(self, df: pl.DataFrame) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        提取价格数据（简化版）
        直接从已处理的DataFrame中提取价格列

        Args:
            df: 已应用复权的数据

        Returns:
            Tuple: (dates, opens, highs, lows, closes)
        """
        dates = df['date'].to_numpy()
        opens = df['open'].to_numpy()
        highs = df['high'].to_numpy()
        lows = df['low'].to_numpy()
        closes = df['close'].to_numpy()

        return dates, opens, highs, lows, closes

    def create_ohlc_data(
        self,
        opens: np.ndarray,
        highs: np.ndarray,
        lows: np.ndarray,
        closes: np.ndarray
    ) -> list:
        """
        创建OHLC数据列表

        Args:
            opens: 开盘价数组
            highs: 最高价数组
            lows: 最低价数组
            closes: 收盘价数组

        Returns:
            list: OHLC列表
        """
        x = np.arange(len(opens))
        ohlc = np.column_stack((x, opens, highs, lows, closes))
        return [tuple(row) for row in ohlc]

    def calculate_price_extremes(
        self,
        highs: np.ndarray,
        lows: np.ndarray
    ) -> Tuple[float, float, int, int]:
        """
        计算价格极值

        Args:
            highs: 最高价数组
            lows: 最低价数组

        Returns:
            Tuple: (current_high, current_low, high_index, low_index)
        """
        current_high = np.max(highs)
        current_low = np.min(lows)
        high_index = np.argmax(highs)
        low_index = np.argmin(lows)

        return current_high, current_low, high_index, low_index
