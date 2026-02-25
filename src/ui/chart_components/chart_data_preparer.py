#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
图表数据准备类
负责K线图数据的预处理、复权计算、MA计算等
"""

import polars as pl
import numpy as np
from typing import Optional, Tuple, Any
from loguru import logger


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
        
        Args:
            df: 原始股票数据
            bar_count: 显示的柱体数量
            adjustment_type: 复权类型 ('qfq', 'hfq', 'none')
            
        Returns:
            pl.DataFrame: 处理后的数据
        """
        try:
            df_pl = df
            
            # 对于后复权，先进行归一化（使用完整数据的最早因子作为基准）
            if adjustment_type == 'hfq' and 'hfq_factor' in df_pl.columns:
                df_pl = self._normalize_hfq_data(df_pl)
            
            # 应用复权计算
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
        应用复权计算，统一处理所有价格列（open/high/low/close）
        
        Args:
            df: 原始数据
            adjustment_type: 复权类型 ('qfq', 'hfq', 'none')
            
        Returns:
            pl.DataFrame: 复权后的数据
        """
        if adjustment_type == 'qfq':
            # 前复权：使用 qfq_ 前缀的列替换所有价格列
            df = self._apply_qfq_adjustment(df)
        elif adjustment_type == 'hfq':
            # 后复权：先归一化，然后使用归一化后的列替换所有价格列
            df = self._apply_hfq_adjustment(df)
        # 'none' 或其他情况保持原始价格不变
        
        return df
    
    def _apply_qfq_adjustment(self, df: pl.DataFrame) -> pl.DataFrame:
        """应用前复权调整"""
        # 检查是否有前复权价格列
        has_qfq_cols = all(col in df.columns for col in ['qfq_open', 'qfq_high', 'qfq_low', 'qfq_close'])
        
        if has_qfq_cols:
            # 使用复权价格列替换原始价格列
            df = df.with_columns([
                pl.col('qfq_open').alias('open'),
                pl.col('qfq_high').alias('high'),
                pl.col('qfq_low').alias('low'),
                pl.col('qfq_close').alias('close')
            ])
            logger.debug("应用前复权价格")
        else:
            # 没有前复权价格列，使用原始价格并添加qfq_列以便后续处理
            df = df.with_columns([
                pl.col('open').alias('qfq_open'),
                pl.col('high').alias('qfq_high'),
                pl.col('low').alias('qfq_low'),
                pl.col('close').alias('qfq_close')
            ])
            logger.debug("使用原始价格并添加qfq_列")
        
        return df
    
    def _apply_hfq_adjustment(self, df: pl.DataFrame) -> pl.DataFrame:
        """应用后复权调整"""
        # 注意：归一化处理已经在 prepare_kline_data 中完成
        
        # 检查是否有归一化后的列
        has_norm_cols = all(col in df.columns for col in ['hfq_open_norm', 'hfq_high_norm', 'hfq_low_norm', 'hfq_close_norm'])
        
        if has_norm_cols:
            # 使用归一化后的复权价格列
            df = df.with_columns([
                pl.col('hfq_open_norm').alias('open'),
                pl.col('hfq_high_norm').alias('high'),
                pl.col('hfq_low_norm').alias('low'),
                pl.col('hfq_close_norm').alias('close')
            ])
            logger.debug("应用归一化后复权价格")
        else:
            # 检查是否有原始后复权价格列（hfq_前缀）
            has_hfq_cols = all(col in df.columns for col in ['hfq_open', 'hfq_high', 'hfq_low', 'hfq_close'])
            if has_hfq_cols:
                df = df.with_columns([
                    pl.col('hfq_open').alias('open'),
                    pl.col('hfq_high').alias('high'),
                    pl.col('hfq_low').alias('low'),
                    pl.col('hfq_close').alias('close')
                ])
                logger.debug("应用后复权价格")
            else:
                # 如果hfq_列不存在，使用原始价格并添加hfq_列以便后续处理
                df = df.with_columns([
                    pl.col('open').alias('hfq_open'),
                    pl.col('high').alias('hfq_high'),
                    pl.col('low').alias('hfq_low'),
                    pl.col('close').alias('hfq_close')
                ])
                # 如果hfq_factor存在，说明数据可能已经是后复权价格
                if 'hfq_factor' in df.columns:
                    logger.debug("使用已处理的后复权价格并添加hfq_列")
                else:
                    logger.debug("使用原始价格并添加hfq_列")
        
        return df
    
    def _normalize_hfq_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """处理后复权数据
        
        后复权数据不再进行归一化，保持累积值以反映真实的历史累积价格。
        这样茅台等长期上涨的股票后复权价格会显示为几千到几万。
        """
        # 后复权数据保持原样，不进行归一化
        # 直接复制 hfq_ 列到 norm 列，保持兼容性
        hfq_cols = ['hfq_open', 'hfq_high', 'hfq_low', 'hfq_close']
        norm_cols = ['hfq_open_norm', 'hfq_high_norm', 'hfq_low_norm', 'hfq_close_norm']
        
        for hfq_col, norm_col in zip(hfq_cols, norm_cols):
            if hfq_col in df.columns:
                df = df.with_columns(pl.col(hfq_col).alias(norm_col))
        
        return df
    
    def _calculate_moving_averages(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        计算移动平均线
        
        Args:
            df: 数据（已应用复权）
            
        Returns:
            pl.DataFrame: 包含MA的数据
        """
        try:
            from src.tech_analysis.indicators.trend import calculate_ma
            
            logger.debug(f"计算MA前的数据列: {df.columns}")
            logger.debug(f"计算MA前的数据行数: {len(df)}")
            
            # 使用calculate_ma直接计算MA
            # 注意：此时close列已经是复权后的价格，MA计算会基于复权价格
            df = calculate_ma(df.lazy(), [5, 10, 20, 60]).collect()
            
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
            from src.tech_analysis.indicators.trend import calculate_dma
            
            logger.debug(f"计算DMA前的数据行数: {len(df)}")
            
            # 使用calculate_dma计算DMA指标
            df = calculate_dma(df.lazy(), short_period=10, long_period=50, signal_period=10).collect()
            
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
            from src.tech_analysis.indicators.trend import calculate_bbi
            
            logger.debug(f"计算BBI前的数据行数: {len(df)}")
            
            # 使用calculate_bbi计算BBI指标
            df = calculate_bbi(df.lazy()).collect()
            
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
