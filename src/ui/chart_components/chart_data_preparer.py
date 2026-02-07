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
            
            # 应用复权计算
            df_pl = self._apply_adjustment(df_pl, adjustment_type)
            
            # 计算移动平均线
            df_pl = self._calculate_moving_averages(df_pl)
            
            # 截取显示数据
            df_pl = self._truncate_data(df_pl, bar_count)
            
            logger.debug(f"数据准备完成，共 {len(df_pl)} 条记录")
            return df_pl
            
        except Exception as e:
            logger.exception(f"准备K线图数据失败: {e}")
            raise
    
    def _apply_adjustment(self, df: pl.DataFrame, adjustment_type: str) -> pl.DataFrame:
        """
        应用复权计算
        
        Args:
            df: 原始数据
            adjustment_type: 复权类型
            
        Returns:
            pl.DataFrame: 复权后的数据
        """
        df_pl = df
        
        # 保存原始close列
        if 'close_original' not in df_pl.columns:
            df_pl = df_pl.with_columns(pl.col('close').alias('close_original'))
        
        # 后复权显示：按当前窗口首日因子归一化
        if adjustment_type == 'hfq' and 'hfq_factor' in df_pl.columns:
            df_pl = self._normalize_hfq_data(df_pl)
        
        # 根据复权类型替换close列
        if adjustment_type == 'qfq' and 'qfq_close' in df_pl.columns:
            df_pl = df_pl.with_columns(pl.col('qfq_close').alias('close'))
        elif adjustment_type == 'hfq':
            df_pl = self._apply_hfq_close(df_pl)
        
        return df_pl
    
    def _normalize_hfq_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """归一化后复权数据"""
        try:
            base_factor_series = df.select(pl.col('hfq_factor').drop_nulls().head(1))
            base_factor = base_factor_series.item(0, 0) if base_factor_series.height > 0 else None
        except Exception:
            base_factor = None
        
        if base_factor and base_factor != 0:
            price_cols = ['hfq_open', 'hfq_high', 'hfq_low', 'hfq_close']
            norm_cols = ['hfq_open_norm', 'hfq_high_norm', 'hfq_low_norm', 'hfq_close_norm']
            
            for price_col, norm_col in zip(price_cols, norm_cols):
                if price_col in df.columns:
                    df = df.with_columns((pl.col(price_col) / base_factor).alias(norm_col))
        
        return df
    
    def _apply_hfq_close(self, df: pl.DataFrame) -> pl.DataFrame:
        """应用后复权收盘价"""
        if 'hfq_close_norm' in df.columns:
            return df.with_columns(pl.col('hfq_close_norm').alias('close'))
        elif 'hfq_close' in df.columns:
            return df.with_columns(pl.col('hfq_close').alias('close'))
        return df
    
    def _calculate_moving_averages(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        计算移动平均线
        
        Args:
            df: 数据
            
        Returns:
            pl.DataFrame: 包含MA的数据
        """
        try:
            from src.tech_analysis.indicators.trend import calculate_ma
            
            # 使用calculate_ma直接计算
            df = calculate_ma(df.lazy(), [5, 10, 20, 60]).collect()
            
            # 恢复原始close列
            if 'close_original' in df.columns:
                df = df.with_columns(pl.col('close_original').alias('close'))
                df = df.drop('close_original')
            
            return df
            
        except Exception as e:
            logger.warning(f"计算MA失败: {e}")
            return df
    
    def _truncate_data(self, df: pl.DataFrame, bar_count: int) -> pl.DataFrame:
        """
        截取显示数据
        
        Args:
            df: 完整数据
            bar_count: 目标显示数量
            
        Returns:
            pl.DataFrame: 截取后的数据
        """
        if bar_count < len(df):
            # 多取60条数据确保MA60计算完整
            df = df.tail(bar_count + 60)
            # 再取最后bar_count条用于显示
            df = df.tail(bar_count)
        
        return df
    
    def extract_price_data(
        self, 
        df: pl.DataFrame, 
        adjustment_type: str = 'qfq'
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        提取价格数据
        
        Args:
            df: 数据
            adjustment_type: 复权类型
            
        Returns:
            Tuple: (dates, opens, highs, lows, closes)
        """
        dates = df['date'].to_numpy()
        
        if adjustment_type == 'qfq':
            opens = df['qfq_open'].to_numpy() if 'qfq_open' in df.columns else df['open'].to_numpy()
            highs = df['qfq_high'].to_numpy() if 'qfq_high' in df.columns else df['high'].to_numpy()
            lows = df['qfq_low'].to_numpy() if 'qfq_low' in df.columns else df['low'].to_numpy()
            closes = df['qfq_close'].to_numpy() if 'qfq_close' in df.columns else df['close'].to_numpy()
        elif adjustment_type == 'hfq':
            opens, highs, lows, closes = self._extract_hfq_prices(df)
        else:
            opens = df['open'].to_numpy()
            highs = df['high'].to_numpy()
            lows = df['low'].to_numpy()
            closes = df['close'].to_numpy()
        
        return dates, opens, highs, lows, closes
    
    def _extract_hfq_prices(self, df: pl.DataFrame) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """提取后复权价格"""
        if 'hfq_open_norm' in df.columns:
            opens = df['hfq_open_norm'].to_numpy()
            highs = df['hfq_high_norm'].to_numpy()
            lows = df['hfq_low_norm'].to_numpy()
            closes = df['hfq_close_norm'].to_numpy()
        else:
            opens = df['hfq_open'].to_numpy() if 'hfq_open' in df.columns else df['open'].to_numpy()
            highs = df['hfq_high'].to_numpy() if 'hfq_high' in df.columns else df['high'].to_numpy()
            lows = df['hfq_low'].to_numpy() if 'hfq_low' in df.columns else df['low'].to_numpy()
            closes = df['hfq_close'].to_numpy() if 'hfq_close' in df.columns else df['close'].to_numpy()
        
        return opens, highs, lows, closes
    
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
