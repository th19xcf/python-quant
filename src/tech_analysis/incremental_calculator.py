#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
增量计算模块，提供技术指标的增量计算功能
"""

import polars as pl
from typing import List, Dict, Any, Optional
from loguru import logger


class IncrementalCalculator:
    """
    增量计算类，提供技术指标的增量计算功能
    """
    
    def __init__(self):
        """
        初始化增量计算器
        """
        # 支持增量计算的指标类型
        self.supported_indicators = {
            'ma': self.incremental_calculate_ma,
            'vol_ma': self.incremental_calculate_vol_ma,
            'macd': self.incremental_calculate_macd,
            'rsi': self.incremental_calculate_rsi,
            'kdj': self.incremental_calculate_kdj,
            'boll': self.incremental_calculate_boll,
            'wr': self.incremental_calculate_wr,
            'obv': self.incremental_calculate_obv,
            'expma': self.incremental_calculate_expma,
        }
    
    def is_supported(self, indicator_type: str) -> bool:
        """
        检查指标是否支持增量计算
        
        Args:
            indicator_type: 指标类型
            
        Returns:
            bool: 是否支持增量计算
        """
        return indicator_type in self.supported_indicators
    
    def incremental_calculate(self, 
                             indicator_type: str, 
                             existing_data: pl.DataFrame, 
                             new_data: pl.DataFrame, 
                             **kwargs) -> pl.DataFrame:
        """
        增量计算指标
        
        Args:
            indicator_type: 指标类型
            existing_data: 已有的数据（包含历史指标值）
            new_data: 新增的数据（不包含指标值）
            **kwargs: 指标计算参数
            
        Returns:
            pl.DataFrame: 包含新增指标值的数据
        """
        if not self.is_supported(indicator_type):
            raise ValueError(f"指标{indicator_type}不支持增量计算")
        
        try:
            # 调用对应的增量计算方法
            calculate_func = self.supported_indicators[indicator_type]
            return calculate_func(existing_data, new_data, **kwargs)
        except Exception as e:
            logger.error(f"增量计算{indicator_type}失败: {e}")
            raise
    
    def incremental_calculate_ma(self, 
                                existing_data: pl.DataFrame, 
                                new_data: pl.DataFrame, 
                                windows: List[int] = None, 
                                **kwargs) -> pl.DataFrame:
        """
        增量计算移动平均线
        
        Args:
            existing_data: 已有的数据
            new_data: 新增的数据
            windows: 移动平均窗口列表
            
        Returns:
            pl.DataFrame: 包含新增MA值的数据
        """
        if windows is None:
            windows = [5, 10, 20, 60]
        
        # 合并数据用于计算
        combined_data = existing_data.vstack(new_data).select(['date', 'close'])
        
        # 计算每个窗口的移动平均
        result = new_data.clone()
        
        for window in windows:
            # 计算完整的移动平均
            ma_col = f'ma{window}'
            ma_values = combined_data.with_columns(
                pl.col('close').rolling_mean(window_size=window, min_periods=window).alias(ma_col)
            )[ma_col]
            
            # 提取新增数据对应的MA值
            new_ma_values = ma_values.tail(len(new_data))
            result = result.with_columns(new_ma_values.alias(ma_col))
        
        return result
    
    def incremental_calculate_vol_ma(self, 
                                   existing_data: pl.DataFrame, 
                                   new_data: pl.DataFrame, 
                                   windows: List[int] = None, 
                                   **kwargs) -> pl.DataFrame:
        """
        增量计算成交量移动平均线
        
        Args:
            existing_data: 已有的数据
            new_data: 新增的数据
            windows: 移动平均窗口列表
            
        Returns:
            pl.DataFrame: 包含新增VOL_MA值的数据
        """
        if windows is None:
            windows = [5, 10]
        
        # 合并数据用于计算
        combined_data = existing_data.vstack(new_data).select(['date', 'volume'])
        
        # 计算每个窗口的移动平均
        result = new_data.clone()
        
        for window in windows:
            # 计算完整的移动平均
            vol_ma_col = f'vol_ma{window}'
            vol_ma_values = combined_data.with_columns(
                pl.col('volume').rolling_mean(window_size=window, min_periods=window).alias(vol_ma_col)
            )[vol_ma_col]
            
            # 提取新增数据对应的VOL_MA值
            new_vol_ma_values = vol_ma_values.tail(len(new_data))
            result = result.with_columns(new_vol_ma_values.alias(vol_ma_col))
        
        return result
    
    def incremental_calculate_macd(self, 
                                 existing_data: pl.DataFrame, 
                                 new_data: pl.DataFrame, 
                                 fast_period: int = 12, 
                                 slow_period: int = 26, 
                                 signal_period: int = 9, 
                                 **kwargs) -> pl.DataFrame:
        """
        增量计算MACD指标
        
        Args:
            existing_data: 已有的数据
            new_data: 新增的数据
            fast_period: 快速EMA周期
            slow_period: 慢速EMA周期
            signal_period: 信号线EMA周期
            
        Returns:
            pl.DataFrame: 包含新增MACD值的数据
        """
        # 合并数据用于计算
        combined_data = existing_data.vstack(new_data).select(['date', 'close'])
        
        # 计算EMA12、EMA26
        ema12 = combined_data.with_columns(
            pl.col('close').ewm_mean(span=fast_period).alias('ema12')
        )['ema12']
        
        ema26 = combined_data.with_columns(
            pl.col('close').ewm_mean(span=slow_period).alias('ema26')
        )['ema26']
        
        # 计算MACD线
        macd_line = ema12 - ema26
        
        # 计算信号线
        macd_signal = macd_line.ewm_mean(span=signal_period)
        
        # 计算柱状图
        macd_hist = macd_line - macd_signal
        
        # 提取新增数据对应的MACD值
        result = new_data.clone()
        result = result.with_columns(
            macd_line.tail(len(new_data)).alias('macd'),
            macd_signal.tail(len(new_data)).alias('macd_signal'),
            macd_hist.tail(len(new_data)).alias('macd_hist')
        )
        
        return result
    
    def incremental_calculate_rsi(self, 
                                existing_data: pl.DataFrame, 
                                new_data: pl.DataFrame, 
                                windows: List[int] = None, 
                                **kwargs) -> pl.DataFrame:
        """
        增量计算RSI指标
        
        Args:
            existing_data: 已有的数据
            new_data: 新增的数据
            windows: RSI计算窗口列表
            
        Returns:
            pl.DataFrame: 包含新增RSI值的数据
        """
        if windows is None:
            windows = [14]
        
        # 合并数据用于计算
        combined_data = existing_data.vstack(new_data).select(['date', 'close'])
        
        # 计算价格变化
        price_change = combined_data.with_columns(
            pl.col('close').diff().alias('change')
        )['change']
        
        # 计算上涨和下跌
        up_change = price_change.map_elements(lambda x: x if x > 0 else 0, return_dtype=pl.Float32)
        down_change = price_change.map_elements(lambda x: abs(x) if x < 0 else 0, return_dtype=pl.Float32)
        
        # 计算每个窗口的RSI
        result = new_data.clone()
        
        for window in windows:
            # 计算平均上涨和下跌
            avg_up = up_change.ewm_mean(span=window)
            avg_down = down_change.ewm_mean(span=window)
            
            # 计算RSI
            rsi_col = f'rsi{window}'
            rsi_values = pl.when(avg_down == 0).then(100.0).otherwise(
                100.0 - (100.0 / (1.0 + avg_up / avg_down))
            )
            
            # 提取新增数据对应的RSI值
            new_rsi_values = rsi_values.tail(len(new_data))
            result = result.with_columns(new_rsi_values.alias(rsi_col))
        
        return result
    
    def incremental_calculate_kdj(self, 
                                existing_data: pl.DataFrame, 
                                new_data: pl.DataFrame, 
                                windows: List[int] = None, 
                                **kwargs) -> pl.DataFrame:
        """
        增量计算KDJ指标
        
        Args:
            existing_data: 已有的数据
            new_data: 新增的数据
            windows: KDJ计算窗口列表
            
        Returns:
            pl.DataFrame: 包含新增KDJ值的数据
        """
        if windows is None:
            windows = [14]
        
        # 合并数据用于计算
        combined_data = existing_data.vstack(new_data).select(['date', 'high', 'low', 'close'])
        
        result = new_data.clone()
        
        for window in windows:
            # 计算RSV
            highest_high = combined_data.with_columns(
                pl.col('high').rolling_max(window_size=window, min_periods=window).alias('highest_high')
            )['highest_high']
            
            lowest_low = combined_data.with_columns(
                pl.col('low').rolling_min(window_size=window, min_periods=window).alias('lowest_low')
            )['lowest_low']
            
            rsv = (combined_data['close'] - lowest_low) / (highest_high - lowest_low) * 100
            
            # 计算K、D、J值
            k_col = f'k{window}'
            d_col = f'd{window}'
            j_col = f'j{window}'
            
            # 初始K值
            k_values = pl.Series([50.0] * len(combined_data))
            d_values = pl.Series([50.0] * len(combined_data))
            
            # 迭代计算K、D值
            for i in range(window, len(combined_data)):
                k_values[i] = 2/3 * k_values[i-1] + 1/3 * rsv[i]
                d_values[i] = 2/3 * d_values[i-1] + 1/3 * k_values[i]
            
            # 计算J值
            j_values = 3 * k_values - 2 * d_values
            
            # 提取新增数据对应的KDJ值
            new_k_values = k_values.tail(len(new_data))
            new_d_values = d_values.tail(len(new_data))
            new_j_values = j_values.tail(len(new_data))
            
            result = result.with_columns(
                new_k_values.alias(k_col),
                new_d_values.alias(d_col),
                new_j_values.alias(j_col)
            )
        
        return result
    
    def incremental_calculate_boll(self, 
                                 existing_data: pl.DataFrame, 
                                 new_data: pl.DataFrame, 
                                 windows: List[int] = None, 
                                 std_dev: float = 2.0, 
                                 **kwargs) -> pl.DataFrame:
        """
        增量计算布林带指标
        
        Args:
            existing_data: 已有的数据
            new_data: 新增的数据
            windows: Boll计算窗口列表
            std_dev: 标准差倍数
            
        Returns:
            pl.DataFrame: 包含新增Boll值的数据
        """
        if windows is None:
            windows = [20]
        
        # 合并数据用于计算
        combined_data = existing_data.vstack(new_data).select(['date', 'close'])
        
        result = new_data.clone()
        
        for window in windows:
            # 计算中轨（移动平均）
            mb_col = f'mb{window}'
            up_col = f'up{window}'
            dn_col = f'dn{window}'
            
            mb_values = combined_data.with_columns(
                pl.col('close').rolling_mean(window_size=window, min_periods=window).alias(mb_col)
            )[mb_col]
            
            # 计算标准差
            std_values = combined_data.with_columns(
                pl.col('close').rolling_std(window_size=window, min_periods=window).alias('std')
            )['std']
            
            # 计算上轨和下轨
            up_values = mb_values + std_dev * std_values
            dn_values = mb_values - std_dev * std_values
            
            # 提取新增数据对应的Boll值
            result = result.with_columns(
                mb_values.tail(len(new_data)).alias(mb_col),
                up_values.tail(len(new_data)).alias(up_col),
                dn_values.tail(len(new_data)).alias(dn_col)
            )
        
        return result
    
    def incremental_calculate_wr(self, 
                               existing_data: pl.DataFrame, 
                               new_data: pl.DataFrame, 
                               windows: List[int] = None, 
                               **kwargs) -> pl.DataFrame:
        """
        增量计算威廉指标
        
        Args:
            existing_data: 已有的数据
            new_data: 新增的数据
            windows: WR计算窗口列表
            
        Returns:
            pl.DataFrame: 包含新增WR值的数据
        """
        if windows is None:
            windows = [10, 6]
        
        # 合并数据用于计算
        combined_data = existing_data.vstack(new_data).select(['date', 'high', 'low', 'close'])
        
        result = new_data.clone()
        
        for window in windows:
            # 计算最高价和最低价
            highest_high = combined_data.with_columns(
                pl.col('high').rolling_max(window_size=window, min_periods=window).alias('highest_high')
            )['highest_high']
            
            lowest_low = combined_data.with_columns(
                pl.col('low').rolling_min(window_size=window, min_periods=window).alias('lowest_low')
            )['lowest_low']
            
            # 计算WR
            wr_col = f'wr{window}'
            wr_values = (highest_high - combined_data['close']) / (highest_high - lowest_low) * -100
            
            # 提取新增数据对应的WR值
            new_wr_values = wr_values.tail(len(new_data))
            result = result.with_columns(new_wr_values.alias(wr_col))
        
        return result
    
    def incremental_calculate_obv(self, 
                                existing_data: pl.DataFrame, 
                                new_data: pl.DataFrame, 
                                **kwargs) -> pl.DataFrame:
        """
        增量计算OBV指标
        
        Args:
            existing_data: 已有的数据
            new_data: 新增的数据
            
        Returns:
            pl.DataFrame: 包含新增OBV值的数据
        """
        # 合并数据用于计算
        combined_data = existing_data.vstack(new_data).select(['date', 'close', 'volume'])
        
        # 计算价格变化
        price_change = combined_data['close'].diff()
        
        # 计算OBV
        obv_values = pl.Series([0.0] * len(combined_data))
        
        for i in range(1, len(combined_data)):
            if price_change[i] > 0:
                obv_values[i] = obv_values[i-1] + combined_data['volume'][i]
            elif price_change[i] < 0:
                obv_values[i] = obv_values[i-1] - combined_data['volume'][i]
            else:
                obv_values[i] = obv_values[i-1]
        
        # 提取新增数据对应的OBV值
        result = new_data.clone()
        result = result.with_columns(obv_values.tail(len(new_data)).alias('obv'))
        
        return result
    
    def incremental_calculate_expma(self, 
                                  existing_data: pl.DataFrame, 
                                  new_data: pl.DataFrame, 
                                  windows: List[int] = None, 
                                  **kwargs) -> pl.DataFrame:
        """
        增量计算EXPMA指标
        
        Args:
            existing_data: 已有的数据
            new_data: 新增的数据
            windows: EXPMA计算窗口列表
            
        Returns:
            pl.DataFrame: 包含新增EXPMA值的数据
        """
        if windows is None:
            windows = [12, 50]
        
        # 合并数据用于计算
        combined_data = existing_data.vstack(new_data).select(['date', 'close'])
        
        result = new_data.clone()
        
        for window in windows:
            # 计算EXPMA
            expma_col = f'expma{window}'
            expma_values = combined_data.with_columns(
                pl.col('close').ewm_mean(span=window).alias(expma_col)
            )[expma_col]
            
            # 提取新增数据对应的EXPMA值
            new_expma_values = expma_values.tail(len(new_data))
            result = result.with_columns(new_expma_values.alias(expma_col))
        
        return result


# 创建全局增量计算器实例
global_incremental_calculator = IncrementalCalculator()
