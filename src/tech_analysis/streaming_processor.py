#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
流式处理框架，支持实时数据的技术指标计算
"""

import polars as pl
from typing import List, Dict, Any, Optional, Callable
from loguru import logger
from .incremental_calculator import global_incremental_calculator
from .gpu_acceleration import calculate_with_gpu, is_gpu_available


class StreamingProcessor:
    """
    流式处理器类，支持实时数据的技术指标计算
    """
    
    def __init__(self, indicators: List[str], **indicator_params):
        """
        初始化流式处理器
        
        Args:
            indicators: 需要计算的指标列表
            **indicator_params: 指标计算参数
        """
        self.indicators = indicators
        self.indicator_params = indicator_params
        self.history_data = None  # 历史数据，用于增量计算
        self.calculated_indicators = {}
        
        # 初始化计算状态
        for indicator in indicators:
            self.calculated_indicators[indicator] = False
    
    def process_data(self, new_data: pl.DataFrame) -> pl.DataFrame:
        """
        处理新数据，计算指标
        
        Args:
            new_data: 新的数据流
            
        Returns:
            pl.DataFrame: 包含计算指标的数据
        """
        if new_data is None or len(new_data) == 0:
            return new_data
        
        # 确保数据按时间排序
        if 'date' in new_data.columns:
            new_data = new_data.sort('date')
        
        # 检查是否有历史数据
        if self.history_data is None:
            # 首次处理数据，使用完整计算
            result = self._calculate_initial(new_data)
            self.history_data = result
        else:
            # 有历史数据，使用增量计算
            result = self._calculate_incremental(new_data)
            self.history_data = self.history_data.vstack(result)
        
        return result
    
    def _calculate_initial(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        首次计算指标
        
        Args:
            data: 初始数据
            
        Returns:
            pl.DataFrame: 包含计算指标的数据
        """
        result = data.clone()
        
        for indicator in self.indicators:
            try:
                # 尝试使用GPU加速计算
                if is_gpu_available():
                    gpu_data = {
                        'close': data['close'].to_numpy() if 'close' in data.columns else None,
                        'high': data['high'].to_numpy() if 'high' in data.columns else None,
                        'low': data['low'].to_numpy() if 'low' in data.columns else None,
                        'volume': data['volume'].to_numpy() if 'volume' in data.columns else None
                    }
                    
                    gpu_result = calculate_with_gpu(indicator, gpu_data, **self.indicator_params)
                    if gpu_result:
                        logger.info(f"使用GPU计算{indicator}")
                        for col_name, values in gpu_result.items():
                            result = result.with_columns(
                                pl.Series(col_name, values).alias(col_name)
                            )
                        self.calculated_indicators[indicator] = True
                        continue
                
                # GPU计算失败或不可用，使用常规计算
                logger.info(f"使用常规计算{indicator}")
                from .indicator_manager import global_indicator_manager
                result = global_indicator_manager.calculate_indicator(
                    result, indicator, return_polars=True, **self.indicator_params
                )
                self.calculated_indicators[indicator] = True
            except Exception as e:
                logger.error(f"计算{indicator}失败: {e}")
        
        return result
    
    def _calculate_incremental(self, new_data: pl.DataFrame) -> pl.DataFrame:
        """
        增量计算指标
        
        Args:
            new_data: 新数据
            
        Returns:
            pl.DataFrame: 包含计算指标的数据
        """
        # 确保新数据包含所有必要的列
        result = new_data.clone()
        for col in self.history_data.columns:
            if col not in result.columns:
                if col in new_data.columns:
                    result = result.with_columns(new_data[col])
                else:
                    # 对于历史数据中存在但新数据中不存在的列，填充空值
                    result = result.with_columns(pl.Series([None] * len(result)).alias(col))
        
        # 按照历史数据的列顺序重新排列
        result = result.select(self.history_data.columns)
        
        for indicator in self.indicators:
            try:
                # 检查是否支持增量计算
                if global_incremental_calculator.is_supported(indicator):
                    logger.info(f"使用增量计算{indicator}")
                    # 准备增量计算所需的数据
                    # 只包含原始列，不包含之前计算的指标列
                    raw_new_data = new_data.select(['date', 'open', 'high', 'low', 'close', 'volume'])
                    raw_history_data = self.history_data.select(['date', 'open', 'high', 'low', 'close', 'volume'])
                    
                    incremental_result = global_incremental_calculator.incremental_calculate(
                        indicator, raw_history_data, raw_new_data, **self.indicator_params
                    )
                    
                    # 合并增量计算结果
                    for col in incremental_result.columns:
                        if col not in result.columns:
                            result = result.with_columns(incremental_result[col])
                        else:
                            # 更新已有列
                            result = result.with_columns(incremental_result[col].alias(col))
                else:
                    # 不支持增量计算，使用完整计算
                    logger.info(f"使用完整计算{indicator}")
                    from .indicator_manager import global_indicator_manager
                    # 合并历史数据和新数据进行计算
                    combined_data = self.history_data.vstack(result)
                    full_result = global_indicator_manager.calculate_indicator(
                        combined_data, indicator, return_polars=True, **self.indicator_params
                    )
                    
                    # 提取新数据对应的指标值
                    for col in full_result.columns:
                        if col not in ['date', 'open', 'high', 'low', 'close', 'volume']:
                            new_values = full_result[col].tail(len(new_data))
                            result = result.with_columns(new_values.alias(col))
            except Exception as e:
                logger.error(f"增量计算{indicator}失败: {e}")
        
        return result
    
    def get_history_data(self) -> Optional[pl.DataFrame]:
        """
        获取历史数据
        
        Returns:
            Optional[pl.DataFrame]: 历史数据
        """
        return self.history_data
    
    def reset(self):
        """
        重置流式处理器
        """
        self.history_data = None
        for indicator in self.indicators:
            self.calculated_indicators[indicator] = False
    
    def add_indicator(self, indicator: str):
        """
        添加指标
        
        Args:
            indicator: 指标类型
        """
        if indicator not in self.indicators:
            self.indicators.append(indicator)
            self.calculated_indicators[indicator] = False
    
    def remove_indicator(self, indicator: str):
        """
        移除指标
        
        Args:
            indicator: 指标类型
        """
        if indicator in self.indicators:
            self.indicators.remove(indicator)
            del self.calculated_indicators[indicator]


class RealTimeDataProcessor:
    """
    实时数据处理器，处理实时数据流
    """
    
    def __init__(self, batch_size: int = 1):
        """
        初始化实时数据处理器
        
        Args:
            batch_size: 批处理大小
        """
        self.batch_size = batch_size
        self.batch_data = []
        self.streaming_processors = {}
    
    def register_processor(self, name: str, processor: StreamingProcessor):
        """
        注册流式处理器
        
        Args:
            name: 处理器名称
            processor: 流式处理器实例
        """
        self.streaming_processors[name] = processor
    
    def process(self, data: pl.DataFrame, processor_name: str) -> Optional[pl.DataFrame]:
        """
        处理实时数据
        
        Args:
            data: 实时数据
            processor_name: 处理器名称
            
        Returns:
            Optional[pl.DataFrame]: 处理后的数据
        """
        if processor_name not in self.streaming_processors:
            logger.error(f"处理器{processor_name}不存在")
            return None
        
        # 添加到批处理
        self.batch_data.append(data)
        
        # 检查批处理大小
        if len(self.batch_data) >= self.batch_size:
            # 合并批数据
            batch = pl.concat(self.batch_data)
            self.batch_data = []
            
            # 处理数据
            processor = self.streaming_processors[processor_name]
            result = processor.process_data(batch)
            return result
        
        return None
    
    def flush(self, processor_name: str) -> Optional[pl.DataFrame]:
        """
        刷新批处理数据
        
        Args:
            processor_name: 处理器名称
            
        Returns:
            Optional[pl.DataFrame]: 处理后的数据
        """
        if not self.batch_data:
            return None
        
        if processor_name not in self.streaming_processors:
            logger.error(f"处理器{processor_name}不存在")
            return None
        
        # 合并批数据
        batch = pl.concat(self.batch_data)
        self.batch_data = []
        
        # 处理数据
        processor = self.streaming_processors[processor_name]
        result = processor.process_data(batch)
        return result
    
    def get_processor(self, name: str) -> Optional[StreamingProcessor]:
        """
        获取流式处理器
        
        Args:
            name: 处理器名称
            
        Returns:
            Optional[StreamingProcessor]: 流式处理器实例
        """
        return self.streaming_processors.get(name)
    
    def remove_processor(self, name: str):
        """
        移除流式处理器
        
        Args:
            name: 处理器名称
        """
        if name in self.streaming_processors:
            del self.streaming_processors[name]


# 创建全局实时数据处理器实例
global_realtime_processor = RealTimeDataProcessor()


def create_streaming_processor(indicators: List[str], **kwargs) -> StreamingProcessor:
    """
    创建流式处理器
    
    Args:
        indicators: 需要计算的指标列表
        **kwargs: 指标计算参数
        
    Returns:
        StreamingProcessor: 流式处理器实例
    """
    return StreamingProcessor(indicators, **kwargs)


def process_realtime_data(data: pl.DataFrame, processor_name: str, batch_size: int = 1) -> Optional[pl.DataFrame]:
    """
    处理实时数据
    
    Args:
        data: 实时数据
        processor_name: 处理器名称
        batch_size: 批处理大小
        
    Returns:
        Optional[pl.DataFrame]: 处理后的数据
    """
    # 如果处理器不存在，创建一个默认处理器
    if processor_name not in global_realtime_processor.streaming_processors:
        # 默认计算MA、MACD、RSI指标
        processor = StreamingProcessor(['ma', 'macd', 'rsi'])
        global_realtime_processor.register_processor(processor_name, processor)
        global_realtime_processor.batch_size = batch_size
    
    return global_realtime_processor.process(data, processor_name)
