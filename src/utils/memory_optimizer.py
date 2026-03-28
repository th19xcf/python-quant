#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
内存优化工具模块
提供数据类型优化和内存管理功能

特性：
1. 支持更多数据类型的内存优化
2. 增加内存使用监控和告警机制
3. 优化大数据集的处理方式
4. 支持分块处理和数据压缩
"""

import polars as pl
import numpy as np
import psutil
import time
import threading
from typing import Dict, List, Optional, Any, Callable, Tuple
from functools import wraps
import gc
import logging

logger = logging.getLogger(__name__)


class MemoryMonitor:
    """
    内存监控器
    提供内存使用监控和告警功能
    """
    
    def __init__(self, interval: int = 5, threshold_mb: float = 500):
        """
        初始化内存监控器
        
        Args:
            interval: 监控间隔（秒）
            threshold_mb: 内存使用阈值（MB）
        """
        self.interval = interval
        self.threshold_mb = threshold_mb
        self.running = False
        self.thread = None
        self.memory_history = []
        self.max_history = 100
        self.callbacks = []
    
    def start(self):
        """开始内存监控"""
        self.running = True
        self.thread = threading.Thread(target=self._monitor, daemon=True)
        self.thread.start()
        logger.info(f"内存监控已启动，监控间隔: {self.interval}秒，阈值: {self.threshold_mb}MB")
    
    def stop(self):
        """停止内存监控"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=2)
        logger.info("内存监控已停止")
    
    def _monitor(self):
        """监控线程函数"""
        while self.running:
            # 获取当前内存使用
            memory_usage = psutil.Process().memory_info().rss / (1024 * 1024)
            timestamp = time.time()
            
            # 记录内存使用历史
            self.memory_history.append((timestamp, memory_usage))
            if len(self.memory_history) > self.max_history:
                self.memory_history.pop(0)
            
            # 检查是否超过阈值
            if memory_usage > self.threshold_mb:
                self._alert(memory_usage)
            
            # 调用回调函数
            for callback in self.callbacks:
                try:
                    callback(memory_usage, timestamp)
                except Exception as e:
                    logger.exception(f"内存监控回调执行错误: {e}")
            
            time.sleep(self.interval)
    
    def _alert(self, memory_usage: float):
        """内存使用告警"""
        logger.warning(f"内存使用超过阈值: {memory_usage:.2f}MB > {self.threshold_mb}MB")
    
    def add_callback(self, callback: Callable[[float, float], None]):
        """添加内存监控回调
        
        Args:
            callback: 回调函数，接收内存使用量和时间戳
        """
        self.callbacks.append(callback)
    
    def remove_callback(self, callback: Callable[[float, float], None]):
        """移除内存监控回调
        
        Args:
            callback: 要移除的回调函数
        """
        if callback in self.callbacks:
            self.callbacks.remove(callback)
    
    def get_memory_history(self) -> List[Tuple[float, float]]:
        """获取内存使用历史
        
        Returns:
            内存使用历史列表，每个元素为(时间戳, 内存使用量MB)
        """
        return self.memory_history.copy()
    
    def get_current_memory(self) -> float:
        """获取当前内存使用
        
        Returns:
            当前内存使用量（MB）
        """
        return psutil.Process().memory_info().rss / (1024 * 1024)


class MemoryOptimizer:
    """
    内存优化器
    提供 DataFrame 类型优化和内存管理功能
    """
    
    # 列类型映射配置
    COLUMN_TYPE_MAP = {
        # 价格字段 -> Float32
        'price_cols': ['open', 'high', 'low', 'close', 
                      'qfq_open', 'qfq_high', 'qfq_low', 'qfq_close',
                      'hfq_open', 'hfq_high', 'hfq_low', 'hfq_close',
                      'pre_close'],
        # 成交量字段 -> Float32 (或 Int32)
        'volume_cols': ['volume', 'vol', 'amount'],
        # 百分比字段 -> Float32
        'pct_cols': ['pct_chg', 'change', 'qfq_factor', 'hfq_factor'],
        # 整数字段 -> Int32
        'int_cols': ['up_count', 'down_count', 'flat_count', 'total_count'],
        # 指标字段 -> Float32
        'indicator_cols': ['ma5', 'ma10', 'ma20', 'ma60', 'macd', 'macd_signal', 'macd_hist',
                          'k', 'd', 'j', 'rsi', 'rsi6', 'rsi12', 'rsi14', 'wr', 'boll_upper',
                          'boll_mid', 'boll_lower', 'dmi_pdi', 'dmi_mdi', 'dmi_adx', 'cci',
                          'roc', 'mtm', 'obv', 'vr', 'psy', 'trix', 'brar', 'asi', 'emv',
                          'mcst', 'dma', 'fsl', 'sar', 'vol_tdx', 'cr'],
        # 分类字段 -> Categorical
        'category_cols': ['industry', 'market', 'area', 'status', 'market_type'],
        # 布尔字段 -> Boolean
        'bool_cols': ['is_st', 'is_hs300', 'is_sz50', 'is_zz500'],
        # 小数字段 -> Float32
        'decimal_cols': ['pe', 'pb', 'ps', 'pcf'],
    }
    
    # 内存使用阈值配置
    MEMORY_THRESHOLDS = {
        'high_memory_mb': 500,  # 高内存使用阈值
        'medium_memory_mb': 200,  # 中等内存使用阈值
        'low_memory_mb': 50,  # 低内存使用阈值
    }
    
    @classmethod
    def optimize_dataframe(cls, df: pl.DataFrame, 
                          preserve_cols: Optional[List[str]] = None, 
                          enable_sparse: bool = False, 
                          enable_compression: bool = False) -> pl.DataFrame:
        """
        优化 DataFrame 数据类型
        
        Args:
            df: 输入 DataFrame
            preserve_cols: 需要保持原样的列
            enable_sparse: 是否启用稀疏数据优化
            enable_compression: 是否启用数据压缩
            
        Returns:
            pl.DataFrame: 优化后的 DataFrame
        """
        if df.is_empty():
            return df
        
        preserve_cols = preserve_cols or []
        type_casts = []
        
        for col in df.columns:
            if col in preserve_cols:
                continue
            
            dtype = df[col].dtype
            
            # 已经是优化类型的跳过
            if dtype in [pl.Float32, pl.Int32, pl.Int16, pl.Int8, pl.Date, pl.Boolean, pl.Categorical]:
                continue
            
            # 根据列名映射类型
            if col in cls.COLUMN_TYPE_MAP['price_cols']:
                if dtype in [pl.Float64, pl.Decimal]:
                    type_casts.append(pl.col(col).cast(pl.Float32))
                    
            elif col in cls.COLUMN_TYPE_MAP['volume_cols']:
                if dtype == pl.Float64:
                    # 保持Float64类型，避免精度损失
                    type_casts.append(pl.col(col).cast(pl.Float64))
                elif dtype == pl.Int64:
                    # 检查数据范围，尝试使用更小的整数类型
                    max_val = df[col].max()
                    if max_val < 2**32:
                        type_casts.append(pl.col(col).cast(pl.Int32))
                    else:
                        type_casts.append(pl.col(col).cast(pl.Int64))
                    
            elif col in cls.COLUMN_TYPE_MAP['pct_cols']:
                if dtype in [pl.Float64, pl.Decimal]:
                    type_casts.append(pl.col(col).cast(pl.Float32))
                    
            elif col in cls.COLUMN_TYPE_MAP['int_cols']:
                if dtype == pl.Int64:
                    # 检查数据范围，尝试使用更小的整数类型
                    max_val = df[col].max()
                    if max_val < 2**8:
                        type_casts.append(pl.col(col).cast(pl.Int8))
                    elif max_val < 2**16:
                        type_casts.append(pl.col(col).cast(pl.Int16))
                    else:
                        type_casts.append(pl.col(col).cast(pl.Int32))
            
            elif col in cls.COLUMN_TYPE_MAP['indicator_cols']:
                if dtype == pl.Float64:
                    type_casts.append(pl.col(col).cast(pl.Float32))
            
            elif col in cls.COLUMN_TYPE_MAP['category_cols']:
                # 转换为分类类型
                if dtype == pl.Utf8:
                    # 检查唯一值数量，只有当唯一值较少时才使用分类类型
                    unique_count = df[col].n_unique()
                    total_count = len(df)
                    if unique_count < total_count * 0.1:  # 唯一值少于10%
                        type_casts.append(pl.col(col).cast(pl.Categorical))
            
            elif col in cls.COLUMN_TYPE_MAP['bool_cols']:
                # 转换为布尔类型
                if dtype in [pl.Int64, pl.Float64, pl.Utf8]:
                    type_casts.append(pl.col(col).cast(pl.Boolean))
            
            elif col in cls.COLUMN_TYPE_MAP['decimal_cols']:
                # 小数字段转换为Float32
                if dtype in [pl.Float64, pl.Decimal]:
                    type_casts.append(pl.col(col).cast(pl.Float32))
            
            # 日期类型优化
            elif 'date' in col.lower() or 'time' in col.lower():
                if dtype == pl.Datetime:
                    # 如果只需要日期部分，转换为 Date
                    type_casts.append(pl.col(col).cast(pl.Date))
                elif dtype == pl.Utf8:
                    # 尝试解析日期字符串，使用strict=False避免转换失败
                    try:
                        # 尝试多种日期格式
                        date_formats = ['%Y%m%d', '%Y-%m-%d', '%Y/%m/%d']
                        # 先尝试常用格式
                        try:
                            type_casts.append(
                                pl.col(col).str.strptime(pl.Date, '%Y-%m-%d', strict=False)
                            )
                        except:
                            # 如果失败，尝试其他格式
                            try:
                                type_casts.append(
                                    pl.col(col).str.strptime(pl.Date, '%Y%m%d', strict=False)
                                )
                            except:
                                try:
                                    type_casts.append(
                                        pl.col(col).str.strptime(pl.Date, '%Y/%m/%d', strict=False)
                                    )
                                except:
                                    pass
                    except:
                        pass
            
            # 通用类型优化
            elif dtype == pl.Float64:
                # 对于其他浮点列，统一转换为Float32
                type_casts.append(pl.col(col).cast(pl.Float32))
            
            elif dtype == pl.Int64:
                # 对于其他整数列，检查范围后转换
                max_val = df[col].max()
                if max_val < 2**8:
                    type_casts.append(pl.col(col).cast(pl.Int8))
                elif max_val < 2**16:
                    type_casts.append(pl.col(col).cast(pl.Int16))
                else:
                    type_casts.append(pl.col(col).cast(pl.Int32))
            
            # 字符串类型优化
            elif dtype == pl.Utf8:
                # 检查字符串长度，考虑使用更高效的存储方式
                max_length = df[col].str.lengths().max()
                if max_length < 100:
                    # 短字符串可以保持Utf8类型
                    pass
                else:
                    # 长字符串考虑使用更高效的存储
                    unique_count = df[col].n_unique()
                    total_count = len(df)
                    if unique_count < total_count * 0.1:
                        type_casts.append(pl.col(col).cast(pl.Categorical))
            
            # 列表类型优化
            elif hasattr(dtype, 'inner'):
                # 处理List类型
                inner_dtype = dtype.inner
                if inner_dtype == pl.Float64:
                    # 列表中的Float64转换为Float32
                    type_casts.append(pl.col(col).cast(pl.List(pl.Float32)))
                elif inner_dtype == pl.Int64:
                    # 列表中的Int64转换为Int32
                    type_casts.append(pl.col(col).cast(pl.List(pl.Int32)))
        
        # 执行类型转换
        if type_casts:
            df = df.with_columns(type_casts)
        
        # 启用稀疏数据优化（如果需要）
        if enable_sparse:
            df = cls._optimize_sparse_data(df, preserve_cols)
        
        # 启用数据压缩（如果需要）
        if enable_compression:
            df = cls._optimize_compression(df, preserve_cols)
        
        return df
    
    @classmethod
    def process_large_dataset(cls, df: pl.DataFrame, chunk_size: int = 10000, **kwargs) -> pl.DataFrame:
        """
        分块处理大数据集
        
        Args:
            df: 输入 DataFrame
            chunk_size: 块大小
            **kwargs: 传递给 optimize_dataframe 的参数
            
        Returns:
            pl.DataFrame: 处理后的 DataFrame
        """
        if len(df) <= chunk_size:
            return cls.optimize_dataframe(df, **kwargs)
        
        # 分块处理
        chunks = []
        total_rows = len(df)
        for i in range(0, total_rows, chunk_size):
            chunk = df.slice(i, min(chunk_size, total_rows - i))
            optimized_chunk = cls.optimize_dataframe(chunk, **kwargs)
            chunks.append(optimized_chunk)
            logger.info(f"处理块 {i//chunk_size + 1}/{(total_rows + chunk_size - 1)//chunk_size}")
        
        # 合并块
        return pl.concat(chunks)
    
    @classmethod
    def estimate_memory_usage(cls, df: pl.DataFrame) -> Dict[str, Any]:
        """
        估算 DataFrame 内存使用
        
        Args:
            df: 输入 DataFrame
            
        Returns:
            Dict: 内存使用统计 (单位: MB)
        """
        total_bytes = 0
        column_usage = {}
        
        for col in df.columns:
            dtype = df[col].dtype
            row_count = len(df)
            
            # 估算每种类型的字节数
            bytes_per_element = {
                pl.Float64: 8, pl.Float32: 4,
                pl.Int64: 8, pl.Int32: 4, pl.Int16: 2, pl.Int8: 1,
                pl.Boolean: 1,
                pl.Date: 4, pl.Datetime: 8,
                pl.Utf8: 50,  # 字符串平均估算
                pl.Categorical: 2,  # 分类类型平均估算
            }.get(dtype, 8)  # 默认 8 字节
            
            # 处理复杂类型
            if hasattr(dtype, 'inner'):
                # List类型
                inner_dtype = dtype.inner
                inner_bytes = {
                    pl.Float64: 8, pl.Float32: 4,
                    pl.Int64: 8, pl.Int32: 4, pl.Int16: 2, pl.Int8: 1,
                }.get(inner_dtype, 8)
                # 假设每个列表平均有2个元素
                bytes_per_element = 4 + (inner_bytes * 2)
            
            col_bytes = row_count * bytes_per_element
            column_usage[col] = {
                'mb': col_bytes / (1024 * 1024),
                'dtype': str(dtype),
                'rows': row_count
            }
            total_bytes += col_bytes
        
        return {
            'total_mb': total_bytes / (1024 * 1024),
            'columns': column_usage,
            'row_count': row_count
        }
    
    @classmethod
    def _optimize_sparse_data(cls, df: pl.DataFrame, preserve_cols: List[str]) -> pl.DataFrame:
        """
        优化稀疏数据，减少内存使用
        
        Args:
            df: 输入 DataFrame
            preserve_cols: 需要保持原样的列
            
        Returns:
            pl.DataFrame: 优化后的 DataFrame
        """
        sparse_casts = []
        
        for col in df.columns:
            if col in preserve_cols:
                continue
            
            # 检查列的稀疏性
            null_count = df[col].null_count()
            total_count = len(df)
            null_ratio = null_count / total_count
            
            # 如果空值比例超过50%，考虑使用更高效的存储方式
            if null_ratio > 0.5:
                dtype = df[col].dtype
                
                # 对于数值列，考虑使用Nullable类型
                if dtype in [pl.Float32, pl.Float64, pl.Int32, pl.Int64]:
                    # Polars会自动优化Nullable类型的存储
                    sparse_casts.append(pl.col(col))
        
        if sparse_casts:
            # 重新构建DataFrame以优化存储
            df = df.select([pl.col(c) for c in df.columns])
        
        return df
    
    @classmethod
    def _optimize_compression(cls, df: pl.DataFrame, preserve_cols: List[str]) -> pl.DataFrame:
        """
        优化数据压缩，减少内存使用
        
        Args:
            df: 输入 DataFrame
            preserve_cols: 需要保持原样的列
            
        Returns:
            pl.DataFrame: 优化后的 DataFrame
        """
        # Polars会自动处理数据压缩，这里主要是确保使用最优的存储方式
        # 重新构建DataFrame以启用压缩
        df = df.select([pl.col(c) for c in df.columns])
        return df
    
    @classmethod
    def optimize_dataframe_inplace(cls, df: pl.DataFrame, **kwargs) -> pl.DataFrame:
        """
        原地优化 DataFrame 数据类型
        
        Args:
            df: 输入 DataFrame
            **kwargs: 传递给 optimize_dataframe 的参数
            
        Returns:
            pl.DataFrame: 优化后的 DataFrame
        """
        optimized_df = cls.optimize_dataframe(df, **kwargs)
        return optimized_df
    
    @classmethod
    def get_memory_usage_level(cls, memory_mb: float) -> str:
        """
        获取内存使用级别
        
        Args:
            memory_mb: 内存使用量（MB）
            
        Returns:
            str: 内存使用级别
        """
        if memory_mb > cls.MEMORY_THRESHOLDS['high_memory_mb']:
            return 'high'
        elif memory_mb > cls.MEMORY_THRESHOLDS['medium_memory_mb']:
            return 'medium'
        elif memory_mb > cls.MEMORY_THRESHOLDS['low_memory_mb']:
            return 'low'
        else:
            return 'very_low'
    
    @classmethod
    def suggest_optimization(cls, df: pl.DataFrame) -> Dict[str, Any]:
        """
        分析 DataFrame 并提供优化建议
        
        Args:
            df: 输入 DataFrame
            
        Returns:
            Dict[str, Any]: 优化建议
        """
        stats = cls.estimate_memory_usage(df)
        suggestions = []
        
        # 分析每列的内存使用
        for col_name, col_stats in stats['columns'].items():
            dtype = col_stats['dtype']
            memory_mb = col_stats['mb']
            
            # 检查是否可以进一步优化
            if dtype == 'Float64' and memory_mb > 1:
                suggestions.append(f"列 '{col_name}' 使用 Float64 类型，建议转换为 Float32")
            elif dtype == 'Int64' and memory_mb > 1:
                suggestions.append(f"列 '{col_name}' 使用 Int64 类型，建议转换为更小的整数类型")
            elif dtype == 'Datetime' and 'date' in col_name.lower():
                suggestions.append(f"列 '{col_name}' 使用 Datetime 类型，建议转换为 Date")
            elif dtype == 'Utf8' and memory_mb > 5:
                unique_count = df[col_name].n_unique()
                total_count = len(df)
                if unique_count < total_count * 0.1:
                    suggestions.append(f"列 '{col_name}' 唯一值较少，建议转换为 Categorical 类型")
            elif 'List' in dtype and memory_mb > 10:
                suggestions.append(f"列 '{col_name}' 是 List 类型且内存使用较大，建议优化内部元素类型")
        
        # 检查数据集大小
        if len(df) > 100000:
            suggestions.append(f"数据集较大（{len(df):,}行），建议使用分块处理")
        
        return {
            'current_memory_mb': stats['total_mb'],
            'memory_level': cls.get_memory_usage_level(stats['total_mb']),
            'suggestions': suggestions,
            'estimated_savings_mb': stats['total_mb'] * 0.3  # 估计可节省30%内存
        }
    
    @classmethod
    def print_memory_stats(cls, df: pl.DataFrame, title: str = "DataFrame"):
        """
        打印内存使用统计
        
        Args:
            df: 输入 DataFrame
            title: 标题
        """
        stats = cls.estimate_memory_usage(df)
        
        print(f"\n{'='*60}")
        print(f"{title} 内存使用统计")
        print(f"{'='*60}")
        print(f"总行数: {stats['row_count']:,}")
        print(f"总内存: {stats['total_mb']:.2f} MB")
        print(f"内存级别: {cls.get_memory_usage_level(stats['total_mb'])}")
        print(f"\n各列内存使用:")
        print(f"{'列名':<20} {'类型':<15} {'内存(MB)':<10} {'占比':<8}")
        print("-" * 60)
        
        # 按内存使用排序
        sorted_cols = sorted(stats['columns'].items(), 
                           key=lambda x: x[1]['mb'], 
                           reverse=True)
        
        for col_name, col_stats in sorted_cols:
            percentage = (col_stats['mb'] / stats['total_mb'] * 100) if stats['total_mb'] > 0 else 0
            print(f"{col_name:<20} {col_stats['dtype']:<15} {col_stats['mb']:<10.2f} {percentage:<8.2f}%")
        
        # 打印优化建议
        suggestions = cls.suggest_optimization(df)
        if suggestions['suggestions']:
            print(f"\n优化建议:")
            for i, suggestion in enumerate(suggestions['suggestions'], 1):
                print(f"{i}. {suggestion}")
            print(f"\n估计可节省内存: {suggestions['estimated_savings_mb']:.2f} MB")
        
        print(f"{'='*60}\n")


# 全局内存监控器
global_memory_monitor = MemoryMonitor()


def optimize_memory(func):
    """
    装饰器：自动优化函数返回的 DataFrame 内存使用
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        
        # 如果返回的是 DataFrame，进行优化
        if isinstance(result, pl.DataFrame):
            result = MemoryOptimizer.optimize_dataframe(result)
        
        return result
    
    return wrapper


def cleanup_memory():
    """
    强制垃圾回收，释放内存
    """
    gc.collect()


# 便捷函数
def optimize_df(df: pl.DataFrame, **kwargs) -> pl.DataFrame:
    """便捷函数：优化DataFrame"""
    return MemoryOptimizer.optimize_dataframe(df, **kwargs)


def process_large_df(df: pl.DataFrame, **kwargs) -> pl.DataFrame:
    """便捷函数：处理大数据集"""
    return MemoryOptimizer.process_large_dataset(df, **kwargs)


def print_df_memory(df: pl.DataFrame, title: str = "DataFrame"):
    """便捷函数：打印DataFrame内存统计"""
    MemoryOptimizer.print_memory_stats(df, title)


def get_memory_monitor() -> MemoryMonitor:
    """获取全局内存监控器"""
    return global_memory_monitor

