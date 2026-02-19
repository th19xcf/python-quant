#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
内存优化工具模块
提供数据类型优化和内存管理功能
"""

import polars as pl
import numpy as np
from typing import Dict, List, Optional, Any
from functools import wraps
import gc


class MemoryOptimizer:
    """
    内存优化器
    提供 DataFrame 类型优化和内存回收功能
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
        'int_cols': ['up_count', 'down_count', 'flat_count'],
        # 指标字段 -> Float32
        'indicator_cols': ['ma5', 'ma10', 'ma20', 'ma60', 'macd', 'macd_signal', 'macd_hist',
                          'k', 'd', 'j', 'rsi', 'rsi6', 'rsi12', 'rsi14'],
    }
    
    @classmethod
    def optimize_dataframe(cls, df: pl.DataFrame, 
                          preserve_cols: Optional[List[str]] = None) -> pl.DataFrame:
        """
        优化 DataFrame 数据类型
        
        Args:
            df: 输入 DataFrame
            preserve_cols: 需要保持原样的列
            
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
            if dtype in [pl.Float32, pl.Int32, pl.Int16, pl.Int8, pl.Date, pl.Boolean]:
                continue
            
            # 根据列名映射类型
            if col in cls.COLUMN_TYPE_MAP['price_cols']:
                if dtype in [pl.Float64, pl.Decimal]:
                    type_casts.append(pl.col(col).cast(pl.Float32))
                    
            elif col in cls.COLUMN_TYPE_MAP['volume_cols']:
                if dtype == pl.Float64:
                    type_casts.append(pl.col(col).cast(pl.Float32))
                elif dtype == pl.Int64:
                    type_casts.append(pl.col(col).cast(pl.Int32))
                    
            elif col in cls.COLUMN_TYPE_MAP['pct_cols']:
                if dtype in [pl.Float64, pl.Decimal]:
                    type_casts.append(pl.col(col).cast(pl.Float32))
                    
            elif col in cls.COLUMN_TYPE_MAP['int_cols']:
                if dtype == pl.Int64:
                    type_casts.append(pl.col(col).cast(pl.Int32))
            
            elif col in cls.COLUMN_TYPE_MAP['indicator_cols']:
                if dtype == pl.Float64:
                    type_casts.append(pl.col(col).cast(pl.Float32))
                    
            # 日期类型优化
            elif 'date' in col.lower() or 'time' in col.lower():
                if dtype == pl.Datetime:
                    # 如果只需要日期部分，转换为 Date
                    type_casts.append(pl.col(col).cast(pl.Date))
                elif dtype == pl.Utf8:
                    # 尝试解析日期字符串
                    try:
                        type_casts.append(
                            pl.col(col).str.strptime(pl.Date, '%Y%m%d')
                        )
                    except:
                        pass
        
        return df.with_columns(type_casts) if type_casts else df
    
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
            }.get(dtype, 8)  # 默认 8 字节
            
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
        print(f"\n各列内存使用:")
        print(f"{'列名':<20} {'类型':<15} {'内存(MB)':<10}")
        print("-" * 60)
        
        # 按内存使用排序
        sorted_cols = sorted(stats['columns'].items(), 
                           key=lambda x: x[1]['mb'], 
                           reverse=True)
        
        for col_name, col_stats in sorted_cols:
            print(f"{col_name:<20} {col_stats['dtype']:<15} {col_stats['mb']:<10.2f}")
        
        print(f"{'='*60}\n")


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
def optimize_df(df: pl.DataFrame) -> pl.DataFrame:
    """便捷函数：优化DataFrame"""
    return MemoryOptimizer.optimize_dataframe(df)


def print_df_memory(df: pl.DataFrame, title: str = "DataFrame"):
    """便捷函数：打印DataFrame内存统计"""
    MemoryOptimizer.print_memory_stats(df, title)
