#!/usr/bin/env python3

"""
内存优化工具模块
提供数据类型优化和内存管理功能

特性：
1. 支持更多数据类型的内存优化
2. 优化大数据集的处理方式
3. 支持分块处理和数据压缩
"""

from typing import Any, Dict, List, cast

import polars as pl

from src.utils.logger import logger


class MemoryOptimizer:
    """
    内存优化器
    提供 DataFrame 类型优化和内存管理功能
    """

    # 列类型映射配置
    COLUMN_TYPE_MAP = {
        # 价格字段与复权因子 -> 保留 Float64（见 optimize_dataframe 中的白名单分支）
        # qfq_factor/hfq_factor 参与复权连乘与"原始价×因子"还原，Float32 的
        # ~7 位有效位会在连乘中累积出可观测误差，严禁按百分比列降精度
        'price_cols': ['open', 'high', 'low', 'close',
                      'pre_close', 'qfq_factor', 'hfq_factor'],
        # 成交量字段 -> Float32 (或 Int32)
        'volume_cols': ['volume', 'vol', 'amount'],
        # 百分比字段 -> Float32
        'pct_cols': ['pct_chg', 'change'],
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

    # 有符号整数上限（Polars Int8/Int16/Int32 均为有符号类型）
    INT8_MAX = 127
    INT16_MAX = 32767
    INT32_MAX = 2_147_483_647

    # 内存使用阈值配置
    MEMORY_THRESHOLDS = {
        'high_memory_mb': 500,  # 高内存使用阈值
        'medium_memory_mb': 200,  # 中等内存使用阈值
        'low_memory_mb': 50,  # 低内存使用阈值
    }

    @classmethod
    def optimize_dataframe(cls, df: pl.DataFrame,
                           preserve_cols: List[str] | None = None,
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

            expr = cls._column_cast_expr(df, col)
            if expr is not None:
                type_casts.append(expr)

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
    def _column_cast_expr(cls, df: pl.DataFrame, col: str) -> Any:
        """按列名映射或 dtype 规则，返回该列的类型转换表达式；无需转换返回 None。"""
        dtype = df[col].dtype

        # 根据列名映射类型
        if col in cls.COLUMN_TYPE_MAP['price_cols']:
            # 价格列必须保留 Float64，否则累计收益、涨跌停比较、
            # 复权因子连乘会出现可观测精度误差（量化场景不能降精度）
            return pl.col(col).cast(pl.Float64) if dtype == pl.Decimal else None
        if col in cls.COLUMN_TYPE_MAP['volume_cols']:
            return cls._volume_cast_expr(df, col, dtype)
        if col in cls.COLUMN_TYPE_MAP['pct_cols']:
            return pl.col(col).cast(pl.Float32) if dtype in [pl.Float64, pl.Decimal] else None
        if col in cls.COLUMN_TYPE_MAP['int_cols']:
            return cls._narrow_int_expr(df, col) if dtype == pl.Int64 else None
        if col in cls.COLUMN_TYPE_MAP['indicator_cols']:
            return pl.col(col).cast(pl.Float32) if dtype == pl.Float64 else None
        if col in cls.COLUMN_TYPE_MAP['category_cols']:
            return cls._category_cast_expr(df, col, dtype)
        if col in cls.COLUMN_TYPE_MAP['bool_cols']:
            return pl.col(col).cast(pl.Boolean) if dtype in [pl.Int64, pl.Float64, pl.Utf8] else None
        if col in cls.COLUMN_TYPE_MAP['decimal_cols']:
            return pl.col(col).cast(pl.Float32) if dtype in [pl.Float64, pl.Decimal] else None

        # 日期类型优化
        if 'date' in col.lower() or 'time' in col.lower():
            return cls._date_cast_expr(col, dtype)

        # 通用类型优化
        if dtype == pl.Float64:
            # 未知浮点列（Alpha 因子、外部特征等）保持 Float64：
            # 通用降精度曾把高精度因子截断为 ~7 位有效位，造成因子
            # 排序/加权出现可观测偏差。仅 COLUMN_TYPE_MAP 中显式列
            # （pct_cols / indicator_cols / decimal_cols）允许降 Float32。
            return None
        if dtype == pl.Int64:
            return cls._narrow_int_expr(df, col)
        if dtype == pl.Utf8:
            return cls._string_cast_expr(df, col)
        if hasattr(dtype, 'inner'):
            return cls._list_cast_expr(col, dtype)
        return None

    @classmethod
    def _volume_cast_expr(cls, df: pl.DataFrame, col: str, dtype) -> Any:
        """成交量列：Float64 保持不变，Int64 按有符号上限缩到 Int32。"""
        if dtype == pl.Float64:
            # 保持Float64类型，避免精度损失
            return pl.col(col).cast(pl.Float64)
        if dtype == pl.Int64:
            # Int32 是有符号类型，上限 2**31-1；曾误用 2**32 导致
            # 成交量 ~3e9 时按 Int32 转换抛 InvalidOperationError（溢出）
            max_val: Any = df[col].max()
            if max_val is not None and max_val <= cls.INT32_MAX:
                return pl.col(col).cast(pl.Int32)
            return pl.col(col).cast(pl.Int64)
        return None

    @classmethod
    def _narrow_int_expr(cls, df: pl.DataFrame, col: str) -> Any:
        """按有符号上限把 Int64 缩到可容纳的最小整数类型。"""
        # 各整数类型的上限必须按有符号区间判断，避免 Int8/Int16
        # 在 256/65536 边界附近溢出抛 InvalidOperationError
        max_val: Any = df[col].max()
        if max_val is None:
            return pl.col(col).cast(pl.Int32)
        if max_val <= cls.INT8_MAX:
            return pl.col(col).cast(pl.Int8)
        if max_val <= cls.INT16_MAX:
            return pl.col(col).cast(pl.Int16)
        if max_val <= cls.INT32_MAX:
            return pl.col(col).cast(pl.Int32)
        return pl.col(col).cast(pl.Int64)

    @classmethod
    def _category_cast_expr(cls, df: pl.DataFrame, col: str, dtype) -> Any:
        """字符串列唯一值少于 10% 时转 Categorical。"""
        if dtype != pl.Utf8:
            return None
        unique_count = df[col].n_unique()
        total_count = len(df)
        if unique_count < total_count * 0.1:  # 唯一值少于10%
            return pl.col(col).cast(pl.Categorical)
        return None

    @classmethod
    def _date_cast_expr(cls, col: str, dtype) -> Any:
        """日期/时间列：Datetime 转 Date，字符串按多种格式尝试解析。"""
        if dtype == pl.Datetime:
            # 如果只需要日期部分，转换为 Date
            return pl.col(col).cast(pl.Date)
        if dtype != pl.Utf8:
            return None
        # 尝试解析日期字符串，使用strict=False避免转换失败
        try:
            try:
                return pl.col(col).str.strptime(pl.Date, '%Y-%m-%d', strict=False)
            # 兜底理由：日期格式解析失败尝试下一种格式
            except Exception as e:  # noqa: BLE001
                logger.debug(f"日期格式 %Y-%m-%d 解析失败，尝试下一种: {e}", exc_info=True)
                return pl.col(col).str.strptime(pl.Date, '%Y%m%d', strict=False)
        # 兜底理由：日期格式解析失败尝试下一种格式
        except Exception as e2:  # noqa: BLE001
            logger.debug(f"日期格式 %Y%m%d 解析失败，尝试下一种: {e2}", exc_info=True)
            try:
                return pl.col(col).str.strptime(pl.Date, '%Y/%m/%d', strict=False)
            # 兜底理由：日期格式解析失败保留原类型
            except Exception as e3:  # noqa: BLE001
                logger.debug(f"日期格式 %Y/%m/%d 解析失败，保留原类型: {e3}", exc_info=True)
                return None

    @classmethod
    def _string_cast_expr(cls, df: pl.DataFrame, col: str) -> Any:
        """长字符串且唯一值少于 10% 时转 Categorical。"""
        # 检查字符串长度，考虑使用更高效的存储方式
        string_series = df[col].drop_nulls()
        if string_series.len() == 0:
            max_length = 0
        else:
            max_length = cast(int, string_series.map_elements(
                lambda x: len(x),
                return_dtype=pl.Int32,
            ).max() or 0)
        if max_length < 100:
            # 短字符串可以保持Utf8类型
            return None
        # 长字符串考虑使用更高效的存储
        unique_count = df[col].n_unique()
        total_count = len(df)
        if unique_count < total_count * 0.1:
            return pl.col(col).cast(pl.Categorical)
        return None

    @classmethod
    def _list_cast_expr(cls, col: str, dtype) -> Any:
        """List 列：内层 Float64 降 Float32、Int64 降 Int32。"""
        inner_dtype = dtype.inner
        if inner_dtype == pl.Float64:
            return pl.col(col).cast(pl.List(pl.Float32))
        if inner_dtype == pl.Int64:
            return pl.col(col).cast(pl.List(pl.Int32))
        return None

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
            sizes: Dict[Any, int] = {
                pl.Float64: 8, pl.Float32: 4,
                pl.Int64: 8, pl.Int32: 4, pl.Int16: 2, pl.Int8: 1,
                pl.Boolean: 1,
                pl.Date: 4, pl.Datetime: 8,
                pl.Utf8: 50,  # 字符串平均估算
                pl.Categorical: 2,  # 分类类型平均估算
            }
            bytes_per_element = sizes.get(dtype, 8)  # 默认 8 字节

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
        """记录内存使用统计（多行汇总为一条 logger.info）。"""
        stats = cls.estimate_memory_usage(df)

        lines = [
            f"\n{'='*60}",
            f"{title} 内存使用统计",
            f"{'='*60}",
            f"总行数: {stats['row_count']:,}",
            f"总内存: {stats['total_mb']:.2f} MB",
            f"内存级别: {cls.get_memory_usage_level(stats['total_mb'])}",
            "\n各列内存使用:",
            f"{'列名':<20} {'类型':<15} {'内存(MB)':<10} {'占比':<8}",
            "-" * 60,
        ]

        sorted_cols = sorted(stats['columns'].items(),
                           key=lambda x: x[1]['mb'],
                           reverse=True)
        for col_name, col_stats in sorted_cols:
            percentage = (col_stats['mb'] / stats['total_mb'] * 100) if stats['total_mb'] > 0 else 0
            lines.append(f"{col_name:<20} {col_stats['dtype']:<15} {col_stats['mb']:<10.2f} {percentage:<8.2f}%")

        suggestions = cls.suggest_optimization(df)
        if suggestions['suggestions']:
            lines.append("\n优化建议:")
            for i, suggestion in enumerate(suggestions['suggestions'], 1):
                lines.append(f"{i}. {suggestion}")
            lines.append(f"\n估计可节省内存: {suggestions['estimated_savings_mb']:.2f} MB")

        lines.append(f"{'='*60}")
        logger.info("\n".join(lines))


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

