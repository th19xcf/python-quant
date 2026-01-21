import polars as pl
import numpy as np

class DataProcessor:
    """
    数据处理工具类，包含数据转换和处理函数
    """
    
    @staticmethod
    def ensure_dataframe_type(data):
        """
        确保数据是Polars DataFrame类型
        
        Args:
            data: 输入数据，可以是Polars DataFrame或其他类型
        
        Returns:
            Polars DataFrame
        """
        if isinstance(data, pl.DataFrame):
            return data
        # 如果是其他类型，尝试转换为Polars DataFrame
        try:
            return pl.DataFrame(data)
        except Exception as e:
            raise ValueError(f"无法将数据转换为Polars DataFrame: {e}")
    
    @staticmethod
    def get_display_data(df, bar_count=100):
        """
        获取显示数量的数据
        
        Args:
            df: 原始数据
            bar_count: 显示的柱体数量
        
        Returns:
            显示数量的数据
        """
        if hasattr(df, 'tail'):
            return df.tail(bar_count)
        return df
    
    @staticmethod
    def calculate_x_axis(df):
        """
        计算x轴坐标
        
        Args:
            df: 数据DataFrame
        
        Returns:
            x轴坐标数组
        """
        return np.arange(len(df))
    
    @staticmethod
    def ensure_numeric_columns(df, columns):
        """
        确保指定列是数值类型
        
        Args:
            df: 数据DataFrame
            columns: 需要确保为数值类型的列名列表
        
        Returns:
            更新后的DataFrame
        """
        for col in columns:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))
        return df
    
    @staticmethod
    def fill_missing_values(df, columns=None, method='forward'):
        """
        填充缺失值
        
        Args:
            df: 数据DataFrame
            columns: 需要填充的列名列表，如果为None则填充所有列
            method: 填充方法，'forward'或'backward'
        
        Returns:
            更新后的DataFrame
        """
        if columns is None:
            columns = df.columns
        
        for col in columns:
            if col in df.columns:
                if method == 'forward':
                    df = df.with_columns(pl.col(col).forward_fill())
                elif method == 'backward':
                    df = df.with_columns(pl.col(col).backward_fill())
        
        return df