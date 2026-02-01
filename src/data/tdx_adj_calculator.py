#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
通达信数据复权计算器
结合通达信原始价格数据和stock_adj_factor表计算复权价格
"""

import os
import sys
from datetime import datetime, date
from typing import Optional, Union
import pandas as pd
import polars as pl
from loguru import logger

# 添加项目路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)
sys.path.insert(0, project_root)

from database.db_manager import DatabaseManager
from database.models.stock import StockAdjFactor


class TdxAdjCalculator:
    """
    通达信数据复权计算器
    
    功能：
    1. 读取通达信原始价格数据
    2. 从数据库获取复权因子
    3. 实时计算复权价格
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        初始化
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db_manager = db_manager
        self.session = db_manager.get_session()
    
    def get_adj_factors(self, ts_code: str, 
                       start_date: Optional[date] = None,
                       end_date: Optional[date] = None) -> pd.DataFrame:
        """
        获取复权因子
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            DataFrame包含复权因子
        """
        try:
            query = self.session.query(StockAdjFactor).filter_by(ts_code=ts_code)
            
            if start_date:
                query = query.filter(StockAdjFactor.trade_date >= start_date)
            if end_date:
                query = query.filter(StockAdjFactor.trade_date <= end_date)
            
            factors = query.order_by(StockAdjFactor.trade_date).all()
            
            if not factors:
                logger.warning(f"{ts_code} 没有找到复权因子数据")
                return None
            
            data = []
            for f in factors:
                data.append({
                    'trade_date': f.trade_date,
                    'qfq_factor': f.qfq_factor,
                    'hfq_factor': f.hfq_factor,
                })
            
            return pd.DataFrame(data)
            
        except Exception as e:
            logger.exception(f"获取复权因子失败: {e}")
            return None
    
    def calculate_adj_prices(self, tdx_df: pl.DataFrame, ts_code: str,
                            adj_type: str = 'qfq') -> pl.DataFrame:
        """
        计算复权价格
        
        Args:
            tdx_df: 通达信原始数据DataFrame
            ts_code: 股票代码
            adj_type: 复权类型 ('qfq'前复权, 'hfq'后复权)
            
        Returns:
            包含复权价格的DataFrame
        """
        try:
            # 获取日期范围
            dates = tdx_df['date'].to_list()
            start_date = min(dates)
            end_date = max(dates)
            
            # 获取复权因子
            factors_df = self.get_adj_factors(ts_code, start_date, end_date)
            
            if factors_df is None:
                logger.warning(f"{ts_code} 没有复权因子，返回原始价格")
                # 添加默认因子1.0
                tdx_df = tdx_df.with_columns([
                    pl.lit(1.0).alias(f'{adj_type}_factor'),
                    pl.col('open').alias(f'{adj_type}_open'),
                    pl.col('high').alias(f'{adj_type}_high'),
                    pl.col('low').alias(f'{adj_type}_low'),
                    pl.col('close').alias(f'{adj_type}_close'),
                ])
                return tdx_df
            
            # 将复权因子转换为polars DataFrame
            factors_pl = pl.from_pandas(factors_df)
            
            # 合并数据
            result_df = tdx_df.join(factors_pl, left_on='date', right_on='trade_date', how='left')
            
            # 填充缺失的复权因子为1.0
            factor_col = f'{adj_type}_factor'
            result_df = result_df.with_columns([
                pl.col(factor_col).fill_null(1.0)
            ])
            
            # 计算复权价格
            result_df = result_df.with_columns([
                (pl.col('open') * pl.col(factor_col)).alias(f'{adj_type}_open'),
                (pl.col('high') * pl.col(factor_col)).alias(f'{adj_type}_high'),
                (pl.col('low') * pl.col(factor_col)).alias(f'{adj_type}_low'),
                (pl.col('close') * pl.col(factor_col)).alias(f'{adj_type}_close'),
            ])
            
            logger.info(f"{ts_code} 复权价格计算完成，共 {len(result_df)} 条记录")
            return result_df
            
        except Exception as e:
            logger.exception(f"计算复权价格失败: {e}")
            return tdx_df
    
    def get_price_with_adj(self, tdx_handler, ts_code: str,
                          start_date: Optional[date] = None,
                          end_date: Optional[date] = None,
                          adj_type: str = 'qfq') -> pl.DataFrame:
        """
        获取带复权的价格数据（一站式接口）
        
        Args:
            tdx_handler: TdxHandler实例
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            adj_type: 复权类型
            
        Returns:
            包含复权价格的DataFrame
        """
        try:
            # 1. 从通达信获取原始价格
            symbol = ts_code.split('.')[0]
            file_path = tdx_handler.tdx_data_path / f"{symbol}.day"
            
            if not file_path.exists():
                logger.warning(f"通达信数据文件不存在: {file_path}")
                return None
            
            tdx_df = tdx_handler.parse_day_file(file_path)
            
            if tdx_df is None or tdx_df.is_empty():
                logger.warning(f"{ts_code} 没有通达信数据")
                return None
            
            # 2. 计算复权价格
            result_df = self.calculate_adj_prices(tdx_df, ts_code, adj_type)
            
            return result_df
            
        except Exception as e:
            logger.exception(f"获取复权价格失败: {e}")
            return None


if __name__ == "__main__":
    # 测试代码
    from utils.config import Config
    from data.tdx_handler import TdxHandler
    
    config = Config()
    db_manager = DatabaseManager(config)
    db_manager.connect()
    
    try:
        # 创建计算器
        calculator = TdxAdjCalculator(db_manager)
        
        # 创建通达信handler
        tdx_handler = TdxHandler(config, db_manager)
        
        # 测试股票
        ts_code = "000001.SZ"
        
        logger.info(f"\n测试 {ts_code} 复权价格计算:")
        
        # 获取前复权价格
        df_qfq = calculator.get_price_with_adj(tdx_handler, ts_code, adj_type='qfq')
        if df_qfq is not None:
            logger.info(f"\n前复权价格 (最近5天):")
            print(df_qfq.tail(5))
        
        # 获取后复权价格
        df_hfq = calculator.get_price_with_adj(tdx_handler, ts_code, adj_type='hfq')
        if df_hfq is not None:
            logger.info(f"\n后复权价格 (最近5天):")
            print(df_hfq.tail(5))
        
    except Exception as e:
        logger.exception(f"测试失败: {e}")
    finally:
        db_manager.disconnect()
