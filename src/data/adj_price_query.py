#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
复权价格查询工具模块
提供便捷的复权数据查询接口，支持前复权/后复权切换
"""

import os
import sys
from datetime import datetime, date
from typing import List, Optional, Union
import pandas as pd
import numpy as np
from loguru import logger

# 添加项目路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)
sys.path.insert(0, project_root)

from database.db_manager import DatabaseManager
from database.models.stock import StockDaily, StockAdjFactor


class AdjPriceQuery:
    """
    复权价格查询工具
    
    功能：
    1. 查询复权价格（前复权/后复权）
    2. 支持实时计算（如果数据库中没有）
    3. 批量查询多只股票的复权数据
    4. 提供统一的查询接口
    """
    
    # 复权类型常量
    ADJ_TYPE_NONE = "none"      # 不复权
    ADJ_TYPE_QFQ = "qfq"        # 前复权
    ADJ_TYPE_HFQ = "hfq"        # 后复权
    
    def __init__(self, db_manager: DatabaseManager):
        """
        初始化
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db_manager = db_manager
        self.session = db_manager.get_session()
    
    def get_price(self, ts_code: str, 
                  start_date: Optional[Union[str, date]] = None,
                  end_date: Optional[Union[str, date]] = None,
                  adj_type: str = ADJ_TYPE_QFQ) -> pd.DataFrame:
        """
        获取单只股票的复权价格
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            adj_type: 复权类型 ('none', 'qfq', 'hfq')
            
        Returns:
            DataFrame包含价格数据
        """
        try:
            # 转换日期格式
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
            
            # 根据复权类型选择查询方式
            if adj_type == self.ADJ_TYPE_NONE:
                return self._get_raw_price(ts_code, start_date, end_date)
            elif adj_type == self.ADJ_TYPE_QFQ:
                return self._get_qfq_price(ts_code, start_date, end_date)
            elif adj_type == self.ADJ_TYPE_HFQ:
                return self._get_hfq_price(ts_code, start_date, end_date)
            else:
                logger.error(f"不支持的复权类型: {adj_type}")
                return None
                
        except Exception as e:
            logger.exception(f"获取 {ts_code} 价格数据失败: {e}")
            return None
    
    def _get_raw_price(self, ts_code: str, start_date: date, end_date: date) -> pd.DataFrame:
        """获取原始价格（不复权）"""
        query = self.session.query(StockDaily).filter_by(ts_code=ts_code)
        
        if start_date:
            query = query.filter(StockDaily.trade_date >= start_date)
        if end_date:
            query = query.filter(StockDaily.trade_date <= end_date)
        
        prices = query.order_by(StockDaily.trade_date).all()
        
        if not prices:
            return None
        
        data = []
        for p in prices:
            data.append({
                'ts_code': p.ts_code,
                'trade_date': p.trade_date,
                'open': p.open,
                'high': p.high,
                'low': p.low,
                'close': p.close,
                'volume': p.vol,
                'amount': p.amount,
            })
        
        return pd.DataFrame(data)
    
    def _get_qfq_price(self, ts_code: str, start_date: date, end_date: date) -> pd.DataFrame:
        """获取前复权价格"""
        # 首先尝试从stock_adj_factor表查询
        df = self._get_from_adj_factor(ts_code, start_date, end_date, adj_type='qfq')
        if df is not None:
            return df
        
        # 如果stock_adj_factor表中没有，尝试从stock_daily表查询
        df = self._get_from_stock_daily(ts_code, start_date, end_date, adj_type='qfq')
        if df is not None:
            return df
        
        # 如果都没有，返回原始价格并记录警告
        logger.warning(f"{ts_code} 没有找到前复权数据，返回原始价格")
        return self._get_raw_price(ts_code, start_date, end_date)
    
    def _get_hfq_price(self, ts_code: str, start_date: date, end_date: date) -> pd.DataFrame:
        """获取后复权价格"""
        # 首先尝试从stock_adj_factor表查询
        df = self._get_from_adj_factor(ts_code, start_date, end_date, adj_type='hfq')
        if df is not None:
            return df
        
        # 如果stock_adj_factor表中没有，尝试从stock_daily表查询
        df = self._get_from_stock_daily(ts_code, start_date, end_date, adj_type='hfq')
        if df is not None:
            return df
        
        # 如果都没有，返回原始价格并记录警告
        logger.warning(f"{ts_code} 没有找到后复权数据，返回原始价格")
        return self._get_raw_price(ts_code, start_date, end_date)
    
    def _get_from_adj_factor(self, ts_code: str, start_date: date, end_date: date, adj_type: str) -> pd.DataFrame:
        """从stock_adj_factor表查询复权数据"""
        query = self.session.query(StockAdjFactor).filter_by(ts_code=ts_code)
        
        if start_date:
            query = query.filter(StockAdjFactor.trade_date >= start_date)
        if end_date:
            query = query.filter(StockAdjFactor.trade_date <= end_date)
        
        factors = query.order_by(StockAdjFactor.trade_date).all()
        
        if not factors:
            return None
        
        data = []
        for f in factors:
            if adj_type == 'qfq':
                data.append({
                    'ts_code': f.ts_code,
                    'trade_date': f.trade_date,
                    'open': f.qfq_open,
                    'high': f.qfq_high,
                    'low': f.qfq_low,
                    'close': f.qfq_close,
                    'factor': f.qfq_factor,
                })
            else:  # hfq
                data.append({
                    'ts_code': f.ts_code,
                    'trade_date': f.trade_date,
                    'open': f.hfq_open,
                    'high': f.hfq_high,
                    'low': f.hfq_low,
                    'close': f.hfq_close,
                    'factor': f.hfq_factor,
                })
        
        return pd.DataFrame(data)
    
    def _get_from_stock_daily(self, ts_code: str, start_date: date, end_date: date, adj_type: str) -> pd.DataFrame:
        """从stock_daily表查询复权数据"""
        query = self.session.query(StockDaily).filter_by(ts_code=ts_code)
        
        if start_date:
            query = query.filter(StockDaily.trade_date >= start_date)
        if end_date:
            query = query.filter(StockDaily.trade_date <= end_date)
        
        prices = query.order_by(StockDaily.trade_date).all()
        
        if not prices:
            return None
        
        # 检查是否有复权数据
        has_adj_data = any(getattr(p, f'{adj_type}_close') is not None for p in prices)
        if not has_adj_data:
            return None
        
        data = []
        for p in prices:
            if adj_type == 'qfq':
                data.append({
                    'ts_code': p.ts_code,
                    'trade_date': p.trade_date,
                    'open': p.qfq_open,
                    'high': p.qfq_high,
                    'low': p.qfq_low,
                    'close': p.qfq_close,
                    'factor': p.qfq_factor,
                })
            else:  # hfq
                data.append({
                    'ts_code': p.ts_code,
                    'trade_date': p.trade_date,
                    'open': p.hfq_open,
                    'high': p.hfq_high,
                    'low': p.hfq_low,
                    'close': p.hfq_close,
                    'factor': p.hfq_factor,
                })
        
        return pd.DataFrame(data)
    
    def get_batch_prices(self, ts_codes: List[str],
                        start_date: Optional[Union[str, date]] = None,
                        end_date: Optional[Union[str, date]] = None,
                        adj_type: str = ADJ_TYPE_QFQ) -> dict:
        """
        批量获取多只股票的复权价格
        
        Args:
            ts_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            adj_type: 复权类型
            
        Returns:
            dict: {ts_code: DataFrame}
        """
        results = {}
        for ts_code in ts_codes:
            df = self.get_price(ts_code, start_date, end_date, adj_type)
            if df is not None:
                results[ts_code] = df
        return results
    
    def get_latest_price(self, ts_code: str, adj_type: str = ADJ_TYPE_QFQ) -> dict:
        """
        获取最新价格
        
        Args:
            ts_code: 股票代码
            adj_type: 复权类型
            
        Returns:
            dict: 最新价格信息
        """
        df = self.get_price(ts_code, adj_type=adj_type)
        if df is not None and not df.empty:
            latest = df.iloc[-1]
            return {
                'ts_code': latest['ts_code'],
                'trade_date': latest['trade_date'],
                'open': latest['open'],
                'high': latest['high'],
                'low': latest['low'],
                'close': latest['close'],
            }
        return None
    
    def get_price_change(self, ts_code: str, 
                        days: int = 20,
                        adj_type: str = ADJ_TYPE_QFQ) -> dict:
        """
        获取价格变动信息
        
        Args:
            ts_code: 股票代码
            days: 计算多少天的变动
            adj_type: 复权类型
            
        Returns:
            dict: 价格变动信息
        """
        df = self.get_price(ts_code, adj_type=adj_type)
        if df is None or len(df) < days:
            return None
        
        latest = df.iloc[-1]
        past = df.iloc[-days]
        
        change = latest['close'] - past['close']
        pct_change = (change / past['close']) * 100 if past['close'] != 0 else 0
        
        return {
            'ts_code': ts_code,
            'latest_close': latest['close'],
            'past_close': past['close'],
            'change': change,
            'pct_change': pct_change,
            'days': days,
        }


if __name__ == "__main__":
    # 测试代码
    from utils.config import Config
    
    config = Config()
    db_manager = DatabaseManager(config)
    db_manager.connect()
    
    query = AdjPriceQuery(db_manager)
    
    # 测试单只股票查询
    ts_code = "000001.SZ"
    
    print(f"\n测试 {ts_code} 价格查询:")
    
    # 前复权
    df_qfq = query.get_price(ts_code, adj_type=AdjPriceQuery.ADJ_TYPE_QFQ)
    if df_qfq is not None:
        print(f"\n前复权价格 (最近5天):")
        print(df_qfq.tail())
    
    # 后复权
    df_hfq = query.get_price(ts_code, adj_type=AdjPriceQuery.ADJ_TYPE_HFQ)
    if df_hfq is not None:
        print(f"\n后复权价格 (最近5天):")
        print(df_hfq.tail())
    
    # 不复权
    df_raw = query.get_price(ts_code, adj_type=AdjPriceQuery.ADJ_TYPE_NONE)
    if df_raw is not None:
        print(f"\n原始价格 (最近5天):")
        print(df_raw.tail())
    
    # 测试最新价格
    latest = query.get_latest_price(ts_code)
    if latest:
        print(f"\n最新价格: {latest}")
    
    # 测试价格变动
    change = query.get_price_change(ts_code, days=20)
    if change:
        print(f"\n20日价格变动: {change}")
    
    db_manager.disconnect()
