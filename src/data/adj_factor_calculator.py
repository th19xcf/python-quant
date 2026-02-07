#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
复权因子计算和存储模块
用于计算股票的前复权和后复权因子，并存储到数据库
"""

import os
import sys
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import pandas as pd
import numpy as np
from loguru import logger

# 添加项目路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)
sys.path.insert(0, project_root)

from database.db_manager import DatabaseManager
from database.models.stock import StockDaily, StockDividend, StockAdjFactor


class AdjFactorCalculator:
    """
    复权因子计算器
    
    功能：
    1. 计算前复权因子和后复权因子
    2. 计算复权价格
    3. 存储到数据库
    4. 支持增量更新
    5. 支持通达信数据源
    """
    
    def __init__(self, db_manager: DatabaseManager, tdx_handler=None):
        """
        初始化
        
        Args:
            db_manager: 数据库管理器实例
            tdx_handler: 通达信处理器实例（可选）
        """
        self.db_manager = db_manager
        self.session = db_manager.get_session()
        self.tdx_handler = tdx_handler
        
    def calculate_adj_factors(self, ts_code: str, 
                             start_date: Optional[datetime] = None,
                             end_date: Optional[datetime] = None,
                             use_tdx: bool = True) -> pd.DataFrame:
        """
        计算单只股票的复权因子
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            use_tdx: 是否使用通达信数据（默认True）
            
        Returns:
            DataFrame包含复权因子和复权价格
        """
        try:
            # 获取分红数据
            dividends = self._get_dividends(ts_code)
            if dividends is None or dividends.empty:
                logger.warning(f"{ts_code} 没有分红数据，复权因子设为1.0")
                return self._create_default_factors(ts_code, start_date, end_date, use_tdx)
            
            # 获取价格数据
            if use_tdx and self.tdx_handler:
                prices = self._get_prices_from_tdx(ts_code, start_date, end_date)
            else:
                prices = self._get_prices_from_db(ts_code, start_date, end_date)
            
            if prices is None or prices.empty:
                logger.warning(f"{ts_code} 没有价格数据")
                return None
            
            # 计算复权因子
            result = self._calculate_factors(prices, dividends)
            
            logger.info(f"{ts_code} 复权因子计算完成，共 {len(result)} 条记录")
            return result
            
        except Exception as e:
            logger.exception(f"计算 {ts_code} 复权因子失败: {e}")
            return None
    
    def _get_dividends(self, ts_code: str) -> pd.DataFrame:
        """获取分红数据"""
        dividends = self.session.query(StockDividend).filter_by(ts_code=ts_code).order_by(StockDividend.ex_date).all()
        
        if not dividends:
            return None
        
        data = []
        for d in dividends:
            data.append({
                'ts_code': d.ts_code,
                'ex_date': d.ex_date,
                'cash_div': d.cash_div or 0,
                'share_div': d.share_div or 0,
            })
        
        return pd.DataFrame(data)
    
    def _get_prices_from_db(self, ts_code: str, start_date: Optional[datetime], end_date: Optional[datetime]) -> pd.DataFrame:
        """从数据库获取价格数据"""
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
            })
        
        return pd.DataFrame(data)
    
    def _get_prices_from_tdx(self, ts_code: str, start_date: Optional[datetime], end_date: Optional[datetime]) -> pd.DataFrame:
        """从通达信获取价格数据"""
        if not self.tdx_handler:
            logger.warning("通达信处理器未初始化")
            return None
        
        try:
            # 转换日期格式（通达信使用 YYYY-MM-DD 格式）
            start_str = start_date.strftime('%Y-%m-%d') if start_date else '1990-01-01'
            end_str = end_date.strftime('%Y-%m-%d') if end_date else datetime.now().strftime('%Y-%m-%d')
            
            # 获取股票代码（不带后缀）
            symbol = ts_code.split('.')[0]
            
            # 从通达信获取数据
            data_list = self.tdx_handler.get_kline_data(symbol, start_str, end_str)
            
            if not data_list:
                logger.warning(f"{ts_code} 从通达信获取数据失败")
                return None
            
            # 转换为DataFrame
            data = []
            for item in data_list:
                data.append({
                    'ts_code': ts_code,
                    'trade_date': item['date'],
                    'open': item['open'],
                    'high': item['high'],
                    'low': item['low'],
                    'close': item['close'],
                })
            
            return pd.DataFrame(data)
            
        except Exception as e:
            logger.exception(f"从通达信获取价格数据失败: {e}")
            return None
    
    def _create_default_factors(self, ts_code: str, start_date: datetime, end_date: datetime, use_tdx: bool = True) -> pd.DataFrame:
        """创建默认复权因子（全部为1.0）"""
        # 获取价格数据
        if use_tdx and self.tdx_handler:
            prices_df = self._get_prices_from_tdx(ts_code, start_date, end_date)
        else:
            prices_df = self._get_prices_from_db(ts_code, start_date, end_date)
        
        if prices_df is None or prices_df.empty:
            return None
        
        # 添加默认复权因子
        prices_df['qfq_factor'] = 1.0
        prices_df['hfq_factor'] = 1.0
        prices_df['qfq_open'] = prices_df['open']
        prices_df['qfq_high'] = prices_df['high']
        prices_df['qfq_low'] = prices_df['low']
        prices_df['qfq_close'] = prices_df['close']
        prices_df['hfq_open'] = prices_df['open']
        prices_df['hfq_high'] = prices_df['high']
        prices_df['hfq_low'] = prices_df['low']
        prices_df['hfq_close'] = prices_df['close']
        
        return prices_df
    
    def _calculate_factors(self, prices_df: pd.DataFrame, dividends_df: pd.DataFrame) -> pd.DataFrame:
        """
        计算复权因子
        
        算法：
        1. 前复权因子：从后向前累乘，考虑送转股和现金分红
        2. 后复权因子：从前向后累乘，考虑送转股和现金分红
        """
        result = prices_df.copy()
        result['qfq_factor'] = 1.0
        result['hfq_factor'] = 1.0
        
        # 计算前复权因子（从后向前）
        for idx in range(len(dividends_df) - 1, -1, -1):
            div = dividends_df.iloc[idx]
            ex_date = div['ex_date']
            share_div = div['share_div']
            cash_div = div['cash_div']
            
            if pd.isna(ex_date):
                continue
            
            # 找到除权除息日的前一天收盘价
            prev_day_mask = result['trade_date'] < ex_date
            if not prev_day_mask.any():
                continue
            
            prev_day_close = result.loc[prev_day_mask, 'close'].iloc[-1] if not result.loc[prev_day_mask].empty else None
            if prev_day_close is None:
                continue
            
            # 计算复权因子
            if share_div > 0 or cash_div > 0:
                # 兼容送转股+现金分红的综合复权因子
                if prev_day_close <= cash_div:
                    logger.warning(f"{div['ts_code'] if 'ts_code' in div else ''} {ex_date} 现金分红异常，无法计算前复权因子")
                    continue
                factor = (prev_day_close - cash_div) / (prev_day_close * (1 + share_div))
                
                mask = result['trade_date'] < ex_date
                result.loc[mask, 'qfq_factor'] *= factor
        
        # 计算后复权因子（从前向后）
        for idx in range(len(dividends_df)):
            div = dividends_df.iloc[idx]
            ex_date = div['ex_date']
            share_div = div['share_div']
            cash_div = div['cash_div']
            
            if pd.isna(ex_date):
                continue
            
            # 找到除权除息日的前一天收盘价
            prev_day_mask = result['trade_date'] < ex_date
            if not prev_day_mask.any():
                continue
            
            prev_day_close = result.loc[prev_day_mask, 'close'].iloc[-1] if not result.loc[prev_day_mask].empty else None
            if prev_day_close is None:
                continue
            
            # 计算复权因子
            if share_div > 0 or cash_div > 0:
                # 后复权因子为前复权因子的倒数（应用于除权除息日及之后）
                if prev_day_close <= cash_div:
                    logger.warning(f"{div['ts_code'] if 'ts_code' in div else ''} {ex_date} 现金分红异常，无法计算后复权因子")
                    continue
                qfq_factor = (prev_day_close - cash_div) / (prev_day_close * (1 + share_div))
                if qfq_factor == 0:
                    logger.warning(f"{div['ts_code'] if 'ts_code' in div else ''} {ex_date} 前复权因子为0，跳过后复权计算")
                    continue
                factor = 1 / qfq_factor
                
                mask = result['trade_date'] >= ex_date
                result.loc[mask, 'hfq_factor'] *= factor

        # 后复权因子归一化：以最新交易日因子为1
        try:
            latest_hfq_factor = result['hfq_factor'].iloc[-1]
            if latest_hfq_factor and latest_hfq_factor != 0:
                result['hfq_factor'] = result['hfq_factor'] / latest_hfq_factor
        except Exception as e:
            logger.warning(f"后复权因子归一化失败: {e}")
        
        # 计算复权价格
        result['qfq_open'] = result['open'] * result['qfq_factor']
        result['qfq_high'] = result['high'] * result['qfq_factor']
        result['qfq_low'] = result['low'] * result['qfq_factor']
        result['qfq_close'] = result['close'] * result['qfq_factor']
        
        result['hfq_open'] = result['open'] * result['hfq_factor']
        result['hfq_high'] = result['high'] * result['hfq_factor']
        result['hfq_low'] = result['low'] * result['hfq_factor']
        result['hfq_close'] = result['close'] * result['hfq_factor']
        
        return result
    
    def save_adj_factors(self, df: pd.DataFrame, batch_size: int = 1000):
        """
        保存复权因子到数据库
        
        Args:
            df: 包含复权因子的DataFrame
            batch_size: 批量插入大小
        """
        if df is None or df.empty:
            logger.warning("没有数据需要保存")
            return
        
        try:
            records = []
            for _, row in df.iterrows():
                record = StockAdjFactor(
                    ts_code=row['ts_code'],
                    trade_date=row['trade_date'],
                    qfq_factor=row['qfq_factor'],
                    hfq_factor=row['hfq_factor'],
                    qfq_open=row.get('qfq_open'),
                    qfq_high=row.get('qfq_high'),
                    qfq_low=row.get('qfq_low'),
                    qfq_close=row.get('qfq_close'),
                    hfq_open=row.get('hfq_open'),
                    hfq_high=row.get('hfq_high'),
                    hfq_low=row.get('hfq_low'),
                    hfq_close=row.get('hfq_close'),
                )
                records.append(record)
                
                if len(records) >= batch_size:
                    self._batch_insert(records)
                    records = []
            
            # 插入剩余记录
            if records:
                self._batch_insert(records)
            
            logger.info(f"成功保存 {len(df)} 条复权因子记录")
            
        except Exception as e:
            logger.exception(f"保存复权因子失败: {e}")
            self.session.rollback()
    
    def _batch_insert(self, records: List[StockAdjFactor]):
        """批量插入记录"""
        try:
            for record in records:
                # 检查是否已存在
                existing = self.session.query(StockAdjFactor).filter_by(
                    ts_code=record.ts_code,
                    trade_date=record.trade_date
                ).first()
                
                if existing:
                    # 更新现有记录
                    existing.qfq_factor = record.qfq_factor
                    existing.hfq_factor = record.hfq_factor
                    existing.qfq_open = record.qfq_open
                    existing.qfq_high = record.qfq_high
                    existing.qfq_low = record.qfq_low
                    existing.qfq_close = record.qfq_close
                    existing.hfq_open = record.hfq_open
                    existing.hfq_high = record.hfq_high
                    existing.hfq_low = record.hfq_low
                    existing.hfq_close = record.hfq_close
                else:
                    # 插入新记录
                    self.session.add(record)
            
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e
    
    def update_stock_daily_adj(self, df: pd.DataFrame):
        """
        更新stock_daily表的复权价格和因子
        
        Args:
            df: 包含复权因子的DataFrame
        """
        if df is None or df.empty:
            return
        
        try:
            for _, row in df.iterrows():
                daily = self.session.query(StockDaily).filter_by(
                    ts_code=row['ts_code'],
                    trade_date=row['trade_date']
                ).first()
                
                if daily:
                    daily.qfq_factor = row['qfq_factor']
                    daily.hfq_factor = row['hfq_factor']
                    daily.qfq_open = row.get('qfq_open')
                    daily.qfq_high = row.get('qfq_high')
                    daily.qfq_low = row.get('qfq_low')
                    daily.qfq_close = row.get('qfq_close')
                    daily.hfq_open = row.get('hfq_open')
                    daily.hfq_high = row.get('hfq_high')
                    daily.hfq_low = row.get('hfq_low')
                    daily.hfq_close = row.get('hfq_close')
            
            self.session.commit()
            logger.info(f"成功更新 {len(df)} 条stock_daily记录")
            
        except Exception as e:
            logger.exception(f"更新stock_daily失败: {e}")
            self.session.rollback()
    
    def batch_calculate_and_save(self, ts_codes: List[str], 
                                  start_date: Optional[datetime] = None,
                                  end_date: Optional[datetime] = None,
                                  update_daily: bool = True):
        """
        批量计算并保存复权因子
        
        Args:
            ts_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            update_daily: 是否同时更新stock_daily表
        """
        total = len(ts_codes)
        success = 0
        failed = 0
        
        for i, ts_code in enumerate(ts_codes):
            try:
                logger.info(f"[{i+1}/{total}] 正在处理 {ts_code}")
                
                # 计算复权因子
                df = self.calculate_adj_factors(ts_code, start_date, end_date)
                if df is not None:
                    # 保存到stock_adj_factor表
                    self.save_adj_factors(df)
                    
                    # 可选：更新stock_daily表
                    if update_daily:
                        self.update_stock_daily_adj(df)
                    
                    success += 1
                else:
                    failed += 1
                    
            except Exception as e:
                logger.error(f"处理 {ts_code} 失败: {e}")
                failed += 1
        
        logger.info(f"批量处理完成：成功 {success}，失败 {failed}，总计 {total}")


if __name__ == "__main__":
    # 测试代码
    from utils.config import Config
    
    config = Config()
    db_manager = DatabaseManager(config)
    db_manager.connect()
    
    calculator = AdjFactorCalculator(db_manager)
    
    # 测试单只股票
    ts_code = "000001.SZ"
    result = calculator.calculate_adj_factors(ts_code)
    
    if result is not None:
        print(f"\n{ts_code} 复权因子计算结果:")
        print(result[['trade_date', 'close', 'qfq_factor', 'hfq_factor', 'qfq_close', 'hfq_close']].tail(10))
        
        # 保存到数据库
        calculator.save_adj_factors(result)
    
    db_manager.disconnect()
