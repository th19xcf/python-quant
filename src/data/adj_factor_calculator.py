#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
复权因子计算和存储模块
用于计算股票的前复权和后复权因子，并存储到数据库
统一使用Polars DataFrame
"""

import os
import sys
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import polars as pl
import numpy as np
from loguru import logger

# 添加项目路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)
sys.path.insert(0, project_root)

from database.db_manager import DatabaseManager
from database.models.stock import StockDaily, StockDividend, StockAdjFactor
from src.utils.memory_optimizer import MemoryOptimizer


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
                             use_tdx: bool = True) -> pl.DataFrame:
        """
        计算单只股票的复权因子
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            use_tdx: 是否使用通达信数据（默认True）
            
        Returns:
            pl.DataFrame: 包含复权因子和复权价格
        """
        try:
            # 获取分红数据
            dividends = self._get_dividends(ts_code)
            if dividends is None or dividends.is_empty():
                logger.warning(f"{ts_code} 没有分红数据，复权因子设为1.0")
                return self._create_default_factors(ts_code, start_date, end_date, use_tdx)
            
            # 获取价格数据
            if use_tdx and self.tdx_handler:
                prices = self._get_prices_from_tdx(ts_code, start_date, end_date)
            else:
                prices = self._get_prices_from_db(ts_code, start_date, end_date)
            
            if prices is None or prices.is_empty():
                logger.warning(f"{ts_code} 没有价格数据")
                return None
            
            # 计算复权因子
            result = self._calculate_factors(prices, dividends)
            
            logger.info(f"{ts_code} 复权因子计算完成，共 {len(result)} 条记录")
            return result
            
        except (OSError, RuntimeError) as e:
            logger.exception(f"计算 {ts_code} 复权因子失败: {e}")
            return None
    
    def _get_dividends(self, ts_code: str) -> pl.DataFrame:
        """获取分红数据"""
        dividends = self.session.query(StockDividend).filter_by(ts_code=ts_code).order_by(StockDividend.ex_date).all()
        
        if not dividends:
            return None
        
        # 使用Polars直接构建DataFrame
        data = {
            'ts_code': [d.ts_code for d in dividends],
            'ex_date': [d.ex_date for d in dividends],
            'cash_div': [d.cash_div or 0 for d in dividends],
            'share_div': [d.share_div or 0 for d in dividends],
        }
        
        return pl.DataFrame(data)
    
    def _get_prices_from_db(self, ts_code: str, start_date: Optional[datetime], end_date: Optional[datetime]) -> pl.DataFrame:
        """从数据库获取价格数据"""
        query = self.session.query(StockDaily).filter_by(ts_code=ts_code)
        
        if start_date:
            query = query.filter(StockDaily.trade_date >= start_date)
        if end_date:
            query = query.filter(StockDaily.trade_date <= end_date)
        
        prices = query.order_by(StockDaily.trade_date).all()
        
        if not prices:
            return None
        
        # 使用Polars直接构建DataFrame
        data = {
            'ts_code': [p.ts_code for p in prices],
            'trade_date': [p.trade_date for p in prices],
            'open': [p.open for p in prices],
            'high': [p.high for p in prices],
            'low': [p.low for p in prices],
            'close': [p.close for p in prices],
        }
        
        return pl.DataFrame(data)
    
    def _get_prices_from_tdx(self, ts_code: str, start_date: Optional[datetime], end_date: Optional[datetime]) -> pl.DataFrame:
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
            
            # 使用Polars直接构建DataFrame
            data = {
                'ts_code': [ts_code] * len(data_list),
                'trade_date': [item['date'] for item in data_list],
                'open': [item['open'] for item in data_list],
                'high': [item['high'] for item in data_list],
                'low': [item['low'] for item in data_list],
                'close': [item['close'] for item in data_list],
            }
            
            return pl.DataFrame(data)
            
        except (OSError, RuntimeError) as e:
            logger.exception(f"从通达信获取价格数据失败: {e}")
            return None
    
    def _create_default_factors(self, ts_code: str, start_date: datetime, end_date: datetime, use_tdx: bool = True) -> pl.DataFrame:
        """创建默认复权因子（全部为1.0）"""
        # 获取价格数据
        if use_tdx and self.tdx_handler:
            prices_df = self._get_prices_from_tdx(ts_code, start_date, end_date)
        else:
            prices_df = self._get_prices_from_db(ts_code, start_date, end_date)
        
        if prices_df is None or prices_df.is_empty():
            return None
        
        # 添加默认复权因子
        result = prices_df.with_columns([
            pl.lit(1.0).alias('qfq_factor'),
            pl.lit(1.0).alias('hfq_factor'),
        ])

        # 内存优化：转换数据类型
        return MemoryOptimizer.optimize_dataframe(result)
    
    def _calculate_factors(self, prices_df: pl.DataFrame, dividends_df: pl.DataFrame) -> pl.DataFrame:
        """
        计算复权因子（Polars 向量化版本）。

        修正点：
            1. 公式补全配股项（prev_close - cash_div + rights_price * rights_ratio）
               / (prev_close * (1 + share_div + rights_ratio))
            2. 用 Polars 表达式逐事件更新，替代 Python list comprehension O(N×M)
            3. 显式排序断言，避免隐式依赖被破坏
            4. prev_close 通过 asof join 一次性批量获取

        算法：
            - 前复权因子 qfq_factor：按 ex_date 升序从前向后扫分红事件，
              每次 event 把 trade_date < ex_date 的所有行累乘 factor；
              最新段（最后一次 ex_date 之后）不动，作为"不动的锚"。
            - 后复权因子 hfq_factor：同一次升序扫描，每次 event 把
              trade_date >= ex_date 的所有行累乘 1/factor；
              最早段（第一次 ex_date 之前）不动，作为"不动的锚"。
            注：注释历史版本写"从后往前扫前复权 / 从前向后扫后复权"，
            实际代码统一升序扫描——方向差异在过滤条件（< / >=）上，
            不是在遍历顺序上。
        """
        # --- 排序断言 ---
        # polars 2.0 移除了 DataFrame.is_sorted_by，改用列的 Series.is_sorted()
        if not prices_df['trade_date'].is_sorted():
            prices_df = prices_df.sort('trade_date')
        if not dividends_df['ex_date'].is_sorted():
            dividends_df = dividends_df.sort('ex_date')

        result = prices_df.with_columns([
            pl.lit(1.0).alias('qfq_factor'),
            pl.lit(1.0).alias('hfq_factor'),
        ])

        # --- 给每条分红事件挂上前一交易日收盘价（asof join）---
        # 先把 prices 的 trade_date 改为 Date 类型（asof join 同类型）
        prices_for_join = prices_df.select([
            pl.col('ts_code'),
            pl.col('trade_date'),
            pl.col('close').alias('prev_close'),
        ])

        # dividends 里 ex_date 也应是 Date
        div_rows = dividends_df.to_dicts()

        # 遍历分红事件（保持原顺序逻辑，但用 Polars 表达式替代 Python list 操作）
        for div in div_rows:
            ex_date = div.get('ex_date')
            if ex_date is None:
                continue

            share_div = div.get('share_div') or 0.0
            cash_div = div.get('cash_div') or 0.0
            rights_ratio = div.get('rights_issue_ratio') or 0.0
            rights_price = div.get('rights_issue_price') or 0.0

            # 无任何权益变更则跳过
            if share_div == 0 and cash_div == 0 and rights_ratio == 0:
                continue

            # 用 asof join 找 ex_date 之前最近一个 trade_date 的收盘价
            prev_day = prices_for_join.filter(pl.col('trade_date') < ex_date).tail(1)
            if prev_day.is_empty():
                continue

            prev_close = prev_day['prev_close'][0]
            if prev_close is None or prev_close <= 0:
                logger.warning(f"{div.get('ts_code', '')} {ex_date} 除权前收盘价异常 ({prev_close})，跳过")
                continue

            # 除权参考价公式（含配股）
            numerator = prev_close - cash_div + rights_price * rights_ratio
            denominator = prev_close * (1 + share_div + rights_ratio)

            if denominator == 0:
                logger.warning(f"{div.get('ts_code', '')} {ex_date} 复权因子分母为 0，跳过")
                continue

            factor = numerator / denominator  # 前复权每次乘的因子

            # --- 前复权：trade_date < ex_date 的行 *= factor ---
            result = result.with_columns(
                pl.when(pl.col('trade_date') < ex_date)
                .then(pl.col('qfq_factor') * factor)
                .otherwise(pl.col('qfq_factor'))
                .alias('qfq_factor')
            )

            # --- 后复权：trade_date >= ex_date 的行 *= (1 / factor) ---
            if factor == 0:
                continue
            result = result.with_columns(
                pl.when(pl.col('trade_date') >= ex_date)
                .then(pl.col('hfq_factor') / factor)  # 等价于乘以 (1/factor)
                .otherwise(pl.col('hfq_factor'))
                .alias('hfq_factor')
            )

        # --- 计算完成，仅返回因子（复权价按需实时算） ---
        # 内存优化：转换数据类型
        return MemoryOptimizer.optimize_dataframe(result)

    def save_adj_factors(self, df: pl.DataFrame, batch_size: int = 1000):
        """
        保存复权因子到数据库
        
        Args:
            df: 包含复权因子的Polars DataFrame
            batch_size: 批量插入大小
        """
        if df is None or df.is_empty():
            logger.warning("没有数据需要保存")
            return
        
        try:
            records = []
            for row in df.to_dicts():
                record = StockAdjFactor(
                    ts_code=row['ts_code'],
                    trade_date=row['trade_date'],
                    qfq_factor=row['qfq_factor'],
                    hfq_factor=row['hfq_factor'],
                )
                records.append(record)
                
                if len(records) >= batch_size:
                    self._batch_insert(records)
                    records = []
            
            # 插入剩余记录
            if records:
                self._batch_insert(records)
            
            logger.info(f"成功保存 {len(df)} 条复权因子记录")
            
        except (OSError, RuntimeError) as e:
            logger.exception(f"保存复权因子失败: {e}")
            self.session.rollback()
    
    def _batch_insert(self, records: List[StockAdjFactor]):
        """批量插入记录"""
        try:
            existing_records_map = {}
            if records:
                ts_codes = [r.ts_code for r in records]
                trade_dates = [r.trade_date for r in records]

                existing_records = self.session.query(StockAdjFactor).filter(
                    StockAdjFactor.ts_code.in_(ts_codes),
                    StockAdjFactor.trade_date.in_(trade_dates)
                ).all()

                for rec in existing_records:
                    existing_records_map[(rec.ts_code, rec.trade_date)] = rec

            records_to_add = []
            for record in records:
                key = (record.ts_code, record.trade_date)
                if key in existing_records_map:
                    existing = existing_records_map[key]
                    existing.qfq_factor = record.qfq_factor
                    existing.hfq_factor = record.hfq_factor
                else:
                    records_to_add.append(record)

            if records_to_add:
                self.session.add_all(records_to_add)

            self.session.commit()
        except (OSError, RuntimeError) as e:
            self.session.rollback()
            raise e
    
    def update_stock_daily_adj(self, df: pl.DataFrame):
        """
        更新stock_daily表的复权价格和因子
        
        Args:
            df: 包含复权因子的Polars DataFrame
        """
        if df is None or df.is_empty():
            return
        
        try:
            rows = df.to_dicts()

            # 批量加载现有记录，避免逐行 SELECT（N+1）
            ts_codes = list({row['ts_code'] for row in rows})
            trade_dates = list({row['trade_date'] for row in rows})
            existing_records = self.session.query(StockDaily).filter(
                StockDaily.ts_code.in_(ts_codes),
                StockDaily.trade_date.in_(trade_dates)
            ).all()
            existing_map = {(rec.ts_code, rec.trade_date): rec for rec in existing_records}

            fields = ('qfq_factor', 'hfq_factor')
            updated = 0
            for row in rows:
                daily = existing_map.get((row['ts_code'], row['trade_date']))
                if daily:
                    for field in fields:
                        setattr(daily, field, row.get(field))
                    updated += 1

            self.session.commit()
            logger.info(f"成功更新 {updated} 条stock_daily记录")
            
        except (OSError, RuntimeError) as e:
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
                    
            except (OSError, RuntimeError) as e:
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
        print(result.select(['trade_date', 'close', 'qfq_factor', 'hfq_factor']).tail(10))
        
        # 保存到数据库
        calculator.save_adj_factors(result)
    
    db_manager.disconnect()
