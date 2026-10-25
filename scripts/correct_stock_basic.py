#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
校正stock_basic表，以通达信数据源为准
"""

import sys
import os
from loguru import logger

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.config import Config
from src.database.db_manager import DatabaseManager
from src.data.tdx_handler import TdxHandler
from src.database.models.stock import StockBasic

def correct_stock_basic():
    """
    校正stock_basic表，以通达信数据源为准
    """
    try:
        # 初始化配置
        config = Config()
        
        # 初始化数据库管理器
        db_manager = DatabaseManager(config)
        db_manager.connect()
        logger.info("数据库连接成功")
        
        # 初始化通达信处理器
        tdx_handler = TdxHandler(config, db_manager)
        logger.info("通达信处理器初始化成功")
        
        # 从通达信获取股票列表
        tdx_stock_codes = tdx_handler.get_stock_list()
        tdx_stock_set = set(tdx_stock_codes)
        logger.info(f"从通达信获取到 {len(tdx_stock_codes)} 只股票")
        
        # 从数据库获取股票列表
        session = db_manager.get_session()
        db_stocks = session.query(StockBasic).all()
        db_stock_codes = [stock.ts_code for stock in db_stocks]
        db_stock_set = set(db_stock_codes)
        logger.info(f"从数据库获取到 {len(db_stock_codes)} 只股票")
        
        # 找出需要删除的股票（数据库中有但通达信中没有）
        stocks_to_delete = db_stock_set - tdx_stock_set
        logger.info(f"需要删除 {len(stocks_to_delete)} 只股票")
        
        # 删除多余的股票记录
        deleted_count = 0
        for ts_code in stocks_to_delete:
            try:
                stock = session.query(StockBasic).filter_by(ts_code=ts_code).first()
                if stock:
                    session.delete(stock)
                    deleted_count += 1
                    logger.info(f"删除股票: {ts_code}")
            except Exception as e:
                logger.warning(f"删除股票 {ts_code} 失败: {e}")
        
        # 提交事务
        if deleted_count > 0:
            session.commit()
            logger.info(f"成功删除 {deleted_count} 只股票")
        else:
            logger.info("没有需要删除的股票")
        
        # 验证校正结果
        after_db_stocks = session.query(StockBasic).all()
        after_db_stock_codes = [stock.ts_code for stock in after_db_stocks]
        after_db_stock_set = set(after_db_stock_codes)
        logger.info(f"校正后数据库中有 {len(after_db_stock_codes)} 只股票")
        
        # 检查是否还有差异
        remaining_diff = after_db_stock_set - tdx_stock_set
        if remaining_diff:
            logger.warning(f"仍然存在差异，剩余 {len(remaining_diff)} 只股票")
            logger.warning(f"差异股票: {list(remaining_diff)[:10]}...")
        else:
            logger.info("校正完成，数据库股票与通达信数据源一致")
        
    except Exception as e:
        logger.exception(f"校正stock_basic失败: {e}")
        if 'session' in locals():
            try:
                session.rollback()
            except Exception as rollback_e:
                logger.warning(f"回滚事务失败: {rollback_e}")
    finally:
        if 'db_manager' in locals():
            try:
                db_manager.disconnect()
                logger.info("数据库连接已关闭")
            except Exception as close_e:
                logger.warning(f"关闭数据库连接失败: {close_e}")
        if 'tdx_handler' in locals():
            try:
                tdx_handler.close()
                logger.info("通达信处理器已关闭")
            except Exception as close_e:
                logger.warning(f"关闭通达信处理器失败: {close_e}")

if __name__ == "__main__":
    logger.info("开始校正stock_basic表")
    logger.info("=" * 60)
    correct_stock_basic()
    logger.info("=" * 60)
    logger.info("校正完成")
