#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
检查数据库中是否存在 399260 指数
"""

import sys
import os
from loguru import logger

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.config import Config
from src.database.db_manager import DatabaseManager
from src.database.models.stock import StockBasic

def check_399260():
    """
    检查数据库中是否存在 399260 指数
    """
    try:
        # 初始化配置
        config = Config()
        
        # 初始化数据库管理器
        db_manager = DatabaseManager(config)
        db_manager.connect()
        logger.info("数据库连接成功")
        
        # 从数据库获取 399260 指数
        session = db_manager.get_session()
        
        # 查找 399260 指数
        stock = session.query(StockBasic).filter(
            StockBasic.symbol == '399260'
        ).first()
        
        if stock:
            logger.info(f"找到 399260 指数: {stock.ts_code} - {stock.name}")
        else:
            logger.warning("未找到 399260 指数")
        
        # 查找所有 399 开头的指数，检查是否有类似的记录
        stocks = session.query(StockBasic).filter(
            StockBasic.symbol.like('399%')
        ).all()
        
        logger.info(f"找到 {len(stocks)} 个 399 开头的股票/指数")
        
        # 打印前20个，看看是否有相关记录
        logger.info("前20个 399 开头的股票/指数:")
        for i, s in enumerate(stocks[:20]):
            logger.info(f"{i+1}. {s.ts_code} - {s.name}")
        
    except Exception as e:
        logger.exception(f"检查 399260 指数失败: {e}")
    finally:
        if 'db_manager' in locals():
            try:
                db_manager.disconnect()
                logger.info("数据库连接已关闭")
            except Exception as close_e:
                logger.warning(f"关闭数据库连接失败: {close_e}")

if __name__ == "__main__":
    logger.info("开始检查 399260 指数")
    logger.info("=" * 60)
    check_399260()
    logger.info("=" * 60)
    logger.info("检查完成")
