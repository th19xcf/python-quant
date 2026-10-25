#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
手动添加 399260 先进制造指数到数据库
"""

import sys
import os
from loguru import logger

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.config import Config
from src.database.db_manager import DatabaseManager
from src.database.models.stock import StockBasic

def add_399260():
    """
    手动添加 399260 先进制造指数到数据库
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
        
        # 检查是否已存在
        existing_stock = session.query(StockBasic).filter(
            StockBasic.symbol == '399260'
        ).first()
        
        if existing_stock:
            logger.info(f"399260 指数已存在: {existing_stock.ts_code} - {existing_stock.name}")
        else:
            # 创建新的指数记录
            new_stock = StockBasic(
                ts_code='399260.SZ',
                symbol='399260',
                name='先进制造',
                area='深圳',
                industry='指数',
                market='创业板',
                list_date='2020-01-01',  # 假设上市日期
                delist_date=None,
                status='L'
            )
            session.add(new_stock)
            session.commit()
            logger.info("成功添加 399260 先进制造指数到数据库")
        
    except Exception as e:
        logger.exception(f"添加 399260 指数失败: {e}")
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

if __name__ == "__main__":
    logger.info("开始添加 399260 先进制造指数")
    logger.info("=" * 60)
    add_399260()
    logger.info("=" * 60)
    logger.info("添加完成")
