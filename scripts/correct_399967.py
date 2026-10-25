#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
校正 399967 指数名称为中证军工
"""

import sys
import os
from loguru import logger

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.config import Config
from src.database.db_manager import DatabaseManager
from src.database.models.stock import StockBasic

def correct_399967():
    """
    校正 399967 指数名称为中证军工
    """
    try:
        # 初始化配置
        config = Config()
        
        # 初始化数据库管理器
        db_manager = DatabaseManager(config)
        db_manager.connect()
        logger.info("数据库连接成功")
        
        # 从数据库获取 399967 指数
        session = db_manager.get_session()
        
        # 查找 399967 指数
        stock = session.query(StockBasic).filter(
            StockBasic.symbol == '399967'
        ).first()
        
        if stock:
            logger.info(f"找到 399967 指数: {stock.ts_code} - {stock.name}")
            
            # 更新指数名称为中证军工
            if stock.name != '中证军工':
                logger.info(f"更新指数名称: {stock.name} -> 中证军工")
                stock.name = '中证军工'
                session.commit()
                logger.info("成功更新 399967 指数名称为中证军工")
            else:
                logger.info("399967 指数名称已经是中证军工，无需更新")
        else:
            logger.warning("未找到 399967 指数")
            # 如果不存在，创建新的指数记录
            new_stock = StockBasic(
                ts_code='399967.SZ',
                symbol='399967',
                name='中证军工',
                area='深圳',
                industry='指数',
                market='创业板',
                list_date='2020-01-01',  # 假设上市日期
                delist_date=None,
                status='L'
            )
            session.add(new_stock)
            session.commit()
            logger.info("成功添加 399967 中证军工指数到数据库")
        
    except Exception as e:
        logger.exception(f"校正 399967 指数失败: {e}")
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
    logger.info("开始校正 399967 指数名称")
    logger.info("=" * 60)
    correct_399967()
    logger.info("=" * 60)
    logger.info("校正完成")
