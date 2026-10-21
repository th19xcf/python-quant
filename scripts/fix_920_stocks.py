#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复数据库中920开头股票的市场代码
将920开头的股票从深市（.SZ）改为北市（.BJ）
"""

import os
import sys
import logging
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.db_manager import DatabaseManager
from src.database.models.stock import StockBasic
from src.utils.config import get_config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def fix_920_stocks():
    """
    修复数据库中920开头股票的市场代码
    """
    try:
        # 从项目配置读取数据库连接信息（.env 中的 DATABASE__* 配置项）
        config = get_config()

        # 初始化数据库管理器
        db_manager = DatabaseManager(config)
        
        # 连接数据库
        session = db_manager.get_session()
        
        # 查询920开头的股票记录
        stocks_to_fix = session.query(StockBasic).filter(
            StockBasic.symbol.like('920%'),
            StockBasic.ts_code.like('%.SZ')
        ).all()
        
        logger.info(f"找到 {len(stocks_to_fix)} 只920开头但市场代码为.SZ的股票")
        
        for stock in stocks_to_fix:
            try:
                # 生成新的ts_code
                new_ts_code = f"{stock.symbol}.BJ"
                
                # 检查新的ts_code是否已经存在
                existing_stock = session.query(StockBasic).filter_by(ts_code=new_ts_code).first()
                
                if existing_stock:
                    # 如果新的ts_code已经存在，删除旧记录
                    logger.info(f"删除重复记录: {stock.ts_code} (已存在 {new_ts_code})")
                    session.delete(stock)
                else:
                    # 否则，更新ts_code
                    old_ts_code = stock.ts_code
                    stock.ts_code = new_ts_code
                    logger.info(f"更新股票市场代码: {old_ts_code} -> {new_ts_code}")
                
            except Exception as e:
                logger.exception(f"处理股票 {stock.ts_code} 失败: {e}")
                continue
        
        # 提交事务
        session.commit()
        logger.info("修复完成")
        
    except Exception as e:
        logger.exception(f"修复920开头股票失败: {e}")
        if 'session' in locals():
            session.rollback()
    finally:
        if 'db_manager' in locals():
            db_manager.disconnect()

if __name__ == '__main__':
    logger.info("开始修复920开头股票的市场代码")
    fix_920_stocks()
    logger.info("修复完成")
