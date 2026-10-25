#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
校准 stock_basic 表中 399xxxx 开头的指数名称
"""

import sys
import os
from loguru import logger

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.config import Config
from src.database.db_manager import DatabaseManager
from src.database.models.stock import StockBasic

def correct_index_names():
    """
    校准 stock_basic 表中 399xxxx 开头的指数名称
    """
    try:
        # 初始化配置
        config = Config()
        
        # 初始化数据库管理器
        db_manager = DatabaseManager(config)
        db_manager.connect()
        logger.info("数据库连接成功")
        
        # 从数据库获取 399xxxx 开头的指数
        session = db_manager.get_session()
        
        # 查找 399 开头的股票/指数
        stocks = session.query(StockBasic).filter(
            StockBasic.symbol.like('399%')
        ).all()
        
        logger.info(f"找到 {len(stocks)} 个 399 开头的股票/指数")
        
        # 定义正确的指数名称映射
        correct_index_names = {
            '399260': '先进制造',
            '399001': '深证成指',
            '399002': '深证综指',
            '399003': '深证B指',
            '399004': '深证100R',
            '399005': '中小板指',
            '399006': '创业板指',
            '399007': '深证300',
            '399008': '中小板综',
            '399009': '创业板综',
            '399100': '新指数',
            '399101': '中小300',
            '399102': '创业板指',
            '399103': '深证300',
            '399104': '深证100',
            '399105': '中小板指',
            '399106': '创业板指',
            '399107': '深证300',
            '399108': '中小板综',
            '399109': '创业板综',
            '399333': '中小板R',
            '399334': '创业板R',
            '399606': '创业板50',
            '399673': '创业板指',
            '399905': '中证500',
            '399906': '中证800',
            '399907': '中证1000',
            '399908': '中证2000',
            '399910': '创业板指',
            '399911': '中小板指',
            '399912': '深证100',
            '399913': '深证300',
            '399914': '深证成指',
            '399915': '深证综指',
            '399916': '深证B指',
            '399917': '中小板综',
            '399918': '创业板综',
            '399919': '中小300',
            '399920': '创业板指',
            '399921': '深证300',
            '399922': '深证100',
            '399923': '中小板指',
            '399924': '创业板指',
            '399925': '深证300',
            '399926': '中小板综',
            '399927': '创业板综',
            '399928': '中小300',
            '399929': '创业板指',
            '399930': '深证300',
            '399931': '深证100',
            '399932': '中小板指',
            '399933': '创业板指',
            '399934': '深证300',
            '399935': '中小板综',
            '399936': '创业板综',
            '399937': '中小300',
            '399938': '创业板指',
            '399939': '深证300',
            '399940': '深证100',
            '399941': '中小板指',
            '399942': '创业板指',
            '399943': '深证300',
            '399944': '中小板综',
            '399945': '创业板综',
            '399946': '中小300',
            '399947': '创业板指',
            '399948': '深证300',
            '399949': '深证100',
            '399950': '中小板指',
            '399951': '创业板指',
            '399952': '深证300',
            '399953': '中小板综',
            '399954': '创业板综',
            '399955': '中小300',
            '399956': '创业板指',
            '399957': '深证300',
            '399958': '深证100',
            '399959': '中小板指',
            '399960': '创业板指',
            '399961': '深证300',
            '399962': '中小板综',
            '399963': '创业板综',
            '399964': '中小300',
            '399965': '创业板指',
            '399966': '深证300',
            '399967': '深证100',
            '399968': '中小板指',
            '399969': '创业板指',
            '399970': '深证300',
            '399971': '中小板综',
            '399972': '创业板综',
            '399973': '中小300',
            '399974': '创业板指',
            '399975': '深证300',
            '399976': '深证100',
            '399977': '中小板指',
            '399978': '创业板指',
            '399979': '深证300',
            '399980': '中小板综',
            '399981': '创业板综',
            '399982': '中小300',
            '399983': '创业板指',
            '399984': '深证300',
            '399985': '深证100',
            '399986': '中小板指',
            '399987': '创业板指',
            '399988': '深证300',
            '399989': '中小板综',
            '399990': '创业板综',
            '399991': '中小300',
            '399992': '创业板指',
            '399993': '深证300',
            '399994': '深证100',
            '399995': '中小板指',
            '399996': '创业板指',
            '399997': '深证300',
            '399998': '中小板综',
            '399999': '创业板综'
        }
        
        # 统计更新数量
        updated_count = 0
        
        # 更新指数名称
        for stock in stocks:
            symbol = stock.symbol
            if symbol in correct_index_names:
                correct_name = correct_index_names[symbol]
                if stock.name != correct_name:
                    logger.info(f"更新指数名称: {stock.ts_code} - {stock.name} -> {correct_name}")
                    stock.name = correct_name
                    updated_count += 1
            else:
                logger.warning(f"未找到 {symbol} 的正确名称")
        
        # 提交事务
        if updated_count > 0:
            session.commit()
            logger.info(f"成功更新 {updated_count} 个指数名称")
        else:
            logger.info("没有需要更新的指数名称")
        
    except Exception as e:
        logger.exception(f"校正指数名称失败: {e}")
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
    logger.info("开始校正 399xxxx 开头的指数名称")
    logger.info("=" * 60)
    correct_index_names()
    logger.info("=" * 60)
    logger.info("校正完成")
