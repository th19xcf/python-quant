#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据库管理模块
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from loguru import logger


Base = declarative_base()


class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self, config):
        """
        初始化数据库管理器
        
        Args:
            config: 配置对象，包含数据库相关配置
        """
        self.config = config
        self.engine = None
        self.Session = None
        self.session = None
    
    def connect(self):
        """
        连接数据库
        
        Returns:
            sqlalchemy.orm.Session: 数据库会话对象
        """
        try:
            # 创建数据库连接引擎
            db_config = self.config.database
            connection_url = f"mysql+pymysql://{db_config.username}:{db_config.password}@{db_config.host}:{db_config.port}/{db_config.database}?charset={db_config.charset}"
            
            self.engine = create_engine(
                connection_url,
                pool_size=db_config.pool_size,
                pool_recycle=db_config.pool_recycle,
                pool_pre_ping=True,
                echo=False
            )
            
            # 创建会话工厂
            self.Session = scoped_session(sessionmaker(bind=self.engine))
            
            # 创建会话
            self.session = self.Session()
            
            # 测试连接
            self.engine.connect()
            
            logger.info(f"成功连接到数据库: {db_config.host}:{db_config.port}/{db_config.database}")
            
            # 返回创建的会话对象
            return self.session
            
        except Exception as e:
            logger.exception(f"数据库连接失败: {e}")
            raise
    
    def disconnect(self):
        """
        断开数据库连接
        """
        try:
            if self.session:
                try:
                    self.session.close()
                    self.Session.remove()
                except Exception as session_e:
                    logger.warning(f"关闭数据库会话时发生错误（可能是连接已丢失）: {session_e}")
                    # 即使会话关闭失败，也要继续释放其他资源
                    self.session = None
            
            if self.engine:
                try:
                    self.engine.dispose()
                except Exception as engine_e:
                    logger.warning(f"释放数据库引擎时发生错误: {engine_e}")
            
            logger.info("数据库连接已断开")
            
        except Exception as e:
            logger.exception(f"数据库断开连接失败: {e}")
    
    def create_tables(self):
        """
        创建数据库表
        """
        try:
            # 导入已存在的模型，确保Base.metadata包含所有表定义
            try:
                from src.database.models import stock, index
                logger.info("成功导入stock和index模型")
            except ImportError as import_e:
                logger.warning(f"导入模型时发生错误: {import_e}")
            
            # 创建所有表
            Base.metadata.create_all(self.engine)
            logger.info("数据库表创建成功")
            
        except Exception as e:
            logger.exception(f"数据库表创建失败: {e}")
            raise
    
    def drop_tables(self):
        """
        删除数据库表
        """
        try:
            # 删除所有表
            Base.metadata.drop_all(self.engine)
            logger.info("数据库表删除成功")
            
        except Exception as e:
            logger.exception(f"数据库表删除失败: {e}")
            raise
    
    def get_session(self):
        """
        获取数据库会话
        
        Returns:
            sqlalchemy.orm.Session: 数据库会话对象
        """
        if not self.session:
            self.connect()
        return self.session
