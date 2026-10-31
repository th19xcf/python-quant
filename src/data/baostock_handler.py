#!/usr/bin/env python3

"""
Baostock数据处理器

上游字段串、列映射与行→记录转换收口于 src/data/sources/baostock_schema.py。
"""

from loguru import logger

from src.data.sources.baostock.index import BaostockIndexMixin
from src.data.sources.baostock.minute import BaostockMinuteMixin
from src.data.sources.baostock.session import BaostockSessionMixin
from src.data.sources.baostock.stock import BaostockStockMixin
from src.data.sources.base import DataSourceBase


class BaostockHandler(BaostockSessionMixin, BaostockStockMixin, BaostockIndexMixin, BaostockMinuteMixin, DataSourceBase):
    """
    Baostock数据处理器，负责从Baostock获取数据并存储到数据库

    继承 DataSourceBase 获得统一接口；DataFetcher 通过标准接口
    get_stock_data / get_index_data 调度，download_stock_daily 等
    方法供内部与批量同步使用。

    事务契约：
        1. 单表批量写入可按标的/批次 commit（checkpoint），用于长任务断点续传；
        2. 同一方法内出现跨表写入时，必须收敛到单一 commit 边界
           （优先 db_manager.transaction()），禁止跨表多次提交；
        3. 跨阶段的流水线（如行情同步与复权因子计算）不合并为同一事务，
           避免长事务持锁，一致性由各阶段的原子写入 + 事后校验保证。
    """

    def __init__(self, config, db_manager):
        """
        初始化Baostock数据处理器

        Args:
            config: 配置对象
            db_manager: 数据库管理器实例
        """
        self.config = config
        self.db_manager = db_manager
        self.name = "BaostockHandler"
        self.available = True
        self.session = None
        self.bs_login = False

        # 离线模式支持
        if db_manager:
            try:
                self.session = db_manager.get_session()
            except (ConnectionError, TimeoutError, OSError) as e:
                logger.warning(f"数据库会话获取失败（离线模式下正常）: {e}")
                self.session = None
        else:
            logger.info("BaostockHandler在离线模式下初始化")

        # 根据配置决定是否在初始化时登录Baostock
        if hasattr(self.config.data, 'auto_login_baostock') and self.config.data.auto_login_baostock:
            self._login_baostock()
        else:
            logger.info("跳过自动登录Baostock（未开启或配置不允许）")

        # 从配置文件获取默认配置
        self.default_stock_codes = self.config.data.default_stock_codes
        self.default_index_codes = self.config.data.default_index_codes
        self.supported_frequencies = self.config.data.supported_frequencies
