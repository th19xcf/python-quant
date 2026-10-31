#!/usr/bin/env python3

"""
AkShare数据处理器

对 akshare 上游的全部依赖（端点名、字段名、别名）集中在
src/data/sources/akshare_schema.py；本模块只做编排与入库，
不再 import akshare、不再出现上游列名。
"""

from loguru import logger

from src.data.sources.akshare.dividend import AkShareDividendMixin
from src.data.sources.akshare.etf import AkShareEtfMixin
from src.data.sources.akshare.index import AkShareIndexMixin
from src.data.sources.akshare.stock import AkShareStockMixin
from src.data.sources.base import DataSourceBase


class AkShareHandler(AkShareStockMixin, AkShareIndexMixin, AkShareDividendMixin, AkShareEtfMixin, DataSourceBase):
    """
    AkShare数据处理器，负责从AkShare获取数据并存储到数据库

    继承 DataSourceBase 获得统一接口（is_available / 15+ update_* 默认 stubs）。
    实际数据获取由 DataFetcher._get_data_from_sources 按 handler 名硬编码
    调用对应方法（get_stock_data / get_index_data）。

    事务契约：
        1. 单表批量写入可按标的/批次 commit（checkpoint），用于长任务断点续传；
        2. 同一方法内出现跨表写入时，必须收敛到单一 commit 边界
           （优先 db_manager.transaction()），禁止跨表多次提交；
        3. 跨阶段的流水线（如行情同步与复权因子计算）不合并为同一事务，
           避免长事务持锁，一致性由各阶段的原子写入 + 事后校验保证。
    """

    def __init__(self, config, db_manager):
        """
        初始化AkShare数据处理器

        Args:
            config: 配置对象
            db_manager: 数据库管理器实例
        """
        self.config = config
        self.db_manager = db_manager
        self.name = "AkShareHandler"
        self.available = True
        self._factor_warning_logged = False
        self.session = None

        # 离线模式支持
        if db_manager:
            try:
                self.session = db_manager.get_session()
            except (ConnectionError, TimeoutError, OSError) as e:
                logger.warning(f"数据库会话获取失败（离线模式下正常）: {e}")
                self.session = None
        else:
            logger.info("AkShareHandler在离线模式下初始化")

    # ------------------------------------------------------------------
    # 标准 DataSource 接口（统一列名/单位/复权口径，供 DataFetcher 调用）
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_date_str(date_str: str) -> str:
        """将 YYYYMMDD 或 YYYY-MM-DD 统一为 YYYYMMDD（akshare 接口要求）"""
        return str(date_str).replace('-', '')
