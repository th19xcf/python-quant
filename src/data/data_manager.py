#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据获取与管理模块

注意：此模块已被重构，建议使用 DataService 代替
"""

from loguru import logger
from typing import List, Dict, Any, Optional, Union
import polars as pl
from src.api.data_api import IDataProvider, IDataProcessor
from src.utils.event_bus import EventBus
from src.utils.exceptions import (
    DataSourceConnectionError,
    DataSourceNotAvailableError,
    DataSourceConfigError,
    DataValidationError,
    DataNotFoundError,
    DataSaveError,
    QuantException
)
from src.utils.exception_handler import handle_exception_with_retry, handle_error_gracefully
from src.data.data_cache import global_data_cache
from src.data.managers import DataFetcher, DataUpdater, DataProcessor
from src.data.services.data_service import DataService
from src.utils.memory_optimizer import MemoryOptimizer

# 异步数据管理器（延迟导入）
AsyncDataManager = None


class DataManager(IDataProvider, IDataProcessor):
    """
    数据管理器，负责统一管理各种数据源的获取、清洗和存储
    实现了IDataProvider和IDataProcessor接口
    
    注意：此类已被重构为使用DataService，建议直接使用DataService
    """
    
    def __init__(self, config, db_manager, plugin_manager=None):
        """
        初始化数据管理器
        
        Args:
            config: 配置对象
            db_manager: 数据库管理器实例
            plugin_manager: 插件管理器实例
        """
        self.config = config
        self.db_manager = db_manager
        self.plugin_manager = plugin_manager
        
        # 使用新的DataService
        self.data_service = DataService(config, db_manager, plugin_manager)
        
        # 为了保持向后兼容性，保留旧的组件
        self.data_fetcher = DataFetcher(config, db_manager)
        self.data_updater = DataUpdater(config, db_manager)
        self.data_processor = DataProcessor(config)
        
        # 插件数据源映射
        self.plugin_datasources = {}
        
        # 异步数据管理器
        self.async_data_manager = None
        
        self._init_handlers()
        self._init_plugin_datasources()
        self._init_async_manager()
        
        logger.info("DataManager 已初始化，建议使用 DataService 代替")
    
    def _init_handlers(self):
        """
+        初始化各个数据源处理器

        注意：为了避免重复初始化，handler的初始化现在由DataService统一管理。
        这里只进行必要的配置和注册操作。
        """
        from src.data.services.data_provider import DataProvider
        from src.data.services.data_updater import DataUpdaterService

        if isinstance(self.data_service.data_provider, DataProvider):
            provider = self.data_service.data_provider
            fetcher_sources = provider.data_fetcher.get_sources()
            for source in fetcher_sources:
                if source not in self.data_fetcher.sources:
                    self.data_fetcher.register_source(source)

        if isinstance(self.data_service.data_updater, DataUpdaterService):
            updater = self.data_service.data_updater
            updater_sources = getattr(updater.data_updater, 'sources', [])
            for source in updater_sources:
                if source not in self.data_updater.sources:
                    self.data_updater.register_source(source)

        logger.info("数据处理器初始化完成（复用DataService中的handler）")

        self._init_async_manager()
    
    def _init_plugin_datasources(self):
        """
        初始化插件数据源
        """
        if not self.plugin_manager:
            return
        
        # 获取所有可用的数据源插件
        datasource_plugins = self.plugin_manager.get_available_datasource_plugins()
        
        for plugin_name, plugin in datasource_plugins.items():
            self.plugin_datasources[plugin_name] = plugin
            # 注册到数据获取和更新模块
            self.data_fetcher.register_source(plugin)
            self.data_updater.register_source(plugin)
            logger.info(f"已注册插件数据源: {plugin_name}")
    
    def _init_async_manager(self):
        """
        初始化异步数据管理器
        """
        try:
            global AsyncDataManager
            if AsyncDataManager is None:
                from src.data.async_data_manager import AsyncDataManager
            
            self.async_data_manager = AsyncDataManager(self)
            logger.info("异步数据管理器初始化成功")
        except ImportError as e:
            logger.info(f"异步数据管理器初始化失败（可选功能）: {e}")
            self.async_data_manager = None
        except (OSError, RuntimeError) as e:
            logger.warning(f"异步数据管理器初始化失败: {e}")
            self.async_data_manager = None
    
    def _publish_data_updated_event(self, data_type, ts_code, status="success", message=""):
        """
        发布数据更新事件
        
        Args:
            data_type: 数据类型，如'stock', 'index', 'macro', 'news'
            ts_code: 股票代码或指数代码
            status: 更新状态，success或error
            message: 附加消息
        """
        from src.utils.event_bus import publish
        publish(
            'data_updated' if status == 'success' else 'data_error',
            data_type=data_type,
            ts_code=ts_code,
            message=message
        )
    
    def _update_data(self, data_type, handler, method_name, event_type, identifier='all', **kwargs):
        """
        通用数据更新方法
        
        Args:
            data_type: 数据类型名称（用于日志）
            handler: 数据处理器
            method_name: 要调用的方法名
            event_type: 事件类型
            identifier: 标识符，默认为'all'
            **kwargs: 传递给更新方法的参数
            
        Raises:
            DataSourceNotAvailableError: 数据源不可用
            DataValidationError: 数据验证失败
            DataSaveError: 数据保存失败
        """
        if not handler:
            error_msg = f"{data_type}处理器未初始化"
            logger.warning(error_msg)
            self._publish_data_updated_event(event_type, identifier, status='error', message=error_msg)
            raise DataSourceNotAvailableError("unknown", error_msg)
        
        try:
            method = getattr(handler, method_name)
            method(**kwargs)
            logger.info(f"{data_type}更新完成")
            
            # 发布数据更新事件
            self._publish_data_updated_event(event_type, identifier)
            
        except DataSourceConnectionError as e:
            logger.error(f"{data_type}更新失败 - 连接错误: {e.message}")
            self._publish_data_updated_event(event_type, identifier, status='error', message=e.message)
            raise
        except DataValidationError as e:
            logger.error(f"{data_type}更新失败 - 数据验证错误: {e.message}")
            self._publish_data_updated_event(event_type, identifier, status='error', message=e.message)
            raise
        except DataSaveError as e:
            logger.error(f"{data_type}更新失败 - 保存错误: {e.message}")
            self._publish_data_updated_event(event_type, identifier, status='error', message=e.message)
            raise
        except (OSError, RuntimeError, ValueError) as e:
            error_msg = f"{data_type}更新失败: {str(e)}"
            logger.exception(error_msg)
            self._publish_data_updated_event(event_type, identifier, status='error', message=error_msg)
            raise DataSaveError(error_msg) from e
    
    def update_stock_basic(self):
        """
        更新股票基本信息
        """
        return self.data_service.update_stock_basic()
    
    def update_stock_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新股票日线数据
        
        Args:
            ts_codes: 股票代码列表，None表示更新所有股票
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
        """
        return self.data_service.update_stock_daily(ts_codes, start_date, end_date)
    
    def update_fund_basic(self):
        """
        更新基金基本信息
        """
        return self.data_service.update_fund_basic()
    
    def update_fund_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新基金日线数据
        
        Args:
            ts_codes: 基金代码列表，None表示更新所有基金
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
        """
        return self.data_service.update_fund_daily(ts_codes, start_date, end_date)
    
    def update_closed_fund_basic(self):
        """
        更新封闭式基金基本信息
        """
        return self.data_service.update_closed_fund_basic()
    
    def update_closed_fund_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新封闭式基金日线数据
        
        Args:
            ts_codes: 封闭式基金代码列表，None表示更新所有封闭式基金
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
        """
        return self.data_service.update_closed_fund_daily(ts_codes, start_date, end_date)
    
    def update_index_basic(self):
        """
        更新指数基本信息
        """
        return self.data_service.update_index_basic()
    
    def update_index_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新指数日线数据
        
        Args:
            ts_codes: 指数代码列表，None表示更新所有指数
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
        """
        return self.data_service.update_index_daily(ts_codes, start_date, end_date)
    
    def update_macro_data(self, indicators: List[str] = None):
        """
        更新宏观经济数据
        
        Args:
            indicators: 宏观经济指标列表，None表示更新所有指标
        """
        return self.data_service.update_macro_data(indicators)
    
    def update_news_data(self, sources: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新新闻数据
        
        Args:
            sources: 新闻来源列表，None表示更新所有来源
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
        """
        return self.data_service.update_news_data(sources, start_date, end_date)

    def update_stock_dividend(self, ts_codes: List[str] = None):
        """
        更新股票分红配股数据

        Args:
            ts_codes: 股票代码列表，None表示更新所有股票
            
        Raises:
            DataSourceConnectionError: 数据源连接失败
            DataValidationError: 数据验证失败
            DataSaveError: 数据保存失败
        """
        return self.data_service.update_stock_dividend(ts_codes)

    def get_stock_dividend(self, ts_code: str) -> pl.DataFrame:
        """
        获取股票分红配股数据

        Args:
            ts_code: 股票代码

        Returns:
            pl.DataFrame: 分红配股数据
        """
        # 参数验证
        if not ts_code:
            logger.warning("股票代码不能为空")
            return pl.DataFrame()
        
        # 检查数据库连接
        if not self.db_manager:
            logger.warning("数据库管理器未初始化，无法获取分红配股数据")
            return pl.DataFrame()
        
        if not self.db_manager.is_connected():
            logger.warning("数据库未连接，无法获取分红配股数据")
            return pl.DataFrame()

        try:
            from src.database.models.stock import StockDividend
        except ImportError as e:
            logger.error(f"无法导入StockDividend模型: {e}")
            return pl.DataFrame()

        session = None
        try:
            session = self.db_manager.get_session()
            if not session:
                logger.warning("无法获取数据库会话")
                return pl.DataFrame()

            # 查询分红配股数据
            try:
                dividends = session.query(StockDividend).filter_by(ts_code=ts_code).all()
            except (OSError, RuntimeError) as query_e:
                logger.error(f"查询分红配股数据失败: {query_e}")
                return pl.DataFrame()

            if not dividends:
                return pl.DataFrame()

            # 转换为Polars DataFrame
            try:
                data = [{
                    'ts_code': d.ts_code,
                    'symbol': d.symbol,
                    'name': d.name,
                    'dividend_year': d.dividend_year,
                    'report_date': d.report_date,
                    'record_date': d.record_date,
                    'ex_date': d.ex_date,
                    'pay_date': d.pay_date,
                    'cash_div': d.cash_div,
                    'share_div': d.share_div,
                    'total_div': d.total_div,
                    'rights_issue_price': d.rights_issue_price,
                    'rights_issue_ratio': d.rights_issue_ratio
                } for d in dividends]

                df = pl.DataFrame(data)
                # 内存优化：转换数据类型
                return MemoryOptimizer.optimize_dataframe(df)
            except (ValueError, TypeError) as convert_e:
                logger.error(f"转换分红配股数据失败: {convert_e}")
                return pl.DataFrame()

        except (OSError, RuntimeError) as e:
            logger.exception(f"获取股票分红配股数据失败: {e}")
            return pl.DataFrame()
        finally:
            if session:
                try:
                    session.close()
                except (OSError, RuntimeError):
                    pass
    
    @handle_exception_with_retry(max_retries=3, retry_delay=1.0)
    def get_stock_data(self, stock_code: str, start_date: str, end_date: str, frequency: str = '1d', adjustment_type: str = 'qfq') -> pl.DataFrame:
        """
        获取股票历史数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线），支持1d/1w/1m（日/周/月线）
            adjustment_type: 复权类型，qfq=前复权, hfq=后复权, none=不复权
        
        Returns:
            pl.DataFrame: 股票历史数据
        """
        # 使用新的DataService
        return self.data_service.get_stock_data(stock_code, start_date, end_date, frequency, adjustment_type)
    

    
    @handle_exception_with_retry(max_retries=3, retry_delay=1.0)
    def get_index_data(self, index_code: str, start_date: str, end_date: str, frequency: str = '1d') -> pl.DataFrame:
        """
        获取指数历史数据
        
        Args:
            index_code: 指数代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线）
        
        Returns:
            pl.DataFrame: 指数历史数据
        """
        # 使用新的DataService
        return self.data_service.get_index_data(index_code, start_date, end_date, frequency)
    
    async def get_stock_data_async(self, stock_code: str, start_date: str, end_date: str, frequency: str = '1d', adjustment_type: str = 'qfq') -> pl.DataFrame:
        """
        异步获取股票历史数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线），支持1d/1w/1m（日/周/月线）
            adjustment_type: 复权类型，qfq=前复权, hfq=后复权, none=不复权
        
        Returns:
            pl.DataFrame: 股票历史数据
        """
        if self.async_data_manager:
            return await self.async_data_manager.get_stock_data_async(stock_code, start_date, end_date, frequency, adjustment_type)
        else:
            # 回退到同步方法
            return self.get_stock_data(stock_code, start_date, end_date, frequency, adjustment_type)
    
    async def get_index_data_async(self, index_code: str, start_date: str, end_date: str, frequency: str = '1d') -> pl.DataFrame:
        """
        异步获取指数历史数据
        
        Args:
            index_code: 指数代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线）
        
        Returns:
            pl.DataFrame: 指数历史数据
        """
        if self.async_data_manager:
            return await self.async_data_manager.get_index_data_async(index_code, start_date, end_date, frequency)
        else:
            # 回退到同步方法
            return self.get_index_data(index_code, start_date, end_date, frequency)
    
    async def get_multiple_stocks_data_async(self, stock_codes: List[str], start_date: str, end_date: str, frequency: str = '1d', adjustment_type: str = 'qfq', batch_size: int = 10, timeout: int = 30) -> Dict[str, pl.DataFrame]:
        """
        异步并行获取多只股票数据
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线）
            adjustment_type: 复权类型，qfq=前复权, hfq=后复权, none=不复权
            batch_size: 批量处理大小，默认10
            timeout: 每个任务的超时时间（秒），默认30
        
        Returns:
            Dict[str, pl.DataFrame]: 股票代码到数据的映射
        """
        if self.async_data_manager:
            return await self.async_data_manager.get_multiple_stocks_data_async(stock_codes, start_date, end_date, frequency, adjustment_type, batch_size, timeout)
        else:
            # 回退到同步方法（串行获取）
            result = {}
            total_stocks = len(stock_codes)
            processed_stocks = 0
            start_time = time.time()
            
            logger.info(f"开始串行获取{total_stocks}只股票数据")
            
            for stock_code in stock_codes:
                result[stock_code] = self.get_stock_data(stock_code, start_date, end_date, frequency, adjustment_type)
                processed_stocks += 1
                
                # 打印进度
                if processed_stocks % 5 == 0 or processed_stocks == total_stocks:
                    progress = (processed_stocks / total_stocks) * 100
                    elapsed_time = time.time() - start_time
                    remaining_time = (elapsed_time / processed_stocks) * (total_stocks - processed_stocks)
                    logger.info(f"进度: {processed_stocks}/{total_stocks} ({progress:.1f}%)，耗时: {elapsed_time:.2f}秒，预计剩余: {remaining_time:.2f}秒")
            
            total_time = time.time() - start_time
            logger.info(f"串行获取{total_stocks}只股票数据完成，总耗时: {total_time:.2f}秒")
            
            return result
    
    def get_stock_basic(self, exchange: Optional[str] = None) -> pl.DataFrame:
        """
        获取股票基本信息
        
        Args:
            exchange: 交易所，可选值：'sh'（上海）、'sz'（深圳）、'bj'（北京）
        
        Returns:
            pl.DataFrame: 股票基本信息
        """
        session = None
        try:
            if not self.db_manager:
                logger.warning("数据库连接不可用，无法获取股票基本信息")
                return pl.DataFrame()
            
            from src.database.models.stock import StockBasic
            
            session = self.db_manager.get_session()
            if not session:
                return pl.DataFrame()
            
            try:
                # 检查stock_basic表是否有数据
                query = session.query(StockBasic)
                
                # 根据交易所筛选
                if exchange:
                    if exchange == 'sh':
                        query = query.filter(StockBasic.ts_code.like('%.SH'))
                    elif exchange == 'sz':
                        query = query.filter(StockBasic.ts_code.like('%.SZ'))
                    elif exchange == 'bj':
                        query = query.filter(StockBasic.ts_code.like('%.BJ'))
                
                stock_basics = query.all()
                
                # 如果没有数据，插入默认股票信息
                if not stock_basics:
                    logger.info("stock_basic表为空，插入默认股票信息")
                    default_stocks = [
                        {"ts_code": "600000.SH", "name": "浦发银行"},
                        {"ts_code": "000001.SZ", "name": "平安银行"},
                        {"ts_code": "300001.SZ", "name": "特锐德"}
                    ]
                    
                    for stock_info in default_stocks:
                        # 检查是否已存在
                        existing_stock = session.query(StockBasic).filter_by(ts_code=stock_info["ts_code"]).first()
                        if not existing_stock:
                            new_stock = StockBasic(
                                ts_code=stock_info["ts_code"],
                                name=stock_info["name"]
                            )
                            session.add(new_stock)
                    
                    # 提交事务
                    session.commit()
                    # 重新查询数据
                    stock_basics = query.all()
                
                # 构建DataFrame
                if stock_basics:
                    data = {
                        'ts_code': [stock.ts_code for stock in stock_basics],
                        'name': [stock.name for stock in stock_basics]
                    }
                    df = pl.DataFrame(data)
                    # 内存优化：字符串列不需要优化
                    return df
                else:
                    return pl.DataFrame()
            except (OSError, RuntimeError) as query_e:
                # 如果查询失败，可能是表不存在，返回空DataFrame
                logger.warning(f"股票基本信息查询失败: {query_e}")
                return pl.DataFrame()

        except (OSError, RuntimeError) as e:
            logger.exception(f"获取股票基本信息失败: {e}")
            return pl.DataFrame()
        finally:
            # 确保会话被关闭
            if session and self.db_manager:
                try:
                    self.db_manager._cleanup_session()
                except Exception as cleanup_e:
                    logger.debug(f"清理会话时出错: {cleanup_e}")
    
    def get_index_basic(self, exchange: Optional[str] = None) -> pl.DataFrame:
        """
        获取指数基本信息
        
        Args:
            exchange: 交易所，可选值：'sh'（上海）、'sz'（深圳）
        
        Returns:
            pl.DataFrame: 指数基本信息
        """
        session = None
        try:
            if not self.db_manager:
                logger.warning("数据库连接不可用，无法获取指数基本信息")
                return pl.DataFrame()
            
            from src.database.models.index import IndexBasic
            
            session = self.db_manager.get_session()
            if not session:
                return pl.DataFrame()
            
            try:
                # 检查index_basic表是否有数据
                query = session.query(IndexBasic)
                
                # 根据交易所筛选
                if exchange:
                    if exchange == 'sh':
                        query = query.filter(IndexBasic.ts_code.like('%.SH'))
                    elif exchange == 'sz':
                        query = query.filter(IndexBasic.ts_code.like('%.SZ'))
                
                index_basics = query.all()
                
                # 构建DataFrame
                if index_basics:
                    data = {
                        'ts_code': [index.ts_code for index in index_basics],
                        'name': [index.name for index in index_basics]
                    }
                    df = pl.DataFrame(data)
                    # 内存优化：字符串列不需要优化
                    return df
                else:
                    return pl.DataFrame()
            except (OSError, RuntimeError) as query_e:
                # 如果查询失败，可能是表不存在，返回空DataFrame
                logger.warning(f"指数基本信息查询失败: {query_e}")
                return pl.DataFrame()

        except (OSError, RuntimeError) as e:
            logger.exception(f"获取指数基本信息失败: {e}")
            return pl.DataFrame()
        finally:
            # 确保会话被关闭
            if session and self.db_manager:
                try:
                    self.db_manager._cleanup_session()
                except Exception as cleanup_e:
                    logger.debug(f"清理会话时出错: {cleanup_e}")
    
    def get_fund_basic(self, exchange: Optional[str] = None) -> pl.DataFrame:
        """
        获取基金基本信息
        
        Args:
            exchange: 交易所，可选值：'sh'（上海）、'sz'（深圳）
        
        Returns:
            pl.DataFrame: 基金基本信息
        """
        session = None
        try:
            if not self.db_manager:
                logger.warning("数据库连接不可用，无法获取基金基本信息")
                return pl.DataFrame()
            
            from src.database.models.fund import FundBasic
            
            session = self.db_manager.get_session()
            if not session:
                return pl.DataFrame()
            
            try:
                # 检查fund_basic表是否有数据
                query = session.query(FundBasic)
                
                # 根据交易所筛选
                if exchange:
                    if exchange == 'sh':
                        query = query.filter(FundBasic.ts_code.like('%.SH'))
                    elif exchange == 'sz':
                        query = query.filter(FundBasic.ts_code.like('%.SZ'))
                
                fund_basics = query.all()
                
                # 构建DataFrame
                if fund_basics:
                    data = {
                        'ts_code': [fund.ts_code for fund in fund_basics],
                        'name': [fund.name for fund in fund_basics]
                    }
                    df = pl.DataFrame(data)
                    # 内存优化：字符串列不需要优化
                    return df
                else:
                    return pl.DataFrame()
            except Exception as query_e:
                # 如果查询失败，可能是表不存在，返回空DataFrame
                logger.warning(f"基金基本信息查询失败: {query_e}")
                return pl.DataFrame()

        except Exception as e:
            logger.exception(f"获取基金基本信息失败: {e}")
            return pl.DataFrame()
        finally:
            # 确保会话被关闭
            if session and self.db_manager:
                try:
                    self.db_manager._cleanup_session()
                except Exception as cleanup_e:
                    logger.debug(f"清理会话时出错: {cleanup_e}")
    
    def get_closed_fund_basic(self, exchange: Optional[str] = None) -> pl.DataFrame:
        """
        获取封闭式基金基本信息
        
        Args:
            exchange: 交易所，可选值：'sh'（上海）、'sz'（深圳）
        
        Returns:
            pl.DataFrame: 封闭式基金基本信息
        """
        session = None
        try:
            if not self.db_manager:
                logger.warning("数据库连接不可用，无法获取封闭式基金基本信息")
                return pl.DataFrame()
            
            from src.database.models.fund import ClosedFundBasic
            
            session = self.db_manager.get_session()
            if not session:
                return pl.DataFrame()
            
            try:
                # 检查closed_fund_basic表是否有数据
                query = session.query(ClosedFundBasic)
                
                # 根据交易所筛选
                if exchange:
                    if exchange == 'sh':
                        query = query.filter(ClosedFundBasic.ts_code.like('%.SH'))
                    elif exchange == 'sz':
                        query = query.filter(ClosedFundBasic.ts_code.like('%.SZ'))
                
                closed_fund_basics = query.all()
                
                # 如果没有数据，插入默认封闭式基金信息
                if not closed_fund_basics:
                    logger.info("closed_fund_basic表为空，插入默认封闭式基金信息")
                    default_closed_funds = [
                        {"ts_code": "500018.SH", "name": "基金兴和"},
                        {"ts_code": "500025.SH", "name": "基金汉盛"},
                        {"ts_code": "500038.SH", "name": "基金通乾"}
                    ]
                    
                    for fund_info in default_closed_funds:
                        # 检查是否已存在
                        existing_fund = session.query(ClosedFundBasic).filter_by(ts_code=fund_info["ts_code"]).first()
                        if not existing_fund:
                            new_fund = ClosedFundBasic(
                                ts_code=fund_info["ts_code"],
                                name=fund_info["name"]
                            )
                            session.add(new_fund)
                    
                    # 提交事务
                    session.commit()
                    # 重新查询数据
                    closed_fund_basics = query.all()
                
                # 构建DataFrame
                if closed_fund_basics:
                    data = {
                        'ts_code': [fund.ts_code for fund in closed_fund_basics],
                        'name': [fund.name for fund in closed_fund_basics]
                    }
                    df = pl.DataFrame(data)
                    # 内存优化：字符串列不需要优化
                    return df
                else:
                    return pl.DataFrame()
            except Exception as query_e:
                # 如果查询失败，可能是表不存在，返回空DataFrame
                logger.warning(f"封闭式基金基本信息查询失败: {query_e}")
                return pl.DataFrame()

        except Exception as e:
            logger.exception(f"获取封闭式基金基本信息失败: {e}")
            return pl.DataFrame()
        finally:
            # 确保会话被关闭
            if session and self.db_manager:
                try:
                    self.db_manager._cleanup_session()
                except Exception as cleanup_e:
                    logger.debug(f"清理会话时出错: {cleanup_e}")
    
    def preprocess_data(self, data: Union[pl.DataFrame, pl.LazyFrame]) -> Union[pl.DataFrame, pl.LazyFrame]:
        """
        预处理数据
        
        Args:
            data: 原始数据（Polars DataFrame或LazyFrame）
        
        Returns:
            Union[pl.DataFrame, pl.LazyFrame]: 预处理后的数据
        """
        return self.data_processor.preprocess_data(data)
    
    def sample_data(self, data: Union[pl.DataFrame, pl.LazyFrame], target_points: int = 1000, strategy: str = 'adaptive') -> Union[pl.DataFrame, pl.LazyFrame]:
        """
        采样数据，减少数据量
        
        Args:
            data: 原始数据（Polars DataFrame或LazyFrame）
            target_points: 目标采样点数
            strategy: 采样策略，可选值：'uniform'（均匀采样）、'adaptive'（自适应采样）
        
        Returns:
            Union[pl.DataFrame, pl.LazyFrame]: 采样后的数据
        """
        return self.data_processor.sample_data(data, target_points, strategy)
    
    def convert_data_type(self, data: Union[pl.DataFrame, pl.LazyFrame], target_type: str = 'float32') -> Union[pl.DataFrame, pl.LazyFrame]:
        """
        转换数据类型
        
        Args:
            data: 原始数据（Polars DataFrame或LazyFrame）
            target_type: 目标数据类型，默认：float32
        
        Returns:
            Union[pl.DataFrame, pl.LazyFrame]: 转换后的数据
        """
        return self.data_processor.convert_data_type(data, target_type)
    
    def clean_data(self, data: Union[pl.DataFrame, pl.LazyFrame]) -> Union[pl.DataFrame, pl.LazyFrame]:
        """
        清洗数据，处理缺失值、异常值等
        
        Args:
            data: 原始数据（Polars DataFrame或LazyFrame）
        
        Returns:
            Union[pl.DataFrame, pl.LazyFrame]: 清洗后的数据
        """
        return self.data_processor.clean_data(data)
