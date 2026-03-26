#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据获取与管理模块
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
from src.utils.memory_optimizer import MemoryOptimizer

# 异步数据管理器（延迟导入）
AsyncDataManager = None


class DataManager(IDataProvider, IDataProcessor):
    """
    数据管理器，负责统一管理各种数据源的获取、清洗和存储
    实现了IDataProvider和IDataProcessor接口
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
        
        # 初始化各个模块
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
    
    def _init_handlers(self):
        """
        初始化各个数据源处理器
        """
        # 初始化通达信数据处理器
        try:
            from src.data.tdx_handler import TdxHandler
            tdx_handler = TdxHandler(self.config, self.db_manager)
            self.data_fetcher.register_source(tdx_handler)
            self.data_updater.register_source(tdx_handler)
        except ImportError as e:
            logger.info(f"通达信模块未安装: {e}")
        except Exception as e:
            logger.warning(f"通达信数据处理器初始化失败: {e}")
        
        # 初始化Baostock数据处理器
        try:
            from src.data.baostock_handler import BaostockHandler
            baostock_handler = BaostockHandler(self.config, self.db_manager)
            self.data_fetcher.register_source(baostock_handler)
            self.data_updater.register_source(baostock_handler)
        except ImportError as e:
            logger.warning(f"Baostock模块未安装: {e}")
        except Exception as e:
            logger.warning(f"Baostock数据处理器初始化失败: {e}")
        
        # 初始化AkShare数据处理器
        try:
            from src.data.akshare_handler import AkShareHandler
            akshare_handler = AkShareHandler(self.config, self.db_manager)
            self.data_fetcher.register_source(akshare_handler)
            self.data_updater.register_source(akshare_handler)
        except ImportError as e:
            logger.info(f"AkShare模块未安装: {e}")
        except Exception as e:
            logger.warning(f"AkShare数据处理器初始化失败: {e}")
        
        # 初始化宏观数据处理器
        try:
            from src.data.macro_handler import MacroHandler
            macro_handler = MacroHandler(self.config, self.db_manager)
            self.data_fetcher.register_source(macro_handler)
            self.data_updater.register_source(macro_handler)
        except ImportError as e:
            logger.info(f"宏观数据模块未安装: {e}")
        except Exception as e:
            logger.warning(f"宏观数据处理器初始化失败: {e}")
        
        # 初始化新闻数据处理器
        try:
            from src.data.news_handler import NewsHandler
            news_handler = NewsHandler(self.config, self.db_manager)
            self.data_fetcher.register_source(news_handler)
            self.data_updater.register_source(news_handler)
        except ImportError as e:
            logger.info(f"新闻数据模块未安装: {e}")
        except Exception as e:
            logger.warning(f"新闻数据处理器初始化失败: {e}")
        
        logger.info("数据处理器初始化完成")
        
        # 初始化完成后，根据配置决定是否自动更新股票基本信息
        try:
            if hasattr(self.config.data, 'auto_update_stock_basic') and self.config.data.auto_update_stock_basic:
                self.update_stock_basic()
                logger.info("自动更新股票基本信息完成")
            else:
                logger.info("跳过自动更新股票基本信息（未开启或配置不允许）")
        except DataSourceConnectionError as e:
            logger.warning(f"自动更新股票基本信息失败 - 连接错误: {e.message}")
        except DataValidationError as e:
            logger.warning(f"自动更新股票基本信息失败 - 数据验证错误: {e.message}")
        except (OSError, RuntimeError) as update_e:
            logger.warning(f"自动更新股票基本信息失败: {update_e}")
    
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
        success = self.data_updater.update_stock_basic()
        if success:
            # 使股票基本信息缓存失效
            global_data_cache.invalidate_by_type('stock_basic')
    
    def update_stock_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新股票日线数据
        
        Args:
            ts_codes: 股票代码列表，None表示更新所有股票
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
        """
        success = self.data_updater.update_stock_daily(ts_codes, start_date, end_date)
        
        # 数据更新后，使相关缓存失效
        if success:
            if ts_codes:
                for ts_code in ts_codes:
                    global_data_cache.invalidate('stock', ts_code)
            else:
                # 如果更新所有股票，使所有股票缓存失效
                global_data_cache.invalidate_by_type('stock')
    
    def update_fund_basic(self):
        """
        更新基金基本信息
        """
        success = self.data_updater.update_fund_basic()
        if success:
            # 使基金基本信息缓存失效
            global_data_cache.invalidate_by_type('fund_basic')
    
    def update_fund_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新基金日线数据
        
        Args:
            ts_codes: 基金代码列表，None表示更新所有基金
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
        """
        success = self.data_updater.update_fund_daily(ts_codes, start_date, end_date)
        
        # 数据更新后，使相关缓存失效
        if success:
            if ts_codes:
                for ts_code in ts_codes:
                    global_data_cache.invalidate('fund', ts_code)
            else:
                # 如果更新所有基金，使所有基金缓存失效
                global_data_cache.invalidate_by_type('fund')
    
    def update_closed_fund_basic(self):
        """
        更新封闭式基金基本信息
        """
        success = self.data_updater.update_closed_fund_basic()
        if success:
            # 使封闭式基金基本信息缓存失效
            global_data_cache.invalidate_by_type('closed_fund_basic')
    
    def update_closed_fund_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新封闭式基金日线数据
        
        Args:
            ts_codes: 封闭式基金代码列表，None表示更新所有封闭式基金
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
        """
        success = self.data_updater.update_closed_fund_daily(ts_codes, start_date, end_date)
        
        # 数据更新后，使相关缓存失效
        if success:
            if ts_codes:
                for ts_code in ts_codes:
                    global_data_cache.invalidate('closed_fund', ts_code)
            else:
                # 如果更新所有封闭式基金，使所有封闭式基金缓存失效
                global_data_cache.invalidate_by_type('closed_fund')
    
    def update_index_basic(self):
        """
        更新指数基本信息
        """
        success = self.data_updater.update_index_basic()
        if success:
            # 使指数基本信息缓存失效
            global_data_cache.invalidate_by_type('index_basic')
    
    def update_index_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新指数日线数据
        
        Args:
            ts_codes: 指数代码列表，None表示更新所有指数
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
        """
        success = self.data_updater.update_index_daily(ts_codes, start_date, end_date)
        
        # 数据更新后，使相关缓存失效
        if success:
            if ts_codes:
                for ts_code in ts_codes:
                    global_data_cache.invalidate('index', ts_code)
            else:
                # 如果更新所有指数，使所有指数缓存失效
                global_data_cache.invalidate_by_type('index')
    
    def update_macro_data(self, indicators: List[str] = None):
        """
        更新宏观经济数据
        
        Args:
            indicators: 宏观经济指标列表，None表示更新所有指标
        """
        self.data_updater.update_macro_data(indicators)
    
    def update_news_data(self, sources: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新新闻数据
        
        Args:
            sources: 新闻来源列表，None表示更新所有来源
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
        """
        self.data_updater.update_news_data(sources, start_date, end_date)

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
        success = self.data_updater.update_stock_dividend(ts_codes)
        if success:
            # 使分红配股数据缓存失效
            if ts_codes:
                for ts_code in ts_codes:
                    global_data_cache.invalidate('stock_dividend', ts_code)
            else:
                global_data_cache.invalidate_by_type('stock_dividend')

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
    
    def _get_data_from_sources(self, data_type: str, ts_code: str, start_date: str, end_date: str, freq: str = "daily", adjustment_type: str = "qfq"):
        """
        通用数据获取方法
        
        Args:
            data_type: 数据类型，stock或index
            ts_code: 代码
            start_date: 开始日期
            end_date: 结束日期
            freq: 周期，daily或minute
            adjustment_type: 复权类型，qfq=前复权, hfq=后复权, none=不复权
            
        Returns:
            pl.DataFrame: 数据
        """
        try:
            import polars as pl
            import time
            from concurrent.futures import ThreadPoolExecutor, as_completed
            import concurrent.futures
            
            # 数据类型映射
            data_type_map = {
                'stock': {
                    'model': 'StockDaily',
                    'module': 'stock',
                    'handler_methods': {
                        'tdx': 'get_kline_data',
                        'akshare': 'get_stock_data',
                        'baostock': 'download_stock_daily',
                        'plugin': 'get_stock_data'
                    }
                },
                'index': {
                    'model': 'IndexDaily',
                    'module': 'index',
                    'handler_methods': {
                        'tdx': 'get_kline_data',
                        'akshare': 'get_index_data',
                        'baostock': 'download_index_daily',
                        'plugin': 'get_index_data'
                    }
                },
                'fund': {
                    'model': 'FundDaily',
                    'module': 'fund',
                    'handler_methods': {
                        'tdx': 'get_kline_data',
                        'akshare': 'get_fund_data',
                        'baostock': 'download_fund_daily',
                        'plugin': 'get_fund_data'
                    }
                },
                'closed_fund': {
                    'model': 'ClosedFundDaily',
                    'module': 'fund',
                    'handler_methods': {
                        'tdx': 'get_kline_data',
                        'akshare': 'get_closed_fund_data',
                        'baostock': 'download_closed_fund_daily',
                        'plugin': 'get_closed_fund_data'
                    }
                }
            }
            
            if data_type not in data_type_map:
                raise ValueError(f"不支持的数据类型: {data_type}")
            
            type_map = data_type_map[data_type]
            type_name = "股票" if data_type == "stock" else "指数"
            
            def process_result(result):
                """
                统一处理数据源结果，转换为Polars DataFrame
                
                Args:
                    result: 数据源返回的结果
                    
                Returns:
                    pl.DataFrame: Polars DataFrame格式的数据
                """
                if result is None:
                    return None
                
                # 已经是Polars LazyFrame，执行并返回DataFrame
                if hasattr(result, 'collect'):
                    return result.collect()
                
                # 已经是Polars DataFrame
                if hasattr(result, 'to_pandas'):
                    return result
                
                # 是pandas DataFrame，转换为Polars
                if hasattr(result, 'to_dict'):
                    if hasattr(result, 'empty') and result.empty:
                        return None
                    return pl.from_pandas(result)
                
                # 是dict类型（Baostock离线模式返回），取第一个值
                if isinstance(result, dict):
                    if len(result) > 0:
                        first_value = list(result.values())[0]
                        if hasattr(first_value, 'to_dict'):
                            if hasattr(first_value, 'empty') and first_value.empty:
                                return None
                            return pl.from_pandas(first_value)
                        elif isinstance(first_value, list):
                            return pl.DataFrame(first_value)
                    return None
                
                # 是list类型，转换为Polars
                if isinstance(result, list):
                    if len(result) == 0:
                        return None
                    return pl.DataFrame(result)
                
                # 其他类型，尝试直接转换
                try:
                    return pl.DataFrame(result)
                except (ValueError, TypeError):
                    return None
            
            def fetch_from_source(source_name, handler, method_name, **kwargs):
                """
                从单个数据源获取数据
                
                Args:
                    source_name: 数据源名称
                    handler: 数据处理器
                    method_name: 方法名
                    **kwargs: 方法参数
                    
                Returns:
                    tuple: (成功标志, 数据或错误信息)
                """
                start_time = time.time()
                success = False
                
                try:
                    logger.info(f"线程获取: 从{source_name}获取{type_name}{ts_code}数据")
                    result = getattr(handler, method_name)(**kwargs)
                    processed_result = process_result(result)
                    if processed_result is not None:
                        # 内存优化
                        optimized_result = MemoryOptimizer.optimize_dataframe(processed_result, enable_sparse=True)
                        success = True
                        return True, optimized_result
                    else:
                        return False, f"{source_name}返回空数据"
                except (OSError, RuntimeError, ValueError) as e:
                    logger.warning(f"从{source_name}获取{type_name}数据失败: {e}")
                    return False, str(e)
                finally:
                    # 记录监控数据
                    response_time = time.time() - start_time
                    from src.utils.monitoring import global_monitoring_system
                    global_monitoring_system.record_data_source_request(source_name, response_time, success)
                    
                    # 更新数据源优先级
                    if source_name not in self.source_priorities:
                        self.source_priorities[source_name] = []
                    self.source_priorities[source_name].append(response_time)
                    # 只保留最近10次的响应时间
                    if len(self.source_priorities[source_name]) > 10:
                        self.source_priorities[source_name] = self.source_priorities[source_name][-10:]
            
            # 优先从数据库获取数据
            if self.db_manager and self.db_manager.is_connected():
                logger.info(f"从数据库获取{type_name}{ts_code}数据")
                try:
                    # 动态导入模型
                    module_path = f"src.database.models.{type_map['module']}"
                    module = __import__(module_path, fromlist=[type_map['model']])
                    model_class = getattr(module, type_map['model'])
                    
                    session = self.db_manager.get_session()
                    if session:
                        query = session.query(model_class).filter(
                            getattr(model_class, 'ts_code') == ts_code,
                            getattr(model_class, 'trade_date') >= start_date,
                            getattr(model_class, 'trade_date') <= end_date
                        ).order_by(getattr(model_class, 'trade_date'))
                        
                        data = query.all()
                        if data:
                            # 提取数据记录，直接构建适合Polars的字典列表
                            has_pct_chg = hasattr(data[0], 'pct_chg')
                            
                            # 检查是否有复权因子字段
                            has_qfq_factor = hasattr(data[0], 'qfq_factor')
                            has_hfq_factor = hasattr(data[0], 'hfq_factor')
                            
                            # 根据复权类型选择对应的价格字段
                            if adjustment_type == 'qfq':
                                # 前复权
                                data_records = []
                                for item in data:
                                    # 优先使用预计算的复权价格
                                    if hasattr(item, 'qfq_open') and item.qfq_open:
                                        open_price = item.qfq_open
                                        high_price = item.qfq_high
                                        low_price = item.qfq_low
                                        close_price = item.qfq_close
                                    # 其次使用复权因子实时计算
                                    elif has_qfq_factor and item.qfq_factor and item.qfq_factor != 1.0:
                                        open_price = item.open * item.qfq_factor
                                        high_price = item.high * item.qfq_factor
                                        low_price = item.low * item.qfq_factor
                                        close_price = item.close * item.qfq_factor
                                    # 回退到原始价格
                                    else:
                                        open_price = item.open
                                        high_price = item.high
                                        low_price = item.low
                                        close_price = item.close

                                    record = {
                                        'trade_date': item.trade_date,
                                        'open': open_price,
                                        'high': high_price,
                                        'low': low_price,
                                        'close': close_price,
                                        'volume': item.vol,
                                        'amount': item.amount,
                                        **({'pct_chg': item.pct_chg} if has_pct_chg else {})
                                    }
                                    # 同时添加复权价格列，供chart_data_preparer使用
                                    if hasattr(item, 'qfq_open') and item.qfq_open:
                                        record['qfq_open'] = item.qfq_open
                                        record['qfq_high'] = item.qfq_high
                                        record['qfq_low'] = item.qfq_low
                                        record['qfq_close'] = item.qfq_close
                                    else:
                                        record['qfq_open'] = open_price
                                        record['qfq_high'] = high_price
                                        record['qfq_low'] = low_price
                                        record['qfq_close'] = close_price
                                    data_records.append(record)
                            elif adjustment_type == 'hfq':
                                # 后复权
                                data_records = []
                                for item in data:
                                    # 优先使用预计算的复权价格
                                    if hasattr(item, 'hfq_open') and item.hfq_open:
                                        open_price = item.hfq_open
                                        high_price = item.hfq_high
                                        low_price = item.hfq_low
                                        close_price = item.hfq_close
                                    # 其次使用复权因子实时计算
                                    elif has_hfq_factor and item.hfq_factor and item.hfq_factor != 1.0:
                                        open_price = item.open * item.hfq_factor
                                        high_price = item.high * item.hfq_factor
                                        low_price = item.low * item.hfq_factor
                                        close_price = item.close * item.hfq_factor
                                    # 回退到原始价格
                                    else:
                                        open_price = item.open
                                        high_price = item.high
                                        low_price = item.low
                                        close_price = item.close

                                    record = {
                                        'trade_date': item.trade_date,
                                        'open': open_price,
                                        'high': high_price,
                                        'low': low_price,
                                        'close': close_price,
                                        'volume': item.vol,
                                        'amount': item.amount,
                                        **({'pct_chg': item.pct_chg} if has_pct_chg else {})
                                    }
                                    # 同时添加复权价格列，供chart_data_preparer使用
                                    if hasattr(item, 'hfq_open') and item.hfq_open:
                                        record['hfq_open'] = item.hfq_open
                                        record['hfq_high'] = item.hfq_high
                                        record['hfq_low'] = item.hfq_low
                                        record['hfq_close'] = item.hfq_close
                                    else:
                                        record['hfq_open'] = open_price
                                        record['hfq_high'] = high_price
                                        record['hfq_low'] = low_price
                                        record['hfq_close'] = close_price
                                    data_records.append(record)
                            else:
                                # 不复权
                                data_records = [{
                                    'trade_date': item.trade_date,
                                    'open': item.open,
                                    'high': item.high,
                                    'low': item.low,
                                    'close': item.close,
                                    'volume': item.vol,
                                    'amount': item.amount,
                                    **({'pct_chg': item.pct_chg} if has_pct_chg else {})
                                } for item in data]
                            
                            # 使用pl.from_dicts创建Polars DataFrame，效率更高
                            df = pl.from_dicts(data_records)
                            
                            # 转换为标准格式，使用向量化的str.strptime替代apply，效率更高
                            df = df.with_columns(
                                pl.col('trade_date').str.strptime(pl.Datetime, format='%Y%m%d').alias('date')
                            )
                            
                            # 内存优化：转换数据类型
                            optimized_df = MemoryOptimizer.optimize_dataframe(df, enable_sparse=True)
                            # 打印内存优化效果
                            MemoryOptimizer.print_memory_stats(optimized_df, f"{type_name}{ts_code} 优化后")
                            return optimized_df
                except (OSError, RuntimeError, ValueError) as db_e:
                    logger.warning(f"从数据库获取{type_name}数据失败: {db_e}")
            
            # 数据库获取失败或无数据，并行尝试从其他数据源获取
            data_sources = []
            
            # 数据源已通过data_fetcher和data_updater管理，不再直接引用
            
            # 插件数据源
            for plugin_name, plugin in self.plugin_datasources.items():
                method_name = type_map['handler_methods']['plugin']
                data_sources.append((f'plugin_{plugin_name}', plugin, method_name, {
                    'ts_code': ts_code, 'start_date': start_date, 'end_date': end_date, 'freq': freq
                }))
            
            # 直接使用data_fetcher获取数据
            if data_type == 'stock':
                result = self.data_fetcher.get_stock_data(ts_code, start_date, end_date, freq, adjustment_type)
            else:
                result = self.data_fetcher.get_index_data(ts_code, start_date, end_date, freq)
            
            return result

        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"获取{type_name}数据失败: {e}")
            raise
    
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
        # 尝试从缓存获取数据
        cached_data = global_data_cache.get('stock', stock_code, start_date, end_date, 
                                         frequency=frequency, adjustment_type=adjustment_type)
        if cached_data is not None:
            logger.info(f"从缓存获取股票数据: {stock_code} {start_date} to {end_date}")
            return cached_data
        
        # 日线或分钟线，直接获取
        if frequency in ['1d', '1m']:
            result = self.data_fetcher.get_stock_data(stock_code, start_date, end_date, frequency, adjustment_type)
        else:
            # 周线或月线，先获取日线数据，再转换
            df = self.data_fetcher.get_stock_data(stock_code, start_date, end_date, '1d', adjustment_type)
            result = self.data_processor.convert_frequency(df, frequency)
        
        # 将结果存入缓存
        if not result.is_empty():
            global_data_cache.set(result, 'stock', stock_code, start_date, end_date, 
                                frequency=frequency, adjustment_type=adjustment_type)
            logger.info(f"股票数据缓存已更新: {stock_code} {start_date} to {end_date}")
        
        return result
    

    
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
        # 尝试从缓存获取数据
        cached_data = global_data_cache.get('index', index_code, start_date, end_date, 
                                         frequency=frequency)
        if cached_data is not None:
            logger.info(f"从缓存获取指数数据: {index_code} {start_date} to {end_date}")
            return cached_data
        
        # 日线或分钟线，直接获取
        if frequency in ['1d', '1m']:
            result = self.data_fetcher.get_index_data(index_code, start_date, end_date, frequency)
        else:
            # 周线或月线，先获取日线数据，再转换
            df = self.data_fetcher.get_index_data(index_code, start_date, end_date, '1d')
            result = self.data_processor.convert_frequency(df, frequency)
        
        # 将结果存入缓存
        if not result.is_empty():
            global_data_cache.set(result, 'index', index_code, start_date, end_date, 
                                frequency=frequency)
            logger.info(f"指数数据缓存已更新: {index_code} {start_date} to {end_date}")
        
        return result
    
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
    
    async def get_multiple_stocks_data_async(self, stock_codes: List[str], start_date: str, end_date: str, frequency: str = '1d', adjustment_type: str = 'qfq') -> Dict[str, pl.DataFrame]:
        """
        异步并行获取多只股票数据
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线）
            adjustment_type: 复权类型，qfq=前复权, hfq=后复权, none=不复权
        
        Returns:
            Dict[str, pl.DataFrame]: 股票代码到数据的映射
        """
        if self.async_data_manager:
            return await self.async_data_manager.get_multiple_stocks_data_async(stock_codes, start_date, end_date, frequency, adjustment_type)
        else:
            # 回退到同步方法（串行获取）
            result = {}
            for stock_code in stock_codes:
                result[stock_code] = self.get_stock_data(stock_code, start_date, end_date, frequency, adjustment_type)
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
                
                # 如果没有数据，插入默认基金信息
                if not fund_basics:
                    logger.info("fund_basic表为空，插入默认基金信息")
                    default_funds = [
                        {"ts_code": "510050.SH", "name": "上证50ETF"},
                        {"ts_code": "510300.SH", "name": "沪深300ETF"},
                        {"ts_code": "159919.SZ", "name": "创业板ETF"},
                        {"ts_code": "510500.SH", "name": "中证500ETF"},
                        {"ts_code": "517520.SH", "name": "黄金股ETF"},
                        {"ts_code": "159562.SZ", "name": "中证金矿ETF"}
                    ]
                    
                    for fund_info in default_funds:
                        # 检查是否已存在
                        existing_fund = session.query(FundBasic).filter_by(ts_code=fund_info["ts_code"]).first()
                        if not existing_fund:
                            new_fund = FundBasic(
                                ts_code=fund_info["ts_code"],
                                name=fund_info["name"]
                            )
                            session.add(new_fund)
                    
                    # 提交事务
                    session.commit()
                    # 重新查询数据
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
