#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据获取与管理模块
"""

from loguru import logger
from typing import List, Dict, Any
from src.utils.event_bus import EventBus


class DataManager:
    """
    数据管理器，负责统一管理各种数据源的获取、清洗和存储
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
        
        # 初始化各个数据源处理器
        self.tdx_handler = None
        self.akshare_handler = None
        self.macro_handler = None
        self.news_handler = None
        self.baostock_handler = None
        
        # 插件数据源映射
        self.plugin_datasources = {}
        
        self._init_handlers()
        self._init_plugin_datasources()
    
    def _init_handlers(self):
        """
        初始化各个数据源处理器
        """
        try:
            # 初始化通达信数据处理器
            try:
                from src.data.tdx_handler import TdxHandler
                self.tdx_handler = TdxHandler(self.config, self.db_manager)
            except Exception as tdx_e:
                logger.warning(f"通达信数据处理器初始化失败（离线模式下正常）: {tdx_e}")
                self.tdx_handler = None
            
            # 初始化Baostock数据处理器
            try:
                from src.data.baostock_handler import BaostockHandler
                self.baostock_handler = BaostockHandler(self.config, self.db_manager)
                logger.info("Baostock数据处理器初始化成功")
            except Exception as e:
                logger.exception(f"Baostock数据处理器初始化失败: {e}")
                self.baostock_handler = None
            
            # 初始化宏观数据处理器
            try:
                from src.data.macro_handler import MacroHandler
                self.macro_handler = MacroHandler(self.config, self.db_manager)
            except Exception as macro_e:
                logger.warning(f"宏观数据处理器初始化失败（离线模式下正常）: {macro_e}")
                self.macro_handler = None
            
            # 初始化新闻数据处理器
            try:
                from src.data.news_handler import NewsHandler
                self.news_handler = NewsHandler(self.config, self.db_manager)
            except Exception as news_e:
                logger.warning(f"新闻数据处理器初始化失败（离线模式下正常）: {news_e}")
                self.news_handler = None
            
            logger.info("数据处理器初始化完成")
            
            # 初始化完成后，根据配置决定是否自动更新股票基本信息
            try:
                if hasattr(self.config.data, 'auto_update_stock_basic') and self.config.data.auto_update_stock_basic:
                    self.update_stock_basic()
                    logger.info("自动更新股票基本信息完成")
                else:
                    logger.info("跳过自动更新股票基本信息（未开启或配置不允许）")
            except Exception as update_e:
                logger.warning(f"自动更新股票基本信息失败: {update_e}")
            
        except Exception as e:
            logger.exception(f"数据处理器初始化失败: {e}")
            # 离线模式下不抛出异常，继续运行
            logger.info("离线模式下继续运行")
    
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
            logger.info(f"已注册插件数据源: {plugin_name}")
    
    def _publish_data_updated_event(self, data_type, ts_code, status="success", message=""):
        """
        发布数据更新事件
        
        Args:
            data_type: 数据类型，如'stock', 'index', 'macro', 'news'
            ts_code: 股票代码或指数代码
            status: 更新状态，success或error
            message: 附加消息
        """
        EventBus.publish(
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
        """
        try:
            if handler:
                method = getattr(handler, method_name)
                method(**kwargs)
            logger.info(f"{data_type}更新完成")
            
            # 发布数据更新事件
            self._publish_data_updated_event(event_type, identifier)
            
        except Exception as e:
            logger.exception(f"{data_type}更新失败: {e}")
            self._publish_data_updated_event(event_type, identifier, status='error', message=str(e))
            raise
    
    def update_stock_basic(self):
        """
        更新股票基本信息
        """
        self._update_data(
            data_type="股票基本信息",
            handler=self.baostock_handler,
            method_name="update_stock_basic",
            event_type="stock_basic"
        )
    
    def update_stock_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新股票日线数据
        
        Args:
            ts_codes: 股票代码列表，None表示更新所有股票
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
        """
        self._update_data(
            data_type="股票日线数据",
            handler=self.baostock_handler,
            method_name="update_stock_daily",
            event_type="stock_daily",
            identifier=ts_codes[0] if ts_codes else 'all',
            ts_codes=ts_codes,
            start_date=start_date,
            end_date=end_date
        )
    
    def update_index_basic(self):
        """
        更新指数基本信息
        """
        self._update_data(
            data_type="指数基本信息",
            handler=self.baostock_handler,
            method_name="update_index_basic",
            event_type="index_basic"
        )
    
    def update_index_daily(self, ts_codes: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新指数日线数据
        
        Args:
            ts_codes: 指数代码列表，None表示更新所有指数
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
        """
        self._update_data(
            data_type="指数日线数据",
            handler=self.baostock_handler,
            method_name="update_index_daily",
            event_type="index_daily",
            identifier=ts_codes[0] if ts_codes else 'all',
            ts_codes=ts_codes,
            start_date=start_date,
            end_date=end_date
        )
    
    def update_macro_data(self, indicators: List[str] = None):
        """
        更新宏观经济数据
        
        Args:
            indicators: 宏观经济指标列表，None表示更新所有指标
        """
        self._update_data(
            data_type="宏观经济数据",
            handler=self.macro_handler,
            method_name="update_macro_data",
            event_type="macro",
            identifier=indicators[0] if indicators else 'all',
            indicators=indicators
        )
    
    def update_news_data(self, sources: List[str] = None, start_date: str = None, end_date: str = None):
        """
        更新新闻数据
        
        Args:
            sources: 新闻来源列表，None表示更新所有来源
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
        """
        self._update_data(
            data_type="新闻数据",
            handler=self.news_handler,
            method_name="update_news_data",
            event_type="news",
            identifier=sources[0] if sources else 'all',
            sources=sources,
            start_date=start_date,
            end_date=end_date
        )
    
    def _get_data_from_sources(self, data_type: str, ts_code: str, start_date: str, end_date: str, freq: str = "daily"):
        """
        通用数据获取方法
        
        Args:
            data_type: 数据类型，stock或index
            ts_code: 代码
            start_date: 开始日期
            end_date: 结束日期
            freq: 周期，daily或minute
            
        Returns:
            pd.DataFrame: 数据
        """
        try:
            import pandas as pd
            
            # 数据类型映射
            data_type_map = {
                'stock': {
                    'model': 'StockDaily',
                    'module': 'stock',
                    'handler_methods': {
                        'tdx': 'get_stock_data',
                        'akshare': 'get_stock_data',
                        'baostock': 'get_stock_data',
                        'plugin': 'get_stock_data'
                    }
                },
                'index': {
                    'model': 'IndexDaily',
                    'module': 'index',
                    'handler_methods': {
                        'tdx': 'get_index_data',
                        'akshare': 'get_index_data',
                        'baostock': 'get_index_data',
                        'plugin': 'get_index_data'
                    }
                }
            }
            
            if data_type not in data_type_map:
                raise ValueError(f"不支持的数据类型: {data_type}")
            
            type_map = data_type_map[data_type]
            type_name = "股票" if data_type == "stock" else "指数"
            
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
                            # 转换为DataFrame
                            data_dict = {
                                'trade_date': [item.trade_date for item in data],
                                'open': [item.open for item in data],
                                'high': [item.high for item in data],
                                'low': [item.low for item in data],
                                'close': [item.close for item in data],
                                'volume': [item.vol for item in data],
                                'amount': [item.amount for item in data]
                            }
                            
                            # 只有股票数据有pct_chg字段
                            if hasattr(data[0], 'pct_chg'):
                                data_dict['pct_chg'] = [item.pct_chg for item in data]
                            
                            df = pd.DataFrame(data_dict)
                            # 转换为标准格式
                            df['date'] = pd.to_datetime(df['trade_date'])
                            df.set_index('date', inplace=True)
                            return df
                except Exception as db_e:
                    logger.warning(f"从数据库获取{type_name}数据失败: {db_e}")
            
            # 数据库获取失败或无数据，尝试从其他数据源获取
            data_sources = [
                ('tdx', self.tdx_handler),
                ('akshare', self.akshare_handler),
                ('baostock', self.baostock_handler)
            ]
            
            for source_name, handler in data_sources:
                if handler:
                    logger.info(f"从{source_name}获取{type_name}{ts_code}数据")
                    try:
                        # 调用相应的数据源方法
                        method_name = type_map['handler_methods'][source_name]
                        return getattr(handler, method_name)(ts_code, start_date, end_date, freq)
                    except Exception as source_e:
                        logger.warning(f"从{source_name}获取{type_name}数据失败: {source_e}")
            
            # 尝试从插件数据源获取数据
            for plugin_name, plugin in self.plugin_datasources.items():
                logger.info(f"从插件数据源{plugin_name}获取{type_name}{ts_code}数据")
                try:
                    # 调用插件的对应方法
                    method_name = type_map['handler_methods']['plugin']
                    result = getattr(plugin, method_name)(ts_code, start_date, end_date, freq)
                    if result is not None and not result.empty:
                        return result
                except Exception as plugin_e:
                    logger.warning(f"从插件数据源{plugin_name}获取{type_name}数据失败: {plugin_e}")
            
            # 所有数据源都失败，返回空DataFrame
            logger.warning(f"无法从任何数据源获取{type_name}{ts_code}数据")
            return pd.DataFrame()
            
        except Exception as e:
            logger.exception(f"获取{type_name}数据失败: {e}")
            raise
    
    def get_stock_data(self, ts_code: str, start_date: str, end_date: str, freq: str = "daily"):
        """
        获取股票数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            freq: 周期，daily或minute
            
        Returns:
            pd.DataFrame: 股票数据
        """
        return self._get_data_from_sources("stock", ts_code, start_date, end_date, freq)
    
    def get_index_data(self, ts_code: str, start_date: str, end_date: str, freq: str = "daily"):
        """
        获取指数数据
        
        Args:
            ts_code: 指数代码
            start_date: 开始日期
            end_date: 结束日期
            freq: 周期，daily或minute
            
        Returns:
            pd.DataFrame: 指数数据
        """
        return self._get_data_from_sources("index", ts_code, start_date, end_date, freq)
    
    def get_stock_basic(self, ts_code: str = None):
        """
        获取股票基本信息
        
        Args:
            ts_code: 股票代码，None表示获取所有股票基本信息
            
        Returns:
            dict: 股票代码到名称的映射
        """
        try:
            if not self.db_manager:
                logger.warning("数据库连接不可用，无法获取股票基本信息")
                return {}
            
            from src.database.models.stock import StockBasic
            
            session = self.db_manager.get_session()
            if not session:
                return {}
            
            try:
                # 检查stock_basic表是否有数据
                query = session.query(StockBasic)
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
                    stock_basics = session.query(StockBasic).all()
                
                # 构建股票代码到名称的映射
                stock_map = {}
                for stock in stock_basics:
                    stock_map[stock.ts_code] = stock.name
                
                # 如果指定了ts_code但没有找到，添加到映射中
                if ts_code and ts_code not in stock_map:
                    default_map = {
                        "600000.SH": "浦发银行",
                        "000001.SZ": "平安银行",
                        "300001.SZ": "特锐德"
                    }
                    if ts_code in default_map:
                        stock_map[ts_code] = default_map[ts_code]
                
                return stock_map
            except Exception as query_e:
                # 如果查询失败，可能是表不存在，使用默认映射
                logger.warning(f"股票基本信息查询失败: {query_e}")
                # 返回默认映射，使用代码作为名称
                default_map = {
                    "600000.SH": "浦发银行",
                    "000001.SZ": "平安银行",
                    "300001.SZ": "特锐德"
                }
                return default_map
            
        except Exception as e:
            logger.exception(f"获取股票基本信息失败: {e}")
            # 返回默认映射，使用代码作为名称
            default_map = {
                "600000.SH": "浦发银行",
                "000001.SZ": "平安银行",
                "300001.SZ": "特锐德"
            }
            return default_map
