#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据获取与管理模块
"""

from loguru import logger
from typing import List, Dict, Any, Optional, Union
import polars as pl
import pandas as pd
from src.api.data_api import IDataProvider, IDataProcessor
from src.utils.event_bus import EventBus


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
            pl.DataFrame: 数据
        """
        try:
            import polars as pl
            
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
                
                # 已经是Polars DataFrame
                if hasattr(result, 'to_pandas'):
                    return result
                
                # 是pandas DataFrame，转换为Polars
                if hasattr(result, 'to_dict'):
                    # 检查是否为空DataFrame
                    if hasattr(result, 'empty') and result.empty:
                        return None
                    return pl.from_pandas(result)
                
                # 其他类型，尝试直接转换
                try:
                    return pl.DataFrame(result)
                except Exception:
                    return None
            
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
                            
                            # 使用列表推导式一次性构建数据记录，比for循环更高效
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
                            return df
                except Exception as db_e:
                    logger.warning(f"从数据库获取{type_name}数据失败: {db_e}")
            
            # 数据库获取失败或无数据，尝试从其他数据源获取
            data_sources = [
                ('tdx', self.tdx_handler),
                ('akshare', self.akshare_handler),
                ('baostock', self.baostock_handler)
            ]
            
            # 先尝试内置数据源
            for source_name, handler in data_sources:
                if handler:
                    logger.info(f"从{source_name}获取{type_name}{ts_code}数据")
                    try:
                        # 调用相应的数据源方法
                        method_name = type_map['handler_methods'][source_name]
                        result = getattr(handler, method_name)(ts_code, start_date, end_date, freq)
                        
                        # 统一处理结果
                        processed_result = process_result(result)
                        if processed_result is not None:
                            return processed_result
                    except Exception as source_e:
                        logger.warning(f"从{source_name}获取{type_name}数据失败: {source_e}")
            
            # 尝试从插件数据源获取数据
            for plugin_name, plugin in self.plugin_datasources.items():
                logger.info(f"从插件数据源{plugin_name}获取{type_name}{ts_code}数据")
                try:
                    # 调用插件的对应方法
                    method_name = type_map['handler_methods']['plugin']
                    result = getattr(plugin, method_name)(ts_code, start_date, end_date, freq)
                    
                    # 统一处理结果
                    processed_result = process_result(result)
                    if processed_result is not None:
                        return processed_result
                except Exception as plugin_e:
                    logger.warning(f"从插件数据源{plugin_name}获取{type_name}数据失败: {plugin_e}")
            
            # 所有数据源都失败，返回空DataFrame
            logger.warning(f"无法从任何数据源获取{type_name}{ts_code}数据")
            return pl.DataFrame()
            
        except Exception as e:
            logger.exception(f"获取{type_name}数据失败: {e}")
            raise
    
    def get_stock_data(self, stock_code: str, start_date: str, end_date: str, frequency: str = '1d') -> Union[pl.DataFrame, pd.DataFrame]:
        """
        获取股票历史数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线）
        
        Returns:
            pl.DataFrame或pd.DataFrame: 股票历史数据
        """
        freq_map = {'1d': 'daily', '1m': 'minute'}
        freq = freq_map.get(frequency, 'daily')
        result = self._get_data_from_sources("stock", stock_code, start_date, end_date, freq)
        return result
    
    def get_index_data(self, index_code: str, start_date: str, end_date: str, frequency: str = '1d') -> Union[pl.DataFrame, pd.DataFrame]:
        """
        获取指数历史数据
        
        Args:
            index_code: 指数代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线）
        
        Returns:
            pl.DataFrame或pd.DataFrame: 指数历史数据
        """
        freq_map = {'1d': 'daily', '1m': 'minute'}
        freq = freq_map.get(frequency, 'daily')
        result = self._get_data_from_sources("index", index_code, start_date, end_date, freq)
        return result
    
    def get_stock_basic(self, exchange: Optional[str] = None) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        获取股票基本信息
        
        Args:
            exchange: 交易所，可选值：'sh'（上海）、'sz'（深圳）、'bj'（北京）
        
        Returns:
            pl.DataFrame或pd.DataFrame: 股票基本信息
        """
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
                    return pl.DataFrame(data)
                else:
                    return pl.DataFrame()
            except Exception as query_e:
                # 如果查询失败，可能是表不存在，返回空DataFrame
                logger.warning(f"股票基本信息查询失败: {query_e}")
                return pl.DataFrame()
            
        except Exception as e:
            logger.exception(f"获取股票基本信息失败: {e}")
            return pl.DataFrame()
    
    def get_index_basic(self, exchange: Optional[str] = None) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        获取指数基本信息
        
        Args:
            exchange: 交易所，可选值：'sh'（上海）、'sz'（深圳）
        
        Returns:
            pl.DataFrame或pd.DataFrame: 指数基本信息
        """
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
                    return pl.DataFrame(data)
                else:
                    return pl.DataFrame()
            except Exception as query_e:
                # 如果查询失败，可能是表不存在，返回空DataFrame
                logger.warning(f"指数基本信息查询失败: {query_e}")
                return pl.DataFrame()
            
        except Exception as e:
            logger.exception(f"获取指数基本信息失败: {e}")
            return pl.DataFrame()
    
    def preprocess_data(self, data: Union[pl.DataFrame, pd.DataFrame]) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        预处理数据
        
        Args:
            data: 原始数据
        
        Returns:
            pl.DataFrame或pd.DataFrame: 预处理后的数据
        """
        if isinstance(data, pd.DataFrame):
            data = pl.from_pandas(data)
        
        # 确保数据包含必要的列
        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']
        
        if not all(col in data.columns for col in required_columns):
            logger.warning(f"数据缺少必要列，当前列: {data.columns}")
            return data
        
        # 排序数据
        if 'date' in data.columns:
            data = data.sort('date')
        
        # 去除重复数据
        data = data.unique(subset=['date'])
        
        return data
    
    def sample_data(self, data: Union[pl.DataFrame, pd.DataFrame], target_points: int = 1000, strategy: str = 'adaptive') -> Union[pl.DataFrame, pd.DataFrame]:
        """
        采样数据，减少数据量
        
        Args:
            data: 原始数据
            target_points: 目标采样点数
            strategy: 采样策略，可选值：'uniform'（均匀采样）、'adaptive'（自适应采样）
        
        Returns:
            pl.DataFrame或pd.DataFrame: 采样后的数据
        """
        if isinstance(data, pd.DataFrame):
            data = pl.from_pandas(data)
        
        if len(data) <= target_points:
            return data
        
        if strategy == 'uniform':
            # 均匀采样
            step = len(data) // target_points
            return data[::step]
        elif strategy == 'adaptive':
            # 自适应采样 - 这里使用简单的均匀采样作为默认实现
            # 实际自适应采样可以根据数据波动率进行调整
            step = len(data) // target_points
            return data[::step]
        else:
            logger.warning(f"不支持的采样策略: {strategy}，使用默认均匀采样")
            step = len(data) // target_points
            return data[::step]
    
    def convert_data_type(self, data: Union[pl.DataFrame, pd.DataFrame], target_type: str = 'float32') -> Union[pl.DataFrame, pd.DataFrame]:
        """
        转换数据类型
        
        Args:
            data: 原始数据
            target_type: 目标数据类型，默认：float32
        
        Returns:
            pl.DataFrame或pd.DataFrame: 转换后的数据
        """
        if isinstance(data, pd.DataFrame):
            data = pl.from_pandas(data)
        
        # 转换数值列的数据类型
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount', 'pct_chg']
        
        for col in data.columns:
            if col in numeric_columns:
                if target_type == 'float32':
                    data = data.with_columns(pl.col(col).cast(pl.Float32))
                elif target_type == 'float64':
                    data = data.with_columns(pl.col(col).cast(pl.Float64))
        
        return data
    
    def clean_data(self, data: Union[pl.DataFrame, pd.DataFrame]) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        清洗数据，处理缺失值、异常值等
        
        Args:
            data: 原始数据
        
        Returns:
            pl.DataFrame或pd.DataFrame: 清洗后的数据
        """
        if isinstance(data, pd.DataFrame):
            data = pl.from_pandas(data)
        
        # 去除包含空值的行
        data = data.drop_nulls()
        
        # 去除成交量为0的行
        if 'volume' in data.columns:
            data = data.filter(pl.col('volume') > 0)
        
        return data
