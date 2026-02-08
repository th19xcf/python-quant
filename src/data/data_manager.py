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

    def update_stock_dividend(self, ts_codes: List[str] = None):
        """
        更新股票分红配股数据

        Args:
            ts_codes: 股票代码列表，None表示更新所有股票
        """
        try:
            # 初始化AkShare处理器（如果尚未初始化）
            if not self.akshare_handler:
                from src.data.akshare_handler import AkShareHandler
                self.akshare_handler = AkShareHandler(self.config, self.db_manager)
                logger.info("AkShare数据处理器初始化成功")

            self._update_data(
                data_type="股票分红配股数据",
                handler=self.akshare_handler,
                method_name="update_stock_dividend",
                event_type="stock_dividend",
                identifier=ts_codes[0] if ts_codes else 'all',
                ts_codes=ts_codes
            )
        except Exception as e:
            logger.exception(f"更新股票分红配股数据失败: {e}")
            raise

    def get_stock_dividend(self, ts_code: str) -> pl.DataFrame:
        """
        获取股票分红配股数据

        Args:
            ts_code: 股票代码

        Returns:
            pl.DataFrame: 分红配股数据
        """
        try:
            if not self.db_manager or not self.db_manager.is_connected():
                logger.warning("数据库未连接，无法获取分红配股数据")
                return pl.DataFrame()

            from src.database.models.stock import StockDividend

            session = self.db_manager.get_session()
            if not session:
                logger.warning("无法获取数据库会话")
                return pl.DataFrame()

            # 查询分红配股数据
            dividends = session.query(StockDividend).filter_by(ts_code=ts_code).all()

            if not dividends:
                return pl.DataFrame()

            # 转换为Polars DataFrame
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

            return pl.DataFrame(data)

        except Exception as e:
            logger.exception(f"获取股票分红配股数据失败: {e}")
            return pl.DataFrame()

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
                
                # 是list类型（TdxHandler返回），转换为Polars
                if isinstance(result, list):
                    if len(result) == 0:
                        return None
                    return pl.DataFrame(result)
                
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
                        
                        # 根据不同的handler调整参数
                        if source_name == 'tdx':
                            # TdxHandler.get_kline_data(stock_code, start_date, end_date, adjust)
                            # TDX数据源支持复权，传递adjustment_type参数
                            result = getattr(handler, method_name)(ts_code, start_date, end_date, adjust=adjustment_type)
                        elif source_name == 'baostock':
                            # BaostockHandler.download_stock_daily(ts_codes=[ts_code], start_date, end_date, adjustflag)
                            # 将adjustment_type转换为baostock的adjustflag参数
                            adjustflag_map = {'qfq': '2', 'hfq': '1', 'none': '3'}
                            adjustflag = adjustflag_map.get(adjustment_type, '2')
                            result = getattr(handler, method_name)(ts_codes=[ts_code], start_date=start_date, end_date=end_date, adjustflag=adjustflag)
                        else:
                            # 其他handler使用标准参数
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
    
    def get_stock_data(self, stock_code: str, start_date: str, end_date: str, frequency: str = '1d', adjustment_type: str = 'qfq') -> Union[pl.DataFrame, pd.DataFrame]:
        """
        获取股票历史数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            frequency: 数据频率，默认：1d（日线），支持1d/1w/1m（日/周/月线）
            adjustment_type: 复权类型，qfq=前复权, hfq=后复权, none=不复权
        
        Returns:
            pl.DataFrame或pd.DataFrame: 股票历史数据
        """
        # 将周线和月线转换为日线获取，然后进行聚合
        if frequency in ['1w', '1m']:
            # 获取日线数据
            freq_map = {'1d': 'daily', '1m': 'minute'}
            freq = freq_map.get('1d', 'daily')
            df = self._get_data_from_sources("stock", stock_code, start_date, end_date, freq, adjustment_type)
            
            # 将日线数据转换为周线或月线
            if not df.is_empty():
                df = self._convert_to_period(df, frequency)
            
            return df
        else:
            # 日线或分钟线，直接获取
            freq_map = {'1d': 'daily', '1m': 'minute'}
            freq = freq_map.get(frequency, 'daily')
            result = self._get_data_from_sources("stock", stock_code, start_date, end_date, freq, adjustment_type)
            return result
    
    def _convert_to_period(self, df: pl.DataFrame, frequency: str) -> pl.DataFrame:
        """
        将日线数据转换为周线或月线数据
        使用 Polars Lazy API 优化性能
        
        Args:
            df: 日线数据
            frequency: 目标频率，1w=周线，1m=月线
        
        Returns:
            pl.DataFrame: 转换后的数据
        """
        if df.is_empty():
            return df
        
        try:
            # 使用 Lazy API 进行数据处理
            lazy_df = df.lazy()
            
            # 确保有日期列
            if 'trade_date' in df.columns:
                lazy_df = lazy_df.with_columns(pl.col('trade_date').alias('date'))
            elif 'date' not in df.columns:
                logger.error("DataFrame中没有日期列")
                return df
            
            # 转换日期列为datetime类型
            lazy_df = lazy_df.with_columns(pl.col('date').str.strptime(pl.Date, "%Y-%m-%d"))
            
            # 根据频率确定分组方式
            if frequency == '1w':
                # 周线：按周分组
                lazy_df = lazy_df.with_columns(
                    pl.col('date').dt.week().alias('week'),
                    pl.col('date').dt.year().alias('year')
                )
                group_cols = ['year', 'week']
            elif frequency == '1m':
                # 月线：按月分组
                lazy_df = lazy_df.with_columns(
                    pl.col('date').dt.month().alias('month'),
                    pl.col('date').dt.year().alias('year')
                )
                group_cols = ['year', 'month']
            else:
                return df
            
            # 聚合数据
            lazy_df = lazy_df.group_by(group_cols).agg([
                pl.col('date').first().alias('date'),
                pl.col('open').first().alias('open'),
                pl.col('high').max().alias('high'),
                pl.col('low').min().alias('low'),
                pl.col('close').last().alias('close'),
                pl.col('vol').sum().alias('vol'),
                pl.col('amount').sum().alias('amount'),
                pl.col('pct_chg').sum().alias('pct_chg'),
                pl.col('change').sum().alias('change')
            ])
            
            # 按日期排序
            lazy_df = lazy_df.sort('date')
            
            # 将日期转换回字符串格式
            lazy_df = lazy_df.with_columns(
                pl.col('date').dt.strftime("%Y-%m-%d").alias('date')
            )
            
            # 执行计算
            result = lazy_df.collect()
            
            logger.info(f"将日线数据转换为{frequency}数据，从{df.height}条转换为{result.height}条")
            return result
            
        except Exception as e:
            logger.exception(f"转换数据频率失败: {e}")
            return df
    
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
    
    def preprocess_data(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        预处理数据
        
        Args:
            data: 原始数据（Polars DataFrame）
        
        Returns:
            pl.DataFrame: 预处理后的数据
        """
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
    
    def sample_data(self, data: pl.DataFrame, target_points: int = 1000, strategy: str = 'adaptive') -> pl.DataFrame:
        """
        采样数据，减少数据量
        
        Args:
            data: 原始数据（Polars DataFrame）
            target_points: 目标采样点数
            strategy: 采样策略，可选值：'uniform'（均匀采样）、'adaptive'（自适应采样）
        
        Returns:
            pl.DataFrame: 采样后的数据
        """
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
    
    def convert_data_type(self, data: pl.DataFrame, target_type: str = 'float32') -> pl.DataFrame:
        """
        转换数据类型
        
        Args:
            data: 原始数据（Polars DataFrame）
            target_type: 目标数据类型，默认：float32
        
        Returns:
            pl.DataFrame: 转换后的数据
        """
        # 转换数值列的数据类型
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount', 'pct_chg']
        
        for col in data.columns:
            if col in numeric_columns:
                if target_type == 'float32':
                    data = data.with_columns(pl.col(col).cast(pl.Float32))
                elif target_type == 'float64':
                    data = data.with_columns(pl.col(col).cast(pl.Float64))
        
        return data
    
    def clean_data(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        清洗数据，处理缺失值、异常值等
        
        Args:
            data: 原始数据（Polars DataFrame）
        
        Returns:
            pl.DataFrame: 清洗后的数据
        """
        # 去除包含空值的行
        data = data.drop_nulls()
        
        # 去除成交量为0的行
        if 'volume' in data.columns:
            data = data.filter(pl.col('volume') > 0)
        
        return data
