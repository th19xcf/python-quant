#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
通达信数据处理器
"""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any

import polars as pl
from loguru import logger

from src.data.sources.base import DataSourceBase
from src.data.tdx_day_parser import parse_day_file as parse_tdx_day_file
from src.data.tdx_day_parser import read_recent_records


class TdxHandler(DataSourceBase):
    """
    通达信数据处理器，负责解析通达信数据文件并存储到数据库

    继承 DataSourceBase 获得统一接口；DataFetcher 通过标准接口
    get_stock_data / get_index_data 调度。get_kline_data 为本地文件级
    底层方法，仍供内部与增量同步使用。提供 close() 方法（基类没有）。
    """
    
    def __init__(self, config, db_manager):
        """
        初始化通达信数据处理器
        
        Args:
            config: 配置对象
            db_manager: 数据库管理器实例
        """
        self.config = config
        self.db_manager = db_manager
        self.name = "TdxHandler"
        self.is_local = True  # 本地文件源，优先于远程源
        self.tdx_data_path = Path(config.data.tdx_data_path)

        # 检查通达信数据路径是否存在；路径缺失时标记为不可用
        self.available = self.tdx_data_path.exists()
        if not self.available:
            logger.warning(f"通达信数据路径不存在: {self.tdx_data_path}")
        
        # 初始化线程池，使用配置中的最大工作线程数
        self.max_workers = self.config.data.max_workers
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        logger.info(f"TdxHandler线程池初始化完成，最大工作线程数: {self.max_workers}")
        
        # 线程本地存储，用于保存每个线程的数据库会话
        self.thread_local = threading.local()

        # 离线模式支持
        self.offline_mode = db_manager is None

    def __del__(self):
        """
        析构函数，确保线程池正确关闭
        """
        self.close()

    def close(self):
        """
        关闭处理器，清理资源
        """
        try:
            # 关闭线程池
            if hasattr(self, 'executor') and self.executor:
                logger.info("正在关闭TdxHandler线程池...")
                self.executor.shutdown(wait=True)
                logger.info("TdxHandler线程池已关闭")

            # 关闭线程本地存储的数据库会话
            if hasattr(self.thread_local, 'session') and self.thread_local.session:
                try:
                    self.thread_local.session.close()
                    logger.debug(f"线程 {threading.current_thread().name} 的数据库会话已关闭")
                except (OSError, IOError) as e:
                    logger.warning(f"关闭线程数据库会话时出错: {e}")
        except (OSError, IOError) as e:
            logger.exception(f"关闭TdxHandler时出错: {e}")
        
    def _get_thread_safe_session(self):
        """
        获取线程安全的数据库会话
        
        Returns:
            session: 当前线程的数据库会话
        """
        if not hasattr(self.thread_local, 'session') or not self.thread_local.session:
            if self.db_manager:
                try:
                    self.thread_local.session = self.db_manager.get_session()
                    logger.debug(f"线程 {threading.current_thread().name} 获取新的数据库会话")
                except (OSError, RuntimeError) as e:
                    logger.warning(f"线程 {threading.current_thread().name} 获取数据库会话失败: {e}")
                    self.thread_local.session = None
            else:
                self.thread_local.session = None
        return self.thread_local.session
    
    def get_stock_list(self) -> List[str]:
        """
        从通达信数据源获取所有股票代码列表
        
        Returns:
            List[str]: 股票代码列表 (格式: 600000.SH, 000001.SZ 等)
        """
        stock_codes = []
        
        try:
            if not self.tdx_data_path.exists():
                logger.warning(f"通达信数据路径不存在: {self.tdx_data_path}")
                return stock_codes
            
            sh_lday = self.tdx_data_path / 'sh' / 'lday'
            sz_lday = self.tdx_data_path / 'sz' / 'lday'
            bj_lday = self.tdx_data_path / 'bj' / 'lday'
            
            def is_valid_stock_code(code):
                """验证股票代码是否有效"""
                if len(code) != 6:
                    return False
                if not code.isdigit():
                    return False
                # 忽略 B 股：900 开头（沪市 B 股）、200 开头（深市 B 股）
                if code.startswith('900') or code.startswith('200'):
                    return False
                # 沪市指数：000001-000999
                if code.startswith('000'):
                    return True
                # 沪市股票：600000-603999, 688000-688999
                elif code.startswith('6'):
                    num = int(code)
                    return (600000 <= num <= 603999) or (688000 <= num <= 688999)
                # 深市指数：399001-399999
                elif code.startswith('399'):
                    return True
                # 深市股票：000000-002999, 300000-300999
                elif code.startswith('0') or code.startswith('3'):
                    num = int(code)
                    return (0 <= num <= 2999) or (300000 <= num <= 300999)
                # 北交所：800000-899999（原新三板精选层）
                elif code.startswith('8'):
                    num = int(code)
                    return 800000 <= num <= 899999
                # 北交所新股：920000-920999
                elif code.startswith('92'):
                    num = int(code)
                    return 920000 <= num <= 920999
                # 上海 ETF：510xxx, 511xxx, 512xxx, 513xxx, 515xxx, 588xxx
                elif code.startswith('51') or code.startswith('58'):
                    return True
                # 深圳 ETF：150xxx, 151xxx, 152xxx, 153xxx, 159xxx, 160xxx, 161xxx
                elif code.startswith('15') or code.startswith('16'):
                    return True
                return False
            
            if sh_lday.exists():
                sh_files = list(sh_lday.glob('sh*.day'))
                for f in sh_files:
                    code = f.stem[2:]
                    if is_valid_stock_code(code):
                        stock_codes.append(f"{code}.SH")
                logger.info(f"从沪市通达信数据中找到 {len([c for c in stock_codes if c.endswith('.SH')])} 个有效股票文件")
            
            if sz_lday.exists():
                sz_files = list(sz_lday.glob('sz*.day'))
                for f in sz_files:
                    code = f.stem[2:]
                    if is_valid_stock_code(code):
                        stock_codes.append(f"{code}.SZ")
                logger.info(f"从深市通达信数据中找到 {len([c for c in stock_codes if c.endswith('.SZ')])} 个有效股票文件")
            
            # 北市（科创板）
            if bj_lday.exists():
                bj_files = list(bj_lday.glob('bj*.day'))
                for f in bj_files:
                    code = f.stem[2:]
                    if is_valid_stock_code(code):
                        stock_codes.append(f"{code}.BJ")
                logger.info(f"从北市通达信数据中找到 {len([c for c in stock_codes if c.endswith('.BJ')])} 个有效股票文件")
            
            logger.info(f"通达信股票代码列表获取完成，共 {len(stock_codes)} 只有效股票")
            
        except Exception as e:
            logger.exception(f"获取通达信股票代码列表失败: {e}")
        
        return stock_codes
    
    def parse_day_file(self, file_path: Path, max_days: int = None):
        """
        解析通达信日线数据文件
        
        Args:
            file_path: 日线数据文件路径
            max_days: 最大天数，只解析最近的max_days天数据，None表示解析所有数据
            
        Returns:
            polars.DataFrame: 解析后的日线数据
        """
        try:
            logger.info(f"开始解析通达信日线数据文件: {file_path}，max_days: {max_days}")
            df = parse_tdx_day_file(file_path, max_days)
            logger.info(f"成功解析通达信日线数据文件: {file_path}，获取{len(df)}条数据")
            return df
        except (OSError, IOError) as e:
            logger.exception(f"解析通达信日线数据文件失败: {e}")
            raise
            
    def _process_single_stock(self, file_path: Path, ts_code: str):
        """
        处理单只股票的数据导入
        
        Args:
            file_path: 股票数据文件路径
            ts_code: 股票代码
            
        Returns:
            Dict[str, Any]: 处理结果
        """
        try:
            logger.info(f"线程 {threading.current_thread().name} 正在处理股票 {ts_code} 的数据")
            
            # 解析数据文件
            df = self.parse_day_file(file_path)
            
            if df.empty:
                logger.warning(f"股票 {ts_code} 没有数据")
                return {"code": ts_code, "success": False, "message": "没有数据"}
            
            # 离线模式下，只返回数据，不存储
            if self.offline_mode:
                return {"code": ts_code, "success": True, "data": df}
            
            # 在线模式下，存储到数据库
            session = self._get_thread_safe_session()
            if not session:
                logger.error(f"线程 {threading.current_thread().name} 无法获取数据库会话，跳过股票 {ts_code} 的数据存储")
                return {"code": ts_code, "success": False, "message": "无法获取数据库会话"}
            
            # TODO: 实现数据存储逻辑
            # 从股票代码中提取symbol和market
            symbol, market = ts_code.split('.')
            
            # 遍历数据，存储到数据库
            for _, row in df.iterrows():
                try:
                    # TODO: 实现数据库存储逻辑
                    # 示例：
                    # from src.database.models.stock import StockDaily
                    # daily_data = StockDaily(
                    #     ts_code=ts_code,
                    #     trade_date=row['date'],
                    #     open=row['open'],
                    #     high=row['high'],
                    #     low=row['low'],
                    #     close=row['close'],
                    #     volume=row['volume'],
                    #     amount=row['amount']
                    # )
                    # session.add(daily_data)
                    pass
                except (ValueError, TypeError) as row_e:
                    logger.exception(f"线程 {threading.current_thread().name} 处理股票 {ts_code} 的数据行失败: {row_e}")
                    continue
            
            # 提交事务
            session.commit()
            logger.info(f"线程 {threading.current_thread().name} 成功导入股票 {ts_code} 的 {len(df)} 条数据")
            
            return {"code": ts_code, "success": True, "message": f"成功导入 {len(df)} 条数据"}
            
        except (OSError, RuntimeError) as e:
            if 'session' in locals() and session:
                try:
                    session.rollback()
                except (OSError, RuntimeError) as rollback_e:
                    logger.warning(f"线程 {threading.current_thread().name} 回滚事务失败: {rollback_e}")
            logger.exception(f"线程 {threading.current_thread().name} 处理股票 {ts_code} 失败: {e}")
            return {"code": ts_code, "success": False, "message": str(e)}
        finally:
            # 不要关闭会话，由线程本地存储管理
            pass
    
    def parse_minute_file(self, file_path: Path, freq: str = "1min"):
        """
        解析通达信分钟线数据文件
        
        Args:
            file_path: 分钟线数据文件路径
            freq: 周期，1min, 5min, 15min, 30min, 60min
            
        Returns:
            polars.DataFrame: 解析后的分钟线数据
        """
        try:
            logger.info(f"开始解析通达信{freq}数据文件: {file_path}")
            
            # TODO: 实现通达信分钟线数据文件解析逻辑
            
        except (OSError, IOError) as e:
            logger.exception(f"解析通达信{freq}数据文件失败: {e}")
            raise
    
    def import_stock_data(self, ts_code: str = None):
        """
        导入通达信股票数据，支持并行处理多只股票
        
        Args:
            ts_code: 股票代码，None表示导入所有股票数据
            
        Returns:
            dict: 导入结果，股票代码到数据的映射（离线模式下）
        """
        try:
            logger.info(f"开始导入通达信股票数据，股票代码: {ts_code}")
            
            # 构建通达信日线数据目录路径
            lday_path = self.tdx_data_path / 'sh' / 'lday'  # 沪市日线数据目录
            
            # 检查目录是否存在
            if not lday_path.exists():
                logger.warning(f"通达信沪市日线数据目录不存在: {lday_path}")
                lday_path = self.tdx_data_path / 'sz' / 'lday'  # 深市日线数据目录
                if not lday_path.exists():
                    logger.error(f"通达信深市日线数据目录不存在: {lday_path}")
                    return
            
            # 获取所有日线数据文件
            sh_stock_files = list(Path(self.tdx_data_path / 'sh' / 'lday').glob('sh*.day')) if (self.tdx_data_path / 'sh' / 'lday').exists() else []
            sz_stock_files = list(Path(self.tdx_data_path / 'sz' / 'lday').glob('sz*.day')) if (self.tdx_data_path / 'sz' / 'lday').exists() else []
            bj_stock_files = list(Path(self.tdx_data_path / 'bj' / 'lday').glob('bj*.day')) if (self.tdx_data_path / 'bj' / 'lday').exists() else []
            all_stock_files = sh_stock_files + sz_stock_files + bj_stock_files
            
            logger.info(f"找到{len(all_stock_files)}个通达信股票数据文件")
            
            # 构建股票代码到文件路径的映射
            stock_file_map = {}
            for file_path in all_stock_files:
                # 提取股票代码，文件名格式：sh600000.day -> 600000.SH
                file_name = file_path.stem
                if file_name.startswith('sh'):
                    ts_code_formatted = f"{file_name[2:]}.SH"
                elif file_name.startswith('sz'):
                    ts_code_formatted = f"{file_name[2:]}.SZ"
                elif file_name.startswith('bj'):
                    ts_code_formatted = f"{file_name[2:]}.BJ"
                else:
                    continue
                
                # 如果指定了股票代码，只处理指定的股票
                if ts_code and ts_code_formatted != ts_code:
                    continue
                
                stock_file_map[ts_code_formatted] = file_path
            
            logger.info(f"共找到{len(stock_file_map)}只需要处理的股票")
            
            if not stock_file_map:
                logger.warning("没有需要处理的股票")
                return
            
            # 存储结果
            result = {}
            
            # 提交所有任务到线程池
            futures = {}
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                for ts_code_formatted, file_path in stock_file_map.items():
                    future = executor.submit(self._process_single_stock, file_path, ts_code_formatted)
                    futures[future] = ts_code_formatted
            
            # 收集结果
            for future in as_completed(futures):
                ts_code_formatted = futures[future]
                try:
                    res = future.result()
                    if res["success"]:
                        if self.offline_mode:
                            result[ts_code_formatted] = res.get("data", {})
                        logger.info(f"股票 {ts_code_formatted} 数据导入成功")
                    else:
                        logger.warning(f"股票 {ts_code_formatted} 数据导入失败: {res.get('message', '未知错误')}")
                except (OSError, RuntimeError) as e:
                    logger.exception(f"处理股票 {ts_code_formatted} 的结果时发生异常: {e}")

            logger.info(f"所有股票数据导入完成，成功 {sum(1 for res in futures.values() if res['success'])} 只，失败 {sum(1 for res in futures.values() if not res['success'])} 只")

            # 离线模式下返回结果
            if self.offline_mode:
                return result
            
        except (OSError, RuntimeError) as e:
            logger.exception(f"导入通达信股票数据失败: {e}")
            raise
    
    def _get_adj_factors(self, ts_code: str, start_date: date, end_date: date) -> pl.DataFrame:
        """
        从数据库获取复权因子
        如果没有预计算的复权因子，则根据分红数据实时计算
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            Polars DataFrame包含复权因子
        """
        try:
            from src.database.models.stock import StockAdjFactor
            
            session = self._get_thread_safe_session()
            if not session:
                logger.warning(f"无法获取数据库会话，无法获取复权因子")
                return None
            
            # 首先尝试获取预计算的复权因子
            query = session.query(StockAdjFactor).filter_by(ts_code=ts_code)
            
            if start_date:
                query = query.filter(StockAdjFactor.trade_date >= start_date)
            if end_date:
                query = query.filter(StockAdjFactor.trade_date <= end_date)
            
            factors = query.order_by(StockAdjFactor.trade_date).all()
            
            if factors:
                # 使用预计算的复权因子
                logger.debug(f"{ts_code} 使用预计算的复权因子数据")
                data = {
                    'trade_date': [f.trade_date for f in factors],
                    'qfq_factor': [f.qfq_factor for f in factors],
                    'hfq_factor': [f.hfq_factor for f in factors],
                }
                return pl.DataFrame(data)
            
            # 如果没有预计算的复权因子，根据分红数据实时计算
            logger.info(f"{ts_code} 没有预计算的复权因子，根据分红数据实时计算")
            return self._calculate_adj_factors_from_dividends(ts_code, start_date, end_date)
            
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"获取复权因子失败: {e}")
            return None
    
    def _calculate_adj_factors_from_dividends(self, ts_code: str, start_date: date, end_date: date) -> pl.DataFrame:
        """
        根据分红数据实时计算复权因子
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            Polars DataFrame包含复权因子
        """
        try:
            from src.database.models.stock import StockDividend
            
            session = self._get_thread_safe_session()
            if not session:
                return None
            
            # 获取分红数据（获取所有历史分红数据，不限制日期范围）
            query = session.query(StockDividend).filter_by(ts_code=ts_code)
            query = query.filter(StockDividend.ex_date != None)
            
            # 注意：这里不限制日期范围，因为复权因子计算需要所有历史分红数据
            # 即使当前显示的K线范围内没有分红，历史分红也会影响当前价格的复权计算
            
            dividends = query.order_by(StockDividend.ex_date).all()
            
            if not dividends:
                logger.debug(f"{ts_code} 没有找到分红数据")
                return None
            
            logger.info(f"{ts_code} 找到 {len(dividends)} 条分红记录，开始计算复权因子")
            
            # 打印第一条和最后一条分红记录用于调试
            if dividends:
                first_div = dividends[0]
                last_div = dividends[-1]
                logger.info(f"第一条分红: 日期={first_div.ex_date}, 现金={first_div.cash_div}, 送转={first_div.share_div}")
                logger.info(f"最后一条分红: 日期={last_div.ex_date}, 现金={last_div.cash_div}, 送转={last_div.share_div}")
            
            # 获取价格数据用于计算复权因子
            symbol = ts_code.split('.')[0]
            if ts_code.endswith('.SH'):
                market = 'sh'
            elif ts_code.endswith('.BJ'):
                market = 'bj'
            else:
                market = 'sz'
            file_name = f"{market}{symbol}.day"
            file_path = self.tdx_data_path / market / "lday" / file_name
            
            if not file_path.exists():
                logger.warning(f"价格数据文件不存在: {file_path}")
                return None
            
            # 读取价格数据（复用统一的 .day 解析器，只需日期与收盘价）
            prices = [
                {'trade_date': r['date'], 'close': r['close']}
                for r in read_recent_records(file_path)
            ]

            if not prices:
                return None

            # 按日期排序
            prices.sort(key=lambda x: x['trade_date'])
            
            # 计算复权因子
            # 前复权因子：从后向前累乘
            # 后复权因子：从前向后累乘
            
            # 初始化所有日期的因子为1.0
            factors_dict = {p['trade_date']: {'qfq_factor': 1.0, 'hfq_factor': 1.0} for p in prices}
            
            # 计算前复权因子（从后向前）
            for div in reversed(dividends):
                ex_date = div.ex_date
                cash_div = div.cash_div or 0
                share_div = div.share_div or 0
                
                if cash_div == 0 and share_div == 0:
                    continue
                
                # 找到除权除息日前一天的收盘价
                prev_prices = [p for p in prices if p['trade_date'] < ex_date]
                if not prev_prices:
                    continue
                
                prev_close = prev_prices[-1]['close']
                if prev_close <= 0:
                    continue
                
                # 计算复权因子
                # 前复权因子 = (前收盘价 - 每股现金分红) / (前收盘价 * (1 + 每股送转股))
                if prev_close <= cash_div:
                    logger.warning(f"{ts_code} {ex_date} 现金分红异常，跳过")
                    continue
                
                factor = (prev_close - cash_div) / (prev_close * (1 + share_div))
                if factor <= 0:
                    continue
                
                # 应用到所有早于除权除息日的日期
                for p in prices:
                    if p['trade_date'] < ex_date:
                        factors_dict[p['trade_date']]['qfq_factor'] *= factor
            
            # 计算后复权因子（从前向后）
            for div in dividends:
                ex_date = div.ex_date
                cash_div = div.cash_div or 0
                share_div = div.share_div or 0
                
                if cash_div == 0 and share_div == 0:
                    continue
                
                # 找到除权除息日前一天的收盘价
                prev_prices = [p for p in prices if p['trade_date'] < ex_date]
                if not prev_prices:
                    continue
                
                prev_close = prev_prices[-1]['close']
                if prev_close <= 0:
                    continue
                
                # 计算复权因子
                if prev_close <= cash_div:
                    continue
                
                qfq_factor = (prev_close - cash_div) / (prev_close * (1 + share_div))
                if qfq_factor <= 0:
                    continue
                
                hfq_factor = 1 / qfq_factor
                
                # 应用到所有晚于或等于除权除息日的日期
                for p in prices:
                    if p['trade_date'] >= ex_date:
                        factors_dict[p['trade_date']]['hfq_factor'] *= hfq_factor
            
            # 转换为DataFrame
            data = {
                'trade_date': list(factors_dict.keys()),
                'qfq_factor': [f['qfq_factor'] for f in factors_dict.values()],
                'hfq_factor': [f['hfq_factor'] for f in factors_dict.values()],
            }
            
            df = pl.DataFrame(data)
            
            # 打印复权因子统计信息用于调试
            if len(df) > 0:
                min_qfq = df['qfq_factor'].min()
                max_qfq = df['qfq_factor'].max()
                min_hfq = df['hfq_factor'].min()
                max_hfq = df['hfq_factor'].max()
                logger.info(f"{ts_code} 复权因子统计: 前复权=[{min_qfq:.6f}, {max_qfq:.6f}], 后复权=[{min_hfq:.6f}, {max_hfq:.6f}]")
            
            logger.info(f"{ts_code} 复权因子计算完成，共 {len(df)} 条记录")
            return df
            
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"根据分红数据计算复权因子失败: {e}")
            return None
    
    def _apply_adj_factor(self, df: pl.DataFrame, factors_df: pl.DataFrame, adj_type: str = 'qfq') -> pl.DataFrame:
        """
        应用复权因子计算复权价格
        
        Args:
            df: 原始价格DataFrame
            factors_df: 复权因子DataFrame (Polars)
            adj_type: 复权类型 ('qfq'前复权, 'hfq'后复权)
            
        Returns:
            包含复权价格的DataFrame
        """
        try:
            if factors_df is None or factors_df.is_empty():
                # 没有复权因子，返回原始价格并添加默认因子1.0
                return df.with_columns([
                    pl.lit(1.0).alias(f'{adj_type}_factor'),
                    pl.col('open').alias(f'{adj_type}_open'),
                    pl.col('high').alias(f'{adj_type}_high'),
                    pl.col('low').alias(f'{adj_type}_low'),
                    pl.col('close').alias(f'{adj_type}_close'),
                ])
            
            # 直接使用Polars DataFrame
            factors_pl = factors_df
            
            # 合并数据
            result_df = df.join(factors_pl, left_on='date', right_on='trade_date', how='left')
            
            # 填充缺失的复权因子
            # 使用向前填充（ffill）策略：如果某天没有复权因子，使用最近的有效因子
            # 这样可以避免新数据因为没有复权因子而显示为原始价格
            factor_col = f'{adj_type}_factor'
            result_df = result_df.with_columns([
                pl.col(factor_col).fill_null(strategy='forward').alias(factor_col)
            ])
            
            # 如果还有缺失（说明前面没有有效因子），则填充为1.0
            result_df = result_df.with_columns([
                pl.col(factor_col).fill_null(1.0)
            ])
            
            # 计算复权价格
            result_df = result_df.with_columns([
                (pl.col('open') * pl.col(factor_col)).alias(f'{adj_type}_open'),
                (pl.col('high') * pl.col(factor_col)).alias(f'{adj_type}_high'),
                (pl.col('low') * pl.col(factor_col)).alias(f'{adj_type}_low'),
                (pl.col('close') * pl.col(factor_col)).alias(f'{adj_type}_close'),
            ])
            
            return result_df
            
        except (ValueError, TypeError) as e:
            logger.exception(f"应用复权因子失败: {e}")
            return df
    
    def get_kline_data(self, stock_code: str, start_date: str, end_date: str, adjust: str = "none"):
        """
        获取指定股票在指定日期范围内的K线数据，支持复权
        
        Args:
            stock_code: 股票代码，如"600000"或"sh.600000"
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            adjust: 复权类型，"qfq"=前复权，"hfq"=后复权，"none"=不复权（默认）
        
        Returns:
            pl.LazyFrame: K线数据的LazyFrame，包含date, open, high, low, close, volume, amount字段
                           如果指定了adjust，还会包含复权因子（qfq_factor/hfq_factor），
                           复权价由调用方实时计算
        """
        try:
            logger.info(f"开始获取股票 {stock_code} 在 {start_date} 到 {end_date} 期间的K线数据")
            
            # 处理不同格式的股票代码
            if '.' in stock_code:
                # 格式如sh.600000或600000.SH
                if stock_code.count('.') == 1:
                    market_part, code_part = stock_code.split('.')
                    # 处理sh.600000格式
                    if market_part in ['sh', 'sz', 'bj']:
                        market = market_part
                        code = code_part
                    # 处理600000.SH格式
                    elif code_part in ['SH', 'SZ', 'BJ']:
                        # 特殊处理920开头的股票，即使后缀是.SZ，也识别为京市股票
                        if market_part.startswith('92') and len(market_part) == 6:
                            market = 'bj'
                            code = market_part
                        else:
                            market = code_part.lower()
                            code = market_part
                    else:
                        logger.warning(f"无效的股票代码格式: {stock_code}")
                        return pl.LazyFrame({})
                else:
                    logger.warning(f"无效的股票代码格式: {stock_code}")
                    return pl.LazyFrame({})
            else:
                # 纯数字格式，如600000
                if stock_code.startswith("6"):
                    market = "sh"
                elif stock_code.startswith("92") and len(stock_code) == 6:
                    # 京市个股（北交所，92开头）
                    market = "bj"
                else:
                    market = "sz"
                code = stock_code
            
            logger.info(f"解析后的股票代码: {code}，市场: {market}")
            
            # 构建通达信数据文件名
            file_name = f"{market}{code}.day"
            
            # 构建数据文件路径
            lday_path = self.tdx_data_path / market / "lday"
            file_path = lday_path / file_name
            
            logger.info(f"尝试从路径 {file_path} 读取数据")
            
            # 检查文件是否存在
            if not file_path.exists():
                logger.warning(f"股票 {stock_code} 的通达信数据文件不存在: {file_path}")
                return pl.LazyFrame({})
            
            # 复用统一的 .day 解析器（成交额元、成交量手）
            logger.info(f"开始解析文件 {file_path}")
            try:
                data = read_recent_records(file_path)
            except (OSError, IOError) as file_e:
                logger.exception(f"文件操作失败: {file_e}")
                return pl.LazyFrame({})

            logger.info(f"成功解析{len(data)}条数据")
            if not data:
                return pl.LazyFrame({})
            
            # 转换为polars DataFrame
            df = pl.DataFrame(data)
            
            # 转换为LazyFrame
            lazy_df = df.lazy()
            
            # 转换start_date和end_date为date对象
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
            
            # 使用Lazy API过滤日期范围内的数据
            lazy_df = lazy_df.filter(
                (pl.col('date') >= start_dt) & (pl.col('date') <= end_dt)
            )
            
            # 计算复权价格（无论是否需要复权，都计算所有复权类型）
            logger.info(f"计算复权价格...")
            
            # 构建ts_code
            ts_code = f"{code}.{market.upper()}"
            
            # 获取复权因子（使用完整日期范围，因为复权因子计算需要所有历史数据）
            # 传入None作为日期范围，让_get_adj_factors获取所有历史数据
            factors_df = self._get_adj_factors(ts_code, None, None)
            
            if factors_df is not None and not factors_df.is_empty():
                # 转换复权因子为LazyFrame
                factors_lazy = factors_df.lazy()
                
                # 使用Lazy API进行复权计算
                lazy_df = lazy_df.join(factors_lazy, left_on='date', right_on='trade_date', how='left')
                
                # 填充缺失的复权因子
                for adj_type in ['qfq', 'hfq']:
                    factor_col = f'{adj_type}_factor'
                    lazy_df = lazy_df.with_columns([
                        pl.col(factor_col).fill_null(strategy='forward').fill_null(1.0).alias(factor_col)
                    ])
                
                # 计算所有复权类型的价格
                for adj_type in ['qfq', 'hfq']:
                    factor_col = f'{adj_type}_factor'
                    lazy_df = lazy_df.with_columns([
                        (pl.col('open') * pl.col(factor_col)).alias(f'{adj_type}_open'),
                        (pl.col('high') * pl.col(factor_col)).alias(f'{adj_type}_high'),
                        (pl.col('low') * pl.col(factor_col)).alias(f'{adj_type}_low'),
                        (pl.col('close') * pl.col(factor_col)).alias(f'{adj_type}_close'),
                    ])
                
                # 添加调试日志
                logger.info(f"{ts_code} 复权价格计算完成，复权因子已应用到价格数据")
            else:
                logger.warning(f"{ts_code} 没有获取到复权因子，使用原始价格并添加默认复权因子")
                # 添加默认复权因子（值为1.0），保证数据结构一致性
                for adj_type in ['qfq', 'hfq']:
                    factor_col = f'{adj_type}_factor'
                    price_col = f'{adj_type}_'
                    lazy_df = lazy_df.with_columns([
                        pl.lit(1.0).alias(factor_col),
                        pl.col('open').alias(f'{price_col}open'),
                        pl.col('high').alias(f'{price_col}high'),
                        pl.col('low').alias(f'{price_col}low'),
                        pl.col('close').alias(f'{price_col}close'),
                    ])
            
            logger.info(f"复权价格计算完成")
            
            logger.info(f"成功获取股票 {stock_code} 的K线数据，返回LazyFrame")
            return lazy_df
            
        except (OSError, IOError, ValueError) as e:
            logger.exception(f"获取股票 {stock_code} 的K线数据失败: {e}")
            return pl.LazyFrame({})

    # ------------------------------------------------------------------
    # 标准 DataSource 接口（统一列名/单位/复权口径，供 DataFetcher 调用）
    # ------------------------------------------------------------------

    def get_stock_data(self, ts_code: str, start_date: str, end_date: str,
                       freq: str = 'daily', adjustment_type: str = 'qfq'):
        """
        标准接口：获取股票日线数据，返回统一格式（原始价 + 复权因子）。

        复用 get_kline_data() 的本地文件解析 + 复权因子计算，
        补齐 pre_close/change/pct_chg 衍生列后归一化。

        Returns:
            pl.DataFrame: 归一化后的K线数据
        """
        try:
            # 多取一段前置数据，保证区间首根K线能拿到真实的昨收
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            fetch_start = (start_dt - timedelta(days=30)).strftime('%Y-%m-%d')

            # get_kline_data 返回 LazyFrame，含 date/open/high/low/close/volume/amount
            # 以及 qfq_factor/hfq_factor 列
            lazy_df = self.get_kline_data(ts_code, fetch_start, end_date, adjust='none')
            if lazy_df is None:
                return pl.DataFrame()

            df = lazy_df.collect()
            if df.is_empty():
                return pl.DataFrame()

            df = df.sort('date')

            # 昨收按交易所口径推导：除权日需用复权因子比值还原除权参考价
            #   pre_close_t = close_{t-1} * qfq_factor_{t-1} / qfq_factor_t
            # 非除权日因子相同，退化为前一日收盘价
            prev_close = pl.col('close').shift(1)
            if 'qfq_factor' in df.columns:
                pre_close_expr = prev_close * (
                    pl.col('qfq_factor').shift(1) / pl.col('qfq_factor'))
            else:
                pre_close_expr = prev_close

            df = df.with_columns(pre_close_expr.alias('pre_close'))
            df = df.with_columns([
                (pl.col('close') - pl.col('pre_close')).alias('change'),
                pl.when(pl.col('pre_close').is_not_null() & (pl.col('pre_close') != 0))
                .then((pl.col('close') / pl.col('pre_close') - 1) * 100)
                .otherwise(None).alias('pct_chg'),
            ])

            # 丢弃为补齐昨收而多取的前置数据
            df = df.filter(
                (pl.col('date') >= start_dt) & (pl.col('date') <= end_dt))

            # tdx 列名已为标准名，成交量单位为"手"
            column_map = {
                'date': 'date', 'open': 'open', 'high': 'high', 'low': 'low',
                'close': 'close', 'pre_close': 'pre_close', 'volume': 'volume',
                'amount': 'amount', 'pct_chg': 'pct_chg', 'change': 'change',
                'qfq_factor': 'qfq_factor', 'hfq_factor': 'hfq_factor',
            }
            return self._normalize_kline(
                df, column_map=column_map, ts_code=ts_code,
                volume_unit='hand', date_format=None, include_factors=True,
            )

        except Exception as e:
            logger.warning(f"tdx get_stock_data({ts_code}) 失败: {e}")
            return pl.DataFrame()

    def get_index_data(self, ts_code: str, start_date: str, end_date: str,
                       freq: str = 'daily'):
        """
        标准接口：获取指数日线数据，返回统一格式。

        指数无复权因子，复用 get_kline_data 解析本地文件。

        Returns:
            pl.DataFrame: 归一化后的指数K线数据
        """
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            fetch_start = (start_dt - timedelta(days=30)).strftime('%Y-%m-%d')

            lazy_df = self.get_kline_data(ts_code, fetch_start, end_date, adjust='none')
            if lazy_df is None:
                return pl.DataFrame()

            df = lazy_df.collect()
            if df.is_empty():
                return pl.DataFrame()

            df = df.sort('date')

            # 指数无除权，昨收即前一日收盘；多取的前置数据用于补齐区间首行
            df = df.with_columns([
                pl.col('close').shift(1).alias('pre_close'),
            ])
            df = df.with_columns([
                (pl.col('close') - pl.col('pre_close')).alias('change'),
                pl.when(pl.col('pre_close').is_not_null() & (pl.col('pre_close') != 0))
                .then((pl.col('close') / pl.col('pre_close') - 1) * 100)
                .otherwise(None).alias('pct_chg'),
            ])
            df = df.filter(
                (pl.col('date') >= start_dt) & (pl.col('date') <= end_dt))

            column_map = {
                'date': 'date', 'open': 'open', 'high': 'high', 'low': 'low',
                'close': 'close', 'pre_close': 'pre_close', 'volume': 'volume',
                'amount': 'amount', 'pct_chg': 'pct_chg', 'change': 'change',
            }
            return self._normalize_kline(
                df, column_map=column_map, ts_code=ts_code,
                volume_unit='hand', date_format=None, include_factors=True,
            )

        except Exception as e:
            logger.warning(f"tdx get_index_data({ts_code}) 失败: {e}")
            return pl.DataFrame()
