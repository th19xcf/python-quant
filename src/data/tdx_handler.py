#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
通达信数据处理器
"""

import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any

import polars as pl
from loguru import logger


class TdxHandler:
    """
    通达信数据处理器，负责解析通达信数据文件并存储到数据库
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
        self.tdx_data_path = Path(config.data.tdx_data_path)
        
        # 检查通达信数据路径是否存在
        if not self.tdx_data_path.exists():
            logger.warning(f"通达信数据路径不存在: {self.tdx_data_path}")
        
        # 初始化线程池，使用配置中的最大工作线程数
        self.max_workers = self.config.data.max_workers
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        logger.info(f"TdxHandler线程池初始化完成，最大工作线程数: {self.max_workers}")
        
        # 线程本地存储，用于保存每个线程的数据库会话
        self.thread_local = threading.local()
        
        # 离线模式支持
        self.offline_mode = db_manager is None
        
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
                except Exception as e:
                    logger.warning(f"线程 {threading.current_thread().name} 获取数据库会话失败: {e}")
                    self.thread_local.session = None
            else:
                self.thread_local.session = None
        return self.thread_local.session
    
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
            
            # 通达信日线数据文件格式：每个交易日数据占32字节
            # 字段顺序：日期(4字节)、开盘价(4字节)、最高价(4字节)、最低价(4字节)、收盘价(4字节)、成交量(4字节)、成交额(4字节)
            # 价格单位：元，成交量单位：手，成交额单位：元
            
            data = []
            with open(file_path, 'rb') as f:
                # 获取文件大小
                f.seek(0, 2)
                file_size = f.tell()
                
                # 计算数据条数
                record_count = file_size // 32
                logger.info(f"文件{file_path}包含{record_count}条交易日数据")
                
                # 如果指定了max_days，只解析最近的max_days条记录
                if max_days is not None and max_days > 0:
                    start_record = max(0, record_count - max_days)
                    logger.info(f"只解析最近{max_days}条记录，从第{start_record}条开始")
                    # 跳转到起始记录位置
                    f.seek(start_record * 32)
                else:
                    # 从头开始解析
                    f.seek(0)
                
                # 读取剩余数据
                file_content = f.read()
                
                # 计算需要解析的记录数
                parse_count = len(file_content) // 32
                logger.info(f"需要解析{parse_count}条记录")
                
                # 解析每条记录
                for i in range(parse_count):
                    # 提取32字节数据
                    record = file_content[i*32:(i+1)*32]
                    
                    # 解析字段
                    date_int = struct.unpack('I', record[0:4])[0]  # 日期，格式：YYYYMMDD
                    open_val = struct.unpack('I', record[4:8])[0] / 100  # 开盘价，转换为元
                    high_val = struct.unpack('I', record[8:12])[0] / 100  # 最高价，转换为元
                    low_val = struct.unpack('I', record[12:16])[0] / 100  # 最低价，转换为元
                    close_val = struct.unpack('I', record[16:20])[0] / 100  # 收盘价，转换为元
                    volume = struct.unpack('I', record[20:24])[0]  # 成交量，单位：手
                    amount = struct.unpack('I', record[24:28])[0]  # 成交额，单位：元
                    
                    # 转换日期格式
                    date_str = str(date_int)
                    date = datetime.strptime(date_str, '%Y%m%d').date()
                    
                    # 添加到数据列表
                    data.append({
                        'date': date,
                        'open': open_val,
                        'high': high_val,
                        'low': low_val,
                        'close': close_val,
                        'volume': volume,
                        'amount': amount
                    })
            
            # 转换为polars DataFrame
            df = pl.DataFrame(data)
            logger.info(f"成功解析通达信日线数据文件: {file_path}，获取{len(df)}条数据")
            return df
            
        except Exception as e:
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
                except Exception as row_e:
                    logger.exception(f"线程 {threading.current_thread().name} 处理股票 {ts_code} 的数据行失败: {row_e}")
                    continue
            
            # 提交事务
            session.commit()
            logger.info(f"线程 {threading.current_thread().name} 成功导入股票 {ts_code} 的 {len(df)} 条数据")
            
            return {"code": ts_code, "success": True, "message": f"成功导入 {len(df)} 条数据"}
            
        except Exception as e:
            if 'session' in locals() and session:
                try:
                    session.rollback()
                except Exception as rollback_e:
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
            
        except Exception as e:
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
            all_stock_files = sh_stock_files + sz_stock_files
            
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
                except Exception as e:
                    logger.exception(f"处理股票 {ts_code_formatted} 的结果时发生异常: {e}")
            
            logger.info(f"所有股票数据导入完成，成功 {sum(1 for res in futures.values() if res['success'])} 只，失败 {sum(1 for res in futures.values() if not res['success'])} 只")
            
            # 离线模式下返回结果
            if self.offline_mode:
                return result
            
        except Exception as e:
            logger.exception(f"导入通达信股票数据失败: {e}")
            raise
    
    def get_kline_data(self, stock_code: str, start_date: str, end_date: str):
        """
        获取指定股票在指定日期范围内的K线数据
        
        Args:
            stock_code: 股票代码，如"600000"或"sh.600000"
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            
        Returns:
            List[Dict[str, Any]]: K线数据列表，每个元素包含date, open, high, low, close, volume, amount字段
        """
        try:
            logger.info(f"开始获取股票 {stock_code} 在 {start_date} 到 {end_date} 期间的K线数据")
            
            # 处理不同格式的股票代码
            if '.' in stock_code:
                # 格式如sh.600000或600000.SH
                if stock_code.count('.') == 1:
                    market_part, code_part = stock_code.split('.')
                    # 处理sh.600000格式
                    if market_part in ['sh', 'sz']:
                        market = market_part
                        code = code_part
                    # 处理600000.SH格式
                    elif code_part in ['SH', 'SZ']:
                        market = code_part.lower()
                        code = market_part
                    else:
                        logger.warning(f"无效的股票代码格式: {stock_code}")
                        return None
                else:
                    logger.warning(f"无效的股票代码格式: {stock_code}")
                    return None
            else:
                # 纯数字格式，如600000
                market = "sh" if stock_code.startswith("6") else "sz"
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
                return None
            
            # 先尝试直接解析数据，不使用pandas
            logger.info(f"开始直接解析文件 {file_path}")
            
            # 添加超时机制，避免程序无响应
            start_time = time.time()
            
            data = []
            try:
                with open(file_path, 'rb') as f:
                    # 获取文件大小
                    logger.info(f"正在获取文件大小")
                    f.seek(0, 2)
                    file_size = f.tell()
                    logger.info(f"文件大小: {file_size} 字节")
                    
                    # 计算数据条数
                    record_count = file_size // 32
                    logger.info(f"文件{file_path}包含{record_count}条交易日数据")
                    
                    # 读取所有记录，由绘图层根据displayed_bar_count截取显示的数据
                    # 这样可以支持动态调整柱体数量而无需重新读取数据文件
                    max_days = None
                    start_record = 0
                    logger.info(f"只解析最近{max_days}条记录，从第{start_record}条开始")
                    
                    # 跳转到起始记录位置
                    f.seek(start_record * 32)
                    logger.info(f"已跳转到起始记录位置")
                    
                    # 读取剩余数据
                    logger.info(f"开始读取文件内容")
                    file_content = f.read()
                    logger.info(f"文件内容读取完成，大小: {len(file_content)} 字节")
                    
                    # 计算需要解析的记录数
                    parse_count = len(file_content) // 32
                    logger.info(f"需要解析{parse_count}条记录")
                    
                    # 解析每条记录
                    for i in range(parse_count):
                        # 添加超时检查
                        if time.time() - start_time > 5:  # 超过5秒就退出
                            logger.warning(f"解析数据超时，已处理{i}条记录")
                            break
                        
                        # 提取32字节数据
                        record = file_content[i*32:(i+1)*32]
                        
                        # 解析字段
                        date_int = struct.unpack('I', record[0:4])[0]  # 日期，格式：YYYYMMDD
                        open_val = struct.unpack('I', record[4:8])[0] / 100  # 开盘价，转换为元
                        high_val = struct.unpack('I', record[8:12])[0] / 100  # 最高价，转换为元
                        low_val = struct.unpack('I', record[12:16])[0] / 100  # 最低价，转换为元
                        close_val = struct.unpack('I', record[16:20])[0] / 100  # 收盘价，转换为元
                        volume = struct.unpack('I', record[20:24])[0]  # 成交量，单位：手
                        amount = struct.unpack('I', record[24:28])[0]  # 成交额，单位：元
                        
                        # 转换日期格式
                        date_str = str(date_int)
                        date = datetime.strptime(date_str, '%Y%m%d').date()
                        
                        # 添加到数据列表
                        data.append({
                            'date': date,
                            'open': open_val,
                            'high': high_val,
                            'low': low_val,
                            'close': close_val,
                            'volume': volume,
                            'amount': amount
                        })
            except Exception as file_e:
                logger.exception(f"文件操作失败: {file_e}")
                return None
            
            logger.info(f"成功解析{len(data)}条数据")
            
            # 转换start_date和end_date为date对象
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
            
            # 过滤日期范围内的数据
            filtered_data = [
                item for item in data 
                if start_dt <= item['date'] <= end_dt
            ]
            
            logger.info(f"过滤后的数据条数: {len(filtered_data)}")
            
            if not filtered_data:
                logger.warning(f"股票 {stock_code} 在 {start_date} 到 {end_date} 期间没有数据")
                return None
            
            logger.info(f"成功获取股票 {stock_code} 在 {start_date} 到 {end_date} 期间的 {len(filtered_data)} 条K线数据")
            return filtered_data
            
        except Exception as e:
            logger.exception(f"获取股票 {stock_code} 的K线数据失败: {e}")
            return None
