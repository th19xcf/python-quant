#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
异步处理模块，负责数据读取、指标计算和图表数据准备的异步处理
"""

from PySide6.QtCore import QThread, Signal
from PySide6.QtWidgets import QApplication
import polars as pl
import struct
from datetime import datetime
from pathlib import Path
from loguru import logger


class DataReadThread(QThread):
    """
    数据读取线程类，负责异步读取和解析股票数据文件
    """
    # 定义信号
    data_read_completed = Signal(pl.DataFrame, str, str)  # 数据读取完成信号
    data_read_error = Signal(str)  # 数据读取错误信号
    data_read_progress = Signal(int, int)  # 数据读取进度信号

    def __init__(self, file_path, name, code):
        """
        初始化数据读取线程
        
        Args:
            file_path: 数据文件路径
            name: 股票名称
            code: 股票代码
        """
        super().__init__()
        self.file_path = file_path
        self.name = name
        self.code = code
        self.is_running = True

    def run(self):
        """
        线程运行函数，实现异步数据读取和解析
        """
        try:
            logger.info(f"开始异步读取股票数据文件: {self.file_path}")
            
            # 读取并解析通达信日线数据文件
            data = []
            tdx_file_path = Path(self.file_path)
            
            if not tdx_file_path.exists():
                error_msg = f"找不到股票数据文件: {tdx_file_path}"
                logger.warning(error_msg)
                self.data_read_error.emit(error_msg)
                return
            
            with open(tdx_file_path, 'rb') as f:
                # 获取文件大小
                f.seek(0, 2)
                file_size = f.tell()
                f.seek(0)
                
                # 计算数据条数
                record_count = file_size // 32
                if record_count == 0:
                    error_msg = f"股票数据文件为空: {tdx_file_path}"
                    logger.warning(error_msg)
                    self.data_read_error.emit(error_msg)
                    return
                
                # 读取所有记录
                for i in range(record_count):
                    if not self.is_running:
                        logger.info("数据读取线程被取消")
                        return
                    
                    record = f.read(32)
                    if len(record) < 32:
                        break
                    
                    # 解析记录
                    date_int = struct.unpack('I', record[0:4])[0]  # 日期，格式：YYYYMMDD
                    open_val = struct.unpack('I', record[4:8])[0] / 100  # 开盘价，转换为元
                    high_val = struct.unpack('I', record[8:12])[0] / 100  # 最高价，转换为元
                    low_val = struct.unpack('I', record[12:16])[0] / 100  # 最低价，转换为元
                    close_val = struct.unpack('I', record[16:20])[0] / 100  # 收盘价，转换为元
                    volume = struct.unpack('I', record[20:24])[0]  # 成交量，单位：手
                    amount = struct.unpack('I', record[24:28])[0] / 100  # 成交额，转换为元
                    
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
                    
                    # 发送进度信号
                    if i % 100 == 0 or i == record_count - 1:
                        progress = int((i + 1) / record_count * 100)
                        self.data_read_progress.emit(progress, record_count)
            
            # 将数据转换为Polars DataFrame
            df = pl.DataFrame(data)
            logger.info(f"异步读取到{len(df)}条历史数据")
            
            # 发送数据读取完成信号
            self.data_read_completed.emit(df, self.name, self.code)
            
        except Exception as e:
            error_msg = f"处理股票数据失败: {str(e)}"
            logger.exception(error_msg)
            self.data_read_error.emit(error_msg)
    
    def stop(self):
        """
        停止线程
        """
        self.is_running = False
        self.wait()


class IndicatorCalculateThread(QThread):
    """
    指标计算线程类，负责异步计算技术指标
    """
    # 定义信号
    indicator_calculated = Signal(pl.DataFrame)  # 指标计算完成信号
    indicator_calculate_error = Signal(str)  # 指标计算错误信号
    indicator_calculate_progress = Signal(int, int)  # 指标计算进度信号

    def __init__(self, df):
        """
        初始化指标计算线程
        
        Args:
            df: 股票数据Polars DataFrame
        """
        super().__init__()
        self.df = df
        self.is_running = True

    def run(self):
        """
        线程运行函数，实现异步指标计算
        """
        try:
            logger.info(f"开始异步计算技术指标，数据形状: {self.df.shape}")
            
            # 延迟导入，避免循环导入
            from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
            
            # 创建TechnicalAnalyzer实例
            analyzer = TechnicalAnalyzer(self.df)
            
            # 计算所有技术指标
            analyzer.calculate_all_indicators(parallel=True)
            
            # 获取计算结果
            result_df = analyzer.get_data(return_polars=True)
            
            logger.info(f"异步计算指标完成，结果DataFrame形状: {result_df.shape}")
            
            # 发送指标计算完成信号
            self.indicator_calculated.emit(result_df)
            
        except Exception as e:
            error_msg = f"计算技术指标失败: {str(e)}"
            logger.exception(error_msg)
            self.indicator_calculate_error.emit(error_msg)
    
    def stop(self):
        """
        停止线程
        """
        self.is_running = False
        self.wait()


class ChartDataPrepareThread(QThread):
    """
    图表数据准备线程类，负责异步准备图表绘制数据
    """
    # 定义信号
    chart_data_prepared = Signal(pl.DataFrame, list, list, list, list, list)
    chart_data_prepare_error = Signal(str)
    chart_data_prepare_progress = Signal(int, int)

    def __init__(self, df):
        """
        初始化图表数据准备线程
        
        Args:
            df: 包含所有指标的Polars DataFrame
        """
        super().__init__()
        self.df = df
        self.is_running = True

    def run(self):
        """
        线程运行函数，实现异步图表数据准备
        """
        try:
            logger.info(f"开始异步准备图表数据，数据形状: {self.df.shape}")
            
            # 提取必要的列
            dates = self.df['date'].to_list()
            opens = self.df['open'].to_list()
            highs = self.df['high'].to_list()
            lows = self.df['low'].to_list()
            closes = self.df['close'].to_list()
            
            logger.info(f"异步准备图表数据完成，提取了{len(dates)}个数据点")
            
            # 发送图表数据准备完成信号
            self.chart_data_prepared.emit(self.df, dates, opens, highs, lows, closes)
            
        except Exception as e:
            error_msg = f"准备图表数据失败: {str(e)}"
            logger.exception(error_msg)
            self.chart_data_prepare_error.emit(error_msg)
    
    def stop(self):
        """
        停止线程
        """
        self.is_running = False
        self.wait()
