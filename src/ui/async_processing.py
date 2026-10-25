#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
异步处理模块，负责数据读取、指标计算和图表数据准备的异步处理
"""

from PySide6.QtCore import QThread, Signal
from PySide6.QtWidgets import QApplication
import polars as pl
from pathlib import Path
from loguru import logger
from src.data.tdx_day_parser import parse_day_file
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
from src.utils.event_bus import publish, EventType


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

        解析统一委托给 src.data.tdx_day_parser 的 np.frombuffer 向量化实现，
        UI 层不再保留逐记录 struct.unpack 的副本：该副本既慢，又把
        volume/amount 两个字段读反了（offset 20 实为 float32 成交额，
        offset 24 才是 uint32 成交量）。
        """
        try:
            logger.info(f"开始异步读取股票数据文件: {self.file_path}")

            tdx_file_path = Path(self.file_path)
            if not tdx_file_path.exists():
                error_msg = f"找不到股票数据文件: {tdx_file_path}"
                logger.warning(error_msg)
                # 文件不存在时返回空DataFrame，与tdx_handler处理方式一致
                df = pl.DataFrame()
                self.data_read_completed.emit(df, self.name, self.code)
                return

            df = parse_day_file(tdx_file_path)
            if df.is_empty():
                error_msg = f"股票数据文件为空: {tdx_file_path}"
                logger.warning(error_msg)
                self.data_read_error.emit(error_msg)
                return

            self.data_read_progress.emit(100, df.height)

            # 发送数据读取完成信号
            self.data_read_completed.emit(df, self.name, self.code)

            # 发布数据读取完成事件到事件总线
            publish(
                EventType.DATA_UPDATED,
                data_type='stock',
                ts_code=self.code,
                data=df,
                name=self.name
            )

        except (OSError, RuntimeError, ValueError) as e:
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
            # 延迟导入，避免循环导入

            # 创建TechnicalAnalyzer实例
            analyzer = TechnicalAnalyzer(self.df)

            # 计算所有技术指标
            analyzer.calculate_all_indicators(self.df, parallel=True)

            # 获取计算结果
            result_df = analyzer.get_data(return_polars=True)

            # 发送指标计算完成信号
            self.indicator_calculated.emit(result_df)

            # 发布指标计算完成事件到事件总线
            publish(
                EventType.INDICATOR_CALCULATED,
                data_type='stock',
                data=result_df,
                success=True
            )

        except (OSError, RuntimeError) as e:
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

        except (OSError, RuntimeError) as e:
            error_msg = f"准备图表数据失败: {str(e)}"
            logger.exception(error_msg)
            self.chart_data_prepare_error.emit(error_msg)

    def stop(self):
        """
        停止线程
        """
        self.is_running = False
        self.wait()
