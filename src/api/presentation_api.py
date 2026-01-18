#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
表现层接口定义
"""

from typing import Any, Dict, List, Optional, Union
import polars as pl
import pandas as pd


class IView:
    """视图接口，定义视图更新和消息显示方法"""
    
    def update_chart(self, data: Union[pl.DataFrame, pd.DataFrame], indicators: Optional[List[str]] = None, **params):
        """更新图表数据
        
        Args:
            data: 图表数据
            indicators: 指标列表
            **params: 图表更新参数
        """
        pass
    
    def show_message(self, message: str, message_type: str = 'info', title: Optional[str] = None):
        """显示消息
        
        Args:
            message: 消息内容
            message_type: 消息类型，可选值：'info'（信息）、'warning'（警告）、'error'（错误）、'success'（成功）
            title: 消息标题
        """
        pass
    
    def update_progress(self, progress: int, message: Optional[str] = None):
        """更新进度
        
        Args:
            progress: 进度值（0-100）
            message: 进度消息
        """
        pass
    
    def clear_progress(self):
        """清除进度显示"""
        pass
    
    def show_loading(self, visible: bool, message: Optional[str] = None):
        """显示或隐藏加载指示器
        
        Args:
            visible: 是否显示加载指示器
            message: 加载消息
        """
        pass


class IController:
    """控制器接口，定义用户交互事件处理方法"""
    
    def on_stock_selected(self, stock_code: str, **params):
        """处理股票选择事件
        
        Args:
            stock_code: 选中的股票代码
            **params: 其他参数
        """
        pass
    
    def on_indicator_selected(self, indicator_type: str, **params):
        """处理指标选择事件
        
        Args:
            indicator_type: 选中的指标类型
            **params: 指标参数
        """
        pass
    
    def on_timeframe_changed(self, timeframe: str):
        """处理时间周期变化事件
        
        Args:
            timeframe: 新的时间周期
        """
        pass
    
    def on_chart_type_changed(self, chart_type: str):
        """处理图表类型变化事件
        
        Args:
            chart_type: 新的图表类型
        """
        pass
    
    def on_signal_clicked(self, signal: Dict[str, Any]):
        """处理信号点击事件
        
        Args:
            signal: 信号数据
        """
        pass
    
    def on_refresh_data(self):
        """处理刷新数据事件"""
        pass
    
    def on_backtest_start(self, strategy: str, **params):
        """处理回测开始事件
        
        Args:
            strategy: 策略名称
            **params: 回测参数
        """
        pass


class IChartComponent:
    """图表组件接口，定义图表绘制和交互方法"""
    
    def draw_kline(self, data: Union[pl.DataFrame, pd.DataFrame], **params):
        """绘制K线图
        
        Args:
            data: K线数据
            **params: 绘图参数
        """
        pass
    
    def draw_indicator(self, data: Union[pl.DataFrame, pd.DataFrame], indicator_type: str, **params):
        """绘制技术指标
        
        Args:
            data: 指标数据
            indicator_type: 指标类型
            **params: 绘图参数
        """
        pass
    
    def draw_signal(self, data: Union[pl.DataFrame, pd.DataFrame], **params):
        """绘制买卖信号
        
        Args:
            data: 信号数据
            **params: 绘图参数
        """
        pass
    
    def set_chart_theme(self, theme: str):
        """设置图表主题
        
        Args:
            theme: 主题名称，如：'dark'（深色主题）、'light'（浅色主题）
        """
        pass
    
    def enable_crosshair(self, enable: bool):
        """启用或禁用十字光标
        
        Args:
            enable: 是否启用十字光标
        """
        pass
    
    def enable_zoom(self, enable: bool):
        """启用或禁用缩放功能
        
        Args:
            enable: 是否启用缩放功能
        """
        pass
    
    def reset_view(self):
        """重置视图
        """
        pass
    
    def save_chart(self, file_path: str, file_format: str = 'png'):
        """保存图表到文件
        
        Args:
            file_path: 文件路径
            file_format: 文件格式，默认：png
        """
        pass