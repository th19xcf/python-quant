#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
K线图可视化插件，封装K线图绘制功能
"""

from loguru import logger
from src.plugin.plugin_base import VisualizationPlugin


class KLineVisualizationPlugin(VisualizationPlugin):
    """
    K线图可视化插件，用于绘制K线图
    """
    
    def __init__(self):
        super().__init__()
        self.name = "KLineVisualization"
        self.version = "0.1.0"
        self.author = "Quant System"
        self.description = "K线图可视化插件，用于绘制K线图及相关指标"
        self.chart = None
        
    def get_name(self) -> str:
        return self.name
    
    def get_version(self) -> str:
        return self.version
    
    def get_author(self) -> str:
        return self.author
    
    def get_description(self) -> str:
        return self.description
    
    def render(self, data, container=None, **kwargs):
        """
        渲染K线图
        
        Args:
            data: 要可视化的数据，通常为包含K线数据的DataFrame
            container: 渲染容器，如Qt的Widget
            **kwargs: 渲染参数，包括：
                - stock_name: 股票名称
                - stock_code: 股票代码
                - indicators: 要显示的指标列表
        
        Returns:
            Any: 渲染结果，如图表对象
        """
        try:
            import pyqtgraph as pg
            import numpy as np
            from pyqtgraph import Point
            from src.ui.chart_items import CandleStickItem
            
            # 准备数据
            df = data.copy()
            dates = df['date'].tolist()
            opens = df['open'].tolist()
            highs = df['high'].tolist()
            lows = df['low'].tolist()
            closes = df['close'].tolist()
            
            # 创建图表
            if container is None:
                container = pg.PlotWidget()
            
            # 清除图表
            container.clear()
            
            # 设置图表样式
            container.setBackground('#222222')
            container.showGrid(x=True, y=True, alpha=0.3)
            container.setLabel('left', '价格', color='#ffffff', size='10pt')
            container.setLabel('bottom', '日期', color='#ffffff', size='10pt')
            container.getAxis('left').setPen(pg.mkPen('#ffffff', width=1))
            container.getAxis('bottom').setPen(pg.mkPen('#ffffff', width=1))
            container.getAxis('left').setTextPen(pg.mkPen('#ffffff', width=1))
            container.getAxis('bottom').setTextPen(pg.mkPen('#ffffff', width=1))
            
            # 创建K线数据
            kline_data = []
            for i in range(len(df)):
                kline_data.append((i, opens[i], highs[i], lows[i], closes[i]))
            
            # 创建K线图项
            candle_item = CandleStickItem(kline_data)
            container.addItem(candle_item)
            
            # 保存图表引用
            self.chart = container
            
            logger.info(f"K线图绘制完成，股票: {kwargs.get('stock_name', 'Unknown')}({kwargs.get('stock_code', 'Unknown')})")
            return container
        except Exception as e:
            logger.error(f"K线图渲染失败: {e}")
            raise
    
    def get_supported_data_types(self) -> list:
        """
        获取支持的数据类型
        
        Returns:
            List[str]: 支持的数据类型列表
        """
        return ['stock', 'index', 'kline']
    
    def update_data(self, data):
        """
        更新K线图数据
        
        Args:
            data: 新的K线数据
        """
        try:
            if self.chart is None:
                logger.warning("图表未初始化，无法更新数据")
                return
            
            # 重新渲染图表
            self.render(data, self.chart)
            logger.info("K线图数据更新成功")
        except Exception as e:
            logger.error(f"K线图数据更新失败: {e}")
            raise
