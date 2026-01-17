#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
技术指标可视化插件，封装技术指标图表绘制功能
"""

import numpy as np
import pyqtgraph as pg
from loguru import logger
from src.plugin.plugin_base import VisualizationPlugin


class IndicatorVisualizationPlugin(VisualizationPlugin):
    """
    技术指标可视化插件，用于绘制各种技术指标图表
    """
    
    def __init__(self):
        super().__init__()
        self.name = "IndicatorVisualization"
        self.version = "0.1.0"
        self.author = "Quant System"
        self.description = "技术指标可视化插件，用于绘制各种技术指标图表，如MACD、KDJ、RSI等"
        self.chart = None
        self.current_indicator = None
        
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
        渲染技术指标图
        
        Args:
            data: 要可视化的数据，通常为包含指标数据的DataFrame
            container: 渲染容器，如Qt的Widget
            **kwargs: 渲染参数，包括：
                - indicator_type: 指标类型，如'macd', 'kdj', 'rsi', 'ma', 'vol_ma', 'volume'
        
        Returns:
            Any: 渲染结果，如图表对象
        """
        try:
            # 获取指标类型
            indicator_type = kwargs.get('indicator_type', 'macd')
            self.current_indicator = indicator_type
            
            # 准备数据
            df = data.copy()
            
            # 创建图表
            if container is None:
                container = pg.PlotWidget()
            
            # 清除图表
            container.clear()
            
            # 设置图表样式
            container.setBackground('#222222')
            container.showGrid(x=True, y=True, alpha=0.3)
            container.getAxis('left').setPen(pg.mkPen('#ffffff', width=1))
            container.getAxis('bottom').setPen(pg.mkPen('#ffffff', width=1))
            container.getAxis('left').setTextPen(pg.mkPen('#ffffff', width=1))
            container.getAxis('bottom').setTextPen(pg.mkPen('#ffffff', width=1))
            
            # 绘制不同类型的指标
            if indicator_type == 'macd':
                self._render_macd(df, container)
            elif indicator_type == 'kdj':
                self._render_kdj(df, container)
            elif indicator_type == 'rsi':
                self._render_rsi(df, container)
            elif indicator_type == 'ma':
                self._render_ma(df, container)
            elif indicator_type == 'vol_ma' or indicator_type == 'volume':
                self._render_vol_ma(df, container)
            elif indicator_type == 'boll':
                self._render_boll(df, container)
            elif indicator_type == 'wr':
                self._render_wr(df, container)
            else:
                logger.warning(f"不支持的指标类型: {indicator_type}")
                container.setLabel('left', '指标', color='#ffffff', size='10pt')
            
            # 保存图表引用
            self.chart = container
            
            logger.info(f"{indicator_type}指标图绘制完成")
            return container
        except Exception as e:
            logger.error(f"技术指标图渲染失败: {e}")
            raise
    
    def _render_macd(self, df, container):
        """
        渲染MACD指标图
        
        Args:
            df: 包含MACD数据的DataFrame
            container: 渲染容器
        """
        # 设置标签
        container.setLabel('left', 'MACD', color='#ffffff', size='10pt')
        
        # 准备数据
        x = np.arange(len(df))
        
        # 检查是否包含MACD数据
        if 'macd' not in df.columns or 'macd_signal' not in df.columns or 'macd_hist' not in df.columns:
            logger.warning("数据中缺少MACD指标")
            return
        
        macd = df['macd'].values
        macd_signal = df['macd_signal'].values
        macd_hist = df['macd_hist'].values
        
        # 绘制MACD线
        macd_item = container.plot(x, macd, pen=pg.mkPen('y', width=1), name='MACD')
        signal_item = container.plot(x, macd_signal, pen=pg.mkPen('w', width=1), name='Signal')
        
        # 绘制MACD柱状图
        from pyqtgraph import BarGraphItem
        
        positive_bars = []
        negative_bars = []
        
        for i in range(len(df)):
            if macd_hist[i] >= 0:
                positive_bars.append({
                    'x': i, 'y': macd_hist[i], 'width': 0.6, 'height': macd_hist[i],
                    'pen': pg.mkPen('r', width=0), 'brush': pg.mkBrush('r', alpha=0.8)
                })
            else:
                negative_bars.append({
                    'x': i, 'y': macd_hist[i], 'width': 0.6, 'height': abs(macd_hist[i]),
                    'pen': pg.mkPen('g', width=0), 'brush': pg.mkBrush('g', alpha=0.8)
                })
        
        # 添加正柱
        if positive_bars:
            pos_x = [bar['x'] for bar in positive_bars]
            pos_y = [bar['y'] for bar in positive_bars]
            pos_width = [bar['width'] for bar in positive_bars]
            pos_height = [bar['height'] for bar in positive_bars]
            positive_bargraph = BarGraphItem(x=pos_x, y=pos_y, width=pos_width, height=pos_height,
                                            pen=pg.mkPen('r', width=0), brush=pg.mkBrush('r', alpha=0.8))
            container.addItem(positive_bargraph)
        
        # 添加负柱
        if negative_bars:
            neg_x = [bar['x'] for bar in negative_bars]
            neg_y = [bar['y'] for bar in negative_bars]
            neg_width = [bar['width'] for bar in negative_bars]
            neg_height = [bar['height'] for bar in negative_bars]
            negative_bargraph = BarGraphItem(x=neg_x, y=neg_y, width=neg_width, height=neg_height,
                                            pen=pg.mkPen('g', width=0), brush=pg.mkBrush('g', alpha=0.8))
            container.addItem(negative_bargraph)
    
    def _render_kdj(self, df, container):
        """
        渲染KDJ指标图
        
        Args:
            df: 包含KDJ数据的DataFrame
            container: 渲染容器
        """
        # 设置标签
        container.setLabel('left', 'KDJ', color='#ffffff', size='10pt')
        container.setYRange(0, 100)
        
        # 准备数据
        x = np.arange(len(df))
        
        # 检查是否包含KDJ数据
        if 'k' not in df.columns or 'd' not in df.columns or 'j' not in df.columns:
            logger.warning("数据中缺少KDJ指标")
            return
        
        k = df['k'].values
        d = df['d'].values
        j = df['j'].values
        
        # 绘制KDJ线
        k_item = container.plot(x, k, pen=pg.mkPen('w', width=1), name='K')
        d_item = container.plot(x, d, pen=pg.mkPen('y', width=1), name='D')
        j_item = container.plot(x, j, pen=pg.mkPen('c', width=1), name='J')
        
        # 绘制超买超卖线
        overbought_item = container.plot(x, [80] * len(df), pen=pg.mkPen('r', width=1, style=pg.QtCore.Qt.DashLine), name='超买线')
        oversold_item = container.plot(x, [20] * len(df), pen=pg.mkPen('g', width=1, style=pg.QtCore.Qt.DashLine), name='超卖线')
    
    def _render_rsi(self, df, container):
        """
        渲染RSI指标图
        
        Args:
            df: 包含RSI数据的DataFrame
            container: 渲染容器
        """
        # 设置标签
        container.setLabel('left', 'RSI', color='#ffffff', size='10pt')
        container.setYRange(0, 100)
        
        # 准备数据
        x = np.arange(len(df))
        
        # 检查是否包含RSI数据
        if 'rsi14' not in df.columns:
            logger.warning("数据中缺少RSI指标")
            return
        
        rsi = df['rsi14'].values
        
        # 绘制RSI线
        rsi_item = container.plot(x, rsi, pen=pg.mkPen('w', width=1), name='RSI14')
        
        # 绘制超买超卖线
        overbought_item = container.plot(x, [70] * len(df), pen=pg.mkPen('r', width=1, style=pg.QtCore.Qt.DashLine), name='超买线')
        oversold_item = container.plot(x, [30] * len(df), pen=pg.mkPen('g', width=1, style=pg.QtCore.Qt.DashLine), name='超卖线')
    
    def _render_ma(self, df, container):
        """
        渲染MA指标图（通常在K线图上叠加，这里单独绘制）
        
        Args:
            df: 包含MA数据的DataFrame
            container: 渲染容器
        """
        # 设置标签
        container.setLabel('left', 'MA', color='#ffffff', size='10pt')
        
        # 准备数据
        x = np.arange(len(df))
        
        # 绘制不同周期的MA线
        if 'ma5' in df.columns:
            ma5_item = container.plot(x, df['ma5'].values, pen=pg.mkPen('w', width=1), name='MA5')
        if 'ma10' in df.columns:
            ma10_item = container.plot(x, df['ma10'].values, pen=pg.mkPen('c', width=1), name='MA10')
        if 'ma20' in df.columns:
            ma20_item = container.plot(x, df['ma20'].values, pen=pg.mkPen('r', width=1), name='MA20')
        if 'ma60' in df.columns:
            ma60_item = container.plot(x, df['ma60'].values, pen=pg.mkPen('g', width=1), name='MA60')
    
    def _render_vol_ma(self, df, container):
        """
        渲染成交量MA指标图
        
        Args:
            df: 包含成交量MA数据的DataFrame
            container: 渲染容器
        """
        # 设置标签
        container.setLabel('left', 'VOL', color='#ffffff', size='10pt')
        
        # 准备数据
        x = np.arange(len(df))
        volumes = df['volume'].values
        opens = df['open'].values
        closes = df['close'].values
        
        # 设置Y轴范围，上下留10%的空白
        max_vol = max(volumes) if len(volumes) > 0 else 1
        container.setYRange(0, max_vol * 1.1)
        
        # 绘制成交量柱状图（根据涨跌颜色区分）
        from pyqtgraph import BarGraphItem
        
        # 分离涨跌柱
        positive_bars = []
        negative_bars = []
        
        for i in range(len(df)):
            if closes[i] >= opens[i]:
                # 上涨，红色
                positive_bars.append({
                    'x': i, 'y': volumes[i], 'width': 0.6, 'height': volumes[i],
                    'pen': pg.mkPen('r', width=0), 'brush': pg.mkBrush('r', alpha=0.8)
                })
            else:
                # 下跌，绿色
                negative_bars.append({
                    'x': i, 'y': 0, 'width': 0.6, 'height': volumes[i],
                    'pen': pg.mkPen('g', width=0), 'brush': pg.mkBrush('g', alpha=0.8)
                })
        
        # 添加正柱（上涨）
        if positive_bars:
            pos_x = [bar['x'] for bar in positive_bars]
            pos_y = [bar['y'] for bar in positive_bars]
            pos_width = [bar['width'] for bar in positive_bars]
            pos_height = [bar['height'] for bar in positive_bars]
            positive_bargraph = BarGraphItem(x=pos_x, y=pos_y, width=pos_width, height=pos_height,
                                            pen=pg.mkPen('r', width=0), brush=pg.mkBrush('r', alpha=0.8))
            container.addItem(positive_bargraph)
        
        # 添加负柱（下跌）
        if negative_bars:
            neg_x = [bar['x'] for bar in negative_bars]
            neg_y = [bar['y'] for bar in negative_bars]
            neg_width = [bar['width'] for bar in negative_bars]
            neg_height = [bar['height'] for bar in negative_bars]
            negative_bargraph = BarGraphItem(x=neg_x, y=neg_y, width=neg_width, height=neg_height,
                                            pen=pg.mkPen('g', width=0), brush=pg.mkBrush('g', alpha=0.8))
            container.addItem(negative_bargraph)
        
        # 绘制成交量MA线
        if 'vol_ma5' in df.columns:
            vol_ma5_item = container.plot(x, df['vol_ma5'].values, pen=pg.mkPen('w', width=1), name='VOL MA5')
        if 'vol_ma10' in df.columns:
            vol_ma10_item = container.plot(x, df['vol_ma10'].values, pen=pg.mkPen('c', width=1), name='VOL MA10')
    
    def get_supported_data_types(self) -> list:
        """
        获取支持的数据类型
        
        Returns:
            List[str]: 支持的数据类型列表
        """
        return ['stock', 'index', 'indicator', 'volume']
    
    def _render_boll(self, df, container):
        """
        渲染Boll指标图（布林带）
        
        Args:
            df: 包含Boll数据的DataFrame
            container: 渲染容器
        """
        # 设置标签
        container.setLabel('left', 'BOLL', color='#ffffff', size='10pt')
        
        # 准备数据
        x = np.arange(len(df))
        
        # 检查是否包含Boll数据
        window = 20  # 默认窗口大小
        mb_col = f'mb{window}'
        up_col = f'up{window}'
        dn_col = f'dn{window}'
        
        if mb_col not in df.columns or up_col not in df.columns or dn_col not in df.columns:
            # 尝试使用默认列名
            mb_col = 'mb'
            up_col = 'up'
            dn_col = 'dn'
            if mb_col not in df.columns or up_col not in df.columns or dn_col not in df.columns:
                logger.warning("数据中缺少BOLL指标")
                return
        
        mb = df[mb_col].values
        up = df[up_col].values
        dn = df[dn_col].values
        
        # 绘制BOLL线
        mb_item = container.plot(x, mb, pen=pg.mkPen('w', width=1), name='MB')
        up_item = container.plot(x, up, pen=pg.mkPen('r', width=1), name='UP')
        dn_item = container.plot(x, dn, pen=pg.mkPen('g', width=1), name='DN')
    
    def _render_wr(self, df, container):
        """
        渲染WR指标图（威廉指标），模拟通达信WR(10,6)效果
        
        Args:
            df: 包含WR数据的DataFrame
            container: 渲染容器
        """
        # 设置标签
        container.setLabel('left', 'WR', color='#ffffff', size='10pt')
        container.setYRange(0, 100)  # 通达信风格：0-100
        
        # 准备数据
        x = np.arange(len(df))
        
        # 检查是否包含WR数据（通达信默认使用WR(10,6)）
        wr1_col = 'wr1'  # 默认使用wr1和wr2列
        wr2_col = 'wr2'
        
        if wr1_col not in df.columns or wr2_col not in df.columns:
            # 尝试使用带窗口的列名
            wr1_col = 'wr10'
            wr2_col = 'wr6'
            if wr1_col not in df.columns or wr2_col not in df.columns:
                logger.warning("数据中缺少WR指标")
                return
        
        wr1 = df[wr1_col].values
        wr2 = df[wr2_col].values
        
        # 绘制WR线（通达信风格：黄色和白色）
        # WR1（10日）使用黄色
        wr1_item = container.plot(x, wr1, pen=pg.mkPen('y', width=1), name='WR1(10)')
        # WR2（6日）使用白色
        wr2_item = container.plot(x, wr2, pen=pg.mkPen('w', width=1), name='WR2(6)')
        
        # 绘制超买超卖线
        # WR指标：>80为超卖，<20为超买
        container.addItem(pg.InfiniteLine(pos=20, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='超买线'))
        container.addItem(pg.InfiniteLine(pos=80, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='超卖线'))
    
    def update_data(self, data):
        """
        更新技术指标图数据
        
        Args:
            data: 新的数据
        """
        try:
            if self.chart is None:
                logger.warning("图表未初始化，无法更新数据")
                return
            
            # 重新渲染图表
            self.render(data, self.chart, indicator_type=self.current_indicator)
            logger.info("技术指标图数据更新成功")
        except Exception as e:
            logger.error(f"技术指标图数据更新失败: {e}")
            raise
