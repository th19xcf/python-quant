import pyqtgraph as pg

from .base_drawer import BaseIndicatorDrawer


class RSIDrawer(BaseIndicatorDrawer):
    """
    RSI指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    y_range = (-50, 150)
    
    def draw_indicator(self, plot_widget, x, df_pl):
        """
        绘制RSI指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含rsi相关列
        """
        rsi_columns = self.find_matching_columns(df_pl, 'rsi')
        
        for i, rsi_col in enumerate(rsi_columns):
            colors = ['#FFFFFF', '#FFFF00', '#FF00FF']
            self.plot_line(plot_widget, x, df_pl, rsi_col, pen_color=colors[i % len(colors)], name='RSI')
        
        self.add_horizontal_line(plot_widget, 20, name='超卖线')
        self.add_horizontal_line(plot_widget, 80, name='超买线')
