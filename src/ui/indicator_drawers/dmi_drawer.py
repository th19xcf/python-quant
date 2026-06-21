import pyqtgraph as pg

from .base_drawer import BaseIndicatorDrawer


class DMIDrawer(BaseIndicatorDrawer):
    """
    DMI指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    required_columns = ['pdi', 'ndi', 'adx', 'adxr']
    
    def draw_indicator(self, plot_widget, x, df_pl):
        """
        绘制DMI指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含dmi相关列
        """
        self.plot_line(plot_widget, x, df_pl, 'pdi', pen_color='#FFFFFF', name='+DI')
        self.plot_line(plot_widget, x, df_pl, 'ndi', pen_color='#FFFF00', name='-DI')
        self.plot_line(plot_widget, x, df_pl, 'adx', pen_color='#FF00FF', name='ADX')
        self.plot_line(plot_widget, x, df_pl, 'adxr', pen_color='#00FF00', name='ADXR')
