import pyqtgraph as pg

from .base_drawer import BaseIndicatorDrawer


class KDJDrawer(BaseIndicatorDrawer):
    """
    KDJ指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    required_columns = ['k', 'd', 'j']
    y_range = (-50, 150)
    
    def draw_indicator(self, plot_widget, x, df_pl):
        """
        绘制KDJ指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含kdj相关列
        """
        self.plot_line(plot_widget, x, df_pl, 'k', pen_color='#FFFFFF', name='K')
        self.plot_line(plot_widget, x, df_pl, 'd', pen_color='#FFFF00', name='D')
        self.plot_line(plot_widget, x, df_pl, 'j', pen_color='#FF00FF', name='J')
        
        self.add_horizontal_line(plot_widget, 20, name='超卖线')
        self.add_horizontal_line(plot_widget, 80, name='超买线')
