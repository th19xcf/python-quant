import pyqtgraph as pg

from .base_drawer import BaseIndicatorDrawer


class BOLLDrawer(BaseIndicatorDrawer):
    """
    BOLL指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    required_columns = ['mb', 'up', 'dn']
    auto_y_range_columns = ['up', 'dn']
    y_range_padding = 0.01
    
    def draw_indicator(self, plot_widget, x, df_pl):
        """
        绘制BOLL指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含boll相关列
        """
        self.plot_line(plot_widget, x, df_pl, 'mb', pen_color='#FFFFFF', name='MB')
        self.plot_line(plot_widget, x, df_pl, 'up', pen_color='#FF0000', name='UP')
        self.plot_line(plot_widget, x, df_pl, 'dn', pen_color='#00FF00', name='DN')
