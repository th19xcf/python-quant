import pyqtgraph as pg

from .base_drawer import BaseIndicatorDrawer


class WRDrawer(BaseIndicatorDrawer):
    """
    WR指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    required_columns = ['wr1', 'wr2']
    y_range = (0, 100)
    
    def draw_indicator(self, plot_widget, x, df_pl):
        """
        绘制WR指标（威廉指标）
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含wr相关列
        """
        self.plot_line(plot_widget, x, df_pl, 'wr1', pen_color='#FFFF00', name='WR1(10)')
        self.plot_line(plot_widget, x, df_pl, 'wr2', pen_color='#FFFFFF', name='WR2(6)')
        
        self.add_horizontal_line(plot_widget, 20, name='超买线')
        self.add_horizontal_line(plot_widget, 80, name='超卖线')
