import pyqtgraph as pg

from .base_drawer import BaseIndicatorDrawer


class TRIXDrawer(BaseIndicatorDrawer):
    """
    TRIX指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    required_columns = ['trix', 'trma']
    auto_y_range_columns = ['trix', 'trma']
    y_range_padding = 0.2
    
    def draw_indicator(self, plot_widget, x, df_pl):
        """
        绘制TRIX指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含trix相关列
        """
        self.plot_line(plot_widget, x, df_pl, 'trix', pen_color='#FFFFFF', name='TRIX')
        self.plot_line(plot_widget, x, df_pl, 'trma', pen_color='#FFFF00', name='MATRIX')
