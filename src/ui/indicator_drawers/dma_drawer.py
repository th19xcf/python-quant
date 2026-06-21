import pyqtgraph as pg

from .base_drawer import BaseIndicatorDrawer


class DMADrawer(BaseIndicatorDrawer):
    """
    DMA指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    required_columns = ['dma', 'ama']
    
    def draw_indicator(self, plot_widget, x, df_pl):
        """
        绘制DMA指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含dma相关列
        """
        self.plot_line(plot_widget, x, df_pl, 'dma', pen_color='#FFFFFF', name='DMA')
        self.plot_line(plot_widget, x, df_pl, 'ama', pen_color='#FFFF00', name='AMA')
        
        self.add_horizontal_line(plot_widget, 0, name='零线')
