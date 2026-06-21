import pyqtgraph as pg

from .base_drawer import BaseIndicatorDrawer


class FSLDrawer(BaseIndicatorDrawer):
    """
    FSL指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    required_columns = ['swl', 'sws']
    
    def draw_indicator(self, plot_widget, x, df_pl):
        """
        绘制FSL指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含fsl相关列
        """
        self.plot_line(plot_widget, x, df_pl, 'swl', pen_color='#FFFFFF', name='SWL')
        self.plot_line(plot_widget, x, df_pl, 'sws', pen_color='#FFFF00', name='SWS')
