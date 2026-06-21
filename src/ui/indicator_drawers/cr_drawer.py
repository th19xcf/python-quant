import pyqtgraph as pg

from .base_drawer import BaseIndicatorDrawer


class CRDrawer(BaseIndicatorDrawer):
    """
    CR指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    required_columns = ['cr']
    y_range = (0, 300)
    
    def draw_indicator(self, plot_widget, x, df_pl):
        """
        绘制CR指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含cr相关列
        """
        self.plot_line(plot_widget, x, df_pl, 'cr', pen_color='#FFFFFF', name='CR')
        
        self.add_horizontal_line(plot_widget, 100, name='中线')
