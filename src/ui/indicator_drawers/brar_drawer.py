import pyqtgraph as pg

from .base_drawer import BaseIndicatorDrawer


class BRARDrawer(BaseIndicatorDrawer):
    """
    BRAR指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    required_columns = ['br', 'ar']
    auto_y_range_columns = ['br', 'ar']
    y_range_padding = 0.2
    
    def _setup_y_range(self, plot_widget, df_pl):
        max_val = max(df_pl['br'].max(), df_pl['ar'].max()) * 1.2
        plot_widget.setYRange(0, max(200, max_val))
    
    def draw_indicator(self, plot_widget, x, df_pl):
        """
        绘制BRAR指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含brar相关列
        """
        self.plot_line(plot_widget, x, df_pl, 'br', pen_color='#FFFF00', name='BR')
        self.plot_line(plot_widget, x, df_pl, 'ar', pen_color='#FFFFFF', name='AR')
