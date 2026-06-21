import pyqtgraph as pg

from .base_drawer import BaseIndicatorDrawer


class VRDrawer(BaseIndicatorDrawer):
    """
    VR指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    required_columns = ['vr', 'mavr']
    auto_y_range_columns = ['vr', 'mavr']
    y_range_padding = 0.2
    
    def _setup_y_range(self, plot_widget, df_pl):
        max_val = max(df_pl['vr'].max(), df_pl['mavr'].max()) * 1.2
        plot_widget.setYRange(0, max(200, max_val))
    
    def draw_indicator(self, plot_widget, x, df_pl):
        """
        绘制VR指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含vr和mavr相关列
        """
        self.plot_line(plot_widget, x, df_pl, 'vr', pen_color='#FFFFFF', name='VR')
        self.plot_line(plot_widget, x, df_pl, 'mavr', pen_color='#FFFF00', name='MAVR')
        
        self.add_horizontal_line(plot_widget, 100)
