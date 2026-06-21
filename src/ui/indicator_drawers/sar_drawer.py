import pyqtgraph as pg

from .base_drawer import BaseIndicatorDrawer


class SARDrawer(BaseIndicatorDrawer):
    """
    SAR指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    required_columns = ['sar']
    
    def draw_indicator(self, plot_widget, x, df_pl):
        """
        绘制SAR指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含sar相关列
        """
        sar_scatter = pg.ScatterPlotItem(
            x=x,
            y=df_pl['sar'].to_numpy(),
            pen=pg.mkPen(color='#FFFFFF', width=1),
            brush=pg.mkBrush(color='#FFFFFF'),
            size=3,
            name='SAR'
        )
        plot_widget.addItem(sar_scatter)
