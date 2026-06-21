import pyqtgraph as pg

from .base_drawer import BaseIndicatorDrawer


class MACDDrawer(BaseIndicatorDrawer):
    """
    MACD指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    required_columns = ['macd', 'macd_signal', 'macd_hist']
    auto_y_range_columns = ['macd', 'macd_signal', 'macd_hist']
    y_range_padding = 0.2
    
    def draw_indicator(self, plot_widget, x, df_pl):
        """
        绘制MACD指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含macd相关列
        """
        self.plot_line(plot_widget, x, df_pl, 'macd', pen_color='#FFFFFF', name='MACD')
        self.plot_line(plot_widget, x, df_pl, 'macd_signal', pen_color='#FFFF00', name='DEA')
        
        histogram = []
        colors = []
        
        macd_hist_list = df_pl['macd_hist'].to_list()
        for i in range(len(x)):
            hist_value = macd_hist_list[i]
            histogram.append((x[i], 0, hist_value))
            colors.append(pg.mkBrush('r') if hist_value >= 0 else pg.mkBrush('#00FF00'))
        
        from .histogram_item import HistogramItem
        macd_histogram = HistogramItem(histogram, brush=colors)
        plot_widget.addItem(macd_histogram, name='MACD Histogram')
