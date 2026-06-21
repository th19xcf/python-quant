import pyqtgraph as pg
import numpy as np

from .base_drawer import BaseIndicatorDrawer


class VOLDrawer(BaseIndicatorDrawer):
    """
    VOL指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    required_columns = ['volume', 'close', 'open', 'vol_ma5', 'vol_ma10']
    
    def _setup_y_range(self, plot_widget, df_pl):
        volume_data_full = df_pl['volume'].to_numpy()
        volume_max = volume_data_full.max() if len(volume_data_full) > 0 else 100
        
        if volume_max > 0:
            plot_widget.setYRange(0, volume_max * 1.5, padding=0)
        else:
            plot_widget.setYRange(0, 100)
    
    def draw_indicator(self, plot_widget, x, df_pl):
        """
        绘制VOL指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含volume和vol_ma相关列
        """
        volume_data_full = df_pl['volume'].to_numpy()
        close_data_full = df_pl['close'].to_numpy()
        open_data_full = df_pl['open'].to_numpy()
        
        volume_data = volume_data_full[-len(x):] if len(volume_data_full) > len(x) else volume_data_full
        close_data = close_data_full[-len(x):] if len(close_data_full) > len(x) else close_data_full
        open_data = open_data_full[-len(x):] if len(open_data_full) > len(x) else open_data_full
        
        vol_ma5_data = df_pl['vol_ma5'].to_numpy()[-len(x):] if len(df_pl['vol_ma5']) > len(x) else df_pl['vol_ma5'].to_numpy()
        vol_ma10_data = df_pl['vol_ma10'].to_numpy()[-len(x):] if len(df_pl['vol_ma10']) > len(x) else df_pl['vol_ma10'].to_numpy()
        
        brush_list = []
        for i in range(len(x)):
            brush_list.append(pg.mkBrush('#FF0000') if close_data[i] >= open_data[i] else pg.mkBrush('#00FF00'))
        
        for i in range(len(x)):
            bar = pg.BarGraphItem(
                x=[x[i]],
                height=[volume_data[i]],
                width=0.8,
                brush=brush_list[i],
                pen=None
            )
            plot_widget.addItem(bar)
        
        self.plot_line(plot_widget, x, df_pl, 'vol_ma5', pen_color='#FFFFFF', name='MA5')
        self.plot_line(plot_widget, x, df_pl, 'vol_ma10', pen_color='#FFFF00', name='MA10')
