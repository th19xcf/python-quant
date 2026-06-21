import pyqtgraph as pg

from .base_drawer import BaseIndicatorDrawer


class VOLTDXDrawer(BaseIndicatorDrawer):
    """
    VOL-TDX指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    required_columns = ['vol_tdx', 'close']
    
    def draw_indicator(self, plot_widget, x, df_pl):
        """
        绘制VOL-TDX指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含vol_tdx相关列
        """
        vol_tdx_values = df_pl['vol_tdx'].to_numpy()
        close_values = df_pl['close'].to_numpy()
        prev_close = df_pl['close'].shift(1).to_numpy()

        pos_mask = close_values >= prev_close
        neg_mask = close_values < prev_close

        if pos_mask.any():
            pos_bar = pg.BarGraphItem(
                x=x[pos_mask],
                height=vol_tdx_values[pos_mask],
                width=0.35,
                brush='r',
                pen='r'
            )
            plot_widget.addItem(pos_bar)

        if neg_mask.any():
            neg_bar = pg.BarGraphItem(
                x=x[neg_mask],
                height=vol_tdx_values[neg_mask],
                width=0.35,
                brush='g',
                pen='g'
            )
            plot_widget.addItem(neg_bar)
