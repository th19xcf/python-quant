import pyqtgraph as pg
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer

class VOLTDXDrawer:
    """
    VOL-TDX指标绘制器，负责绘制VOL-TDX指标（成交量趋势）
    """

    def draw(self, plot_widget, x, df_pl):
        """
        绘制VOL-TDX指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含vol_tdx数据

        Returns:
            更新后的df_pl，包含vol_tdx数据
        """
        # 确保VOL-TDX相关列存在
        if 'vol_tdx' not in df_pl.columns:
            # 使用TechnicalAnalyzer计算VOL-TDX指标
            analyzer = TechnicalAnalyzer(df_pl)
            analyzer.calculate_indicator_parallel('vol_tdx', ma_period=5)
            df_pl = analyzer.get_data(return_polars=True)

        # 绘制VOL-TDX柱状图（红色/绿色）
        vol_tdx_values = df_pl['vol_tdx'].to_numpy()

        # 根据涨跌决定颜色
        close_values = df_pl['close'].to_numpy()
        prev_close = df_pl['close'].shift(1).to_numpy()

        pos_mask = close_values >= prev_close
        neg_mask = close_values < prev_close

        # 绘制红色柱（上涨）
        if pos_mask.any():
            pos_bar = pg.BarGraphItem(
                x=x[pos_mask],
                height=vol_tdx_values[pos_mask],
                width=0.35,
                brush='r',
                pen='r'
            )
            plot_widget.addItem(pos_bar)

        # 绘制绿色柱（下跌）
        if neg_mask.any():
            neg_bar = pg.BarGraphItem(
                x=x[neg_mask],
                height=vol_tdx_values[neg_mask],
                width=0.35,
                brush='g',
                pen='g'
            )
            plot_widget.addItem(neg_bar)

        return df_pl
