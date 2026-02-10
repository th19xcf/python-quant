import pyqtgraph as pg
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer

class DMADrawer:
    """
    DMA指标绘制器，负责绘制DMA指标（平均线差）
    """

    def draw(self, plot_widget, x, df_pl):
        """
        绘制DMA指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含dma数据

        Returns:
            更新后的df_pl，包含dma数据
        """
        # 确保DMA相关列存在
        if 'dma' not in df_pl.columns or 'ama' not in df_pl.columns:
            # 使用TechnicalAnalyzer计算DMA指标
            analyzer = TechnicalAnalyzer(df_pl)
            analyzer.calculate_indicator_parallel('dma', short_period=10, long_period=50, signal_period=10)
            df_pl = analyzer.get_data(return_polars=True)

        # 绘制DMA线（白色）
        plot_widget.plot(x, df_pl['dma'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='DMA')
        # 绘制AMA线（黄色）
        plot_widget.plot(x, df_pl['ama'].to_numpy(), pen=pg.mkPen(color='#FFFF00', width=1.0), name='AMA')

        # 绘制零线
        plot_widget.addItem(pg.InfiniteLine(pos=0, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='零线'))

        return df_pl
