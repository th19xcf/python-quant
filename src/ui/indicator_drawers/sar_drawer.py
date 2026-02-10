import pyqtgraph as pg
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer

class SARDrawer:
    """
    SAR指标绘制器，负责绘制SAR指标（抛物线转向指标）
    """

    def draw(self, plot_widget, x, df_pl):
        """
        绘制SAR指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含sar数据

        Returns:
            更新后的df_pl，包含sar数据
        """
        # 确保SAR相关列存在
        if 'sar' not in df_pl.columns:
            # 使用TechnicalAnalyzer计算SAR指标
            analyzer = TechnicalAnalyzer(df_pl)
            analyzer.calculate_indicator_parallel('sar', af_step=0.02, max_af=0.2)
            df_pl = analyzer.get_data(return_polars=True)

        # 绘制SAR点（白色圆点）
        sar_scatter = pg.ScatterPlotItem(
            x=x,
            y=df_pl['sar'].to_numpy(),
            pen=pg.mkPen(color='#FFFFFF', width=1),
            brush=pg.mkBrush(color='#FFFFFF'),
            size=3,
            name='SAR'
        )
        plot_widget.addItem(sar_scatter)

        return df_pl
