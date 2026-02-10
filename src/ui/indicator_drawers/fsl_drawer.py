import pyqtgraph as pg
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer

class FSLDrawer:
    """
    FSL指标绘制器，负责绘制FSL指标（分水岭指标）
    """

    def draw(self, plot_widget, x, df_pl):
        """
        绘制FSL指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含fsl数据

        Returns:
            更新后的df_pl，包含fsl数据
        """
        # 确保FSL相关列存在
        if 'swl' not in df_pl.columns or 'sws' not in df_pl.columns:
            # 使用TechnicalAnalyzer计算FSL指标
            analyzer = TechnicalAnalyzer(df_pl)
            analyzer.calculate_indicator_parallel('fsl')
            df_pl = analyzer.get_data(return_polars=True)

        # 绘制SWL线（白色）- 加权平均线
        plot_widget.plot(x, df_pl['swl'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='SWL')
        # 绘制SWS线（黄色）- 分水岭线
        plot_widget.plot(x, df_pl['sws'].to_numpy(), pen=pg.mkPen(color='#FFFF00', width=1.0), name='SWS')

        return df_pl
