import pyqtgraph as pg
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer

class CRDrawer:
    """
    CR指标绘制器，负责绘制CR指标（能量指标）
    """

    def draw(self, plot_widget, x, df_pl):
        """
        绘制CR指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含cr数据

        Returns:
            更新后的df_pl，包含cr数据
        """
        # 确保CR相关列存在
        if 'cr' not in df_pl.columns:
            # 使用TechnicalAnalyzer计算CR指标
            analyzer = TechnicalAnalyzer(df_pl)
            analyzer.calculate_indicator_parallel('cr', windows=[26])
            df_pl = analyzer.get_data(return_polars=True)

        # 设置Y轴范围，CR指标一般在0-300之间
        plot_widget.setYRange(0, 300)

        # 绘制CR线（白色）
        plot_widget.plot(x, df_pl['cr'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='CR')

        # 绘制参考线（100）
        plot_widget.addItem(pg.InfiniteLine(pos=100, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='中线'))

        return df_pl
