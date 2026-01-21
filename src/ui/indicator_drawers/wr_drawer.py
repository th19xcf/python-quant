import pyqtgraph as pg
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer

class WRDrawer:
    """
    WR指标绘制器，负责绘制WR指标
    """
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制WR指标（威廉指标）
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含wr数据
        
        Returns:
            更新后的df_pl，包含wr数据
        """
        # 确保WR相关列存在（通达信默认使用WR(10,6)）
        if 'wr1' not in df_pl.columns or 'wr2' not in df_pl.columns:
            # 使用TechnicalAnalyzer计算WR指标
            analyzer = TechnicalAnalyzer(df_pl)
            analyzer.calculate_wr([10, 6])
            df_pl = analyzer.get_data(return_polars=True)
        
        # 设置WR指标图的Y轴范围（通达信风格：0-100）
        plot_widget.setYRange(0, 100)
        
        # 绘制WR线（通达信风格：黄色和白色）
        # WR1（10日）使用黄色
        plot_widget.plot(x, df_pl['wr1'].to_numpy(), pen=pg.mkPen('y', width=1), name='WR1(10)')
        # WR2（6日）使用白色
        plot_widget.plot(x, df_pl['wr2'].to_numpy(), pen=pg.mkPen('w', width=1), name='WR2(6)')
        
        # 绘制超买超卖线
        # WR指标：>80为超卖，<20为超买
        plot_widget.addItem(pg.InfiniteLine(pos=20, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='超买线'))
        plot_widget.addItem(pg.InfiniteLine(pos=80, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='超卖线'))
        
        return df_pl