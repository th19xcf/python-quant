import pyqtgraph as pg
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer

class KDJDrawer:
    """
    KDJ指标绘制器，负责绘制KDJ指标
    """
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制KDJ指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含kdj数据
        
        Returns:
            更新后的df_pl，包含kdj数据
        """
        # 确保KDJ相关列存在
        if 'k' not in df_pl.columns or 'd' not in df_pl.columns or 'j' not in df_pl.columns:
            # 使用TechnicalAnalyzer计算KDJ指标
            analyzer = TechnicalAnalyzer(df_pl)
            analyzer.calculate_indicator_parallel('kdj', windows=[9])
            df_pl = analyzer.get_data(return_polars=True)
        
        # 设置Y轴范围，KDJ指标范围一般是0-100
        plot_widget.setYRange(-50, 150)
        
        # 绘制KDJ线
        # K线（白色）
        plot_widget.plot(x, df_pl['k'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='K')
        # D线（黄色）
        plot_widget.plot(x, df_pl['d'].to_numpy(), pen=pg.mkPen(color='#FFFF00', width=1.0), name='D')
        # J线（紫色）
        plot_widget.plot(x, df_pl['j'].to_numpy(), pen=pg.mkPen(color='#FF00FF', width=1.0), name='J')
        
        # 绘制超买超卖线（80和20）
        plot_widget.addItem(pg.InfiniteLine(pos=20, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='超卖线'))
        plot_widget.addItem(pg.InfiniteLine(pos=80, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='超买线'))
        
        return df_pl