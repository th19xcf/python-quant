import pyqtgraph as pg
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer

class RSIDrawer:
    """
    RSI指标绘制器，负责绘制RSI指标
    """
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制RSI指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含rsi数据
        
        Returns:
            更新后的df_pl，包含rsi数据
        """
        # 确保RSI相关列存在
        if 'rsi14' not in df_pl.columns:
            # 使用TechnicalAnalyzer计算RSI指标
            analyzer = TechnicalAnalyzer(df_pl)
            analyzer.calculate_indicator_parallel('rsi', windows=[14])
            df_pl = analyzer.get_data(return_polars=True)
        
        # 设置Y轴范围，RSI指标范围一般是0-100
        plot_widget.setYRange(-50, 150)
        
        # 绘制RSI线（白色）
        plot_widget.plot(x, df_pl['rsi14'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='RSI')
        
        # 绘制超买超卖线（80和20）
        plot_widget.addItem(pg.InfiniteLine(pos=20, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='超卖线'))
        plot_widget.addItem(pg.InfiniteLine(pos=80, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='超买线'))
        
        return df_pl