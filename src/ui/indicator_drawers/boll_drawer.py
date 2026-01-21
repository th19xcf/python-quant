import pyqtgraph as pg
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer

class BOLLDrawer:
    """
    BOLL指标绘制器，负责绘制BOLL指标
    """
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制BOLL指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含boll数据
        
        Returns:
            更新后的df_pl，包含boll数据
        """
        # 确保BOLL相关列存在
        if 'mb' not in df_pl.columns or 'up' not in df_pl.columns or 'dn' not in df_pl.columns:
            # 使用TechnicalAnalyzer计算BOLL指标
            analyzer = TechnicalAnalyzer(df_pl)
            analyzer.calculate_indicator_parallel('boll', windows=[20])
            df_pl = analyzer.get_data(return_polars=True)
        
        # 设置Y轴范围，基于BOLL通道
        min_val = df_pl['dn'].min() * 0.99
        max_val = df_pl['up'].max() * 1.01
        plot_widget.setYRange(min_val, max_val)
        
        # 绘制中轨线（白色）
        plot_widget.plot(x, df_pl['mb'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='MB')
        # 绘制上轨线（红色）
        plot_widget.plot(x, df_pl['up'].to_numpy(), pen=pg.mkPen('r', width=1), name='UP')
        # 绘制下轨线（绿色，与K线绿色一致）
        plot_widget.plot(x, df_pl['dn'].to_numpy(), pen=pg.mkPen(pg.mkColor(0, 255, 0), width=1), name='DN')
        
        return df_pl