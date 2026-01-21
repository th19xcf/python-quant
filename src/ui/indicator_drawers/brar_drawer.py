import pyqtgraph as pg
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer

class BRARDrawer:
    """
    BRAR指标绘制器，负责绘制BRAR指标
    """
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制BRAR指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含brar数据
        
        Returns:
            更新后的df_pl，包含brar数据
        """
        # 确保BRAR相关列存在
        if 'br' not in df_pl.columns or 'ar' not in df_pl.columns:
            # 使用TechnicalAnalyzer计算BRAR指标
            analyzer = TechnicalAnalyzer(df_pl)
            analyzer.calculate_indicator_parallel('brar', windows=[26])
            df_pl = analyzer.get_data(return_polars=True)
        
        # 绘制BRAR指标，使用通达信配色
        # 设置Y轴范围，通达信BRAR通常在0-200之间
        max_val = max(df_pl['br'].max(), df_pl['ar'].max()) * 1.2
        plot_widget.setYRange(0, max(200, max_val))
        
        # 绘制BR线（黄色）
        plot_widget.plot(x, df_pl['br'].to_numpy(), pen=pg.mkPen(color='#FFFF00', width=1.0), name='BR')
        # 绘制AR线（白色）
        plot_widget.plot(x, df_pl['ar'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='AR')
        
        return df_pl