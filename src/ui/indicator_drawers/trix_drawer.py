import pyqtgraph as pg
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer

class TRIXDrawer:
    """
    TRIX指标绘制器，负责绘制TRIX指标
    """
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制TRIX指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含trix数据
        
        Returns:
            更新后的df_pl，包含trix数据
        """
        # 确保TRIX相关列存在
        if 'trix' not in df_pl.columns or 'trma' not in df_pl.columns:
            # 使用TechnicalAnalyzer计算TRIX指标
            analyzer = TechnicalAnalyzer(df_pl)
            analyzer.calculate_indicator_parallel('trix', windows=[12], signal_period=9)
            df_pl = analyzer.get_data(return_polars=True)
        
        # 绘制TRIX指标，使用通达信配色
        # 设置Y轴范围
        min_val = min(df_pl['trix'].min(), df_pl['trma'].min()) * 1.2
        max_val = max(df_pl['trix'].max(), df_pl['trma'].max()) * 1.2
        plot_widget.setYRange(min_val, max_val)
        
        # 绘制TRIX线（白色）
        plot_widget.plot(x, df_pl['trix'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='TRIX')
        # 绘制MATRIX线（黄色）
        plot_widget.plot(x, df_pl['trma'].to_numpy(), pen=pg.mkPen(color='#FFFF00', width=1.0), name='MATRIX')
        
        return df_pl