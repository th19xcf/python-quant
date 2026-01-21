import pyqtgraph as pg
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer

class DMIDrawer:
    """
    DMI指标绘制器，负责绘制DMI指标
    """
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制DMI指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含dmi数据
        
        Returns:
            更新后的df_pl，包含dmi数据
        """
        # 确保DMI相关列存在
        if 'pdi' not in df_pl.columns or 'ndi' not in df_pl.columns or 'adx' not in df_pl.columns or 'adxr' not in df_pl.columns:
            # 使用TechnicalAnalyzer计算DMI指标
            analyzer = TechnicalAnalyzer(df_pl)
            analyzer.calculate_indicator_parallel('dmi', windows=[14])
            df_pl = analyzer.get_data(return_polars=True)
        
        # 绘制DMI指标，使用通达信配色，调整线段宽度使其更亮
        plot_widget.plot(x, df_pl['pdi'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='+DI')
        plot_widget.plot(x, df_pl['ndi'].to_numpy(), pen=pg.mkPen(color='#FFFF00', width=1.0), name='-DI')
        plot_widget.plot(x, df_pl['adx'].to_numpy(), pen=pg.mkPen(color='#FF00FF', width=1.0), name='ADX')
        plot_widget.plot(x, df_pl['adxr'].to_numpy(), pen=pg.mkPen(color='#00FF00', width=1.0), name='ADXR')
        
        return df_pl