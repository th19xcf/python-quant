import pyqtgraph as pg
from PySide6.QtCore import Qt
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer
import polars as pl

class VRDrawer:
    """
    VR指标绘制器，负责绘制VR指标
    """
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制VR指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含vr数据
        
        Returns:
            更新后的df_pl，包含vr数据
        """
        # 确保VR相关列存在
        if 'vr' not in df_pl.columns:
            # 使用TechnicalAnalyzer计算VR指标
            analyzer = TechnicalAnalyzer(df_pl)
            analyzer.calculate_indicator_parallel('vr', windows=[26])
            df_pl = analyzer.get_data(return_polars=True)
        
        # 计算MAVR（VR的移动平均线），默认使用10日移动平均
        if 'mavr' not in df_pl.columns:
            # 使用rolling_mean计算MAVR
            df_pl = df_pl.with_columns(
                pl.col('vr').rolling_mean(window_size=10, min_periods=1).alias('mavr')
            )
        
        # 绘制VR指标，使用通达信配色
        # 设置Y轴范围，VR通常在0-200之间，并添加100参考线
        max_val = max(df_pl['vr'].max(), df_pl['mavr'].max()) * 1.2
        plot_widget.setYRange(0, max(200, max_val))
        
        # 添加100参考线（通达信风格）
        plot_widget.addItem(pg.InfiniteLine(pos=100, pen=pg.mkPen(color='#444444', width=1.0, style=Qt.DotLine)))
        
        # 绘制VR线（白色）
        plot_widget.plot(x, df_pl['vr'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='VR')
        # 绘制MAVR线（黄色）
        plot_widget.plot(x, df_pl['mavr'].to_numpy(), pen=pg.mkPen(color='#FFFF00', width=1.0), name='MAVR')
        
        return df_pl