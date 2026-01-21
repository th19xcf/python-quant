import pyqtgraph as pg
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer

class MACDDrawer:
    """
    MACD指标绘制器，负责绘制MACD指标
    """
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制MACD指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含macd数据
        
        Returns:
            更新后的df_pl，包含macd数据
        """
        # 确保MACD相关列存在
        if 'macd' not in df_pl.columns or 'macd_signal' not in df_pl.columns or 'macd_hist' not in df_pl.columns:
            # 使用TechnicalAnalyzer计算MACD指标
            analyzer = TechnicalAnalyzer(df_pl)
            analyzer.calculate_indicator_parallel('macd', windows=[12], signal_period=9)
            df_pl = analyzer.get_data(return_polars=True)
        
        # 设置Y轴范围
        min_val = min(df_pl['macd'].min(), df_pl['macd_signal'].min(), df_pl['macd_hist'].min()) * 1.2
        max_val = max(df_pl['macd'].max(), df_pl['macd_signal'].max(), df_pl['macd_hist'].max()) * 1.2
        plot_widget.setYRange(min_val, max_val)
        
        # 绘制MACD线（白色）
        plot_widget.plot(x, df_pl['macd'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='MACD')
        # 绘制信号线（黄色）
        plot_widget.plot(x, df_pl['macd_signal'].to_numpy(), pen=pg.mkPen(color='#FFFF00', width=1.0), name='DEA')
        
        # 绘制柱状图
        # 先创建一个空的柱状图数据数组
        histogram = []
        colors = []
        
        # 填充柱状图数据和颜色
        for i in range(len(x)):
            histogram.append((x[i], 0, df_pl['macd_hist'][i]))
            # 根据macd_hist的正负值设置颜色
            if df_pl['macd_hist'][i] >= 0:
                colors.append(pg.mkBrush('r'))
            else:
                colors.append(pg.mkBrush('#00FF00'))
        
        # 创建柱状图项
        from .histogram_item import HistogramItem
        macd_histogram = HistogramItem(histogram, brush=colors)
        plot_widget.addItem(macd_histogram, name='MACD Histogram')
        
        return df_pl