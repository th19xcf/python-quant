import pyqtgraph as pg

class MACDDrawer:
    """
    MACD指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制MACD指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含macd相关列
        
        Returns:
            df_pl: 输入的数据（不做修改）
            
        Raises:
            ValueError: 如果数据缺少必要的MACD列
        """
        # 检查MACD相关列是否存在
        required_columns = ['macd', 'macd_signal', 'macd_hist']
        missing_columns = [col for col in required_columns if col not in df_pl.columns]
        
        if missing_columns:
            raise ValueError(
                f"MACD绘制失败：数据缺少必要的列 {missing_columns}。"
                f"请确保在调用绘制前已通过IndicatorManager计算MACD指标。"
            )
        
        # 设置Y轴范围
        min_val = min(df_pl['macd'].min(), df_pl['macd_signal'].min(), df_pl['macd_hist'].min()) * 1.2
        max_val = max(df_pl['macd'].max(), df_pl['macd_signal'].max(), df_pl['macd_hist'].max()) * 1.2
        plot_widget.setYRange(min_val, max_val)
        
        # 绘制MACD线（白色）
        plot_widget.plot(x, df_pl['macd'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='MACD')
        # 绘制信号线（黄色）
        plot_widget.plot(x, df_pl['macd_signal'].to_numpy(), pen=pg.mkPen(color='#FFFF00', width=1.0), name='DEA')
        
        # 绘制柱状图
        histogram = []
        colors = []
        
        macd_hist_list = df_pl['macd_hist'].to_list()
        for i in range(len(x)):
            hist_value = macd_hist_list[i]
            histogram.append((x[i], 0, hist_value))
            # 根据macd_hist的正负值设置颜色
            if hist_value >= 0:
                colors.append(pg.mkBrush('r'))
            else:
                colors.append(pg.mkBrush('#00FF00'))
        
        # 创建柱状图项
        from .histogram_item import HistogramItem
        macd_histogram = HistogramItem(histogram, brush=colors)
        plot_widget.addItem(macd_histogram, name='MACD Histogram')
        
        return df_pl
