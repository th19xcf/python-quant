import pyqtgraph as pg
import numpy as np
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer

class VOLDrawer:
    """
    VOL指标绘制器，负责绘制VOL指标
    """
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制VOL指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含volume数据
        
        Returns:
            更新后的df_pl，包含volume数据
        """
        # 确保volume列存在且为数值类型
        if 'volume' not in df_pl.columns:
            return df_pl
        
        # 确保vol_ma5和vol_ma10列存在
        if 'vol_ma5' not in df_pl.columns or 'vol_ma10' not in df_pl.columns:
            # 使用TechnicalAnalyzer计算成交量均线
            analyzer = TechnicalAnalyzer(df_pl)
            analyzer.calculate_vol_ma([5, 10])
            df_pl = analyzer.get_data(return_polars=True)
        
        # 提取当前显示范围内的数据（只取x对应的部分）
        volume_data_full = df_pl['volume'].to_numpy()
        close_data_full = df_pl['close'].to_numpy()
        open_data_full = df_pl['open'].to_numpy()
        
        # 提取当前显示范围内的数据
        volume_data = volume_data_full[-len(x):] if len(volume_data_full) > len(x) else volume_data_full
        close_data = close_data_full[-len(x):] if len(close_data_full) > len(x) else close_data_full
        open_data = open_data_full[-len(x):] if len(open_data_full) > len(x) else open_data_full
        
        # 提取当前显示范围内的成交量均线数据
        vol_ma5_data = df_pl['vol_ma5'].to_numpy()[-len(x):] if len(df_pl['vol_ma5']) > len(x) else df_pl['vol_ma5'].to_numpy()
        vol_ma10_data = df_pl['vol_ma10'].to_numpy()[-len(x):] if len(df_pl['vol_ma10']) > len(x) else df_pl['vol_ma10'].to_numpy()
        
        # 设置成交量图的Y轴范围 - 使用当前显示数据的最大值
        volume_max = volume_data.max() if len(volume_data) > 0 else 100
        
        # 计算合理的Y轴范围，确保成交量柱体完整显示
        if volume_max > 0:
            # 简单直接的Y轴范围计算，确保柱体完整显示
            # 从0开始，顶部留出50%的空间
            y_min = 0
            y_max = volume_max * 1.5  # 顶部留出50%空间，确保柱体完整显示
            plot_widget.setYRange(y_min, y_max, padding=0)
        else:
            # 成交量都是0，使用默认范围
            plot_widget.setYRange(0, 100)
        
        # 绘制成交量柱状图
        # 使用 pyqtgraph 的 BarGraphItem，正确配置参数
        
        # 确保 BarGraphItem 支持颜色列表
        # 修复：将颜色列表转换为 QBrush 对象列表
        brush_list = []
        for i in range(len(x)):
            if close_data[i] >= open_data[i]:
                brush_list.append(pg.mkBrush('#FF0000'))  # 红色 - 上涨
            else:
                brush_list.append(pg.mkBrush('#00FF00'))  # 绿色 - 下跌
        
        # 创建 BarGraphItem，使用单个颜色（这是 BarGraphItem 支持的）
        # 然后我们将手动绘制每个柱子
        for i in range(len(x)):
            # 创建单个柱子的数据
            bar_data = {
                'x': [x[i]],
                'height': [volume_data[i]],
                'width': 0.8,
                'brush': brush_list[i],
                'pen': None
            }
            
            # 创建单个柱子的 BarGraphItem
            bar = pg.BarGraphItem(**bar_data)
            
            # 添加到图表
            plot_widget.addItem(bar)
        
        # 绘制成交量均线
        # 5日均线（白色）
        plot_widget.plot(x, vol_ma5_data, pen=pg.mkPen(color='#FFFFFF', width=1.0), name='MA5')
        # 10日均线（黄色）
        plot_widget.plot(x, vol_ma10_data, pen=pg.mkPen(color='#FFFF00', width=1.0), name='MA10')
        
        return df_pl