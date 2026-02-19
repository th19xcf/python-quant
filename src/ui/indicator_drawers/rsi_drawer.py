import pyqtgraph as pg

class RSIDrawer:
    """
    RSI指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制RSI指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含rsi相关列
        
        Returns:
            df_pl: 输入的数据（不做修改）
            
        Raises:
            ValueError: 如果数据缺少必要的RSI列
        """
        # 检查RSI列是否存在（支持rsi14或其他窗口的RSI）
        rsi_columns = [col for col in df_pl.columns if col.startswith('rsi')]
        
        if not rsi_columns:
            raise ValueError(
                f"RSI绘制失败：数据缺少RSI列。"
                f"请确保在调用绘制前已通过IndicatorManager计算RSI指标。"
            )
        
        # 使用第一个找到的RSI列
        rsi_col = rsi_columns[0]
        
        # 设置Y轴范围，RSI指标范围一般是0-100
        plot_widget.setYRange(-50, 150)
        
        # 绘制RSI线（白色）
        plot_widget.plot(x, df_pl[rsi_col].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='RSI')
        
        # 绘制超买超卖线（80和20）
        plot_widget.addItem(pg.InfiniteLine(pos=20, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='超卖线'))
        plot_widget.addItem(pg.InfiniteLine(pos=80, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='超买线'))
        
        return df_pl
