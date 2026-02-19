import pyqtgraph as pg

class WRDrawer:
    """
    WR指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制WR指标（威廉指标）
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含wr相关列
        
        Returns:
            df_pl: 输入的数据（不做修改）
            
        Raises:
            ValueError: 如果数据缺少必要的WR列
        """
        # 检查WR相关列是否存在（通达信默认使用WR(10,6)）
        required_columns = ['wr1', 'wr2']
        missing_columns = [col for col in required_columns if col not in df_pl.columns]
        
        if missing_columns:
            raise ValueError(
                f"WR绘制失败：数据缺少必要的列 {missing_columns}。"
                f"请确保在调用绘制前已通过IndicatorManager计算WR指标（窗口：[10, 6]）。"
            )
        
        # 设置WR指标图的Y轴范围（通达信风格：0-100）
        plot_widget.setYRange(0, 100)
        
        # 绘制WR线（通达信风格：黄色和白色）
        # WR1（10日）使用黄色
        plot_widget.plot(x, df_pl['wr1'].to_numpy(), pen=pg.mkPen('y', width=1), name='WR1(10)')
        # WR2（6日）使用白色
        plot_widget.plot(x, df_pl['wr2'].to_numpy(), pen=pg.mkPen('w', width=1), name='WR2(6)')
        
        # 绘制超买超卖线
        # WR指标：>80为超卖，<20为超买
        plot_widget.addItem(pg.InfiniteLine(pos=20, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='超买线'))
        plot_widget.addItem(pg.InfiniteLine(pos=80, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='超卖线'))
        
        return df_pl
