import pyqtgraph as pg

class KDJDrawer:
    """
    KDJ指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制KDJ指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含kdj相关列
        
        Returns:
            df_pl: 输入的数据（不做修改）
            
        Raises:
            ValueError: 如果数据缺少必要的KDJ列
        """
        # 检查KDJ相关列是否存在
        required_columns = ['k', 'd', 'j']
        missing_columns = [col for col in required_columns if col not in df_pl.columns]
        
        if missing_columns:
            raise ValueError(
                f"KDJ绘制失败：数据缺少必要的列 {missing_columns}。"
                f"请确保在调用绘制前已通过IndicatorManager计算KDJ指标。"
            )
        
        # 设置Y轴范围，KDJ指标范围一般是0-100
        plot_widget.setYRange(-50, 150)
        
        # 绘制KDJ线
        # K线（白色）
        plot_widget.plot(x, df_pl['k'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='K')
        # D线（黄色）
        plot_widget.plot(x, df_pl['d'].to_numpy(), pen=pg.mkPen(color='#FFFF00', width=1.0), name='D')
        # J线（紫色）
        plot_widget.plot(x, df_pl['j'].to_numpy(), pen=pg.mkPen(color='#FF00FF', width=1.0), name='J')
        
        # 绘制超买超卖线（80和20）
        plot_widget.addItem(pg.InfiniteLine(pos=20, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='超卖线'))
        plot_widget.addItem(pg.InfiniteLine(pos=80, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='超买线'))
        
        return df_pl
