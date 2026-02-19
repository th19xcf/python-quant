import pyqtgraph as pg

class BOLLDrawer:
    """
    BOLL指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制BOLL指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含boll相关列
        
        Returns:
            df_pl: 输入的数据（不做修改）
            
        Raises:
            ValueError: 如果数据缺少必要的BOLL列
        """
        # 检查BOLL相关列是否存在
        required_columns = ['mb', 'up', 'dn']
        missing_columns = [col for col in required_columns if col not in df_pl.columns]
        
        if missing_columns:
            raise ValueError(
                f"BOLL绘制失败：数据缺少必要的列 {missing_columns}。"
                f"请确保在调用绘制前已通过IndicatorManager计算BOLL指标。"
            )
        
        # 设置Y轴范围，基于BOLL通道
        min_val = df_pl['dn'].min() * 0.99
        max_val = df_pl['up'].max() * 1.01
        plot_widget.setYRange(min_val, max_val)
        
        # 绘制中轨线（白色）
        plot_widget.plot(x, df_pl['mb'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='MB')
        # 绘制上轨线（红色）
        plot_widget.plot(x, df_pl['up'].to_numpy(), pen=pg.mkPen('r', width=1), name='UP')
        # 绘制下轨线（绿色，与K线绿色一致）
        plot_widget.plot(x, df_pl['dn'].to_numpy(), pen=pg.mkPen(pg.mkColor(0, 255, 0), width=1), name='DN')
        
        return df_pl
