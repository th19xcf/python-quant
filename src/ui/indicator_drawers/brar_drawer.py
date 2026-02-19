import pyqtgraph as pg

class BRARDrawer:
    """
    BRAR指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制BRAR指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含brar相关列
        
        Returns:
            df_pl: 输入的数据（不做修改）
            
        Raises:
            ValueError: 如果数据缺少必要的BRAR列
        """
        # 检查BRAR相关列是否存在
        required_columns = ['br', 'ar']
        missing_columns = [col for col in required_columns if col not in df_pl.columns]
        
        if missing_columns:
            raise ValueError(
                f"BRAR绘制失败：数据缺少必要的列 {missing_columns}。"
                f"请确保在调用绘制前已通过IndicatorManager计算BRAR指标。"
            )
        
        # 绘制BRAR指标，使用通达信配色
        # 设置Y轴范围，通达信BRAR通常在0-200之间
        max_val = max(df_pl['br'].max(), df_pl['ar'].max()) * 1.2
        plot_widget.setYRange(0, max(200, max_val))
        
        # 绘制BR线（黄色）
        plot_widget.plot(x, df_pl['br'].to_numpy(), pen=pg.mkPen(color='#FFFF00', width=1.0), name='BR')
        # 绘制AR线（白色）
        plot_widget.plot(x, df_pl['ar'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='AR')
        
        return df_pl
