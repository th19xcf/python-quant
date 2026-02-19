import pyqtgraph as pg

class TRIXDrawer:
    """
    TRIX指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制TRIX指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含trix相关列
        
        Returns:
            df_pl: 输入的数据（不做修改）
            
        Raises:
            ValueError: 如果数据缺少必要的TRIX列
        """
        # 检查TRIX相关列是否存在
        required_columns = ['trix', 'trma']
        missing_columns = [col for col in required_columns if col not in df_pl.columns]
        
        if missing_columns:
            raise ValueError(
                f"TRIX绘制失败：数据缺少必要的列 {missing_columns}。"
                f"请确保在调用绘制前已通过IndicatorManager计算TRIX指标。"
            )
        
        # 绘制TRIX指标，使用通达信配色
        # 设置Y轴范围
        min_val = min(df_pl['trix'].min(), df_pl['trma'].min()) * 1.2
        max_val = max(df_pl['trix'].max(), df_pl['trma'].max()) * 1.2
        plot_widget.setYRange(min_val, max_val)
        
        # 绘制TRIX线（白色）
        plot_widget.plot(x, df_pl['trix'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='TRIX')
        # 绘制MATRIX线（黄色）
        plot_widget.plot(x, df_pl['trma'].to_numpy(), pen=pg.mkPen(color='#FFFF00', width=1.0), name='MATRIX')
        
        return df_pl
