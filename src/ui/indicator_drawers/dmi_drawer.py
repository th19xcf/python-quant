import pyqtgraph as pg

class DMIDrawer:
    """
    DMI指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制DMI指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含dmi相关列
        
        Returns:
            df_pl: 输入的数据（不做修改）
            
        Raises:
            ValueError: 如果数据缺少必要的DMI列
        """
        # 检查DMI相关列是否存在
        required_columns = ['pdi', 'ndi', 'adx', 'adxr']
        missing_columns = [col for col in required_columns if col not in df_pl.columns]
        
        if missing_columns:
            raise ValueError(
                f"DMI绘制失败：数据缺少必要的列 {missing_columns}。"
                f"请确保在调用绘制前已通过IndicatorManager计算DMI指标。"
            )
        
        # 绘制DMI指标，使用通达信配色，调整线段宽度使其更亮
        plot_widget.plot(x, df_pl['pdi'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='+DI')
        plot_widget.plot(x, df_pl['ndi'].to_numpy(), pen=pg.mkPen(color='#FFFF00', width=1.0), name='-DI')
        plot_widget.plot(x, df_pl['adx'].to_numpy(), pen=pg.mkPen(color='#FF00FF', width=1.0), name='ADX')
        plot_widget.plot(x, df_pl['adxr'].to_numpy(), pen=pg.mkPen(color='#00FF00', width=1.0), name='ADXR')
        
        return df_pl
