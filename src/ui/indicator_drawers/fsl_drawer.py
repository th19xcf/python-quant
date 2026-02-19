import pyqtgraph as pg

class FSLDrawer:
    """
    FSL指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """

    def draw(self, plot_widget, x, df_pl):
        """
        绘制FSL指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含fsl相关列

        Returns:
            df_pl: 输入的数据（不做修改）
            
        Raises:
            ValueError: 如果数据缺少必要的FSL列
        """
        # 检查FSL相关列是否存在
        required_columns = ['swl', 'sws']
        missing_columns = [col for col in required_columns if col not in df_pl.columns]
        
        if missing_columns:
            raise ValueError(
                f"FSL绘制失败：数据缺少必要的列 {missing_columns}。"
                f"请确保在调用绘制前已通过IndicatorManager计算FSL指标。"
            )

        # 绘制SWL线（白色）- 加权平均线
        plot_widget.plot(x, df_pl['swl'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='SWL')
        # 绘制SWS线（黄色）- 分水岭线
        plot_widget.plot(x, df_pl['sws'].to_numpy(), pen=pg.mkPen(color='#FFFF00', width=1.0), name='SWS')

        return df_pl
