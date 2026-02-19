import pyqtgraph as pg

class DMADrawer:
    """
    DMA指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """

    def draw(self, plot_widget, x, df_pl):
        """
        绘制DMA指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含dma相关列

        Returns:
            df_pl: 输入的数据（不做修改）
            
        Raises:
            ValueError: 如果数据缺少必要的DMA列
        """
        # 检查DMA相关列是否存在
        required_columns = ['dma', 'ama']
        missing_columns = [col for col in required_columns if col not in df_pl.columns]
        
        if missing_columns:
            raise ValueError(
                f"DMA绘制失败：数据缺少必要的列 {missing_columns}。"
                f"请确保在调用绘制前已通过IndicatorManager计算DMA指标。"
            )

        # 绘制DMA线（白色）
        plot_widget.plot(x, df_pl['dma'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='DMA')
        # 绘制AMA线（黄色）
        plot_widget.plot(x, df_pl['ama'].to_numpy(), pen=pg.mkPen(color='#FFFF00', width=1.0), name='AMA')

        # 绘制零线
        plot_widget.addItem(pg.InfiniteLine(pos=0, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='零线'))

        return df_pl
