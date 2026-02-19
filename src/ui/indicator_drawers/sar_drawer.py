import pyqtgraph as pg

class SARDrawer:
    """
    SAR指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """

    def draw(self, plot_widget, x, df_pl):
        """
        绘制SAR指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含sar相关列

        Returns:
            df_pl: 输入的数据（不做修改）
            
        Raises:
            ValueError: 如果数据缺少必要的SAR列
        """
        # 检查SAR相关列是否存在
        if 'sar' not in df_pl.columns:
            raise ValueError(
                f"SAR绘制失败：数据缺少'sar'列。"
                f"请确保在调用绘制前已通过IndicatorManager计算SAR指标。"
            )

        # 绘制SAR点（白色圆点）
        sar_scatter = pg.ScatterPlotItem(
            x=x,
            y=df_pl['sar'].to_numpy(),
            pen=pg.mkPen(color='#FFFFFF', width=1),
            brush=pg.mkBrush(color='#FFFFFF'),
            size=3,
            name='SAR'
        )
        plot_widget.addItem(sar_scatter)

        return df_pl
