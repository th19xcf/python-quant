import pyqtgraph as pg

class CRDrawer:
    """
    CR指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """

    def draw(self, plot_widget, x, df_pl):
        """
        绘制CR指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含cr相关列

        Returns:
            df_pl: 输入的数据（不做修改）
            
        Raises:
            ValueError: 如果数据缺少必要的CR列
        """
        # 检查CR相关列是否存在
        if 'cr' not in df_pl.columns:
            raise ValueError(
                f"CR绘制失败：数据缺少'cr'列。"
                f"请确保在调用绘制前已通过IndicatorManager计算CR指标。"
            )

        # 设置Y轴范围，CR指标一般在0-300之间
        plot_widget.setYRange(0, 300)

        # 绘制CR线（白色）
        plot_widget.plot(x, df_pl['cr'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='CR')

        # 绘制参考线（100）
        plot_widget.addItem(pg.InfiniteLine(pos=100, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='中线'))

        return df_pl
