import pyqtgraph as pg


class ABIDrawer:
    """
    ABI指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """

    def draw(self, plot_widget, x, df_pl, period=10):
        """
        绘制ABI指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含abi相关列
            period: 计算周期

        Returns:
            df_pl: 输入的数据（不做修改）
            
        Raises:
            ValueError: 如果数据缺少必要的ABI列
        """
        column_name = f'abi{period}'

        if column_name not in df_pl.columns:
            raise ValueError(
                f"ABI绘制失败：数据缺少'{column_name}'列。"
                f"请确保在调用绘制前已通过IndicatorManager计算ABI指标（period={period}）。"
            )

        plot_widget.setYRange(0, 100)

        plot_widget.plot(x, df_pl[column_name].to_numpy(), pen=pg.mkPen(color='#00FF00', width=1.0), name='ABI')

        plot_widget.addItem(pg.InfiniteLine(pos=50, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='中轴'))

        return df_pl


class ADLDrawer:
    """
    ADL指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """

    def draw(self, plot_widget, x, df_pl):
        """
        绘制ADL指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含adl相关列

        Returns:
            df_pl: 输入的数据（不做修改）
            
        Raises:
            ValueError: 如果数据缺少必要的ADL列
        """
        if 'adl' not in df_pl.columns:
            raise ValueError(
                f"ADL绘制失败：数据缺少'adl'列。"
                f"请确保在调用绘制前已通过IndicatorManager计算ADL指标。"
            )

        adl_data = df_pl['adl'].to_numpy()
        plot_widget.setYRange(adl_data.min() * 1.1, adl_data.max() * 1.1)

        plot_widget.plot(x, adl_data, pen=pg.mkPen(color='#00FFFF', width=1.0), name='ADL')

        plot_widget.addItem(pg.InfiniteLine(pos=0, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='零轴'))

        return df_pl


class ADRDrawer:
    """
    ADR指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """

    def draw(self, plot_widget, x, df_pl, period=10):
        """
        绘制ADR指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含adr相关列
            period: 计算周期

        Returns:
            df_pl: 输入的数据（不做修改）
            
        Raises:
            ValueError: 如果数据缺少必要的ADR列
        """
        column_name = f'adr{period}'

        if column_name not in df_pl.columns:
            raise ValueError(
                f"ADR绘制失败：数据缺少'{column_name}'列。"
                f"请确保在调用绘制前已通过IndicatorManager计算ADR指标（period={period}）。"
            )

        plot_widget.setYRange(0, 200)

        plot_widget.plot(x, df_pl[column_name].to_numpy(), pen=pg.mkPen(color='#FF00FF', width=1.0), name='ADR')

        plot_widget.addItem(pg.InfiniteLine(pos=100, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='中轴'))

        return df_pl


class OBOSDrawer:
    """
    OBOS指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """

    def draw(self, plot_widget, x, df_pl, period=10):
        """
        绘制OBOS指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含obos相关列
            period: 计算周期

        Returns:
            df_pl: 输入的数据（不做修改）
            
        Raises:
            ValueError: 如果数据缺少必要的OBOS列
        """
        column_name = f'obos{period}'

        if column_name not in df_pl.columns:
            raise ValueError(
                f"OBOS绘制失败：数据缺少'{column_name}'列。"
                f"请确保在调用绘制前已通过IndicatorManager计算OBOS指标（period={period}）。"
            )

        obos_data = df_pl[column_name].to_numpy()
        plot_widget.setYRange(obos_data.min() * 1.1, obos_data.max() * 1.1)

        plot_widget.plot(x, obos_data, pen=pg.mkPen(color='#FFFF00', width=1.0), name='OBOS')

        plot_widget.addItem(pg.InfiniteLine(pos=0, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='零轴'))

        return df_pl
