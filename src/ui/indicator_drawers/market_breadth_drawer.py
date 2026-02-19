import pyqtgraph as pg
from src.tech_analysis.technical_analyzer import TechnicalAnalyzer


class ABIDrawer:
    """
    ABI指标绘制器，负责绘制绝对广量指标
    """

    def draw(self, plot_widget, x, df_pl, period=10):
        """
        绘制ABI指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含abi数据
            period: 计算周期

        Returns:
            更新后的df_pl，包含abi数据
        """
        column_name = f'abi{period}'

        if column_name not in df_pl.columns:
            analyzer = TechnicalAnalyzer(df_pl)
            analyzer.calculate_indicator_parallel('abi', period=period)
            df_pl = analyzer.get_data(return_polars=True)

        plot_widget.setYRange(0, 100)

        plot_widget.plot(x, df_pl[column_name].to_numpy(), pen=pg.mkPen(color='#00FF00', width=1.0), name='ABI')

        plot_widget.addItem(pg.InfiniteLine(pos=50, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='中轴'))

        return df_pl


class ADLDrawer:
    """
    ADL指标绘制器，负责绘制腾落指标
    """

    def draw(self, plot_widget, x, df_pl):
        """
        绘制ADL指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含adl数据

        Returns:
            更新后的df_pl，包含adl数据
        """
        if 'adl' not in df_pl.columns:
            analyzer = TechnicalAnalyzer(df_pl)
            analyzer.calculate_indicator_parallel('adl')
            df_pl = analyzer.get_data(return_polars=True)

        adl_data = df_pl['adl'].to_numpy()
        plot_widget.setYRange(adl_data.min() * 1.1, adl_data.max() * 1.1)

        plot_widget.plot(x, adl_data, pen=pg.mkPen(color='#00FFFF', width=1.0), name='ADL')

        plot_widget.addItem(pg.InfiniteLine(pos=0, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='零轴'))

        return df_pl


class ADRDrawer:
    """
    ADR指标绘制器，负责绘制涨跌比率
    """

    def draw(self, plot_widget, x, df_pl, period=10):
        """
        绘制ADR指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含adr数据
            period: 计算周期

        Returns:
            更新后的df_pl，包含adr数据
        """
        column_name = f'adr{period}'

        if column_name not in df_pl.columns:
            analyzer = TechnicalAnalyzer(df_pl)
            analyzer.calculate_indicator_parallel('adr', period=period)
            df_pl = analyzer.get_data(return_polars=True)

        plot_widget.setYRange(0, 200)

        plot_widget.plot(x, df_pl[column_name].to_numpy(), pen=pg.mkPen(color='#FF00FF', width=1.0), name='ADR')

        plot_widget.addItem(pg.InfiniteLine(pos=100, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='中轴'))

        return df_pl


class OBOSDrawer:
    """
    OBOS指标绘制器，负责绘制超买超卖指标
    """

    def draw(self, plot_widget, x, df_pl, period=10):
        """
        绘制OBOS指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含obos数据
            period: 计算周期

        Returns:
            更新后的df_pl，包含obos数据
        """
        column_name = f'obos{period}'

        if column_name not in df_pl.columns:
            analyzer = TechnicalAnalyzer(df_pl)
            analyzer.calculate_indicator_parallel('obos', period=period)
            df_pl = analyzer.get_data(return_polars=True)

        obos_data = df_pl[column_name].to_numpy()
        plot_widget.setYRange(obos_data.min() * 1.1, obos_data.max() * 1.1)

        plot_widget.plot(x, obos_data, pen=pg.mkPen(color='#FFFF00', width=1.0), name='OBOS')

        plot_widget.addItem(pg.InfiniteLine(pos=0, pen=pg.mkPen('#444444', style=pg.QtCore.Qt.DashLine), name='零轴'))

        return df_pl
