import pyqtgraph as pg

from .base_drawer import BaseIndicatorDrawer


class ABIDrawer(BaseIndicatorDrawer):
    """
    ABI指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    y_range = (0, 100)
    
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

        self._setup_y_range(plot_widget, df_pl)
        self.plot_line(plot_widget, x, df_pl, column_name, pen_color='#00FF00', name='ABI')
        self.add_horizontal_line(plot_widget, 50, name='中轴')

        return df_pl


class ADLDrawer(BaseIndicatorDrawer):
    """
    ADL指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    required_columns = ['adl']
    
    def _setup_y_range(self, plot_widget, df_pl):
        adl_data = df_pl['adl'].to_numpy()
        plot_widget.setYRange(adl_data.min() * 1.1, adl_data.max() * 1.1)
    
    def draw_indicator(self, plot_widget, x, df_pl):
        """
        绘制ADL指标

        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含adl相关列
        """
        self.plot_line(plot_widget, x, df_pl, 'adl', pen_color='#00FFFF', name='ADL')
        self.add_horizontal_line(plot_widget, 0, name='零轴')


class ADRDrawer(BaseIndicatorDrawer):
    """
    ADR指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    y_range = (0, 200)
    
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

        self._setup_y_range(plot_widget, df_pl)
        self.plot_line(plot_widget, x, df_pl, column_name, pen_color='#FF00FF', name='ADR')
        self.add_horizontal_line(plot_widget, 100, name='中轴')

        return df_pl


class OBOSDrawer(BaseIndicatorDrawer):
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

        self.plot_line(plot_widget, x, df_pl, column_name, pen_color='#FFFF00', name='OBOS')
        self.add_horizontal_line(plot_widget, 0, name='零轴')

        return df_pl
