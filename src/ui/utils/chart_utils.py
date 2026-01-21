import pyqtgraph as pg
from PySide6.QtCore import Qt

class ChartUtils:
    """
    图表工具类，包含图表配置和辅助函数
    """
    
    @staticmethod
    def configure_chart(plot_widget, title=""):
        """
        配置图表的基本样式
        
        Args:
            plot_widget: 要配置的图表
            title: 图表标题
        """
        # 设置坐标轴标签
        plot_widget.setLabel('left', title, color='#C0C0C0')
        plot_widget.setLabel('bottom', '', color='#C0C0C0')
        
        # 设置坐标轴样式
        plot_widget.getAxis('left').setPen(pg.mkPen('#C0C0C0'))
        plot_widget.getAxis('bottom').setPen(pg.mkPen('#C0C0C0'))
        plot_widget.getAxis('left').setTextPen(pg.mkPen('#C0C0C0'))
        plot_widget.getAxis('bottom').setTextPen(pg.mkPen('#C0C0C0'))
        
        # 显示网格
        plot_widget.showGrid(x=True, y=True, alpha=0.3)
    
    @staticmethod
    def set_y_range(plot_widget, indicator_name, df_pl):
        """
        设置Y轴范围
        
        Args:
            plot_widget: 图表控件
            indicator_name: 指标名称
            df_pl: 数据DataFrame
        """
        # 使用字典映射替代条件判断，设置初始Y轴范围
        indicator_y_ranges = {
            "VOL": (0, 1000000000),  # 成交量指标范围
            "MACD": (-5, 5),  # MACD指标范围
            "KDJ": (-50, 150),  # KDJ指标范围
            "RSI": (-50, 150),  # RSI指标范围
            "BOLL": (0, 100),  # BOLL指标范围，实际会根据数据动态调整
            "WR": (-50, 150),  # WR指标范围，取值范围0-100
            "DMI": (0, 100),  # DMI指标范围，取值范围0-100
            "VR": (0, 200)  # VR指标范围，取值范围0-200
        }
        
        if indicator_name in indicator_y_ranges:
            y_min, y_max = indicator_y_ranges[indicator_name]
            plot_widget.setYRange(y_min, y_max)
            
            # 特殊处理VOL指标的对数模式
            if indicator_name == "VOL":
                plot_widget.setLogMode(y=False)
    
    @staticmethod
    def add_horizontal_line(plot_widget, pos, color='#444444', style=Qt.DashLine, width=1.0, name=""):
        """
        添加水平线
        
        Args:
            plot_widget: 图表控件
            pos: 水平线位置
            color: 线条颜色
            style: 线条样式
            width: 线条宽度
            name: 线条名称
        
        Returns:
            添加的水平线实例
        """
        line = pg.InfiniteLine(pos=pos, pen=pg.mkPen(color, width=width, style=style), name=name)
        plot_widget.addItem(line)
        return line
    
    @staticmethod
    def configure_axes(plot_widget):
        """
        配置坐标轴，禁用科学计数法，设置样式
        
        Args:
            plot_widget: 图表控件
        """
        # 禁用科学计数法，使用正常的数值显示
        y_axis = plot_widget.getAxis('left')
        y_axis.enableAutoSIPrefix(False)
        y_axis.setStyle(tickTextOffset=20)
        
        # 重置缩放比例，确保显示真实数值
        y_axis.setScale(1.0)
    
    @staticmethod
    def sync_x_axis(plot_widgets):
        """
        同步多个图表的X轴范围
        
        Args:
            plot_widgets: 图表控件列表
        """
        if not plot_widgets:
            return
        
        # 获取第一个图表的X轴范围
        x_range = plot_widgets[0].getViewBox().viewRange()[0]
        
        # 将X轴范围应用到其他图表
        for widget in plot_widgets[1:]:
            widget.setXRange(x_range[0], x_range[1], padding=0)