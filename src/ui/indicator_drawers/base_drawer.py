import pyqtgraph as pg


class BaseIndicatorDrawer:
    """
    指标绘制器基类，封装通用绘制逻辑
    
    子类需要实现：
    - required_columns: 类属性，必需的列名列表
    - draw_indicator(self, plot_widget, x, df_pl): 具体绘制逻辑
    
    可选配置：
    - y_range: Y轴范围，格式为 (min, max)，None表示自动计算
    - auto_y_range_columns: 自动计算Y轴范围时使用的列名列表
    - y_range_padding: Y轴范围padding比例，默认0.1
    """
    
    required_columns = []
    y_range = None
    auto_y_range_columns = None
    y_range_padding = 0.1
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制指标的通用入口方法
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含指标数据
        
        Returns:
            df_pl: 输入的数据（不做修改）
            
        Raises:
            ValueError: 如果数据缺少必要的列
        """
        self._validate_columns(df_pl)
        self._setup_y_range(plot_widget, df_pl)
        self.draw_indicator(plot_widget, x, df_pl)
        return df_pl
    
    def _validate_columns(self, df_pl):
        """
        验证必要列是否存在
        
        Args:
            df_pl: polars DataFrame
            
        Raises:
            ValueError: 如果数据缺少必要的列
        """
        if not self.required_columns:
            return
        
        missing_columns = [col for col in self.required_columns if col not in df_pl.columns]
        
        if missing_columns:
            raise ValueError(
                f"{self.__class__.__name__}绘制失败：数据缺少必要的列 {missing_columns}。"
                f"请确保在调用绘制前已通过IndicatorManager计算指标。"
            )
    
    def _setup_y_range(self, plot_widget, df_pl):
        """
        设置Y轴范围
        
        Args:
            plot_widget: 绘图控件
            df_pl: polars DataFrame
        """
        if self.y_range is not None:
            plot_widget.setYRange(*self.y_range)
            return
        
        if self.auto_y_range_columns:
            columns_to_check = [col for col in self.auto_y_range_columns if col in df_pl.columns]
            if columns_to_check:
                all_values = []
                for col in columns_to_check:
                    col_data = df_pl[col].to_numpy()
                    all_values.extend(col_data)
                
                if all_values:
                    min_val = min(all_values) * (1 - self.y_range_padding)
                    max_val = max(all_values) * (1 + self.y_range_padding)
                    plot_widget.setYRange(min_val, max_val)
    
    def plot_line(self, plot_widget, x, df_pl, column_name, pen_color='#FFFFFF', pen_width=1.0, name=None):
        """
        绘制线条
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame
            column_name: 数据列名
            pen_color: 线条颜色
            pen_width: 线条宽度
            name: 线条名称
        """
        if column_name in df_pl.columns:
            data = df_pl[column_name].to_numpy()
            name = name or column_name
            plot_widget.plot(x, data, pen=pg.mkPen(color=pen_color, width=pen_width), name=name)
    
    def add_horizontal_line(self, plot_widget, pos, color='#444444', style=pg.QtCore.Qt.DashLine, name=None):
        """
        添加水平线
        
        Args:
            plot_widget: 绘图控件
            pos: 水平线位置
            color: 线条颜色
            style: 线条样式
            name: 线条名称
        """
        plot_widget.addItem(pg.InfiniteLine(pos=pos, pen=pg.mkPen(color, style=style), name=name))
    
    def find_matching_columns(self, df_pl, prefix):
        """
        查找匹配前缀的列名
        
        Args:
            df_pl: polars DataFrame
            prefix: 列名前缀
            
        Returns:
            list: 匹配的列名列表
            
        Raises:
            ValueError: 如果没有找到匹配的列
        """
        columns = [col for col in df_pl.columns if col.startswith(prefix)]
        
        if not columns:
            raise ValueError(
                f"{self.__class__.__name__}绘制失败：数据缺少以'{prefix}'开头的列。"
                f"请确保在调用绘制前已通过IndicatorManager计算指标。"
            )
        
        return columns
    
    def draw_indicator(self, plot_widget, x, df_pl):
        """
        绘制指标的具体逻辑，由子类实现
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame
        """
        raise NotImplementedError("子类必须实现draw_indicator方法")
