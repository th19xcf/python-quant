import pyqtgraph as pg
from PySide6.QtCore import Qt

class VRDrawer:
    """
    VR指标绘制器，仅负责绘制，不计算指标
    指标计算应在数据准备阶段完成
    """
    
    def draw(self, plot_widget, x, df_pl):
        """
        绘制VR指标
        
        Args:
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，必须已包含vr和mavr相关列
        
        Returns:
            df_pl: 输入的数据（不做修改）
            
        Raises:
            ValueError: 如果数据缺少必要的VR列
        """
        # 检查VR相关列是否存在
        required_columns = ['vr', 'mavr']
        missing_columns = [col for col in required_columns if col not in df_pl.columns]
        
        if missing_columns:
            raise ValueError(
                f"VR绘制失败：数据缺少必要的列 {missing_columns}。"
                f"请确保在调用绘制前已通过IndicatorManager计算VR指标（包含MAVR）。"
            )
        
        # 绘制VR指标，使用通达信配色
        # 设置Y轴范围，VR通常在0-200之间，并添加100参考线
        max_val = max(df_pl['vr'].max(), df_pl['mavr'].max()) * 1.2
        plot_widget.setYRange(0, max(200, max_val))
        
        # 添加100参考线（通达信风格）
        plot_widget.addItem(pg.InfiniteLine(pos=100, pen=pg.mkPen(color='#444444', width=1.0, style=Qt.DotLine)))
        
        # 绘制VR线（白色）
        plot_widget.plot(x, df_pl['vr'].to_numpy(), pen=pg.mkPen(color='#FFFFFF', width=1.0), name='VR')
        # 绘制MAVR线（黄色）
        plot_widget.plot(x, df_pl['mavr'].to_numpy(), pen=pg.mkPen(color='#FFFF00', width=1.0), name='MAVR')
        
        return df_pl
