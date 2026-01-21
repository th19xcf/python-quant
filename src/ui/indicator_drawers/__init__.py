# 指标绘制器管理器，用于统一管理和调用各个指标绘制器

from .kdj_drawer import KDJDrawer
from .macd_drawer import MACDDrawer
from .rsi_drawer import RSIDrawer
from .vr_drawer import VRDrawer
from .dmi_drawer import DMIDrawer
from .trix_drawer import TRIXDrawer
from .brar_drawer import BRARDrawer
from .vol_drawer import VOLDrawer
from .boll_drawer import BOLLDrawer
from .wr_drawer import WRDrawer

class IndicatorDrawerManager:
    """
    指标绘制器管理器，负责统一管理和调用各个指标绘制器
    """
    
    def __init__(self):
        """
        初始化指标绘制器管理器，创建各个指标绘制器实例
        """
        self.drawers = {
            "KDJ": KDJDrawer(),
            "MACD": MACDDrawer(),
            "RSI": RSIDrawer(),
            "VR": VRDrawer(),
            "DMI": DMIDrawer(),
            "TRIX": TRIXDrawer(),
            "BRAR": BRARDrawer(),
            "VOL": VOLDrawer(),
            "BOLL": BOLLDrawer(),
            "WR": WRDrawer()
        }
    
    def get_drawer(self, indicator_name):
        """
        获取指定指标的绘制器
        
        Args:
            indicator_name: 指标名称
        
        Returns:
            指标绘制器实例，如果指标不存在则返回None
        """
        return self.drawers.get(indicator_name)
    
    def draw_indicator(self, indicator_name, plot_widget, x, df_pl):
        """
        绘制指定指标
        
        Args:
            indicator_name: 指标名称
            plot_widget: 绘图控件
            x: x轴数据
            df_pl: polars DataFrame，包含指标数据
        
        Returns:
            更新后的df_pl，包含指标数据
        """
        drawer = self.get_drawer(indicator_name)
        if drawer:
            return drawer.draw(plot_widget, x, df_pl)
        return df_pl