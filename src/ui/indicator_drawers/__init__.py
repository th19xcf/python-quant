"""
指标绘制器管理器

自动发现 src/ui/indicator_drawers/ 下所有继承 BaseIndicatorDrawer 的类，
按类名推断指标名注册到 self.drawers。新增指标只需：
  1. 新建 xxx_drawer.py，定义 class XXXDrawer(BaseIndicatorDrawer)
  2. 在本文件 import 该类（触发扫描）
本文件自动完成注册，无需改 self.drawers dict。

约定：
  - 类名 XXXDrawer → 指标名 = XXX.upper()（自动推断）
  - 少数异形名（如 VOLTDX → VOL-TDX）在 SPECIAL_NAMES 里覆盖
  - BaseIndicatorDrawer 本身不注册
"""

import re

from .base_drawer import BaseIndicatorDrawer

# 所有 drawer 文件的类 import（触发扫描）。保持 alphabetical 方便维护。
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
from .dma_drawer import DMADrawer
from .fsl_drawer import FSLDrawer
from .sar_drawer import SARDrawer
from .vol_tdx_drawer import VOLTDXDrawer
from .cr_drawer import CRDrawer
from .expma_drawer import ExpmaDrawer
from .bbi_drawer import BbiDrawer
from .hsl_drawer import HslDrawer
from .lb_drawer import LbDrawer
from .cyc_drawer import CycDrawer
from .market_breadth_drawer import (
    ABIDrawer, ADLDrawer, ADRDrawer, OBOSDrawer,
)

# 异形指标名覆盖：类名（去 Drawer）→ 实际指标名（大小写/连字符不同）
SPECIAL_NAMES = {
    "VOLTDX": "VOL-TDX",     # 通达信成交量
    "EXPMADrawer": "EXPMA",  # 避免误匹配
    "BBIDrawer": "BBI",
    "CYCDrawer": "CYC",
    "HSLDrawer": "HSL",
    "LBDrawer": "LB",
}


def _class_to_indicator_name(cls) -> str | None:
    """从 Drawer 类名推断指标名"""
    name = cls.__name__
    if not name.endswith("Drawer"):
        return None
    stem = name[:-6]  # 去 Drawer 后缀
    # 查异形覆盖表（key 是 stem 或完整类名）
    if stem in SPECIAL_NAMES:
        return SPECIAL_NAMES[stem]
    if name in SPECIAL_NAMES:
        return SPECIAL_NAMES[name]
    # 默认：类名去 Drawer 后 → 全大写（处理 Bbi/Cyc/Expma/Hsl/Lb 这类 CamelCase）
    return stem.upper()


class IndicatorDrawerManager:
    """
    指标绘制器管理器，负责统一管理和调用各个指标绘制器

    self.drawers 在 __init__ 时通过反射扫描本模块的 BaseIndicatorDrawer
    子类自动构建，新增 drawer 文件后只需加 import 即可自动注册。
    """

    def __init__(self):
        """初始化时扫描 self.__class__.__module__ 的所有子类"""
        self.drawers = self._build_drawers()

    @classmethod
    def _build_drawers(cls) -> dict[str, BaseIndicatorDrawer]:
        """反射扫描：找本模块内所有继承 BaseIndicatorDrawer 的类"""
        import sys
        import inspect
        module = sys.modules[cls.__module__]
        result: dict[str, BaseIndicatorDrawer] = {}
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                inspect.isclass(attr)
                and issubclass(attr, BaseIndicatorDrawer)
                and attr is not BaseIndicatorDrawer
            ):
                ind_name = _class_to_indicator_name(attr)
                if ind_name is None:
                    continue
                if ind_name in result:
                    # 重名冲突警告但允许覆盖（便于热替换）
                    import warnings
                    warnings.warn(
                        f"指标名 '{ind_name}' 已注册，{attr.__name__} 覆盖旧实现",
                        stacklevel=2,
                    )
                result[ind_name] = attr()
        return result

    def get_drawer(self, indicator_name: str) -> BaseIndicatorDrawer | None:
        """获取指定指标的绘制器"""
        return self.drawers.get(indicator_name)

    def draw_indicator(self, indicator_name, plot_widget, x, df_pl):
        """
        绘制指定指标
        """
        drawer = self.get_drawer(indicator_name)
        if drawer:
            return drawer.draw(plot_widget, x, df_pl)
        return df_pl
