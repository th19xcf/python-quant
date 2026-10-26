"""指标标签管理器（展示层适配器）。

绘制指标后需要把最新一根 K 线的指标数值写到窗口 3 的标签上。本模块
不再自行维护一套格式化规则，而是把 DataFrame 转成「列名 -> 序列」的
映射后，统一委托给 src.ui.event_handlers.indicator_display 的
WINDOW3_TEXTS 渲染，保证「刚绘制时」与「鼠标悬停时」显示的文本同源。

模块本身不 import 任何 GUI 库（仅在需要创建标签时才延迟导入 Qt 相关工具）。
"""

from collections.abc import Mapping
from typing import Any, Dict

from src.ui.event_handlers.indicator_display import (
    WINDOW3_TEXTS,
    render_indicator_text,
)


def _format_number(value: Any) -> str:
    """统一的数值格式化：保留两位小数。"""
    return f"{value:.2f}"


def _to_column_dict(df: Any) -> Dict[str, Any]:
    """把 polars / pandas DataFrame 转成「列名 -> list」映射。"""
    if df is None:
        return {}
    try:
        # polars: to_dict(as_series=False) 直接返回 list，避免转 pandas
        return df.to_dict(as_series=False)
    except TypeError:
        # pandas 不支持 as_series 参数，退回按列取 list
        return {col: df[col].tolist() for col in df.columns}


class IndicatorLabelManager:
    """指标标签管理器：把「最新一根 K 线的指标值」渲染到窗口 3 标签。"""

    # DataFrame 列名与共享展示表的键名不一致时的别名映射
    _COLUMN_ALIASES = {'rsi14': 'rsi'}

    def __init__(self, main_window):
        """
        初始化指标标签管理器

        Args:
            main_window: 主窗口实例
        """
        self.main_window = main_window

    def update_indicator_values_label(self, indicator_name, df_pl):
        """
        更新指标值显示标签（显示最新一根 K 线的数值）

        Args:
            indicator_name: 指标名称（如 KDJ、MACD、EXPMA）
            df_pl: polars DataFrame，包含指标数据
        """
        if not hasattr(self.main_window, 'kdj_values_label'):
            from .utils.label_utils import LabelUtils
            self.main_window.kdj_values_label = LabelUtils.create_label()

        # 保存指标数据，供鼠标移动时按索引取值
        self.main_window.chart_manager.save_indicator_data(df_pl)

        data = self._normalize(_to_column_dict(df_pl))
        if not data:
            return

        lengths = [len(v) for v in data.values() if v is not None]
        if not lengths or min(lengths) == 0:
            return
        index = min(lengths) - 1

        text = render_indicator_text(
            WINDOW3_TEXTS, indicator_name, data, index, _format_number)

        if not text:
            # 未注册的指标名（如窗口 2 的 VOL）沿用旧行为：回退显示 KDJ；
            # KDJ 数据也不可用时保持原先的提示文案
            text = render_indicator_text(
                WINDOW3_TEXTS, 'KDJ', data, index, _format_number)
        if not text:
            text = "<font color='white'>指标数据不可用</font>"

        self.main_window.kdj_values_label.setText(text)

    def _normalize(self, data: Mapping[str, Any]) -> Dict[str, Any]:
        """按别名映射补齐共享展示表需要的键名。"""
        normalized = dict(data)
        for source, target in self._COLUMN_ALIASES.items():
            if source in normalized and target not in normalized:
                normalized[target] = normalized[source]
        return normalized
