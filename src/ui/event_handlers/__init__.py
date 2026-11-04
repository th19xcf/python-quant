from src.utils.logger import logger

from .indicator_display import missing_indicator_renderers
from .indicator_handlers import IndicatorHandler
from .key_handlers import KeyHandler
from .mouse_handlers import MouseHandler


class EventHandlerManager:
    """
    事件处理器管理器，负责统一管理和调度各种事件处理器
    """

    def __init__(self, main_window, registered_types: set[str] | None = None):
        """
        初始化事件处理器管理器

        Args:
            main_window: 主窗口实例
            registered_types: 已注册的指标类型集合，由上层注入。
                传 None 时跳过启动自检（UI 层不依赖指标管理器实现）。
        """
        self.main_window = main_window
        self.mouse_handler = MouseHandler(main_window)
        self.key_handler = KeyHandler(main_window)
        self.indicator_handler = IndicatorHandler(main_window)
        self._report_indicator_text_coverage(registered_types)

    @staticmethod
    def _report_indicator_text_coverage(registered_types: set[str] | None) -> None:
        """启动时自检一次：把缺少副图提示文本的指标清单打进日志。

        缺少提示不影响绘图，因此只在启动时汇总一次 INFO，避免每次悬停刷 WARNING。
        registered_types 为 None 时不告警、不做任何推断。

        Args:
            registered_types: 上层注入的已注册指标类型集合
        """
        if registered_types is None:
            return
        missing = missing_indicator_renderers(registered_types)
        if missing:
            logger.info(f"以下指标缺少副图提示文本渲染器（悬停时不显示数值）: {missing}")
        else:
            logger.debug("全部已注册指标均有副图提示文本渲染器")

    def get_mouse_handler(self):
        """
        获取鼠标事件处理器

        Returns:
            鼠标事件处理器实例
        """
        return self.mouse_handler

    def get_key_handler(self):
        """
        获取键盘事件处理器

        Returns:
            键盘事件处理器实例
        """
        return self.key_handler

    def get_indicator_handler(self):
        """
        获取指标事件处理器

        Returns:
            指标事件处理器实例
        """
        return self.indicator_handler
