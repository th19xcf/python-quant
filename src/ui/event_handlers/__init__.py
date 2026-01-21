from .mouse_handlers import MouseHandler
from .key_handlers import KeyHandler
from .menu_handlers import MenuHandler
from .indicator_handlers import IndicatorHandler

class EventHandlerManager:
    """
    事件处理器管理器，负责统一管理和调度各种事件处理器
    """
    
    def __init__(self, main_window):
        """
        初始化事件处理器管理器
        
        Args:
            main_window: 主窗口实例
        """
        self.main_window = main_window
        self.mouse_handler = MouseHandler(main_window)
        self.key_handler = KeyHandler(main_window)
        self.menu_handler = MenuHandler(main_window)
        self.indicator_handler = IndicatorHandler(main_window)
    
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
    
    def get_menu_handler(self):
        """
        获取菜单事件处理器
        
        Returns:
            菜单事件处理器实例
        """
        return self.menu_handler
    
    def get_indicator_handler(self):
        """
        获取指标事件处理器
        
        Returns:
            指标事件处理器实例
        """
        return self.indicator_handler