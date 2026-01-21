from PySide6.QtWidgets import QMenu
from PySide6.QtGui import QAction

class MenuHandler:
    """
    菜单事件处理器，负责处理菜单相关事件
    """
    
    def __init__(self, main_window):
        """
        初始化菜单事件处理器
        
        Args:
            main_window: 主窗口实例
        """
        self.main_window = main_window
    
    def handle_custom_context_menu(self, pos):
        """
        处理自定义右键菜单事件
        
        Args:
            pos: 鼠标位置
        """
        try:
            self.main_window.logger.info("自定义右键菜单被调用")
            
            # 创建自定义菜单
            menu = QMenu(self.main_window.tech_plot_widget)
            
            # 如果有选中的均线，添加修改指标参数选项
            if hasattr(self.main_window, 'selected_ma') and self.main_window.selected_ma:
                modify_action = QAction(f"修改{self.main_window.selected_ma}指标参数", self.main_window)
                modify_action.triggered.connect(lambda: self.main_window.on_modify_indicator(self.main_window.selected_ma))
                menu.addAction(modify_action)
            else:
                # 如果没有选中均线，添加提示信息
                no_select_action = QAction("未选中均线，请先点击选中均线", self.main_window)
                no_select_action.setEnabled(False)  # 禁用选项
                menu.addAction(no_select_action)
            
            # 在鼠标位置显示菜单，确保使用QPoint类型
            menu.exec(self.main_window.tech_plot_widget.mapToGlobal(pos))
        except Exception as e:
            self.main_window.logger.exception(f"处理自定义右键菜单事件时发生错误: {e}")