from functools import wraps

from PySide6.QtWidgets import QApplication, QMessageBox

from src.ui.task_manager import global_task_manager
from src.utils.logger import logger


def event_handler(event_type):
    """
    事件处理装饰器，统一记录日志和处理事件
    
    Args:
        event_type: 事件类型，用于日志记录
        
    Returns:
        decorator: 装饰器函数
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, sender, *args, **kwargs):
            # 记录事件接收
            logger.info(f"收到{event_type}事件: {args} {kwargs}")
            try:
                # 调用实际的事件处理函数
                return func(self, sender, *args, **kwargs)
            except (OSError, RuntimeError, ValueError) as e:
                # 记录异常
                logger.exception(f"处理{event_type}事件时发生错误: {e}")
        return wrapper
    return decorator

class MainWindowEventMixin:
    @event_handler("数据更新")
    def _handle_data_updated(self, sender, data_type, ts_code, message="", **kwargs):
        """
        处理数据更新事件
        
        Args:
            data_type: 数据类型
            ts_code: 股票代码或指数代码
            message: 附加消息
            **kwargs: 其他关键字参数，包括data等
        """
        self.event_manager.handle_data_updated(sender, data_type, ts_code, message, **kwargs)

    @event_handler("数据错误")
    def _handle_data_error(self, sender, data_type, ts_code, message="", **kwargs):
        """
        处理数据错误事件
        
        Args:
            data_type: 数据类型
            ts_code: 股票代码或指数代码
            message: 错误消息
            **kwargs: 其他关键字参数
        """
        self.event_manager.handle_data_error(sender, data_type, ts_code, message, **kwargs)
    
    @event_handler("指标计算完成")
    def _handle_indicator_calculated(self, sender, data_type=None, indicators=None, calculated_indicators=None, success=True, error=None, indicator_name=None, ts_code=None, result=None, **kwargs):
        """
        处理技术指标计算完成事件
        
        Args:
            data_type: 数据类型
            indicators: 计算的指标列表
            calculated_indicators: 已计算的指标状态
            success: 是否成功
            error: 错误信息
            indicator_name: 指标名称（兼容旧版事件）
            ts_code: 股票代码（兼容旧版事件）
            result: 计算结果（兼容旧版事件）
            **kwargs: 其他关键字参数
        """
        self.event_manager.handle_indicator_calculated(
            sender,
            data_type=data_type,
            indicators=indicators,
            calculated_indicators=calculated_indicators,
            success=success,
            error=error,
            indicator_name=indicator_name,
            ts_code=ts_code,
            result=result,
            **kwargs
        )
    
    @event_handler("指标计算错误")
    def _handle_indicator_error(self, sender, data_type=None, indicators=None, error="", indicator_name=None, ts_code=None, **kwargs):
        """
        处理技术指标计算错误事件
        
        Args:
            data_type: 数据类型
            indicators: 指标列表
            error: 错误消息
            indicator_name: 指标名称（兼容旧版事件）
            ts_code: 股票代码（兼容旧版事件）
            **kwargs: 其他关键字参数
        """
        self.event_manager.handle_indicator_error(sender, data_type, indicators, error, indicator_name, ts_code, **kwargs)
    
    @event_handler("系统关闭")
    def _handle_system_shutdown(self, sender):
        """
        处理系统关闭事件
        """
        self.event_manager.handle_system_shutdown(sender)
    
    
    def closeEvent(self, event):
        """
        Application close event handler
        """
        # 创建自定义消息框以使用中文按钮
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle('退出确认')
        msg_box.setText('确定要退出程序吗？')
        msg_box.setIcon(QMessageBox.Question)
        
        # 添加中文按钮
        yes_btn = msg_box.addButton('是', QMessageBox.YesRole)
        no_btn = msg_box.addButton('否', QMessageBox.NoRole)
        msg_box.setDefaultButton(no_btn)
        
        msg_box.exec()
        
        if msg_box.clickedButton() == yes_btn:
            # Save window state
            try:
                # Save window geometry
                # self.settings.setValue('window_geometry', self.saveGeometry())
                # self.settings.setValue('window_state', self.saveState())
                pass
            except (OSError, RuntimeError, ValueError) as e:
                logger.error(f"Error saving window state: {e}")
            
            # 关闭任务管理器，确保所有后台线程被正确终止
            try:
                from src.ui.task_manager import global_task_manager
                global_task_manager.shutdown()
                logger.info("任务管理器已关闭")
            except Exception as e:
                logger.error(f"关闭任务管理器时出错: {e}")
            
            # 停止内存监控
            try:
                from src.utils.memory_manager import global_memory_manager
                global_memory_manager.stop_monitoring()
                logger.info("内存监控已停止")
            except Exception as e:
                logger.error(f"停止内存监控时出错: {e}")
            
            # 停止系统监控
            try:
                from src.utils.monitoring import global_monitoring_system
                global_monitoring_system.stop()
                logger.info("系统监控已停止")
            except Exception as e:
                logger.error(f"停止系统监控时出错: {e}")
            
            # 调用父类的关闭方法
            try:
                super().closeEvent(event)
            except Exception:
                pass
                
            event.accept()
            
            # 强制退出应用程序，确保所有线程都被终止
            from PySide6.QtWidgets import QApplication
            QApplication.quit()
        else:
            event.ignore()

    def _show_global_search_dialog(self, initial_text=""):
        """
        在右下角显示股票搜索对话框
        
        Args:
            initial_text: 初始搜索文本
        """
        try:
            from src.ui.stock_search_dialog import StockSearchDialog

            # 创建搜索对话框（股票基础信息统一由 DataManager 提供）
            dialog = StockSearchDialog(self, getattr(self, 'data_manager', None), initial_text)
            
            # 设置对话框位置在右下角
            self._position_dialog_at_bottom_right(dialog)
            
            if dialog.exec() == StockSearchDialog.Accepted:
                selected_stock = dialog.get_selected_stock()
                if selected_stock:
                    ts_code = selected_stock['ts_code']
                    stock_name = selected_stock['name']
                    logger.info(f"选中股票: {ts_code} - {stock_name}")
                    
                    # 加载选中股票的K线图
                    if hasattr(self, 'action_manager'):
                        self.action_manager._load_stock_chart(ts_code, stock_name)
            
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"显示全局搜索对话框失败: {e}")
    
    def _position_dialog_at_bottom_right(self, dialog):
        """
        设置对话框位置在主窗口右下角
        
        Args:
            dialog: 搜索对话框
        """
        try:
            # 获取主窗口几何信息
            window_geometry = self.geometry()
            
            # 计算右下角位置
            dialog_width = dialog.width()
            dialog_height = dialog.height()
            
            x = window_geometry.x() + window_geometry.width() - dialog_width - 20
            y = window_geometry.y() + window_geometry.height() - dialog_height - 20
            
            # 确保不超出屏幕
            screen = self.screen()
            if screen:
                screen_geometry = screen.geometry()
                x = max(0, min(x, screen_geometry.width() - dialog_width))
                y = max(0, min(y, screen_geometry.height() - dialog_height))
            
            # 移动对话框
            dialog.move_to_position(x, y)
            
        except (OSError, RuntimeError, ValueError) as e:
            logger.warning(f"设置对话框位置失败: {e}")
