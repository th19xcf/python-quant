import contextlib
from functools import wraps

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
        应用关闭事件：点击关闭直接退出，不再弹二次确认。

        关闭顺序：先隐藏窗口（用户感知立即关闭）→ 有界等待后台任务收敛 →
        停止内存监控 → 接受事件并退出事件循环。任何清理失败都不阻断退出。
        """
        logger.info("收到窗口关闭事件，开始退出流程")

        # 先隐藏窗口：后续清理即使被后台任务拖慢，窗口也已从屏幕上消失
        with contextlib.suppress(Exception):
            self.hide()

        # 关闭任务管理器：有界等待，避免取数/回测任务卡住关窗
        try:
            from src.ui.task_manager import global_task_manager
            global_task_manager.shutdown(wait_ms=800)
            logger.info("任务管理器已关闭")
        except Exception as e:  # 关窗清理失败不得阻断退出流程
            logger.error(f"关闭任务管理器时出错: {e}")

        # 停止内存监控（daemon 线程，超时即放弃等待，不阻断退出）
        try:
            from src.utils.memory_manager import global_memory_manager
            global_memory_manager.stop_monitoring()
            logger.info("内存监控已停止")
        except Exception as e:  # 关窗清理失败不得阻断退出流程
            logger.error(f"停止内存监控时出错: {e}")

        # 调用父类的关闭方法
        with contextlib.suppress(Exception):
            super().closeEvent(event)

        event.accept()

        # 退出事件循环；进程在 main.py 完成资源清理后强制收敛
        from PySide6.QtWidgets import QApplication
        QApplication.quit()

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
