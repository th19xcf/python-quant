from PySide6.QtCore import Qt
from PySide6.QtWidgets import QMenu, QMessageBox, QDialog, QVBoxLayout, QLabel, QLineEdit, QHBoxLayout, QPushButton
from PySide6.QtGui import QAction
import pyqtgraph as pg
from functools import wraps
from datetime import datetime
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
    """
    Main Window Event Handling Mixin
    Handles user interactions and events
    """

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

    def on_stock_clicked(self, index):
        """
        Single click on stock list item
        """
        # Get data for the clicked row
        row = index.row()
        
        try:
            # Get stock code and name
            # Assuming code is in column 1 and name in column 2 (0-indexed)
            code_item = self.stock_table.item(row, 1)
            name_item = self.stock_table.item(row, 2)
            
            if code_item and name_item:
                code = code_item.text()
                name = name_item.text()
                
                # logger.info(f"Selected stock: {name}({code})")
                
                # Update status bar
                self.statusBar().showMessage(f"Selected: {name}({code})")
                
        except (OSError, RuntimeError, ValueError) as e:
            logger.error(f"Error handling stock click: {e}")

    def on_stock_double_clicked(self, index):
        """
        Double click on stock list item
        """
        row = index.row()
        
        try:
            # Get stock code and name
            code_item = self.stock_table.item(row, 1)
            name_item = self.stock_table.item(row, 2)
            
            if code_item and name_item:
                code = code_item.text()
                name = name_item.text()
                
                logger.info(f"Double clicked stock: {name}({code})")
                
                # Switch to chart tab
                self.tab_widget.setCurrentIndex(1)
                
                # Process stock data and draw chart
                # This calls the method in MainWindow (or DataMixin/DrawingMixin)
                if hasattr(self, 'process_stock_data'):
                    self.process_stock_data(code, name)
                
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"Error handling stock double click: {e}")

    def on_nav_item_clicked(self, item, column):
        """
        Navigation tree item clicked
        """
        if hasattr(self, '_on_nav_item_clicked_impl'):
            self._on_nav_item_clicked_impl(item, column)

    def _on_nav_item_clicked_impl(self, item, column):
        """
        Implementation of navigation item click
        """
        text = item.text(column)
        logger.info(f"Clicked nav item: {text}")
        
        # Switch to market tab
        self.tab_widget.setCurrentIndex(0)
        
        try:
            # Handle index items
            if text == "沪市指数":
                # 点击沪市指数时显示所有沪市指数
                if hasattr(self, 'data_view_manager'):
                    self.data_view_manager.show_sh_index_overview()
            elif text == "深市指数":
                # 点击深市指数时显示所有深市指数
                if hasattr(self, 'data_view_manager'):
                    self.data_view_manager.show_sz_index_overview()
            elif text == "京市指数":
                # 点击京市指数时显示所有京市指数
                if hasattr(self, 'data_view_manager'):
                    self.data_view_manager.show_bj_index_overview()
            elif text in ["创业板指", "科创板指"]:
                # 其他指数显示各自的K线图
                if hasattr(self, 'data_view_manager'):
                    self.data_view_manager.show_index_data(text)
            # Handle HS/AJ items
            elif text == "沪深京个股" or text == "沪深京A股":
                if hasattr(self, 'data_view_manager'):
                    self.data_view_manager.show_stock_data_by_type("全部A股")
            # Handle stock categories
            elif text in ["全部A股", "上证A股", "深证A股", "创业板", "科创板"]:
                if hasattr(self, 'data_view_manager'):
                    self.data_view_manager.show_stock_data_by_type(text)
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"Failed to handle nav item click: {e}")

    def on_indicator_changed(self, window_index, indicator_name):
        """
        Indicator selection changed
        """
        return self.indicator_interaction_manager.on_indicator_changed(window_index, indicator_name)

    def on_window_count_changed(self, count):
        """
        Chart window count changed
        """
        return self.indicator_interaction_manager.on_window_count_changed(count)
    
    def on_period_changed(self, period):
        """
        Chart period changed
        """
        # Save current period
        self.current_period = period
        logger.info(f"Period changed to: {period}")
        
        if hasattr(self, 'current_stock_code') and self.current_stock_code:
            # If a stock is currently selected, refresh data
            if hasattr(self, 'process_stock_data'):
                self.process_stock_data(self.current_stock_code, self.current_stock_name)

    def keyPressEvent(self, event):
        """
        Handle key press events
        ESC: Return to market tab
        Up/Down: Switch to adjacent stock in tech tab
        Other keys: Show stock search dialog at bottom right
        """
        key = event.key()
        
        # ESC键：返回行情标签页
        if key == Qt.Key_Escape:
            if self.tab_widget.currentWidget() == self.tech_tab:
                logger.info("ESC pressed, returning to market tab")
                self.tab_widget.setCurrentWidget(self.market_tab)
            return
        
        # 上下箭头键：在技术分析界面切换相邻股票
        if key in (Qt.Key_Up, Qt.Key_Down):
            if self.tab_widget.currentWidget() == self.tech_tab:
                direction = 'prev' if key == Qt.Key_Up else 'next'
                self._switch_to_adjacent_stock(direction)
                return
            else:
                # 不在技术分析界面，传递给父类处理
                try:
                    super().keyPressEvent(event)
                except AttributeError:
                    pass
                return
        
        # 左右箭头键：在技术分析界面实现放大缩小功能
        if key in (Qt.Key_Left, Qt.Key_Right):
            if self.tab_widget.currentWidget() == self.tech_tab:
                if key == Qt.Key_Left:
                    # 左箭头：放大（增加柱体数）
                    self._on_zoom_in()
                else:
                    # 右箭头：缩小（减少柱体数）
                    self._on_zoom_out()
                return
            else:
                # 不在技术分析界面，传递给父类处理
                try:
                    super().keyPressEvent(event)
                except AttributeError:
                    pass
                return
        
        # 忽略其他功能键、控制键等特殊按键
        if key in (Qt.Key_Control, Qt.Key_Shift, Qt.Key_Alt, Qt.Key_Meta,
                   Qt.Key_F1, Qt.Key_F2, Qt.Key_F3, Qt.Key_F4, Qt.Key_F5,
                   Qt.Key_F6, Qt.Key_F7, Qt.Key_F8, Qt.Key_F9, Qt.Key_F10,
                   Qt.Key_F11, Qt.Key_F12, Qt.Key_Tab, Qt.Key_CapsLock,
                   Qt.Key_NumLock, Qt.Key_ScrollLock, Qt.Key_Pause,
                   Qt.Key_Insert, Qt.Key_Delete, Qt.Key_Home, Qt.Key_End,
                   Qt.Key_PageUp, Qt.Key_PageDown):
            try:
                super().keyPressEvent(event)
            except AttributeError:
                pass
            return
        
        # 获取按键字符
        text = event.text()
        if not text or not text.isprintable():
            try:
                super().keyPressEvent(event)
            except AttributeError:
                pass
            return
        
        # 显示股票搜索对话框（右下角）
        logger.debug(f"Key pressed: {text}, showing search dialog")
        self._show_global_search_dialog(text)
        
        # Call parent implementation
        try:
            super().keyPressEvent(event)
        except AttributeError:
            pass
    
    def _switch_to_adjacent_stock(self, direction: str):
        """
        切换到相邻的股票或指数
        
        Args:
            direction: 'prev' 上一个 或 'next' 下一个
        """
        try:
            # 获取当前代码
            if not hasattr(self, 'current_stock_code') or not self.current_stock_code:
                logger.warning("没有当前股票/指数，无法切换")
                return
            
            current_code = self.current_stock_code
            logger.info(f"切换到{direction}，当前: {current_code}")
            
            # 判断当前是股票还是指数
            is_index = self._is_index_code(current_code)
            
            # 获取当前显示的列表
            if is_index:
                item_list = self._get_current_index_list()
                item_type = "指数"
            else:
                item_list = self._get_current_stock_list()
                item_type = "股票"
            
            if not item_list:
                logger.warning(f"没有可用的{item_type}列表")
                return
            
            # 标准化当前代码（确保带后缀）
            if '.' not in current_code:
                # 如果没有后缀，添加默认后缀
                if current_code.startswith('sh'):
                    # 沪市代码，如 sh000016
                    symbol = current_code[2:]
                    current_code = f"{symbol}.SH"
                elif current_code.startswith('sz'):
                    # 深市代码，如 sz399001
                    symbol = current_code[2:]
                    current_code = f"{symbol}.SZ"
                elif current_code.startswith('bj'):
                    # 京市代码，如 bj899001
                    symbol = current_code[2:]
                    current_code = f"{symbol}.BJ"
                elif current_code.startswith('6'):
                    current_code = f"{current_code}.SH"
                elif current_code.startswith('8') or current_code.startswith('4'):
                    # 京市股票代码（北交所）
                    current_code = f"{current_code}.BJ"
                else:
                    current_code = f"{current_code}.SZ"
            
            # 找到当前代码在列表中的位置
            try:
                current_index = item_list.index(current_code)
            except ValueError:
                logger.warning(f"当前{item_type} {current_code} 不在列表中，尝试模糊匹配")
                # 尝试模糊匹配（只比较代码部分）
                current_symbol = current_code.split('.')[0] if '.' in current_code else current_code
                for i, code in enumerate(item_list):
                    symbol = code.split('.')[0] if '.' in code else code
                    if symbol == current_symbol:
                        current_index = i
                        current_code = code  # 使用列表中的完整代码
                        break
                else:
                    logger.warning(f"当前{item_type} {current_code} 不在列表中")
                    return
            
            # 按照列表顺序查找下一个项（不跳过任何项）
            if direction == 'prev':
                target_index = current_index - 1
                if target_index < 0:
                    target_index = len(item_list) - 1  # 循环到末尾
            else:  # next
                target_index = current_index + 1
                if target_index >= len(item_list):
                    target_index = 0  # 循环到开头
            
            # 获取目标代码和名称
            target_code = item_list[target_index]
            if is_index:
                target_name = self._get_index_name(target_code)
            else:
                target_name = self._get_stock_name(target_code)
            
            logger.info(f"切换到{item_type}: {target_code} - {target_name}")
            
            # 加载目标K线图（指数和股票使用相同的方法）
            if hasattr(self, 'action_manager'):
                self.action_manager._load_stock_chart(target_code, target_name)
            else:
                logger.warning(f"没有action_manager，无法加载{item_type}")
                
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"切换失败: {e}")

    def _on_prev_stock_clicked(self):
        """
        点击"上一只"按钮时切换到列表中的上一只股票
        """
        logger.info("点击上一只按钮")
        self._switch_to_adjacent_stock('prev')

    def _on_next_stock_clicked(self):
        """
        点击"下一只"按钮时切换到列表中的下一只股票
        """
        logger.info("点击下一只按钮")
        self._switch_to_adjacent_stock('next')

    def _get_current_stock_list(self):
        """
        获取当前显示的股票列表
        从行情窗口的表格中获取当前显示的股票代码列表
        
        Returns:
            list: 股票代码列表
        """
        try:
            # 优先从当前显示的表格中获取股票列表
            if hasattr(self, 'stock_table') and self.stock_table:
                stock_list = []
                row_count = self.stock_table.rowCount()
                
                for row in range(row_count):
                    # 获取代码列的值（通常是第1列）
                    code_item = self.stock_table.item(row, 1)  # 代码列
                    if code_item:
                        code = code_item.text()
                        # 转换代码格式
                        if code.startswith('sh') or code.startswith('sz') or code.startswith('bj'):
                            # 已经是完整格式，如 sh000001, bj899001
                            if code.startswith('sh'):
                                market = 'SH'
                            elif code.startswith('sz'):
                                market = 'SZ'
                            else:
                                market = 'BJ'
                            symbol = code[2:]
                            ts_code = f"{symbol}.{market}"
                        elif code.isdigit():
                            # 纯数字代码，如 000014, 899001
                            if code.startswith('6'):
                                ts_code = f"{code}.SH"
                            elif code.startswith('8') or code.startswith('4'):
                                # 京市股票代码（北交所）
                                ts_code = f"{code}.BJ"
                            else:
                                ts_code = f"{code}.SZ"
                        else:
                            # 已经是ts_code格式
                            ts_code = code
                        
                        stock_list.append(ts_code)
                
                if stock_list:
                    logger.info(f"从表格中获取到 {len(stock_list)} 只股票")
                    return stock_list
            
            # 如果表格中没有数据，尝试从数据管理器获取
            if hasattr(self, 'data_manager') and self.data_manager:
                session = self.data_manager.db_manager.get_session()
                if session:
                    from src.database.models.stock import StockBasic
                    stocks = session.query(StockBasic).all()
                    return [stock.ts_code for stock in stocks if stock.ts_code]
            
            # 如果没有数据，返回空列表
            return []
            
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"获取股票列表失败: {e}")
            return []
    
    def _get_stock_name(self, ts_code: str):
        """
        获取股票名称
        
        Args:
            ts_code: 股票代码
            
        Returns:
            str: 股票名称
        """
        try:
            # 尝试从表格中获取名称
            if hasattr(self, 'stock_table') and self.stock_table:
                row_count = self.stock_table.rowCount()
                for row in range(row_count):
                    code_item = self.stock_table.item(row, 1)
                    name_item = self.stock_table.item(row, 2)
                    if code_item and name_item:
                        code = code_item.text()
                        symbol = ts_code.split('.')[0]
                        # 匹配各种格式：纯数字、sh/sz/bj前缀
                        if code == symbol or code == f"sh{symbol}" or code == f"sz{symbol}" or code == f"bj{symbol}":
                            return name_item.text()
            
            # 从数据库查询
            if hasattr(self, 'data_manager') and self.data_manager:
                session = self.data_manager.db_manager.get_session()
                if session:
                    from src.database.models.stock import StockBasic
                    stock = session.query(StockBasic).filter(StockBasic.ts_code == ts_code).first()
                    if stock and stock.name:
                        return stock.name
            
            # 默认返回代码
            return ts_code
            
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"获取股票名称失败: {e}")
            return ts_code
    
    def _check_stock_data_exists(self, ts_code: str) -> bool:
        """
        检查股票数据文件是否存在
        
        Args:
            ts_code: 股票代码 (如: 600519.SH, 835305.BJ)
            
        Returns:
            bool: 数据文件是否存在
        """
        try:
            # 转换代码格式
            if ts_code.endswith('.SH'):
                market = 'sh'
                tdx_code = f"sh{ts_code[:-3]}"
            elif ts_code.endswith('.SZ'):
                market = 'sz'
                tdx_code = f"sz{ts_code[:-3]}"
            elif ts_code.endswith('.BJ'):
                market = 'bj'
                tdx_code = f"bj{ts_code[:-3]}"
            else:
                # 假设是纯数字代码
                if ts_code.startswith('6'):
                    market = 'sh'
                elif ts_code.startswith('8') or ts_code.startswith('4'):
                    market = 'bj'
                else:
                    market = 'sz'
                tdx_code = f"{market}{ts_code}"
            
            from pathlib import Path
            if hasattr(self, 'data_manager') and self.data_manager:
                tdx_data_path = Path(self.data_manager.config.data.tdx_data_path)
                tdx_file_path = tdx_data_path / market / 'lday' / f'{tdx_code}.day'
                return tdx_file_path.exists()
            
            return False
            
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"检查股票数据文件失败: {e}")
            return False

    def _is_index_code(self, code: str) -> bool:
        """
        判断代码是否为指数代码
        
        Args:
            code: 股票/指数代码
            
        Returns:
            bool: 是否为指数
        """
        # 检查不同格式的指数代码
        if '.' in code:
            # 格式：数字.SH/SZ/BJ
            symbol, market = code.split('.')
            return (symbol.startswith('000') and market == 'SH') or \
                   (symbol.startswith('399') and market == 'SZ') or \
                   (symbol.startswith('899') and market == 'BJ')
        else:
            # 格式：sh/sz/bj+数字
            if code.startswith('sh'):
                return code[2:].startswith('000')
            elif code.startswith('sz'):
                return code[2:].startswith('399')
            elif code.startswith('bj'):
                return code[2:].startswith('899')
            else:
                # 纯数字格式，根据数字开头和市场判断
                # 注意：纯数字格式无法准确判断，这里保守处理
                return False

    def _get_current_index_list(self):
        """
        获取当前显示的指数列表
        从行情窗口的表格中获取当前显示的指数代码列表
        
        Returns:
            list: 指数代码列表
        """
        try:
            # 从当前显示的表格中获取指数列表
            if hasattr(self, 'stock_table') and self.stock_table:
                index_list = []
                row_count = self.stock_table.rowCount()
                
                for row in range(row_count):
                    # 获取代码列的值（通常是第1列）
                    code_item = self.stock_table.item(row, 1)  # 代码列
                    if code_item:
                        code = code_item.text()
                        # 只保留指数代码（沪市000、深市399、京市899）
                        if code.startswith('sh000') or code.startswith('sz399') or code.startswith('bj899'):
                            if code.startswith('sh'):
                                market = 'SH'
                            elif code.startswith('sz'):
                                market = 'SZ'
                            else:
                                market = 'BJ'
                            symbol = code[2:]
                            ts_code = f"{symbol}.{market}"
                            index_list.append(ts_code)
                        elif code.isdigit() and (code.startswith('000') or code.startswith('399') or code.startswith('899')):
                            if code.startswith('000'):
                                ts_code = f"{code}.SH"
                            elif code.startswith('899'):
                                ts_code = f"{code}.BJ"
                            else:
                                ts_code = f"{code}.SZ"
                            index_list.append(ts_code)
                
                if index_list:
                    logger.info(f"从表格中获取到 {len(index_list)} 个指数")
                    return index_list
            
            # 如果表格中没有数据，返回空列表
            return []
            
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"获取指数列表失败: {e}")
            return []

    def _get_index_name(self, ts_code: str):
        """
        获取指数名称
        
        Args:
            ts_code: 指数代码
            
        Returns:
            str: 指数名称
        """
        try:
            # 从硬编码映射中获取名称
            index_name_map = {
                "000001.SH": "上证指数", "000002.SH": "上证A股", "000003.SH": "上证B股",
                "000016.SH": "上证50", "000010.SH": "上证180", "000009.SH": "上证380",
                "000688.SH": "科创50", "000300.SH": "沪深300",
                "399001.SZ": "深证成指", "399002.SZ": "深成指R", "399003.SZ": "深证100",
                "399005.SZ": "中小板指", "399006.SZ": "创业板指",
                "399007.SZ": "深证200", "399008.SZ": "深证700", "399009.SZ": "深证1000",
                "899001.BJ": "北证50", "899050.BJ": "北证创新",
            }
            
            if ts_code in index_name_map:
                return index_name_map[ts_code]
            
            # 尝试从表格中获取名称
            if hasattr(self, 'stock_table') and self.stock_table:
                row_count = self.stock_table.rowCount()
                for row in range(row_count):
                    code_item = self.stock_table.item(row, 1)
                    name_item = self.stock_table.item(row, 2)
                    if code_item and name_item:
                        code = code_item.text()
                        symbol = ts_code.split('.')[0]
                        if code == symbol or code == f"sh{symbol}" or code == f"sz{symbol}" or code == f"bj{symbol}":
                            return name_item.text()
            
            # 默认返回代码
            return ts_code
            
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"获取指数名称失败: {e}")
            return ts_code

    def _check_index_data_exists(self, ts_code: str) -> bool:
        """
        检查指数数据文件是否存在
        
        Args:
            ts_code: 指数代码 (如: 000001.SH, 899001.BJ)
            
        Returns:
            bool: 数据文件是否存在
        """
        try:
            # 转换代码格式
            if ts_code.endswith('.SH'):
                market = 'sh'
                tdx_code = f"sh{ts_code[:-3]}"
            elif ts_code.endswith('.SZ'):
                market = 'sz'
                tdx_code = f"sz{ts_code[:-3]}"
            elif ts_code.endswith('.BJ'):
                market = 'bj'
                tdx_code = f"bj{ts_code[:-3]}"
            else:
                # 假设是纯数字代码
                if ts_code.startswith('000'):
                    market = 'sh'
                    tdx_code = f"sh{ts_code}"
                elif ts_code.startswith('899'):
                    market = 'bj'
                    tdx_code = f"bj{ts_code}"
                else:
                    market = 'sz'
                    tdx_code = f"sz{ts_code}"
            
            from pathlib import Path
            if hasattr(self, 'data_manager') and self.data_manager:
                tdx_data_path = Path(self.data_manager.config.data.tdx_data_path)
                tdx_file_path = tdx_data_path / market / 'lday' / f'{tdx_code}.day'
                return tdx_file_path.exists()
            
            return False
            
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"检查指数数据文件失败: {e}")
            return False

    def _on_zoom_in(self):
        """
        放大K线图（增加柱体数）
        对应工具栏的 + 按钮功能
        """
        try:
            if hasattr(self, 'indicator_interaction_manager') and self.indicator_interaction_manager:
                self.indicator_interaction_manager.on_plus_btn_clicked()
                logger.info("放大K线图（增加柱体数）")
            else:
                logger.warning("没有indicator_interaction_manager，无法放大")
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"放大K线图失败: {e}")

    def _on_zoom_out(self):
        """
        缩小K线图（减少柱体数）
        对应工具栏的 - 按钮功能
        """
        try:
            if hasattr(self, 'indicator_interaction_manager') and self.indicator_interaction_manager:
                self.indicator_interaction_manager.on_minus_btn_clicked()
                logger.info("缩小K线图（减少柱体数）")
            else:
                logger.warning("没有indicator_interaction_manager，无法缩小")
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"缩小K线图失败: {e}")

    def _show_global_search_dialog(self, initial_text=""):
        """
        在右下角显示股票搜索对话框
        
        Args:
            initial_text: 初始搜索文本
        """
        try:
            from src.ui.stock_search_dialog import StockSearchDialog
            
            # 获取数据库管理器
            db_manager = None
            if hasattr(self, 'data_manager') and self.data_manager:
                db_manager = self.data_manager.db_manager
            
            # 创建搜索对话框
            dialog = StockSearchDialog(self, db_manager, initial_text)
            
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

    def on_kline_double_clicked(self, event, dates, opens, highs, lows, closes):
        """
        Double click on K-line chart
        """
        if event.double():
            self.crosshair_enabled = not self.crosshair_enabled
            
            if self.crosshair_enabled:
                logger.info("Double click: Enable crosshair")
                if self.current_kline_data:
                    pos = event.pos()
                    view_box = self.tech_plot_widget.getViewBox()
                    view_pos = view_box.mapSceneToView(pos)
                    x_val = view_pos.x()
                    index = int(round(x_val))
                    index = max(0, min(len(dates) - 1, index))
                    
                    self.current_kline_index = index
                    self.current_mouse_pos = pos
                    self.current_kline_data['index'] = index
                    
                    # Update crosshairs
                    if hasattr(self, 'vline'): self.vline.setValue(index); self.vline.show()
                    if hasattr(self, 'hline'): self.hline.setValue(view_pos.y()); self.hline.show()
                    if hasattr(self, 'volume_vline'): self.volume_vline.setValue(index); self.volume_vline.show()
                    
                    volume_view_box = self.volume_plot_widget.getViewBox()
                    volume_pos = volume_view_box.mapSceneToView(pos)
                    if hasattr(self, 'volume_hline'): self.volume_hline.setValue(volume_pos.y()); self.volume_hline.show()

                    if hasattr(self, 'kdj_vline'): 
                        self.kdj_vline.setValue(index)
                        self.kdj_vline.show()
                    
                    kdj_view_box = self.kdj_plot_widget.getViewBox()
                    kdj_pos = kdj_view_box.mapSceneToView(pos)
                    if hasattr(self, 'kdj_hline'):
                        self.kdj_hline.setValue(kdj_pos.y())
                        self.kdj_hline.show()

                    self.show_info_box()
            else:
                logger.info("Double click: Disable crosshair")
                if hasattr(self, 'vline'): self.vline.hide()
                if hasattr(self, 'hline'): self.hline.hide()
                if hasattr(self, 'volume_vline'): self.volume_vline.hide()
                if hasattr(self, 'volume_hline'): self.volume_hline.hide()
                if hasattr(self, 'kdj_vline'): self.kdj_vline.hide()
                if hasattr(self, 'kdj_hline'): self.kdj_hline.hide()
                if hasattr(self, 'info_text') and self.info_text: self.info_text.hide()

    def on_kline_clicked(self, event, dates, opens, highs, lows, closes):
        """
        Click on K-line chart
        """
        if event.double():
            self.on_kline_double_clicked(event, dates, opens, highs, lows, closes)
        else:
            self.on_ma_clicked(event)

    def on_ma_clicked(self, event):
        """
        Click on MA line
        """
        try:
            pos = event.scenePos()
            view_box = self.tech_plot_widget.getViewBox()
            view_pos = view_box.mapSceneToView(pos)
            x_val = view_pos.x()
            y_val = view_pos.y()
            
            index = int(round(x_val))
            clicked_ma = None
            min_distance = float('inf')
            tolerance = 0.02
            
            y_range = self.tech_plot_widget.viewRange()[1]
            y_min, y_max = y_range
            price_tolerance = (y_max - y_min) * tolerance
            
            if hasattr(self, 'moving_averages'):
                for ma_name, ma_info in self.moving_averages.items():
                    x_data, y_data = ma_info['data']
                    if 0 <= index < len(x_data):
                        ma_value = y_data[index]
                        if ma_value is None: continue 
                        distance = abs(y_val - ma_value)
                        
                        if distance < price_tolerance and distance < min_distance:
                            min_distance = distance
                            clicked_ma = ma_name
            
            if clicked_ma:
                logger.info(f"Clicked MA: {clicked_ma}")
                if not hasattr(self, 'ma_points'): self.ma_points = []
                for p in self.ma_points: self.tech_plot_widget.removeItem(p)
                self.ma_points.clear()
                
                ma_info = self.moving_averages.get(clicked_ma)
                if ma_info:
                    x_data, y_data = ma_info['data']
                    step = max(1, len(x_data) // 20)
                    import math
                    for i in range(0, len(x_data), step):
                        if y_data[i] is not None and not (isinstance(y_data[i], (int, float)) and math.isnan(y_data[i])):
                            point = pg.ScatterPlotItem([x_data[i]], [y_data[i]], size=6, pen=pg.mkPen('w', width=1), brush=pg.mkBrush('w'))
                            self.tech_plot_widget.addItem(point)
                            self.ma_points.append(point)
                
                self.selected_ma = clicked_ma
            else:
                if hasattr(self, 'ma_points'):
                    for p in self.ma_points: self.tech_plot_widget.removeItem(p)
                    self.ma_points.clear()
                self.selected_ma = None
                
                if event.button() == Qt.RightButton:
                    self._show_custom_context_menu(event)

        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"Error handling MA click: {e}")

    def _show_custom_context_menu(self, event):
        menu = QMenu(self) # type: ignore
        if hasattr(self, 'selected_ma') and self.selected_ma:
            modify_action = QAction(f"修改{self.selected_ma}指标参数", self) # type: ignore
            modify_action.triggered.connect(lambda: self.on_modify_indicator(self.selected_ma))
            menu.addAction(modify_action)
        else:
            no_select_action = QAction("未选中均线，请先点击选中均线", self) # type: ignore
            no_select_action.setEnabled(False)
            menu.addAction(no_select_action)
        
        scene_pos = event.pos()
        screen_pos = self.tech_plot_widget.mapToGlobal(scene_pos)
        menu.exec(screen_pos.toPoint())
        event.accept()

    def on_custom_context_menu(self, pos):
        """
        Handle customContextMenuRequested
        """
        menu = QMenu(self.tech_plot_widget)
        if hasattr(self, 'selected_ma') and self.selected_ma:
            modify_action = QAction(f"修改{self.selected_ma}指标参数", self) # type: ignore
            modify_action.triggered.connect(lambda: self.on_modify_indicator(self.selected_ma))
            menu.addAction(modify_action)
        else:
            no_select_action = QAction("未选中均线，请先点击选中均线", self) # type: ignore
            no_select_action.setEnabled(False)
            menu.addAction(no_select_action)
        
        global_pos = self.tech_plot_widget.mapToGlobal(pos)
        menu.exec(global_pos)

    def on_modify_indicator(self, ma_name):
        """
        Dialog to modify indicator parameters
        """
        dialog = QDialog(self) # type: ignore
        dialog.setWindowTitle(f"修改{ma_name}指标参数")
        dialog.setGeometry(300, 300, 300, 200)
        
        layout = QVBoxLayout(dialog)
        
        current_window = int(ma_name.replace("MA", ""))
        
        window_label = QLabel("周期:", dialog)
        layout.addWidget(window_label)
        
        window_input = QLineEdit(dialog)
        window_input.setText(str(current_window))
        layout.addWidget(window_input)
        
        button_layout = QHBoxLayout()
        ok_button = QPushButton("确定", dialog)
        cancel_button = QPushButton("取消", dialog)
        button_layout.addWidget(ok_button)
        button_layout.addWidget(cancel_button)
        layout.addLayout(button_layout)
        
        def on_ok():
            try:
                new_window = int(window_input.text())
                if new_window <= 0: raise ValueError("周期必须大于0")
                logger.info(f"Modify {ma_name} to: {new_window}")
                # TODO: Implement update logic
                dialog.accept()
            except ValueError as e:
                logger.error(f"Input error: {e}")
        
        ok_button.clicked.connect(on_ok)
        cancel_button.clicked.connect(dialog.reject)
        dialog.exec()

    def on_kline_mouse_moved(self, pos, dates, opens, highs, lows, closes):
        """
        Mouse move on K-line chart
        """
        try:
            tech_view_box = self.tech_plot_widget.getViewBox()
            volume_view_box = self.volume_plot_widget.getViewBox()
            kdj_view_box = self.kdj_plot_widget.getViewBox()
            
            x_val = -1
            
            tech_pos = tech_view_box.mapSceneToView(pos)
            if 0 <= tech_pos.x() < len(dates):
                x_val = tech_pos.x()
            else:
                volume_pos = volume_view_box.mapSceneToView(pos)
                if 0 <= volume_pos.x() < len(dates):
                    x_val = volume_pos.x()
                else:
                    kdj_pos = kdj_view_box.mapSceneToView(pos)
                    if 0 <= kdj_pos.x() < len(dates):
                        x_val = kdj_pos.x()
            
            index = int(round(x_val))
            if 0 <= index < len(dates):
                self.current_mouse_pos = pos
                self.current_kline_index = index
                self.current_kline_data['index'] = index
                
                self.update_ma_values_display(index, dates, opens, highs, lows, closes)
                
                # Update volume label
                if hasattr(self, 'volume_values_label') and hasattr(self, 'current_volume_data'):
                    self._update_volume_label(index)

                # Update KDJ label
                if hasattr(self, 'kdj_values_label'):
                    self._update_kdj_label(index)
                
                # Update Crosshair
                if self.crosshair_enabled:
                    if hasattr(self, 'vline'): self.vline.setValue(index); self.vline.show()
                    if hasattr(self, 'hline'): self.hline.setValue(tech_view_box.mapSceneToView(pos).y()); self.hline.show()
                    if hasattr(self, 'volume_vline'): self.volume_vline.setValue(index); self.volume_vline.show()
                    
                    if hasattr(self, 'volume_hline'):
                         volume_pos = volume_view_box.mapSceneToView(pos)
                         self.volume_hline.setValue(volume_pos.y())
                         self.volume_hline.show()

                    if hasattr(self, 'kdj_vline'): self.kdj_vline.setValue(index); self.kdj_vline.show()
                    if hasattr(self, 'kdj_hline'):
                        kdj_pos = kdj_view_box.mapSceneToView(pos)
                        self.kdj_hline.setValue(kdj_pos.y())
                        self.kdj_hline.show()
                    
                    self.show_info_box()
            else:
                if hasattr(self, 'vline'): self.vline.hide()
                if hasattr(self, 'hline'): self.hline.hide()
                if hasattr(self, 'volume_vline'): self.volume_vline.hide()
                if hasattr(self, 'volume_hline'): self.volume_hline.hide()
                if hasattr(self, 'kdj_vline'): self.kdj_vline.hide()
                if hasattr(self, 'kdj_hline'): self.kdj_hline.hide()
                if hasattr(self, 'info_text') and self.info_text: self.info_text.hide()

        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"Error handling mouse move: {e}")

    def _update_volume_label(self, index):
        try:
            # Need to access window_indicators safely
            current_indicator = self.window_indicators[2] if hasattr(self, 'window_indicators') else "VOL"
            # Logic similar to original file... simplified here
            # Ideally delegated to chart_manager or similar
            # For now keep minimal implementation to satisfy variable updates
            pass 
        except (OSError, RuntimeError, ValueError):
            pass

    def _update_kdj_label(self, index):
        try:
             # Logic similar to original file
             pass
        except (OSError, RuntimeError, ValueError):
            pass

    def update_ma_values_display(self, index, dates, opens, highs, lows, closes):
        """
        Update MA values and overlay indicator values
        """
        if not hasattr(self, 'ma_values_label'): return
        if index < 0 or index >= len(dates): return
        
        # 将日期转换为字符串格式
        date_val = dates[index]
        if hasattr(date_val, 'strftime'):
            current_date = date_val.strftime('%Y-%m-%d')
        else:
            current_date = str(date_val)[:10]
        ma_values = {}
        
        if hasattr(self, 'ma_data'):
            for ma in ['MA5', 'MA10', 'MA20', 'MA60']:
                if 0 <= index < len(self.ma_data.get(ma, [])):
                    val = self.ma_data[ma][index]
                    ma_values[ma] = f"{val:.2f}" if val and str(val) != 'nan' else "--"
                else:
                    ma_values[ma] = "--"
        else:
            ma_values = {k: "--" for k in ['MA5', 'MA10', 'MA20', 'MA60']}
            
        ma5_color = 'white'
        ma10_color = 'cyan'
        ma20_color = 'red'
        ma60_color = '#00FF00'
        
        ma_text = f"<font color='#C0C0C0'>日期: {current_date}</font>  <font color='{ma5_color}'>MA5: {ma_values['MA5']}</font>  <font color='{ma10_color}'>MA10: {ma_values['MA10']}</font>  <font color='{ma20_color}'>MA20: {ma_values['MA20']}</font>  <font color='{ma60_color}'>MA60: {ma_values['MA60']}</font>"
        
        # 添加主图叠加指标显示（SAR、BOLL等）
        overlay_text = ""
        
        # SAR指标
        if hasattr(self, 'indicator_buttons') and self.indicator_buttons.get('SAR', {}).isChecked():
            if hasattr(self, 'current_sar_data') and self.current_sar_data is not None and 'sar' in self.current_sar_data:
                sar_list = self.current_sar_data['sar']
                if 0 <= index < len(sar_list):
                    sar_val = sar_list[index]
                    if sar_val is not None and str(sar_val) != 'nan':
                        overlay_text += f"  <font color='white'>SAR: {sar_val:.2f}</font>"
        
        # BOLL指标
        if hasattr(self, 'indicator_buttons') and self.indicator_buttons.get('BOLL', {}).isChecked():
            if hasattr(self, 'current_boll_data') and self.current_boll_data is not None:
                mb_list = self.current_boll_data.get('mb', [])
                up_list = self.current_boll_data.get('up', [])
                dn_list = self.current_boll_data.get('dn', [])
                if 0 <= index < len(mb_list):
                    mb_val = mb_list[index]
                    up_val = up_list[index] if index < len(up_list) else None
                    dn_val = dn_list[index] if index < len(dn_list) else None
                    if mb_val is not None and str(mb_val) != 'nan':
                        overlay_text += f"  <font color='white'>BOLL: {mb_val:.2f}/{up_val:.2f}/{dn_val:.2f}</font>"
        
        self.ma_values_label.setText(ma_text + overlay_text)

    def show_info_box(self):
        """
        Show info box on chart with smart positioning
        When crosshair is near the right edge, show info box on the left side
        """
        try:
            if self.current_kline_index >= 0 and self.current_kline_data:
                dates = self.current_kline_data['dates']
                opens = self.current_kline_data['opens']
                highs = self.current_kline_data['highs']
                lows = self.current_kline_data['lows']
                closes = self.current_kline_data['closes']
                index = self.current_kline_index

                if 0 <= index < len(dates):
                    pre_close = closes[index-1] if index > 0 else closes[index]
                    change = closes[index] - pre_close
                    pct_change = (change / pre_close) * 100 if pre_close != 0 else 0

                    # 获取日期和星期
                    date_val = dates[index]
                    if hasattr(date_val, 'strftime'):
                        date_str = date_val.strftime('%Y-%m-%d')
                        weekday = date_val.weekday()
                    else:
                        date_str = str(date_val)[:10]
                        # 尝试从字符串日期计算星期几
                        try:
                            from datetime import datetime
                            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                            weekday = date_obj.weekday()
                        except:
                            weekday = 0  # 默认周一
                    weekday_str = ['一', '二', '三', '四', '五', '六', '日'][weekday]

                    info_html = f"""
                    <div style="background-color: rgba(0, 0, 0, 0.8); padding: 8px; border: 1px solid #666; color: white; font-family: monospace;">
                    <div style="font-weight: bold;">{date_str}/{weekday_str}</div>
                    <div>开盘: {opens[index]:.2f}</div>
                    <div>最高: {highs[index]:.2f}</div>
                    <div>最低: {lows[index]:.2f}</div>
                    <div>收盘: {closes[index]:.2f}</div>
                    <div>涨跌: {change:.2f}</div>
                    <div>涨幅: {pct_change:.2f}%</div>
                    </div>
                    """

                    if hasattr(self, 'info_text') and self.info_text:
                        self.info_text.setHtml(info_html)

                        if self.current_mouse_pos:
                            view_box = self.tech_plot_widget.getViewBox()
                            view_pos = view_box.mapSceneToView(self.current_mouse_pos)

                            # 获取视图范围
                            x_range = view_box.viewRange()[0]
                            x_min, x_max = x_range[0], x_range[1]
                            view_width = x_max - x_min

                            # 判断十字线是否靠近右侧（超过70%位置）
                            threshold = x_min + view_width * 0.7

                            if view_pos.x() > threshold:
                                # 靠近右侧，信息框显示在十字线左侧
                                self.info_text.setAnchor((1, 1))
                                # 向左偏移约0.5%视图宽度，让信息框紧贴十字线
                                offset = view_width * 0.005
                                self.info_text.setPos(view_pos.x() - offset, view_pos.y())
                            else:
                                # 正常位置，信息框显示在十字线右侧
                                self.info_text.setAnchor((0, 1))
                                self.info_text.setPos(view_pos.x(), view_pos.y())

                            self.info_text.show()
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"Error showing info box: {e}")
