#!/usr/bin/env python3

"""
主窗口导航 Mixin：股票/指数点击、导航树联动、键盘与按钮切换标的。
"""

import contextlib

from PySide6.QtCore import Qt

from src.utils.logger import logger


class MainWindowNavigationMixin:
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

        market_view_items = {
            "沪深京指数", "沪市指数", "深市指数", "京市指数", "创业板指", "科创板指",
            "沪深京个股", "沪深京A股", "全部A股", "上证A股", "深证A股",
            "京市个股", "创业板", "科创板", "开放式基金", "封闭式基金"
        }

        if text in market_view_items:
            previous_market_view = getattr(self, 'current_market_view', None)
            if previous_market_view != text and hasattr(self, 'table_interaction_manager'):
                self.table_interaction_manager.reset_sort_state()
            self.current_market_view = text

        # Switch to market tab
        self.tab_widget.setCurrentIndex(0)

        try:
            # Handle index items
            if text == "沪深京指数":
                # 点击沪深京指数时显示所有指数
                if hasattr(self, 'data_view_manager'):
                    self.data_view_manager.show_index_overview()
            elif text == "沪市指数":
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
                # 按 index_basic 的 category 展示对应指数列表（不做名称到单一代码的强制映射）
                category_name = "创业板" if text == "创业板指" else "科创板"
                if hasattr(self, 'data_view_manager'):
                    self.data_view_manager.show_index_data_by_category(category_name)
            # Handle HS/AJ items
            elif text == "沪深京个股" or text == "沪深京A股":
                if hasattr(self, 'data_view_manager'):
                    self.data_view_manager.show_stock_data_by_type("全部A股")
            # Handle stock categories
            elif text in ["全部A股", "上证A股", "深证A股", "京市个股", "创业板", "科创板"]:
                if hasattr(self, 'data_view_manager'):
                    self.data_view_manager.show_stock_data_by_type(text)
            # Handle fund items
            elif text == "开放式基金":
                # 显示ETF基金数据
                self.fund_section.show_etf_funds()
            elif text == "封闭式基金":
                # 显示封闭式基金数据
                self.fund_section.show_closed_funds()
        except (OSError, RuntimeError, ValueError) as e:
            logger.exception(f"Failed to handle nav item click: {e}")

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
                with contextlib.suppress(AttributeError):
                    super().keyPressEvent(event)
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
                with contextlib.suppress(AttributeError):
                    super().keyPressEvent(event)
                return

        # 忽略其他功能键、控制键等特殊按键
        if key in (Qt.Key_Control, Qt.Key_Shift, Qt.Key_Alt, Qt.Key_Meta,
                   Qt.Key_F1, Qt.Key_F2, Qt.Key_F3, Qt.Key_F4, Qt.Key_F5,
                   Qt.Key_F6, Qt.Key_F7, Qt.Key_F8, Qt.Key_F9, Qt.Key_F10,
                   Qt.Key_F11, Qt.Key_F12, Qt.Key_Tab, Qt.Key_CapsLock,
                   Qt.Key_NumLock, Qt.Key_ScrollLock, Qt.Key_Pause,
                   Qt.Key_Insert, Qt.Key_Delete, Qt.Key_Home, Qt.Key_End,
                   Qt.Key_PageUp, Qt.Key_PageDown):
            with contextlib.suppress(AttributeError):
                super().keyPressEvent(event)
            return

        # 获取按键字符
        text = event.text()
        if not text or not text.isprintable():
            with contextlib.suppress(AttributeError):
                super().keyPressEvent(event)
            return

        # 显示股票搜索对话框（右下角）
        logger.debug(f"Key pressed: {text}, showing search dialog")
        self._show_global_search_dialog(text)

        # Call parent implementation
        with contextlib.suppress(AttributeError):
            super().keyPressEvent(event)

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
                elif current_code.startswith('8') or current_code.startswith('4') or current_code.startswith('92'):
                    # 京市股票代码（北交所，8、4、92开头）
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
                            if code.startswith('6') or code.startswith('51'):
                                # 沪市股票（6开头）和沪市ETF（51开头）
                                ts_code = f"{code}.SH"
                            elif code.startswith('8') or code.startswith('4') or code.startswith('92'):
                                # 京市股票代码（北交所，8、4、92开头）
                                ts_code = f"{code}.BJ"
                            elif code.startswith('15'):
                                # 深市ETF（15开头）
                                ts_code = f"{code}.SZ"
                            else:
                                # 其他代码默认为深市
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
                return self.data_manager.list_stock_ts_codes()

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
                name = self.data_manager.get_stock_name(ts_code)
                if name:
                    return name

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
            # 直接从表格中获取名称，不再使用硬编码映射

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
