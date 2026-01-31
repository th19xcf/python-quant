#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
表格交互管理器
"""

from src.utils.logger import logger


class TableInteractionManager:
    """表格交互管理器"""

    def __init__(self, window):
        self.window = window

    def on_header_clicked(self, logical_index):
        """处理表头点击事件"""
        logger.debug(f"表头点击: {logical_index}")
        window = self.window
        try:
            current_state = window.column_sort_states.get(logical_index, 0)
            next_state = (current_state + 1) % 3
            window.column_sort_states[logical_index] = next_state

            from PySide6.QtCore import Qt
            from PySide6.QtWidgets import QTableWidgetItem
            from PySide6.QtGui import QColor

            if next_state == 1:
                window.stock_table.setSortingEnabled(True)
                window.stock_table.sortItems(logical_index, Qt.AscendingOrder)
                window.current_sorted_column = logical_index
            elif next_state == 2:
                window.stock_table.setSortingEnabled(True)
                window.stock_table.sortItems(logical_index, Qt.DescendingOrder)
                window.current_sorted_column = logical_index
            else:
                window.stock_table.setSortingEnabled(False)

                current_data = []
                row_count = window.stock_table.rowCount()
                for row in range(row_count):
                    row_data = []
                    for col in range(window.stock_table.columnCount()):
                        item = window.stock_table.item(row, col)
                        row_data.append(item.text() if item else "")
                    current_data.append(row_data)

                window.stock_table.setRowCount(0)

                for row_data in current_data:
                    row_pos = window.stock_table.rowCount()
                    window.stock_table.insertRow(row_pos)
                    for col, value in enumerate(row_data):
                        item = QTableWidgetItem(value)
                        if col in [3, 4, 5, 6, 7, 8, 9, 10, 11, 12]:
                            item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                        if col == 3:
                            if value.startswith("+") or (value.replace(".", "").isdigit() and float(value) > 0):
                                item.setForeground(QColor(255, 0, 0))
                            elif value.startswith("-"):
                                item.setForeground(QColor(0, 255, 0))
                            else:
                                item.setForeground(QColor(204, 204, 204))
                        elif col == 5:
                            if value.startswith("+") or (value.replace(".", "").isdigit() and float(value) > 0):
                                item.setForeground(QColor(255, 0, 0))
                            elif value.startswith("-"):
                                item.setForeground(QColor(0, 255, 0))
                            else:
                                item.setForeground(QColor(204, 204, 204))
                        elif col == 4:
                            preclose = float(row_data[11]) if len(row_data) > 11 and row_data[11] != "-" else 0.0
                            current_price = float(value) if value != "-" else 0.0
                            if current_price > preclose:
                                item.setForeground(QColor(255, 0, 0))
                            elif current_price < preclose:
                                item.setForeground(QColor(0, 255, 0))
                            else:
                                item.setForeground(QColor(204, 204, 204))
                        elif col == 8:
                            preclose = float(row_data[11]) if len(row_data) > 11 and row_data[11] != "-" else 0.0
                            open_price = float(value) if value != "-" else 0.0
                            if open_price > preclose:
                                item.setForeground(QColor(255, 0, 0))
                            elif open_price < preclose:
                                item.setForeground(QColor(0, 255, 0))
                            else:
                                item.setForeground(QColor(204, 204, 204))
                        elif col == 9:
                            preclose = float(row_data[11]) if len(row_data) > 11 and row_data[11] != "-" else 0.0
                            high_price = float(value) if value != "-" else 0.0
                            if high_price > preclose:
                                item.setForeground(QColor(255, 0, 0))
                            elif high_price < preclose:
                                item.setForeground(QColor(0, 255, 0))
                            else:
                                item.setForeground(QColor(204, 204, 204))
                        elif col == 10:
                            preclose = float(row_data[11]) if len(row_data) > 11 and row_data[11] != "-" else 0.0
                            low_price = float(value) if value != "-" else 0.0
                            if low_price > preclose:
                                item.setForeground(QColor(255, 0, 0))
                            elif low_price < preclose:
                                item.setForeground(QColor(0, 255, 0))
                            else:
                                item.setForeground(QColor(204, 204, 204))
                        window.stock_table.setItem(row_pos, col, item)

                window.stock_table.setSortingEnabled(True)
                window.current_sorted_column = -1
        except Exception as e:
            logger.exception(f"处理表头点击事件失败: {e}")

    def on_stock_double_clicked(self, row, column):
        """处理股票双击事件"""
        logger.debug(f"表格双击: row={row}, column={column}")
        window = self.window
        try:
            code_item = window.stock_table.item(row, 1)
            name_item = window.stock_table.item(row, 2)
            if not code_item or not name_item:
                return

            code = code_item.text()
            name = name_item.text()
            logger.debug(f"双击了股票: {name}({code})")

            if code.startswith('sh'):
                market = 'sh'
                tdx_code = code
                ts_code = f'{code[2:]}.SH'
            elif code.startswith('sz'):
                market = 'sz'
                tdx_code = code
                ts_code = f'{code[2:]}.SZ'
            elif code.startswith('6'):
                market = 'sh'
                tdx_code = f'sh{code}'
                ts_code = f'{code}.SH'
            elif code.endswith('.SH'):
                market = 'sh'
                tdx_code = f'sh{code[:-3]}'
                ts_code = code
            elif code.endswith('.SZ'):
                market = 'sz'
                tdx_code = f'sz{code[:-3]}'
                ts_code = code
            else:
                market = 'sz'
                tdx_code = f'sz{code}'
                ts_code = f'{code}.SZ'

            from pathlib import Path
            tdx_data_path = Path(window.data_manager.config.data.tdx_data_path)
            tdx_file_path = tdx_data_path / market / 'lday' / f'{tdx_code}.day'

            window.statusBar().showMessage(f"正在加载 {name}({code}) 数据...", 0)
            window.tab_widget.setCurrentIndex(1)
            
            # 显示进度条
            if hasattr(window, 'progress_bar'):
                window.progress_bar.setVisible(True)
                window.progress_bar.setValue(0)

            from src.ui.async_processing import DataReadThread
            window.data_read_thread = DataReadThread(str(tdx_file_path), name, code)

            window.data_read_thread.data_read_completed.connect(self.on_data_read_completed)
            window.data_read_thread.data_read_error.connect(self.on_data_read_error)
            window.data_read_thread.data_read_progress.connect(self.on_data_read_progress)

            window.data_read_thread.start()
        except Exception as e:
            logger.exception(f"处理股票双击事件失败: {e}")
            window.statusBar().showMessage(f"处理股票双击事件失败: {str(e)[:50]}...", 5000)

    def on_data_read_completed(self, df, name, code):
        """数据读取完成"""
        window = self.window
        try:
            window.statusBar().showMessage(f"正在计算 {name}({code}) 技术指标...", 0)
            
            # 更新进度条到50%
            if hasattr(window, 'progress_bar'):
                window.progress_bar.setValue(50)

            from src.ui.async_processing import IndicatorCalculateThread
            window.indicator_calculate_thread = IndicatorCalculateThread(df)

            window.indicator_calculate_thread.indicator_calculated.connect(lambda result_df: self.on_indicator_calculated(result_df, name, code))
            window.indicator_calculate_thread.indicator_calculate_error.connect(self.on_indicator_calculate_error)
            window.indicator_calculate_thread.indicator_calculate_progress.connect(self.on_indicator_calculate_progress)

            window.indicator_calculate_thread.start()
        except Exception as e:
            logger.exception(f"处理数据读取完成信号失败: {e}")
            window.statusBar().showMessage(f"处理数据失败: {str(e)[:50]}...", 5000)
            if hasattr(window, 'progress_bar'):
                window.progress_bar.setVisible(False)

    def on_data_read_error(self, error_msg):
        """数据读取错误"""
        self.window.statusBar().showMessage(error_msg, 5000)
        logger.error(f"数据读取错误: {error_msg}")
        if hasattr(self.window, 'progress_bar'):
            self.window.progress_bar.setVisible(False)

    def on_data_read_progress(self, progress, total):
        """数据读取进度"""
        logger.debug(f"数据读取进度: {progress}%，共{total}条记录")
        window = self.window
        if hasattr(window, 'progress_bar'):
            window.progress_bar.setValue(progress // 2)  # 数据读取占50%

    def on_indicator_calculated(self, df, name, code):
        """指标计算完成"""
        window = self.window
        try:
            window.statusBar().showMessage(f"正在绘制 {name}({code}) 图表...", 0)
            
            # 更新进度条到90%
            if hasattr(window, 'progress_bar'):
                window.progress_bar.setValue(90)

            window.plot_k_line(df, name, code)
            window.statusBar().showMessage(f"{name}({code}) 图表绘制完成", 5000)
            
            # 更新进度条到100%并隐藏
            if hasattr(window, 'progress_bar'):
                window.progress_bar.setValue(100)
                window.progress_bar.setVisible(False)
        except Exception as e:
            logger.exception(f"处理指标计算完成信号失败: {e}")
            window.statusBar().showMessage(f"绘制图表失败: {str(e)[:50]}...", 5000)
            if hasattr(window, 'progress_bar'):
                window.progress_bar.setVisible(False)

    def on_indicator_calculate_error(self, error_msg):
        """指标计算错误"""
        logger.error(f"指标计算错误: {error_msg}")
        self.window.statusBar().showMessage(error_msg, 5000)

    def on_indicator_calculate_progress(self, progress, total):
        """指标计算进度"""
        logger.debug(f"指标计算进度: {progress}%，共{total}个步骤")
