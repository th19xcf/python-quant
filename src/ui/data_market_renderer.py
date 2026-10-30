#!/usr/bin/env python3

"""行情表格渲染：清空、单元格构造、涨跌配色与结果写入表格。"""

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import QAbstractItemView, QTableWidgetItem

from src.utils.logger import logger


class MarketTableRenderer:
    """把行情数据渲染到主窗口行情表格。"""

    def __init__(self, window):
        self.window = window

    def _clear_market_table_display(self):
        """清空行情表格，避免旧菜单数据残留。"""
        if not hasattr(self.window, 'stock_table') or self.window.stock_table is None:
            return

        self.window.stock_table.setSortingEnabled(False)
        self.window.stock_table.clearContents()
        self.window.stock_table.setRowCount(0)

    @staticmethod
    def _price_color(text_value, base_value):
        """
        统一涨跌配色：涨红(255,0,0)、跌绿(0,255,0)、平灰(204,204,204)。

        base_value 为 None 时按文本符号判断（涨跌幅列，如 +0.00 仍判红）；
        否则与 base_value（昨收价）比较（价格列）。
        """
        text = str(text_value)
        if base_value is None:
            if text.startswith("+") or (text.replace(".", "").isdigit() and float(text) > 0):
                return QColor(255, 0, 0)
            if text.startswith("-"):
                return QColor(0, 255, 0)
            return QColor(204, 204, 204)

        number = float(text) if text != "-" else 0.0
        if number > base_value:
            return QColor(255, 0, 0)
        if number < base_value:
            return QColor(0, 255, 0)
        return QColor(204, 204, 204)

    def _build_market_cell(self, col, value, data_row):
        """构造行情单元格：数字列右对齐，并按涨跌设置前景色。"""
        item = QTableWidgetItem(value)

        # 设置对齐方式
        if col in [3, 4, 5, 6, 7, 8, 9, 10, 11, 12]:
            item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)

        # 获取昨收价用于比较
        preclose = float(data_row[11]) if len(data_row) > 11 and data_row[11] != "-" else 0.0

        # 设置通达信风格的颜色
        if col in (3, 5):  # 涨跌幅%
            item.setForeground(self._price_color(value, None))
        elif col in (4, 8, 9, 10):  # 现价/今开/最高/最低
            item.setForeground(self._price_color(value, preclose))

        return item

    def _append_market_rows_to_table(self, table, rows):
        """清空表格并逐行重建：关闭排序 → 释放旧 item → 插入新行。"""
        # 清空现有数据前先关闭排序
        table.setSortingEnabled(False)

        # 清空现有数据，手动释放QTableWidgetItem对象
        for row in range(table.rowCount()):
            for col in range(table.columnCount()):
                item = table.takeItem(row, col)
                if item:
                    del item
        table.setRowCount(0)

        # 添加数据到表格
        for data_row in rows:
            # 添加行
            row_pos = table.rowCount()
            table.insertRow(row_pos)
            # 设置数据
            for col, value in enumerate(data_row):
                table.setItem(row_pos, col, self._build_market_cell(col, value, data_row))

    def _render_market_result(self, result):
        """在 GUI 线程把行情结果写入表格，并更新日志与状态栏。"""
        if result["success"]:
            # 检查是否有table_data键（股票数据）
            if "table_data" in result:
                self._append_market_rows_to_table(self.window.stock_table, result["table_data"])

                # 仅在当前页面已有有效排序时恢复排序
                if hasattr(self.window, 'table_interaction_manager'):
                    self.window.table_interaction_manager.restore_sort_state()

                logger.info(f"{result['stock_type']}数据显示完成")
                self.window.statusBar().showMessage(
                    f"成功显示{result['stock_count']}只{result['stock_type']}股票的最新交易日数据", 3000
                )
            return

        self.window._clear_market_table_display()
        self.window.statusBar().showMessage(result["message"], 5000)

    @staticmethod
    def _to_float(text):
        """把带千分位/百分号的文本转为 float，无法解析时返回 0.0。"""
        try:
            if text is None:
                return 0.0
            cleaned = str(text).replace(',', '').replace('%', '').strip()
            return float(cleaned) if cleaned and cleaned != '-' else 0.0
        except (ValueError, TypeError):
            return 0.0

    def _add_table_row(self, data_row):
        """
        Helper to add a row to stock_table with coloring
        """
        row_pos = self.window.stock_table.rowCount()
        self.window.stock_table.insertRow(row_pos)

        preclose = self._to_float(data_row[11]) if len(data_row) > 11 else 0.0

        for col, value in enumerate(data_row):
            item = QTableWidgetItem(value)

            # Align right for numbers
            if col in [3, 4, 5, 6, 7, 8, 9, 10, 11, 12]:
                item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)

            # Colors
            if col == 3 or col == 5: # pct_chg or change
                if str(value).startswith("+") or self._to_float(value) > 0:
                    item.setForeground(QColor(255, 0, 0))
                elif str(value).startswith("-"):
                    item.setForeground(QColor(0, 255, 0))
                else:
                    item.setForeground(QColor(204, 204, 204))
            elif col in [4, 8, 9, 10]:  # 现价/开盘价/最高价/最低价
                current_val = self._to_float(value)
                if current_val > preclose:
                    item.setForeground(QColor(255, 0, 0))
                elif current_val < preclose:
                    item.setForeground(QColor(0, 255, 0))
                else:
                    item.setForeground(QColor(204, 204, 204))

            self.window.stock_table.setItem(row_pos, col, item)

    def _format_color_item(self, item, value):
        try:
            val = float(value)
            if val > 0:
                item.setForeground(QColor(255, 0, 0))
            elif val < 0:
                item.setForeground(QColor(0, 255, 0))
            else:
                item.setForeground(QColor(200, 200, 200))
        except Exception as e:
            logger.debug(f"设置涨跌单元格颜色失败: {e}", exc_info=True)
