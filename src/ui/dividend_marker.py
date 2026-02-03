#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
分红配股标记模块
在K线图下方显示分红配股标记，支持鼠标悬停显示详细信息
"""

import numpy as np
import pyqtgraph as pg
from pyqtgraph import GraphicsObject
from PySide6.QtCore import QRectF, QPointF, Qt, QDate
from PySide6.QtGui import QPainter, QPicture, QColor, QFont, QPen, QBrush

from src.utils.logger import logger


class DividendMarkerItem(GraphicsObject):
    """
    分红配股标记项
    在K线图下方绘制分红配股标记，参考通达信风格
    标记始终显示在图表的最底部，紧贴x轴
    """

    def __init__(self, dividend_data, kline_dates, plot_widget):
        """
        初始化分红配股标记项

        Args:
            dividend_data: 分红配股数据列表，每项包含：
                - ex_date: 除权除息日 (datetime.date)
                - cash_div: 每股派现(元)
                - share_div: 每股送转(股)
                - record_date: 股权登记日
                - pay_date: 派息日
            kline_dates: K线日期列表 (datetime.date列表)
            plot_widget: pyqtgraph的PlotWidget，用于动态获取视图范围
        """
        GraphicsObject.__init__(self)

        self.dividend_data = dividend_data
        self.kline_dates = kline_dates
        self.plot_widget = plot_widget

        # 计算每个分红数据对应的K线索引
        self.dividend_indices = self._calculate_dividend_indices()

        # 设置标志
        self.setFlag(self.GraphicsItemFlag.ItemSendsGeometryChanges, False)

    def _calculate_dividend_indices(self):
        """
        计算每个分红数据对应的K线索引

        Returns:
            list: 分红数据对应的K线索引列表
        """
        indices = []
        for div in self.dividend_data:
            ex_date = div.get('ex_date')
            if ex_date:
                # 查找除权除息日在K线日期中的位置
                try:
                    # 将日期转换为datetime.date进行比较
                    if hasattr(ex_date, 'date'):
                        ex_date = ex_date.date()

                    for i, kline_date in enumerate(self.kline_dates):
                        if hasattr(kline_date, 'date'):
                            kline_date = kline_date.date()

                        if kline_date == ex_date:
                            indices.append(i)
                            break
                    else:
                        indices.append(-1)  # 未找到对应日期
                except Exception as e:
                    logger.warning(f"计算分红索引失败: {e}")
                    indices.append(-1)
            else:
                indices.append(-1)

        return indices

    def paint(self, p, *args):
        """
        绘制标记
        每次绘制时动态获取当前视图范围，确保标记始终紧贴x轴
        """
        try:
            # 动态获取当前视图范围
            view_range = self.plot_widget.viewRange()
            if not view_range or len(view_range) < 2:
                return

            y_min, y_max = view_range[1]
            y_range = y_max - y_min

            # 计算标记高度（基于Y轴范围的固定比例）
            # 使用较小的比例，确保标记不会占用太多价格区域
            marker_height_ratio = 0.025  # 2.5%的Y轴范围
            marker_height = y_range * marker_height_ratio

            # 标记底部紧贴y_min（x轴），向上延伸
            marker_y_bottom = y_min
            marker_y_top = y_min + marker_height

            # 标记宽度（基于K线索引）
            marker_width = 0.6

            logger.debug(f"绘制分红标记: Y范围 {y_min:.2f} - {y_max:.2f}, 标记高度: {marker_height:.2f}")

            for i, (div, idx) in enumerate(zip(self.dividend_data, self.dividend_indices)):
                if idx < 0:
                    continue  # 跳过未找到对应日期的分红数据

                x = idx  # K线索引作为X坐标

                # 判断分红类型
                cash_div = div.get('cash_div', 0) or 0
                share_div = div.get('share_div', 0) or 0

                # 根据分红类型选择颜色和标记
                if cash_div > 0 and share_div > 0:
                    # 既有分红又有送转 - 紫色
                    marker_type = 'both'
                elif cash_div > 0:
                    # 只有分红 - 红色
                    marker_type = 'cash'
                elif share_div > 0:
                    # 只有送转 - 蓝色
                    marker_type = 'share'
                else:
                    continue  # 没有有效的分红数据

                # 定义标记矩形区域（底部紧贴y_min）
                marker_rect = QRectF(x - marker_width/2, marker_y_bottom, marker_width, marker_height)

                # 根据类型设置颜色
                if marker_type == 'cash':
                    bg_color = QColor(255, 0, 0, 200)  # 红色半透明背景
                    text_color = QColor(255, 255, 255)  # 白色文字
                    text = "分"
                elif marker_type == 'share':
                    bg_color = QColor(0, 128, 255, 200)  # 蓝色半透明背景
                    text_color = QColor(255, 255, 255)  # 白色文字
                    text = "送"
                else:
                    bg_color = QColor(255, 0, 255, 200)  # 紫色半透明背景
                    text_color = QColor(255, 255, 255)  # 白色文字
                    text = "权"

                # 绘制背景矩形
                p.setBrush(QBrush(bg_color))
                p.setPen(QPen(bg_color, 1))
                p.drawRect(marker_rect)

                # 绘制文字
                p.setPen(QPen(text_color, 1))
                # 字体大小根据标记高度自适应
                font_size = max(8, min(12, int(marker_height / y_range * 100)))
                font = QFont("Microsoft YaHei", font_size, QFont.Bold)
                p.setFont(font)
                p.drawText(marker_rect, Qt.AlignCenter, text)

        except Exception as e:
            logger.warning(f"绘制分红标记失败: {e}")

    def boundingRect(self):
        """
        返回边界矩形
        使用一个足够大的矩形，确保标记始终可见
        """
        try:
            view_range = self.plot_widget.viewRange()
            if view_range and len(view_range) >= 2:
                x_min, x_max = view_range[0]
                y_min, y_max = view_range[1]
                return QRectF(x_min, y_min, x_max - x_min, y_max - y_min)
        except:
            pass
        return QRectF(0, 0, 1000, 1000)

    def get_dividend_at_index(self, index):
        """
        获取指定索引位置的分红数据

        Args:
            index: K线索引

        Returns:
            dict: 分红数据，如果没有则返回None
        """
        for i, idx in enumerate(self.dividend_indices):
            if idx == index:
                return self.dividend_data[i]
        return None

    def get_dividend_info_text(self, index):
        """
        获取指定索引位置的分红信息文本（用于悬停显示）

        Args:
            index: K线索引

        Returns:
            str: HTML格式的分红信息文本
        """
        div = self.get_dividend_at_index(index)
        if div is None:
            return None

        # 构建信息文本
        info_lines = []
        info_lines.append("<div style='background-color: rgba(0, 0, 0, 0.9); padding: 8px; border: 1px solid #666; color: white; font-family: monospace; font-size: 12px;'>")
        info_lines.append("<div style='font-weight: bold; color: #FFD700; margin-bottom: 5px;'>权息变动信息</div>")

        # 股权登记日
        record_date = div.get('record_date')
        if record_date:
            if hasattr(record_date, 'strftime'):
                record_date_str = record_date.strftime('%Y-%m-%d')
            else:
                record_date_str = str(record_date)
            info_lines.append(f"<div>股权登记日: {record_date_str}</div>")

        # 除权除息日
        ex_date = div.get('ex_date')
        if ex_date:
            if hasattr(ex_date, 'strftime'):
                ex_date_str = ex_date.strftime('%Y-%m-%d')
            else:
                ex_date_str = str(ex_date)
            info_lines.append(f"<div>除权除息日: {ex_date_str}</div>")

        # 派息日
        pay_date = div.get('pay_date')
        if pay_date:
            if hasattr(pay_date, 'strftime'):
                pay_date_str = pay_date.strftime('%Y-%m-%d')
            else:
                pay_date_str = str(pay_date)
            info_lines.append(f"<div>派息日: {pay_date_str}</div>")

        # 分红方案
        cash_div = div.get('cash_div', 0) or 0
        share_div = div.get('share_div', 0) or 0

        if cash_div > 0 or share_div > 0:
            info_lines.append("<div style='margin-top: 5px; color: #00FF00;'>分红方案:</div>")

            if cash_div > 0:
                # 转换为10派多少元
                cash_div_10 = cash_div * 10
                info_lines.append(f"<div style='padding-left: 10px;'>每10股派现: {cash_div_10:.2f}元</div>")

            if share_div > 0:
                # 转换为10送多少股
                share_div_10 = share_div * 10
                info_lines.append(f"<div style='padding-left: 10px;'>每10股送转: {share_div_10:.2f}股</div>")

        info_lines.append("</div>")

        return "\n".join(info_lines)


class DividendMarkerManager:
    """
    分红配股标记管理器
    管理分红标记的创建、显示和交互
    """

    def __init__(self, plot_widget):
        """
        初始化分红标记管理器

        Args:
            plot_widget: pyqtgraph的PlotWidget
        """
        self.plot_widget = plot_widget
        self.marker_item = None
        self.tooltip_item = None
        self.dividend_data = []

    def set_dividend_data(self, dividend_data, kline_dates):
        """
        设置分红数据并创建标记

        Args:
            dividend_data: 分红配股数据列表
            kline_dates: K线日期列表
        """
        self.dividend_data = dividend_data

        # 移除旧的标记
        self.clear_markers()

        if not dividend_data:
            return

        try:
            # 创建新的标记项，传入plot_widget用于动态获取视图范围
            self.marker_item = DividendMarkerItem(dividend_data, kline_dates, self.plot_widget)

            # 添加到图表
            self.plot_widget.addItem(self.marker_item)

            logger.info(f"成功创建分红标记，共{len(dividend_data)}条分红数据")

        except Exception as e:
            logger.exception(f"创建分红标记失败: {e}")

    def clear_markers(self):
        """
        清除所有标记
        """
        if self.marker_item is not None:
            try:
                self.plot_widget.removeItem(self.marker_item)
            except Exception as e:
                logger.debug(f"移除分红标记时发生错误: {e}")
            self.marker_item = None

        if self.tooltip_item is not None:
            try:
                self.plot_widget.removeItem(self.tooltip_item)
            except Exception as e:
                logger.debug(f"移除分红提示框时发生错误: {e}")
            self.tooltip_item = None

    def show_tooltip(self, index, pos):
        """
        显示分红信息提示框

        Args:
            index: K线索引
            pos: 显示位置 (QPointF)
        """
        if self.marker_item is None:
            return

        # 获取分红信息文本
        info_html = self.marker_item.get_dividend_info_text(index)
        if info_html is None:
            self.hide_tooltip()
            return

        try:
            # 创建或更新提示框
            if self.tooltip_item is None:
                self.tooltip_item = pg.TextItem(anchor=(0, 1))
                self.tooltip_item.setColor(pg.mkColor('w'))
                self.plot_widget.addItem(self.tooltip_item)

            self.tooltip_item.setHtml(info_html)
            self.tooltip_item.setPos(pos.x(), pos.y())
            self.tooltip_item.show()

        except Exception as e:
            logger.exception(f"显示分红提示框失败: {e}")

    def hide_tooltip(self):
        """
        隐藏分红信息提示框
        """
        if self.tooltip_item is not None:
            self.tooltip_item.hide()

    def update_position(self):
        """
        更新标记位置（当图表缩放或滚动时调用）
        由于标记现在动态获取视图范围，此方法只需触发重绘
        """
        if self.marker_item is not None:
            try:
                self.marker_item.update()
                logger.debug("触发分红标记重绘")
            except Exception as e:
                logger.debug(f"更新分红标记位置失败: {e}")

    def has_dividend_at_index(self, index):
        """
        检查指定索引位置是否有分红数据

        Args:
            index: K线索引

        Returns:
            bool: 是否有分红数据
        """
        if self.marker_item is None:
            return False
        return self.marker_item.get_dividend_at_index(index) is not None
