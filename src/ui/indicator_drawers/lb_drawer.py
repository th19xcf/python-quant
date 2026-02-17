#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
LB（量比）指标绘制器
"""

import pyqtgraph as pg
from .histogram_item import HistogramItem


class LbDrawer:
    """LB量比指标绘制器"""

    @staticmethod
    def draw(view, data, **kwargs):
        """
        绘制LB指标

        Args:
            view: 图表视图
            data: 包含LB数据的DataFrame
            **kwargs: 额外参数
        """
        if data is None or data.empty:
            return

        # 获取颜色配置
        colors = kwargs.get('colors', {
            'lb': (0, 255, 127),  # 春绿色
        })

        # 绘制LB柱状图
        if 'lb' in data.columns:
            lb_values = data['lb'].values
            brushes = []
            for val in lb_values:
                if val > 2:  # 高量比
                    brushes.append(pg.mkBrush((255, 0, 0, 180)))  # 红色
                elif val > 1.5:  # 较高量比
                    brushes.append(pg.mkBrush((255, 165, 0, 180)))  # 橙色
                elif val > 1:  # 正常量比
                    brushes.append(pg.mkBrush((0, 255, 127, 180)))  # 绿色
                elif val > 0.5:  # 较低量比
                    brushes.append(pg.mkBrush((0, 191, 255, 180)))  # 蓝色
                else:  # 低量比
                    brushes.append(pg.mkBrush((128, 128, 128, 180)))  # 灰色

            bar_item = HistogramItem(
                x=data.index,
                y=lb_values,
                brushes=brushes,
                pens=None
            )
            view.addItem(bar_item)

        # 添加参考线
        view.addLine(y=1, pen=pg.mkPen((255, 255, 255, 150), width=1.5))  # 基准线
        view.addLine(y=0.5, pen=pg.mkPen((128, 128, 128, 100), width=1, style=pg.QtCore.Qt.DashLine))
        view.addLine(y=1.5, pen=pg.mkPen((128, 128, 128, 100), width=1, style=pg.QtCore.Qt.DashLine))
        view.addLine(y=2, pen=pg.mkPen((255, 0, 0, 100), width=1, style=pg.QtCore.Qt.DashLine))

    @staticmethod
    def get_default_range(data, **kwargs):
        """
        获取默认Y轴范围

        Args:
            data: 数据DataFrame
            **kwargs: 额外参数

        Returns:
            tuple: (最小值, 最大值)
        """
        if data is None or data.empty or 'lb' not in data.columns:
            return 0, 3

        min_val = data['lb'].min()
        max_val = data['lb'].max()

        # 限制显示范围，但保留一定边距
        lower_limit = max(0, min_val * 0.8)
        upper_limit = max(3, max_val * 1.1)

        return lower_limit, upper_limit

    @staticmethod
    def get_info_text(data, index, **kwargs):
        """
        获取信息文本

        Args:
            data: 数据DataFrame
            index: 当前索引
            **kwargs: 额外参数

        Returns:
            str: 信息文本
        """
        if data is None or data.empty or index < 0 or index >= len(data) or 'lb' not in data.columns:
            return "LB: N/A"

        value = data.iloc[index]['lb']

        # 根据量比值给出解读
        interpretation = ""
        if value > 2:
            interpretation = "(放量)"
        elif value > 1.5:
            interpretation = "(温和放量)"
        elif value > 1:
            interpretation = "(正常)"
        elif value > 0.5:
            interpretation = "(缩量)"
        else:
            interpretation = "(极度缩量)"

        return f"LB: {value:.2f} {interpretation}"
