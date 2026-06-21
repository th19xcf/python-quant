#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HSL（换手率）指标绘制器
"""

import pyqtgraph as pg
from .histogram_item import HistogramItem

from .base_drawer import BaseIndicatorDrawer


class HslDrawer(BaseIndicatorDrawer):
    """HSL换手率指标绘制器"""

    @staticmethod
    def draw(view, data, **kwargs):
        """
        绘制HSL指标

        Args:
            view: 图表视图
            data: 包含HSL数据的DataFrame
            **kwargs: 额外参数
        """
        if data is None or data.empty:
            return

        colors = kwargs.get('colors', {
            'hsl': (0, 191, 255),
            'hsl_ma5': (255, 165, 0),
            'hsl_ma10': (255, 0, 255),
        })

        if 'hsl' in data.columns:
            hsl_values = data['hsl'].values
            brushes = []
            for val in hsl_values:
                if val > 10:
                    brushes.append(pg.mkBrush((255, 0, 0, 180)))
                elif val > 5:
                    brushes.append(pg.mkBrush((255, 165, 0, 180)))
                else:
                    brushes.append(pg.mkBrush((0, 191, 255, 180)))

            bar_item = HistogramItem(
                x=data.index,
                y=hsl_values,
                brushes=brushes,
                pens=None
            )
            view.addItem(bar_item)

        if 'hsl_ma5' in data.columns:
            pen = pg.mkPen(color=colors.get('hsl_ma5', (255, 165, 0)), width=1.5)
            view.plot(data.index, data['hsl_ma5'], pen=pen, name='HSL_MA5')

        if 'hsl_ma10' in data.columns:
            pen = pg.mkPen(color=colors.get('hsl_ma10', (255, 0, 255)), width=1.5)
            view.plot(data.index, data['hsl_ma10'], pen=pen, name='HSL_MA10')

        view.addLine(y=5, pen=pg.mkPen((128, 128, 128, 100), width=1, style=pg.QtCore.Qt.DashLine))
        view.addLine(y=10, pen=pg.mkPen((255, 0, 0, 100), width=1, style=pg.QtCore.Qt.DashLine))

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
        if data is None or data.empty or 'hsl' not in data.columns:
            return 0, 20

        max_val = data['hsl'].max()
        upper_limit = max(20, max_val * 1.1)

        return 0, upper_limit

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
        if data is None or data.empty or index < 0 or index >= len(data):
            return "HSL: N/A"

        info_parts = ["HSL:"]

        if 'hsl' in data.columns:
            value = data.iloc[index]['hsl']
            info_parts.append(f"{value:.2f}%")

        if 'hsl_ma5' in data.columns:
            value = data.iloc[index]['hsl_ma5']
            info_parts.append(f"MA5={value:.2f}%")

        if 'hsl_ma10' in data.columns:
            value = data.iloc[index]['hsl_ma10']
            info_parts.append(f"MA10={value:.2f}%")

        return " ".join(info_parts)
