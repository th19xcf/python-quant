#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
EXPMA指标绘制器
"""

import pyqtgraph as pg
from .histogram_item import HistogramItem


class ExpmaDrawer:
    """EXPMA指标绘制器"""

    @staticmethod
    def draw(view, data, **kwargs):
        """
        绘制EXPMA指标

        Args:
            view: 图表视图
            data: 包含EXPMA数据的DataFrame
            **kwargs: 额外参数
        """
        if data is None or data.empty:
            return

        # 获取颜色配置
        colors = kwargs.get('colors', {
            'expma12': (255, 255, 0),    # 黄色
            'expma50': (255, 0, 255),    # 紫色
        })

        # 绘制EXPMA12
        if 'expma12' in data.columns:
            pen = pg.mkPen(color=colors.get('expma12', (255, 255, 0)), width=1.5)
            view.plot(data.index, data['expma12'], pen=pen, name='EXPMA12')

        # 绘制EXPMA50
        if 'expma50' in data.columns:
            pen = pg.mkPen(color=colors.get('expma50', (255, 0, 255)), width=1.5)
            view.plot(data.index, data['expma50'], pen=pen, name='EXPMA50')

        # 绘制其他周期的EXPMA
        for col in data.columns:
            if col.startswith('expma') and col not in ['expma12', 'expma50', 'expma']:
                pen = pg.mkPen(color=(128, 128, 128), width=1)
                view.plot(data.index, data[col], pen=pen, name=col.upper())

        # 绘制默认EXPMA
        if 'expma' in data.columns and 'expma12' not in data.columns:
            pen = pg.mkPen(color=colors.get('expma', (255, 255, 0)), width=1.5)
            view.plot(data.index, data['expma'], pen=pen, name='EXPMA')

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
        if data is None or data.empty:
            return 0, 100

        expma_cols = [col for col in data.columns if col.startswith('expma')]
        if not expma_cols:
            return 0, 100

        min_val = data[expma_cols].min().min()
        max_val = data[expma_cols].max().max()
        padding = (max_val - min_val) * 0.1

        return min_val - padding, max_val + padding

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
            return "EXPMA: N/A"

        info_parts = ["EXPMA:"]

        # 优先显示EXPMA12和EXPMA50
        if 'expma12' in data.columns:
            value = data.iloc[index]['expma12']
            info_parts.append(f"12={value:.2f}")

        if 'expma50' in data.columns:
            value = data.iloc[index]['expma50']
            info_parts.append(f"50={value:.2f}")

        # 显示默认EXPMA
        if 'expma' in data.columns and 'expma12' not in data.columns:
            value = data.iloc[index]['expma']
            info_parts.append(f"{value:.2f}")

        return " ".join(info_parts)
