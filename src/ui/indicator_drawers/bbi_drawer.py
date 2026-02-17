#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BBI指标绘制器
"""

import pyqtgraph as pg


class BbiDrawer:
    """BBI指标绘制器"""

    @staticmethod
    def draw(view, data, **kwargs):
        """
        绘制BBI指标

        Args:
            view: 图表视图
            data: 包含BBI数据的DataFrame
            **kwargs: 额外参数
        """
        if data is None or data.empty:
            return

        # 获取颜色配置
        colors = kwargs.get('colors', {
            'bbi': (255, 165, 0),    # 橙色
        })

        # 绘制BBI线
        if 'bbi' in data.columns:
            pen = pg.mkPen(color=colors.get('bbi', (255, 165, 0)), width=2)
            view.plot(data.index, data['bbi'], pen=pen, name='BBI')

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
        if data is None or data.empty or 'bbi' not in data.columns:
            return 0, 100

        min_val = data['bbi'].min()
        max_val = data['bbi'].max()
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
        if data is None or data.empty or index < 0 or index >= len(data) or 'bbi' not in data.columns:
            return "BBI: N/A"

        value = data.iloc[index]['bbi']
        return f"BBI: {value:.2f}"
