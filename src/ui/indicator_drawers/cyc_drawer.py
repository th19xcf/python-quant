#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CYC（成本均线）指标绘制器
"""

import pyqtgraph as pg


class CycDrawer:
    """CYC成本均线指标绘制器"""

    @staticmethod
    def draw(view, data, **kwargs):
        """
        绘制CYC指标

        Args:
            view: 图表视图
            data: 包含CYC数据的DataFrame
            **kwargs: 额外参数
        """
        if data is None or data.empty:
            return

        # 获取颜色配置
        colors = kwargs.get('colors', {
            'cyc5': (255, 255, 0),     # 黄色 - 短期
            'cyc13': (255, 165, 0),    # 橙色 - 中期
            'cyc34': (255, 0, 255),    # 紫色 - 长期
            'cyc_inf': (0, 255, 255),  # 青色 - 无穷
        })

        # 绘制CYC5（短期成本均线）
        if 'cyc5' in data.columns:
            pen = pg.mkPen(color=colors.get('cyc5', (255, 255, 0)), width=1.5)
            view.plot(data.index, data['cyc5'], pen=pen, name='CYC5')

        # 绘制CYC13（中期成本均线）
        if 'cyc13' in data.columns:
            pen = pg.mkPen(color=colors.get('cyc13', (255, 165, 0)), width=1.5)
            view.plot(data.index, data['cyc13'], pen=pen, name='CYC13')

        # 绘制CYC34（长期成本均线）
        if 'cyc34' in data.columns:
            pen = pg.mkPen(color=colors.get('cyc34', (255, 0, 255)), width=1.5)
            view.plot(data.index, data['cyc34'], pen=pen, name='CYC34')

        # 绘制CYC无穷（无穷成本均线）
        if 'cyc_inf' in data.columns:
            pen = pg.mkPen(color=colors.get('cyc_inf', (0, 255, 255)), width=2)
            view.plot(data.index, data['cyc_inf'], pen=pen, name='CYC∞')

        # 绘制默认CYC
        if 'cyc' in data.columns:
            pen = pg.mkPen(color=colors.get('cyc', (255, 165, 0)), width=1.5)
            view.plot(data.index, data['cyc'], pen=pen, name='CYC')

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

        cyc_cols = [col for col in data.columns if col.startswith('cyc')]
        if not cyc_cols:
            return 0, 100

        min_val = data[cyc_cols].min().min()
        max_val = data[cyc_cols].max().max()
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
            return "CYC: N/A"

        info_parts = ["CYC:"]

        # 优先显示常用周期
        if 'cyc5' in data.columns:
            value = data.iloc[index]['cyc5']
            info_parts.append(f"5={value:.2f}")

        if 'cyc13' in data.columns:
            value = data.iloc[index]['cyc13']
            info_parts.append(f"13={value:.2f}")

        if 'cyc34' in data.columns:
            value = data.iloc[index]['cyc34']
            info_parts.append(f"34={value:.2f}")

        if 'cyc_inf' in data.columns:
            value = data.iloc[index]['cyc_inf']
            info_parts.append(f"∞={value:.2f}")

        # 显示默认CYC
        if 'cyc' in data.columns and 'cyc13' not in data.columns:
            value = data.iloc[index]['cyc']
            info_parts.append(f"{value:.2f}")

        return " ".join(info_parts)
