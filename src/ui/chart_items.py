#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
图表项模块，包含自定义的图表项实现
"""

import numpy as np
import pyqtgraph as pg
from pyqtgraph import GraphicsObject
from PySide6.QtCore import QRectF, QPointF, Qt
from PySide6.QtGui import QPainter, QPicture

from src.utils.logger import logger


class CandleStickItem(GraphicsObject):
    """
    优化的K线图项类，具有以下特性：
    1. 只绘制可见区域的K线
    2. 使用numpy数组存储数据，提高访问效率
    3. 精确计算可见区域的K线索引
    4. 减少绘制状态切换
    5. 支持动态数据更新
    """
    
    def __init__(self, data):
        """
        初始化K线图项
        
        Args:
            data: K线数据，列表格式 [(x, open, high, low, close), ...]
        """
        GraphicsObject.__init__(self)
        
        # 使用numpy数组存储数据，提高访问效率
        self.data = np.array(data, dtype=np.float64)
        
        # 预计算上涨/下跌颜色
        self.is_up = self.data[:, 4] >= self.data[:, 1]
        
        # 预计算实体高度
        self.body_height = np.abs(self.data[:, 4] - self.data[:, 1])
        
        # 预计算实体底部位置
        self.body_bottom = np.minimum(self.data[:, 1], self.data[:, 4])
        
        # 缓存可见区域的图片
        self.visible_picture = None
        
        # 缓存上一次的可见区域
        self.last_visible_rect = None
        self.last_visible_indices = None
        
        # 计算边界矩形
        self._bounding_rect = self._calculate_bounding_rect()
        
        # 禁用自动连接sigRangeChanged信号，避免PyQtGraph内部错误
        self.setFlag(self.GraphicsItemFlag.ItemSendsGeometryChanges, False)
        
    def _calculate_bounding_rect(self):
        """
        预计算边界矩形
        """
        if len(self.data) == 0:
            return QRectF(0, 0, 1, 1)
        
        x_min = np.min(self.data[:, 0]) - 0.5
        x_max = np.max(self.data[:, 0]) + 0.5
        y_min = np.min(self.data[:, 3])  # low
        y_max = np.max(self.data[:, 2])  # high
        
        return QRectF(x_min, y_min, x_max - x_min, y_max - y_min)
    
    def get_visible_indices(self, view_rect):
        """
        计算可见区域内的K线索引
        
        Args:
            view_rect: 当前视图矩形
        
        Returns:
            tuple: (start_index, end_index) 可见区域的K线索引范围
        """
        if len(self.data) == 0:
            return 0, 0
        
        # 提取可见区域的x范围
        x_min = view_rect.left()
        x_max = view_rect.right()
        
        # 提取所有K线的x坐标
        x_coords = self.data[:, 0]
        
        # 计算可见的K线索引范围
        start_index = np.searchsorted(x_coords, x_min - 0.5, side='left')
        end_index = np.searchsorted(x_coords, x_max + 0.5, side='right')
        
        # 确保索引在有效范围内
        start_index = max(0, start_index)
        end_index = min(len(self.data), end_index)
        
        return start_index, end_index
    
    def generate_visible_picture(self, view_rect):
        """
        只生成可见区域的K线图片
        
        Args:
            view_rect: 当前视图矩形
        """
        # 计算可见区域的K线索引
        start_idx, end_idx = self.get_visible_indices(view_rect)
        
        # 如果可见区域没有变化，直接返回
        if (self.last_visible_rect == view_rect and 
            self.last_visible_indices == (start_idx, end_idx)):
            return
        
        # 更新缓存
        self.last_visible_rect = view_rect
        self.last_visible_indices = (start_idx, end_idx)
        
        # 创建新的图片
        self.visible_picture = QPicture()
        p = QPainter(self.visible_picture)
        
        # 只绘制可见区域的K线
        for i in range(start_idx, end_idx):
            x = self.data[i, 0]
            open_val = self.data[i, 1]
            high_val = self.data[i, 2]
            low_val = self.data[i, 3]
            close_val = self.data[i, 4]
            
            # 增大K线宽度，减小柱体间隙
            kline_width = 0.9
            
            # 根据涨跌设置颜色，使用更鲜明的颜色
            if self.is_up[i]:
                # 上涨，红色
                color = pg.mkColor(255, 0, 0)  # 红色
            else:
                # 下跌，绿色
                color = pg.mkColor(0, 255, 0)  # 绿色
            
            # 调整笔宽，使用更合适的笔宽
            pen = pg.mkPen(color, width=1.0)
            p.setPen(pen)
            
            # 绘制上下影线（先绘制影线，再绘制实体，避免影线被实体覆盖）
            p.setBrush(Qt.NoBrush)  # 不填充
            p.drawLine(QPointF(x, high_val), QPointF(x, low_val))
            
            # 绘制实体部分 - 使用更精确的位置计算
            p.setBrush(color)  # 填充颜色
            
            # 将实际绘制宽度比例设置为1.0，消除柱体间隙
            actual_width = kline_width * 1.0
            body_rect = QRectF(
                x - actual_width / 2,  # x坐标，居中显示
                self.body_bottom[i],  # 底部位置
                actual_width,  # 实际绘制宽度
                self.body_height[i]  # 高度
            )
            p.drawRect(body_rect)
        
        p.end()
    
    def paint(self, p, *args):
        """
        优化的绘制方法：
        1. 只绘制可见区域
        2. 只生成可见区域的图片
        3. 减少绘制状态切换
        """
        # 获取当前视图矩形
        view_box = self.getViewBox()
        if view_box is not None:
            # 获取当前视图范围
            view_rect = view_box.viewRect()
            
            # 生成可见区域的图片
            self.generate_visible_picture(view_rect)
            
            # 设置裁剪区域
            p.setClipRect(view_rect)
            
            # 绘制可见区域的图片
            if self.visible_picture is not None:
                p.drawPicture(0, 0, self.visible_picture)
    
    def getViewBox(self):
        """
        获取父视图的ViewBox
        
        Returns:
            ViewBox: 父视图的ViewBox对象，或None
        """
        item = self.parentItem()
        while item is not None:
            if hasattr(item, 'viewRect'):
                return item
            item = item.parentItem()
        return None
    
    def boundingRect(self):
        """
        返回K线图项的边界矩形
        
        Returns:
            QRectF: 边界矩形
        """
        return self._bounding_rect
    
    def update_data(self, new_data):
        """
        更新K线数据
        
        Args:
            new_data: 新的K线数据，列表格式 [(x, open, high, low, close), ...]
        """
        # 更新数据
        self.data = np.array(new_data, dtype=np.float64)
        
        # 重新计算预计算属性
        self.is_up = self.data[:, 4] >= self.data[:, 1]
        self.body_height = np.abs(self.data[:, 4] - self.data[:, 1])
        self.body_bottom = np.minimum(self.data[:, 1], self.data[:, 4])
        
        # 更新边界矩形
        self._bounding_rect = self._calculate_bounding_rect()
        
        # 清除缓存
        self.visible_picture = None
        self.last_visible_rect = None
        self.last_visible_indices = None
        
        # 通知视图更新
        self.prepareGeometryChange()
        self.update()
    
    def clear_cache(self):
        """
        清除缓存
        """
        self.visible_picture = None
        self.last_visible_rect = None
        self.last_visible_indices = None
        self.update()
    
    def itemChange(self, change, value):
        """
        重写itemChange方法，避免PyQtGraph尝试连接不存在的sigRangeChanged信号
        
        Args:
            change: 项目变化类型
            value: 变化值
        
        Returns:
            QVariant: 处理后的变化值
        """
        # 调用父类的itemChange方法，但不执行changeParent
        # 这是为了避免PyQtGraph尝试连接ChildGroup对象的sigRangeChanged信号
        if change == self.GraphicsItemChange.ItemParentChange:
            # 直接返回value，不调用父类的itemChange方法
            # 这样可以避免触发changeParent调用
            return value
        return super().itemChange(change, value)
    
    def _updateView(self):
        """
        重写_updateView方法，避免连接不存在的sigRangeChanged信号
        """
        # 什么都不做，避免PyQtGraph尝试连接不存在的sigRangeChanged信号
        pass
