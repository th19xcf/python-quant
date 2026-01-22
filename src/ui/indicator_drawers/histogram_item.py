import pyqtgraph as pg
import numpy as np
from pyqtgraph import Point

class HistogramItem(pg.GraphicsObject):
    """
    自定义柱状图项，用于绘制MACD柱状图
    """
    
    def __init__(self, data, brush=None):
        """
        初始化柱状图项
        
        Args:
            data: 柱状图数据，格式为[(x, base, height), ...]
            brush: 画笔，用于填充柱状图
        """
        pg.GraphicsObject.__init__(self)
        self.data = data
        self.brush = brush if brush is not None else pg.mkBrush('w')
        self.generatePicture()
    
    def generatePicture(self):
        """
        生成柱状图的Picture对象
        """
        self.picture = pg.QtGui.QPicture()
        p = pg.QtGui.QPainter(self.picture)
        
        for i, (x, base, height) in enumerate(self.data):
            # 获取当前柱状图的画笔
            if isinstance(self.brush, list) and i < len(self.brush):
                # 使用列表中对应的画笔
                brush = self.brush[i]
            else:
                # 使用默认画笔
                brush = self.brush
            
            p.setBrush(brush)
            p.setPen(pg.mkPen(None))  # 不绘制边框
            
            # 正确的坐标计算：根据height的正负值确定柱体方向
            if height >= 0:
                # 正柱体：从base向上绘制
                rect = pg.QtCore.QRectF(x - 0.4, base, 0.8, height)
            else:
                # 负柱体：从base向下绘制
                rect = pg.QtCore.QRectF(x - 0.4, base + height, 0.8, abs(height))
            p.drawRect(rect)
        
        p.end()
    
    def paint(self, p, *args):
        """
        绘制柱状图
        """
        p.drawPicture(0, 0, self.picture)
    
    def boundingRect(self):
        """
        返回柱状图的边界矩形
        """
        return pg.QtCore.QRectF(self.picture.boundingRect())