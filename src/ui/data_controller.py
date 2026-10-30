#!/usr/bin/env python3

"""DataController: 数据加载与表格显示的聚合入口。

按职责拆分为四个子控制器（行情扫描 / 表格渲染 / 指数 / 个股），
本类只负责组合与持有引用，MainWindow 的既有调用路径保持不变。
"""

from src.ui.data_index_controller import IndexDataController
from src.ui.data_market_renderer import MarketTableRenderer
from src.ui.data_market_scan import MarketScanService
from src.ui.data_stock_controller import StockDataController


class DataController:
    """组合子控制器，视图与服务依赖统一通过 self.window 显式访问。"""

    def __init__(self, window):
        self.window = window
        self.scanner = MarketScanService(window)
        self.renderer = MarketTableRenderer(window)
        self.index = IndexDataController(window)
        self.stock = StockDataController(window, self.scanner, self.renderer)
