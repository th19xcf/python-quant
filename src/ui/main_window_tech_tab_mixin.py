#!/usr/bin/env python3

"""
主窗口技术分析页 Mixin：周期工具栏与 K线/成交量/KDJ 图表画布创建。
"""

import pyqtgraph as pg
from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QSizePolicy,
    QSplitter,
    QVBoxLayout,
    QWidget,
)


class MainWindowTechTabMixin:
    """技术分析标签页：周期/柱体数/复权工具栏与三个画布。"""

    def create_tech_tab(self):
        """
        创建技术分析标签页
        """
        tech_layout = QVBoxLayout(self.tech_tab)

        self._create_tech_toolbar(tech_layout)
        self._create_tech_charts(tech_layout)

        # 添加指标选择窗口
        self.create_indicator_selection()
        # 设置指标选择栏的尺寸策略，确保显示完整
        self.indicator_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.indicator_widget.setMinimumHeight(36)  # 确保高度足够显示按钮
        tech_layout.addWidget(self.indicator_widget)

        # 保存k线图数据项
        self.candle_plot_item = None
        self.volume_bar_item = None
        self.volume_ma_item = None

        # 初始化完成后，设置为3个窗口模式
        self.indicator_interaction_manager.on_window_count_changed(3, True)

    def _create_tech_toolbar(self, tech_layout):
        """
        创建技术分析标签页顶部工具栏：周期切换、柱体数、复权与切换股票按钮
        """
        toolbar = QWidget()
        toolbar_layout = QHBoxLayout(toolbar)
        toolbar.setStyleSheet("background-color: #222222;")

        # 添加周期按钮
        self.period_buttons = {
            '日线': QPushButton('📅 日线'),
            '周线': QPushButton('📆 周线'),
            '月线': QPushButton('📈 月线')
        }

        # 设置按钮样式
        button_style = """
        QPushButton {
            background-color: #333333;
            color: #C0C0C0;
            border: 1px solid #444444;
            padding: 6px 12px;
            border-radius: 4px;
            font-family: 'Microsoft YaHei';
            font-size: 12px;
        }
        QPushButton:hover {
            background-color: #444444;
        }
        QPushButton:checked {
            background-color: #555555;
            border: 1px solid #666666;
        }
        """

        # 添加按钮到工具栏
        for name, button in self.period_buttons.items():
            button.setCheckable(True)
            button.setStyleSheet(button_style)
            button.toggled.connect(lambda checked, p=name: self.indicator_interaction_manager.on_period_changed(p, checked))
            toolbar_layout.addWidget(button)

        # 当前窗口数量已在__init__方法中初始化

        # 默认选中日线
        self.period_buttons['日线'].setChecked(True)
        self.current_period = '日线'

        # 添加显示柱体数量的输入框
        bar_count_label = QLabel("柱体数:")
        bar_count_label.setStyleSheet("color: #C0C0C0; font-family: 'Microsoft YaHei'; font-size: 12px;")
        toolbar_layout.addWidget(bar_count_label)

        self.bar_count_input = QLineEdit()
        self.bar_count_input.setText("100")  # 默认显示100个柱体
        self.bar_count_input.setStyleSheet("""
        QLineEdit {
            background-color: #333333;
            color: #C0C0C0;
            border: 1px solid #444444;
            padding: 4px 6px;
            border-radius: 4px;
            font-family: 'Microsoft YaHei';
            font-size: 12px;
            width: 60px;
        }
        QLineEdit:focus {
            border: 1px solid #666666;
            background-color: #444444;
        }
        """)
        # 连接输入框事件
        self.bar_count_input.editingFinished.connect(self.indicator_interaction_manager.on_bar_count_changed)
        toolbar_layout.addWidget(self.bar_count_input)

        # 添加分隔符
        toolbar_layout.addSpacing(10)

        # 添加复权按钮
        self.adjustment_btn = QPushButton("📊 前复权")  # 默认显示前复权
        self.adjustment_btn.setStyleSheet(button_style)
        self.adjustment_btn.setCheckable(True)
        self.adjustment_btn.setChecked(True)  # 默认选中前复权
        self.adjustment_btn.clicked.connect(self.indicator_interaction_manager.on_adjustment_clicked)
        toolbar_layout.addWidget(self.adjustment_btn)

        # 添加分隔符
        toolbar_layout.addSpacing(10)

        # 添加上下箭头按钮用于切换相邻股票
        self.prev_stock_btn = QPushButton("⬆️ 上一只")
        self.prev_stock_btn.setStyleSheet(button_style)
        self.prev_stock_btn.setToolTip("切换到列表中的上一只股票")
        self.prev_stock_btn.clicked.connect(self._on_prev_stock_clicked)
        toolbar_layout.addWidget(self.prev_stock_btn)

        self.next_stock_btn = QPushButton("⬇️ 下一只")
        self.next_stock_btn.setStyleSheet(button_style)
        self.next_stock_btn.setToolTip("切换到列表中的下一只股票")
        self.next_stock_btn.clicked.connect(self._on_next_stock_clicked)
        toolbar_layout.addWidget(self.next_stock_btn)

        # 添加工具栏到布局
        tech_layout.addWidget(toolbar)

    def _create_tech_charts(self, tech_layout):
        """
        创建图表区域：K线、成交量、KDJ 三个画布及其垂直分割布局
        """
        # 创建图表容器，用于放置标签和分割器
        self.chart_container = QWidget()
        self.chart_layout = QVBoxLayout(self.chart_container)
        self.chart_layout.setSpacing(0)
        self.chart_layout.setContentsMargins(0, 0, 0, 0)

        # 创建垂直分割器，用于放置K线图和成交量图
        self.chart_splitter = QSplitter(Qt.Vertical)
        self.chart_splitter.setStyleSheet("QSplitter::handle:vertical { background-color: #444444; height: 2px; border: 0px; }")
        self.chart_splitter.setHandleWidth(2)
        self.chart_splitter.setOpaqueResize(True)

        # 创建K线图
        self.tech_plot_widget = pg.PlotWidget()
        self.tech_plot_widget.setBackground('#000000')
        self.tech_plot_widget.setLabel('left', '价格', color='#C0C0C0')
        # 不显示X轴标签
        self.tech_plot_widget.setLabel('bottom', '')
        self.tech_plot_widget.getAxis('left').setPen(pg.mkPen('#C0C0C0'))
        self.tech_plot_widget.getAxis('bottom').setPen(pg.mkPen('#C0C0C0'))
        self.tech_plot_widget.getAxis('left').setTextPen(pg.mkPen('#C0C0C0'))
        self.tech_plot_widget.getAxis('bottom').setTextPen(pg.mkPen('#C0C0C0'))
        self.tech_plot_widget.showGrid(x=True, y=True, alpha=0.3)

        # 创建成交量图
        self.volume_plot_widget = pg.PlotWidget()
        self.volume_plot_widget.setBackground('#000000')
        self.volume_plot_widget.setLabel('left', '成交量', color='#C0C0C0')
        self.volume_plot_widget.setLabel('bottom', '', color='#C0C0C0')
        self.volume_plot_widget.getAxis('left').setPen(pg.mkPen('#C0C0C0'))
        self.volume_plot_widget.getAxis('bottom').setPen(pg.mkPen('#C0C0C0'))
        self.volume_plot_widget.getAxis('left').setTextPen(pg.mkPen('#C0C0C0'))
        self.volume_plot_widget.getAxis('bottom').setTextPen(pg.mkPen('#C0C0C0'))
        self.volume_plot_widget.showGrid(x=True, y=True, alpha=0.3)

        # 确保两个图的Y轴宽度一致，实现竖轴对齐
        # 设置Y轴宽度为50像素
        self.tech_plot_widget.getAxis('left').setWidth(50)
        self.volume_plot_widget.getAxis('left').setWidth(50)

        # 设置X轴宽度，确保两个图的X轴对齐
        self.tech_plot_widget.getAxis('bottom').setHeight(20)
        self.volume_plot_widget.getAxis('bottom').setHeight(20)

        # 创建KDJ指标图
        self.kdj_plot_widget = pg.PlotWidget()
        self.kdj_plot_widget.setBackground('#000000')
        self.kdj_plot_widget.setLabel('left', 'KDJ', color='#C0C0C0')
        self.kdj_plot_widget.setLabel('bottom', '', color='#C0C0C0')
        self.kdj_plot_widget.getAxis('left').setPen(pg.mkPen('#C0C0C0'))
        self.kdj_plot_widget.getAxis('bottom').setPen(pg.mkPen('#C0C0C0'))
        self.kdj_plot_widget.getAxis('left').setTextPen(pg.mkPen('#C0C0C0'))
        self.kdj_plot_widget.getAxis('bottom').setTextPen(pg.mkPen('#C0C0C0'))
        self.kdj_plot_widget.showGrid(x=True, y=True, alpha=0.3)
        # 设置KDJ指标图的Y轴范围，考虑到KDJ可能超出0-100范围，特别是J值
        self.kdj_plot_widget.setYRange(-50, 150)
        # 确保KDJ图的Y轴宽度与其他图一致
        self.kdj_plot_widget.getAxis('left').setWidth(50)
        self.kdj_plot_widget.getAxis('bottom').setHeight(20)

        # 为K线图创建容器，只包含K线图
        self.tech_container = QWidget()
        self.tech_container_layout = QVBoxLayout(self.tech_container)
        self.tech_container_layout.setSpacing(0)
        self.tech_container_layout.setContentsMargins(0, 0, 0, 0)

        self.tech_container_layout.addWidget(self.tech_plot_widget)

        # 添加点击事件，用于选中窗口
        self.tech_container.mousePressEvent = lambda event: self.indicator_interaction_manager.on_window_clicked(1)

        # 为成交量图创建容器，只包含成交量图
        self.volume_container = QWidget()
        self.volume_container_layout = QVBoxLayout(self.volume_container)
        self.volume_container_layout.setSpacing(0)
        self.volume_container_layout.setContentsMargins(0, 0, 0, 0)

        self.volume_container_layout.addWidget(self.volume_plot_widget)

        # 添加点击事件，用于选中窗口
        self.volume_container.mousePressEvent = lambda event: self.indicator_interaction_manager.on_window_clicked(2)

        # 为KDJ指标图创建容器，只包含KDJ指标图
        self.kdj_container = QWidget()
        self.kdj_container_layout = QVBoxLayout(self.kdj_container)
        self.kdj_container_layout.setSpacing(0)
        self.kdj_container_layout.setContentsMargins(0, 0, 0, 0)

        self.kdj_container_layout.addWidget(self.kdj_plot_widget)

        # 添加点击事件，用于选中窗口
        self.kdj_container.mousePressEvent = lambda event: self.indicator_interaction_manager.on_window_clicked(3)

        # 添加图表到分割器
        self.chart_splitter.addWidget(self.tech_container)
        self.chart_splitter.addWidget(self.volume_container)
        self.chart_splitter.addWidget(self.kdj_container)

        # 设置分割比例（K线图占50%，成交量图占25%，KDJ图占25%）
        # 先使用setStretchFactor设置相对比例
        self.chart_splitter.setStretchFactor(0, 2)  # 第一个组件（K线图）占2份
        self.chart_splitter.setStretchFactor(1, 1)  # 第二个组件（成交量图）占1份
        self.chart_splitter.setStretchFactor(2, 1)  # 第三个组件（KDJ图）占1份

        # 再使用setSizes设置初始尺寸，确保比例正确
        self.chart_splitter.setSizes([2000, 1000, 1000])

        # 添加分割器到容器布局
        self.chart_layout.addWidget(self.chart_splitter, 1)  # 1表示垂直方向拉伸

        # 添加容器到主布局
        tech_layout.addWidget(self.chart_container, 1)  # 1表示垂直方向拉伸
