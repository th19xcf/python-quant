#!/usr/bin/env python3

"""
主窗口技术分析页 Mixin：周期工具栏与 K线/成交量/KDJ 图表画布创建。
"""

import pyqtgraph as pg
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont, QFontMetrics
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSplitter,
    QVBoxLayout,
    QWidget,
)

from src.ui.chip_panel import ChipController, ChipPanel
from src.ui.data_stock_controller import DEFAULT_BAR_COUNT, MIN_BAR_COUNT


class MainWindowTechTabMixin:
    """技术分析标签页：周期/柱体数/复权工具栏与三个画布。"""

    # 股票名称标签的最大宽度，超出部分以省略号截断，避免挤压周期按钮
    _STOCK_NAME_MAX_WIDTH = 260
    _STOCK_NAME_PADDING = 8
    _STOCK_NAME_FONT_SIZE = 14
    _STOCK_NAME_COLOR = '#FFD04B'

    # 主图/副图的最小可读高度（像素，含各自坐标轴；副图的指标标题栏另计入容器最小高度）。
    # 3 张图时最小总高 = 110 + (84 + 约 24) * 2 + 分割条 2 * 2 ≈ 330，
    # 因此窗口内容区高度 ≥ 约 600px 时三张图可首屏完整显示，低于该高度由滚动容器兜底。
    _MAIN_PLOT_MIN_HEIGHT = 110
    _SUB_PLOT_MIN_HEIGHT = 84

    # 深色主题滚动条：与页面 #222222 / #333333 配色一致，不出现系统亮色条
    _CHART_SCROLLBAR_STYLE = """
    QScrollArea { background-color: transparent; border: none; }
    QScrollArea > QWidget > QWidget { background-color: transparent; }
    QScrollBar:vertical {
        background-color: #222222;
        width: 10px;
        margin: 0px;
        border: none;
    }
    QScrollBar::handle:vertical {
        background-color: #555555;
        min-height: 24px;
        border-radius: 5px;
    }
    QScrollBar::handle:vertical:hover {
        background-color: #777777;
    }
    QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
        height: 0px;
        background: none;
        border: none;
    }
    QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {
        background: none;
    }
    """

    def create_tech_tab(self):
        """
        创建技术分析标签页
        """
        tech_layout = QVBoxLayout(self.tech_tab)
        tech_layout.setContentsMargins(0, 0, 0, 0)
        tech_layout.setSpacing(0)

        self._create_tech_toolbar(tech_layout)
        self._create_tech_charts(tech_layout)

        # 添加指标选择窗口
        self.create_indicator_selection()
        # 筹码显隐按钮/全区间开关与筹码栏联动
        self.chip_controller.bind_controls(
            self.chip_toggle_btn,
            self.chip_full_range_checkbox,
            self.chip_floor_combo,
        )
        # 把下栏已创建的增减柱体按钮移动到上栏复权按钮左侧（复用同一实例）
        self._relocate_bar_count_buttons()
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
        self.tech_toolbar_layout = toolbar_layout
        toolbar.setStyleSheet("background-color: #222222;")
        # 顶部工具栏常驻：垂直方向固定高度，不随图表区压缩或滚动
        toolbar.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

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
        self._tech_button_style = button_style

        self._build_stock_name_label(toolbar_layout)
        # 上一只/下一只放在周期按钮左侧
        self._build_stock_switch_buttons(toolbar_layout, button_style)
        self._build_period_buttons(toolbar_layout, button_style)
        self._build_bar_count_input(toolbar_layout)
        self._build_adjustment_button(toolbar_layout, button_style)

        # 添加工具栏到布局
        tech_layout.addWidget(toolbar)

    def _build_stock_name_label(self, toolbar_layout):
        """
        创建工具栏最左侧的股票名称标签（位于周期按钮之前）
        """
        self.stock_name_label = QLabel('--')
        self.stock_name_label.setStyleSheet(
            f"color: {self._STOCK_NAME_COLOR}; font-family: 'Microsoft YaHei';"
            f"font-size: {self._STOCK_NAME_FONT_SIZE}px; font-weight: bold;"
            f"padding-right: {self._STOCK_NAME_PADDING}px;"
        )
        self.stock_name_label.setMaximumWidth(self._STOCK_NAME_MAX_WIDTH)
        self.stock_name_label.setSizePolicy(QSizePolicy.Maximum, QSizePolicy.Preferred)
        self.stock_name_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.stock_name_label.setWordWrap(False)
        toolbar_layout.addWidget(self.stock_name_label)

    def update_stock_name_label(self, stock_name='', stock_code=''):
        """
        刷新工具栏股票名称标签

        切换股票、切换周期与清空图表共用此入口，保证工具栏与图表标题状态一致。
        名称过长时按标签最大宽度省略，避免把日线/周线/月线按钮推走。

        Args:
            stock_name: 股票名称
            stock_code: 股票代码
        """
        label = getattr(self, 'stock_name_label', None)
        if label is None:
            return

        text = f"{stock_name}({stock_code})" if (stock_name or stock_code) else '--'
        available_width = self._STOCK_NAME_MAX_WIDTH - self._STOCK_NAME_PADDING
        # 用与样式表一致的字体度量做省略，避免依赖控件默认字体导致截断判断失真
        metrics = QFontMetrics(self._stock_name_font())
        label.setText(metrics.elidedText(text, Qt.ElideRight, available_width))
        label.setToolTip(text)

    def _stock_name_font(self) -> QFont:
        """股票名称标签使用的字体（与 _build_stock_name_label 的样式表保持一致）"""
        font = QFont('Microsoft YaHei')
        font.setPixelSize(self._STOCK_NAME_FONT_SIZE)
        font.setWeight(QFont.Weight.Bold)
        return font

    def _build_period_buttons(self, toolbar_layout, button_style):
        """
        创建周期切换按钮并默认选中日线
        """
        # 添加周期按钮
        self.period_buttons = {
            '日线': QPushButton('📅 日线'),
            '周线': QPushButton('📆 周线'),
            '月线': QPushButton('📈 月线')
        }

        # 添加按钮到工具栏
        for name, button in self.period_buttons.items():
            button.setCheckable(True)
            button.setObjectName({'日线': 'periodDayBtn', '周线': 'periodWeekBtn', '月线': 'periodMonthBtn'}[name])
            button.setStyleSheet(button_style)
            button.toggled.connect(lambda checked, p=name: self.indicator_interaction_manager.on_period_changed(p, checked))
            toolbar_layout.addWidget(button)

        # 当前窗口数量已在__init__方法中初始化

        # 默认选中日线
        self.period_buttons['日线'].setChecked(True)
        self.current_period = '日线'

    def _build_bar_count_input(self, toolbar_layout):
        """
        创建柱体数输入框并连接编辑完成事件
        """
        # 添加显示柱体数量的输入框
        bar_count_label = QLabel("柱体数:")
        bar_count_label.setStyleSheet("color: #C0C0C0; font-family: 'Microsoft YaHei'; font-size: 12px;")
        toolbar_layout.addWidget(bar_count_label)

        self.bar_count_input = QLineEdit()
        self.bar_count_input.setObjectName('barCountInput')
        self.bar_count_input.setText(str(DEFAULT_BAR_COUNT))  # 默认显示柱体数
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

    def _relocate_bar_count_buttons(self):
        """把下栏创建的增减柱体按钮移动到上栏复权按钮左侧。

        直接搬移原 QPushButton 实例，不重新创建，原有 clicked 连接保持不变；
        两个按钮只操作 window.displayed_bar_count 这一份设定值。
        """
        bottom_layout = self.plus_btn.parentWidget().layout()
        bottom_layout.removeWidget(self._bar_count_separator)
        self._bar_count_separator.setParent(None)

        bottom_layout.removeWidget(self.plus_btn)
        bottom_layout.removeWidget(self.minus_btn)

        # 套用上方工具栏样式，保持视觉一致
        self.plus_btn.setStyleSheet(self._tech_button_style)
        self.minus_btn.setStyleSheet(self._tech_button_style)

        index = self.tech_toolbar_layout.indexOf(self.adjustment_btn)
        self.tech_toolbar_layout.insertWidget(index, self.plus_btn)
        self.tech_toolbar_layout.insertWidget(index + 1, self.minus_btn)

        self.indicator_interaction_manager.update_bar_count_controls()

    def _build_adjustment_button(self, toolbar_layout, button_style):
        """
        创建复权按钮并默认选中前复权
        """
        # 添加复权按钮
        self.adjustment_btn = QPushButton("📊 前复权")  # 默认显示前复权
        self.adjustment_btn.setStyleSheet(button_style)
        self.adjustment_btn.setCheckable(True)
        self.adjustment_btn.setChecked(True)  # 默认选中前复权
        self.adjustment_btn.clicked.connect(self.indicator_interaction_manager.on_adjustment_clicked)
        toolbar_layout.addWidget(self.adjustment_btn)

        # 添加分隔符
        toolbar_layout.addSpacing(10)

    def _build_stock_switch_buttons(self, toolbar_layout, button_style):
        """
        创建用于切换相邻股票的上下箭头按钮
        """
        # 添加上下箭头按钮用于切换相邻股票
        self.prev_stock_btn = QPushButton("⬆️ 上一只")
        self.prev_stock_btn.setObjectName('prevStockBtn')
        self.prev_stock_btn.setStyleSheet(button_style)
        self.prev_stock_btn.setToolTip("切换到列表中的上一只股票")
        self.prev_stock_btn.clicked.connect(self._on_prev_stock_clicked)
        toolbar_layout.addWidget(self.prev_stock_btn)

        self.next_stock_btn = QPushButton("⬇️ 下一只")
        self.next_stock_btn.setObjectName('nextStockBtn')
        self.next_stock_btn.setStyleSheet(button_style)
        self.next_stock_btn.setToolTip("切换到列表中的下一只股票")
        self.next_stock_btn.clicked.connect(self._on_next_stock_clicked)
        toolbar_layout.addWidget(self.next_stock_btn)

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
        # 禁止拖拽把某张副图折叠成 0 高度（避免只剩标题栏）
        self.chart_splitter.setChildrenCollapsible(False)

        self._create_chart_plot_widgets()
        self._create_chart_containers()
        self._setup_chart_splitter()

        # 指标区外层套竖向滚动容器：高度足够时铺满、无滚动条；
        # 高度不足（低于各图最小可读高度之和）时出现竖向滚动条，底部副图可滚动访问。
        self.chart_scroll_area = QScrollArea()
        self.chart_scroll_area.setWidgetResizable(True)
        self.chart_scroll_area.setFrameShape(QFrame.NoFrame)
        self.chart_scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.chart_scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.chart_scroll_area.setStyleSheet(self._CHART_SCROLLBAR_STYLE)
        self.chart_scroll_area.setWidget(self.chart_splitter)

        # 添加滚动容器到容器布局
        self.chart_layout.addWidget(self.chart_scroll_area, 1)  # 1表示垂直方向拉伸

        # 添加容器到主布局
        tech_layout.addWidget(self.chart_container, 1)  # 1表示垂直方向拉伸

    def _create_chart_plot_widgets(self):
        """
        创建K线、成交量与KDJ三个画布，统一坐标轴样式并对齐竖轴
        """
        # 创建K线图
        self.tech_plot_widget = pg.PlotWidget()
        self.tech_plot_widget.setBackground('#000000')
        self.tech_plot_widget.setMinimumHeight(self._MAIN_PLOT_MIN_HEIGHT)
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
        self.volume_plot_widget.setMinimumHeight(self._SUB_PLOT_MIN_HEIGHT)
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
        self.kdj_plot_widget.setMinimumHeight(self._SUB_PLOT_MIN_HEIGHT)
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

    def _create_chart_containers(self):
        """
        为三个画布分别创建容器并绑定点击选中窗口事件
        """
        # 为K线图创建容器：主图 + 右侧筹码栏共用价格轴
        self.tech_container = QWidget()
        self.tech_container_layout = QHBoxLayout(self.tech_container)
        self.tech_container_layout.setSpacing(0)
        self.tech_container_layout.setContentsMargins(0, 0, 0, 0)

        self.tech_container_layout.addWidget(self.tech_plot_widget, 3)

        self.chip_panel = ChipPanel(self.tech_plot_widget)
        self.chip_panel.setObjectName('chipPanel')
        self.tech_container_layout.addWidget(self.chip_panel, 1)
        self.chip_controller = ChipController(self, self.chip_panel)

        # 添加点击事件，用于选中窗口
        self.tech_container.mousePressEvent = lambda event: self.indicator_interaction_manager.on_window_clicked(1)

        # 副图：指标标题栏占据整行，标题栏下方才是「绘图区 + 右侧占位列」。
        # 占位列宽度与筹码栏一致，保证所有绘图区左右边界逐像素对齐。
        self.volume_container = QWidget()
        self.volume_container_layout = QVBoxLayout(self.volume_container)
        self.volume_container_layout.setSpacing(0)
        self.volume_container_layout.setContentsMargins(0, 0, 0, 0)

        self.volume_plot_row = QWidget()
        self.volume_plot_row.setMinimumHeight(self._SUB_PLOT_MIN_HEIGHT)
        self.volume_plot_row_layout = QHBoxLayout(self.volume_plot_row)
        self.volume_plot_row_layout.setSpacing(0)
        self.volume_plot_row_layout.setContentsMargins(0, 0, 0, 0)
        self.volume_chip_gutter = self._create_chip_gutter()
        self.volume_plot_row_layout.addWidget(self.volume_plot_widget, 1)
        self.volume_plot_row_layout.addWidget(self.volume_chip_gutter)
        self.volume_container_layout.addWidget(self.volume_plot_row, 1)

        # 添加点击事件，用于选中窗口
        self.volume_container.mousePressEvent = lambda event: self.indicator_interaction_manager.on_window_clicked(2)

        # 为KDJ指标图创建容器：结构同成交量图
        self.kdj_container = QWidget()
        self.kdj_container_layout = QVBoxLayout(self.kdj_container)
        self.kdj_container_layout.setSpacing(0)
        self.kdj_container_layout.setContentsMargins(0, 0, 0, 0)

        self.kdj_plot_row = QWidget()
        self.kdj_plot_row.setMinimumHeight(self._SUB_PLOT_MIN_HEIGHT)
        self.kdj_plot_row_layout = QHBoxLayout(self.kdj_plot_row)
        self.kdj_plot_row_layout.setSpacing(0)
        self.kdj_plot_row_layout.setContentsMargins(0, 0, 0, 0)
        self.kdj_chip_gutter = self._create_chip_gutter()
        self.kdj_plot_row_layout.addWidget(self.kdj_plot_widget, 1)
        self.kdj_plot_row_layout.addWidget(self.kdj_chip_gutter)
        self.kdj_container_layout.addWidget(self.kdj_plot_row, 1)

        # 添加点击事件，用于选中窗口
        self.kdj_container.mousePressEvent = lambda event: self.indicator_interaction_manager.on_window_clicked(3)

        # 列宽同步：筹码栏自身宽度变化、显隐切换都会驱动副图占位列
        self._chip_gutters = (self.volume_chip_gutter, self.kdj_chip_gutter)
        self.chip_panel.set_width_callback(self._sync_chip_gutters)
        self.chip_controller.set_layout_callback(self._sync_chip_gutters)
        self._sync_chip_gutters()

    @staticmethod
    def _create_chip_gutter():
        """创建副图右侧的黑色占位块，宽度跟随筹码栏。"""
        gutter = QWidget()
        gutter.setObjectName('chipGutter')
        gutter.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        gutter.setStyleSheet('background-color: #000000;')
        gutter.setFixedWidth(0)
        gutter.hide()
        return gutter

    def _sync_chip_gutters(self, width: int | None = None):
        """把副图右侧占位列统一到筹码栏的当前宽度。

        所有占位列使用同一个像素宽度（筹码栏实测宽度），而不是各行的
        stretch 比例，避免四舍五入后左右边界出现 1~2px 偏差。
        """
        gutters = getattr(self, '_chip_gutters', ())
        controller = getattr(self, 'chip_controller', None)
        panel = getattr(self, 'chip_panel', None)
        visible = bool(controller is not None and controller.is_visible())
        target = 0
        if visible and panel is not None:
            target = max(int(width if width is not None else panel.width()), 0)
        for gutter in gutters:
            gutter.setFixedWidth(target)
            gutter.setVisible(visible and target > 0)

    def _setup_chart_splitter(self):
        """
        将三个容器加入分割器并设置分割比例
        """
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
        self._update_chart_height_policy(rebalance=True)

    def _update_chart_height_policy(self, rebalance: bool = False):
        """
        按当前可见画布数量重算最小高度，并在需要时按 2:1:1 重新分配。

        每张画布都有最小可读高度（主图 _MAIN_PLOT_MIN_HEIGHT、副图
        _SUB_PLOT_MIN_HEIGHT，副图另加指标标题栏高度）。可见画布最小高度之和
        即分割器的最小高度：
        - 视口高于该值时，分割器铺满视口，剩余高度按拉伸因子分配，不出现滚动条；
        - 视口低于该值时，分割器保持最小高度，由外层 QScrollArea 提供竖向滚动，
          底部副图可滚动访问，不会被窗口底边裁掉。

        Args:
            rebalance: 为 True 时立即按 2:1:1 重算尺寸（窗口数量切换、指标增删、
                切换周期/股票后的布局重算）；为 False 时只更新最小高度，
                保留用户手动拖拽的分割比例。
        """
        if not hasattr(self, 'chart_splitter'):
            return

        containers = [self.tech_container, self.volume_container, self.kdj_container]
        visible = [c for c in containers if not c.isHidden()]
        if not visible:
            return

        handle_total = self.chart_splitter.handleWidth() * (len(visible) - 1)
        # 容器最小高度 = 画布最小高度 + 指标标题栏高度（副图才有，位于布局第 0 项）
        container_min = {
            c: self._chart_container_min_height(c)
            for c in visible
        }
        min_total = sum(container_min.values()) + handle_total
        self.chart_splitter.setMinimumHeight(min_total)

        if not rebalance:
            return

        viewport = 0
        if hasattr(self, 'chart_scroll_area'):
            viewport = self.chart_scroll_area.viewport().height()
        if viewport <= 0:
            viewport = self.chart_splitter.height()
        target = max(viewport, min_total)
        extra = target - min_total
        weights = [2 if c is self.tech_container else 1 for c in visible]
        weight_total = sum(weights) or 1

        sizes = []
        visible_sizes = {
            c: int(container_min[c] + extra * w / weight_total)
            for c, w in zip(visible, weights, strict=True)
        }
        for c in containers:
            sizes.append(visible_sizes.get(c, 0))
        self.chart_splitter.setSizes(sizes)

    @staticmethod
    def _chart_container_min_height(container):
        """
        计算单个画布容器的最小高度：画布自身最小高度 + 指标标题栏高度。

        直接取子项尺寸而非 container.minimumSizeHint()，避免 Qt 尺寸缓存
        在标题栏刚插入、尚未重新布局时返回过期值。
        """
        layout = container.layout()
        if layout is None:
            return container.minimumHeight()
        height = 0
        for i in range(layout.count()):
            widget = layout.itemAt(i).widget()
            if widget is None:
                continue
            if i == 0 and layout.count() > 1:
                # 第 0 项是副图的指标标题栏，按固定行高计入
                # （新建控件尚未布局时 height() 是默认 480，必须用 sizeHint）
                height += widget.sizeHint().height()
            else:
                height += widget.minimumHeight()
        return height

