#!/usr/bin/env python3

"""
主窗口指标选择栏 Mixin：指标按钮滚动区、指标菜单与指标点击处理。
"""

from PySide6.QtCore import QPoint, Qt
from PySide6.QtGui import QAction
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QMenu,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QWidget,
)

from src.utils.logger import logger


class MainWindowIndicatorBarMixin:
    """通达信风格指标选择栏与指标按钮交互。"""

    window_menu: QMenu

    def _create_indicator_button(self, indicator_name, style_sheet, checkable=True, fixed_width=None, clicked_handler=None):
        """
        创建指标按钮的通用方法

        Args:
            indicator_name: 指标名称
            style_sheet: 按钮样式
            checkable: 是否可勾选
            fixed_width: 固定宽度，None表示自适应
            clicked_handler: 点击事件处理函数

        Returns:
            QPushButton: 创建的按钮实例
        """
        btn = QPushButton(indicator_name)
        btn.setStyleSheet(style_sheet)
        btn.setCheckable(checkable)

        if fixed_width is not None:
            btn.setFixedWidth(fixed_width)

        if clicked_handler is not None:
            btn.clicked.connect(clicked_handler)
        else:
            btn.clicked.connect(lambda checked, ind=indicator_name: self.indicator_interaction_manager.on_indicator_clicked(ind, checked))

        return btn

    def _create_separator(self):
        """
        创建分隔符的通用方法

        Returns:
            QLabel: 创建的分隔符实例
        """
        separator = QLabel("|")
        separator.setStyleSheet("color: #666666; font-size: 12px;")
        return separator

    def create_indicator_selection(self):
        """
        创建指标选择窗口，类似通达信的指标选择栏
        """
        # 创建指标选择容器
        self.indicator_widget = QWidget()
        self.indicator_widget.setStyleSheet("background-color: #222222;")
        indicator_layout = QHBoxLayout(self.indicator_widget)
        indicator_layout.setContentsMargins(5, 3, 10, 3)  # 增加右边距，确保按钮显示完整
        indicator_layout.setSpacing(2)

        # 创建指标按钮样式
        indicator_button_style = """
        QPushButton {
            background-color: #333333;
            color: #C0C0C0;
            border: 1px solid #444444;
            padding: 3px 8px;
            border-radius: 3px;
            font-family: 'Microsoft YaHei';
            font-size: 11px;
        }
        QPushButton:hover {
            background-color: #444444;
        }
        QPushButton:checked {
            background-color: #555555;
            border: 1px solid #666666;
            color: #FFFFFF;
        }
        """

        # 保存指标按钮
        self.indicator_buttons = {}

        # 创建可横向滚动的指标按钮区（含左侧固定按钮）
        self._create_indicator_scroll_area(indicator_layout, indicator_button_style)

        # 创建滚动区右侧的翻页、柱体数增减按钮
        self._create_indicator_control_buttons(indicator_layout, indicator_button_style)

    def _create_indicator_scroll_area(self, indicator_layout, button_style):
        """
        创建指标按钮滚动区：指标按钮列表 + 左侧窗口/左箭头固定按钮
        """
        # 指标列表 - 按类别排列：趋势指标、成交量指标、震荡指标、成本指标、其他指标
        # 注意："窗口"、"<"、">" 按钮在滚动区域外部单独添加
        # "指标A" 在滚动区域内部
        indicators = [
            "指标A",  # 在滚动区域内部
            "|",  # 分隔符
            # 趋势指标
            "MA", "MACD", "BOLL", "SAR", "TRIX", "DMA", "FSL", "EXPMA", "BBI",
            "|",  # 分隔符
            # 成交量指标
            "VOL", "VOL-TDX", "OBV", "VR", "CR", "BRAR", "ASI", "EMV", "HSL", "LB",
            "|",  # 分隔符
            # 震荡指标
            "KDJ", "RSI", "WR", "CCI", "ROC", "MTM", "PSY",
            "|",  # 分隔符
            # 成本指标
            "CYC", "CYS",
            "|",  # 分隔符
            # 大势型指标
            "ABI", "ADL", "ADR", "OBOS",
            "|",  # 分隔符
            # 其他指标
            "DMI", "MCST"
        ]

        # 创建水平滚动区域
        scroll_area = self._create_indicator_scroll_widget()

        # 创建滚动内容容器并填充指标按钮
        scroll_content = self._build_indicator_scroll_content(indicators, button_style)

        # 设置滚动区域内容
        scroll_area.setWidget(scroll_content)

        # 添加窗口按钮与左箭头等固定在滚动区外部的控件
        self._add_indicator_fixed_buttons(indicator_layout, button_style, scroll_area)

        # 设置滚动区域高度
        scroll_area.setMaximumHeight(30)

    def _create_indicator_scroll_widget(self):
        """
        创建水平滚动区域并注册到指标交互管理器

        Returns:
            QScrollArea: 滚动区域实例
        """
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll_area.setStyleSheet("QScrollArea { background-color: transparent; border: none; }")
        # 设置滚动区域的尺寸策略，确保给右侧按钮留出空间
        scroll_area.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.indicator_interaction_manager.set_scroll_area(scroll_area)
        return scroll_area

    def _build_indicator_scroll_content(self, indicators, button_style):
        """
        创建滚动内容容器：指标按钮、分隔符与指标B/模板按钮

        Args:
            indicators: 指标名称列表
            button_style: 按钮样式

        Returns:
            QWidget: 已填充按钮的滚动内容容器
        """
        # 创建滚动内容容器
        scroll_content = QWidget()
        scroll_layout = QHBoxLayout(scroll_content)
        scroll_layout.setContentsMargins(0, 0, 0, 0)
        scroll_layout.setSpacing(2)

        # 创建指标按钮
        for indicator in indicators:
            self._add_indicator_scroll_button(scroll_layout, indicator, button_style)

        # 添加指标B和模板按钮
        scroll_layout.addWidget(self._create_separator())

        btn = self._create_indicator_button("指标B", button_style)
        scroll_layout.addWidget(btn)
        self.indicator_buttons["指标B"] = btn

        scroll_layout.addWidget(self._create_separator())

        btn = self._create_indicator_button("模板", button_style)
        scroll_layout.addWidget(btn)
        self.indicator_buttons["模板"] = btn

        return scroll_content

    def _add_indicator_fixed_buttons(self, indicator_layout, button_style, scroll_area):
        """
        在滚动区外侧添加窗口菜单按钮、左箭头滚动按钮与分隔符

        Args:
            indicator_layout: 指标栏主布局
            button_style: 按钮样式
            scroll_area: 指标按钮滚动区域
        """
        # 添加窗口按钮（最左侧，固定不动）
        window_btn = self._create_indicator_button(
            "窗口",
            button_style,
            checkable=False
        )
        # 使用已经创建好的顶部菜单
        window_btn.clicked.connect(lambda checked, b=window_btn, m=self.window_menu:
                                   m.popup(b.mapToGlobal(QPoint(0, -m.sizeHint().height()))))
        indicator_layout.addWidget(window_btn)
        self.indicator_buttons["窗口"] = window_btn

        # 添加左箭头滚动按钮（固定在滚动区域左侧外部）
        left_arrow_btn = self._create_indicator_button(
            "<",
            button_style,
            checkable=False,
            fixed_width=24,
            clicked_handler=self.indicator_interaction_manager.on_left_arrow_clicked
        )
        left_arrow_btn.setMinimumWidth(24)
        indicator_layout.addWidget(left_arrow_btn)
        self.indicator_buttons["<"] = left_arrow_btn

        # 添加分隔符
        indicator_layout.addWidget(self._create_separator())

        # 添加滚动区域到主布局
        indicator_layout.addWidget(scroll_area)

    def _add_indicator_scroll_button(self, scroll_layout, indicator, button_style):
        """
        按指标名在滚动区创建对应按钮；分隔符与未实现指标使用不同样式
        """
        if indicator == "指标A":
            # 指标A按钮（在滚动区域内部）
            btn = self._create_indicator_button(indicator, button_style)
            scroll_layout.addWidget(btn)
            self.indicator_buttons[indicator] = btn
        elif indicator == "|":
            # 分类分隔符
            scroll_layout.addWidget(self._create_separator())
        elif indicator in ("指标B", "模板"):
            # 特殊按钮样式
            btn = QPushButton(indicator)
            btn.setCheckable(True)
            btn.setStyleSheet(button_style)
            btn.clicked.connect(lambda checked, ind=indicator: self.indicator_interaction_manager.on_indicator_clicked(ind, checked))
            scroll_layout.addWidget(btn)
            self.indicator_buttons[indicator] = btn

            # 添加分隔符
            if indicator in ["指标A", "指标B"]:
                separator = QLabel("|")
                separator.setStyleSheet("color: #666666; font-size: 12px;")
                scroll_layout.addWidget(separator)
        else:
            # 已实现的指标列表
            implemented_indicators = [
                'MA', 'VOL', 'MACD', 'KDJ', 'DMI', 'DMA', 'FSL', 'TRIX', 'BRAR', 'CR',
                'VR', 'OBV', 'ASI', 'EMV', 'VOL-TDX', 'RSI', 'WR', 'SAR',
                'CCI', 'ROC', 'MTM', 'BOLL', 'PSY', 'MCST',
                # 新增指标
                'EXPMA', 'BBI', 'HSL', 'LB', 'CYC', 'CYS',
                # 大势型指标
                'ABI', 'ADL', 'ADR', 'OBOS'
            ]
            # 未实现指标的样式（灰色文字）
            disabled_indicator_style = button_style + "QPushButton { color: #666666; }"

            # 普通指标按钮
            if indicator in implemented_indicators:
                # 已实现的指标，使用默认样式
                btn = self._create_indicator_button(indicator, button_style)
            else:
                # 未实现的指标，使用灰色文字样式
                btn = self._create_indicator_button(indicator, disabled_indicator_style, checkable=False)
                btn.setDisabled(True)

            scroll_layout.addWidget(btn)
            self.indicator_buttons[indicator] = btn

    def _create_indicator_control_buttons(self, indicator_layout, button_style):
        """
        创建滚动区右侧的翻页按钮与柱体数增减按钮
        """
        # 添加右箭头滚动按钮（固定在滚动区域右侧外部）
        right_arrow_btn = self._create_indicator_button(
            ">",
            button_style,
            checkable=False,
            fixed_width=24,
            clicked_handler=self.indicator_interaction_manager.on_right_arrow_clicked
        )
        # 确保按钮显示完整
        right_arrow_btn.setMinimumWidth(24)
        indicator_layout.addWidget(right_arrow_btn)
        self.indicator_buttons[">"] = right_arrow_btn

        # 添加+、-按钮用于调整柱体数量
        # 添加分隔符
        indicator_layout.addWidget(self._create_separator())

        # 添加+按钮
        plus_btn = self._create_indicator_button(
            "+",
            button_style,
            checkable=False,
            fixed_width=24,
            clicked_handler=self.indicator_interaction_manager.on_plus_btn_clicked
        )
        plus_btn.setMinimumWidth(24)
        indicator_layout.addWidget(plus_btn)

        # 添加-按钮
        minus_btn = self._create_indicator_button(
            "-",
            button_style,
            checkable=False,
            fixed_width=24,
            clicked_handler=self.indicator_interaction_manager.on_minus_btn_clicked
        )
        minus_btn.setMinimumWidth(24)
        indicator_layout.addWidget(minus_btn)

    def create_indicator_menu(self):
        """
        创建指标功能菜单
        """
        # 创建主菜单
        self.indicator_menu = QMenu()

        # 选择副图指标
        submenu = QMenu("选择副图指标", self.indicator_menu)
        self.indicator_menu.addMenu(submenu)

        # 指标用法注释
        action = QAction("指标用法注释", self.indicator_menu)
        action.triggered.connect(self.indicator_interaction_manager.on_indicator_usage)
        self.indicator_menu.addAction(action)

        # 调整指标参数
        action = QAction("调整指标参数", self.indicator_menu)
        action.triggered.connect(self.indicator_interaction_manager.on_indicator_params)
        self.indicator_menu.addAction(action)

        # 修改当前指标公式
        action = QAction("修改当前指标公式", self.indicator_menu)
        action.triggered.connect(self.indicator_interaction_manager.on_indicator_formula)
        self.indicator_menu.addAction(action)

    def create_indicator_menu_button(self, window_num):
        """
        创建功能菜单按钮

        Args:
            window_num: 窗口编号

        Returns:
            QPushButton: 功能菜单按钮
        """
        # 创建功能菜单按钮
        btn = QPushButton("≡")
        # 设置按钮背景色与第1窗口标签栏一致
        btn.setStyleSheet("background-color: transparent; color: #C0C0C0; border: none; font-size: 12px; padding: 0 5px; height: 20px;")
        btn.setFixedSize(20, 20)
        btn.setToolTip("功能菜单")
        # 连接点击事件
        btn.clicked.connect(lambda: self.show_indicator_menu(btn, window_num))

        return btn

    def show_indicator_menu(self, btn, window_num):
        """
        显示指标功能菜单

        Args:
            btn: 触发菜单的按钮
            window_num: 窗口编号
        """
        # 显示菜单
        self.indicator_menu.exec_(btn.mapToGlobal(btn.rect().bottomLeft()))
        logger.info(f"显示窗口{window_num}的指标功能菜单")

    def _on_window_clicked_impl(self, window_num):
        """
        窗口点击事件处理
        """
        if 1 <= window_num <= 3:
            self.current_selected_window = window_num
            logger.info(f"选中了窗口: {window_num}")

            # 更新窗口标题，显示当前选中的窗口
            for container in [self.tech_container, self.volume_container, self.kdj_container]:
                container.setStyleSheet("background-color: transparent;")

            # 如果存在标签栏容器，移除其边框
            if hasattr(self, 'title_ma_container'):
                self.title_ma_container.setStyleSheet("background-color: #222222;")

            # 为选中的窗口添加边框，显示选中状态
            if window_num == 1:
                # 为标签栏容器添加左、右、上边框，移除下边框，确保两边有竖线，中间没有竖分割线
                if hasattr(self, 'title_ma_container'):
                    self.title_ma_container.setStyleSheet("background-color: #222222; border-top: 1px solid #00BFFF; border-left: 1px solid #00BFFF; border-right: 1px solid #00BFFF; border-bottom: none;")
                # 为K线图容器添加完整的边框，使其包含整个K线图区域
                self.tech_container.setStyleSheet("background-color: transparent; border: 1px solid #00BFFF;")
            elif window_num == 2:
                self.volume_container.setStyleSheet("background-color: transparent; border: 1px solid #00BFFF;")
            elif window_num == 3:
                self.kdj_container.setStyleSheet("background-color: transparent; border: 1px solid #00BFFF;")

    def _on_indicator_clicked_impl(self, indicator, checked):
        """
        指标按钮点击事件处理
        """
        # 处理MA指标（纯主图指标，控制MA线显示/隐藏）
        if indicator == "MA":
            if checked:
                logger.info("显示主图MA线")
                self.show_ma_lines = True
            else:
                logger.info("隐藏主图MA线")
                self.show_ma_lines = False
            # 重新绘制K线图
            if hasattr(self, 'current_stock_data') and self.current_stock_data is not None:
                self.plot_k_line(self.current_stock_data, self.current_stock_name, self.current_stock_code)

        # 处理可在主图和副图同时显示的指标（SAR、BOLL）
        elif indicator in ["SAR", "BOLL"]:
            logger.info(f"{'显示' if checked else '隐藏'}{indicator}指标（主图+副图）")
            # 保存到当前选中的窗口（作为副图指标）
            if checked:
                # 如果当前选中的是主图窗口(1)，则默认使用副图窗口2
                target_window = self.current_selected_window if self.current_selected_window in [2, 3] else 2
                self.window_indicators[target_window] = indicator
                logger.info(f"为窗口 {target_window} 设置了指标: {indicator}")
            else:
                # 取消选中时，清除所有窗口中的该指标
                for window_id in [2, 3]:
                    if self.window_indicators.get(window_id) == indicator:
                        self.window_indicators[window_id] = None
                        logger.info(f"清除窗口 {window_id} 的指标: {indicator}")
            # 重新绘制K线图
            if hasattr(self, 'current_stock_data') and self.current_stock_data is not None:
                self.plot_k_line(self.current_stock_data, self.current_stock_name, self.current_stock_code)

        elif checked:
            # 取消其他同类型指标的选中状态
            if indicator in ["指标A", "指标B"]:
                # 指标A/B是互斥的
                for name, button in self.indicator_buttons.items():
                    if name in ["指标A", "指标B"] and name != indicator:
                        button.setChecked(False)
            elif indicator != "模板" and indicator != "窗口":
                # 可在主图和副图同时显示的指标
                dual_display_indicators = ["SAR", "BOLL"]
                # 普通副图指标是互斥的
                for name, button in self.indicator_buttons.items():
                    if name not in ["指标A", "指标B", "模板", "窗口", "MA"] + dual_display_indicators and name != indicator:
                        button.setChecked(False)

                # 根据当前选中的窗口设置副图指标
                if indicator not in ["指标A", "指标B", "模板", "窗口", "MA"]:
                    # 如果当前选中的是主图窗口(1)，则默认使用副图窗口2
                    target_window = self.current_selected_window if self.current_selected_window in [2, 3] else 2
                    self.window_indicators[target_window] = indicator
                    logger.info(f"为窗口 {target_window} 设置了指标: {indicator}")

                    # 重新绘制K线图，应用新的指标
                    if hasattr(self, 'current_stock_data') and self.current_stock_data is not None:
                        self.plot_k_line(self.current_stock_data, self.current_stock_name, self.current_stock_code)

        logger.info(f"选择了指标: {indicator}, 状态: {'选中' if checked else '取消'}")
