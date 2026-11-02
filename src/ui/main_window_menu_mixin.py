#!/usr/bin/env python3

"""
主窗口菜单与工具栏 Mixin：菜单栏、菜单动作与工具栏创建。
"""

from PySide6.QtCore import QSize
from PySide6.QtGui import QAction
from PySide6.QtWidgets import QPushButton


class MainWindowMenuMixin:
    """主窗口菜单栏与工具栏构建。

    未接实现的入口一律以 callback=None 声明，由 add_menu_actions 统一置灰
    并写入「未实现」提示，避免界面看起来可点、点击后只静默打日志。
    """

    #: 未实现入口的统一提示文案
    UNIMPLEMENTED_TOOLTIP = "未实现：该功能尚未接入，当前版本不可用"

    def create_menu_bar(self):
        """
        创建菜单栏
        """
        menu_bar = self.menuBar()

        # 行情菜单
        market_menu = menu_bar.addMenu("行情")
        self.add_menu_actions(market_menu, [
            ("自选股", None),
            ("全景图", None),
            ("沪深京指数", self.data_view_manager.show_index_overview),
            ("沪深京板块", None),
            ("沪深京个股", None),
        ])

        # 交易菜单
        trade_menu = menu_bar.addMenu("交易")
        self.add_menu_actions(trade_menu, [
            ("传统交易", None),
            ("条件单", None),
            ("新股申购", None),
        ])

        # 发现菜单
        discovery_menu = menu_bar.addMenu("发现")
        self.add_menu_actions(discovery_menu, [
            ("热点主题", None),
            ("资金流向", None),
            ("研报中心", None),
        ])

        # 资讯菜单
        info_menu = menu_bar.addMenu("资讯")
        self.add_menu_actions(info_menu, [
            ("财经新闻", None),
            ("公司公告", None),
            ("行业资讯", None),
        ])

        # 数据菜单
        data_menu = menu_bar.addMenu("数据")
        self.add_menu_actions(data_menu, [
            ("宏观数据", None),
            ("财务数据", None),
            ("技术指标", None),
            ("盘后数据下载", None),
        ])

        # 特色功能菜单
        feature_menu = menu_bar.addMenu("特色功能")
        self.add_menu_actions(feature_menu, [
            ("自动交易", None),
        ])

        # 量化分析菜单
        quant_menu = menu_bar.addMenu("量化分析")
        self.add_menu_actions(quant_menu, [
            ("策略回测", self.action_manager.on_strategy_backtest),
            ("股票推荐", self.action_manager.on_stock_recommendation),
            ("因子分析", self.action_manager.on_factor_analysis),
        ])

        # 系统菜单
        system_menu = menu_bar.addMenu("系统")
        self.add_menu_actions(system_menu, [
            ("设置", None),
            ("关于", None),
            ("退出", self.action_manager.on_exit),
        ])

        # 窗口菜单已在__init__方法中创建，此处不再重新创建

    def add_menu_actions(self, menu, actions):
        """
        向菜单添加动作

        Args:
            menu: 菜单对象
            actions: 动作列表，每个动作是一个元组(名称, 回调函数, 图标名称)；
                     callback 为 None 表示该入口尚未实现，动作会被置灰并标注提示
        """

        # 图标映射字典
        icon_map = {
            # 行情菜单图标
            "自选股": "📋",
            "全景图": "🌐",
            "沪深京指数": "📊",
            "沪深京板块": "🏢",
            "沪深京个股": "📈",

            # 交易菜单图标
            "传统交易": "💱",
            "条件单": "🎯",
            "新股申购": "📝",

            # 发现菜单图标
            "热点主题": "🔥",
            "资金流向": "💹",
            "研报中心": "📄",

            # 资讯菜单图标
            "财经新闻": "📰",
            "公司公告": "📢",
            "行业资讯": "📊",

            # 数据菜单图标
            "宏观数据": "📈",
            "财务数据": "💰",
            "技术指标": "📊",
            "盘后数据下载": "💾",

            # 特色功能菜单图标
            "自动交易": "🤖",

            # 量化分析菜单图标
            "策略回测": "📊",
            "股票推荐": "⭐",
            "因子分析": "🔍",

            # 系统菜单图标
            "设置": "⚙️",
            "关于": "ℹ️",
            "退出": "🚪"
        }

        for item in actions:
            if len(item) == 2:
                name, callback = item
                icon_name = None
            elif len(item) == 3:
                name, callback, icon_name = item
            else:
                continue

            if icon_name or name in icon_map:
                icon_char = icon_map.get(name, icon_name)
                if icon_char:
                    # 使用Unicode图标
                    action = QAction(f"{icon_char} {name}", self)
                else:
                    action = QAction(name, self)
            else:
                action = QAction(name, self)

            if callback is None:
                action.setEnabled(False)
                action.setToolTip(self.UNIMPLEMENTED_TOOLTIP)
            else:
                action.triggered.connect(callback)
            menu.addAction(action)

    def create_tool_bar(self):
        """
        创建工具栏
        """
        tool_bar = self.addToolBar("工具栏")
        tool_bar.setIconSize(QSize(16, 16))

        # 添加分隔符
        tool_bar.addSeparator()

        # 添加自选股按钮（未实现，置灰并标注）
        self.selected_btn = QPushButton("📋 自选股")
        self.selected_btn.setEnabled(False)
        self.selected_btn.setToolTip(self.UNIMPLEMENTED_TOOLTIP)
        tool_bar.addWidget(self.selected_btn)

        # 添加行情按钮（未实现，置灰并标注）
        self.market_btn = QPushButton("📊 行情")
        self.market_btn.setEnabled(False)
        self.market_btn.setToolTip(self.UNIMPLEMENTED_TOOLTIP)
        tool_bar.addWidget(self.market_btn)

        # 添加技术分析按钮
        self.tech_btn = QPushButton("📈 技术分析")
        self.tech_btn.clicked.connect(self.action_manager.on_technical_analysis)
        tool_bar.addWidget(self.tech_btn)

        # 添加分隔符
        tool_bar.addSeparator()

        # 添加刷新按钮（未实现，置灰并标注）
        refresh_btn = QPushButton("🔄 刷新")
        refresh_btn.setEnabled(False)
        refresh_btn.setToolTip(self.UNIMPLEMENTED_TOOLTIP)
        tool_bar.addWidget(refresh_btn)

        # 添加设置按钮（未实现，置灰并标注）
        settings_btn = QPushButton("⚙️ 设置")
        settings_btn.setEnabled(False)
        settings_btn.setToolTip(self.UNIMPLEMENTED_TOOLTIP)
        tool_bar.addWidget(settings_btn)
