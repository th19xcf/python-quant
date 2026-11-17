"""策略试算标签页工厂：只负责创建原生 Qt 页面。"""
from __future__ import annotations

from src.ui.strategy_lab.page import StrategyLabPage


def create_strategy_lab_tab(parent=None) -> StrategyLabPage:
    return StrategyLabPage(parent)
