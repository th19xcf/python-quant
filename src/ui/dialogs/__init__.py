"""
UI 对话框模块。从 src/quant/analysis/ 搬入，切断 UI 层反向依赖。
"""

from src.ui.dialogs.factor_analysis_dialog import FactorAnalysisDialog
from src.ui.dialogs.stock_recommendation_dialog import StockRecommendationDialog
from src.ui.dialogs.strategy_backtest_dialog import StrategyBacktestDialog

__all__ = [
    'StrategyBacktestDialog',
    'StockRecommendationDialog',
    'FactorAnalysisDialog',
]
