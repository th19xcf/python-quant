"""
核心领域层（Headless API）。

设计原则：
    1. 不依赖 PyQt / UI。CLI、GUI、服务端共用同一套用例。
    2. 用例层只编排组件调用（DataFetcher → BacktestEngine → RiskEngine），
       不持有具体 UI 状态。
    3. 返回值统一是 polars.DataFrame / dict / list，不做 UI 渲染。

与 src/api/ 的关系：
    - src/api/ 是接口定义层（抽象接口）
    - src/core/usecases/ 是具体的用例编排
    - 两者可以共存，但 UI 层只应该依赖 usecases
"""

from src.core.usecases.run_backtest import run_backtest
from src.core.usecases.screen_stocks import screen_stocks
from src.core.usecases.update_data import update_stock_data

__all__ = [
    'run_backtest',
    'screen_stocks',
    'update_stock_data',
]
