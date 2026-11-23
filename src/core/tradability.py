"""兼容入口：实现已迁至 :mod:`src.risk.tradability`（2026-11-23 分层整改）。

可交易性过滤只依赖 polars / loguru，属于风控层基础能力；放在 ``src.core``
会让 ``src.backtest.runner.pipeline`` 反向依赖用例层（tests/test_panel_inputs_layering.py）。
新代码请直接 ``from src.risk.tradability import ...``；本模块仅为 work/ 脚本等存量调用保留。
"""
from src.risk.tradability import (  # noqa: F401
    ItemStatus,
    TradabilityConfig,
    TradabilityFilter,
    TradabilityResult,
    latest_mean_amount,
)

__all__ = ['ItemStatus', 'TradabilityConfig', 'TradabilityFilter',
           'TradabilityResult', 'latest_mean_amount']
