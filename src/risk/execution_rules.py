#!/usr/bin/env python3

"""兼容层：涨跌停 / 可交易性规则已下沉到 :mod:`src.core.market_rules`。

``src.data`` 曾模块级反向 import 本模块的 ``ExecutionRules``，使数据批处理
被策略/执行层拖累（risk 新增依赖会直接传导到 ST 推断与市场宽度）。规则本身
不撮合、不记账、不读文件，是纯函数，现由 ``src.core``（领域层）承载，
data / advice / risk / backtest 统一下向依赖。

本模块只做重导出，不复制任何逻辑，保持
``from src.risk.execution_rules import ExecutionRules`` 的既有调用不变。
"""

from src.core.market_rules import PRICE_TICK, ExecutionRules, finite_float

__all__ = ['PRICE_TICK', 'ExecutionRules', 'finite_float']
