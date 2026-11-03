#!/usr/bin/env python3

"""
量化因子库（factor_lib）—— 独立于 ``src/alpha`` 的公开因子研究与验证模块。

模块边界：
    - 只做「公开公式 → 统一口径 → 可核验评估」，不接入生产信号；
    - 不修改 ``src/alpha`` / ``src/backtest``，只调用其唯一真源
      （``ExecutionRules`` / ``CostModel`` / ``tech_analysis.formulas``）；
    - 报告产物写 ``reports/factor_lib/``，不进生产配置。

快速开始::

    from src.quant.factor_lib import data, compute, evaluate, redundancy

    panel = data.load_panel(data.PanelRequest(universe_size=80))
    result = compute.compute_all(panel)
"""

from __future__ import annotations

__all__ = ['compute', 'data', 'evaluate', 'redundancy', 'report', 'strategies']
