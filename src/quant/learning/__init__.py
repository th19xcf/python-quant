#!/usr/bin/env python3

"""自学习第一阶段：walk-forward 框架、权重学习器与评估器。

- L1 ``run_param_adaptation``：按市场状态滚动选择策略参数；
- L2 ``run_weight_learning``：滚动估计因子权重（IC/ICIR/衰减/Ridge/LightGBM）；
- 评估 ``evaluate_scheme``：IC/ICIR、分层单调性、换手、成本后收益与回撤。

入口：``python -m src.quant.learning``。
"""

from src.quant.learning.config import (
                                       LearningReport,
                                       SchemeMetrics,
                                       WalkForwardConfig,
                                       WalkForwardWindow,
)
from src.quant.learning.evaluator import (
                                       evaluate_scheme,
                                       ic_stats,
                                       quantile_analysis,
                                       simulate_long_only,
)
from src.quant.learning.walk_forward import (
                                       build_learning_report,
                                       prepare_panel,
                                       run_param_adaptation,
                                       run_weight_learning,
)

__all__ = [
    'LearningReport',
    'SchemeMetrics',
    'WalkForwardConfig',
    'WalkForwardWindow',
    'build_learning_report',
    'evaluate_scheme',
    'ic_stats',
    'prepare_panel',
    'quantile_analysis',
    'run_param_adaptation',
    'run_weight_learning',
    'simulate_long_only',
]
