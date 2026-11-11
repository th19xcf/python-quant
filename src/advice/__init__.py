#!/usr/bin/env python3

"""每日收盘后推荐清单（Advice）包。

定位：交易日收盘后把横截面因子打分转成可人工执行的建议清单。
本包只产出建议，不承担实盘下单；撮合约束复用
``src.risk.execution_rules.ExecutionRules``，不在推荐层重复实现。

对外入口：
- ``AdviceEngine.generate``：生成当日三档榜单；
- ``run_replay``：历史回放与成本后统计；
- ``AdviceConfig`` / ``TIER_PRESETS``：参数与档位预设。
"""

from src.advice.config import (
    TIER_PRESETS,
    AdviceConfig,
    PortfolioConstraints,
    TierConfig,
    TradingCosts,
    default_config,
)
from src.advice.models import AdviceResult, Recommendation
from src.advice.pipeline import AdviceEngine
from src.advice.replay import ReplayResult, run_replay

__all__ = [
    'TIER_PRESETS',
    'AdviceConfig',
    'AdviceEngine',
    'AdviceResult',
    'PortfolioConstraints',
    'Recommendation',
    'ReplayResult',
    'TierConfig',
    'TradingCosts',
    'default_config',
    'run_replay',
]
