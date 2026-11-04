#!/usr/bin/env python3

"""数据准确性巡检、已知异常台账与跨源对拍。"""

from src.data.quality.anomalies import (
    KnownAnomaly,
    anomaly_action,
    find_anomalies,
    is_known_anomaly,
    iter_anomalies,
    query_anomalies,
    register_anomaly,
)
from src.data.quality.checker import (
    LocalStoreFrameSource,
    QualityChecker,
    QualityReport,
    QualityViolation,
    TdxFrameSource,
    coerce_date,
    prepare_frame,
)
from src.data.quality.rules import RULES, QualityRule, RuleContext, rule_map

__all__ = [
    'KnownAnomaly',
    'LocalStoreFrameSource',
    'QualityChecker',
    'QualityReport',
    'QualityRule',
    'QualityViolation',
    'RULES',
    'RuleContext',
    'TdxFrameSource',
    'anomaly_action',
    'coerce_date',
    'find_anomalies',
    'is_known_anomaly',
    'iter_anomalies',
    'prepare_frame',
    'query_anomalies',
    'register_anomaly',
    'rule_map',
]
