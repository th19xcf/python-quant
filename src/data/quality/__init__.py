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
from src.data.quality.semantics import (
    STATISTIC_SERIES,
    STATISTIC_SERIES_CODES,
    is_statistic_series,
)
from src.data.quality.tradable_universe import (
    build_exclusions,
    exclusion_reasons,
    is_tradable,
    load_exclusions,
)

__all__ = [
    'KnownAnomaly',
    'LocalStoreFrameSource',
    'QualityChecker',
    'QualityReport',
    'QualityRule',
    'QualityViolation',
    'RULES',
    'RuleContext',
    'STATISTIC_SERIES',
    'STATISTIC_SERIES_CODES',
    'TdxFrameSource',
    'anomaly_action',
    'build_exclusions',
    'coerce_date',
    'exclusion_reasons',
    'find_anomalies',
    'is_known_anomaly',
    'is_statistic_series',
    'is_tradable',
    'iter_anomalies',
    'load_exclusions',
    'prepare_frame',
    'query_anomalies',
    'register_anomaly',
    'rule_map',
]
