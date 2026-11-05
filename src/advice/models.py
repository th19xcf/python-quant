#!/usr/bin/env python3

"""推荐清单的数据模型与导出 schema。"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

import polars as pl

DIRECTION_BUY = 'BUY'
DIRECTION_SELL = 'SELL'
DIRECTION_HOLD = 'HOLD'
DIRECTION_AVOID = 'AVOID'

#: 导出列顺序（CLI CSV/JSON 统一口径）
OUTPUT_COLUMNS = (
    'trade_date', 'ts_code', 'name', 'name_source', 'direction',
    'disposition',
    'target_weight_pct', 'shares', 'ref_price', 'horizon', 'validity_days',
    'confidence', 'top_factors', 'risk_notes', 'industry', 'industry_source',
    'score', 'score_percentile',
)


@dataclass
class Recommendation:
    """一条可人工执行的推荐记录。"""

    trade_date: date
    ts_code: str
    name: str
    direction: str
    target_weight: float  # 0~1，占总资金
    shares: int
    ref_price: float
    horizon: str
    validity_days: int
    confidence: float
    top_factors: list[tuple[str, float]]
    risk_notes: list[str]
    industry: str
    industry_source: str
    score: float
    score_percentile: float
    name_source: str = 'code'
    #: ``active``（本次可执行/持有）或 ``deferred``（被换手上限延后）
    disposition: str = 'active'
    #: 延后原因（仅 ``disposition='deferred'`` 时填写）
    defer_reason: str = ''
    #: 预计可执行日（仅 ``disposition='deferred'`` 时填写）
    defer_until: date | None = None

    def to_row(self) -> dict[str, Any]:
        """转为 CSV 友好的扁平字典（百分比字段按百分数输出）。"""
        factors = ' | '.join(
            f'{name} {value:+.2f}' for name, value in self.top_factors[:3])
        return {
            'trade_date': self.trade_date.isoformat(),
            'ts_code': self.ts_code,
            'name': self.name,
            'name_source': self.name_source,
            'direction': self.direction,
            'disposition': self.disposition,
            'target_weight_pct': round(self.target_weight * 100, 2),
            'shares': int(self.shares),
            'ref_price': round(float(self.ref_price), 3),
            'horizon': self.horizon,
            'validity_days': int(self.validity_days),
            'confidence': round(float(self.confidence), 3),
            'top_factors': factors,
            'risk_notes': '；'.join(self.risk_notes),
            'industry': self.industry,
            'industry_source': self.industry_source,
            'score': round(float(self.score), 4),
            'score_percentile': round(float(self.score_percentile), 4),
        }


@dataclass
class TierAdvice:
    """单档位的推荐结果与组合概况。"""

    tier: str
    label: str
    trade_date: date
    rows: list[Recommendation] = field(default_factory=list)
    equity: float = 0.0
    invested: float = 0.0
    cash: float = 0.0
    excluded: int = 0
    candidates: int = 0
    turnover: float = 0.0
    notes: list[str] = field(default_factory=list)
    #: 风险过滤计数（st_dropped / event_dropped / *_unknown_passed 等）
    filter_stats: dict[str, int] = field(default_factory=dict)
    #: 逐条剔除记录（交易日、标的、类型、原因），供人工核对
    filter_audit: list[dict[str, Any]] = field(default_factory=list)
    #: 同日同档重复行合并记录（合并键 = 档位 + 代码）
    row_merge: list[dict[str, Any]] = field(default_factory=list)

    def to_frame(self) -> pl.DataFrame:
        rows = [rec.to_row() for rec in self.rows]
        if not rows:
            return pl.DataFrame({col: [] for col in OUTPUT_COLUMNS})
        return pl.DataFrame(rows).select(list(OUTPUT_COLUMNS))

    def summary(self) -> dict[str, Any]:
        counts: dict[str, int] = {}
        for rec in self.rows:
            counts[rec.direction] = counts.get(rec.direction, 0) + 1
        return {
            'tier': self.tier,
            'label': self.label,
            'trade_date': self.trade_date.isoformat(),
            'equity': round(self.equity, 2),
            'invested': round(self.invested, 2),
            'cash': round(self.cash, 2),
            'candidates': self.candidates,
            'excluded': self.excluded,
            'turnover_pct': round(self.turnover * 100, 2),
            'counts': counts,
            'notes': self.notes,
            'filter_stats': dict(self.filter_stats),
            'filter_audit': self.filter_audit[:20],
            'row_merge': {
                'dropped': len(self.row_merge),
                'details': self.row_merge[:20],
            },
        }


@dataclass
class AdviceResult:
    """一次收盘后推荐生成的完整结果（三张榜）。"""

    trade_date: date
    equity: float
    tiers: dict[str, TierAdvice] = field(default_factory=dict)
    #: 信号日（面板最后交易日），与 ``trade_date`` 同义，显式写出避免相对词歧义
    signal_date: date | None = None
    #: 最早可执行交易日（T+1）
    earliest_exec_date: date | None = None
    #: 执行状态：``pending`` / ``expired``
    exec_status: str = 'pending'
    #: 下一交易日的来源：``calendar`` / ``weekday_fallback``
    exec_date_basis: str = 'weekday_fallback'

    def to_frame(self) -> pl.DataFrame:
        frames = [advice.to_frame() for advice in self.tiers.values()]
        frames = [frame for frame in frames if not frame.is_empty()]
        if not frames:
            return pl.DataFrame({col: [] for col in OUTPUT_COLUMNS})
        return pl.concat(frames).select(list(OUTPUT_COLUMNS))

    def summary(self) -> dict[str, Any]:
        return {
            'trade_date': self.trade_date.isoformat(),
            'signal_date': (self.signal_date or self.trade_date).isoformat(),
            'earliest_exec_date': self.earliest_exec_date.isoformat()
            if self.earliest_exec_date else None,
            'exec_status': self.exec_status,
            'exec_date_basis': self.exec_date_basis,
            'equity': round(self.equity, 2),
            'tiers': {
                key: advice.summary() for key, advice in self.tiers.items()
            },
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [{**rec.to_row(), 'tier': key}
                for key, advice in self.tiers.items() for rec in advice.rows]


@dataclass
class HoldingState:
    """跨日持久化的持仓状态（换手控制的输入）。"""

    ts_code: str
    shares: int
    entry_date: date
    entry_price: float
    smoothed_percentile: float = 0.5
    sell_confirm: int = 0

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload['entry_date'] = self.entry_date.isoformat()
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> HoldingState:
        return cls(
            ts_code=str(payload['ts_code']),
            shares=int(payload['shares']),
            entry_date=date.fromisoformat(str(payload['entry_date'])),
            entry_price=float(payload['entry_price']),
            smoothed_percentile=float(payload.get('smoothed_percentile', 0.5)),
            sell_confirm=int(payload.get('sell_confirm', 0)),
        )
