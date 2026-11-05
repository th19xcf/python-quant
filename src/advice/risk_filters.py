#!/usr/bin/env python3

"""三类风险过滤：ST 三态、行业二级桶、公告事件冷却。

三条红线：

1. ``unknown`` 不是 ``non_st``：ST 状态未知时 **fail-closed**（默认 ``drop``），
   一律不进买入候选并单列 ``st_unknown_rejected``；只有明确 ``non_st`` 才放行；
2. 行业未知单独成桶（``unknown``），绝不并入任何真实行业；
3. 事件数据覆盖不到的日期返回 ``unknown``，不默认当成「没有事件」。

所有查询都按**信号日 / 调仓日**传 ``as_of``，禁止用「数据最新日」回填历史。
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime
from pathlib import Path
from typing import Any, Sequence

from src.advice.config import RiskFilterConfig
from src.advice.universe import StockMeta
from src.data import st_history
from src.data.events import STATUS_COOLING, STATUS_UNKNOWN, EventStore
from src.data.industry import load_snapshot as load_industry_snapshot
from src.data.risk_flags import (
    ST,
)
from src.data.risk_flags import (
    UNKNOWN as ST_UNKNOWN,
)
from src.data.risk_flags import (
    load_snapshot as load_st_snapshot,
)

INDUSTRY_UNKNOWN = 'unknown'
INDUSTRY_SOURCE_L2 = 'industry_l2'
INDUSTRY_SOURCE_MISSING = 'missing'

ST_UNKNOWN_POLICY_KEEP = 'keep'
ST_UNKNOWN_POLICY_DROP = 'drop'


def _as_date(value: Any) -> date | None:
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


@dataclass
class RiskVerdict:
    """单标的在信号日的风险判定。"""

    ok: bool
    kind: str = ''
    reason: str = ''
    detail: dict[str, Any] = field(default_factory=dict)


class RiskContext:
    """一次运行内的风险上下文（按信号日缓存快照，避免逐行读盘）。"""

    def __init__(self, config: RiskFilterConfig | None,
                 store_root: Path | str,
                 trading_dates: Sequence[date]) -> None:
        self.config = config or RiskFilterConfig()
        self.config.validate()
        self.store_root = Path(store_root)
        self.trading_dates = sorted(trading_dates)
        self.st_snapshot_date: date | None = None
        self.industry_snapshot_date: date | None = None
        self.counts: dict[str, int] = {
            'st_dropped': 0, 'st_unknown_rejected': 0, 'st_unknown_passed': 0,
            'event_dropped': 0, 'event_unknown_passed': 0,
            'industry_unknown': 0,
        }
        self.audit: list[dict[str, Any]] = []
        self.audit_limit = 200

        self.events: EventStore | None = None
        self.event_coverage: dict[str, Any] = {}
        self._blocked: dict[date, dict[str, dict]] = {}
        if self.config.event_enabled:
            self.events = EventStore(self.store_root,
                                     self.config.cooldowns())
            self.event_coverage = dict(self.events.coverage)
            if self.events.available:
                self._blocked = self.events.blocked_map(self.trading_dates)

        self._st_maps: dict[date, dict[str, str]] = {}
        self._industry_maps: dict[date, dict[str, tuple[str, str]]] = {}

    # ------------------------------------------------------------------
    # 快照查询
    # ------------------------------------------------------------------
    def _st_map(self, as_of: date) -> dict[str, str] | None:
        frame, snapshot_day = load_st_snapshot(self.store_root, as_of)
        if frame is None or snapshot_day is None:
            return None
        if snapshot_day not in self._st_maps:
            self._st_maps[snapshot_day] = {
                str(row['code']): str(row['status'])
                for row in frame.select('code', 'status').to_dicts()
            }
            self.st_snapshot_date = snapshot_day
        return self._st_maps[snapshot_day]

    def st_status(self, code: str, as_of: date) -> str:
        """ST 三态：快照优先，早于首份快照的日期回退历史区间表。

        无快照且不在区间表可信窗口内一律 ``unknown``（不是 ``non_st``）。
        """
        mapping = self._st_map(as_of)
        status = mapping.get(str(code).upper(), ST_UNKNOWN) if mapping else ST_UNKNOWN
        if status != ST_UNKNOWN:
            return status
        hit = st_history.status_at(self.store_root, code, as_of)
        return hit.status if hit.status != ST_UNKNOWN else ST_UNKNOWN

    def _industry_map(self, as_of: date) -> dict[str, tuple[str, str]] | None:
        frame, snapshot_day = load_industry_snapshot(self.store_root, as_of)
        if frame is None or snapshot_day is None:
            return None
        if snapshot_day not in self._industry_maps:
            self._industry_maps[snapshot_day] = {
                str(row['code']): (str(row['industry_l2']), INDUSTRY_SOURCE_L2)
                if str(row['industry_l2']) != INDUSTRY_UNKNOWN
                else (INDUSTRY_UNKNOWN, INDUSTRY_SOURCE_MISSING)
                for row in frame.select('code', 'industry_l2').to_dicts()
            }
            self.industry_snapshot_date = snapshot_day
        return self._industry_maps[snapshot_day]

    def industry(self, code: str, as_of: date) -> tuple[str, str]:
        """返回 ``(行业桶, 来源)``；未知单独成桶，绝不并入真实行业。"""
        mapping = self._industry_map(as_of)
        if mapping is None:
            return INDUSTRY_UNKNOWN, INDUSTRY_SOURCE_MISSING
        return mapping.get(str(code).upper(),
                           (INDUSTRY_UNKNOWN, INDUSTRY_SOURCE_MISSING))

    def enrich_meta(self, meta: dict[str, StockMeta],
                    as_of: date) -> dict[str, StockMeta]:
        """把展示用行业替换为行业二级桶（开关关闭时原样返回）。"""
        if not self.config.industry_enabled:
            return meta
        enriched: dict[str, StockMeta] = {}
        for code, item in meta.items():
            bucket, source = self.industry(code, as_of)
            if bucket == INDUSTRY_UNKNOWN:
                self.counts['industry_unknown'] += 1
            enriched[code] = replace(item, industry=bucket,
                                     industry_source=source)
        return enriched

    def event_status(self, code: str, as_of: date) -> dict[str, Any]:
        """事件冷却状态；覆盖之外返回 ``unknown``。"""
        if self.events is None:
            return {'status': STATUS_UNKNOWN, 'reason': 'disabled'}
        if not self.events.available:
            return {'status': STATUS_UNKNOWN, 'reason': 'no_data'}
        if not self.events.covers(as_of):
            return {'status': STATUS_UNKNOWN, 'reason': 'no_coverage',
                    'coverage': self.event_coverage}
        hit = self._blocked.get(as_of, {}).get(str(code).upper())
        if hit:
            return {'status': STATUS_COOLING, **hit}
        return {'status': 'clear'}

    # ------------------------------------------------------------------
    # 建仓判定
    # ------------------------------------------------------------------
    def evaluate(self, code: str, as_of: date,
                 record: bool = True) -> RiskVerdict:
        """建仓前风险判定：ST 与事件冷却都会直接拦截。

        ``record=False`` 用于样本行等只读场景，避免重复计数。
        """
        if self.config.st_enabled:
            status = self.st_status(code, as_of)
            if status == ST:
                if record:
                    self._drop(code, as_of, 'st', 'ST/*ST 风险警示',
                               {'status': status})
                return RiskVerdict(False, 'st', 'ST/*ST 风险警示',
                                   {'status': status})
            if status == ST_UNKNOWN:
                if self.config.st_unknown_policy == ST_UNKNOWN_POLICY_DROP:
                    reason = 'ST 状态未知（fail-closed，不进买入候选）'
                    if record:
                        self._drop(code, as_of, 'st_unknown', reason,
                                   {'status': status},
                                   count_key='st_unknown_rejected')
                    return RiskVerdict(False, 'st_unknown', reason,
                                       {'status': status})
                if record:
                    self.counts['st_unknown_passed'] += 1

        if self.config.event_enabled and self.events is not None:
            if not self.events.available or not self.events.covers(as_of):
                if record:
                    self.counts['event_unknown_passed'] += 1
                if self.config.event_unknown_policy == ST_UNKNOWN_POLICY_DROP:
                    reason = '公告事件数据未覆盖（按 drop 策略剔除）'
                    if record:
                        self._drop(code, as_of, 'event', reason,
                                   {'status': STATUS_UNKNOWN})
                    return RiskVerdict(False, 'event', reason,
                                       {'status': STATUS_UNKNOWN})
            else:
                hit = self._blocked.get(as_of, {}).get(str(code).upper())
                if hit:
                    reason = (f"{hit['event_type']} 公告冷却至 "
                              f"{hit['cooldown_until']}")
                    if record:
                        self._drop(code, as_of, 'event', reason, dict(hit))
                    return RiskVerdict(False, 'event', reason, dict(hit))
        return RiskVerdict(True)

    def _drop(self, code: str, as_of: date, kind: str, reason: str,
              detail: dict[str, Any], count_key: str | None = None) -> None:
        self.counts[count_key or f'{kind}_dropped'] += 1
        if len(self.audit) < self.audit_limit:
            self.audit.append({
                'date': as_of.isoformat(), 'ts_code': str(code).upper(),
                'kind': kind, 'reason': reason, **detail,
            })

    # ------------------------------------------------------------------
    # 报告
    # ------------------------------------------------------------------
    def describe(self) -> dict[str, Any]:
        """过滤覆盖率与开关状态（写入档位说明与回放摘要）。"""
        return {
            'st': {
                'enabled': self.config.st_enabled,
                'unknown_policy': self.config.st_unknown_policy,
                'snapshot': self.st_snapshot_date.isoformat()
                if self.st_snapshot_date else None,
            },
            'industry': {
                'enabled': self.config.industry_enabled,
                'snapshot': self.industry_snapshot_date.isoformat()
                if self.industry_snapshot_date else None,
                'unknown_uncapped': True,
            },
            'events': {
                'enabled': self.config.event_enabled,
                'unknown_policy': self.config.event_unknown_policy,
                'coverage': self.event_coverage,
                'cooldowns': self.config.cooldowns(),
            },
            'counts': dict(self.counts),
        }

    def notes(self, industry_cap: float | None = None,
              counts: dict[str, int] | None = None) -> list[str]:
        counts = counts or self.counts
        notes: list[str] = []
        if self.config.st_enabled:
            snapshot = self.st_snapshot_date
            if self.config.st_unknown_policy == ST_UNKNOWN_POLICY_KEEP:
                notes.append(
                    f'ST 过滤：当日快照 {snapshot.isoformat() if snapshot else "无"}，'
                    f'未知 {counts["st_unknown_passed"]} 只按 keep 放行'
                    f'（未当作非 ST），剔除 {counts["st_dropped"]} 只')
            else:
                notes.append(
                    f'ST 过滤：unknown 按 drop（fail-closed）剔除，'
                    f'共拒绝 {counts["st_unknown_rejected"]} 只，'
                    f'明确 ST 剔除 {counts["st_dropped"]} 只')
        if self.config.industry_enabled:
            snapshot = self.industry_snapshot_date
            cap_text = f'{industry_cap:.0%}' if industry_cap else '未设置'
            notes.append(
                f'行业约束：通达信二级行业（快照 '
                f'{snapshot.isoformat() if snapshot else "无"}），'
                f'单行业上限 {cap_text}；'
                f'未知行业 {counts["industry_unknown"]} 只单独成桶且不设上限')
        if self.config.event_enabled:
            coverage = self.event_coverage
            span = f"{coverage.get('start')}~{coverage.get('end')}"
            if not self.events or not self.events.available:
                notes.append('事件过滤：本地事件表为空，全程 unknown（未当作无事件）')
            else:
                notes.append(
                    f'事件过滤：覆盖 {span}，覆盖外为 unknown 按 '
                    f'{self.config.event_unknown_policy} 处理，'
                    f'剔除 {counts["event_dropped"]} 只')
        return notes
