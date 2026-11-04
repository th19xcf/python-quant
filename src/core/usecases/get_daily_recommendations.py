#!/usr/bin/env python3

"""每日推荐用例（Headless API）。

UI、CLI 与后续服务端只依赖本模块返回的稳定 DTO，不直接接触
``src.advice`` 内部实现或数据库/量化模块。

契约：
- ``DailyRecommendationDTO`` 字段与推荐闭环输出一一对应；
- ``DailyRecommendationBundle`` 额外携带数据日期、生成时间、新鲜度与告警；
- 引擎调整时只替换数据来源，UI 表格/详情映射保持不变。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from loguru import logger

from src.advice.config import AdviceConfig, default_config
from src.advice.daily_store import (
    list_daily_snapshots,
    load_daily_snapshot,
)
from src.advice.pipeline import AdviceEngine
from src.utils.code_converter import is_fund_code

_FACTOR_RE = re.compile(r'(?P<label>.+?)\s+(?P<value>[+-]?\d+\.\d+)')

TIER_LABELS = {'short': '短线', 'mid': '中线', 'long': '长线'}
ALL_TIERS = ('short', 'mid', 'long')


@dataclass
class DailyRecommendationDTO:
    """一条推荐（UI 稳定契约）。"""

    trade_date: str
    ts_code: str
    name: str
    direction: str
    target_weight_pct: float
    shares: int
    ref_price: float
    horizon: str
    validity_days: int
    confidence: float
    top_factors: str
    risk_notes: str
    industry: str
    asset_type: str = '股票'
    tier: str = 'mid'
    tier_label: str = '中线'
    score: float = 0.0
    score_percentile: float = 0.0
    factor_contributions: list[dict[str, Any]] = field(default_factory=list)
    name_source: str = 'code'
    industry_source: str = 'code_prefix_board'

    def to_row(self) -> dict[str, Any]:
        """表格展示用的扁平字典（列名与 UI 表格一一对应）。"""
        return {
            'ts_code': self.ts_code,
            'name': self.name,
            'direction': self.direction,
            'target_weight_pct': self.target_weight_pct,
            'shares': self.shares,
            'ref_price': self.ref_price,
            'confidence': self.confidence,
            'top_factors': self.top_factors,
            'risk_notes': self.risk_notes,
            'horizon': self.horizon,
            'validity_days': self.validity_days,
            'industry': self.industry,
            'asset_type': self.asset_type,
            'tier': self.tier,
            'trade_date': self.trade_date,
            'score': self.score,
            'score_percentile': self.score_percentile,
        }


@dataclass
class DailyRecommendationBundle:
    """推荐清单聚合结果与新鲜度元信息。"""

    items: list[DailyRecommendationDTO] = field(default_factory=list)
    trade_date: date | None = None
    generated_at: datetime = field(default_factory=datetime.now)
    data_lag_days: int = 0
    is_sample: bool = False
    equity: float = 1_000_000.0
    warnings: list[str] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)
    source: str = 'engine'

    def by_tier(self, tier: str) -> list[DailyRecommendationDTO]:
        if tier == 'all':
            return list(self.items)
        return [item for item in self.items if item.tier == tier]

    def counts(self) -> dict[str, int]:
        result: dict[str, int] = {}
        for item in self.items:
            result[item.direction] = result.get(item.direction, 0) + 1
        return result

    @property
    def is_stale(self) -> bool:
        """>1 个自然日未更新即视为过期，需要显式警示。"""
        return self.data_lag_days > 1


def _parse_factors(text: str) -> list[dict[str, Any]]:
    contributions: list[dict[str, Any]] = []
    for label, value in _FACTOR_RE.findall(str(text or '')):
        try:
            contributions.append({'label': label.strip(),
                                  'value': float(value)})
        except ValueError:
            continue
    return contributions


_ETF_PREFIXES = ('51', '56', '58', '15', '16')


def _asset_type(ts_code: str) -> str:
    """资产类型：ETF 按代码段细分，其余基金统一归为「基金」。"""
    if not is_fund_code(ts_code):
        return '股票'
    symbol = str(ts_code).split('.')[0]
    return 'ETF' if symbol[:2] in _ETF_PREFIXES else '基金'


def _to_dto(row: dict[str, Any], tier: str) -> DailyRecommendationDTO:
    return DailyRecommendationDTO(
        trade_date=str(row.get('trade_date', '')),
        ts_code=str(row.get('ts_code', '')),
        name=str(row.get('name', row.get('ts_code', ''))),
        direction=str(row.get('direction', 'HOLD')),
        target_weight_pct=float(row.get('target_weight_pct') or 0.0),
        shares=int(row.get('shares') or 0),
        ref_price=float(row.get('ref_price') or 0.0),
        horizon=str(row.get('horizon', TIER_LABELS.get(tier, '中线'))),
        validity_days=int(row.get('validity_days') or 0),
        confidence=float(row.get('confidence') or 0.0),
        top_factors=str(row.get('top_factors') or ''),
        risk_notes=str(row.get('risk_notes') or ''),
        industry=str(row.get('industry') or '未知'),
        asset_type=_asset_type(str(row.get('ts_code', ''))),
        tier=tier,
        tier_label=TIER_LABELS.get(tier, tier),
        score=float(row.get('score') or 0.0),
        score_percentile=float(row.get('score_percentile') or 0.0),
        factor_contributions=_parse_factors(str(row.get('top_factors') or '')),
        name_source=str(row.get('name_source') or 'code'),
        industry_source=str(row.get('industry_source') or 'code_prefix_board'),
    )


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _default_dirs() -> tuple[Path, Path]:
    base = _project_root() / 'reports' / 'advice'
    return base, base / 'state'


def _archived_daily_dir(out_dir: str | Path | None = None) -> Path:
    return Path(out_dir) / 'daily' if out_dir else _default_dirs()[0] / 'daily'


def list_archived_dates(out_dir: str | Path | None = None) -> list[str]:
    """列出已归档的推荐交易日（升序）。"""
    return [str(entry['trade_date'])
            for entry in list_daily_snapshots(_archived_daily_dir(out_dir))]


def load_archived_recommendations(trade_date: Any,
                                  horizon: str = 'short',
                                  *, out_dir: str | Path | None = None
                                  ) -> DailyRecommendationBundle | None:
    """读取某交易日的归档清单；不存在返回 ``None``。

    归档不含换手统计，``summary`` 只标记 ``archive``，UI 用 ``source`` 区分。
    """
    try:
        parsed = (trade_date if isinstance(trade_date, date)
                  else date.fromisoformat(str(trade_date)[:10]))
    except ValueError:
        return None
    payload = load_daily_snapshot(_archived_daily_dir(out_dir), parsed)
    if payload is None:
        return None
    rows: list[dict[str, Any]] = []
    for tier in payload.get('tiers', []):
        key = str(tier.get('tier', 'mid'))
        label = str(tier.get('label', TIER_LABELS.get(key, '中线')))
        for item in tier.get('items', []):
            rows.append({**item, 'tier': key, 'horizon': label})
    items = [_to_dto(row, str(row['tier'])) for row in rows]
    if horizon != 'all':
        items = [item for item in items if item.tier == horizon]
    generated_at = datetime.now()
    try:
        generated_at = datetime.fromisoformat(str(payload.get('generated_at')))
    except (TypeError, ValueError):
        pass
    return DailyRecommendationBundle(
        items=items, trade_date=parsed, generated_at=generated_at,
        data_lag_days=0, equity=float(payload.get('equity') or 1_000_000.0),
        warnings=['历史归档：展示当时落盘的清单，不会重新计算'],
        summary={'archive': True}, source='archive')


def _load_snapshot(path: Path, horizon: str, equity: float) -> DailyRecommendationBundle | None:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        logger.warning(f'[usecase] 推荐快照读取失败: {path}: {exc}')
        return None
    items = [_to_dto(row, str(row.get('tier', 'mid'))) for row in payload.get('items', [])]
    if horizon != 'all':
        items = [item for item in items if item.tier == horizon]
    trade_date = payload.get('trade_date')
    parsed_date = None
    if trade_date:
        try:
            parsed_date = date.fromisoformat(str(trade_date)[:10])
        except ValueError:
            parsed_date = None
    generated_at = datetime.now()
    try:
        generated_at = datetime.fromisoformat(str(payload.get('generated_at')))
    except (TypeError, ValueError):
        pass
    lag = (date.today() - parsed_date).days if parsed_date else 0
    warnings: list[str] = []
    if lag > 1:
        warnings.append(f'数据日期 {parsed_date} 已落后 {lag} 天，请重新生成')
    return DailyRecommendationBundle(
        items=items, trade_date=parsed_date, generated_at=generated_at,
        data_lag_days=lag, is_sample=bool(payload.get('is_sample', False)),
        equity=float(payload.get('equity') or equity), warnings=warnings,
        summary=payload.get('tiers', {}), source='snapshot')


def _save_snapshot(bundle: DailyRecommendationBundle, path: Path,
                   summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'trade_date': bundle.trade_date.isoformat() if bundle.trade_date else None,
        'generated_at': bundle.generated_at.isoformat(timespec='seconds'),
        'equity': bundle.equity,
        'is_sample': bundle.is_sample,
        'tiers': summary,
        'items': [{**item.to_row(), 'factor_contributions': item.factor_contributions}
                  for item in bundle.items],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding='utf-8')


def get_daily_recommendations(trade_date: Any = None, horizon: str = 'short',
                              *, equity: float = 1_000_000.0,
                              refresh: bool = False,
                              config: AdviceConfig | None = None,
                              store_root: Any = None,
                              state_dir: str | Path | None = None,
                              out_dir: str | Path | None = None,
                              tiers: Iterable[str] | None = None) -> DailyRecommendationBundle:
    """获取每日推荐清单（UI 唯一入口）。

    Args:
        trade_date: 基准日；None 表示最新交易日。支持 date/str。
        horizon: ``short``/``mid``/``long``/``all``，只返回对应档位。
        refresh: True 时忽略本地快照并重新生成。
        equity: 组合总资金，用于仓位与股数换算。

    返回结构化 DTO 列表与新鲜度元信息；没有数据时返回空列表而非抛异常，
    由 UI 显示空状态。
    """
    snapshot_dir, default_state = _default_dirs()
    output_dir = Path(out_dir) if out_dir else snapshot_dir
    state_path = Path(state_dir) if state_dir else default_state
    snapshot_path = output_dir / 'latest.json'
    horizon = horizon if horizon in ('short', 'mid', 'long', 'all') else 'short'

    if not refresh and trade_date is None:
        cached = _load_snapshot(snapshot_path, horizon, equity)
        if cached is not None and cached.items:
            return cached

    advice_config = config or default_config()
    engine = AdviceEngine(config=advice_config, store_root=store_root)
    result = engine.generate(
        as_of=trade_date, equity=equity,
        tiers=list(tiers) if tiers else list(ALL_TIERS),
        state_dir=state_path,
        daily_dir=output_dir / 'daily', persist_daily=True)
    generated_at = datetime.now()
    lag = (date.today() - result.trade_date).days if result.trade_date else 0
    items = [_to_dto({**rec.to_row(), 'tier': key}, key)
             for key, advice in result.tiers.items() for rec in advice.rows]
    warnings: list[str] = []
    if lag > 1:
        warnings.append(f'数据日期 {result.trade_date} 已落后 {lag} 天，请重新生成')
    if not items:
        warnings.append('本地列存暂无可推荐数据，请先更新行情或检查数据目录')
    bundle = DailyRecommendationBundle(
        items=items, trade_date=result.trade_date, generated_at=generated_at,
        data_lag_days=lag, is_sample=False, equity=equity, warnings=warnings,
        summary=result.summary()['tiers'], source='engine')
    if horizon != 'all':
        bundle.items = [item for item in bundle.items if item.tier == horizon]
    _save_snapshot(bundle, snapshot_path, result.summary()['tiers'])
    logger.info(
        f"[usecase] 推荐就绪: {result.trade_date}, {len(bundle.items)} 条, "
        f"档位={horizon}, 来源={'引擎' if bundle.source == 'engine' else '快照'}")
    return bundle
