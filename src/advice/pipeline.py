#!/usr/bin/env python3

"""收盘后推荐闭环编排：本地行情 → 因子 → 打分 → 换手控制 → 仓位 → 清单。"""

from __future__ import annotations

import json
import math
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from loguru import logger

from src.advice.config import AdviceConfig, default_config
from src.advice.data import LocalPanelLoader, panel_trading_dates
from src.advice.eligibility import check_eligibility
from src.advice.factor_panel import compute_factor_panel
from src.advice.models import (
    AdviceResult, DIRECTION_AVOID, DIRECTION_BUY, DIRECTION_HOLD, DIRECTION_SELL,
    HoldingState, Recommendation, TierAdvice,
)
from src.advice.portfolio import Candidate, Allocation, allocate
from src.advice.scoring import (
    attach_score, confidence_from_score, explain_factors, factor_stats,
)
from src.advice.turnover import (
    HoldingDecision, TradeIntent, apply_turnover_cap, count_trading_days,
    entry_allowed, update_holding,
)
from src.advice.universe import (
    INDUSTRY_SOURCE_BOARD, build_meta, list_local_stock_codes, load_name_map,
)
from src.utils.config import get_config


def _resolve_as_of(config: AdviceConfig, values: list[date], as_of: date | None) -> date:
    if as_of is not None:
        if values:
            return max(value for value in values if value <= as_of) if any(
                value <= as_of for value in values) else max(values)
        return as_of
    return max(values) if values else date.today()


def _rescale(allocations: list[Allocation], cap_amount: float, equity: float,
             lot: int) -> list[Allocation]:
    """把新开仓总额压到可用资金以内，并重新按整手下单。"""
    total = sum(item.target_weight for item in allocations) * equity
    if total <= cap_amount or total <= 0:
        return allocations
    scale = cap_amount / total
    result: list[Allocation] = []
    for item in allocations:
        weight = item.target_weight * scale
        shares = int(math.floor(weight * equity / item.price / lot) * lot) if item.price > 0 else 0
        if shares <= 0:
            continue
        result.append(Allocation(
            ts_code=item.ts_code,
            target_weight=(shares * item.price) / equity,
            shares=shares,
            price=item.price,
            industry=item.industry,
        ))
    return result


class AdviceEngine:
    """每日推荐引擎（无 UI 依赖）。"""

    def __init__(self, config: AdviceConfig | None = None, store_root: Any = None,
                 name_map: dict[str, str] | None = None,
                 industry_map: dict[str, str] | None = None,
                 project_config: Any = None) -> None:
        self.config = config or default_config()
        if store_root is None:
            project_config = project_config or get_config()
            store_root = project_config.data.local_store_path
        self.store_root = Path(store_root)
        default_names = self.store_root / 'meta' / 'names.json'
        self.name_map = name_map if name_map is not None else load_name_map(default_names)
        self.industry_map = industry_map or {}

    # ------------------------------------------------------------------
    # 状态持久化
    # ------------------------------------------------------------------
    def load_state(self, state_dir: Path, tier_key: str) -> dict[str, HoldingState]:
        path = state_dir / f'{tier_key}.json'
        if not path.is_file():
            return {}
        try:
            payload = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, ValueError) as exc:
            logger.warning(f"[advice] 状态文件读取失败，按空仓处理: {path}: {exc}")
            return {}
        holdings: dict[str, HoldingState] = {}
        for item in payload.get('holdings', []):
            try:
                state = HoldingState.from_dict(item)
            except (KeyError, TypeError, ValueError) as exc:
                logger.warning(f"[advice] 状态条目损坏已跳过: {item}: {exc}")
                continue
            holdings[state.ts_code] = state
        return holdings

    def save_state(self, state_dir: Path, tier_key: str, trade_date: date,
                   equity: float, holdings: dict[str, HoldingState]) -> Path:
        state_dir.mkdir(parents=True, exist_ok=True)
        path = state_dir / f'{tier_key}.json'
        payload = {
            'tier': tier_key,
            'trade_date': trade_date.isoformat(),
            'equity': round(equity, 2),
            'holdings': [state.to_dict() for state in holdings.values()],
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                        encoding='utf-8')
        return path

    # ------------------------------------------------------------------
    # 主流程
    # ------------------------------------------------------------------
    def generate(self, as_of: Any = None, equity: float = 1_000_000.0,
                 tiers: Iterable[str] | None = None,
                 state_dir: str | Path | None = None,
                 persist_state: bool = True,
                 codes: Iterable[str] | None = None) -> AdviceResult:
        """生成指定日期（默认最新交易日）的三档推荐清单。"""
        tier_keys = list(tiers) if tiers else list(self.config.tiers)
        selected = [self.config.tier(key) for key in tier_keys]
        max_lookback = max(tier.lookback_days for tier in selected)

        loader = LocalPanelLoader(
            _StoreConfig(self.store_root), min_rows=self.config.min_history_rows)
        as_of_date = _as_date(as_of)
        end = as_of_date or date.today()
        start = loader.default_start(end, max_lookback)
        universe = list(codes) if codes is not None else list_local_stock_codes(
            _StoreConfig(self.store_root))
        panel, report = loader.load(universe, start, end)
        if panel.is_empty():
            return AdviceResult(trade_date=end, equity=float(equity))

        factors = compute_factor_panel(panel)
        trade_date = _resolve_as_of(self.config, factors['date'].unique().to_list(),
                                    as_of_date)
        meta = build_meta(factors['ts_code'].unique().to_list(),
                          self.name_map, self.industry_map)
        trading_dates = panel_trading_dates(factors)
        state_path = Path(state_dir) if state_dir else (
            Path(__file__).resolve().parents[2] / 'reports' / 'advice' / 'state')

        logger.info(
            f"[advice] 生成推荐: trade_date={trade_date}, equity={equity:,.0f}, "
            f"标的={report.loaded}, 档位={tier_keys}")

        result = AdviceResult(trade_date=trade_date, equity=float(equity))
        for tier in selected:
            scored = attach_score(factors, tier)
            state = self.load_state(state_path, tier.key)
            advice, new_state = self._run_tier(
                scored, trade_date, meta, tier, float(equity), state,
                trading_dates, report.summary())
            result.tiers[tier.key] = advice
            if persist_state:
                self.save_state(state_path, tier.key, trade_date,
                                float(equity), new_state)
        return result

    def _run_tier(self, scored: pl.DataFrame, trade_date: date,
                  meta: dict[str, Any], tier: Any, equity: float,
                  state: dict[str, HoldingState], trading_dates: list[date],
                  coverage: dict[str, Any]) -> tuple[TierAdvice, dict[str, HoldingState]]:
        latest = scored.filter(pl.col('date') == trade_date)
        if latest.is_empty():
            return TierAdvice(tier=tier.key, label=tier.label,
                              trade_date=trade_date, equity=equity), state
        rows = latest.to_dicts()
        stats_frame = factor_stats(scored, tier).filter(
            pl.col('date') == trade_date)
        stats = stats_frame.to_dicts()[0] if not stats_frame.is_empty() else None
        by_code = {str(row['ts_code']): row for row in rows}

        advice = TierAdvice(
            tier=tier.key, label=tier.label, trade_date=trade_date,
            equity=equity, candidates=len(rows),
            notes=[
                '行业无权威表，使用代码前缀/板块近似（industry_source=code_prefix_board）',
                f"本地面板覆盖 {coverage.get('loaded', 0)}/"
                f"{coverage.get('requested', 0)} 标的",
            ])

        # 1) 候选池：分数达标 + 可交易 + 成本门槛
        candidates: list[tuple[dict, Candidate, Any]] = []
        avoid_rows: list[tuple[dict, Any]] = []
        ranked = sorted(rows, key=lambda row: row.get('score_percentile') or 0.0,
                        reverse=True)
        for row in ranked:
            code = str(row['ts_code'])
            if code in state:
                continue
            percentile = float(row.get('score_percentile') or 0.0)
            if percentile >= tier.buy_percentile:
                eligibility = check_eligibility(row, tier, self.config, 'buy')
                if eligibility.ok:
                    ok_edge, reason = entry_allowed(percentile, tier, self.config)
                    if ok_edge:
                        stock_meta = meta.get(code)
                        candidates.append((row, Candidate(
                            ts_code=code,
                            score_percentile=percentile,
                            price=float(row.get('raw_close') or 0.0),
                            industry=stock_meta.industry if stock_meta else '未知',
                            industry_source=(stock_meta.industry_source
                                             if stock_meta else INDUSTRY_SOURCE_BOARD),
                        ), eligibility))
                    elif len(avoid_rows) < self.config.max_avoid_rows:
                        avoid_rows.append((row, eligibility))
                elif len(avoid_rows) < self.config.max_avoid_rows:
                    avoid_rows.append((row, eligibility))
            elif percentile >= tier.buy_percentile - 0.05:
                eligibility = check_eligibility(row, tier, self.config, 'buy')
                if not eligibility.ok and len(avoid_rows) < self.config.max_avoid_rows:
                    avoid_rows.append((row, eligibility))

        best_pct = candidates[0][1].score_percentile if candidates else None

        # 2) 持仓决策：最小持有期 + 确认日 + 成本门槛
        decisions: dict[str, HoldingDecision] = {}
        for code, holding in state.items():
            row = by_code.get(code)
            days = count_trading_days(holding.entry_date, trade_date, trading_dates)
            if row is None:
                percentile = 0.0
                note_code = '当日无行情'
            else:
                percentile = float(row.get('score_percentile') or 0.0)
                note_code = ''
            decision = update_holding(
                prev_percentile=holding.smoothed_percentile,
                percentile=percentile,
                sell_confirm=holding.sell_confirm,
                days_held=days,
                tier=tier,
                config=self.config,
                best_candidate_pct=best_pct,
                ts_code=code,
            )
            if note_code:
                decision.reason = f'{decision.reason}；{note_code}'
            decisions[code] = decision

        exits = {code: decision for code, decision in decisions.items()
                 if decision.action == 'SELL'}
        remaining = {code: holding for code, holding in state.items()
                     if code not in exits}
        existing_value = sum(
            holding.shares * float(by_code[code]['raw_close'])
            for code, holding in remaining.items()
            if code in by_code and by_code[code].get('raw_close'))

        # 3) 新开仓分配（受可用资金、档位持仓数与单票/行业上限约束）
        slots = max(0, tier.max_positions - len(remaining))
        available = max(0.0, equity * (1.0 - self.config.constraints.cash_buffer)
                        - existing_value)
        constraints = replace(self.config.constraints, cash_buffer=0.0)
        allocations = allocate([item[1] for item in candidates], equity,
                               constraints, max_positions=slots)
        allocations = _rescale(allocations, available, equity,
                               self.config.constraints.lot_size)
        alloc_by_code = {item.ts_code: item for item in allocations}

        # 4) 换手上限
        intents: list[TradeIntent] = []
        for code, holding in state.items():
            if code in exits:
                price = float(by_code.get(code, {}).get('raw_close') or holding.entry_price)
                intents.append(TradeIntent(
                    ts_code=code, side='sell', notional=holding.shares * price,
                    priority=decisions[code].smoothed_percentile))
        for code, item in alloc_by_code.items():
            intents.append(TradeIntent(
                ts_code=code, side='buy', notional=item.target_weight * equity,
                priority=item.target_weight))
        plan = apply_turnover_cap(
            intents, equity, self.config.turnover.max_turnover_per_rebalance)
        admitted_sells = {intent.ts_code for intent in plan.admitted
                          if intent.side == 'sell'}
        admitted_buys = {intent.ts_code for intent in plan.admitted
                         if intent.side == 'buy'}

        # 5) 组装清单
        new_state: dict[str, HoldingState] = {}
        for code, holding in state.items():
            row = by_code.get(code)
            decision = decisions[code]
            if code in exits and code not in admitted_sells:
                # 换手上限延后卖出：仍保留持仓
                advice.rows.append(self._hold_row(
                    row, holding, decision, meta, tier, equity,
                    stats, note='卖出被换手上限延后'))
                new_state[code] = HoldingState(
                    ts_code=code, shares=holding.shares,
                    entry_date=holding.entry_date, entry_price=holding.entry_price,
                    smoothed_percentile=decision.smoothed_percentile,
                    sell_confirm=decision.sell_confirm)
            elif code in exits:
                advice.rows.append(self._sell_row(
                    row, holding, decision, meta, tier, stats))
            else:
                advice.rows.append(self._hold_row(
                    row, holding, decision, meta, tier, equity, stats))
                new_state[code] = HoldingState(
                    ts_code=code, shares=holding.shares,
                    entry_date=holding.entry_date, entry_price=holding.entry_price,
                    smoothed_percentile=decision.smoothed_percentile,
                    sell_confirm=decision.sell_confirm)

        for code, item in alloc_by_code.items():
            if code not in admitted_buys:
                continue
            row, candidate, eligibility = next(
                (entry for entry in candidates if entry[1].ts_code == code), (None, None, None))
            if row is None:
                continue
            ref_price = float(row.get('raw_close') or item.price)
            advice.rows.append(self._buy_row(
                row, item, eligibility, meta, tier, ref_price, stats))
            new_state[code] = HoldingState(
                ts_code=code, shares=item.shares, entry_date=trade_date,
                entry_price=ref_price,
                smoothed_percentile=float(row.get('score_percentile') or 0.0),
                sell_confirm=0)

        for row, eligibility in avoid_rows:
            advice.rows.append(self._avoid_row(row, eligibility, meta, tier, stats))

        advice.invested = sum(rec.target_weight for rec in advice.rows) * equity
        advice.cash = equity - advice.invested
        # gross_notional 为买卖双边合计；组合换手按行业惯例取单边口径
        advice.turnover = (plan.gross_notional / (2 * equity)) if equity > 0 else 0.0
        advice.excluded = max(0, len(ranked) - len(candidates) - len(avoid_rows))
        if plan.deferred:
            advice.notes.append(
                f"换手上限 {self.config.turnover.max_turnover_per_rebalance:.0%} "
                f"延后 {len(plan.deferred)} 笔调仓")
        return advice, new_state

    # ------------------------------------------------------------------
    # 行构造
    # ------------------------------------------------------------------
    def _common(self, row: dict, meta: dict[str, Any], tier: Any,
                stats: dict | None, direction: str, equity: float) -> dict[str, Any]:
        code = str(row['ts_code'])
        stock_meta = meta.get(code)
        percentile = float(row.get('score_percentile') or 0.0)
        score = float(row.get('score') or 0.0)
        return {
            'trade_date': row['date'],
            'ts_code': code,
            'name': stock_meta.name if stock_meta else code,
            'name_source': stock_meta.name_source if stock_meta else 'code',
            'direction': direction,
            'ref_price': float(row.get('raw_close') or 0.0),
            'horizon': tier.label,
            'validity_days': tier.validity_days,
            'confidence': confidence_from_score(score, percentile),
            'top_factors': explain_factors(
                row, stats, tier, prefer_negative=direction in (DIRECTION_SELL, DIRECTION_AVOID)),
            'industry': stock_meta.industry if stock_meta else '未知',
            'industry_source': (stock_meta.industry_source
                                if stock_meta else INDUSTRY_SOURCE_BOARD),
            'score': score,
            'score_percentile': percentile,
        }

    def _risk_notes(self, eligibility: Any, industry_source: str,
                    extra: str = '') -> list[str]:
        notes = list(eligibility.notes if eligibility else [])
        if industry_source == INDUSTRY_SOURCE_BOARD:
            notes.append('行业为代码前缀/板块近似，非权威分类')
        notes.append('参考价为推荐日收盘价，次日实际成交可能跳空')
        if extra:
            notes.append(extra)
        return notes

    def _buy_row(self, row: dict, allocation: Allocation, eligibility: Any,
                 meta: dict[str, Any], tier: Any, ref_price: float,
                 stats: dict | None) -> Recommendation:
        common = self._common(row, meta, tier, stats, DIRECTION_BUY,
                              allocation.target_weight)
        common['target_weight'] = allocation.target_weight
        common['shares'] = allocation.shares
        common['ref_price'] = ref_price
        common['risk_notes'] = self._risk_notes(
            eligibility, common['industry_source'],
            f'持有期{tier.min_hold_days}-{tier.validity_days}个交易日')
        return Recommendation(**common)

    def _sell_row(self, row: dict, holding: HoldingState, decision: HoldingDecision,
                  meta: dict[str, Any], tier: Any,
                  stats: dict | None) -> Recommendation:
        common = self._common(row, meta, tier, stats, DIRECTION_SELL, 0.0)
        common['target_weight'] = 0.0
        common['shares'] = holding.shares
        common['risk_notes'] = [
            '卖出后不再持有该标的',
            decision.reason,
            '若次日跌停可能无法按参考价卖出',
        ]
        common['validity_days'] = max(1, tier.validity_days - decision.days_held)
        return Recommendation(**common)

    def _hold_row(self, row: dict | None, holding: HoldingState,
                  decision: HoldingDecision, meta: dict[str, Any], tier: Any,
                  equity: float, stats: dict | None,
                  note: str = '') -> Recommendation:
        if row is None:
            row = {
                'ts_code': holding.ts_code, 'date': holding.entry_date,
                'score': 0.0, 'score_percentile': 0.0,
                'raw_close': holding.entry_price,
            }
        weight = (holding.shares * float(row.get('raw_close') or 0.0) / equity
                  if equity > 0 else 0.0)
        common = self._common(row, meta, tier, stats, DIRECTION_HOLD, weight)
        common['target_weight'] = weight
        common['shares'] = holding.shares
        common['validity_days'] = max(1, tier.validity_days - decision.days_held)
        common['risk_notes'] = [
            decision.reason,
            f'已持有{decision.days_held}个交易日',
        ]
        if note:
            common['risk_notes'].append(note)
        return Recommendation(**common)

    def _avoid_row(self, row: dict, eligibility: Any, meta: dict[str, Any],
                   tier: Any, stats: dict | None) -> Recommendation:
        common = self._common(row, meta, tier, stats, DIRECTION_AVOID, 0.0)
        common['target_weight'] = 0.0
        common['shares'] = 0
        notes = list(eligibility.notes)
        if eligibility.blocked_reason:
            notes.insert(0, f'剔除原因：{eligibility.blocked_reason}')
        notes.append('仅作风险观察，不建议建仓')
        common['risk_notes'] = notes
        return Recommendation(**common)


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


class _StoreConfig:
    """最小配置适配器：``LocalPanelLoader`` 只依赖 ``data.local_store_path``。"""

    def __init__(self, root: Path) -> None:
        self.data = _StoreData(root)


class _StoreData:
    def __init__(self, root: Path) -> None:
        self.local_store_path = str(root)
        self.local_store_enabled = True
        self.allow_remote_sources = False
        self.remote_database_fallback = False
