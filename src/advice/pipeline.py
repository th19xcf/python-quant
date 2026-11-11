#!/usr/bin/env python3

"""收盘后推荐闭环编排：本地行情 → 因子 → 打分 → 换手控制 → 仓位 → 清单。"""

from __future__ import annotations

import json
import math
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from loguru import logger

from src.advice.config import (
    TIER_PRESETS,
    AdviceConfig,
    default_config,
    default_output_tiers,
)
from src.advice.daily_store import (
    default_daily_dir,
    next_trading_day,
    resolve_exec_status,
    save_daily_snapshot,
)
from src.advice.data import LocalPanelLoader, panel_trading_dates
from src.advice.eligibility import EligibilityResult, check_eligibility
from src.advice.factor_panel import compute_factor_panel
from src.advice.ic_monitor import ic_gate_decision, monitor_note, run_ic_monitor
from src.advice.models import (
    DIRECTION_AVOID,
    DIRECTION_BUY,
    DIRECTION_HOLD,
    DIRECTION_SELL,
    AdviceResult,
    HoldingState,
    Recommendation,
    TierAdvice,
)
from src.advice.portfolio import Allocation, Candidate, allocate
from src.advice.risk_filters import RiskContext
from src.advice.scoring import (
    attach_score,
    confidence_from_score,
    explain_factors,
    factor_stats,
)
from src.advice.turnover import (
    HoldingDecision,
    TradeIntent,
    apply_turnover_cap,
    count_trading_days,
    entry_allowed,
    update_holding,
)
from src.advice.universe import (
    INDUSTRY_SOURCE_BOARD,
    build_meta,
    list_local_stock_codes,
    load_name_map,
)
from src.utils.config import get_config
from src.utils.dates import as_date


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


#: 同码冲突时的方向优先级（数值越大越优先）
DIRECTION_PRIORITY = {
    DIRECTION_BUY: 4,
    DIRECTION_SELL: 3,
    DIRECTION_HOLD: 2,
    DIRECTION_AVOID: 1,
}


def merge_duplicate_rows(
        rows: list[Recommendation]) -> tuple[list[Recommendation], list[dict[str, Any]]]:
    """按 **(档位, 代码)** 合并重复行，冲突时保留优先级更高的方向。

    合并键里的“档位”由调用方保证（``_run_tier`` 一次只处理一个档位）。
    优先级写死为 ``BUY > SELL > HOLD > AVOID``：持仓/卖出语义优先于
    观察性 AVOID，避免同一标的在清单里同时出现「持有」和「AVOID」。
    """
    kept: dict[str, Recommendation] = {}
    order: list[str] = []
    merges: list[dict[str, Any]] = []
    for rec in rows:
        code = rec.ts_code
        prev = kept.get(code)
        if prev is None:
            kept[code] = rec
            order.append(code)
            continue
        prev_rank = DIRECTION_PRIORITY.get(prev.direction, 0)
        rank = DIRECTION_PRIORITY.get(rec.direction, 0)
        if rank > prev_rank:
            merges.append({
                'ts_code': code,
                'kept': rec.direction,
                'dropped': prev.direction,
                'reason': '同码冲突，按 BUY>SELL>HOLD>AVOID 保留高优先级',
            })
            kept[code] = rec
        else:
            merges.append({
                'ts_code': code,
                'kept': prev.direction,
                'dropped': rec.direction,
                'reason': '同码冲突，按 BUY>SELL>HOLD>AVOID 保留高优先级',
            })
    return [kept[code] for code in order], merges


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
                 codes: Iterable[str] | None = None,
                 daily_dir: str | Path | None = None,
                 persist_daily: bool = False,
                 generated_at: datetime | None = None) -> AdviceResult:
        """生成指定日期（默认最新交易日）的三档推荐清单。

        ``persist_daily=True`` 时把结果另存为按日归档（``daily_dir``，
        默认 ``reports/advice/daily/{YYYYMMDD}.json``）；归档与 ``state`` 完全解耦。
        ``generated_at`` 仅用于执行时点判定（测试可注入固定时刻），默认当前时间。
        """
        gen_at = generated_at or datetime.now()
        tier_keys = (list(tiers) if tiers
                     else default_output_tiers(self.config.tiers))
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

        base_columns = {
            spec.column for preset in TIER_PRESETS.values() for spec in preset.factors
        }
        extended = any(
            spec.column not in base_columns
            for preset in selected for spec in preset.factors
        )
        factors = compute_factor_panel(panel, extended=extended)
        trade_date = _resolve_as_of(self.config, factors['date'].unique().to_list(),
                                    as_of_date)
        meta = build_meta(factors['ts_code'].unique().to_list(),
                          self.name_map, self.industry_map)
        trading_dates = panel_trading_dates(factors)
        risk = RiskContext(self.config.risk, self.store_root, trading_dates)
        meta = risk.enrich_meta(meta, trade_date)
        state_path = Path(state_dir) if state_dir else (
            Path(__file__).resolve().parents[2] / 'reports' / 'advice' / 'state')

        logger.info(
            f"[advice] 生成推荐: trade_date={trade_date}, equity={equity:,.0f}, "
            f"标的={report.loaded}, 档位={tier_keys}")

        result = AdviceResult(trade_date=trade_date, equity=float(equity))
        earliest_exec, exec_basis = next_trading_day(trade_date, trading_dates)
        result.signal_date = trade_date
        result.earliest_exec_date = earliest_exec
        result.exec_date_basis = exec_basis
        result.exec_status = resolve_exec_status(earliest_exec, gen_at)
        soft_enabled = extended and self.config.soft_filter.enabled
        for tier in selected:
            scored = attach_score(
                factors, tier, min_history_rows=self.config.min_history_rows)
            if soft_enabled:
                from src.advice.soft_filters import apply_soft_filter

                scored = apply_soft_filter(scored, self.config.soft_filter)
            # IC 门禁必须在出单前判定：failed=true 时本期不出新建仓
            monitor = run_ic_monitor(scored, tier.key, trade_date=trade_date)
            blocked, gate_reason = ic_gate_decision(monitor)
            state = self.load_state(state_path, tier.key)
            advice, new_state = self._run_tier(
                scored, trade_date, meta, tier, float(equity), state,
                trading_dates, report.summary(), risk, earliest_exec,
                block_new_buys=blocked, gate_reason=gate_reason)
            advice.notes.append(monitor_note(monitor))
            result.tiers[tier.key] = advice
            if persist_state:
                self.save_state(state_path, tier.key, trade_date,
                                float(equity), new_state)
        if persist_daily:
            save_daily_snapshot(result, daily_dir or default_daily_dir(),
                                generated_at=gen_at,
                                earliest_exec_date=earliest_exec,
                                exec_date_basis=exec_basis)
        return result

    def _run_tier(self, scored: pl.DataFrame, trade_date: date,  # noqa: C901
                  meta: dict[str, Any], tier: Any, equity: float,
                  state: dict[str, HoldingState], trading_dates: list[date],
                  coverage: dict[str, Any],
                  risk: RiskContext,
                  earliest_exec: date | None = None,
                  block_new_buys: bool = False,
                  gate_reason: str = '',
                  ) -> tuple[TierAdvice, dict[str, HoldingState]]:
        latest = scored.filter(pl.col('date') == trade_date)
        if latest.is_empty():
            return TierAdvice(tier=tier.key, label=tier.label,
                              trade_date=trade_date, equity=equity), state
        rows = latest.to_dicts()
        count_start = dict(risk.counts)
        audit_start = len(risk.audit)
        stats_frame = factor_stats(scored, tier).filter(
            pl.col('date') == trade_date)
        stats = stats_frame.to_dicts()[0] if not stats_frame.is_empty() else None
        by_code = {str(item['ts_code']): item for item in rows}

        advice = TierAdvice(
            tier=tier.key, label=tier.label, trade_date=trade_date,
            equity=equity, candidates=len(rows),
            notes=[
                f"本地面板覆盖 {coverage.get('loaded', 0)}/"
                f"{coverage.get('requested', 0)} 标的",
            ] + ([tier.note] if tier.note else []))

        # 可买只数漏斗：逐级计数写进 filter_stats，供「每日可买只数」时间序列核对
        funnel: dict[str, int] = {
            'funnel_panel_rows': len(rows),
            'funnel_soft_pass': len(rows),
            'funnel_score_pass': 0,
            'funnel_candidates': 0,
            'funnel_buys': 0,
            'funnel_holds': 0,
            'funnel_sells': 0,
            'funnel_avoid': 0,
        }

        # 1) 候选池：软筛选 + 分数达标 + 可交易 + 成本门槛
        #    IC 门禁触发时清空候选池：本期不出新建仓，只保留持仓的持有/卖出管理
        candidates: list[tuple[dict, Candidate, Any]] = []
        avoid_rows: list[tuple[dict, Any]] = []
        if block_new_buys:
            advice.notes.append(gate_reason)
        if 'soft_pass' in scored.columns:
            latest = scored.filter(pl.col('date') == trade_date)
            passed = latest.filter(pl.col('soft_pass')).height
            funnel['funnel_soft_pass'] = passed
            advice.notes.append(f"软筛选通过 {passed}/{latest.height}（仅 extended）")
        ranked = sorted(rows, key=lambda entry: entry.get('score_percentile') or 0.0,
                        reverse=True)
        funnel['funnel_score_pass'] = sum(
            1 for entry in rows
            if float(entry.get('score_percentile') or 0.0) >= tier.buy_percentile)
        loop_entries = [] if block_new_buys else ranked
        for entry in loop_entries:
            code = str(entry['ts_code'])
            if entry.get('soft_pass') is False:
                if len(avoid_rows) < self.config.max_avoid_rows:
                    reason = str(entry.get('soft_reason') or '软筛选未通过')
                    avoid_rows.append((entry, EligibilityResult(
                        False, [f'软筛选:{reason}'], f'软筛选不通过：{reason}')))
                continue
            if code in state:
                continue
            percentile = float(entry.get('score_percentile') or 0.0)
            if percentile >= tier.buy_percentile:
                verdict = risk.evaluate(code, trade_date)
                st_status = risk.st_status(code, trade_date)
                eligibility = check_eligibility(
                    entry, tier, self.config, 'buy', st_status=st_status)
                if not eligibility.ok and len(avoid_rows) < self.config.max_avoid_rows:
                    avoid_rows.append((entry, eligibility))
                    continue
                if not verdict.ok:
                    if len(avoid_rows) < self.config.max_avoid_rows:
                        avoid_rows.append((entry, EligibilityResult(
                            False, [verdict.reason], verdict.reason)))
                    continue
                if eligibility.ok:
                    ok_edge, reason = entry_allowed(percentile, tier, self.config)
                    if ok_edge:
                        stock_meta = meta.get(code)
                        candidates.append((entry, Candidate(
                            ts_code=code,
                            score_percentile=percentile,
                            price=float(entry.get('raw_close') or 0.0),
                            industry=stock_meta.industry if stock_meta else '未知',
                            industry_source=(stock_meta.industry_source
                                             if stock_meta else INDUSTRY_SOURCE_BOARD),
                            adv_amount=float(entry.get('amount_ma20') or 0.0),
                        ), eligibility))
                    elif len(avoid_rows) < self.config.max_avoid_rows:
                        avoid_rows.append((entry, eligibility))
            elif percentile >= tier.buy_percentile - 0.05:
                eligibility = check_eligibility(
                    entry, tier, self.config, 'buy',
                    st_status=risk.st_status(code, trade_date))
                if not eligibility.ok and len(avoid_rows) < self.config.max_avoid_rows:
                    avoid_rows.append((entry, eligibility))

        best_pct = candidates[0][1].score_percentile if candidates else None
        funnel['funnel_candidates'] = len(candidates)

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
            if row is None:
                new_state[code] = holding
                continue
            decision = decisions[code]
            if code in exits and code not in admitted_sells:
                # 换手上限延后卖出：仍保留持仓，但归档里显式标 deferred
                row_out = self._hold_row(
                    row, holding, decision, meta, tier, equity,
                    stats, note='卖出被换手上限延后')
                row_out.disposition = 'deferred'
                row_out.defer_reason = '卖出被换手上限（单边 20%）延后'
                row_out.defer_until = earliest_exec
                advice.rows.append(row_out)
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
            match = next(
                (entry for entry in candidates if entry[1].ts_code == code), None)
            if match is None:
                continue
            row, candidate, eligibility = match
            ref_price = float(row.get('raw_close') or item.price)
            advice.rows.append(self._buy_row(
                row, item, eligibility, meta, tier, ref_price, stats))
            new_state[code] = HoldingState(
                ts_code=code, shares=item.shares, entry_date=trade_date,
                entry_price=ref_price,
                smoothed_percentile=float(row.get('score_percentile') or 0.0),
                sell_confirm=0)

        # 5b) 被换手上限延后的买入也必须写入归档（disposition=deferred），
        #     否则归档与当日实际建议集合不一致。
        for intent in plan.deferred:
            if intent.side != 'buy':
                continue
            deferred_item = alloc_by_code.get(intent.ts_code)
            if deferred_item is None:
                continue
            match = next(
                (entry for entry in candidates if entry[1].ts_code == intent.ts_code),
                None)
            if match is None:
                continue
            row, candidate, eligibility = match
            ref_price = float(row.get('raw_close') or deferred_item.price)
            rec = self._buy_row(
                row, deferred_item, eligibility, meta, tier, ref_price, stats)
            rec.disposition = 'deferred'
            rec.defer_reason = '单次换手上限（单边 20%）预算不足'
            rec.defer_until = earliest_exec
            rec.risk_notes.append(
                f'本次未成交：{rec.defer_reason}；预计 {earliest_exec} 可再排')
            advice.rows.append(rec)

        for row, eligibility in avoid_rows:
            advice.rows.append(self._avoid_row(row, eligibility, meta, tier, stats))

        # 5c) 同日同档按 (档位, 代码) 合并；冲突优先级 BUY > SELL > HOLD > AVOID
        advice.rows, advice.row_merge = merge_duplicate_rows(advice.rows)
        if advice.row_merge:
            advice.notes.append(
                f'行合并：按 (档位,代码) 去掉 {len(advice.row_merge)} 条重复行'
                f'（优先级 BUY>SELL>HOLD>AVOID）')

        advice.invested = sum(
            rec.target_weight for rec in advice.rows
            if rec.disposition != 'deferred') * equity
        advice.cash = equity - advice.invested
        # gross_notional 为买卖双边合计；组合换手按行业惯例取单边口径
        advice.turnover = (plan.gross_notional / (2 * equity)) if equity > 0 else 0.0
        advice.excluded = max(
            0, len(ranked) - len(candidates) - len(avoid_rows))
        if plan.deferred:
            advice.notes.append(
                f"换手上限 {self.config.turnover.max_turnover_per_rebalance:.0%} "
                f"延后 {len(plan.deferred)} 笔调仓（已写入归档并标 deferred）")
        stats_local = {key: risk.counts[key] - count_start.get(key, 0)
                       for key in risk.counts}
        stats_local['row_merge_dropped'] = len(advice.row_merge)
        stats_local['ic_gate_blocked'] = 1 if block_new_buys else 0
        for direction, key in ((DIRECTION_BUY, 'funnel_buys'),
                               (DIRECTION_HOLD, 'funnel_holds'),
                               (DIRECTION_SELL, 'funnel_sells'),
                               (DIRECTION_AVOID, 'funnel_avoid')):
            funnel[key] = sum(1 for rec in advice.rows if rec.direction == direction)
        stats_local.update(funnel)
        advice.filter_stats = stats_local
        advice.filter_audit = list(risk.audit[audit_start:])
        advice.notes.extend(
            risk.notes(self.config.constraints.max_industry_weight,
                       counts=stats_local))
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
        extra = f'持有期{tier.min_hold_days}-{tier.validity_days}个交易日'
        if tier.note:
            extra = f'{extra}；{tier.note}'
        common['risk_notes'] = self._risk_notes(
            eligibility, common['industry_source'], extra)
        # T 日涨停不拦截，只把「次日可能买不进」随条目带出（成交由撮合层判定）
        common['limit_up_risk'] = bool(getattr(eligibility, 'limit_up_risk', False))
        common['limit_up_basis'] = getattr(eligibility, 'limit_up_basis', '')
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
    """统一日期归一；原实现会把 datetime 当 date 原样返回（类型不一致）。"""
    return as_date(value)


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
