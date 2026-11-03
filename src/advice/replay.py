#!/usr/bin/env python3

"""历史回放：验证换手控制与成本后收益。

回放口径：
- 推荐在交易日 T 收盘后生成，T+1 开盘执行（人工可执行的最早时点）；
- 涨跌停/停牌用 ``ExecutionRules`` 判定，不可成交的订单顺延放弃并计数；
- 成本 = 双边佣金 + 卖出印花税 + 双边冲击成本（冲击直接计入成交价）；
- 组合估值使用后复权收盘价，与因子口径一致。
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from loguru import logger

from src.advice.config import AdviceConfig, default_config
from src.advice.data import LocalPanelLoader, panel_trading_dates
from src.advice.eligibility import check_eligibility
from src.advice.factor_panel import compute_factor_panel
from src.advice.pipeline import AdviceEngine
from src.advice.portfolio import Candidate, allocate
from src.advice.scoring import (
    attach_score,
    confidence_from_score,
    explain_factors,
    factor_stats,
)
from src.advice.turnover import (
    TradeIntent,
    apply_turnover_cap,
    count_trading_days,
    entry_allowed,
    update_holding,
)
from src.advice.universe import build_meta, list_local_stock_codes, load_name_map
from src.backtest.engine.execution_core import ExecutionRules
from src.utils.config import get_config


@dataclass
class ReplayPosition:
    """回放中的持仓。"""

    ts_code: str
    shares: int
    entry_idx: int
    entry_date: date
    entry_price: float
    last_price: float
    smoothed: float = 0.5
    confirm: int = 0


@dataclass
class ClosedTrade:
    """已平仓交易记录。"""

    ts_code: str
    entry_date: date
    exit_date: date
    hold_days: int
    entry_price: float
    exit_price: float
    pnl: float


@dataclass
class ReplayResult:
    """单档回放结果。"""

    tier: str
    label: str
    start: date
    end: date
    initial_equity: float
    final_equity: float
    gross_return: float
    net_return: float
    annualized_return: float
    max_drawdown: float
    total_cost: float
    buy_count: int
    sell_count: int
    avg_holding_days: float
    annual_turnover: float
    untradable_skipped: int
    equity_curve: list[tuple[str, float]] = field(default_factory=list)
    sample_rows: list[dict[str, Any]] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        return {
            'tier': self.tier,
            'label': self.label,
            'start': self.start.isoformat(),
            'end': self.end.isoformat(),
            'initial_equity': round(self.initial_equity, 2),
            'final_equity': round(self.final_equity, 2),
            'net_return': round(self.net_return, 4),
            'gross_return': round(self.gross_return, 4),
            'annualized_return': round(self.annualized_return, 4),
            'max_drawdown': round(self.max_drawdown, 4),
            'total_cost': round(self.total_cost, 2),
            'buy_count': self.buy_count,
            'sell_count': self.sell_count,
            'avg_holding_days': round(self.avg_holding_days, 1),
            'annual_turnover': round(self.annual_turnover, 4),
            'untradable_skipped': self.untradable_skipped,
            'sample_rows': self.sample_rows[:8],
        }


class _ReplayTier:
    """单档位回放器。"""

    def __init__(self, engine: AdviceEngine, tier: Any, scored: pl.DataFrame,
                 dates: list[date], meta: dict[str, Any], equity: float) -> None:
        self.engine = engine
        self.config: AdviceConfig = engine.config
        self.tier = tier
        self.scored = scored
        self.dates = dates
        self.meta = meta
        self.cash = equity
        self.initial = equity
        self.positions: dict[str, ReplayPosition] = {}
        self.closed: list[ClosedTrade] = []
        self.equity_curve: list[tuple[str, float]] = []
        self.market_value = 0.0
        self.total_cost = 0.0
        self.bought = 0.0
        self.sold = 0.0
        self.buy_count = 0
        self.sell_count = 0
        self.skipped = 0
        self.last_prices: dict[str, float] = {}
        self.last_date = self.dates[-1] if self.dates else date.today()

    def run(self) -> ReplayResult:
        rules = ExecutionRules()
        for index in range(len(self.dates) - 1):
            today = self._rows(self.dates[index])
            tomorrow = self._rows(self.dates[index + 1])
            self._step(index, today, tomorrow, rules)
            self._mark(self.dates[index + 1], tomorrow)
        final = self.cash + self.market_value
        years = max(1e-9, len(self.dates) / 252.0)
        return self._result(final, years)

    def _rows(self, day: date) -> dict[str, dict]:
        frame = self.scored.filter(pl.col('date') == day)
        return {str(row['ts_code']): row for row in frame.to_dicts()}

    def _step(self, index: int, today: dict[str, dict], tomorrow: dict[str, dict],
              rules: ExecutionRules) -> None:
        best_pct = self._best_candidate(today)
        exit_codes = self._decide_exits(index, today, best_pct)
        buy_candidates = self._buy_candidates(today)
        allocations = self._allocate(buy_candidates)

        sells = [TradeIntent(code, 'sell',
                             self.positions[code].shares
                             * float(today.get(code, {}).get('raw_close')
                                     or self.positions[code].last_price),
                             self.positions[code].smoothed)
                 for code in exit_codes if code in self.positions]
        buys = [TradeIntent(item.ts_code, 'buy',
                            item.target_weight * (self.cash + self.market_value),
                            item.target_weight)
                for item in allocations]
        plan = apply_turnover_cap(
            sells + buys, self.cash + self.market_value,
            self.config.turnover.max_turnover_per_rebalance)
        admitted_sells = {item.ts_code for item in plan.admitted if item.side == 'sell'}
        admitted_buys = {item.ts_code for item in plan.admitted if item.side == 'buy'}

        for code in admitted_sells:
            self._execute_sell(code, tomorrow.get(code), index, rules)
        for item in allocations:
            if item.ts_code in admitted_buys:
                self._execute_buy(item, tomorrow.get(item.ts_code), index + 1,
                                  self.dates[index + 1], rules)

    def _best_candidate(self, rows: dict[str, dict]) -> float | None:
        values = [
            float(row.get('score_percentile') or 0.0)
            for code, row in rows.items()
            if code not in self.positions
            and float(row.get('score_percentile') or 0.0) >= self.tier.buy_percentile
        ]
        return max(values) if values else None

    def _decide_exits(self, index: int, rows: dict[str, dict],
                      best_pct: float | None) -> set[str]:
        exits: set[str] = set()
        for code, position in list(self.positions.items()):
            row = rows.get(code)
            percentile = float(row.get('score_percentile') or 0.0) if row else 0.0
            days = index - position.entry_idx
            decision = update_holding(
                prev_percentile=position.smoothed,
                percentile=percentile,
                sell_confirm=position.confirm,
                days_held=days,
                tier=self.tier,
                config=self.config,
                best_candidate_pct=best_pct,
                ts_code=code,
            )
            position.smoothed = decision.smoothed_percentile
            position.confirm = decision.sell_confirm
            if decision.action == 'SELL':
                exits.add(code)
        return exits

    def _buy_candidates(self, rows: dict[str, dict]) -> list[tuple[dict, Candidate]]:
        result: list[tuple[dict, Candidate]] = []
        for code, row in rows.items():
            if code in self.positions:
                continue
            percentile = float(row.get('score_percentile') or 0.0)
            if percentile < self.tier.buy_percentile:
                continue
            eligibility = check_eligibility(row, self.tier, self.config, 'buy')
            if not eligibility.ok:
                continue
            if not entry_allowed(percentile, self.tier, self.config)[0]:
                continue
            stock_meta = self.meta.get(code)
            result.append((row, Candidate(
                ts_code=code, score_percentile=percentile,
                price=float(row.get('raw_close') or 0.0),
                industry=stock_meta.industry if stock_meta else '未知',
                industry_source=(stock_meta.industry_source
                                 if stock_meta else 'code_prefix_board'),
            )))
        result.sort(key=lambda item: item[1].score_percentile, reverse=True)
        return result

    def _allocate(self, candidates: list[tuple[dict, Candidate]]) -> list[Any]:
        slots = max(0, self.tier.max_positions - len(self.positions))
        if slots <= 0 or not candidates:
            return []
        equity = self.cash + self.market_value
        available = max(0.0, equity * (1.0 - self.config.constraints.cash_buffer)
                        - self.market_value)
        constraints = self.config.constraints
        from dataclasses import replace
        allocations = allocate([item[1] for item in candidates], equity,
                               replace(constraints, cash_buffer=0.0),
                               max_positions=slots)
        total = sum(item.target_weight for item in allocations) * equity
        if total > available and total > 0:
            scale = available / total
            for item in allocations:
                item.target_weight *= scale
                item.shares = int(math.floor(
                    item.target_weight * equity / item.price
                    / self.config.constraints.lot_size)
                    * self.config.constraints.lot_size)
        return [item for item in allocations if item.shares > 0]

    def _execute_sell(self, code: str, row: dict | None, index: int,
                      rules: ExecutionRules) -> None:
        if row is None:
            self.skipped += 1
            return
        if not rules.is_tradable(
                'sell', prev_close=self._finite(row.get('raw_prev_close')),
                open_price=self._finite(row.get('raw_open')),
                high=self._finite(row.get('raw_high')),
                low=self._finite(row.get('raw_low')),
                close=self._finite(row.get('raw_close')),
                volume=self._finite(row.get('raw_volume')),
                ts_code=code, is_st=None, trade_date=row.get('date')):
            self.skipped += 1
            return
        position = self.positions.pop(code)
        price = float(row['open']) * (1.0 - self.config.costs.impact_rate)
        notional = position.shares * price
        commission = self.config.costs.commission(notional)
        stamp = notional * self.config.costs.stamp_duty_rate
        self.cash += notional - commission - stamp
        self.total_cost += commission + stamp
        self.sold += notional
        self.sell_count += 1
        self.closed.append(ClosedTrade(
            ts_code=code, entry_date=position.entry_date,
            exit_date=row['date'], hold_days=index - position.entry_idx,
            entry_price=position.entry_price, exit_price=price,
            pnl=(price - position.entry_price) * position.shares,
        ))

    def _execute_buy(self, allocation: Any, row: dict | None, index: int,
                     trade_date: date, rules: ExecutionRules) -> None:
        if row is None or allocation.shares <= 0:
            self.skipped += 1
            return
        if not rules.is_tradable(
                'buy', prev_close=self._finite(row.get('raw_prev_close')),
                open_price=self._finite(row.get('raw_open')),
                high=self._finite(row.get('raw_high')),
                low=self._finite(row.get('raw_low')),
                close=self._finite(row.get('raw_close')),
                volume=self._finite(row.get('raw_volume')),
                ts_code=allocation.ts_code, is_st=None, trade_date=row.get('date')):
            self.skipped += 1
            return
        price = float(row['open']) * (1.0 + self.config.costs.impact_rate)
        lot = self.config.constraints.lot_size
        shares = int(math.floor(
            allocation.target_weight * (self.cash + self.market_value) / price / lot) * lot)
        if shares <= 0:
            return
        notional = shares * price
        commission = self.config.costs.commission(notional)
        if notional + commission > self.cash:
            shares = int(math.floor(
                (self.cash - commission) / price / lot) * lot)
            if shares <= 0:
                return
            notional = shares * price
            commission = self.config.costs.commission(notional)
        self.cash -= notional + commission
        self.total_cost += commission
        self.bought += notional
        self.buy_count += 1
        self.positions[allocation.ts_code] = ReplayPosition(
            ts_code=allocation.ts_code, shares=shares, entry_idx=index,
            entry_date=trade_date, entry_price=price, last_price=price,
            smoothed=float(row.get('score_percentile') or 0.5), confirm=0)

    def _mark(self, day: date, rows: dict[str, dict]) -> None:
        self.market_value = 0.0
        for code, position in self.positions.items():
            row = rows.get(code)
            if row and row.get('close'):
                position.last_price = float(row['close'])
            self.market_value += position.shares * position.last_price
        self.last_prices = {
            code: position.last_price for code, position in self.positions.items()}
        self.last_date = day
        self.equity_curve.append((day.isoformat(), self.cash + self.market_value))

    def _result(self, final: float, years: float) -> ReplayResult:
        peak = -1e18
        max_drawdown = 0.0
        for _, value in self.equity_curve:
            peak = max(peak, value)
            if peak > 0:
                max_drawdown = max(max_drawdown, (peak - value) / peak)
        net_return = final / self.initial - 1.0
        gross_return = (final + self.total_cost) / self.initial - 1.0
        annualized = ((final / self.initial) ** (1.0 / years) - 1.0
                      if final > 0 else -1.0)
        avg_days = (sum(trade.hold_days for trade in self.closed) / len(self.closed)
                    if self.closed else 0.0)
        average_equity = (
            sum(value for _, value in self.equity_curve) / len(self.equity_curve)
            if self.equity_curve else self.initial)
        annual_turnover = ((self.bought + self.sold) / 2.0
                           / max(average_equity, 1e-9) / years)
        sample = self._sample_rows()
        return ReplayResult(
            tier=self.tier.key, label=self.tier.label, start=self.dates[0],
            end=self.dates[-1], initial_equity=self.initial, final_equity=final,
            gross_return=gross_return, net_return=net_return,
            annualized_return=annualized, max_drawdown=max_drawdown,
            total_cost=self.total_cost, buy_count=self.buy_count,
            sell_count=self.sell_count, avg_holding_days=avg_days,
            annual_turnover=annual_turnover, untradable_skipped=self.skipped,
            equity_curve=self.equity_curve, sample_rows=sample)

    def _sample_rows(self) -> list[dict[str, Any]]:
        """用最后交易日的决策生成与线上清单同 schema 的样例行。"""
        rows = self._rows(self.dates[-1])
        stats_frame = factor_stats(self.scored, self.tier).filter(
            pl.col('date') == self.dates[-1])
        stats = stats_frame.to_dicts()[0] if not stats_frame.is_empty() else None
        output: list[dict[str, Any]] = []
        for code, position in self.positions.items():
            row = rows.get(code)
            if row is None:
                continue
            output.append(self._sample_row(row, 'HOLD', position.shares,
                                            position.last_price, stats))
        for row, _candidate in self._buy_candidates(rows)[:5]:
            output.append(self._sample_row(row, 'BUY', 0,
                                           float(row.get('raw_close') or 0.0), stats))
        return output

    def _sample_row(self, row: dict, direction: str, shares: int,
                    price: float, stats: dict | None) -> dict[str, Any]:
        code = str(row['ts_code'])
        stock_meta = self.meta.get(code)
        percentile = float(row.get('score_percentile') or 0.0)
        return {
            'trade_date': self.dates[-1].isoformat(),
            'ts_code': code,
            'name': stock_meta.name if stock_meta else code,
            'direction': direction,
            'target_weight_pct': 0.0 if direction != 'HOLD' else round(
                shares * price / max(self.cash + self.market_value, 1.0) * 100, 2),
            'shares': shares,
            'ref_price': round(price, 3),
            'horizon': self.tier.label,
            'validity_days': self.tier.validity_days,
            'confidence': confidence_from_score(float(row.get('score') or 0.0),
                                                percentile),
            'top_factors': ' | '.join(
                f'{name} {value:+.2f}' for name, value in
                explain_factors(row, stats, self.tier)[:3]),
            'risk_notes': '样本行：回放最后一日决策，非线上实时清单',
        }

    @staticmethod
    def _finite(value: Any) -> float | None:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if math.isfinite(number) else None


def run_replay(config: AdviceConfig | None = None, store_root: Any = None,
               start: date | None = None, end: date | None = None,
               max_symbols: int = 300, initial_equity: float = 1_000_000.0,
               tiers: Iterable[str] | None = None,
               name_map: dict[str, str] | None = None) -> dict[str, ReplayResult]:
    """用最近 1-2 年本地数据跑历史回放，返回各档位统计。"""
    advice_config = config or default_config()
    if store_root is None:
        store_root = get_config().data.local_store_path
    root = Path(store_root)
    engine = AdviceEngine(config=advice_config, store_root=root,
                          name_map=name_map)
    tier_keys = list(tiers) if tiers else list(advice_config.tiers)
    selected = [advice_config.tier(key) for key in tier_keys]

    codes = list_local_stock_codes(_engine_store_config(root))
    if max_symbols and max_symbols > 0 and len(codes) > max_symbols:
        step = max(1, len(codes) // max_symbols)
        codes = codes[::step][:max_symbols]

    max_lookback = max(tier.lookback_days for tier in selected)
    loader = LocalPanelLoader(_engine_store_config(root),
                              min_rows=advice_config.min_history_rows)
    end_date = end or date.today()
    start_date = start or (end_date - timedelta(days=730))
    panel, report = loader.load(
        codes, loader.default_start(start_date, max_lookback), end_date)
    if panel.is_empty():
        logger.warning('[replay] 没有可用行情，返回空结果')
        return {}

    factors = compute_factor_panel(panel)
    all_dates = panel_trading_dates(factors)
    window = [item for item in all_dates if item >= start_date]
    meta = build_meta(factors['ts_code'].unique().to_list(),
                      name_map if name_map is not None else engine.name_map,
                      engine.industry_map)
    if len(window) < 2:
        logger.warning('[replay] 回放窗口不足 2 个交易日，返回空结果')
        return {}
    logger.info(
        f"[replay] 标的 {report.loaded} 只, 回放 {len(window)} 个交易日 "
        f"({window[0]} ~ {window[-1]})")

    results: dict[str, ReplayResult] = {}
    for tier in selected:
        scored = attach_score(factors, tier)
        runner = _ReplayTier(engine, tier, scored, window, meta, initial_equity)
        results[tier.key] = runner.run()
    return results


def _engine_store_config(root: Path) -> Any:
    from src.advice.pipeline import _StoreConfig
    return _StoreConfig(root)
