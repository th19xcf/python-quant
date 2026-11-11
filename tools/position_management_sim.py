"""金字塔加仓 / 网格仓位的组合级模拟器（只读调用回测内核，不修改 src）。

口径与 ``src.advice.factor_eval.simulate_fixed_hold`` 保持一致：
- t 日收盘出信号，t+1 开盘成交；
- 涨跌停 / 停牌不可成交则顺延；
- 成本唯一来源 ``src.backtest.costs.cost_model.CostModel``；
- 组合净值按当日收盘 mark-to-market。

本模块只负责模拟，不做指标汇总与报告。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

import polars as pl

from src.advice.factor_eval import _curve_stats


@dataclass(frozen=True)
class StagedConfig:
    """金字塔/分批建仓参数。``fracs`` 是相对单票目标权重的各档比例。"""

    name: str
    fracs: tuple[float, ...]
    target_weight: float
    add_trigger_pct: float = 0.10
    entry_stop_pct: float | None = 0.10
    trail_stop_pct: float | None = 0.10
    atr_mode: bool = False
    atr_stop: bool = False
    atr_add_mult: float = 1.5
    atr_entry_mult: float = 2.0
    atr_trail_mult: float = 1.5
    risk_budget: float = 0.02
    hold_days: int = 20
    top_k: int = 20
    cash_buffer: float = 0.05
    max_tranches: int = 4


@dataclass
class StagedResult:
    """模拟输出：净值、统计、成交明细与规则触达诊断。"""

    curve: pl.DataFrame
    stats: dict[str, Any]
    trades: list[dict[str, Any]]
    diagnostics: dict[str, Any]
    costs: dict[str, float] = field(default_factory=dict)


def build_atr_map(frame: pl.DataFrame, window: int = 20) -> dict[tuple[date, str], float]:
    """按 ts_code 计算 ATR20，返回 (date, code) → ATR 的映射。"""
    cols = {'date', 'ts_code', 'high', 'low', 'close'}
    if not cols.issubset(set(frame.columns)):
        return {}
    work = frame.select('date', 'ts_code', 'high', 'low', 'close').sort(
        ['ts_code', 'date'])
    prev_close = pl.col('close').shift(1).over('ts_code')
    true_range = pl.max_horizontal(
        (pl.col('high') - pl.col('low')).abs(),
        (pl.col('high') - prev_close).abs(),
        (pl.col('low') - prev_close).abs(),
    )
    work = work.with_columns(true_range.alias('_tr')).with_columns(
        pl.col('_tr').rolling_mean(window_size=window, min_samples=window)
        .over('ts_code').alias('_atr'))
    out: dict[tuple[date, str], float] = {}
    for row in work.select('date', 'ts_code', '_atr').drop_nulls().to_dicts():
        out[(row['date'], str(row['ts_code']))] = float(row['_atr'])
    return out


def _num(value: Any, default: float = 0.0) -> float:
    """Polars 标量（宽 Union）安全转 float。"""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _can_buy(row: dict[str, Any] | None, prev_row: dict[str, Any] | None,
             day: date, rules: Any) -> bool:
    if row is None or prev_row is None:
        return False
    if not row.get('raw_open') or row.get('volume', 0) <= 0:
        return False
    limit = rules.resolve_limit_pct(str(row['ts_code']), is_st=None, trade_date=day)
    move = abs(float(row['raw_open']) / float(prev_row['raw_close']) - 1.0)
    return move < limit - 0.004


def _can_sell(row: dict[str, Any] | None, prev_row: dict[str, Any] | None,
              day: date, rules: Any, code: str) -> bool:
    if row is None or prev_row is None:
        return False
    if not row.get('raw_open') or row.get('volume', 0) <= 0:
        return False
    limit = rules.resolve_limit_pct(code, is_st=None, trade_date=day)
    move = float(row['raw_open']) / float(prev_row['raw_close']) - 1.0
    return move > -limit + 0.004


def _exit_reason(pos: dict[str, Any], prev_row: dict[str, Any] | None,
                 cfg: StagedConfig, atr_map: dict[tuple[date, str], float],
                 prev: date) -> str | None:
    if pos['held'] >= cfg.hold_days:
        return 'time'
    if prev_row is None:
        return None
    close = float(prev_row['close'])
    if cfg.atr_stop or cfg.atr_mode:
        entry_atr = max(float(pos.get('atr_entry', 0.0)), 1e-9)
        stop_line = pos['entry_price'] - cfg.atr_entry_mult * entry_atr
        trail_atr = atr_map.get((prev, pos['ts_code']), entry_atr)
        trail_line = pos['high'] - cfg.atr_trail_mult * trail_atr
        if close <= stop_line:
            return 'stop_entry'
        if close <= trail_line:
            return 'stop_trail'
        return None
    if cfg.entry_stop_pct is not None and close <= pos['entry_price'] * (
            1.0 - cfg.entry_stop_pct):
        return 'stop_entry'
    if cfg.trail_stop_pct is not None and close <= pos['high'] * (
            1.0 - cfg.trail_stop_pct):
        return 'stop_trail'
    return None


def _add_triggered(pos: dict[str, Any], prev_row: dict[str, Any] | None,
                   cfg: StagedConfig, atr_map: dict[tuple[date, str], float],
                   prev: date) -> bool:
    if pos['tranches'] >= cfg.max_tranches:
        return False
    if prev_row is None:
        return False
    close = float(prev_row['close'])
    if cfg.atr_mode:
        atr = atr_map.get((prev, pos['ts_code']), 0.0)
        return close >= pos['last_add_price'] + cfg.atr_add_mult * atr
    return close >= pos['last_add_price'] * (1.0 + cfg.add_trigger_pct)


def simulate_staged(frame: pl.DataFrame, cfg: StagedConfig, *,  # noqa: C901
                    atr_map: dict[tuple[date, str], float] | None = None,
                    initial: float = 1_000_000.0) -> StagedResult:
    """组合级分批建仓模拟；``fracs`` 相对 ``target_weight`` 分档。

    日内状态机包含退出/加仓/建仓/估值四段独立分支，拆分后仍集中在同一闭包内
    以共享现金与持仓状态；圈复杂度以 noqa 标注，避免为拆而拆引入参数传递噪声。
    """
    from src.risk.execution_rules import ExecutionRules

    rules = ExecutionRules()
    from src.backtest.costs.cost_model import CostModel

    cost_model = CostModel()
    atr_map = atr_map or {}
    dates = sorted(frame['date'].unique().to_list())
    if len(dates) < 2:
        empty = pl.DataFrame({'date': [], 'equity': []})
        return StagedResult(empty, {}, [], {}, {})

    by_code_by_date = {
        day: {str(row['ts_code']): row
              for row in frame.filter(pl.col('date') == day).to_dicts()}
        for day in dates
    }
    cash = initial
    positions: list[dict[str, Any]] = []
    curve: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    buy_notional = 0.0
    total_cost = 0.0
    entries = 0
    add_events = 0
    stop_counts: dict[str, int] = {'stop_entry': 0, 'stop_trail': 0, 'time': 0}
    capped_entries = 0
    tranche_hist: dict[str, int] = {}

    def _do_exits(day: date, prev: date,
                  today: dict[str, dict[str, Any]],
                  prev_rows: dict[str, dict[str, Any]]) -> None:
        nonlocal cash, total_cost
        still: list[dict[str, Any]] = []
        for pos in positions:
            pos['held'] += 1
            prev_row = prev_rows.get(pos['ts_code'])
            reason = _exit_reason(pos, prev_row, cfg, atr_map, prev)
            if reason is None:
                still.append(pos)
                continue
            row = today.get(pos['ts_code'])
            if row is None or not _can_sell(row, prev_row, day, rules,
                                            pos['ts_code']):
                still.append(pos)
                continue
            price = float(row['open'])
            proceeds = pos['shares'] * price
            sell_cost = cost_model.calculate_sell_cost(
                pos['shares'], price, trade_date=day, ts_code=pos['ts_code'])
            cash += proceeds - sell_cost
            total_cost += sell_cost
            pnl = proceeds - sell_cost - pos['cost_basis']
            trades.append({
                'ts_code': pos['ts_code'], 'entry_date': pos['entry_date'],
                'exit_date': day, 'exit_reason': reason,
                'entry_price': pos['entry_price'], 'exit_price': price,
                'shares': pos['shares'], 'buy_notional': pos['buy_notional'],
                'cost_basis': pos['cost_basis'],
                'pnl': pnl, 'ret': pnl / max(pos['cost_basis'], 1e-9),
                'held_days': pos['held'], 'tranches': pos['tranches'],
            })
            stop_counts[reason] = stop_counts.get(reason, 0) + 1
        positions[:] = still

    def _do_adds(day: date, prev: date,
                 today: dict[str, dict[str, Any]],
                 prev_rows: dict[str, dict[str, Any]]) -> None:
        nonlocal cash, total_cost, buy_notional, add_events
        for pos in positions:
            if not _add_triggered(pos, prev_rows.get(pos['ts_code']), cfg,
                                  atr_map, prev):
                continue
            row = today.get(pos['ts_code'])
            prev_row = prev_rows.get(pos['ts_code'])
            if row is None or not _can_buy(row, prev_row, day, rules):
                continue
            frac = cfg.fracs[min(pos['tranches'], len(cfg.fracs) - 1)]
            budget = max(0.0, cash - initial * cfg.cash_buffer)
            cap = initial * cfg.target_weight - pos['buy_notional']
            size = (initial * cfg.target_weight / 3.0 if cfg.atr_mode
                    else initial * cfg.target_weight * frac)
            notional = min(budget, cap, size)
            if notional < 1000:
                continue
            price = float(row['open'])
            shares = int(notional / price / 100) * 100
            if shares <= 0:
                continue
            buy_cost = cost_model.calculate_buy_cost(
                shares, price, trade_date=day, ts_code=pos['ts_code'])
            cash -= shares * price + buy_cost
            total_cost += buy_cost
            buy_notional += shares * price
            pos['shares'] += shares
            pos['buy_notional'] += shares * price
            pos['cost_basis'] += shares * price + buy_cost
            pos['last_add_price'] = price
            pos['tranches'] += 1
            add_events += 1

    def _do_entries(day: date, prev: date,
                    today: dict[str, dict[str, Any]],
                    rows: list[dict[str, Any]]) -> None:
        nonlocal cash, total_cost, buy_notional, entries, capped_entries
        ranked = [
            row for row in rows
            if row.get('__score') is not None
            and not row.get('__short_history')
            and row.get('raw_close')
            and row.get('volume', 0) > 0
        ]
        ranked.sort(key=lambda r: float(r['__score']), reverse=True)
        slots = max(0, cfg.top_k - len(positions))
        for row in ranked[:slots]:
            code = str(row['ts_code'])
            if any(pos['ts_code'] == code for pos in positions):
                continue
            nrow = today.get(code)
            if nrow is None or not _can_buy(nrow, row, day, rules):
                continue
            price = float(nrow['open'])
            weight = cfg.target_weight
            if cfg.atr_mode or cfg.atr_stop:
                atr = atr_map.get((prev, code), 0.0)
                stop_pct = (cfg.atr_entry_mult * atr
                            / max(price, 1e-9)) if atr > 0 else 0.10
                weight = min(cfg.target_weight,
                             cfg.risk_budget / max(stop_pct, 1e-6))
                if weight >= cfg.target_weight - 1e-9:
                    capped_entries += 1
            budget = max(0.0, cash - initial * cfg.cash_buffer)
            notional = min(budget, initial * weight * cfg.fracs[0])
            if notional < 1000:
                continue
            shares = int(notional / price / 100) * 100
            if shares <= 0:
                continue
            buy_cost = cost_model.calculate_buy_cost(
                shares, price, trade_date=day, ts_code=code)
            cash -= shares * price + buy_cost
            total_cost += buy_cost
            buy_notional += shares * price
            entries += 1
            positions.append({
                'ts_code': code, 'shares': shares, 'price': price,
                'entry_price': price, 'last_add_price': price,
                'entry_date': day, 'held': 0, 'tranches': 1,
                'buy_notional': shares * price,
                'cost_basis': shares * price + buy_cost,
                'high': float(row['close']),
                'atr_entry': (atr_map.get((prev, code), 0.0)
                              if cfg.atr_mode or cfg.atr_stop else 0.0),
            })
            tranche_hist[code] = 1

    def _mark(day: date, today: dict[str, dict[str, Any]]) -> None:
        nonlocal cash
        mark = cash
        for pos in positions:
            row = today.get(pos['ts_code'])
            close = float(row['close']) if row else pos['price']
            pos['high'] = max(pos['high'], close)
            mark += pos['shares'] * close
            tranche_hist[pos['ts_code']] = max(
                tranche_hist.get(pos['ts_code'], 1), pos['tranches'])
        curve.append({'date': day, 'equity': mark})

    for index in range(1, len(dates)):
        day = dates[index]
        prev = dates[index - 1]
        rows = list(by_code_by_date[prev].values())
        today = by_code_by_date[day]
        prev_rows = by_code_by_date[prev]
        _do_exits(day, prev, today, prev_rows)
        _do_adds(day, prev, today, prev_rows)
        _do_entries(day, prev, today, rows)
        _mark(day, today)

    curve_frame = pl.DataFrame(curve)
    stats = _curve_stats(curve_frame, initial, len(dates))
    years = max(1e-9, len(dates) / 252.0)
    avg_equity = _num(curve_frame['equity'].mean())
    multi = sum(1 for value in tranche_hist.values() if value >= 2)
    diagnostics = {
        'entries': entries,
        'closed_trades': len(trades),
        'add_events': add_events,
        'positions_with_add': multi,
        'add_trigger_ratio': multi / max(1, entries),
        'single_tranche_ratio': 1.0 - multi / max(1, entries),
        'stop_counts': stop_counts,
        'capped_entries': capped_entries,
        'open_positions': len(positions),
        'total_cost': total_cost,
        'cost_ratio': total_cost / initial,
        'cost_annual': total_cost / max(avg_equity, 1.0) / years,
        'turnover_annual': buy_notional / max(avg_equity, 1.0) / years,
        'average_exposure': float(
            (curve_frame['equity'].to_numpy() - cash).mean()) / max(avg_equity, 1.0),
        'cost_params': dict(cost_model.params),
    }
    return StagedResult(curve_frame, stats, trades, diagnostics,
                        {'total_cost': total_cost, 'buy_notional': buy_notional})


def simulate_equal(frame: pl.DataFrame, *, top_k: int = 20, hold_days: int = 20,
                   target_weight: float | None = None,
                   initial: float = 1_000_000.0) -> StagedResult:
    """等权一次建仓（无加仓/无止损），用于与金字塔同口径对照。"""
    weight = target_weight if target_weight is not None else 1.0 / max(1, top_k)
    cfg = StagedConfig(
        name='equal', fracs=(1.0,), target_weight=weight, hold_days=hold_days,
        top_k=top_k, entry_stop_pct=None, trail_stop_pct=None)
    return simulate_staged(frame, cfg, initial=initial)


def simulate_grid(daily: pl.DataFrame, *, name: str, tranches: int = 5,
                  tranche_weight: float = 0.10, step: float = 0.05,
                  trend_filter: bool = False, ma_window: int = 200,
                  start: date | None = None, ts_code: str = '510300.SH',
                  initial: float = 1_000_000.0) -> StagedResult:
    """单标的网格：下跌逐档加仓、不止损；``trend_filter`` 跌破 MA200 停加。"""
    from src.backtest.costs.cost_model import CostModel

    cost_model = CostModel()
    data = daily.sort('date')
    dates = data['date'].to_list()
    if len(dates) < 2:
        empty = pl.DataFrame({'date': [], 'equity': []})
        return StagedResult(empty, {}, [], {}, {})

    close = data['close'].to_numpy()
    opens = data['open'].to_numpy()
    ma = pl.Series(close).rolling_mean(window_size=ma_window,
                                       min_samples=ma_window).to_numpy()
    start_idx = 0
    if start is not None:
        for i, day in enumerate(dates):
            if day >= start:
                start_idx = i
                break
    cash = initial
    shares = 0.0
    entry_price = float(opens[start_idx])
    levels = [entry_price * (1.0 - step * (k + 1)) for k in range(tranches)]
    filled = 0
    buy_notional = 0.0
    total_cost = 0.0
    curve: list[dict[str, Any]] = []
    tranche_marks: list[dict[str, Any]] = []
    tranche_cost_basis: list[float] = []
    tranche_shares: list[float] = []

    def _buy(day: date, price: float, notional: float, idx: int) -> None:
        nonlocal cash, shares, buy_notional, total_cost
        units = notional / price
        cost = cost_model.calculate_buy_cost(units, price, trade_date=day,
                                            ts_code=ts_code)
        if units * price + cost > cash:
            units = max(0.0, (cash - cost) / price)
            cost = cost_model.calculate_buy_cost(units, price, trade_date=day,
                                                ts_code=ts_code)
        cash -= units * price + cost
        shares += units
        buy_notional += units * price
        total_cost += cost
        tranche_cost_basis.append(units * price + cost)
        tranche_shares.append(units)
        tranche_marks.append({'tranche': idx, 'buy_price': price,
                              'buy_cost_basis': units * price + cost})

    _buy(dates[start_idx], entry_price, initial * tranche_weight, 0)
    filled = 1
    curve.append({'date': dates[start_idx],
                  'equity': cash + shares * float(close[start_idx])})
    for idx in range(start_idx + 1, len(dates)):
        day = dates[idx]
        prev_close = float(close[idx - 1])
        price = float(opens[idx])
        while filled < tranches and prev_close <= levels[filled]:
            if trend_filter and (not _finite(ma[idx - 1]) or prev_close < float(ma[idx - 1])):
                break
            _buy(day, price, initial * tranche_weight, filled)
            filled += 1
        curve.append({'date': day, 'equity': cash + shares
                      * float(close[idx])})

    curve_frame = pl.DataFrame(curve)
    stats = _curve_stats(curve_frame, initial, len(dates))
    years = max(1e-9, len(dates) / 252.0)
    avg_equity = _num(curve_frame['equity'].mean())
    marks = []
    for item, cost_basis, units in zip(tranche_marks, tranche_cost_basis,
                                       tranche_shares, strict=True):
        value = units * float(close[-1])
        marks.append({'tranche': item['tranche'], 'buy_price': item['buy_price'],
                      'mark_value': value, 'pnl': value - cost_basis})
    wins = [item['pnl'] for item in marks if item['pnl'] > 0]
    losses = [item['pnl'] for item in marks if item['pnl'] <= 0]
    diagnostics = {
        'entries': filled,
        'add_events': filled - 1,
        'add_trigger_ratio': (filled - 1) / max(1, filled),
        'stop_counts': {},
        'open_positions': 1 if shares > 0 else 0,
        'final_exposure': (shares * float(close[-1])
                           / max(float(curve_frame['equity'][-1]), 1e-9)),
        'final_tranches': filled,
        'total_cost': total_cost,
        'cost_ratio': total_cost / initial,
        'cost_annual': total_cost / max(avg_equity, 1.0) / years,
        'turnover_annual': buy_notional / max(avg_equity, 1.0) / years,
        'win_rate': len(wins) / max(1, len(marks)),
        'profit_factor': (sum(wins) / abs(sum(losses))) if losses else float('inf'),
        'tranche_marks': marks,
        'cost_params': dict(cost_model.params),
    }
    return StagedResult(curve_frame, stats, [], diagnostics,
                        {'total_cost': total_cost, 'buy_notional': buy_notional})


def simulate_buy_hold(daily: pl.DataFrame, *, initial: float = 1_000_000.0,
                      weight: float = 1.0) -> StagedResult:
    """同期买入持有：首日开盘一次性买入 ``weight`` 比例，不再交易。"""
    from src.backtest.costs.cost_model import CostModel

    cost_model = CostModel()
    data = daily.sort('date')
    dates = data['date'].to_list()
    open0 = float(data['open'][0])
    close = data['close'].to_numpy()
    rate = cost_model.get_buy_cost_rate(trade_date=dates[0], ts_code='510300.SH')
    units = (initial * weight) / (open0 * (1.0 + rate))
    cost = cost_model.calculate_buy_cost(units, open0, trade_date=dates[0],
                                        ts_code='510300.SH')
    cash = initial - units * open0 - cost
    curve = pl.DataFrame({
        'date': dates,
        'equity': [cash + units * float(value) for value in close],
    })
    stats = _curve_stats(curve, initial, len(dates))
    years = max(1e-9, len(dates) / 252.0)
    avg_equity = _num(curve['equity'].mean())
    diagnostics = {
        'entries': 1, 'add_events': 0, 'add_trigger_ratio': 0.0,
        'stop_counts': {}, 'open_positions': 1,
        'final_exposure': units * float(close[-1]) / max(
            float(curve['equity'][-1]), 1e-9),
        'final_tranches': 1, 'total_cost': cost, 'cost_ratio': cost / initial,
        'cost_annual': cost / max(avg_equity, 1.0) / years,
        'turnover_annual': units * open0 / max(avg_equity, 1.0) / years,
        'win_rate': 1.0 if curve['equity'][-1] > initial else 0.0,
        'profit_factor': float('inf') if curve['equity'][-1] > initial else 0.0,
        'tranche_marks': [{'tranche': 0, 'buy_price': open0,
                           'mark_value': units * float(close[-1]),
                           'pnl': units * float(close[-1]) - units * open0 - cost}],
        'cost_params': dict(cost_model.params),
    }
    return StagedResult(curve, stats, [], diagnostics,
                        {'total_cost': cost, 'buy_notional': units * open0})


def _finite(value: float) -> bool:
    return value == value and value not in (float('inf'), float('-inf'))
