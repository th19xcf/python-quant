"""规则 → 选股路径 → 复用既有引擎 → 指标与基准对比。

选股（条件链 + 排序 + 取前 N）是本模块唯一的新逻辑；净值、撮合、成本全部
走 ``src.backtest`` 既有实现（``pipeline.run_engine`` / ``SpecCostModel`` /
``PortfolioBacktestEngine``），指标公式复用 ``PerformanceAnalyzer`` 与
``benchmark_eval``，不另写一套净值计算。
"""
from __future__ import annotations

import math
import time
from collections import Counter
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import numpy as np
import polars as pl
from loguru import logger

from src.backtest.performance.benchmark_eval import information_ratio
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer
from src.backtest.runner import compute, pipeline
from src.core.usecases.run_backtest import _load_st_status

from .coverage import dividend_coverage
from .markers import sides_from_weights
from .panels import FieldPanels
from .rules import rule_text
from .switch_timing import WARMUP_DAYS, PlanError, build_plan

ROOT = Path(__file__).resolve().parents[3]
INDEX_QUOTES = ROOT / 'data/local_store/quotes/index'


# ---------------------------------------------------------------------------
# 选股
# ---------------------------------------------------------------------------

def _passes(value: float | None, op: str, operand: Dict[str, Any],
            threshold: float | None = None) -> bool:
    """单条条件判定；缺值一律不通过（无法证伪即不入选）。"""
    if value is None:
        return False
    if operand['type'] == 'quantile':
        if threshold is None:
            return False
        target = threshold
    else:
        target = operand.get('value')
    if op == '>':
        return value > target
    if op == '>=':
        return value >= target
    if op == '<':
        return value < target
    if op == '<=':
        return value <= target
    if op == '==':
        return value == target
    if op == '!=':
        return value != target
    if op == 'between':
        return operand['low'] <= value <= operand['high']
    return False


def _quantile_keep(keep: Set[str], values: Dict[str, float], op: str,
                   operand: Dict[str, Any], day_industry: Dict[str, str]) -> Set[str] | None:
    """按分位阈值保留；``group='industry'`` 时逐行业取阈值。无任何可用值返回 ``None``。"""
    q = operand['q']
    if operand.get('group') == 'industry':
        buckets: Dict[str, List[float]] = {}
        for code in keep:
            industry = day_industry.get(code)
            value = values.get(code)
            if industry is None or value is None or not math.isfinite(value):
                continue
            buckets.setdefault(industry, []).append(value)
        if not buckets:
            return None
        thresholds = {ind: float(np.quantile(np.asarray(vs, dtype=float), q))
                      for ind, vs in buckets.items()}
        return {code for code in keep
                if _passes(values.get(code), op, operand,
                           thresholds.get(day_industry.get(code, '')))}
    pool = [values[c] for c in keep if c in values and math.isfinite(values[c])]
    if not pool:
        return None
    threshold = float(np.quantile(np.asarray(pool, dtype=float), q))
    return {code for code in keep if _passes(values.get(code), op, operand, threshold)}


def _apply_stage(codes: List[str], values: Dict[str, float], stage: Dict[str, Any],
                 is_last: bool, top_n: int) -> List[str] | None:
    """执行一段排序：非末段按 ``keep_pct`` 截断，末段取前 ``top_n``；无可用值返回 ``None``。"""
    items = [(code, values[code]) for code in codes
             if code in values and math.isfinite(values[code])]
    if not items:
        return None
    # 同值按代码升序打破平局，保证跨进程结果一致
    if stage['direction'] == 'desc':
        items.sort(key=lambda kv: (-kv[1], kv[0]))
    else:
        items.sort(key=lambda kv: (kv[1], kv[0]))
    if is_last:
        return [code for code, _ in items[:top_n]]
    keep_pct = stage.get('keep_pct')
    if keep_pct is not None:
        items = items[:max(1, int(math.ceil(len(items) * keep_pct)))]
    return [code for code, _ in items]


def select_members(ctx: compute.Context, panels: FieldPanels, rule: Dict[str, Any],
                   days: List[date]) -> Tuple[Dict[date, List[str]], Dict[str, int]]:
    """按「条件链 → 排序段（可带 keep_pct）→ 取前 N」产出每个调仓日的成分。"""
    cond_panels = [(c, panels.values(c['field'], days)) for c in rule['conditions']]
    grouped = any(c['operand'].get('group') == 'industry' for c in rule['conditions'])
    industry_panel = panels.values('industry', days) if grouped else {}
    stages = rule['sort'] if isinstance(rule['sort'], list) else [rule['sort']]
    stage_panels = [panels.values(stage['field'], days) for stage in stages]
    top_n = int(rule['top_n'])
    skip: Counter[str] = Counter()
    path: Dict[date, List[str]] = {}

    for day in days:
        keep: Set[str] = set(ctx.quoted[day])
        if ctx.allowed is not None:
            keep &= ctx.allowed
        if not keep:
            skip['no_quote'] += 1
            continue

        dropped_missing = 0
        for cond, panel in cond_panels:
            values = panel.get(day, {})
            operand = cond['operand']
            if operand['type'] == 'quantile':
                result = _quantile_keep(keep, values, cond['op'], operand,
                                        industry_panel.get(day, {}) if grouped else {})
                if result is None:
                    skip[f'{cond["field"]}:no_value'] += 1
                    keep = set()
                    break
                dropped_missing += len(keep) - len(result)
                keep = result
                continue
            before = len(keep)
            keep = {c for c in keep if _passes(values.get(c), cond['op'], operand)}
            dropped_missing += before - len(keep)
        if not keep:
            skip['empty_after_conditions'] += 1
            continue
        if dropped_missing:
            skip['missing_value'] += dropped_missing

        codes = list(keep)
        for i, stage in enumerate(stages):
            codes = _apply_stage(codes, stage_panels[i].get(day, {}), stage,
                                 i == len(stages) - 1, top_n)
            if codes is None:
                skip['sort_field_missing'] += 1
                break
        if codes:
            path[day] = codes

    sizes = sorted(len(v) for v in path.values())
    if sizes:
        logger.info('成分路径：{} 个调仓日，中位 {} 只（{}~{}）', len(path),
                    sizes[len(sizes) // 2], sizes[0], sizes[-1])
    if skip:
        logger.info('选股跳过统计：{}', dict(skip))
    return path, dict(skip)


# ---------------------------------------------------------------------------
# 基准与指标
# ---------------------------------------------------------------------------

def load_benchmark_close(code: str) -> Dict[str, float]:
    paths = sorted(INDEX_QUOTES.glob(f'**/{code}.parquet'))
    if not paths:
        raise RuntimeError(f'基准指数行情缺失：{code}（{INDEX_QUOTES}）')
    df = pl.read_parquet(paths[0], columns=['date', 'close']).sort('date')
    return {str(d)[:10]: float(c) for d, c in
            zip(df['date'].to_list(), df['close'].to_list(), strict=True)
            if c is not None}


def _monthly_last(series: List[Tuple[date, float]]) -> List[Tuple[str, float]]:
    out: Dict[str, float] = {}
    for day, value in series:
        out[f'{day.year:04d}-{day.month:02d}'] = value
    return [(k, out[k]) for k in sorted(out)]


def _excess_metrics(nav: List[Tuple[date, float]], bench: Dict[str, float],
                    bench_code: str) -> Dict[str, Any]:
    paired = [(day, value, bench[day.isoformat()]) for day, value in nav
              if day.isoformat() in bench and bench[day.isoformat()] > 0]
    if len(paired) < 2:
        return {'benchmark': bench_code, 'benchmark_annual': None,
                'excess_annual': None, 'information_ratio': None}
    first_day, first_nav, first_bench = paired[0]
    last_day, last_nav, last_bench = paired[-1]
    years = (last_day - first_day).days / 365.25
    if years <= 0:
        return {'benchmark': bench_code, 'benchmark_annual': None,
                'excess_annual': None, 'information_ratio': None}
    bench_annual = (last_bench / first_bench) ** (1.0 / years) - 1.0
    strategy_annual = (last_nav / first_nav) ** (1.0 / years) - 1.0

    monthly_nav = _monthly_last([(d, v) for d, v, _ in paired])
    monthly_bench = _monthly_last([(d, b) for d, _, b in paired])
    nav_returns = [b / a - 1.0
                   for (_, a), (_, b) in zip(monthly_nav, monthly_nav[1:], strict=False)]
    bench_returns = [b / a - 1.0
                     for (_, a), (_, b) in zip(monthly_bench, monthly_bench[1:], strict=False)]
    excess = [s - b for s, b in zip(nav_returns, bench_returns, strict=False)]
    ir = information_ratio(excess, periods_per_year=12)
    return {
        'benchmark': bench_code,
        'benchmark_annual': round(float(bench_annual), 6),
        'excess_annual': round(float(strategy_annual - bench_annual), 6),
        'information_ratio': None if not math.isfinite(ir) else round(float(ir), 4),
        'excess_months': len(excess),
    }


def _period_series(result: Dict[str, Any], rule: Dict[str, Any]
                   ) -> Tuple[List[Tuple[date, float]], List[Dict[str, Any]]]:
    start = date.fromisoformat(rule['period']['start'])
    end = date.fromisoformat(rule['period']['end'])
    curve = [(d, float(e)) for d, e in
             ((pipeline.to_date(r['date']), r['equity']) for r in result['equity_curve'])
             if d is not None and start <= d <= end]
    if len(curve) < 2:
        raise RuntimeError(f'净值曲线在 {start}~{end} 内不足两个点')
    trades = [t for t in (result.get('trades') or [])
              if (d := pipeline.to_date(t.get('date'))) is not None and start <= d <= end]
    return curve, trades


def _core_metrics(curve: List[Tuple[date, float]], trades: List[Dict[str, Any]],
                  rule: Dict[str, Any], bench: Dict[str, float]) -> Dict[str, Any]:
    equity_curve = [{'date': d, 'equity': e} for d, e in curve]
    analyzer = PerformanceAnalyzer()
    perf = analyzer.analyze(equity_curve, trades, frequency='daily')

    first_day, first_eq = curve[0]
    last_day, last_eq = curve[-1]
    years = (last_day - first_day).days / 365.25
    avg_eq = sum(e for _, e in curve) / len(curve)
    buy_notional = sum(t['price'] * t['shares'] for t in trades if t.get('signal') == 'buy')
    total_cost = sum(float(t.get('cost') or 0.0) for t in trades)
    turnover = analyzer.annual_turnover(buy_notional, avg_eq, years)
    cost_pct = analyzer.annual_cost_ratio(total_cost, avg_eq, years)
    metrics = {
        'start': first_day.isoformat(),
        'end': last_day.isoformat(),
        'years': round(years, 2),
        'final_equity': round(last_eq, 2),
        'annual': round(float(perf.get('annual_return', 0.0)) / 100.0, 6),
        'max_drawdown': round(float(perf.get('max_drawdown', 0.0)) / 100.0, 6),
        'sharpe': round(float(perf.get('sharpe_ratio', 0.0)), 4),
        'volatility': round(float(perf.get('volatility', 0.0)) / 100.0, 6),
        'turnover_one_way': round(float(turnover['one_way']), 4),
        'turnover_two_way': round(float(turnover['two_way']), 4),
        'annual_cost_pct': round(float(cost_pct), 4),
        'cost_total': round(total_cost, 2),
        'trades': len(trades),
    }
    metrics.update(_excess_metrics(curve, bench, rule['benchmark']))
    return metrics


def compute_metrics(result: Dict[str, Any], rule: Dict[str, Any],
                    path: Dict[date, List[str]], bench: Dict[str, float]) -> Dict[str, Any]:
    curve, trades = _period_series(result, rule)
    metrics = _core_metrics(curve, trades, rule, bench)
    sizes = sorted(len(v) for v in path.values())
    metrics.update({
        'rebalances': len(path),
        'members_median': sizes[len(sizes) // 2] if sizes else 0,
        'members_min': sizes[0] if sizes else 0,
        'members_max': sizes[-1] if sizes else 0,
    })
    return metrics


def _scheduled_metrics(result: Dict[str, Any], rule: Dict[str, Any],
                       bench: Dict[str, float], rebalances: int) -> Dict[str, Any]:
    curve, trades = _period_series(result, rule)
    metrics = _core_metrics(curve, trades, rule, bench)
    metrics.update({'rebalances': rebalances, 'members_median': None,
                    'members_min': None, 'members_max': None})
    return metrics


def _dca_metrics(result: Dict[str, Any], rule: Dict[str, Any], plan: Any,
                 curve: List[Tuple[date, float]]) -> Dict[str, Any]:
    """定投补仓专属指标：累计投入、平均成本、资金闲置比例与触发次数。"""
    code = rule['target']['code']
    frame = plan.data[code]
    close = {d: float(c) for d, c in
             zip(frame['date'].to_list(), frame['close'].to_list(), strict=True)}
    rows: List[Tuple[date, Dict[str, Any]]] = []
    for trade in (result.get('trades') or []):
        trade_day = pipeline.to_date(trade.get('date'))
        if trade_day is not None:
            rows.append((trade_day, trade))
    rows.sort(key=lambda item: item[0])
    buys = [trade for _, trade in rows if trade.get('signal') == 'buy']
    sells = [trade for _, trade in rows if trade.get('signal') == 'sell']
    invested = sum(float(t['price']) * float(t['shares']) for t in buys)
    sold = sum(float(t['price']) * float(t['shares']) for t in sells)
    buy_shares = sum(float(t['shares']) for t in buys)

    idle: List[float] = []
    index = 0
    shares = 0.0
    for day, equity in curve:
        while index < len(rows) and rows[index][0] <= day:
            row = rows[index][1]
            sign = 1.0 if row.get('signal') == 'buy' else -1.0
            shares += sign * float(row['shares'])
            index += 1
        price = close.get(day)
        if price is None or equity <= 0:
            continue
        idle.append(max(0.0, min(1.0, 1.0 - max(shares, 0.0) * price / equity)))

    reasons = [m.get('reason') for m in plan.markers]
    return {
        'invested_total': round(invested, 2),
        'sold_total': round(sold, 2),
        'avg_cost': round(invested / buy_shares, 4) if buy_shares else None,
        'adds': reasons.count('add'),
        'take_profits': reasons.count('take_profit'),
        'entries': reasons.count('initial') + reasons.count('restart'),
        'max_tranches': max((m['tranches'] for m in plan.markers
                             if m.get('reason') != 'take_profit'), default=0),
        'idle_ratio': round(sum(idle) / len(idle), 4) if idle else None,
        'cycle_returns': [m.get('return') for m in plan.markers
                          if m.get('reason') == 'take_profit'],
    }


def _buy_and_hold(rule: Dict[str, Any], data: Dict[str, Any], dividends: Any,
                  st_status: Any) -> Dict[str, Any]:
    """同区间、同成本口径的买入持有对照：区间首日信号，次日开盘全仓买入后不动。"""
    code = rule['target']['code']
    start = date.fromisoformat(rule['period']['start'])
    days = [d for d in data[code]['date'].to_list() if d >= start]
    if len(days) < 2:
        return {'metrics': {}, 'curve': []}
    schedules = {code: {days[0]: {'action': 'buy', 'fraction': 1.0}}}
    spec = {'_resolved': {'portfolio': rule['portfolio'], 'cost': rule['cost']}}
    result, _, _ = pipeline.run_engine(data, schedules, spec, dividends, st_status)
    curve, trades = _period_series(result, rule)
    metrics = _core_metrics(curve, trades, rule, load_benchmark_close(rule['benchmark']))
    keep = {key: metrics[key] for key in
            ('start', 'end', 'years', 'final_equity', 'annual', 'max_drawdown',
             'sharpe', 'volatility', 'cost_total', 'trades')}
    keep['invested_total'] = round(sum(float(t['price']) * float(t['shares'])
                                       for t in trades if t.get('signal') == 'buy'), 2)
    return {'metrics': keep,
            'curve': [[d.isoformat(), round(e, 2)] for d, e in curve]}


def _drawdown(curve: List[Tuple[date, float]]) -> List[List[Any]]:
    peak = 0.0
    out: List[List[Any]] = []
    for day, equity in curve:
        peak = max(peak, equity)
        out.append([day.isoformat(), round(equity / peak - 1.0, 6) if peak > 0 else 0.0])
    return out


def _final_positions(result: Dict[str, Any]) -> Dict[str, float]:
    return {code: float(qty) for code, qty in (result.get('positions') or {}).items()
            if float(qty) > 0}


def _valuation(curve: List[Tuple[date, float]], data: Dict[str, Any],
               positions: Dict[str, float]) -> Dict[str, Any]:
    """估值日与收盘价：供「查看流水」弹窗算当前持仓的持有盈亏。

    估值日取回测净值曲线最后一根 bar，收盘价取同一天的前复权收盘价（引擎
    盯市用的同一列），只收录仍有持仓的标的；未持有的不输出，避免弹窗拿错价。
    """
    if not curve:
        return {'date': None, 'close': {}}
    last_day = curve[-1][0]
    close: Dict[str, float] = {}
    for code in positions:
        frame = data.get(code)
        if frame is None:
            continue
        prices = {d: float(c) for d, c in
                  zip(frame['date'].to_list(), frame['close'].to_list(),
                      strict=True)}
        price = prices.get(last_day)
        if price is not None and price > 0:
            close[code] = price
    return {'date': last_day.isoformat(), 'close': close}


def _curve_payload(nav: List[Tuple[date, float]], bench: Dict[str, float],
                   rule: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'nav': [[d.isoformat(), round(e, 2)] for d, e in nav],
        'benchmark_curve': _benchmark_curve(nav, bench),
        'drawdown': _drawdown(nav),
    }


# ---------------------------------------------------------------------------
# 编排
# ---------------------------------------------------------------------------

def _empty(reason: str, rule: Dict[str, Any], **extra: Any) -> Dict[str, Any]:
    payload = {'status': 'empty', 'reason': reason, 'rule_text': rule_text(rule),
               'trades': []}
    payload.update(extra)
    return payload


def _trade_records(result: Dict[str, Any], rule: Dict[str, Any]) -> List[Dict[str, Any]]:
    """逐笔成交输出：只把引擎已记录的 trades 归一化，不参与任何净值或指标计算。

    引擎 ``result['trades']`` 每条含 date / stock_code / signal / price / shares /
    cost，这里只裁剪到统计区间并统一字段名，供「查看流水」弹窗消费。
    """
    start = date.fromisoformat(rule['period']['start'])
    end = date.fromisoformat(rule['period']['end'])
    rows: List[Dict[str, Any]] = []
    for trade in (result.get('trades') or []):
        trade_day = pipeline.to_date(trade.get('date'))
        if trade_day is None or not start <= trade_day <= end:
            continue
        rows.append({
            'date': trade_day.isoformat(),
            'code': str(trade.get('stock_code') or ''),
            'side': 'buy' if trade.get('signal') == 'buy' else 'sell',
            'price': float(trade.get('price') or 0.0),
            'shares': float(trade.get('shares') or 0.0),
            'cost': float(trade.get('cost') or 0.0),
        })
    rows.sort(key=lambda row: (row['date'], row['code'], row['side']))
    return rows


def _period_effective(rule: Dict[str, Any], metrics: Dict[str, Any],
                      calendar: List[date]) -> Dict[str, Any]:
    """实际生效区间：起点只影响净值/指标/三图起算日，预热仍取起点前完整历史。

    ``metrics['start']`` 是净值首日：起点早于可用行情首日时它等于实际最早日，
    与 ``requested_start`` 不同即说明被前移过。``prior_trading_days`` 是起点之前
    可加载的交易日数，滚动 250 日分位与 120 日动量不足时点会给出提示。
    """
    requested = rule['period']['start']
    effective = metrics.get('start') or requested
    return {
        'requested_start': requested,
        'start': effective,
        'end': metrics.get('end') or rule['period']['end'],
        'warmup_start': (date.fromisoformat(requested)
                         - timedelta(days=WARMUP_DAYS)).isoformat(),
        'data_first_day': calendar[0].isoformat() if calendar else None,
        'prior_trading_days': sum(1 for d in calendar
                                  if d < date.fromisoformat(effective)),
    }


def _run_stock_select(rule: Dict[str, Any], t0: float) -> Dict[str, Any]:
    period = {
        'start': rule['period']['start'],
        'end': rule['period']['end'],
        'warmup_start': (date.fromisoformat(rule['period']['start'])
                         - timedelta(days=WARMUP_DAYS)).isoformat(),
    }
    data = compute.load_universe(period, rule['universe'], adjustment='qfq')
    data, violations = compute.drop_contract_violations(data)
    if not data:
        return _empty('行情为空：契约预筛后没有可用标的', rule)

    calendar, month_ends, prev_day = compute.build_calendar(data)
    frequency = rule['rebalance']['frequency']
    days = compute.rebalance_days(month_ends, frequency,
                                  date.fromisoformat(rule['period']['start']))
    if not days:
        return _empty('调仓日为空：检查区间与本地行情覆盖', rule)

    quoted = compute.quoted_sets(data, days)
    st_status = _load_st_status(None, data, None, period['warmup_start'],
                                period['end'], None, None)
    ctx = compute.Context(data=data, days=days, quoted=quoted,
                          st_status=st_status or {}, allowed=None)
    panels = FieldPanels(ctx)
    path, skipped = select_members(ctx, panels, rule, days)
    if not path:
        return _empty(f'条件筛选后没有任何调仓日有成分；跳过统计：{skipped}', rule,
                      skipped=skipped)

    schedules = pipeline.build_schedules(path, prev_day,
                                         rule['portfolio']['cash_buffer'])
    # 统一前复权口径（见 switch_timing._normalize）：行情已是前复权价，分红与送转
    # 已内含在价格里，再记一遍除权事件等于重复计入，故四类规则一律不传分红账本。
    dividends = None
    engine_spec = {'_resolved': {'portfolio': rule['portfolio'], 'cost': rule['cost']}}
    result, used, engine_seconds = pipeline.run_engine(
        data, schedules, engine_spec, dividends, st_status)

    bench = load_benchmark_close(rule['benchmark'])
    metrics = compute_metrics(result, rule, path, bench)
    curve, _ = _period_series(result, rule)
    positions = _final_positions(result)
    markers: List[Dict[str, Any]] = []
    prev_weights: Dict[str, float] = {}
    for day, codes in sorted(path.items()):
        weight = (1.0 - rule['portfolio']['cash_buffer']) / len(codes)
        cur = dict.fromkeys(codes, weight)
        for side in sides_from_weights(prev_weights, cur):
            markers.append({'date': day.isoformat(), 'members': len(codes),
                            'direction': side})
        prev_weights = cur
    payload = {
        'status': 'ok',
        'metrics': metrics,
        'skipped': skipped,
        'contract_violations': len(violations),
        'used_codes': used,
        # 缺分红会让收益静默低估：结果里显式标注覆盖不足的标的
        'dividend_coverage': dividend_coverage(list(data)),
        'engine_seconds': round(engine_seconds, 1),
        'elapsed_seconds': round(time.perf_counter() - t0, 1),
        'rule_text': rule_text(rule),
        'markers': markers,
        'trades': _trade_records(result, rule),
        'leg_curves': [],
        'positions': positions,
        'valuation': _valuation(curve, data, positions),
        'plan_note': f'{"月频" if frequency == "monthly" else "半年频"}调仓 {len(path)} 次，'
                     f'成分中位 {metrics["members_median"]} 只',
        'period_effective': _period_effective(rule, metrics, calendar),
    }
    payload.update(_curve_payload(curve, bench, rule))
    return payload


def _run_scheduled(rule: Dict[str, Any], t0: float) -> Dict[str, Any]:
    try:
        plan = build_plan(rule)
    except PlanError as exc:
        return _empty(str(exc), rule)
    if not plan.schedules:
        return _empty('没有产生任何调仓信号：检查区间、窗口与阈值', rule)

    warmup = (date.fromisoformat(rule['period']['start'])
              - timedelta(days=WARMUP_DAYS)).isoformat()
    st_status = _load_st_status(None, plan.data, None, warmup,
                                rule['period']['end'], None, None)
    # 统一前复权口径：行情只有前复权价（见 switch_timing._normalize），撮合与盯市
    # 都在前复权价空间；分红/送转已内含在价格里，不再单独记账除权事件。
    dividends = None
    engine_spec = {'_resolved': {'portfolio': rule['portfolio'], 'cost': rule['cost']}}
    engine_data = plan.data
    result, used, engine_seconds = pipeline.run_engine(
        engine_data, plan.schedules, engine_spec, dividends, st_status)

    bench = load_benchmark_close(rule['benchmark'])
    metrics = _scheduled_metrics(result, rule, bench, len(plan.markers))
    curve, _ = _period_series(result, rule)
    calendar = compute.build_calendar(plan.data)[0]
    positions = _final_positions(result)
    payload = {
        'status': 'ok',
        'metrics': metrics,
        'skipped': {},
        'contract_violations': 0,
        'used_codes': used,
        'dividend_coverage': dividend_coverage(list(plan.data)),
        'engine_seconds': round(engine_seconds, 1),
        'elapsed_seconds': round(time.perf_counter() - t0, 1),
        'rule_text': rule_text(rule),
        'markers': plan.markers,
        'trades': _trade_records(result, rule),
        'leg_curves': plan.leg_curves,
        'positions': positions,
        'valuation': _valuation(curve, engine_data, positions),
        'plan_note': plan.note,
        'period_effective': _period_effective(rule, metrics, calendar),
    }
    if rule['rule_type'] == 'dca_grid':
        metrics.update(_dca_metrics(result, rule, plan, curve))
        hold = _buy_and_hold(rule, engine_data, dividends, st_status)
        metrics['buy_hold'] = hold['metrics']
        payload['buy_hold_curve'] = hold['curve']
    payload.update(_curve_payload(curve, bench, rule))
    return payload


def run_rule(rule: Dict[str, Any], *, with_nav: bool = True) -> Dict[str, Any]:
    """按 ``rule_type`` 分派；空结果以 ``status='empty'`` 返回而非抛错。"""
    t0 = time.perf_counter()
    rule_type = rule['rule_type']
    if rule_type == 'stock_select':
        payload = _run_stock_select(rule, t0)
    elif rule_type in ('asset_switch', 'timing', 'dca_grid'):
        payload = _run_scheduled(rule, t0)
    else:
        raise ValueError(f'未知 rule_type {rule_type!r}；可选值：'
                         "['stock_select', 'asset_switch', 'timing', 'dca_grid']")
    if not with_nav:
        for key in ('nav', 'benchmark_curve', 'buy_hold_curve', 'drawdown',
                    'leg_curves'):
            payload.pop(key, None)
    return payload


def _benchmark_curve(nav: List[Tuple[date, float]], bench: Dict[str, float]) -> List[List[Any]]:
    """把基准收盘价换算到与策略同起点的净值刻度，供界面同图对比。"""
    anchor = next(((d, e) for d, e in nav
                   if d is not None and bench.get(d.isoformat(), 0) > 0), None)
    if anchor is None:
        return []
    start_day, start_equity = anchor
    base = bench[start_day.isoformat()]
    return [[d.isoformat(), round(start_equity * bench[d.isoformat()] / base, 2)]
            for d, _ in nav if d is not None and d.isoformat() in bench]
