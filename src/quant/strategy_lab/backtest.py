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
from src.core.usecases.run_backtest import _load_local_dividends, _load_st_status

from .coverage import dividend_coverage
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


def select_members(ctx: compute.Context, panels: FieldPanels, rule: Dict[str, Any],
                   days: List[date]) -> Tuple[Dict[date, List[str]], Dict[str, int]]:
    """按「条件链 → 排序 → 取前 N」产出每个调仓日的成分。"""
    cond_panels = [(c, panels.values(c['field'], days)) for c in rule['conditions']]
    sort_panel = panels.values(rule['sort']['field'], days)
    descending = rule['sort']['direction'] == 'desc'
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
            threshold = None
            if operand['type'] == 'quantile':
                pool = [values[c] for c in keep
                        if c in values and math.isfinite(values[c])]
                if not pool:
                    skip[f'{cond["field"]}:no_value'] += 1
                    keep = set()
                    break
                threshold = float(np.quantile(np.asarray(pool, dtype=float), operand['q']))
            before = len(keep)
            keep = {c for c in keep
                    if _passes(values.get(c), cond['op'], operand, threshold)}
            dropped_missing += before - len(keep)
        if not keep:
            skip['empty_after_conditions'] += 1
            continue
        if dropped_missing:
            skip['missing_value'] += dropped_missing

        sort_values = sort_panel.get(day, {})
        ranked = [(c, sort_values[c]) for c in keep
                  if c in sort_values and math.isfinite(sort_values[c])]
        if not ranked:
            skip['sort_field_missing'] += 1
            continue
        # 同值按代码升序打破平局，保证跨进程结果一致
        if descending:
            ranked.sort(key=lambda kv: (-kv[1], kv[0]))
        else:
            ranked.sort(key=lambda kv: (kv[1], kv[0]))
        path[day] = [code for code, _ in ranked[:top_n]]

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
    payload = {'status': 'empty', 'reason': reason, 'rule_text': rule_text(rule)}
    payload.update(extra)
    return payload


def _run_stock_select(rule: Dict[str, Any], t0: float) -> Dict[str, Any]:
    period = {
        'start': rule['period']['start'],
        'end': rule['period']['end'],
        'warmup_start': (date.fromisoformat(rule['period']['start'])
                         - timedelta(days=WARMUP_DAYS)).isoformat(),
    }
    data = compute.load_universe(period, rule['universe'])
    data, violations = compute.drop_contract_violations(data)
    if not data:
        return _empty('行情为空：契约预筛后没有可用标的', rule)

    _, month_ends, prev_day = compute.build_calendar(data)
    days = compute.rebalance_days(month_ends, 'monthly',
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
    dividends = _load_local_dividends(list(data), None, None)
    engine_spec = {'_resolved': {'portfolio': rule['portfolio'], 'cost': rule['cost']}}
    result, used, engine_seconds = pipeline.run_engine(
        data, schedules, engine_spec, dividends, st_status)

    bench = load_benchmark_close(rule['benchmark'])
    metrics = compute_metrics(result, rule, path, bench)
    curve, _ = _period_series(result, rule)
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
        'markers': [{'date': d.isoformat(), 'members': len(codes)}
                    for d, codes in sorted(path.items())],
        'leg_curves': [],
        'positions': _final_positions(result),
        'plan_note': f'月频调仓 {len(path)} 次，成分中位 '
                     f'{metrics["members_median"]} 只',
    }
    payload.update(_curve_payload(curve, bench, rule))
    return payload


def _engine_dividends(codes: List[str]) -> Dict[str, Any]:
    """本地分红快照 → 引擎事件；基金分红的表结构不同，缺复权修正列时跳过。

    ``fund_dividend.parquet`` 只有现金分红、没有 ``cash_div_fix`` /
    ``share_div_fix`` / ``no_ex_rights`` 三列修正；ETF 分红是否纳入复权在
    ``clean_set`` 里仍是 ``pending_fund_dividend`` 未结论。这里不补任何默认值，
    缺列即跳过并告警，避免把未修正的基金分红当成个股送转记账。
    """
    required = {'cash_div_fix', 'share_div_fix', 'no_ex_rights'}
    frames = _load_local_dividends(codes, None, None)
    usable = {code: frame for code, frame in frames.items()
              if required <= set(frame.columns)}
    skipped = sorted(set(frames) - set(usable))
    if skipped:
        logger.warning('分红快照缺复权修正列，本次不记账除权事件：{}', skipped)
    return usable


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
    dividends = _engine_dividends(list(plan.data))
    engine_spec = {'_resolved': {'portfolio': rule['portfolio'], 'cost': rule['cost']}}
    # 基金行情里有的腿复权因子恒为 1.0，复权价与原始价逐值相同；成对传进引擎会被
    # 契约判成「拿未复权价冒充复权价」。这类腿直接去掉 raw_* 对，让 auto 按原始价
    # 处理（价格本身没有信息损失，也不补任何复权因子）。
    engine_data: Dict[str, Any] = {}
    for code, frame in plan.data.items():
        pair = {'open', 'raw_open', 'close', 'raw_close'}
        if pair <= set(frame.columns) and \
                bool((frame['open'] == frame['raw_open']).all()) and \
                bool((frame['close'] == frame['raw_close']).all()):
            frame = frame.drop(['raw_open', 'raw_close'])
        engine_data[code] = frame
    result, used, engine_seconds = pipeline.run_engine(
        engine_data, plan.schedules, engine_spec, dividends, st_status)

    bench = load_benchmark_close(rule['benchmark'])
    metrics = _scheduled_metrics(result, rule, bench, len(plan.markers))
    curve, _ = _period_series(result, rule)
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
        'leg_curves': plan.leg_curves,
        'positions': _final_positions(result),
        'plan_note': plan.note,
    }
    payload.update(_curve_payload(curve, bench, rule))
    return payload


def run_rule(rule: Dict[str, Any], *, with_nav: bool = True) -> Dict[str, Any]:
    """按 ``rule_type`` 分派；空结果以 ``status='empty'`` 返回而非抛错。"""
    t0 = time.perf_counter()
    rule_type = rule['rule_type']
    if rule_type == 'stock_select':
        payload = _run_stock_select(rule, t0)
    elif rule_type in ('asset_switch', 'timing'):
        payload = _run_scheduled(rule, t0)
    else:
        raise ValueError(f'未知 rule_type {rule_type!r}；可选值：'
                         "['stock_select', 'asset_switch', 'timing']")
    if not with_nav:
        for key in ('nav', 'benchmark_curve', 'drawdown', 'leg_curves'):
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
