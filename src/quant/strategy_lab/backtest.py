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
from src.data.services.panel_inputs import load_st_status

from .coverage import dividend_coverage
from .markers import sides_from_weights
from .panels import FieldPanels
from .rules import LATEST_END, rule_text
from .switch_timing import WARMUP_DAYS, PlanError, build_plan

#: 依赖周频因子面板（``_staging`` 研究产物）的字段：末日落后时必须在结果里告警
FACTOR_FIELDS = frozenset({'ff_bp', 'ff_ep', 'ff_dy', 'ff_roe_ttm', 'ff_accruals',
                           'ff_gross_margin', 'vq_value', 'vq_quality', 'vq_score'})

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
    target: Any
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

        codes: list[str] | None = list(keep)
        for i, stage in enumerate(stages):
            assert codes is not None  # 上一轮为 None 时已 break
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


# ---------------------------------------------------------------------------
# 当期清单、目标名单与数据新鲜度
# ---------------------------------------------------------------------------

def _resolve_period(rule: Dict[str, Any]) -> Dict[str, Any]:
    """把 ``period.end == 'latest'`` 解析为本地基准指数末根 bar 的日期。

    解析只发生在运行前的规则副本上，返回的规则里 ``period.end`` 一定是 ISO 日期，
    下游（指标、交易裁剪、落盘）不再感知 ``latest``；原始表达记在 ``requested_end``。
    """
    if rule['period']['end'] != LATEST_END:
        return rule
    latest = max(load_benchmark_close(rule['benchmark']))
    resolved = dict(rule)
    resolved['period'] = {**rule['period'], 'end': latest}
    resolved['requested_end'] = LATEST_END
    return resolved


def _current_snapshot(positions: Dict[str, float], valuation: Dict[str, Any],
                      curve: List[Tuple[date, float]]) -> Dict[str, Any]:
    """当期实际持仓：数量 + 按估值日收盘价折算的权重（来自引擎实际成交，不是目标）。"""
    equity = float(curve[-1][1]) if curve else 0.0
    close = (valuation or {}).get('close') or {}
    weights = {code: round(float(qty) * float(close.get(code, 0.0)) / equity, 6)
               for code, qty in positions.items()
               if equity > 0 and float(qty) > 0} if equity > 0 else {}
    return {'date': (valuation or {}).get('date'), 'equity': round(equity, 2),
            'positions': dict(positions), 'weights': weights}


def _targets_from_path(path: Dict[date, List[str]],
                       cash_buffer: float) -> List[Dict[str, Any]]:
    """选股规则的调仓目标名单：每个调仓日的代码与等权目标权重。"""
    out: List[Dict[str, Any]] = []
    for day in sorted(path):
        codes = path[day]
        if not codes:
            continue
        weight = round((1.0 - cash_buffer) / len(codes), 6)
        out.append({'date': day.isoformat(), 'members': len(codes),
                    'weights': dict.fromkeys(codes, weight)})
    return out


def _targets_from_markers(markers: List[Dict[str, Any]],
                          code: str | None) -> List[Dict[str, Any]]:
    """切换/择时/定投的调仓目标：取 markers 里的目标权重（定投为份数占比）。"""
    out: List[Dict[str, Any]] = []
    seen: Set[tuple] = set()
    for marker in markers:
        weights = marker.get('weights')
        if weights is None:
            position = marker.get('position')
            if position is None or not code:
                continue
            weights = {code: float(position)}
        weights = {str(k): round(float(v), 6) for k, v in weights.items()}
        key = (marker.get('date'), tuple(sorted(weights.items())))
        if key in seen:
            continue
        seen.add(key)
        out.append({'date': marker.get('date'), 'signal_date': marker.get('signal_date'),
                    'weights': weights})
    out.sort(key=lambda item: item['date'] or '')
    return out


def _used_fields(rule: Dict[str, Any]) -> Set[str]:
    """规则实际引用的字段（仅选股规则有条件与排序字段）。"""
    if rule['rule_type'] != 'stock_select':
        return set()
    from .panels import required_columns
    return required_columns(rule)


def _factor_panel_end() -> str | None:
    """因子面板末日（只读 date 列）；面板缺失时返回 ``None``。

    aaa3394 已把 staging 的 ``base_rebal.parquet`` 换成正式表 ``factor_panel``
    （``panels.SF_BASE`` → ``panels.FACTOR_PANEL``），本处引用随之更正。
    """
    from .panels import FACTOR_PANEL
    if not FACTOR_PANEL.is_file():
        return None
    # 正式面板的日期列名为 signal_date（panels._ensure_base 读取时也做同样的映射）
    value = (pl.scan_parquet(FACTOR_PANEL)
             .select(pl.col('signal_date').max()).collect().item())
    return str(value)[:10] if value is not None else None


def _freshness(rule: Dict[str, Any], calendar: List[date]) -> Dict[str, Any]:
    """行情与所用面板的末日核对：落后目标日时写告警，不阻断运行。"""
    target = date.fromisoformat(rule['period']['end'])
    quotes_end = calendar[-1] if calendar else None
    warnings: List[str] = []
    payload: Dict[str, Any] = {
        'target_end': target.isoformat(),
        'quotes_end': quotes_end.isoformat() if quotes_end else None,
        'panel_end': None, 'panel_fields': [],
        'panel_lag_days': 0, 'quotes_gap_days': 0, 'warnings': warnings,
    }
    if quotes_end is None:
        warnings.append('行情为空，无法判断数据新鲜度')
        return payload
    used = _used_fields(rule) & FACTOR_FIELDS
    if used:
        payload['panel_fields'] = sorted(used)
        panel_end = _factor_panel_end()
        payload['panel_end'] = panel_end
        if panel_end:
            lag = sum(1 for day in calendar if date.fromisoformat(panel_end) < day <= target)
            payload['panel_lag_days'] = lag
            if lag:
                warnings.append(
                    f'因子面板末日 {panel_end} 落后目标日 {target.isoformat()} '
                    f'{lag} 个交易日，本次信号按旧因子计算')
    if quotes_end < target:
        warnings.append(f'行情末日 {quotes_end.isoformat()} 早于目标日 '
                        f'{target.isoformat()}，目标日尚未被行情覆盖')
    else:
        payload['quotes_gap_days'] = sum(1 for day in calendar if target < day <= quotes_end)
    return payload



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
                'excess_annual': None, 'information_ratio': None,
                'excess_dsr': None}
    first_day, first_nav, first_bench = paired[0]
    last_day, last_nav, last_bench = paired[-1]
    years = (last_day - first_day).days / 365.25
    if years <= 0:
        return {'benchmark': bench_code, 'benchmark_annual': None,
                'excess_annual': None, 'information_ratio': None,
                'excess_dsr': None}
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
    from src.backtest.performance.benchmark_eval import excess_deflated_sharpe
    dsr = excess_deflated_sharpe(nav_returns, bench_returns, n_trials=1,
                                 periods_per_year=12)
    return {
        'benchmark': bench_code,
        'benchmark_annual': round(float(bench_annual), 6),
        'excess_annual': round(float(strategy_annual - bench_annual), 6),
        'information_ratio': None if not math.isfinite(ir) else round(float(ir), 4),
        'excess_dsr': None if not math.isfinite(dsr) else round(float(dsr), 4),
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


#: Sharpe 最少日收益样本数（约 1.5 个月）：不足时视为样本不足，返回 0。
MIN_SHARPE_SAMPLES = 30


def _cagr_sharpe(curve: List[Tuple[date, float]], annual: float) -> float:
    """Sharpe = CAGR ÷ 年化波动率，与工具研究脚本同口径。

    年化波动率 = 日收益标准差（``ddof=1``）× √252；无风险利率按 0（与研究一致）。
    不使用「日收益均值 / 日标准差 × √252」的算术口径——那会让 Sharpe 的分母与
    年化收益（CAGR）不同源，本次对账中正是它造成 +0.077 的口径偏差。

    边界：净值非有限值/非正值先剔除；有效日收益少于 ``MIN_SHARPE_SAMPLES`` 视为
    样本不足返回 0；波动率为 0 或非有限值返回 0；CAGR 为负时保留负 Sharpe；
    最终结果非有限值时返回 0，保证界面不出现 inf / NaN。
    """
    points = [(d, float(e)) for d, e in curve
              if math.isfinite(float(e)) and float(e) > 0.0]
    if len(points) < MIN_SHARPE_SAMPLES + 1:
        return 0.0
    equity = [e for _, e in points]
    returns = [equity[i] / equity[i - 1] - 1.0 for i in range(1, len(equity))]
    returns = [r for r in returns if math.isfinite(r)]
    if len(returns) < MIN_SHARPE_SAMPLES:
        return 0.0
    volatility = float(np.std(returns, ddof=1) * math.sqrt(252))
    if not math.isfinite(volatility) or volatility <= 1e-12:
        return 0.0
    value = annual / volatility
    return round(value, 4) if math.isfinite(value) else 0.0


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
    annual = float(perf.get('annual_return', 0.0)) / 100.0
    metrics = {
        'start': first_day.isoformat(),
        'end': last_day.isoformat(),
        'years': round(years, 2),
        'final_equity': round(last_eq, 2),
        'annual': round(annual, 6),
        'max_drawdown': round(float(perf.get('max_drawdown', 0.0)) / 100.0, 6),
        'sharpe': _cagr_sharpe(curve, annual),
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
        # end 写成 latest 时，requested_end 保留原始表达，end 是解析后的 ISO 日期
        'requested_end': rule.get('requested_end') or rule['period']['end'],
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
    st_status = load_st_status(None, data, None, period['warmup_start'],
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
    valuation = _valuation(curve, data, positions)
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
        'valuation': valuation,
        # 调仓目标名单（调仓日 → 代码 + 等权目标权重），不再只留在内部 path
        'targets': _targets_from_path(path, rule['portfolio']['cash_buffer']),
        'current': _current_snapshot(positions, valuation, curve),
        'freshness': _freshness(rule, calendar),
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
    st_status = load_st_status(None, plan.data, None, warmup,
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
    valuation = _valuation(curve, engine_data, positions)
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
        'valuation': valuation,
        # 切换/择时的目标权重直接来自 markers；定投为份数占比
        'targets': _targets_from_markers(plan.markers,
                                         (rule.get('target') or {}).get('code')),
        'current': _current_snapshot(positions, valuation, curve),
        'freshness': _freshness(rule, calendar),
        'plan_note': plan.note,
        'period_effective': _period_effective(rule, metrics, calendar),
    }
    # 定投补仓：最后一根 bar 的当日信号（是否补仓/止盈、下次触发价）
    if plan.daily:
        payload['daily_signal'] = plan.daily
    # 估值分位择时：最后一期分位与所取估值数据日期，供界面只读展示
    if plan.signal_tail:
        payload['signal_tail'] = plan.signal_tail
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
    rule = _resolve_period(rule)
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
