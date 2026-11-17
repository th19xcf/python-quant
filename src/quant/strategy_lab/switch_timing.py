"""强弱比切换（``asset_switch``）与仓位择时（``timing``）的计划构造。

本模块只做「行情 → 信号 → 调度表」，不撮合、不算净值：调度表交给
``src.backtest.runner.pipeline.run_engine``，引擎按「信号日 D 收盘生成、
D+1 开盘成交」执行，因此换仓时点天然等于下一交易日开盘。

口径：
- 比值中枢 = 分子/分母收盘价比值的 ``window`` 个共同交易日移动均值；
- 比值高于中枢 ``entry`` 切到分子，低于中枢 ``exit`` 切到分母，
  两次切换之间至少间隔 ``min_hold_days`` 个共同交易日；
- 仓位择时的信号（``ma_ratio`` / ``momentum`` / ``drawdown``）落在
  ``levels`` 档位上，跨档需越过阈值 ``hysteresis``，单日最多调一档；
- 数据不足（共同交易日少于窗口）直接报错，不静默降级。
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import polars as pl
from loguru import logger

ROOT = Path(__file__).resolve().parents[3]
QUOTE_ROOT = ROOT / 'data/local_store/quotes'
QUOTE_DOMAINS = ('fund', 'index')
#: 预热：覆盖最长 250 日窗口 + 节假日冗余
WARMUP_DAYS = 400


class PlanError(ValueError):
    """行情缺失或历史不足，无法构造调度表。"""


@dataclass
class Plan:
    """一次切换/择时运行的完整输入：引擎行情、调度表、图线数据。"""

    data: Dict[str, pl.DataFrame]
    schedules: Dict[str, Dict[date, Tuple[str, float]]]
    markers: List[Dict[str, Any]]
    leg_curves: List[Dict[str, Any]]
    note: str


# ---------------------------------------------------------------------------
# 行情加载
# ---------------------------------------------------------------------------

def _quote_path(code: str) -> Path:
    for domain in QUOTE_DOMAINS:
        hits = sorted((QUOTE_ROOT / domain).glob(f'**/{code}.parquet'))
        if hits:
            return hits[0]
    raise PlanError(
        f'标的 {code} 在本地行情中不存在（已查 {"/".join(QUOTE_DOMAINS)}，'
        f'根目录 {QUOTE_ROOT}）')


def _normalize(frame: pl.DataFrame) -> pl.DataFrame:
    """复权价 + 原始价成对输出，口径与 ``src/advice/data.py`` 一致。"""
    frame = frame.with_columns(pl.col('date').cast(pl.Date, strict=False))
    if 'hfq_factor' not in frame.columns:
        frame = frame.with_columns(pl.lit(1.0).alias('hfq_factor'))
    frame = frame.with_columns(
        pl.col('hfq_factor').cast(pl.Float64, strict=False).fill_null(1.0))
    columns = [
        pl.col('date'),
        (pl.col('open') * pl.col('hfq_factor')).alias('open'),
        (pl.col('high') * pl.col('hfq_factor')).alias('high'),
        (pl.col('low') * pl.col('hfq_factor')).alias('low'),
        (pl.col('close') * pl.col('hfq_factor')).alias('close'),
        pl.col('volume').cast(pl.Float64, strict=False).alias('volume'),
        pl.col('open').cast(pl.Float64, strict=False).alias('raw_open'),
        pl.col('close').cast(pl.Float64, strict=False).alias('raw_close'),
    ]
    if 'amount' in frame.columns:
        columns.append(pl.col('amount').cast(pl.Float64, strict=False).alias('amount'))
    return (frame.select(columns)
            .filter(pl.col('close').is_not_null() & (pl.col('close') > 0))
            .sort('date').unique('date', keep='last'))


def load_leg(code: str, start: date, end: date) -> pl.DataFrame:
    path = _quote_path(code)
    raw = pl.read_parquet(path)
    missing = sorted({'date', 'open', 'high', 'low', 'close', 'volume'} - set(raw.columns))
    if missing:
        raise PlanError(f'标的 {code} 行情缺必需列 {missing}（{path}）')
    frame = _normalize(raw).filter(
        (pl.col('date') >= start) & (pl.col('date') <= end))
    if frame.height < 2:
        raise PlanError(
            f'标的 {code} 在 {start}~{end} 内只有 {frame.height} 条行情，无法回测')
    return frame


def load_legs(codes: Iterable[str], start: date, end: date) -> Dict[str, pl.DataFrame]:
    data = {code: load_leg(code, start, end) for code in dict.fromkeys(codes)}
    dates = [set(df['date'].to_list()) for df in data.values()]
    common = sorted(set.intersection(*dates))
    if len(common) < 2:
        raise PlanError(
            f'多标的共同交易日不足（{len(common)} 天）；请检查区间 '
            f'{start}~{end} 与各标的上市时间')
    logger.info('切换/择时行情：{} 只，共同交易日 {} 天（{}~{}）',
                len(data), len(common), common[0], common[-1])
    return data


# ---------------------------------------------------------------------------
# 权重与调度表
# ---------------------------------------------------------------------------

def _scaled(weights: Dict[str, float], cash_buffer: float) -> Dict[str, float]:
    """按现金缓冲等比缩放（保留刻意留出的档位差额），不把低仓位放大回满仓。"""
    return {code: value * (1.0 - cash_buffer) for code, value in weights.items()}


def _apply_rebalance(schedules: Dict[str, Dict[date, Tuple[str, float]]],
                     signal_day: date, old: Dict[str, float],
                     new: Dict[str, float]) -> None:
    """把一次权重变化写成调度表：加仓用 buy、减仓用 sell（目标权重口径）。"""
    for code in new:
        before = old.get(code, 0.0)
        after = new[code]
        if abs(after - before) < 1e-12:
            continue
        action = 'buy' if after > before else 'sell'
        schedules.setdefault(code, {})[signal_day] = (action, after)


def _next_day(calendar: List[date], day: date) -> date | None:
    for item in calendar:
        if item > day:
            return item
    return None


def _curve_points(frame: pl.DataFrame, start: date, end: date) -> List[List[Any]]:
    rows = frame.filter((pl.col('date') >= start) & (pl.col('date') <= end))
    return [[str(d), float(c)] for d, c in
            zip(rows['date'].to_list(), rows['close'].to_list(), strict=True)]


# ---------------------------------------------------------------------------
# asset_switch
# ---------------------------------------------------------------------------

def _switch_weights(rule: Dict[str, Any], state: str) -> Dict[str, float]:
    legs = rule['legs']
    codes = [leg['code'] for leg in legs]
    sw = rule['switch']
    if rule['positions']['mode'] == 'all_in':
        target = sw['numerator'] if state == 'numerator' else sw['denominator']
        return {code: 1.0 if code == target else 0.0 for code in codes}
    for item in rule['positions']['states']:
        if item['state'] == state:
            return dict(item['weights'])
    raise PlanError(f'positions 缺少状态 {state!r} 的权重定义')


def build_switch_plan(rule: Dict[str, Any]) -> Plan:
    start = date.fromisoformat(rule['period']['start'])
    end = date.fromisoformat(rule['period']['end'])
    warmup = start - timedelta(days=WARMUP_DAYS)
    codes = [leg['code'] for leg in rule['legs']]
    data = load_legs(codes, warmup, end)

    close = {code: {d: float(c) for d, c in
                    zip(df['date'].to_list(), df['close'].to_list(), strict=True)}
             for code, df in data.items()}
    common = sorted(set.intersection(*[set(series) for series in close.values()]))
    sw = rule['switch']
    window = int(sw['window'])
    if len(common) < window + 1:
        raise PlanError(
            f'比值中枢需要至少 {window + 1} 个共同交易日，'
            f'区间内只有 {len(common)} 个；请把回测起点后移或缩短窗口')

    denominator = 0.0
    ratios: List[float] = []
    for day in common:
        numerator = close[sw['numerator']][day]
        denominator = close[sw['denominator']][day]
        ratios.append(numerator / denominator if denominator > 0 else math.nan)

    state = sw['initial']
    cash = rule['portfolio']['cash_buffer']
    schedules: Dict[str, Dict[date, Tuple[str, float]]] = {}
    markers: List[Dict[str, Any]] = []
    started = False
    last_change_idx = -1

    for i, day in enumerate(common):
        if i + 1 < window or day < start:
            continue
        center = sum(ratios[i - window + 1:i + 1]) / window
        if not math.isfinite(center) or center <= 0:
            continue
        ratio = ratios[i]
        if not started:
            weights = _scaled(_switch_weights(rule, state), cash)
            _apply_rebalance(schedules, day, dict.fromkeys(codes, 0.0), weights)
            exec_day = _next_day(common, day) or day
            markers.append({
                'signal_date': str(day), 'date': str(exec_day), 'state': state,
                'position': 1.0, 'weights': weights, 'reason': 'initial',
            })
            started = True
            last_change_idx = i
            continue
        if i - last_change_idx < int(sw['min_hold_days']):
            continue
        target = None
        if state == 'denominator' and ratio > center * (1.0 + sw['entry']):
            target = 'numerator'
        elif state == 'numerator' and ratio < center * (1.0 - sw['exit']):
            target = 'denominator'
        if target is None or target == state:
            continue
        old_weights = _scaled(_switch_weights(rule, state), cash)
        new_weights = _scaled(_switch_weights(rule, target), cash)
        _apply_rebalance(schedules, day, old_weights, new_weights)
        exec_day = _next_day(common, day) or day
        markers.append({
            'signal_date': str(day), 'date': str(exec_day),
            'state': target, 'from': state, 'position': 1.0,
            'weights': new_weights, 'ratio': round(ratio / center - 1.0, 6),
        })
        state = target
        last_change_idx = i

    if not started:
        raise PlanError(
            f'区间 {start}~{end} 内没有可评估的比值中枢（窗口 {window} 日）')

    labels = {leg['code']: leg['label'] for leg in rule['legs']}
    leg_curves = [{'code': code, 'label': labels[code],
                   'points': _curve_points(data[code], start, end)}
                  for code in codes]
    note = (f'共同交易日 {len(common)} 天，窗口 {window} 日；'
            f'切换 {len([m for m in markers if m.get("reason") != "initial"])} 次')
    return Plan(data=data, schedules=schedules, markers=markers,
                leg_curves=leg_curves, note=note)


# ---------------------------------------------------------------------------
# timing
# ---------------------------------------------------------------------------

def _signal_series(rule: Dict[str, Any], prices: List[float]) -> List[float | None]:
    kind = rule['signal']['kind']
    window = int(rule['signal']['window'])
    out: List[float | None] = [None] * len(prices)
    for i, price in enumerate(prices):
        if i + 1 < window:
            continue
        past = prices[i - window + 1:i + 1]
        if kind == 'ma_ratio':
            mean = sum(past) / window
            out[i] = price / mean - 1.0 if mean > 0 else None
        elif kind == 'momentum':
            base = past[0]
            out[i] = price / base - 1.0 if base > 0 else None
        else:  # drawdown
            peak = max(past)
            out[i] = price / peak - 1.0 if peak > 0 else None
    return out


def _raw_level(signal: float, levels: List[Dict[str, float]]) -> int:
    index = -1
    for i, level in enumerate(levels):
        if signal >= level['threshold']:
            index = i
        else:
            break
    return index


def _step_level(index: int, signal: float, levels: List[Dict[str, float]],
                hysteresis: float) -> int:
    if index + 1 < len(levels) and signal >= levels[index + 1]['threshold'] + hysteresis:
        return index + 1
    if index >= 0 and signal <= levels[index]['threshold'] - hysteresis:
        return index - 1
    return index


def build_timing_plan(rule: Dict[str, Any]) -> Plan:
    start = date.fromisoformat(rule['period']['start'])
    end = date.fromisoformat(rule['period']['end'])
    warmup = start - timedelta(days=WARMUP_DAYS)
    codes = list(rule['target']['codes'])
    data = load_legs(codes, warmup, end)
    common = sorted(set.intersection(*[set(df['date'].to_list()) for df in data.values()]))

    window = int(rule['signal']['window'])
    if len(common) < window + 1:
        raise PlanError(
            f'{rule["signal"]["kind"]} 信号需要至少 {window + 1} 个共同交易日，'
            f'区间内只有 {len(common)} 个；请把回测起点后移或缩短窗口')

    if len(codes) == 1:
        series = {d: float(c) for d, c in
                  zip(data[codes[0]]['date'].to_list(),
                      data[codes[0]]['close'].to_list(), strict=True)}
        prices = [series[d] for d in common]
    else:
        base = {code: float(data[code].filter(pl.col('date') == common[0])
                            ['close'][0]) for code in codes}
        lookup = {code: {d: float(c) for d, c in
                         zip(df['date'].to_list(), df['close'].to_list(), strict=True)}
                  for code, df in data.items()}
        prices = [sum(lookup[code][d] / base[code] for code in codes) / len(codes)
                  for d in common]

    signals = _signal_series(rule, prices)
    levels = rule['levels']
    hysteresis = float(rule['hysteresis'])
    cash = rule['portfolio']['cash_buffer']
    per_code = {code: (1.0 if len(codes) == 1 else 1.0 / len(codes)) for code in codes}

    schedules: Dict[str, Dict[date, Tuple[str, float]]] = {}
    markers: List[Dict[str, Any]] = []
    level = None
    current_position = 0.0

    for i, day in enumerate(common):
        if day < start or signals[i] is None:
            continue
        signal = signals[i]  # type: ignore[assignment]
        if level is None:
            level = _raw_level(signal, levels)
        else:
            level = _step_level(level, signal, levels, hysteresis)
        position = levels[level]['position'] if level >= 0 else 0.0
        if abs(position - current_position) < 1e-12:
            continue
        weights = _scaled({code: per_code[code] * position for code in codes}, cash)
        old_weights = _scaled({code: per_code[code] * current_position for code in codes},
                              cash)
        _apply_rebalance(schedules, day, old_weights, weights)
        exec_day = _next_day(common, day) or day
        markers.append({
            'signal_date': str(day), 'date': str(exec_day), 'level': level,
            'position': position, 'weights': weights,
            'signal': round(signal, 6),
        })
        current_position = position

    if not markers:
        raise PlanError(
            f'区间 {start}~{end} 内信号始终低于最低档（{levels[0]["threshold"]:g}），'
            '没有产生任何调仓；请调整档位阈值或回测区间')

    labels = {code: code for code in codes}
    leg_curves = [{'code': code, 'label': labels[code],
                   'points': _curve_points(data[code], start, end)}
                  for code in codes]
    note = (f'共同交易日 {len(common)} 天，信号窗口 {window} 日；'
            f'调仓 {len(markers)} 次，期末档位 {level}')
    return Plan(data=data, schedules=schedules, markers=markers,
                leg_curves=leg_curves, note=note)


def build_plan(rule: Dict[str, Any]) -> Plan:
    builders = {'asset_switch': build_switch_plan, 'timing': build_timing_plan}
    try:
        return builders[rule['rule_type']](rule)
    except KeyError as exc:
        raise PlanError(f'rule_type={rule["rule_type"]!r} 没有对应的执行器') from exc
