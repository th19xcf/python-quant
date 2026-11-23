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

import bisect
import math
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import polars as pl
from loguru import logger

from .markers import sides_from_weights

ROOT = Path(__file__).resolve().parents[3]
QUOTE_ROOT = ROOT / 'data/local_store/quotes'
VALUATION_PATH = ROOT / 'data/local_store/meta/index_valuation/index_valuation.parquet'
QUOTE_DOMAINS = ('fund', 'index')
#: 预热：覆盖最长 250 日窗口 + 节假日冗余
WARMUP_DAYS = 400


class PlanError(ValueError):
    """行情缺失或历史不足，无法构造调度表。"""


@dataclass
class Plan:
    """一次切换/择时/定投运行的完整输入：引擎行情、调度表、图线数据。"""

    data: Dict[str, pl.DataFrame]
    schedules: Dict[str, Dict[date, Any]]
    markers: List[Dict[str, Any]]
    leg_curves: List[Dict[str, Any]]
    note: str
    #: 定投补仓的当日信号（最后一根 bar 收盘判定，不写调度）；其它规则为 None
    daily: Dict[str, Any] | None = None
    #: 估值分位择时最后一期信号（日期为所用估值数据的实际日期），供界面只读展示
    signal_tail: Dict[str, Any] | None = None


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
    """前复权价 + 原始价：四类规则统一按前复权价回测（唯一路径）。

    复权口径（唯一实现，不另造第二套换算）：

    - 换算：``复权价 = 原始价 × qfq_factor``，与 ``src/advice/data.py`` 的
      「原始价 × 因子」取数约定一致，从本地行情 parquet 的 ``qfq_factor`` 列取
      因子；该列缺失时才退回 ``hfq_factor``，两者都没有则整列按 1.0 处理；
      因子为空值同样按 1.0（不插值、不猜）。
    - 基准日：前复权因子按「行 t 的因子 = ∏{f_e | 除权日 e > t}」定义
      （见 ``src/data/adj_factor_calculator.py``），即最新交易日的因子为 1.0、
      价格不缩放，历史价按其后发生的分红与送转等比下调。因此基准日就是行情里
      的最后一个交易日，复权序列随数据更新会整体平移，回测内部始终自洽。
    - 执行口径：只输出前复权价，不再附带 ``raw_open`` / ``raw_close``。撮合与
      盯市因此落在前复权价空间，除权账本保持关闭（分红/送转已内含在价格里，
      再记一遍即重复计入）；四类规则共用这一条路径，没有旧分支。
    - 已知限制：本地约 65.6% 的基金复权因子恒为 1（无分红/折算记录），这类标的
      走前复权等于没复权，收益仍可能低估；这条限制没有修复，只做如实标注。
    """
    frame = frame.with_columns(pl.col('date').cast(pl.Date, strict=False))
    factor: str | None = None
    for column in ('qfq_factor', 'hfq_factor'):
        if column in frame.columns:
            factor = column
            break
    if factor is None:
        frame = frame.with_columns(pl.lit(1.0).alias('adj_factor'))
    else:
        frame = frame.with_columns(
            (pl.col(factor).cast(pl.Float64, strict=False).fill_null(1.0))
            .alias('adj_factor'))
    columns = [
        pl.col('date'),
        (pl.col('open') * pl.col('adj_factor')).alias('open'),
        (pl.col('high') * pl.col('adj_factor')).alias('high'),
        (pl.col('low') * pl.col('adj_factor')).alias('low'),
        (pl.col('close') * pl.col('adj_factor')).alias('close'),
        pl.col('volume').cast(pl.Float64, strict=False).alias('volume'),
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
        # 换腿方向按目标权重的净变化：卖出旧腿记卖、买入新腿记买，两侧各一条
        sides: list[str | None] = list(sides_from_weights(old_weights, new_weights)) or [None]
        for side in sides:
            marker = {
                'signal_date': str(day), 'date': str(exec_day),
                'state': target, 'from': state, 'position': 1.0,
                'weights': new_weights, 'ratio': round(ratio / center - 1.0, 6),
            }
            if side is not None:
                marker['direction'] = side
            markers.append(marker)
        state = target
        last_change_idx = i

    if not started:
        raise PlanError(
            f'区间 {start}~{end} 内没有可评估的比值中枢（窗口 {window} 日）')

    labels = {leg['code']: leg['label'] for leg in rule['legs']}
    leg_curves = [{'code': code, 'label': labels[code],
                   'points': _curve_points(data[code], start, end)}
                  for code in codes]
    # 一次换腿会拆买卖两条标记，切换次数按信号日去重
    switches = len({m['signal_date'] for m in markers
                    if m.get('reason') != 'initial'})
    note = (f'共同交易日 {len(common)} 天，窗口 {window} 日；'
            f'切换 {switches} 次')
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


def _index_pct_series(rule: Dict[str, Any], days: List[date],
                      *, with_dates: bool = False):
    """指数估值分位：月末序列滚动 ``window_months`` 期，非月末日取最近一期。

    分位 = 窗口内 ``<= 当前值`` 的期数占比（与 ``tools/dividend_official_probe.py`` 同口径）；
    有效样本少于 ``min_months`` 时返回 ``None``，不降级、不外推。
    ``with_dates=True`` 时一并返回每个交易日实际取用的估值日期。
    """
    spec = rule['signal']
    if not VALUATION_PATH.is_file():
        raise PlanError(f'指数估值表缺失：{VALUATION_PATH}；先运行 '
                        'tools/collect_index_valuation.py --apply')
    frame = pl.read_parquet(VALUATION_PATH,
                            columns=['index_code', 'trade_date', spec['metric']])
    series = frame.filter(pl.col('index_code') == spec['index_code']).sort('trade_date')
    if series.height == 0:
        raise PlanError(f'指数估值表没有 {spec["index_code"]} 的记录')
    dates = series['trade_date'].to_list()
    values = [None if v is None else float(v) for v in series[spec['metric']].to_list()]
    window = int(spec['window_months'])
    min_n = int(spec['min_months'])
    out: List[float | None] = []
    used: List[date | None] = []
    for day in days:
        i = bisect.bisect_right(dates, day) - 1
        if i < 0 or values[i] is None:
            out.append(None)
            used.append(None)
            continue
        hist = [v for v in values[max(0, i - window + 1):i + 1] if v is not None]
        current = values[i]
        assert current is not None  # 上方已对 None 当前值 continue
        out.append(sum(1 for v in hist if v <= current) / len(hist)
                   if len(hist) >= min_n else None)
        used.append(dates[i])
    return (out, used) if with_dates else out


def build_timing_plan(rule: Dict[str, Any]) -> Plan:
    start = date.fromisoformat(rule['period']['start'])
    end = date.fromisoformat(rule['period']['end'])
    warmup = start - timedelta(days=WARMUP_DAYS)
    codes = list(rule['target']['codes'])
    data = load_legs(codes, warmup, end)
    common = sorted(set.intersection(*[set(df['date'].to_list()) for df in data.values()]))

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

    used_dates: List[date | None] | None = None
    if rule['signal']['kind'] == 'index_pe_pct':
        signals, used_dates = _index_pct_series(rule, common, with_dates=True)
        window = int(rule['signal']['window_months'])
        window_text = f'估值分位窗口 {window} 个月'
    else:
        window = int(rule['signal']['window'])
        if len(common) < window + 1:
            raise PlanError(
                f'{rule["signal"]["kind"]} 信号需要至少 {window + 1} 个共同交易日，'
                f'区间内只有 {len(common)} 个；请把回测起点后移或缩短窗口')
        signals = _signal_series(rule, prices)
        window_text = f'信号窗口 {window} 日'

    levels = rule['levels']
    hysteresis = float(rule['hysteresis'])
    cash = rule['portfolio']['cash_buffer']
    per_code = {code: (1.0 if len(codes) == 1 else 1.0 / len(codes)) for code in codes}

    # 最后一期仍有效的估值分位：界面只读展示「当前分位」与估值数据实际日期
    signal_tail: Dict[str, Any] | None = None
    if used_dates is not None:
        last_i = next((i for i in range(len(signals) - 1, -1, -1)
                       if signals[i] is not None), None)
        if last_i is not None:
            signal_tail = {'date': str(used_dates[last_i]),
                           'pct': round(float(signals[last_i]), 6),
                           'threshold': float(levels[-1]['threshold'])}

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
    note = (f'共同交易日 {len(common)} 天，{window_text}；'
            f'调仓 {len(markers)} 次，期末档位 {level}')
    return Plan(data=data, schedules=schedules, markers=markers,
                leg_curves=leg_curves, note=note, signal_tail=signal_tail)


# ---------------------------------------------------------------------------
# dca_grid
# ---------------------------------------------------------------------------

def _dca_decision(close: float, *, held: int, fills: List[float],
                  ref: float | None, observing: bool, peak: float,
                  restart: bool, tranches: int, add_drop: float,
                  take_profit: float, trail: float) -> Dict[str, Any]:
    """按当日收盘价判定动作与观察期迁移（纯函数：不写调度、不改持仓）。

    ``signal`` 取 ``initial`` / ``restart`` / ``add`` / ``take_profit`` /
    ``observe`` / ``hold``：前两者待次日开盘建仓，``add`` 待次日开盘补 1 份，
    ``take_profit`` 待次日开盘清仓，``observe`` 表示当日收盘首次越过止盈阈值、
    进入观察期（不减仓），``hold`` 为无动作。判定顺序与逐日状态机一致：
    先判止盈回撤（触发即清仓），再看补仓，最后才落到观察期进入。
    """
    if restart:
        return {'signal': 'restart', 'observing': False, 'peak': 0.0,
                'avg_cost': None, 'return': None}
    if held == 0:
        return {'signal': 'initial', 'observing': False, 'peak': 0.0,
                'avg_cost': None, 'return': None}
    avg_cost = len(fills) / sum(1.0 / price for price in fills)
    ret = close / avg_cost - 1.0
    entered = False
    if not observing:
        if ret >= take_profit:
            observing, peak, entered = True, close, True
    else:
        peak = max(peak, close)
        if close <= peak * (1.0 - trail):
            return {'signal': 'take_profit', 'observing': True, 'peak': peak,
                    'avg_cost': avg_cost, 'return': ret}
    if held < tranches and ref is not None and close <= ref * (1.0 - add_drop):
        signal = 'add'
    elif entered:
        signal = 'observe'
    else:
        signal = 'hold'
    return {'signal': signal, 'observing': observing, 'peak': peak,
            'avg_cost': avg_cost, 'return': ret}


def _dca_daily_payload(day: date, close: float, decision: Dict[str, Any], *,
                       held: int, ref: float | None, tranches: int,
                       add_drop: float, take_profit: float,
                       trail: float) -> Dict[str, Any]:
    """把最后一根 bar 的判定结果整理成「当日信号」：状态 + 下次触发价位。

    价位全部由现有规则参数推导，不引入新参数：下一次补仓价
    ``ref × (1 - add_drop_pct)``，观察期回撤触发价 ``peak × (1 - trail_drop_pct)``，
    进入观察期价 ``avg_cost × (1 + take_profit_pct)``。
    """
    signal = decision['signal']
    observing = bool(decision['observing'])
    avg_cost = decision['avg_cost']
    action = {'initial': 'buy', 'restart': 'buy', 'add': 'buy',
              'take_profit': 'sell'}.get(signal)
    if signal in ('initial', 'restart', 'take_profit'):
        next_add = None
    elif held < tranches and ref is not None:
        next_add = ref * (1.0 - add_drop)
    else:
        next_add = None
    observe_trigger = (avg_cost * (1.0 + take_profit)
                       if avg_cost is not None and not observing else None)
    trail_trigger = decision['peak'] * (1.0 - trail) if observing else None
    ret = decision['return']
    return {
        'date': str(day), 'close': round(close, 4),
        'state': signal, 'action': action,
        'tranches': held, 'tranches_total': tranches,
        'position': round(held / tranches, 6),
        'avg_cost': round(avg_cost, 4) if avg_cost is not None else None,
        'return': round(ret, 6) if ret is not None else None,
        'observing': observing,
        'peak': round(decision['peak'], 4) if observing else None,
        'next_add_price': round(next_add, 4) if next_add is not None else None,
        'observe_trigger_price': (round(observe_trigger, 4)
                                  if observe_trigger is not None else None),
        'trail_trigger_price': (round(trail_trigger, 4)
                                if trail_trigger is not None else None),
    }


def build_dca_plan(rule: Dict[str, Any]) -> Plan:
    """定投补仓 + 止盈回撤清仓的调度表（逐日状态机，不撮合、不算净值）。

    口径：
    - 资金等分 ``tranches`` 份，每份 = 「当时剩余现金 ÷ 剩余份数」，
      因而首次建仓与清仓重启时各占当期资金的 1/N；
    - 参考价取最近一次买入的成交价（信号日次一交易日开盘），
      收盘较参考价再跌 ``add_drop_pct`` 补 1 份，直到满仓；
    - 持仓收益率 = 收盘 / 平均成本 - 1，平均成本按各份等额买入价加权
      （即买入价的调和平均）；
    - 收益率超过 ``take_profit_pct`` 进入观察期并记录此后收盘最高点，
      自最高点回撤 ``trail_drop_pct`` 一次性清仓，次日起从 1 份重启。

    最后一根 bar 没有「次日开盘」可成交，因此不写调度、不产生成交，只用同一
    套判定算出当日信号（是否可补仓 / 是否进入观察期 / 是否触发止盈）与下次
    触发价位，供 ``daily_signal`` 直接回答「今天该做什么」。
    """
    start = date.fromisoformat(rule['period']['start'])
    end = date.fromisoformat(rule['period']['end'])
    warmup = start - timedelta(days=WARMUP_DAYS)
    target = rule['target']
    code = target['code']
    data = load_legs([code], warmup, end)
    frame = data[code]
    days = frame['date'].to_list()
    closes = [float(c) for c in frame['close'].to_list()]
    opens = [float(o) if o is not None and float(o) > 0 else close
             for o, close in zip(frame['open'].to_list(), closes, strict=True)]

    plan_cfg = rule['plan']
    tranches = int(plan_cfg['tranches'])
    add_drop = float(plan_cfg['add_drop_pct'])
    take_profit = float(plan_cfg['take_profit_pct'])
    trail = float(plan_cfg['trail_drop_pct'])

    schedules: Dict[str, Dict[date, Any]] = {}
    markers: List[Dict[str, Any]] = []
    held = 0
    fills: List[float] = []
    ref: float | None = None
    observing = False
    peak = 0.0
    restart = False
    adds = 0
    exits = 0

    def _buy(index: int, reason: str) -> None:
        """写一条买入调度：fraction = 1/剩余份数，成交价按次日开盘预估。"""
        nonlocal held, ref
        schedules.setdefault(code, {})[days[index]] = {
            'action': 'buy', 'fraction': 1.0 / (tranches - held)}
        fill = opens[index + 1]
        held += 1
        fills.append(fill)
        ref = fill
        markers.append({
            'date': str(days[index + 1]), 'signal_date': str(days[index]),
            'reason': reason, 'tranches': held,
            'position': round(held / tranches, 6), 'price': round(fill, 4),
        })

    for i, day in enumerate(days):
        if day < start or i + 1 >= len(days):
            continue
        close = closes[i]
        decision = _dca_decision(
            close, held=held, fills=fills, ref=ref, observing=observing,
            peak=peak, restart=restart, tranches=tranches, add_drop=add_drop,
            take_profit=take_profit, trail=trail)
        signal = decision['signal']
        if signal == 'restart':
            _buy(i, 'restart')
            restart = False
            continue
        if signal == 'initial':
            _buy(i, 'initial')
            continue
        observing, peak = decision['observing'], decision['peak']
        if signal == 'take_profit':
            schedules.setdefault(code, {})[day] = {
                'action': 'sell', 'fraction': 1.0}
            markers.append({
                'date': str(days[i + 1]), 'signal_date': str(day),
                'reason': 'take_profit', 'tranches': held,
                'position': round(held / tranches, 6),
                'price': round(opens[i + 1], 4),
                'peak': round(peak, 4), 'return': round(decision['return'], 6),
                'drawdown_from_peak': round(close / peak - 1.0, 6),
            })
            exits += 1
            held, fills, ref = 0, [], None
            observing, peak = False, 0.0
            restart = True
            continue
        if signal == 'add':
            _buy(i, 'add')
            adds += 1

    daily: Dict[str, Any] | None = None
    if days and days[-1] >= start:
        close = closes[-1]
        decision = _dca_decision(
            close, held=held, fills=fills, ref=ref, observing=observing,
            peak=peak, restart=restart, tranches=tranches, add_drop=add_drop,
            take_profit=take_profit, trail=trail)
        daily = _dca_daily_payload(
            days[-1], close, decision, held=held, ref=ref, tranches=tranches,
            add_drop=add_drop, take_profit=take_profit, trail=trail)

    if not markers:
        raise PlanError(
            f'区间 {start}~{end} 内没有产生任何定投信号；请检查区间与行情覆盖')
    if held == tranches:
        logger.info('定投补仓：区间内已满仓 {} 份', tranches)

    leg_curves = [{'code': code, 'label': target['label'],
                   'points': _curve_points(frame, start, end)}]
    note = (f'交易日 {len(days)} 天，{tranches} 份制；补仓 {adds} 次，'
            f'止盈清仓 {exits} 次，期末持有 {held} 份')
    return Plan(data=data, schedules=schedules, markers=markers,
                leg_curves=leg_curves, note=note, daily=daily)


def build_plan(rule: Dict[str, Any]) -> Plan:
    builders = {'asset_switch': build_switch_plan, 'timing': build_timing_plan,
                'dca_grid': build_dca_plan}
    try:
        return builders[rule['rule_type']](rule)
    except KeyError as exc:
        raise PlanError(f'rule_type={rule["rule_type"]!r} 没有对应的执行器') from exc
