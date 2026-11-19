"""策略规则的解析、校验、序列化与中文描述（支持三类规则）。

规则只允许由预定义字段与运算符组合，不做任何代码求值（不接 ``eval`` /
``exec``），也不接受用户自带字段名或表达式。

``rule_type`` 四类：

- ``stock_select``：条件链 → 排序 → 取前 N（选股）；
- ``asset_switch``：多标的强弱比切换（比值中枢、切换阈值、最短持有期）；
- ``timing``：单标的或组合的仓位择时（信号 → 档位 + 滞回 + 单日最多调一档）；
- ``dca_grid``：单标的定投补仓 + 止盈回撤清仓（份数、补仓跌幅、止盈阈值、
  止盈回撤幅度）。

未知类型、缺必填字段、类型不匹配的参数一律抛 :class:`RuleError` 并给出
人能看懂的错误，不静默兜底、不忽略多余键。
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from .fields import FIELDS, NON_NUMERIC_FIELDS, OPERATORS, QUANTILE_OPERATORS, FieldSpec

RULE_VERSION = 'strategy_lab/v1'
RULES_DIR = Path(__file__).resolve().parents[3] / 'config' / 'strategy_lab' / 'rules'

RULE_TYPES: Dict[str, str] = {
    'stock_select': '选股',
    'asset_switch': '强弱比切换',
    'timing': '仓位择时',
    'dca_grid': '定投补仓',
}

#: 与既有配方一致的默认成本口径（详见 docs/策略试算使用说明.md）
DEFAULT_COST: Dict[str, Any] = {
    'slippage_bps': 10.0,
    'stamp_tax_basis': 'fixed_0.1%',
    'commission_rate': 0.00025,
    'min_commission': 5.0,
    'impact_coef': 0.015,
}
DEFAULT_PORTFOLIO: Dict[str, Any] = {
    'initial_capital': 1e9,
    'lot_size': 100,
    'cash_buffer': 0.01,
    'pooled_cash': True,
}
#: 既有可选基准（本地指数行情齐全的宽基）
BENCHMARKS: Dict[str, str] = {'000300.SH': '沪深300'}
#: 月频与半年频（6/12 月最后一个交易日）
REBALANCE_FREQUENCIES = ('monthly', 'semi')
#: 最小版执行口径：信号日收盘生成，下一交易日开盘成交
EXECUTION_SPEC = {'price': 'next_open', 'delay_days': 1}
SIGNAL_KINDS = {'ma_ratio': '收盘价/均线', 'momentum': 'N日动量', 'drawdown': 'N日回撤'}

_MAX_CONDITIONS = 10
_MAX_TOP_N = 500
_MAX_WINDOW = 250

_COMMON_KEYS = {'rule_version', 'rule_type', 'name', 'description', 'data_policy',
                'period', 'benchmark', 'cost', 'portfolio'}
_TYPE_KEYS: Dict[str, set] = {
    'stock_select': _COMMON_KEYS | {'universe', 'conditions', 'sort', 'top_n',
                                    'rebalance'},
    'asset_switch': _COMMON_KEYS | {'legs', 'switch', 'positions', 'execution'},
    'timing': _COMMON_KEYS | {'target', 'signal', 'levels', 'hysteresis',
                              'max_step_per_day', 'execution'},
    'dca_grid': _COMMON_KEYS | {'target', 'plan', 'execution'},
}


class RuleError(ValueError):
    """规则非法：类型未知、字段未开放、运算符不匹配、取值越界等。"""


def _reject_unknown(obj: Dict[str, Any], allowed: set, where: str) -> None:
    unknown = sorted(set(obj) - allowed)
    if unknown:
        raise RuleError(f'{where} 出现未知字段 {unknown}；允许字段：{", ".join(sorted(allowed))}')


def _require(obj: Dict[str, Any], key: str, where: str) -> Any:
    if key not in obj:
        raise RuleError(f'{where} 缺少必填字段 {key!r}')
    return obj[key]


def _field(key: Any, where: str) -> FieldSpec:
    if not isinstance(key, str) or key not in FIELDS:
        raise RuleError(
            f'{where} 字段 {key!r} 未开放；可用字段：{", ".join(sorted(FIELDS))}')
    return FIELDS[key]


def _code(value: Any, where: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r'\d{6}\.(SH|SZ|BJ)', value):
        raise RuleError(f'{where} 必须是 6 位代码加交易所后缀（如 510300.SH），'
                        f'收到 {value!r}')
    return value


def _iso(value: Any, where: str) -> str:
    if not isinstance(value, str):
        raise RuleError(f'{where} 必须是 YYYY-MM-DD 字符串，收到 {value!r}')
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise RuleError(f'{where} 不是合法日期：{value!r}') from exc


def _number(value: Any, where: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuleError(f'{where} 必须是数字，收到 {value!r}')
    return float(value)


def _int(value: Any, where: str, low: int, high: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not (low <= value <= high):
        raise RuleError(f'{where} 必须是 {low}~{high} 的整数，收到 {value!r}')
    return value


def _window(value: Any, where: str) -> int:
    return _int(value, where, 2, _MAX_WINDOW)


# ---------------------------------------------------------------------------
# 通用块
# ---------------------------------------------------------------------------

def _validate_conditions(raw: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        raise RuleError('conditions 必须是数组')
    if len(raw) > _MAX_CONDITIONS:
        raise RuleError(f'条件最多 {_MAX_CONDITIONS} 条，收到 {len(raw)}')
    return [_condition(x, i) for i, x in enumerate(raw)]


def _condition(item: Any, index: int) -> Dict[str, Any]:
    where = f'conditions[{index}]'
    if not isinstance(item, dict):
        raise RuleError(f'{where} 必须是对象')
    _reject_unknown(item, {'field', 'op', 'operand'}, where)
    spec = _field(item.get('field'), f'{where}.field')
    if spec.key in NON_NUMERIC_FIELDS:
        raise RuleError(f'{where}.field 字段 {spec.key!r} 只用于分组，不能作为条件字段')
    op = item.get('op')
    if op not in OPERATORS:
        raise RuleError(f"{where}.op 必须是 {list(OPERATORS)} 之一，收到 {op!r}")

    operand = item.get('operand')
    if not isinstance(operand, dict):
        raise RuleError(f'{where}.operand 必须是对象')
    kind = operand.get('type')
    if kind == 'value':
        _reject_unknown(operand, {'type', 'value', 'low', 'high'}, f'{where}.operand')
        if op == 'between':
            low = _number(operand.get('low'), f'{where}.operand.low')
            high = _number(operand.get('high'), f'{where}.operand.high')
            if low > high:
                raise RuleError(f'{where} 区间下限 {low} 大于上限 {high}')
            out = {'type': 'value', 'low': low, 'high': high}
        else:
            out = {'type': 'value',
                   'value': _number(operand.get('value'), f'{where}.operand.value')}
    elif kind == 'quantile':
        _reject_unknown(operand, {'type', 'q', 'group'}, f'{where}.operand')
        if op not in QUANTILE_OPERATORS:
            raise RuleError(
                f'{where} 运算符 {op!r} 不能与分位组合；分位只允许 {list(QUANTILE_OPERATORS)}')
        q = _number(operand.get('q'), f'{where}.operand.q')
        if not (0.0 < q < 1.0):
            raise RuleError(f'{where} 分位 q={q} 越界，必须在 (0, 1) 内')
        out = {'type': 'quantile', 'q': q}
        if 'group' in operand:
            group = operand['group']
            if group != 'industry':
                raise RuleError(f"{where}.operand.group 只支持 'industry'，收到 {group!r}")
            out['group'] = group
    else:
        raise RuleError(f"{where}.operand.type 必须是 'value' 或 'quantile'，收到 {kind!r}")
    return {'field': spec.key, 'op': op, 'operand': out}


def _validate_sort(raw: Any) -> List[Dict[str, Any]]:
    """排序段：兼容单对象（等价一段）与 1~2 段数组，统一规范化为数组。

    非末段可带 ``keep_pct``（在当前保留池内按该字段保留前 x%），末段按该字段
    排序后取 ``top_n``；非数值字段（如 ``industry``）不能用于排序。
    """
    items = raw if isinstance(raw, list) else [raw]
    if not items or len(items) > 2:
        raise RuleError(f'sort 必须是排序对象或 1~2 段数组，收到 {raw!r}')
    stages: List[Dict[str, Any]] = []
    for i, item in enumerate(items):
        where = f'sort[{i}]' if isinstance(raw, list) else 'sort'
        if not isinstance(item, dict):
            raise RuleError(f'{where} 必须是对象（排序字段与方向）')
        _reject_unknown(item, {'field', 'direction', 'keep_pct'}, where)
        spec = _field(_require(item, 'field', where), f'{where}.field')
        if spec.key in NON_NUMERIC_FIELDS:
            raise RuleError(f'{where}.field 字段 {spec.key!r} 只用于分组，不能用于排序')
        direction = _require(item, 'direction', where)
        if direction not in ('asc', 'desc'):
            raise RuleError(f"{where}.direction 必须是 'asc' 或 'desc'，收到 {direction!r}")
        stage: Dict[str, Any] = {'field': spec.key, 'direction': direction}
        if 'keep_pct' in item:
            if i == len(items) - 1:
                raise RuleError(f'{where}.keep_pct 只能用于非末段（末段按排序取前 N）')
            keep = _number(item['keep_pct'], f'{where}.keep_pct')
            if not (0.0 < keep <= 1.0):
                raise RuleError(f'{where}.keep_pct 必须在 (0, 1] 内，收到 {keep}')
            stage['keep_pct'] = keep
        stages.append(stage)
    return stages


def _validate_rebalance(raw: Any) -> Dict[str, str]:
    if not isinstance(raw, dict):
        raise RuleError('rebalance 必须是对象')
    _reject_unknown(raw, {'frequency'}, 'rebalance')
    freq = raw.get('frequency')
    if freq not in REBALANCE_FREQUENCIES:
        raise RuleError(f'只支持月频与半年频（{list(REBALANCE_FREQUENCIES)}），收到 {freq!r}')
    return {'frequency': freq}


def _validate_period(raw: Any) -> Dict[str, str]:
    if not isinstance(raw, dict):
        raise RuleError('period 必须是对象')
    _reject_unknown(raw, {'start', 'end'}, 'period')
    start = _iso(_require(raw, 'start', 'period'), 'period.start')
    end = _iso(_require(raw, 'end', 'period'), 'period.end')
    if start > end:
        raise RuleError(f'period 起始 {start} 晚于结束 {end}')
    return {'start': start, 'end': end}


def _validate_cost(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise RuleError('cost 必须是对象')
    _reject_unknown(raw, set(DEFAULT_COST), 'cost')
    resolved = dict(DEFAULT_COST)
    resolved.update({k: _number(v, f'cost.{k}') for k, v in raw.items()
                     if k != 'stamp_tax_basis'})
    basis = raw.get('stamp_tax_basis', DEFAULT_COST['stamp_tax_basis'])
    if basis not in ('fixed_0.1%', 'fixed_0.0%', 'historical'):
        raise RuleError(f"cost.stamp_tax_basis 必须是 'fixed_0.1%'、'fixed_0.0%' 或 "
                        f"'historical'，收到 {basis!r}")
    resolved['stamp_tax_basis'] = basis
    for key in ('slippage_bps', 'commission_rate', 'min_commission', 'impact_coef'):
        if resolved[key] < 0:
            raise RuleError(f'cost.{key} 不能为负数，收到 {resolved[key]}')
    return resolved


def _validate_portfolio(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise RuleError('portfolio 必须是对象')
    _reject_unknown(raw, set(DEFAULT_PORTFOLIO), 'portfolio')
    capital = _number(raw.get('initial_capital', DEFAULT_PORTFOLIO['initial_capital']),
                      'portfolio.initial_capital')
    if capital <= 0:
        raise RuleError(f'portfolio.initial_capital 必须为正数，收到 {capital}')
    lot = raw.get('lot_size', DEFAULT_PORTFOLIO['lot_size'])
    if isinstance(lot, bool) or not isinstance(lot, int) or lot < 1:
        raise RuleError(f'portfolio.lot_size 必须是正整数，收到 {lot!r}')
    buffer = _number(raw.get('cash_buffer', DEFAULT_PORTFOLIO['cash_buffer']),
                     'portfolio.cash_buffer')
    if not (0 <= buffer < 1):
        raise RuleError(f'portfolio.cash_buffer 必须在 [0, 1) 内，收到 {buffer}')
    pooled = raw.get('pooled_cash', DEFAULT_PORTFOLIO['pooled_cash'])
    if not isinstance(pooled, bool):
        raise RuleError(f'portfolio.pooled_cash 必须是布尔值，收到 {pooled!r}')
    return {'initial_capital': capital, 'lot_size': lot,
            'cash_buffer': buffer, 'pooled_cash': pooled}


def _validate_execution(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise RuleError('execution 必须是对象')
    _reject_unknown(raw, set(EXECUTION_SPEC), 'execution')
    price = _require(raw, 'price', 'execution')
    if price != 'next_open':
        raise RuleError(f"execution.price 最小版只支持 'next_open'（下一交易日开盘），"
                        f'收到 {price!r}')
    delay = _require(raw, 'delay_days', 'execution')
    if delay != 1:
        raise RuleError(f'execution.delay_days 最小版固定为 1（T+1），收到 {delay!r}')
    return dict(EXECUTION_SPEC)


# ---------------------------------------------------------------------------
# stock_select
# ---------------------------------------------------------------------------

def _validate_stock_select(raw: Dict[str, Any]) -> Dict[str, Any]:
    universe = raw.get('universe', {'mode': 'all_market'})
    if not isinstance(universe, dict):
        raise RuleError('universe 必须是对象')
    _reject_unknown(universe, {'mode'}, 'universe')
    if universe.get('mode') != 'all_market':
        raise RuleError("最小版股票池只支持 {'mode': 'all_market'}（全市场洁净集）")
    top_n = raw.get('top_n')
    if isinstance(top_n, bool) or not isinstance(top_n, int) or not (1 <= top_n <= _MAX_TOP_N):
        raise RuleError(f'top_n 必须是 1~{_MAX_TOP_N} 的整数，收到 {top_n!r}')
    return {
        'universe': {'mode': 'all_market'},
        'conditions': _validate_conditions(raw.get('conditions', [])),
        'sort': _validate_sort(_require(raw, 'sort', '规则')),
        'top_n': top_n,
        'rebalance': _validate_rebalance(raw.get('rebalance', {'frequency': 'monthly'})),
    }


# ---------------------------------------------------------------------------
# asset_switch
# ---------------------------------------------------------------------------

def _validate_legs(raw: Any) -> List[Dict[str, str]]:
    if not isinstance(raw, list) or len(raw) < 2:
        raise RuleError(f'legs 必须是至少 2 个标的的数组，收到 {raw!r}')
    legs: List[Dict[str, str]] = []
    for i, item in enumerate(raw):
        where = f'legs[{i}]'
        if not isinstance(item, dict):
            raise RuleError(f'{where} 必须是对象')
        _reject_unknown(item, {'code', 'label'}, where)
        code = _code(_require(item, 'code', where), f'{where}.code')
        label = item.get('label', code)
        if not isinstance(label, str) or not label.strip() or len(label) > 20:
            raise RuleError(f'{where}.label 必须是 1~20 字字符串，收到 {label!r}')
        legs.append({'code': code, 'label': label.strip()})
    codes = [leg['code'] for leg in legs]
    if len(set(codes)) != len(codes):
        raise RuleError(f'legs 代码重复：{codes}')
    return legs


def _validate_switch_block(raw: Any, legs: List[Dict[str, str]]) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise RuleError('switch 必须是对象（比值中枢与切换阈值）')
    _reject_unknown(raw, {'numerator', 'denominator', 'window', 'entry', 'exit',
                          'min_hold_days', 'initial'}, 'switch')
    codes = {leg['code'] for leg in legs}
    numerator = _code(_require(raw, 'numerator', 'switch'), 'switch.numerator')
    denominator = _code(_require(raw, 'denominator', 'switch'), 'switch.denominator')
    if numerator == denominator:
        raise RuleError('switch.numerator 与 switch.denominator 不能相同')
    missing = sorted({numerator, denominator} - codes)
    if missing:
        raise RuleError(f'switch 使用的代码不在 legs 中：{missing}')
    window = _window(_require(raw, 'window', 'switch'), 'switch.window')
    entry = _number(_require(raw, 'entry', 'switch'), 'switch.entry')
    exit_ = _number(_require(raw, 'exit', 'switch'), 'switch.exit')
    if not (0 < entry < 1) or not (0 < exit_ < 1):
        raise RuleError(f'switch.entry/switch.exit 必须在 (0, 1) 内，收到 {entry} / {exit_}')
    min_hold = _int(_require(raw, 'min_hold_days', 'switch'), 'switch.min_hold_days', 0, 250)
    initial = _require(raw, 'initial', 'switch')
    if initial not in ('numerator', 'denominator'):
        raise RuleError(f"switch.initial 必须是 'numerator' 或 'denominator'，收到 {initial!r}")
    return {'numerator': numerator, 'denominator': denominator, 'window': window,
            'entry': entry, 'exit': exit_, 'min_hold_days': min_hold, 'initial': initial}


def _validate_positions(raw: Any, legs: List[Dict[str, str]]) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise RuleError('positions 必须是对象（all_in 或 gears）')
    mode = _require(raw, 'mode', 'positions')
    codes = [leg['code'] for leg in legs]
    if mode == 'all_in':
        _reject_unknown(raw, {'mode'}, 'positions')
        return {'mode': 'all_in'}
    if mode != 'gears':
        raise RuleError(f"positions.mode 必须是 'all_in' 或 'gears'，收到 {mode!r}")
    _reject_unknown(raw, {'mode', 'states'}, 'positions')
    states = _require(raw, 'states', 'positions')
    if not isinstance(states, list) or not states:
        raise RuleError('positions.states 必须是非空数组')
    parsed = []
    seen = set()
    for i, item in enumerate(states):
        where = f'positions.states[{i}]'
        if not isinstance(item, dict):
            raise RuleError(f'{where} 必须是对象')
        _reject_unknown(item, {'state', 'weights'}, where)
        state = _require(item, 'state', where)
        if state not in ('numerator', 'denominator'):
            raise RuleError(f"{where}.state 必须是 'numerator' 或 'denominator'，"
                            f'收到 {state!r}')
        if state in seen:
            raise RuleError(f'{where}.state 重复：{state!r}')
        seen.add(state)
        weights = _require(item, 'weights', where)
        if not isinstance(weights, dict):
            raise RuleError(f'{where}.weights 必须是代码到权重的对象')
        unknown = sorted(set(weights) - set(codes))
        if unknown:
            raise RuleError(f'{where}.weights 出现 legs 之外的代码：{unknown}')
        missing = sorted(set(codes) - set(weights))
        if missing:
            raise RuleError(f'{where}.weights 缺少 legs 代码：{missing}')
        clean = {}
        for code in codes:
            w = _number(weights[code], f'{where}.weights.{code}')
            if not (0 <= w <= 1):
                raise RuleError(f'{where}.weights.{code} 必须在 [0, 1] 内，收到 {w}')
            clean[code] = w
        if sum(clean.values()) > 1.0 + 1e-9:
            raise RuleError(f'{where}.weights 合计超过 1：{sum(clean.values()):.4f}')
        parsed.append({'state': state, 'weights': clean})
    return {'mode': 'gears', 'states': parsed}


def _validate_asset_switch(raw: Dict[str, Any]) -> Dict[str, Any]:
    legs = _validate_legs(_require(raw, 'legs', '规则'))
    return {
        'legs': legs,
        'switch': _validate_switch_block(_require(raw, 'switch', '规则'), legs),
        'positions': _validate_positions(_require(raw, 'positions', '规则'), legs),
        'execution': _validate_execution(_require(raw, 'execution', '规则')),
    }


# ---------------------------------------------------------------------------
# timing
# ---------------------------------------------------------------------------

def _validate_target(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise RuleError('target 必须是对象（单标的或组合）')
    mode = _require(raw, 'mode', 'target')
    if mode == 'single':
        _reject_unknown(raw, {'mode', 'code'}, 'target')
        return {'mode': 'single', 'codes': [_code(_require(raw, 'code', 'target'),
                                                  'target.code')]}
    if mode == 'portfolio':
        _reject_unknown(raw, {'mode', 'codes'}, 'target')
        codes = _require(raw, 'codes', 'target')
        if not isinstance(codes, list) or not codes:
            raise RuleError('target.codes 必须是非空数组')
        clean = [_code(c, f'target.codes[{i}]') for i, c in enumerate(codes)]
        if len(set(clean)) != len(clean):
            raise RuleError(f'target.codes 代码重复：{clean}')
        return {'mode': 'portfolio', 'codes': clean}
    raise RuleError(f"target.mode 必须是 'single' 或 'portfolio'，收到 {mode!r}")


def _validate_signal(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise RuleError('signal 必须是对象')
    _reject_unknown(raw, {'kind', 'window'}, 'signal')
    kind = _require(raw, 'kind', 'signal')
    if kind not in SIGNAL_KINDS:
        raise RuleError(f'signal.kind 必须是 {list(SIGNAL_KINDS)} 之一，收到 {kind!r}')
    return {'kind': kind, 'window': _window(_require(raw, 'window', 'signal'),
                                            'signal.window')}


def _validate_levels(raw: Any) -> List[Dict[str, float]]:
    if not isinstance(raw, list) or len(raw) < 2:
        raise RuleError('levels 必须是至少 2 档的数组')
    parsed = []
    last_threshold = None
    for i, item in enumerate(raw):
        where = f'levels[{i}]'
        if not isinstance(item, dict):
            raise RuleError(f'{where} 必须是对象')
        _reject_unknown(item, {'threshold', 'position'}, where)
        threshold = _number(_require(item, 'threshold', where), f'{where}.threshold')
        position = _number(_require(item, 'position', where), f'{where}.position')
        if not (0 <= position <= 1):
            raise RuleError(f'{where}.position 必须在 [0, 1] 内，收到 {position}')
        if last_threshold is not None and threshold <= last_threshold:
            raise RuleError(f'{where}.threshold 必须严格递增（上一档 {last_threshold}，'
                            f'当前 {threshold}）')
        if parsed and position < parsed[-1]['position']:
            raise RuleError(f'{where}.position 必须随阈值升高不降（上一档 '
                            f'{parsed[-1]["position"]:.0%}，当前 {position:.0%}）')
        last_threshold = threshold
        parsed.append({'threshold': threshold, 'position': position})
    return parsed


def _validate_timing(raw: Dict[str, Any]) -> Dict[str, Any]:
    hysteresis = _number(raw.get('hysteresis', 0.0), 'hysteresis')
    if not (0 <= hysteresis < 0.5):
        raise RuleError(f'hysteresis 必须在 [0, 0.5) 内，收到 {hysteresis}')
    step = _int(raw.get('max_step_per_day', 1), 'max_step_per_day', 1, 1)
    return {
        'target': _validate_target(_require(raw, 'target', '规则')),
        'signal': _validate_signal(_require(raw, 'signal', '规则')),
        'levels': _validate_levels(_require(raw, 'levels', '规则')),
        'hysteresis': hysteresis,
        'max_step_per_day': step,
        'execution': _validate_execution(_require(raw, 'execution', '规则')),
    }


# ---------------------------------------------------------------------------
# dca_grid（定投补仓 + 止盈回撤清仓）
# ---------------------------------------------------------------------------

def _validate_dca_target(raw: Any) -> Dict[str, str]:
    if not isinstance(raw, dict):
        raise RuleError('target 必须是对象（单标的）')
    _reject_unknown(raw, {'code', 'label'}, 'target')
    code = _code(_require(raw, 'code', 'target'), 'target.code')
    label = raw.get('label', code)
    if not isinstance(label, str) or not label.strip() or len(label) > 20:
        raise RuleError(f'target.label 必须是 1~20 字字符串，收到 {label!r}')
    return {'code': code, 'label': label.strip()}


def _validate_dca_plan(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise RuleError('plan 必须是对象（份数与阈值）')
    _reject_unknown(raw, {'tranches', 'add_drop_pct', 'take_profit_pct',
                          'trail_drop_pct'}, 'plan')
    tranches = _int(_require(raw, 'tranches', 'plan'), 'plan.tranches', 2, 50)
    add_drop = _number(_require(raw, 'add_drop_pct', 'plan'), 'plan.add_drop_pct')
    if not (0 < add_drop < 0.5):
        raise RuleError(f'plan.add_drop_pct 必须在 (0, 0.5) 内，收到 {add_drop}')
    take_profit = _number(_require(raw, 'take_profit_pct', 'plan'),
                          'plan.take_profit_pct')
    if not (0 < take_profit <= 2):
        raise RuleError(f'plan.take_profit_pct 必须在 (0, 2] 内，收到 {take_profit}')
    trail = _number(_require(raw, 'trail_drop_pct', 'plan'), 'plan.trail_drop_pct')
    if not (0 < trail < take_profit):
        raise RuleError(f'plan.trail_drop_pct 必须在 (0, plan.take_profit_pct) 内，'
                        f'收到 {trail}（止盈阈值 {take_profit}）')
    return {'tranches': tranches, 'add_drop_pct': add_drop,
            'take_profit_pct': take_profit, 'trail_drop_pct': trail}


def _validate_dca_grid(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'target': _validate_dca_target(_require(raw, 'target', '规则')),
        'plan': _validate_dca_plan(_require(raw, 'plan', '规则')),
        'execution': _validate_execution(_require(raw, 'execution', '规则')),
    }


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

def validate_rule(raw: Any) -> Dict[str, Any]:
    """校验并规范化规则；非法即抛 :class:`RuleError`（不静默忽略任何键）。"""
    if not isinstance(raw, dict):
        raise RuleError('规则顶层必须是 JSON 对象')
    rule_type = raw.get('rule_type')
    if rule_type is None:
        raise RuleError(f"缺少 rule_type，可选值：{list(RULE_TYPES)}")
    if rule_type not in RULE_TYPES:
        raise RuleError(f'未知 rule_type {rule_type!r}；可选值：{list(RULE_TYPES)}')
    _reject_unknown(raw, _TYPE_KEYS[rule_type], f'rule_type={rule_type} 规则')
    if raw.get('rule_version') != RULE_VERSION:
        raise RuleError(f"rule_version 必须是 '{RULE_VERSION}'，收到 {raw.get('rule_version')!r}")

    name = raw.get('name')
    if not isinstance(name, str) or not name.strip() or len(name.strip()) > 40:
        raise RuleError(f'name 必须是 1~40 字非空字符串，收到 {name!r}')
    if re.search(r'[\\/:*?"<>|]', name):
        raise RuleError(f'name 不能包含文件系统保留字符：{name!r}')

    description = raw.get('description', '')
    if not isinstance(description, str):
        raise RuleError('description 必须是字符串')
    data_policy = raw.get('data_policy', '')
    if not isinstance(data_policy, str):
        raise RuleError('data_policy 必须是字符串')

    benchmark = raw.get('benchmark', '000300.SH')
    if benchmark not in BENCHMARKS:
        raise RuleError(f'benchmark 必须是 {list(BENCHMARKS)} 之一，收到 {benchmark!r}')

    parsed: Dict[str, Any] = {
        'stock_select': _validate_stock_select,
        'asset_switch': _validate_asset_switch,
        'timing': _validate_timing,
        'dca_grid': _validate_dca_grid}[rule_type](raw)
    return {
        'rule_version': RULE_VERSION,
        'rule_type': rule_type,
        'name': name.strip(),
        'description': description,
        'data_policy': data_policy,
        'period': _validate_period(_require(raw, 'period', '规则')),
        'benchmark': benchmark,
        'cost': _validate_cost(raw.get('cost', DEFAULT_COST)),
        'portfolio': _validate_portfolio(raw.get('portfolio', DEFAULT_PORTFOLIO)),
        **parsed,
    }


def dump_rule(rule: Dict[str, Any]) -> bytes:
    """规范化 JSON 字节：键排序、两空格缩进、末尾换行。"""
    return (json.dumps(rule, ensure_ascii=False, indent=2, sort_keys=True) + '\n').encode('utf-8')


def load_rule(path: Path) -> Dict[str, Any]:
    try:
        raw = json.loads(Path(path).read_text(encoding='utf-8'))
    except FileNotFoundError as exc:
        raise RuleError(f'规则文件不存在：{path}') from exc
    except json.JSONDecodeError as exc:
        raise RuleError(f'规则文件不是合法 JSON：{path}（{exc}）') from exc
    return validate_rule(raw)


def save_rule(rule: Dict[str, Any], rules_dir: Path | None = None) -> Path:
    """落盘为可读 JSON；同名规则就地覆盖（不产生第二份版本）。"""
    target_dir = Path(rules_dir) if rules_dir is not None else RULES_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f'{rule["name"]}.json'
    path.write_bytes(dump_rule(rule))
    return path


def list_rules(rules_dir: Path | None = None) -> List[Dict[str, str]]:
    """列出规则目录下的 JSON（文件名即规则名，类型需读取后再判定）。"""
    target_dir = Path(rules_dir) if rules_dir is not None else RULES_DIR
    if not target_dir.is_dir():
        return []
    return [{'name': p.stem, 'path': str(p)} for p in sorted(target_dir.glob('*.json'))]


# ---------------------------------------------------------------------------
# 中文描述
# ---------------------------------------------------------------------------

def _operand_text(operand: Dict[str, Any]) -> str:
    if operand['type'] == 'quantile':
        return f'{operand["q"]:.0%} 分位'
    if 'low' in operand:
        return f'{operand["low"]:g} ~ {operand["high"]:g}'
    return f'{operand["value"]:g}'


def _stock_text(rule: Dict[str, Any]) -> str:
    parts = [f'规则「{rule["name"]}」（选股）：股票池=全市场洁净集']
    if rule['conditions']:
        conds = []
        for c in rule['conditions']:
            label = FIELDS[c['field']].label
            operand = c['operand']
            if operand['type'] == 'quantile':
                side = (f'后 {operand["q"]:.0%} 分位' if c['op'] in ('<', '<=')
                        else f'前 {operand["q"]:.0%} 分位')
                scope = '行业内' if operand.get('group') == 'industry' else ''
                conds.append(f'{label} {scope}{OPERATORS[c["op"]]}{side}')
            else:
                conds.append(f'{label} {OPERATORS[c["op"]]} {_operand_text(operand)}')
        parts.append('条件=' + ' 且 '.join(conds))
    else:
        parts.append('条件=无（全市场按排序取前 N）')
    stages = rule['sort'] if isinstance(rule['sort'], list) else [rule['sort']]
    segments = []
    for stage in stages:
        direction = '降序' if stage['direction'] == 'desc' else '升序'
        segment = f'{FIELDS[stage["field"]].label} {direction}'
        if 'keep_pct' in stage:
            segment += f'保留前 {stage["keep_pct"]:.0%}'
        segments.append(segment)
    parts.append(f'排序={" → ".join(segments)}，取前 {rule["top_n"]} 只')
    freq = rule.get('rebalance', {}).get('frequency', 'monthly')
    parts.append('调仓=月频' if freq == 'monthly' else '调仓=半年频（6、12 月最后交易日）')
    return '；'.join(parts + _common_text(rule))


def _switch_text(rule: Dict[str, Any]) -> str:
    labels = {leg['code']: leg['label'] for leg in rule['legs']}
    sw = rule['switch']
    legs = '、'.join(f'{leg["label"]}({leg["code"]})' for leg in rule['legs'])
    if rule['positions']['mode'] == 'all_in':
        pos = '全仓单一标的'
    else:
        pos = '按档位：' + '，'.join(
            f'{item["state"]}=' + '/'.join(f'{labels[c]} {w:.0%}' for c, w in item['weights'].items())
            for item in rule['positions']['states'])
    return '；'.join([
        f'规则「{rule["name"]}」（强弱比切换）：标的={legs}',
        f'比值={labels[sw["numerator"]]}/{labels[sw["denominator"]]}，'
        f'中枢={sw["window"]} 日均值，上阈 +{sw["entry"]:.1%} / 下阈 -{sw["exit"]:.1%}',
        f'最短持有={sw["min_hold_days"]} 个交易日，初始={sw["initial"]}',
        f'持仓={pos}',
        '执行=信号日收盘生成，下一交易日开盘成交',
    ] + _common_text(rule))


def _timing_text(rule: Dict[str, Any]) -> str:
    sig = rule['signal']
    target = ('单标的 ' + rule['target']['codes'][0] if rule['target']['mode'] == 'single'
              else '组合 ' + '、'.join(rule['target']['codes']))
    levels = '，'.join(f'{lv["threshold"]:g}→{lv["position"]:.0%}' for lv in rule['levels'])
    return '；'.join([
        f'规则「{rule["name"]}」（仓位择时）：标的={target}',
        f'信号={SIGNAL_KINDS[sig["kind"]]}（{sig["window"]} 日）',
        f'档位={levels}，滞回={rule["hysteresis"]:g}，单日最多调一档',
        '执行=信号日收盘生成，下一交易日开盘成交',
    ] + _common_text(rule))


def _dca_text(rule: Dict[str, Any]) -> str:
    plan = rule['plan']
    target = rule['target']
    per_tranche = rule['portfolio']['initial_capital'] / plan['tranches']
    return '；'.join([
        f'规则「{rule["name"]}」（定投补仓）：标的={target["label"]}({target["code"]})',
        f'资金分 {plan["tranches"]} 份，每份约 {per_tranche:,.0f} 元，首次建仓 1 份',
        f'补仓=收盘较最近一次买入价再跌 {plan["add_drop_pct"]:.1%} 加 1 份，最多满仓',
        f'止盈=持仓收益率超过 {plan["take_profit_pct"]:.1%} 进入观察期，'
        f'自最高点回撤 {plan["trail_drop_pct"]:.1%} 一次性清仓并从 1 份重启',
        '执行=信号日收盘生成，下一交易日开盘成交',
    ] + _common_text(rule))


def _common_text(rule: Dict[str, Any]) -> List[str]:
    cost = rule['cost']
    return [
        f'区间 {rule["period"]["start"]} ~ {rule["period"]["end"]}',
        f'基准={BENCHMARKS[rule["benchmark"]]}({rule["benchmark"]})',
        f'成本=滑点 {cost["slippage_bps"]:g}bp / 印花税 {cost["stamp_tax_basis"]} / '
        f'佣金 {cost["commission_rate"]:.4%} / 最低 {cost["min_commission"]:g} 元',
    ]


def rule_text(rule: Dict[str, Any]) -> str:
    """把规则渲染成可读中文，界面与命令行共用。"""
    return {'stock_select': _stock_text,
            'asset_switch': _switch_text,
            'timing': _timing_text,
            'dca_grid': _dca_text}[rule['rule_type']](rule) + '。'


@dataclass(frozen=True)
class RuleSummary:
    name: str
    rule_type: str
    type_label: str
    text: str


def summarize(rule: Dict[str, Any]) -> RuleSummary:
    return RuleSummary(rule['name'], rule['rule_type'],
                       RULE_TYPES[rule['rule_type']], rule_text(rule))
