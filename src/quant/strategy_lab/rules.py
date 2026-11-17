"""策略规则的解析、校验、序列化与中文描述。

规则只允许由 :mod:`fields` 中预定义字段与运算符组合，不做任何代码求值
（不接 ``eval`` / ``exec``），也不接受用户自带字段名或表达式。
"""
from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from .fields import FIELDS, OPERATORS, QUANTILE_OPERATORS, FieldSpec

RULE_VERSION = 'strategy_lab/v1'
RULES_DIR = Path(__file__).resolve().parents[3] / 'config' / 'strategy_lab' / 'rules'

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
#: 最小版只开放月频；结构与既有 spec 对齐，后续扩展不改字段名
REBALANCE_FREQUENCIES = ('monthly',)

_MAX_CONDITIONS = 10
_MAX_TOP_N = 500


class RuleError(ValueError):
    """规则非法：字段未开放、运算符不匹配、取值越界等。"""


def _reject_unknown(obj: Dict[str, Any], allowed: set, where: str) -> None:
    unknown = sorted(set(obj) - allowed)
    if unknown:
        raise RuleError(f'{where} 出现未知字段 {unknown}；允许字段：{", ".join(sorted(allowed))}')


def _field(key: Any, where: str) -> FieldSpec:
    if not isinstance(key, str) or key not in FIELDS:
        raise RuleError(
            f'{where} 字段 {key!r} 未开放；可用字段：{", ".join(sorted(FIELDS))}')
    return FIELDS[key]


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


def _condition(item: Any, index: int) -> Dict[str, Any]:
    where = f'conditions[{index}]'
    if not isinstance(item, dict):
        raise RuleError(f'{where} 必须是对象')
    _reject_unknown(item, {'field', 'op', 'operand'}, where)
    spec = _field(item.get('field'), f'{where}.field')
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
            value = _number(operand.get('value'), f'{where}.operand.value')
            out = {'type': 'value', 'value': value}
    elif kind == 'quantile':
        _reject_unknown(operand, {'type', 'q'}, f'{where}.operand')
        if op not in QUANTILE_OPERATORS:
            raise RuleError(
                f'{where} 运算符 {op!r} 不能与分位组合；分位只允许 {list(QUANTILE_OPERATORS)}')
        q = _number(operand.get('q'), f'{where}.operand.q')
        if not (0.0 < q < 1.0):
            raise RuleError(f'{where} 分位 q={q} 越界，必须在 (0, 1) 内')
        out = {'type': 'quantile', 'q': q}
    else:
        raise RuleError(f"{where}.operand.type 必须是 'value' 或 'quantile'，收到 {kind!r}")
    return {'field': spec.key, 'op': op, 'operand': out}


def _validate_conditions(raw: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        raise RuleError('conditions 必须是数组')
    if len(raw) > _MAX_CONDITIONS:
        raise RuleError(f'条件最多 {_MAX_CONDITIONS} 条，收到 {len(raw)}')
    return [_condition(x, i) for i, x in enumerate(raw)]


def _validate_sort(raw: Any) -> Dict[str, str]:
    if not isinstance(raw, dict):
        raise RuleError('sort 必须是对象（排序字段与方向）')
    _reject_unknown(raw, {'field', 'direction'}, 'sort')
    spec = _field(raw.get('field'), 'sort.field')
    direction = raw.get('direction')
    if direction not in ('asc', 'desc'):
        raise RuleError(f"sort.direction 必须是 'asc' 或 'desc'，收到 {direction!r}")
    return {'field': spec.key, 'direction': direction}


def _validate_top_n(raw: Any) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int) or not (1 <= raw <= _MAX_TOP_N):
        raise RuleError(f'top_n 必须是 1~{_MAX_TOP_N} 的整数，收到 {raw!r}')
    return raw


def _validate_rebalance(raw: Any) -> None:
    if not isinstance(raw, dict):
        raise RuleError('rebalance 必须是对象')
    _reject_unknown(raw, {'frequency'}, 'rebalance')
    if raw.get('frequency') not in REBALANCE_FREQUENCIES:
        raise RuleError(f'最小版只支持月频（{list(REBALANCE_FREQUENCIES)}）')


def _validate_period(raw: Any) -> Dict[str, str]:
    if not isinstance(raw, dict):
        raise RuleError('period 必须是对象')
    _reject_unknown(raw, {'start', 'end'}, 'period')
    start = _iso(raw.get('start'), 'period.start')
    end = _iso(raw.get('end'), 'period.end')
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
    if basis not in ('fixed_0.1%', 'historical'):
        raise RuleError(f"cost.stamp_tax_basis 必须是 'fixed_0.1%' 或 'historical'，"
                        f'收到 {basis!r}')
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


def validate_rule(raw: Any) -> Dict[str, Any]:
    """校验并规范化规则；非法即抛 :class:`RuleError`（不静默忽略任何键）。"""
    if not isinstance(raw, dict):
        raise RuleError('规则顶层必须是 JSON 对象')
    _reject_unknown(raw, {'rule_version', 'name', 'description', 'universe', 'conditions',
                          'sort', 'top_n', 'rebalance', 'period', 'benchmark', 'cost',
                          'portfolio'}, '规则')
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

    universe = raw.get('universe', {'mode': 'all_market'})
    if not isinstance(universe, dict):
        raise RuleError('universe 必须是对象')
    _reject_unknown(universe, {'mode'}, 'universe')
    if universe.get('mode') != 'all_market':
        raise RuleError("最小版股票池只支持 {'mode': 'all_market'}（全市场洁净集）")

    parsed_conditions = _validate_conditions(raw.get('conditions', []))
    sort_spec = _validate_sort(raw.get('sort'))
    top_n = _validate_top_n(raw.get('top_n'))
    _validate_rebalance(raw.get('rebalance', {'frequency': 'monthly'}))
    period = _validate_period(raw.get('period'))

    benchmark = raw.get('benchmark', '000300.SH')
    if benchmark not in BENCHMARKS:
        raise RuleError(f'benchmark 必须是 {list(BENCHMARKS)} 之一，收到 {benchmark!r}')

    resolved_cost = _validate_cost(raw.get('cost', DEFAULT_COST))
    portfolio = _validate_portfolio(raw.get('portfolio', DEFAULT_PORTFOLIO))

    return {
        'rule_version': RULE_VERSION,
        'name': name.strip(),
        'description': description,
        'universe': {'mode': 'all_market'},
        'conditions': parsed_conditions,
        'sort': sort_spec,
        'top_n': top_n,
        'rebalance': {'frequency': 'monthly'},
        'period': period,
        'benchmark': benchmark,
        'cost': resolved_cost,
        'portfolio': portfolio,
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


def list_rules(rules_dir: Path | None = None) -> List[Dict[str, Any]]:
    """列出已保存规则（只读文件名与 name，不解析全部内容以外的字段）。"""
    target_dir = Path(rules_dir) if rules_dir is not None else RULES_DIR
    if not target_dir.is_dir():
        return []
    out = []
    for path in sorted(target_dir.glob('*.json')):
        out.append({'name': path.stem, 'path': str(path)})
    return out


def _operand_text(operand: Dict[str, Any]) -> str:
    if operand['type'] == 'value':
        if 'low' in operand:
            return f'{operand["low"]:g} ~ {operand["high"]:g}'
        return f'{operand["value"]:g}'
    q = operand['q']
    return f'{q:.0%} 分位'


def rule_text(rule: Dict[str, Any]) -> str:
    """把规则渲染成一句可读中文，界面与命令行共用。"""
    parts = [f'规则「{rule["name"]}」：股票池=全市场洁净集']
    if rule['conditions']:
        conds = []
        for c in rule['conditions']:
            label = FIELDS[c['field']].label
            operand = c['operand']
            if operand['type'] == 'quantile':
                q = operand['q']
                side = f'后 {q:.0%} 分位' if c['op'] in ('<', '<=') else f'前 {q:.0%} 分位'
                conds.append(f'{label} {OPERATORS[c["op"]]}{side}')
            else:
                conds.append(f'{label} {OPERATORS[c["op"]]} {_operand_text(operand)}')
        parts.append('条件=' + ' 且 '.join(conds))
    else:
        parts.append('条件=无（全市场按排序取前 N）')
    direction = '降序' if rule['sort']['direction'] == 'desc' else '升序'
    parts.append(f'排序={FIELDS[rule["sort"]["field"]].label} {direction}，取前 {rule["top_n"]} 只')
    parts.append('调仓=月频')
    parts.append(f'区间 {rule["period"]["start"]} ~ {rule["period"]["end"]}')
    parts.append(f'基准={BENCHMARKS[rule["benchmark"]]}({rule["benchmark"]})')
    cost = rule['cost']
    parts.append(f'成本=滑点 {cost["slippage_bps"]:g}bp / 印花税 {cost["stamp_tax_basis"]} / '
                 f'佣金 {cost["commission_rate"]:.4%} / 最低 {cost["min_commission"]:g} 元')
    return '；'.join(parts) + '。'
