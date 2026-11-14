"""回测规格（spec.json）的结构校验与规范化。

严格模式：未知字段、未实现名字、参数无人消费、取值越界一律抛 ``SpecError``；
校验通过后把补全默认值的可执行参数挂到 ``_resolved``，供运行期直接消费。
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from .errors import SpecError
from .registry import CONDITIONS, FACTORS

SPEC_VERSION = 'backtest_spec/v1'
_NAME_CHARS = set('abcdefghijklmnopqrstuvwxyz0123456789_')


def load_spec(path: Path) -> Dict[str, Any]:
    try:
        raw = json.loads(Path(path).read_text(encoding='utf-8'))
    except FileNotFoundError as exc:
        raise SpecError(f'规格文件不存在：{path}') from exc
    except json.JSONDecodeError as exc:
        raise SpecError(f'规格文件不是合法 JSON：{path}（{exc}）') from exc
    return validate_spec(raw)


def validate_spec(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise SpecError('规格顶层必须是 JSON 对象')
    _reject_unknown(raw, {'spec_version', 'name', 'description', 'period', 'universe',
                          'factor', 'exclusions', 'rebalance', 'portfolio', 'cost',
                          'random_seed'}, '规格')
    if raw.get('spec_version') != SPEC_VERSION:
        raise SpecError(f"spec_version 必须是 '{SPEC_VERSION}'，收到 {raw.get('spec_version')!r}")

    name = raw.get('name')
    if (not isinstance(name, str) or not name or set(name) - _NAME_CHARS
            or name[0].isdigit()):
        raise SpecError(f"name 必须是英文 snake_case（小写字母/数字/下划线，不以数字开头）：{name!r}")

    description = raw.get('description', '')
    if not isinstance(description, str):
        raise SpecError('description 必须是字符串')

    period = _period(raw.get('period'))
    universe = _universe(raw.get('universe'))
    factor = _factor(raw.get('factor'))
    exclusions = _exclusions(raw.get('exclusions'))
    rebalance = _rebalance(raw.get('rebalance'), period['warmup_start'])
    portfolio = _portfolio(raw.get('portfolio'))
    cost = _cost(raw.get('cost'))
    seed = raw.get('random_seed')
    if seed is not None and (isinstance(seed, bool) or not isinstance(seed, int)):
        raise SpecError(f'random_seed 必须是整数或 null，收到 {seed!r}')

    spec: Dict[str, Any] = {
        'spec_version': SPEC_VERSION,
        'name': name,
        'description': description,
        'period': period,
        'universe': universe,
        'factor': factor['raw'] if factor else None,
        'exclusions': [x['raw'] for x in exclusions],
        'rebalance': rebalance,
        'portfolio': portfolio,
        'cost': cost,
        'random_seed': seed,
    }
    spec['_resolved'] = {
        'factor': factor['resolved'] if factor else None,
        'exclusions': [x['resolved'] for x in exclusions],
        'period': period,
        'rebalance': rebalance,
        'portfolio': portfolio,
        'cost': cost,
        'universe': universe,
        'random_seed': seed,
    }
    return spec


def dump_spec(spec: Dict[str, Any]) -> bytes:
    """规范化 JSON 字节：键排序、两空格缩进、末尾换行；同一语义逐字相同。"""
    public = {k: v for k, v in spec.items() if not k.startswith('_')}
    return (json.dumps(public, ensure_ascii=False, indent=2, sort_keys=True) + '\n').encode('utf-8')


def summary(spec: Dict[str, Any]) -> Dict[str, Any]:
    """解析结果摘要：供「只看解析结果」命令人工确认执行链。"""
    resolved = spec['_resolved']
    factor = resolved['factor']
    return {
        'spec_version': spec['spec_version'],
        'name': spec['name'],
        'description': spec['description'],
        'period': resolved['period'],
        'universe': resolved['universe'],
        'factor': factor,
        'exclusion_chain': [
            {'step': i + 1, 'name': x['name'], 'params': x['params']}
            for i, x in enumerate(resolved['exclusions'])],
        'rebalance': resolved['rebalance'],
        'portfolio': resolved['portfolio'],
        'cost': resolved['cost'],
        'random_seed': resolved['random_seed'],
        'registry': {'factors': FACTORS.names(), 'conditions': CONDITIONS.names()},
    }


# ---------------------------------------------------------------------------
# 分节校验
# ---------------------------------------------------------------------------

def _reject_unknown(obj: Dict[str, Any], allowed: set, where: str) -> None:
    unknown = sorted(set(obj) - allowed)
    if unknown:
        raise SpecError(f'{where} 出现未知字段 {unknown}；允许字段：{", ".join(sorted(allowed))}')


def _iso(value: Any, where: str) -> str:
    if not isinstance(value, str):
        raise SpecError(f'{where} 必须是 YYYY-MM-DD 字符串，收到 {value!r}')
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise SpecError(f'{where} 不是合法日期：{value!r}') from exc


def _period(obj: Any) -> Dict[str, str]:
    if not isinstance(obj, dict):
        raise SpecError('period 必须是对象')
    _reject_unknown(obj, {'warmup_start', 'start', 'end'}, 'period')
    for key in ('warmup_start', 'start', 'end'):
        if key not in obj:
            raise SpecError(f'period 缺少必填字段 {key}')
    warmup = _iso(obj['warmup_start'], 'period.warmup_start')
    start = _iso(obj['start'], 'period.start')
    end = _iso(obj['end'], 'period.end')
    if not (warmup <= start <= end):
        raise SpecError(f'period 日期顺序非法：预热 {warmup} / 起 {start} / 止 {end}')
    return {'warmup_start': warmup, 'start': start, 'end': end}


def _universe(obj: Any) -> Dict[str, Any]:
    if not isinstance(obj, dict):
        raise SpecError('universe 必须是对象')
    _reject_unknown(obj, {'mode', 'codes'}, 'universe')
    mode = obj.get('mode')
    if mode not in ('all_market', 'explicit'):
        raise SpecError(f"universe.mode 必须是 'all_market' 或 'explicit'，收到 {mode!r}")
    codes = obj.get('codes', [])
    if not isinstance(codes, list) or any(not isinstance(c, str) for c in codes):
        raise SpecError('universe.codes 必须是字符串数组')
    if mode == 'explicit' and not codes:
        raise SpecError('universe.mode=explicit 时 codes 不能为空')
    if mode == 'all_market' and codes:
        raise SpecError('universe.mode=all_market 时 codes 必须为空（语义冲突，不静默忽略）')
    return {'mode': mode, 'codes': sorted(codes) if codes else []}


def _factor(obj: Any) -> Dict[str, Any] | None:
    if obj is None:
        return None
    if not isinstance(obj, dict):
        raise SpecError('factor 必须是对象或 null')
    _reject_unknown(obj, {'name', 'params', 'quantile'}, 'factor')
    name = obj.get('name')
    params = FACTORS.resolve(name, obj.get('params') or {})
    q = obj.get('quantile')
    if not isinstance(q, dict):
        raise SpecError('factor.quantile 必须是对象')
    _reject_unknown(q, {'buckets', 'direction', 'action', 'bucket', 'min_names'},
                    'factor.quantile')
    buckets = q.get('buckets')
    if isinstance(buckets, bool) or not isinstance(buckets, int) or buckets < 2 or buckets > 20:
        raise SpecError(f'factor.quantile.buckets 必须是 2~20 的整数，收到 {buckets!r}')
    direction = q.get('direction')
    if direction not in ('asc', 'desc'):
        raise SpecError(f"factor.quantile.direction 必须是 'asc' 或 'desc'，收到 {direction!r}")
    action = q.get('action')
    if action not in ('hold_bucket', 'exclude_bucket'):
        raise SpecError(
            "factor.quantile.action 必须是 'hold_bucket' 或 'exclude_bucket'，"
            f'收到 {action!r}；等权全持请把 factor 写成 null')
    min_names = q.get('min_names', 50)
    if isinstance(min_names, bool) or not isinstance(min_names, int) or min_names < 1:
        raise SpecError(f'factor.quantile.min_names 必须是正整数，收到 {min_names!r}')
    bucket = q.get('bucket')
    if isinstance(bucket, bool) or not isinstance(bucket, int):
        raise SpecError(f'factor.quantile.bucket 必须是整数，收到 {bucket!r}')
    if not (1 <= bucket <= buckets):
        raise SpecError(
            f'factor.quantile.bucket={bucket} 越界，必须在 1~{buckets} 内')
    quantile = {'buckets': buckets, 'direction': direction, 'action': action,
                'min_names': min_names, 'bucket': bucket}
    return {
        'raw': {'name': name, 'params': dict(obj.get('params') or {}), 'quantile': quantile},
        'resolved': {'name': name, 'params': params, 'quantile': quantile},
    }


def _exclusions(obj: Any) -> List[Dict[str, Any]]:
    if obj is None:
        obj = []
    if not isinstance(obj, list):
        raise SpecError('exclusions 必须是数组（顺序即执行顺序）')
    seen: List[str] = []
    out: List[Dict[str, Any]] = []
    for i, item in enumerate(obj):
        if not isinstance(item, dict):
            raise SpecError(f'exclusions[{i}] 必须是对象')
        _reject_unknown(item, {'name', 'params'}, f'exclusions[{i}]')
        name = item.get('name')
        if not isinstance(name, str) or not name:
            raise SpecError(f'exclusions[{i}].name 必须是非空字符串')
        if name in seen:
            raise SpecError(
                f'剔除条件重复出现：{name}；同一条件只允许出现一次'
                f'（当前链：{seen or "空"}）')
        seen.append(name)
        resolved = CONDITIONS.resolve(name, item.get('params') or {})
        out.append({
            'raw': {'name': name, 'params': dict(item.get('params') or {})},
            'resolved': {'name': name, 'params': resolved},
        })
    return out


def _rebalance(obj: Any, warmup_start: str) -> Dict[str, str]:
    if not isinstance(obj, dict):
        raise SpecError('rebalance 必须是对象')
    _reject_unknown(obj, {'frequency', 'first_date'}, 'rebalance')
    freq = obj.get('frequency')
    if freq not in ('monthly', 'quarterly', 'semi'):
        raise SpecError(
            f"rebalance.frequency 必须是 'monthly' / 'quarterly' / 'semi'，收到 {freq!r}")
    first = obj.get('first_date', warmup_start)
    first = _iso(first, 'rebalance.first_date')
    if first < warmup_start:
        raise SpecError(f'rebalance.first_date={first} 早于预热起点 {warmup_start}')
    return {'frequency': freq, 'first_date': first}


def _portfolio(obj: Any) -> Dict[str, Any]:
    if not isinstance(obj, dict):
        raise SpecError('portfolio 必须是对象')
    _reject_unknown(obj, {'initial_capital', 'lot_size', 'cash_buffer', 'pooled_cash'},
                    'portfolio')
    capital = obj.get('initial_capital')
    if isinstance(capital, bool) or not isinstance(capital, (int, float)) or capital <= 0:
        raise SpecError(f'portfolio.initial_capital 必须是正数，收到 {capital!r}')
    lot = obj.get('lot_size', 100)
    if isinstance(lot, bool) or not isinstance(lot, int) or lot < 1:
        raise SpecError(f'portfolio.lot_size 必须是正整数，收到 {lot!r}')
    buffer = obj.get('cash_buffer', 0.01)
    if isinstance(buffer, bool) or not isinstance(buffer, (int, float)) or not (0 <= buffer < 1):
        raise SpecError(f'portfolio.cash_buffer 必须在 [0, 1) 内，收到 {buffer!r}')
    pooled = obj.get('pooled_cash', True)
    if not isinstance(pooled, bool):
        raise SpecError(f'portfolio.pooled_cash 必须是布尔值，收到 {pooled!r}')
    return {'initial_capital': float(capital), 'lot_size': lot,
            'cash_buffer': float(buffer), 'pooled_cash': pooled}


def _cost(obj: Any) -> Dict[str, Any]:
    if not isinstance(obj, dict):
        raise SpecError('cost 必须是对象')
    _reject_unknown(obj, {'slippage_bps', 'impact_coef', 'commission_rate',
                          'min_commission', 'stamp_tax_basis'}, 'cost')
    for key in ('slippage_bps', 'stamp_tax_basis'):
        if key not in obj:
            raise SpecError(f'cost 缺少必填字段 {key}')
    slip = obj['slippage_bps']
    if isinstance(slip, bool) or not isinstance(slip, (int, float)) or slip < 0:
        raise SpecError(f'cost.slippage_bps 必须是非负数，收到 {slip!r}')
    basis = obj['stamp_tax_basis']
    if basis not in ('fixed_0.1%', 'historical'):
        raise SpecError(
            f"cost.stamp_tax_basis 必须是 'fixed_0.1%' 或 'historical'，收到 {basis!r}")
    impact = obj.get('impact_coef', 0.015)
    if isinstance(impact, bool) or not isinstance(impact, (int, float)) or impact < 0:
        raise SpecError(f'cost.impact_coef 必须是非负数，收到 {impact!r}')
    commission = obj.get('commission_rate', 0.00025)
    if isinstance(commission, bool) or not isinstance(commission, (int, float)) or commission < 0:
        raise SpecError(f'cost.commission_rate 必须是非负数，收到 {commission!r}')
    min_commission = obj.get('min_commission', 5.0)
    if isinstance(min_commission, bool) or not isinstance(min_commission, (int, float)) \
            or min_commission < 0:
        raise SpecError(f'cost.min_commission 必须是非负数，收到 {min_commission!r}')
    return {'slippage_bps': float(slip), 'impact_coef': float(impact),
            'commission_rate': float(commission),
            'min_commission': float(min_commission), 'stamp_tax_basis': basis}
