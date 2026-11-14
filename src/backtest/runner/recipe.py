"""中文配方（.recipe.txt）→ spec.json 的解析器。

一行一条规则，顺序即剔除链执行顺序；空白与全角标点由 NFKC 归一化；
数值单位（bp / % / 交易日 / 亿 / 万）在此换算。未识别的行、未实现的名字、
缺参数、重复条件、阈值越界一律报错并列出可用词表与正确写法示例。

同一配方重复解析应得到逐字相同的 spec.json：非剔除行与行序无关，
剔除行保持出现顺序；``spec_sha256`` 可直接用于比对。
"""
from __future__ import annotations

import re
import unicodedata
from bisect import bisect_left
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import compute
from .errors import SpecError
from .registry import CONDITIONS, FACTORS
from .spec import SPEC_VERSION, dump_spec, validate_spec

RECIPE_SUFFIX = '.recipe.txt'

CONDITION_EXAMPLES = [
    '剔除 波动率 最高 20%',
    '剔除 波动率 最高 20%，窗口 60 个交易日，最少 50 只',
    '剔除 上市天数 少于 250',
    '剔除 BP 最贵 20%，最少 50 只',
    '剔除 ST',
    '剔除 成交额 最低 20%，窗口 20 个交易日',
    '剔除 PE 为负，或高于 95% 分位',
]

FACTOR_EXAMPLES = [
    '因子 波动率 分 5 档，取 最低 20%',
    '因子 波动率 分 5 档，剔除 最高 20%，窗口 60 个交易日',
    '因子 BP 分 5 档，取 最高 20%',
    '因子 EP 分 5 档，取 最高 20%',
]

KEYWORDS = ('名称', '说明', '区间', '股票池', '剔除', '因子', '调仓', '成本', '资金',
            '随机种子')

# 因子注册名 → 配方中的中文写法（前端与解析器共用这一份）
FACTOR_LABELS = {'vol': '波动率', 'bp': 'BP', 'ep': 'EP'}

# 剔除条件注册名 → 配方行模板；{参数:percent} 表示按百分数书写
CONDITION_RECIPE = {
    'exclude_vol_high_pct': '剔除 波动率 最高 {pct:percent}，窗口 {window} 个交易日，最少 {min_names} 只',
    'exclude_new_listing': '剔除 上市天数 少于 {min_trading_days}',
    'exclude_bp_expensive_pct': '剔除 BP 最贵 {pct:percent}，最少 {min_names} 只',
    'exclude_st': '剔除 ST',
    'exclude_low_liquidity_pct': '剔除 成交额 最低 {pct:percent}，窗口 {window} 个交易日',
    'exclude_pe_negative_or_expensive_pct': '剔除 PE 为负，或高于 {upper_quantile:percent} 分位',
}

# 参数注册名 → 表单中文标签
PARAM_LABELS = {
    'window': '窗口（交易日）',
    'pct': '比例（%）',
    'min_names': '最少只数',
    'min_trading_days': '最少上市交易日',
    'upper_quantile': '分位（%）',
}

# 以百分数填写、存储为小数的参数
PERCENT_PARAMS = ('pct', 'upper_quantile')

# 该集合内的 pct 必须写成 1/n（n=2~20），与 compute.unit_fraction_buckets 同源
UNIT_FRACTION_CONDITIONS = ('exclude_vol_high_pct', 'exclude_bp_expensive_pct')
UNIT_FRACTION_RANGE = (2, 20)



def parse_recipe_file(path: Path) -> Dict[str, Any]:
    path = Path(path)
    if not path.name.endswith(RECIPE_SUFFIX):
        raise SpecError(f'配方文件扩展名必须是 {RECIPE_SUFFIX}（纯文本，记事本可开）：{path.name}')
    if not path.exists():
        raise SpecError(f'配方文件不存在：{path}')
    return parse_recipe(path.read_text(encoding='utf-8-sig'))


def parse_recipe(text: str) -> Dict[str, Any]:
    state: Dict[str, Any] = {'exclusions': []}
    seen: List[str] = []
    exclusion_names: List[str] = []
    for lineno, raw_line in enumerate(text.splitlines(), start=1):
        line = _normalize(raw_line)
        if not line or line.startswith('#'):
            continue
        keyword, rest = _split_keyword(line, lineno)
        if keyword == '剔除':
            state['exclusions'].append(_parse_exclusion(rest, lineno, exclusion_names))
            continue
        if keyword == '说明':
            # 说明保留原文全角标点，只做首尾空白裁剪，便于人工阅读
            match = re.match(r'\s*说明\s*(.*)$', raw_line)
            state['说明'] = (match.group(1).strip() if match else rest)
            seen.append(keyword)
            continue
        if keyword in seen:
            raise SpecError(f'第 {lineno} 行：关键字「{keyword}」重复出现')
        seen.append(keyword)
        handler = {
            '名称': _parse_name, '说明': _parse_description, '区间': _parse_period,
            '股票池': _parse_universe, '因子': _parse_factor, '调仓': _parse_rebalance,
            '成本': _parse_cost, '资金': _parse_portfolio,
            '随机种子': _parse_seed,
        }[keyword]
        state[keyword] = handler(rest, lineno)

    missing = [k for k in ('名称', '区间', '股票池', '调仓', '成本', '资金') if k not in state]
    if missing:
        raise SpecError(f'配方缺少必需的规则行：{missing}；可用关键字：{", ".join(KEYWORDS)}')
    if '因子' not in state:
        state['因子'] = None

    raw = {
        'spec_version': SPEC_VERSION,
        'name': state['名称'],
        'description': state.get('说明', ''),
        'period': state['区间'],
        'universe': state['股票池'],
        'factor': state['因子'],
        'exclusions': state['exclusions'],
        'rebalance': state['调仓'],
        'portfolio': state['资金'],
        'cost': state['成本'],
        'random_seed': state.get('随机种子'),
    }
    return validate_spec(raw)


def recipe_bytes(path: Path) -> bytes:
    return Path(path).read_bytes()


# ---------------------------------------------------------------------------
# 基础归一化
# ---------------------------------------------------------------------------

def _normalize(raw: str) -> str:
    line = unicodedata.normalize('NFKC', raw).replace('\u3000', ' ')
    line = re.sub(r'\s+', ' ', line).strip()
    return line


def _split_keyword(line: str, lineno: int) -> Tuple[str, str]:
    parts = line.split(' ', 1)
    keyword = parts[0]
    rest = parts[1].strip() if len(parts) > 1 else ''
    if keyword not in KEYWORDS:
        raise SpecError(
            f'第 {lineno} 行：无法识别的规则「{line}」；'
            f'可用关键字：{", ".join(KEYWORDS)}；示例：区间 2010-01-04 ~ 2026-11-13，预热 103 个交易日')
    if not rest:
        raise SpecError(f'第 {lineno} 行：「{keyword}」后面缺少内容')
    return keyword, rest


def _clauses(text: str) -> List[str]:
    return [part.strip() for part in text.split(',') if part.strip()]


def _iso(text: str, where: str) -> str:
    try:
        return date.fromisoformat(text.strip()).isoformat()
    except ValueError as exc:
        raise SpecError(f'{where} 不是合法日期 {text!r}；正确写法：2026-11-13') from exc


def _pct(text: str, where: str) -> float:
    match = re.fullmatch(r'(\d+(?:\.\d+)?)\s*%', text.strip())
    if not match:
        raise SpecError(f'{where} 必须是百分数，如 20%；收到 {text!r}')
    return float(match.group(1)) / 100.0


def _days(text: str, where: str) -> int:
    match = re.fullmatch(r'(\d+)\s*(?:个?\s*交易日)?', text.strip())
    if not match:
        raise SpecError(f'{where} 必须是交易日数，如 250 或 250 个交易日；收到 {text!r}')
    return int(match.group(1))


def _amount(text: str) -> float:
    match = re.fullmatch(r'(\d+(?:\.\d+)?)\s*(亿|万|元)?', text.strip())
    if not match:
        raise SpecError(f'金额无法识别 {text!r}；正确写法：10 亿 / 5000 万 / 1000000000')
    value = float(match.group(1))
    unit = match.group(2) or '元'
    return value * {'亿': 1e8, '万': 1e4, '元': 1.0}[unit]


# ---------------------------------------------------------------------------
# 各规则行
# ---------------------------------------------------------------------------

def _parse_name(rest: str, lineno: int) -> str:
    if not re.fullmatch(r'[a-z0-9_]+', rest) or rest[0].isdigit():
        raise SpecError(
            f'第 {lineno} 行：名称必须是英文 snake_case（小写字母/数字/下划线，'
            f'不以数字开头）；收到 {rest!r}；示例：名称 v3_monthly')
    return rest


def _parse_description(rest: str, lineno: int) -> str:
    del lineno
    return rest


def _parse_period(rest: str, lineno: int) -> Dict[str, str]:
    parts = _clauses(rest)
    if len(parts) != 2:
        raise SpecError(
            f'第 {lineno} 行：区间必须写成「起 ~ 止，预热 ...」；'
            f'示例：区间 2010-01-04 ~ 2026-11-13，预热 103 个交易日')
    match = re.fullmatch(r'(\d{4}-\d{2}-\d{2})\s*[~至到]\s*(\d{4}-\d{2}-\d{2})', parts[0])
    if not match:
        raise SpecError(
            f'第 {lineno} 行：区间日期段无法识别 {parts[0]!r}；'
            f'示例：2010-01-04 ~ 2026-11-13')
    start, end = match.group(1), match.group(2)
    warm_match = re.fullmatch(r'预热\s*(.+)', parts[1])
    if not warm_match:
        raise SpecError(
            f'第 {lineno} 行：缺少预热说明；示例：预热 103 个交易日 或 预热 2009-08-01')
    warm_text = warm_match.group(1).strip()
    if re.fullmatch(r'\d{4}-\d{2}-\d{2}', warm_text):
        warmup = _iso(warm_text, '预热日期')
    else:
        n = _days(warm_text, '预热')
        days = compute.load_trading_days()
        idx = bisect_left(days, date.fromisoformat(start))
        if idx <= 0:
            raise SpecError(f'第 {lineno} 行：起点 {start} 不在交易日内，无法按交易日折算预热')
        if idx - n < 0:
            raise SpecError(f'第 {lineno} 行：预热 {n} 个交易日超出本地交易日范围')
        warmup = days[idx - n].isoformat()
    return {'warmup_start': warmup, 'start': _iso(start, '区间起点'),
            'end': _iso(end, '区间终点')}


def _parse_universe(rest: str, lineno: int) -> Dict[str, Any]:
    if rest == '全市场':
        return {'mode': 'all_market', 'codes': []}
    match = re.fullmatch(r'指定\s+(.+)', rest)
    if not match:
        raise SpecError(
            f'第 {lineno} 行：股票池只支持「全市场」或「指定 代码清单」；'
            f'示例：股票池 指定 000001.SZ, 600000.SH')
    codes = []
    for item in re.split(r'[,、\s]+', match.group(1).strip()):
        if not item:
            continue
        code = item.upper()
        if not re.fullmatch(r'\d{6}\.(SH|SZ|BJ)', code):
            raise SpecError(
                f'第 {lineno} 行：代码 {item!r} 格式非法；正确写法：000001.SZ / 600000.SH')
        codes.append(code)
    if not codes:
        raise SpecError(f'第 {lineno} 行：指定股票池不能为空')
    return {'mode': 'explicit', 'codes': codes}


def _parse_exclusion(rest: str, lineno: int, seen: List[str]) -> Dict[str, Any]:
    pe_match = re.fullmatch(
        r'(?i)PE\s*为负\s*,?\s*或\s*高于\s*(\d+(?:\.\d+)?)\s*%?\s*分位', rest)
    if pe_match:
        name = 'exclude_pe_negative_or_expensive_pct'
        params: Dict[str, Any] = {
            'upper_quantile': float(pe_match.group(1)) / 100.0}
        extra: List[str] = []
    else:
        parts = _clauses(rest)
        phrase, extra = parts[0], parts[1:]
        name, params = _condition_phrase(phrase, lineno)
    if name in seen:
        raise SpecError(
            f'第 {lineno} 行：剔除条件 {name} 重复出现；同一条件只允许出现一次'
            f'（当前链：{seen}）')
    for clause in extra:
        window = re.fullmatch(r'窗口\s*(\d+)\s*个?\s*交易日', clause)
        min_names = re.fullmatch(r'最少\s*(\d+)\s*只', clause)
        if window:
            params['window'] = int(window.group(1))
        elif min_names:
            params['min_names'] = int(min_names.group(1))
        else:
            raise SpecError(
                f'第 {lineno} 行：条件附加片段 {clause!r} 无法识别；'
                f'可用：窗口 N 个交易日 / 最少 N 只')
    entry = CONDITIONS.get(name)
    accepted = {p.name for p in entry.params}
    unknown = sorted(set(params) - accepted)
    if unknown:
        raise SpecError(
            f'第 {lineno} 行：条件 {name} 不接受参数 {unknown}；'
            f'可用参数：{", ".join(sorted(accepted))}')
    resolved = CONDITIONS.resolve(name, params)
    if name in ('exclude_vol_high_pct', 'exclude_bp_expensive_pct'):
        compute.unit_fraction_buckets(resolved['pct'], f'第 {lineno} 行：{name}')
    seen.append(name)
    return {'name': name, 'params': resolved}


def _condition_phrase(phrase: str, lineno: int) -> Tuple[str, Dict[str, Any]]:
    match = re.fullmatch(r'波动率\s*最高\s*(\d+(?:\.\d+)?)\s*%', phrase)
    if match:
        return 'exclude_vol_high_pct', {'pct': float(match.group(1)) / 100.0}
    match = re.fullmatch(r'上市天数\s*少于\s*(\d+)\s*(?:个?\s*交易日)?', phrase)
    if match:
        return 'exclude_new_listing', {'min_trading_days': int(match.group(1))}
    match = re.fullmatch(r'(?i)BP\s*最贵\s*(\d+(?:\.\d+)?)\s*%', phrase)
    if match:
        return 'exclude_bp_expensive_pct', {'pct': float(match.group(1)) / 100.0}
    if re.fullmatch(r'(?i)ST(?:股)?', phrase):
        return 'exclude_st', {}
    match = re.fullmatch(r'(?:成交额|流动性)\s*最低\s*(\d+(?:\.\d+)?)\s*%', phrase)
    if match:
        return 'exclude_low_liquidity_pct', {'pct': float(match.group(1)) / 100.0}
    match = re.fullmatch(
        r'(?i)PE\s*为负\s*,?\s*或\s*高于\s*(\d+(?:\.\d+)?)\s*%?\s*分位', phrase)
    if match:
        return 'exclude_pe_negative_or_expensive_pct', {
            'upper_quantile': float(match.group(1)) / 100.0}
    raise SpecError(
        f'第 {lineno} 行：未识别的剔除写法 {phrase!r}；'
        f'可用写法：{" | ".join(CONDITION_EXAMPLES)}')


def _parse_factor(rest: str, lineno: int) -> Dict[str, Any]:
    parts = _clauses(rest)
    if not parts:
        raise SpecError(f'第 {lineno} 行：因子行缺少内容')
    match = re.fullmatch(r'(?i)(波动率|BP|EP)\s*分\s*(\d+)\s*档', parts[0])
    if not match:
        raise SpecError(
            f'第 {lineno} 行：因子写法无法识别 {parts[0]!r}；'
            f'可用写法：{" | ".join(FACTOR_EXAMPLES)}')
    word, buckets = match.group(1).upper(), int(match.group(2))
    name = {label: key for key, label in FACTOR_LABELS.items()}[word]
    params: Dict[str, Any] = {'window': 60} if name == 'vol' else {}
    action = bucket = None
    for clause in parts[1:]:
        window = re.fullmatch(r'窗口\s*(\d+)\s*个?\s*交易日', clause)
        if window:
            if name != 'vol':
                raise SpecError(f'第 {lineno} 行：因子 {word} 不接受窗口参数')
            params['window'] = int(window.group(1))
            continue
        pick = re.fullmatch(r'(取|剔除)\s*(最低|最高)\s*(\d+(?:\.\d+)?)\s*%', clause)
        if not pick:
            raise SpecError(
                f'第 {lineno} 行：因子附加片段 {clause!r} 无法识别；'
                f'可用：取 最低 20% / 剔除 最高 20% / 窗口 60 个交易日')
        mode, side, pct_text = pick.group(1), pick.group(2), pick.group(3)
        pct = float(pct_text) / 100.0
        expected = 1.0 / buckets
        if abs(pct - expected) > 1e-9:
            raise SpecError(
                f'第 {lineno} 行：分 {buckets} 档时每档为 {100.0 / buckets:.1f}%，'
                f'不能写 {pct_text}%；正确写法：{mode} {side} {100.0 / buckets:.1f}%')
        action = 'hold_bucket' if mode == '取' else 'exclude_bucket'
        bucket = 1 if side == '最低' else buckets
    if action is None:
        raise SpecError(
            f'第 {lineno} 行：因子行必须写明取/剔除哪一档，否则因子未被消费；'
            f'示例：因子 波动率 分 5 档，取 最低 20%')
    return {'name': name, 'params': params,
            'quantile': {'buckets': buckets, 'direction': 'asc',
                         'action': action, 'bucket': bucket, 'min_names': 50}}


def _parse_rebalance(rest: str, lineno: int) -> Dict[str, str]:
    parts = _clauses(rest)
    freq = {'月频': 'monthly', '月度': 'monthly', '季频': 'quarterly',
            '季度': 'quarterly', '半年频': 'semi', '半年度': 'semi'}.get(parts[0])
    if freq is None:
        raise SpecError(
            f'第 {lineno} 行：调仓频率无法识别 {parts[0]!r}；可用：月频 / 季频 / 半年频')
    out = {'frequency': freq}
    for clause in parts[1:]:
        match = re.fullmatch(r'首次\s*(\d{4}-\d{2}-\d{2})', clause)
        if not match:
            raise SpecError(
                f'第 {lineno} 行：调仓附加片段 {clause!r} 无法识别；示例：首次 2009-09-01')
        out['first_date'] = _iso(match.group(1), '首次调仓日')
    return out


def _parse_cost(rest: str, lineno: int) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for clause in _clauses(rest):
        match = re.fullmatch(r'滑点\s*(\d+(?:\.\d+)?)\s*(bp|%)', clause, re.IGNORECASE)
        if match:
            value = float(match.group(1))
            out['slippage_bps'] = value if match.group(2).lower() == 'bp' else value * 100.0
            continue
        if re.fullmatch(r'印花税\s*(真值|历史)', clause):
            out['stamp_tax_basis'] = 'historical'
            continue
        if re.fullmatch(r'印花税\s*(?:固定)?\s*(?:千1|0\.1%)', clause):
            out['stamp_tax_basis'] = 'fixed_0.1%'
            continue
        match = re.fullmatch(r'佣金\s*(?:万|万分之)\s*(\d+(?:\.\d+)?)', clause)
        if match:
            out['commission_rate'] = float(match.group(1)) / 10000.0
            continue
        match = re.fullmatch(r'最低佣金\s*(\d+(?:\.\d+)?)\s*元?', clause)
        if match:
            out['min_commission'] = float(match.group(1))
            continue
        match = re.fullmatch(r'冲击\s*(\d+(?:\.\d+)?)', clause)
        if match:
            out['impact_coef'] = float(match.group(1))
            continue
        raise SpecError(
            f'第 {lineno} 行：成本片段 {clause!r} 无法识别；'
            f'示例：滑点 10bp，印花税 真值，佣金 万2.5，最低佣金 5 元，冲击 0.015')
    missing = [k for k in ('slippage_bps', 'stamp_tax_basis') if k not in out]
    if missing:
        raise SpecError(f'第 {lineno} 行：成本缺少 {missing}；必须写滑点与印花税口径')
    return out


def _parse_portfolio(rest: str, lineno: int) -> Dict[str, Any]:
    parts = _clauses(rest)
    out: Dict[str, Any] = {'cash_buffer': 0.01, 'pooled_cash': True}
    lot_seen = False
    for i, clause in enumerate(parts):
        if i == 0:
            try:
                out['initial_capital'] = _amount(clause)
                continue
            except SpecError as exc:
                raise SpecError(f'第 {lineno} 行：资金首个片段必须是金额；{exc}') from exc
        if re.fullmatch(r'价格取整按手', clause):
            out['lot_size'] = 100
            lot_seen = True
            continue
        if re.fullmatch(r'价格取整按股', clause):
            out['lot_size'] = 1
            lot_seen = True
            continue
        match = re.fullmatch(r'现金缓冲\s*(\d+(?:\.\d+)?)\s*%', clause)
        if match:
            out['cash_buffer'] = float(match.group(1)) / 100.0
            continue
        if clause == '共用现金池':
            out['pooled_cash'] = True
            continue
        if clause == '独立现金池':
            out['pooled_cash'] = False
            continue
        raise SpecError(
            f'第 {lineno} 行：资金片段 {clause!r} 无法识别；'
            f'示例：资金 10 亿，价格取整按手，现金缓冲 1%，共用现金池')
    if not lot_seen:
        raise SpecError(f'第 {lineno} 行：资金行缺少价格取整口径；请写「价格取整按手」或「价格取整按股」')
    return out


def _parse_seed(rest: str, lineno: int) -> int | None:
    if rest == '无':
        return None
    if not re.fullmatch(r'\d+', rest):
        raise SpecError(f'第 {lineno} 行：随机种子必须是整数或「无」；收到 {rest!r}')
    return int(rest)


def canonical_bytes(spec: Dict[str, Any]) -> bytes:
    return dump_spec(spec)


def spec_sha256(spec: Dict[str, Any]) -> str:
    import hashlib
    return hashlib.sha256(dump_spec(spec)).hexdigest()
