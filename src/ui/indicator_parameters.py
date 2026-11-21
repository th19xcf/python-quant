"""副图指标可调参数：规格、校验、持久化与计算参数映射。

设计约束：
- 只在 UI 侧传参，未修改过的指标继续走各计算函数的默认值，日更/回测/策略试算
  的既有硬编码行为不受影响；
- 仅收录「改参数后输出列名不变」或「列名可由周期推导」的指标，避免渲染层找不到列；
- 持久化文件为 ``config/indicator_params.json``，只保存相对默认值发生过变化的项。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

logger = None  # 延迟注入，避免模块导入期引入 logging 配置

DEFAULT_PARAMS_PATH = Path(__file__).resolve().parents[2] / 'config' / 'indicator_params.json'

#: 指标名 -> [(参数键, 中文名, 默认值, 最小值, 最大值)]
PARAM_SPECS: dict[str, list[tuple[str, str, float, float, float]]] = {
    'MACD': [
        ('fast_period', '快线周期', 12, 2, 200),
        ('slow_period', '慢线周期', 26, 3, 400),
        ('signal_period', '信号周期', 9, 2, 100),
    ],
    'RSI': [('period', '计算周期', 14, 2, 200)],
    'KDJ': [('period', '计算周期', 14, 2, 200)],
    'BOLL': [
        ('period', '计算周期', 20, 2, 200),
        ('std_dev', '标准差倍数', 2.0, 0.5, 10.0),
    ],
    'TRIX': [
        ('period', '计算周期', 12, 2, 200),
        ('signal_period', '信号周期', 9, 2, 100),
    ],
    'SUPERTREND': [
        ('period', 'ATR 周期', 10, 2, 200),
        ('multiplier', 'ATR 倍数', 3.0, 0.5, 10.0),
    ],
}

#: 参数变化后需要丢弃重算的列（按前缀匹配，覆盖不同周期产生的列名）
_STALE_PREFIXES: dict[str, tuple[str, ...]] = {
    'MACD': ('dif', 'dea', 'macd'),
    'RSI': ('rsi',),
    'BOLL': ('up', 'mb', 'dn'),
    'TRIX': ('trix',),
    'SUPERTREND': ('supertrend', 'st_lower', 'st_upper', 'st_dir'),
}

#: 列名过短、只能精确匹配的指标列（前缀匹配会误伤 date 等基础列）
_STALE_EXACT: dict[str, frozenset[str]] = {
    'KDJ': frozenset({'k', 'd', 'j', 'kdj_k', 'kdj_d', 'kdj_j'}),
}


def params_path() -> Path:
    """参数文件路径（可用环境变量覆盖，便于测试隔离）。"""
    import os

    override = os.environ.get('QUANT_INDICATOR_PARAMS_PATH')
    return Path(override) if override else DEFAULT_PARAMS_PATH


def load_overrides() -> dict[str, dict[str, float]]:
    """读取已保存的参数覆盖值；文件缺失或损坏时返回空字典。"""
    path = params_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    result: dict[str, dict[str, float]] = {}
    for name, values in data.items():
        if isinstance(values, dict) and name in PARAM_SPECS:
            result[name.upper()] = {
                key: value for key, value in values.items()
                if any(spec[0] == key for spec in PARAM_SPECS[name.upper()])
            }
    return result


def save_overrides(overrides: dict[str, dict[str, float]]) -> None:
    """写回参数覆盖值（只落盘有效指标与有效键）。"""
    cleaned: dict[str, dict[str, float]] = {}
    for name, values in overrides.items():
        specs = PARAM_SPECS.get(name.upper())
        if not specs:
            continue
        defaults = {spec[0]: spec[2] for spec in specs}
        diff = {key: value for key, value in values.items()
                if key in defaults and value != defaults[key]}
        if diff:
            cleaned[name.upper()] = diff
    path = params_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cleaned, ensure_ascii=False, indent=2), encoding='utf-8')


_FULLWIDTH_TRANSLATION = str.maketrans(
    '０１２３４５６７８９．－＋，', '0123456789.-+,')


def parse_param_text(name: str, key: str, text: Any) -> float:
    """解析用户输入（兼容 Excel 常见的空格、全角数字、千分位逗号）。

    空串、非数字、NaN/Inf、越界、整数位带小数都会抛 ValueError，
    消息可直接展示给用户。
    """
    specs = {spec[0]: spec for spec in PARAM_SPECS.get(name.upper(), [])}
    if key not in specs:
        raise ValueError(f'{name} 不支持参数 {key}')
    label = specs[key][1]
    raw = '' if text is None else str(text)
    normalized = raw.translate(_FULLWIDTH_TRANSLATION).strip()
    normalized = normalized.replace(' ', '').replace('\u3000', '')
    normalized = normalized.replace(',', '')
    if normalized.endswith('.'):
        normalized = normalized[:-1]
    if not normalized:
        raise ValueError(f'{label}不能为空，请输入数字')
    try:
        numeric = float(normalized)
    except ValueError as exc:
        raise ValueError(f'{label}「{raw.strip()}」不是有效数字') from exc
    return validate_param_value(name, key, numeric)


def validate_param_value(name: str, key: str, value: Any) -> float:
    """校验单个参数：必须落在规格区间内，整数型参数不允许小数。"""
    specs = {spec[0]: spec for spec in PARAM_SPECS.get(name.upper(), [])}
    if key not in specs:
        raise ValueError(f'{name} 不支持参数 {key}')
    _, label, default, low, high = specs[key]
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f'{label}必须是数字') from exc
    if numeric != numeric or numeric in (float('inf'), float('-inf')):
        raise ValueError(f'{label}必须是有限数字')
    if numeric < low or numeric > high:
        raise ValueError(f'{label}需在 {low:g}~{high:g} 之间')
    if isinstance(default, int) and not numeric.is_integer():
        raise ValueError(f'{label}必须是整数')
    return int(numeric) if isinstance(default, int) else numeric


def effective_params(name: str) -> dict[str, float]:
    """返回某指标的当前生效参数（默认值 + 已保存覆盖）。"""
    upper = name.upper()
    defaults = {spec[0]: spec[2] for spec in PARAM_SPECS.get(upper, [])}
    defaults.update(load_overrides().get(upper, {}))
    return defaults


def build_calc_kwargs(overrides: dict[str, dict[str, float]]) -> dict[str, Any]:
    """把 UI 参数覆盖值转成计算函数认识的 kwargs（与 utils.get_indicator_params 对齐）。"""
    kwargs: dict[str, Any] = {}
    macd = overrides.get('MACD')
    if macd:
        kwargs['fast_period'] = int(macd.get('fast_period', 12))
        kwargs['slow_period'] = int(macd.get('slow_period', 26))
        kwargs['signal_period'] = int(macd.get('signal_period', 9))
    rsi = overrides.get('RSI')
    if rsi:
        kwargs['rsi_windows'] = [int(rsi.get('period', 14))]
    kdj = overrides.get('KDJ')
    if kdj:
        kwargs['kdj_windows'] = [int(kdj.get('period', 14))]
    boll = overrides.get('BOLL')
    if boll:
        kwargs['boll_windows'] = [int(boll.get('period', 20))]
        kwargs['boll_std_dev'] = float(boll.get('std_dev', 2.0))
    trix = overrides.get('TRIX')
    if trix:
        kwargs['trix_windows'] = [int(trix.get('period', 12))]
        kwargs['trix_signal_period'] = int(trix.get('signal_period', 9))
    st = overrides.get('SUPERTREND')
    if st:
        kwargs['supertrend_period'] = int(st.get('period', 10))
        kwargs['supertrend_multiplier'] = float(st.get('multiplier', 3.0))
    return kwargs


def apply_overrides(name: str, values: dict[str, Any]) -> None:
    """校验并保存单个指标的参数；全部等于默认值时从配置中删除该项。"""
    upper = name.upper()
    specs = PARAM_SPECS.get(upper, [])
    if not specs:
        raise ValueError(f'{name} 无可调参数')
    validated = {key: validate_param_value(upper, key, value)
                 for key, value in values.items()}
    defaults = {spec[0]: spec[2] for spec in specs}
    merged = effective_params(upper)
    merged.update(validated)
    overrides = load_overrides()
    if all(merged[key] == defaults[key] for key in defaults):
        overrides.pop(upper, None)
    else:
        overrides[upper] = merged
    save_overrides(overrides)


def reset_overrides(name: str) -> None:
    """恢复某指标的默认参数（从配置文件中移除覆盖项）。"""
    overrides = load_overrides()
    if overrides.pop(name.upper(), None) is not None:
        save_overrides(overrides)


def expected_columns(name: str, overrides: dict[str, dict[str, float]]) -> list[str]:
    """参数化后该指标应产生的列名（RSI 列名随周期变化）。"""
    upper = name.upper()
    params = effective_params(upper)
    params.update(overrides.get(upper, {}))
    if upper == 'RSI':
        return [f"rsi{int(params.get('period', 14))}"]
    from src.ui.chart_components.indicator_renderer import INDICATOR_CALC_MAP

    entry = INDICATOR_CALC_MAP.get(upper)
    return list(entry[1]) if entry else []


def stale_columns(name: str, columns: list[str]) -> list[str]:
    """按前缀/精确名找出需要丢弃的旧列（改周期后列名会变或值需重算）。"""
    upper = name.upper()
    exact = _STALE_EXACT.get(upper, frozenset())
    prefixes = _STALE_PREFIXES.get(upper, ())
    return [col for col in columns
            if col in exact or any(col.startswith(prefix) for prefix in prefixes)]
