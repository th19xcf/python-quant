"""因子与剔除条件的名字注册表。

规格里只出现「名字 + 参数」，名字到实现的映射集中在这里；未实现的名字、
未声明的参数、类型或取值越界都会抛 ``SpecError`` 并列出可用项，禁止静默忽略。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Tuple

from . import compute
from .errors import SpecError

_REQUIRED = object()


@dataclass(frozen=True)
class Param:
    """一个可声明参数的约束；``default`` 为 ``_REQUIRED`` 表示必填。"""

    name: str
    kind: type
    default: Any = _REQUIRED
    minimum: float | None = None
    maximum: float | None = None
    choices: Tuple[Any, ...] | None = None

    def describe(self) -> str:
        text = f'{self.name}:{self.kind.__name__}'
        if self.default is _REQUIRED:
            text += ' 必填'
        else:
            text += f'={self.default}'
        if self.choices is not None:
            text += f' 取值{list(self.choices)}'
        elif self.minimum is not None or self.maximum is not None:
            text += f' 范围[{self.minimum}, {self.maximum}]'
        return text


@dataclass(frozen=True)
class Entry:
    name: str
    func: Callable[..., Any]
    params: Tuple[Param, ...]
    doc: str

    def describe(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'params': {p.name: (None if p.default is _REQUIRED else p.default)
                       for p in self.params},
            'params_required': [p.name for p in self.params if p.default is _REQUIRED],
            'doc': self.doc,
        }


class Registry:
    """名字 → (可调用对象, 参数表) 的注册表。"""

    def __init__(self, label: str):
        self.label = label
        self._entries: Dict[str, Entry] = {}

    def register(self, name: str, func: Callable[..., Any],
                 params: Iterable[Param] = (), doc: str = '') -> None:
        if name in self._entries:
            raise RuntimeError(f'{self.label}名重复注册：{name}')
        self._entries[name] = Entry(name, func, tuple(params), doc)

    def names(self) -> List[str]:
        return sorted(self._entries)

    def get(self, name: str) -> Entry:
        entry = self._entries.get(name)
        if entry is None:
            raise SpecError(
                f"未实现的{self.label} '{name}'；可用{self.label}："
                f"{', '.join(self.names())}")
        return entry

    def resolve(self, name: str, params: Dict[str, Any] | None) -> Dict[str, Any]:
        """校验并补全默认值，返回完整参数字典。"""
        entry = self.get(name)
        if params is None:
            params = {}
        if not isinstance(params, dict):
            raise SpecError(f"{self.label} '{name}' 的 params 必须是对象")
        accepted = {p.name for p in entry.params}
        unknown = sorted(set(params) - accepted)
        if unknown:
            raise SpecError(
                f"{self.label} '{name}' 不接受参数 {unknown}；"
                f"可用参数：{', '.join(sorted(accepted)) or '（无）'}")
        out: Dict[str, Any] = {}
        for p in entry.params:
            if p.name not in params:
                if p.default is _REQUIRED:
                    raise SpecError(
                        f"{self.label} '{name}' 缺少必填参数 '{p.name}'；"
                        f"参数表：{', '.join(x.describe() for x in entry.params)}")
                out[p.name] = p.default
                continue
            out[p.name] = _check_value(self.label, name, p, params[p.name])
        return out

    def describe(self) -> List[Dict[str, Any]]:
        return [self._entries[k].describe() for k in self.names()]


def _check_value(label: str, name: str, p: Param, value: Any) -> Any:
    if p.kind is int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise SpecError(f"{label} '{name}' 参数 '{p.name}' 必须是整数，收到 {value!r}")
    elif p.kind is float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise SpecError(f"{label} '{name}' 参数 '{p.name}' 必须是数字，收到 {value!r}")
        value = float(value)
    elif not isinstance(value, p.kind):
        raise SpecError(
            f"{label} '{name}' 参数 '{p.name}' 必须是 {p.kind.__name__}，收到 {value!r}")
    if p.choices is not None and value not in p.choices:
        raise SpecError(
            f"{label} '{name}' 参数 '{p.name}' 取值 {value!r} 不在 {list(p.choices)} 内")
    if p.minimum is not None and value < p.minimum:
        raise SpecError(
            f"{label} '{name}' 参数 '{p.name}'={value} 小于下限 {p.minimum}")
    if p.maximum is not None and value > p.maximum:
        raise SpecError(
            f"{label} '{name}' 参数 '{p.name}'={value} 大于上限 {p.maximum}")
    return value


FACTORS = Registry('因子')
CONDITIONS = Registry('剔除条件')

FACTORS.register(
    'vol', compute.vol_panel,
    [Param('window', int, 60, minimum=2)],
    '信号日为止最近 window 个交易日 hfq 对数收益率的样本标准差（窗口末端必须是信号日）')
FACTORS.register(
    'bp', compute.bp_panel, [],
    'BP=1/PB（valuation_daily.pb，按信号日 as-of 取最近一条；仅 PB>0 入截面）')
FACTORS.register(
    'ep', compute.ep_panel, [],
    'EP=1/PE（valuation_daily.pe，按信号日 as-of 取最近一条；仅 PE>0 入截面）')

CONDITIONS.register(
    'exclude_vol_high_pct', compute.exclude_vol_high_pct,
    [Param('window', int, 60, minimum=2),
     Param('pct', float, 0.20, minimum=0.0, maximum=1.0),
     Param('min_names', int, 50, minimum=1)],
    '按升序分位剔除波动率最高的 pct（pct 必须为 1/n，n 取 2~20 的整数）')
CONDITIONS.register(
    'exclude_new_listing', compute.exclude_new_listing,
    [Param('min_trading_days', int, 250, minimum=0)],
    '剔除上市不满 min_trading_days 个交易日的标的（stock_basic.list_date，缺失回退面板首日）')
CONDITIONS.register(
    'exclude_bp_expensive_pct', compute.exclude_bp_expensive_pct,
    [Param('pct', float, 0.20, minimum=0.0, maximum=1.0),
     Param('min_names', int, 50, minimum=1)],
    '剔除 BP 最低（最贵）的 pct；PB<=0 或缺失者不参与排序、保留在大池')
CONDITIONS.register(
    'exclude_st', compute.exclude_st, [],
    '剔除信号日处于 ST / 退市风险区间的标的（st_status 区间，区间外按非 ST）')
CONDITIONS.register(
    'exclude_low_liquidity_pct', compute.exclude_low_liquidity_pct,
    [Param('window', int, 20, minimum=1),
     Param('pct', float, 0.20, minimum=0.0, maximum=1.0)],
    '剔除 window 日平均成交额最低的 pct（窗口须满窗且全为有效值）')
CONDITIONS.register(
    'exclude_pe_negative_or_expensive_pct', compute.exclude_pe_negative_or_expensive_pct,
    [Param('upper_quantile', float, 0.95, minimum=0.0, maximum=1.0)],
    '剔除 PE<0 或 PE 高于正 PE 截面 upper_quantile 分位的标的')
