#!/usr/bin/env python3

"""可交易性与风险剔除：复用 ``ExecutionRules``，不重写涨跌停规则。"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

from src.advice.config import AdviceConfig, TierConfig
from src.risk.execution_rules import ExecutionRules

_RULES = ExecutionRules()


@dataclass
class EligibilityResult:
    """单标的在给定方向上的可推荐性。

    ``limit_up_risk=True`` 表示「T 日已涨停」：**不拦截**，只标注次日可能
    买不进。清单说策略想买什么，撮合层说实际能买什么，两层不互相替代。
    """

    ok: bool
    notes: list[str] = field(default_factory=list)
    blocked_reason: str = ''
    #: T 日涨停（仅买入方向标注，不再拦截）
    limit_up_risk: bool = False
    #: 涨停判定依据（涨停价 / 幅度 / 触板形态），供清单逐条核对
    limit_up_basis: str = ''

    def add_note(self, note: str) -> None:
        if note and note not in self.notes:
            self.notes.append(note)


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _limit_up_status(row: dict) -> tuple[bool, str]:
    """T 日是否涨停（开盘价或收盘价触及涨停价），并给出判定依据。

    与 ``ExecutionRules.block_reason`` 的分工：后者判「当日开盘能不能买」，
    这里判「T 日收盘状态是不是涨停」——涨停但开盘未封板的票，次日同样存在
    买不进的可能，必须标注出来。
    """
    code = str(row.get('ts_code', ''))
    prev = _finite(row.get('raw_prev_close'))
    if prev is None or prev <= 0:
        return False, ''
    pct = _RULES.resolve_limit_pct(code, None, trade_date=row.get('date'))
    limit_price = _RULES.resolve_limit_price(prev, pct, 'buy')
    high = _finite(row.get('raw_high'))
    low = _finite(row.get('raw_low'))
    one_line = (high is not None and low is not None
                and abs(high - low) / prev < 1e-6)
    shape = '一字板' if one_line else '盘中触板'
    for field_name, label in (('raw_open', '开盘价'), ('raw_close', '收盘价')):
        price = _finite(row.get(field_name))
        if price is not None and price >= limit_price - 1e-9:
            return True, (f'{label} {price:.2f} ≥ 涨停价 {limit_price:.2f}'
                          f'（幅度 {pct:.0%}，{shape}）')
    return False, ''


def check_eligibility(row: dict, tier: TierConfig, config: AdviceConfig,
                      side: str = 'buy',
                      st_status: str | None = None) -> EligibilityResult:
    """检查单标的是否满足推荐条件。

    ``row`` 需含原始价列（``raw_open/raw_high/raw_low/raw_close/
    raw_prev_close/raw_volume``）与因子列（``amount_ma20/volatility_20/
    atr_pct/history_rows``）。涨跌停/停牌判定统一委托 ``ExecutionRules``。

    ``st_status`` 取 ``'st'|'non_st'|'unknown'``；``unknown`` 只提示不臆测，
    不能当成 ``non_st``。
    """
    notes: list[str] = []
    blocked: list[str] = []

    history = _finite(row.get('history_rows')) or 0.0
    if history < tier.lookback_days:
        blocked.append(f'历史不足{int(history)}日<{tier.lookback_days}日')

    raw_close = _finite(row.get('raw_close'))
    if raw_close is None or raw_close <= 0:
        blocked.append('缺少有效收盘价')
    elif raw_close < config.min_price:
        blocked.append(f'股价{raw_close:.2f}<{config.min_price:.2f}')

    volume = _finite(row.get('raw_volume'))
    if volume is None or volume <= 0:
        blocked.append('停牌或无成交')

    amount_ma20 = _finite(row.get('amount_ma20'))
    if amount_ma20 is None or amount_ma20 < tier.min_amount:
        blocked.append(f'20日均成交额不足({(amount_ma20 or 0.0) / 1e4:.0f}万)')

    volatility = _finite(row.get('volatility_20'))
    if volatility is not None and volatility > tier.max_volatility:
        blocked.append(f'20日波动率{volatility:.0%}超限')
    atr_pct = _finite(row.get('atr_pct'))
    if atr_pct is not None and atr_pct > tier.max_atr_pct:
        blocked.append(f'ATR占比{atr_pct:.1%}超限')

    # 涨跌停/一字板：用推荐日原始价判定当日是否封板，成交与否交给撮合层。
    # 买入侧 T 日涨停**不再拦截**（T 日涨停 ≠ T+1 买不到），改为标注
    # ``limit_up_risk``；卖出侧跌停仍拦截：当日跌停说明次日按参考价卖出大概率失败。
    limit_up_risk, limit_up_basis = (_limit_up_status(row) if side == 'buy'
                                     else (False, ''))
    if limit_up_risk:
        notes.append(f'T日涨停（{limit_up_basis}）：次日可能买不进，'
                     f'能否成交由撮合层按 T+1 开盘价判定')
    block = _RULES.block_reason(
        side,
        prev_close=_finite(row.get('raw_prev_close')),
        open_price=_finite(row.get('raw_open')),
        high=_finite(row.get('raw_high')),
        low=_finite(row.get('raw_low')),
        close=raw_close,
        volume=volume,
        ts_code=str(row.get('ts_code', '')),
        is_st=None,
        trade_date=row.get('date'),
    )
    if block == 'limit_up' and side == 'buy':
        pass  # 已由 limit_up_risk 标注承载，不拦截
    elif block == 'limit_down' and side == 'sell':
        blocked.append('当日跌停，次日可能无法卖出')
    elif block is not None:
        blocked.append('当日停牌或无成交，无法判定可成交性')

    # ST 三态：st 直接拦截；unknown 只提示，不臆测为「非 ST」
    if st_status == 'st':
        blocked.append('ST/*ST 风险警示，不建仓')
    elif st_status == 'unknown' or st_status is None:
        notes.append('ST状态未知（无当日快照），非「非 ST」')

    if blocked:
        return EligibilityResult(False, notes + blocked, '；'.join(blocked),
                                 limit_up_risk=limit_up_risk,
                                 limit_up_basis=limit_up_basis)
    return EligibilityResult(True, notes, '', limit_up_risk=limit_up_risk,
                             limit_up_basis=limit_up_basis)
