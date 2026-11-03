#!/usr/bin/env python3

"""可交易性与风险剔除：复用 ``ExecutionRules``，不重写涨跌停规则。"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

from src.advice.config import AdviceConfig, TierConfig
from src.backtest.engine.execution_core import ExecutionRules

_RULES = ExecutionRules()


@dataclass
class EligibilityResult:
    """单标的在给定方向上的可推荐性。"""

    ok: bool
    notes: list[str] = field(default_factory=list)
    blocked_reason: str = ''

    def add_note(self, note: str) -> None:
        if note and note not in self.notes:
            self.notes.append(note)


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


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

    # 涨跌停/一字板：用推荐日原始价判定；次日执行仍可能变化，故写入风险提示
    tradable = _RULES.is_tradable(
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
    if not tradable and side == 'buy':
        blocked.append('当日涨停/一字板，次日可能无法买入')
    elif not tradable:
        blocked.append('当日跌停，次日可能无法卖出')

    # ST 三态：st 直接拦截；unknown 只提示，不臆测为「非 ST」
    if st_status == 'st':
        blocked.append('ST/*ST 风险警示，不建仓')
    elif st_status == 'unknown' or st_status is None:
        notes.append('ST状态未知（无当日快照），非「非 ST」')

    if blocked:
        return EligibilityResult(False, notes + blocked, '；'.join(blocked))
    return EligibilityResult(True, notes, '')
