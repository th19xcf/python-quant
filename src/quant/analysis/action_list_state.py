"""模型侧操作清单：状态对象与纯函数（不读用户持仓、不接账户、不下单）。"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

#: 台账「变动类型」→ 清单方向
DIRECTION_OF_CHANGE: dict[str, str] = {
    '新建': '买入',
    '加仓': '加仓',
    '减仓': '减持',
    '清仓': '清仓',
    '不变': '持有',
}
#: 方向排序：先卖后买，持有沉底
DIRECTION_ORDER: dict[str, int] = {
    '清仓': 0, '减持': 1, '买入': 2, '加仓': 3, '持有': 4,
}
#: A 股整手
LOT_SIZE = 100


def direction_of(change: str) -> str:
    """台账变动类型 → 操作方向；未知类型按持有处理。"""
    return DIRECTION_OF_CHANGE.get(change, '持有')


def lots_from_amount(amount: float, price: float) -> int:
    """按金额与参考价折算整手股数；价格缺失或金额非正返回 0。"""
    if price <= 0 or amount <= 0:
        return 0
    return int(math.floor(amount / (price * LOT_SIZE))) * LOT_SIZE


@dataclass(frozen=True)
class ActionRow:
    """清单一行：模型侧动作与依据。"""

    code: str
    name: str
    direction: str
    shares: int
    price: float
    target_weight: float
    exec_date: str
    condition: str
    rationale: str


@dataclass(frozen=True)
class ActionListState:
    """一次清单的汇总：日期口径、市场状态与逐行动作。"""

    trade_day: str | None
    signal_day: str | None
    exec_date: str | None
    next_rebalance: str | None
    capital: float
    trend_label: str
    style_direction: str
    rows: tuple[ActionRow, ...] = field(default_factory=tuple)
    notes: tuple[str, ...] = field(default_factory=tuple)
    missing: tuple[str, ...] = field(default_factory=tuple)

    def direction_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in self.rows:
            counts[row.direction] = counts.get(row.direction, 0) + 1
        return counts
