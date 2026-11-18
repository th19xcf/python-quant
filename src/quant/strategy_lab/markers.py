"""调仓标记的方向判定：离线出图与试算页共用同一套口径。

方向只有三档，判定只在这里实现一次（``src/quant/strategy_lab/charts.py`` 的
「调仓 / 持仓」面板与 ``src/ui/strategy_lab/page.py`` 的 ``_plot_markers`` 都调
:func:`marker_side`，不在两边各写一份）：

- 有 ``reason``：``take_profit`` 判卖出（绿色下三角）；``initial`` / ``restart``
  / ``add`` 判买入（红色上三角）——首次建仓、清仓后重启与补仓同属买入；
- 没有可判定的 ``reason``、但带 ``position``（仓位择时 / 强弱比切换）：与上一笔
  仓位比较，上升算买入、下降算卖出，持平视为无方向（中性圆点）；
- 既无 ``reason`` 也无 ``position``（选股规则的成分变动）：无方向（中性圆点）。

``prev_position`` 只被带 ``position`` 的标记更新，调用方按日期顺序推进；
日期不在净值轴上的标记由调用方在调用前跳过，不参与推进。
"""
from __future__ import annotations

from typing import Any, Dict, Tuple

#: 判为买入的 reason：首次建仓 / 清仓后重启 / 补仓
BUY_REASONS = ('initial', 'restart', 'add')
#: 判为卖出的 reason：止盈清仓
SELL_REASONS = ('take_profit',)


def marker_side(marker: Dict[str, Any],
                prev_position: float = 0.0) -> Tuple[str | None, float]:
    """返回 ``(方向, 更新后的上一笔仓位)``；方向为 ``'buy'`` / ``'sell'`` / ``None``。"""
    reason = marker.get('reason')
    position = marker.get('position')
    if reason in SELL_REASONS:
        side = 'sell'
    elif reason in BUY_REASONS:
        side = 'buy'
    elif position is not None:
        delta = float(position) - float(prev_position)
        side = 'buy' if delta > 0 else ('sell' if delta < 0 else None)
    else:
        side = None
    return side, (float(position) if position is not None else prev_position)
