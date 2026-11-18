"""调仓标记的方向判定与点位整理：离线出图与试算页共用同一套口径。

方向只有三档（买 / 卖 / 无方向），判定只在这里实现一次：
``src/quant/strategy_lab/charts.py`` 的「调仓 / 持仓」面板与
``src/ui/strategy_lab/page.py`` 的 ``_plot_markers`` 都调 :func:`layout_markers`。

判定顺序（先命中先返回）：

1. 标记自带 ``direction``（``stock_select`` / ``asset_switch`` 由计划构造时按
   目标权重净变化算好）：直接采用；
2. 有 ``reason``：``take_profit`` 判卖出；``initial`` / ``restart`` / ``add``
   判买入——首次建仓、清仓后重启与补仓同属买入；
3. 没有可判定的 ``reason``、但带 ``position``（仓位择时）：与上一笔仓位比较，
   上升算买入、下降算卖出，持平视为无方向；
4. 既无 ``reason`` 也无 ``position``：无方向（中性圆点）。

选股类（``stock_select``）的方向口径见 :func:`sides_from_weights`：按目标权重
的净变化定方向，同一标的当日只记净变化一笔（与实盘按净持仓记账一致，先卖后买
同一标的相互抵销，保留标的满仓卖出再买回不算动作）；同一调仓日若既有调入/加仓
又有调出/减仓，两侧各出一个标记，不合并成中性点。
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple

#: 判为买入的 reason：首次建仓 / 清仓后重启 / 补仓
BUY_REASONS = ('initial', 'restart', 'add')
#: 判为卖出的 reason：止盈清仓
SELL_REASONS = ('take_profit',)
#: 同日同时有买卖两个标记时，左右各偏半个交易日序号，只为看清两种方向
DODGE = 0.45


def sides_from_weights(prev: Dict[str, float], cur: Dict[str, float],
                       eps: float = 1e-12) -> List[str]:
    """按目标权重的净变化给出当日方向：净增记买入、净减记卖出。

    同一标的只比较「上一期目标权重 → 本期目标权重」的净变化，不按先卖后买拆成
    两笔，因此权重不变的保留标的不产生标记；两侧同时存在时返回
    ``['buy', 'sell']``，由调用方各自出一条标记。
    """
    ups = any(cur[c] - prev.get(c, 0.0) > eps for c in cur)
    downs = any(prev[c] - cur.get(c, 0.0) > eps for c in prev)
    return [side for side, hit in (('buy', ups), ('sell', downs)) if hit]


def marker_side(marker: Dict[str, Any],
                prev_position: float = 0.0) -> Tuple[str | None, float]:
    """返回 ``(方向, 更新后的上一笔仓位)``；方向为 ``'buy'`` / ``'sell'`` / ``None``。"""
    position = marker.get('position')
    reason = marker.get('reason')
    direction = marker.get('direction')
    if direction in ('buy', 'sell'):
        side: str | None = direction
    elif reason in SELL_REASONS:
        side = 'sell'
    elif reason in BUY_REASONS:
        side = 'buy'
    elif position is not None:
        delta = float(position) - float(prev_position)
        side = 'buy' if delta > 0 else ('sell' if delta < 0 else None)
    else:
        side = None
    return side, (float(position) if position is not None else prev_position)


def layout_markers(markers: Iterable[Dict[str, Any]],
                   nav_index: Dict[str, int],
                   dodge: float = DODGE) -> List[Tuple[float, str | None, float]]:
    """整理成可绘制点位 ``(x, 方向, y)``：纵轴取仓位或成分数，缺失日期的标记跳过。

    同一交易日同时出现买卖两个标记时，按方向左右各偏 ``dodge`` 个交易日序号，
    仅用于视觉分离，标记的日期仍是当日。
    """
    prepared: List[List[Any]] = []
    prev_position = 0.0
    for marker in sorted(markers,
                         key=lambda m: nav_index.get(str(m.get('date')), -1)):
        x = nav_index.get(str(marker.get('date')))
        if x is None:
            continue
        side, prev_position = marker_side(marker, prev_position)
        if marker.get('members') is not None:
            y = float(marker['members'])
        else:
            y = float(marker.get('position') or 1.0)
        prepared.append([float(x), side, y])

    sides_by_day: Dict[float, set] = {}
    for x, side, _ in prepared:
        sides_by_day.setdefault(x, set()).add(side)
    for item in prepared:
        sides = sides_by_day[item[0]]
        if 'buy' in sides and 'sell' in sides:
            if item[1] == 'buy':
                item[0] -= dodge
            elif item[1] == 'sell':
                item[0] += dodge
    return [(float(x), side, y) for x, side, y in prepared]
