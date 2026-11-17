"""ETF 仓位择时：五档映射、滞回推进与风格槽位分配（纯函数）。

设计口径（2026-11-17 定版）：
- 五档目标仓位：0% / 25% / 50% / 75% / 100%，分界为 ±0.20、±0.60。
- 滞回带 0.20：进入更高一档需总分 ≥ 该档下边界 + 0.20；降到更低一档需
  总分 ≤ 当前档下边界 - 0.20。
- 单日最多调整一档：即使总分跨越多档，也只降/升一档，避免连续跳档下单。
- 状态完全由历史总分序列推得，不依赖外部存储，任意一天重算结果一致。
"""

from __future__ import annotations

from collections.abc import Sequence
from math import inf, isfinite

#: 五档仓位（由低到高）
LEVEL_POSITIONS: tuple[float, ...] = (0.0, 0.25, 0.50, 0.75, 1.0)
#: 五档标签
LEVEL_LABELS: tuple[str, ...] = ('强下行', '弱势下行', '震荡', '弱上行', '强趋势上行')
#: 各档下边界（第 0 档无下界）
LEVEL_LOWER_BOUNDS: tuple[float, ...] = (-inf, -0.60, -0.20, 0.20, 0.60)
#: 分档边界，供 UI 与文档引用
BAND_EDGES: tuple[float, float, float, float] = (-0.60, -0.20, 0.20, 0.60)
#: 滞回带宽度（0.20 即带宽的一半）
HYSTERESIS = 0.20
#: 风格占优判定阈值
STYLE_THRESHOLD = 0.20

#: 权益内部槽位：基准 / 规模 / 价值 / 低波
SLEEVE_WEIGHTS: dict[str, float] = {'core': 0.30, 'size': 0.25, 'value': 0.25, 'vol': 0.20}
CORE_ETF = '510300.SH'
SIZE_ETFS: dict[str, str] = {'小盘': '512100.SH', '大盘': '510050.SH', '均衡': CORE_ETF}
VALUE_ETFS: dict[str, str] = {'价值': '510880.SH', '成长': '159915.SZ', '均衡': CORE_ETF}
VOL_ETFS: dict[str, str] = {'低波': '512890.SH', '高波': '588000.SH', '均衡': CORE_ETF}

ETF_NAMES: dict[str, str] = {
    '510300.SH': '沪深300ETF',
    '510050.SH': '上证50ETF',
    '512100.SH': '中证1000ETF',
    '510880.SH': '红利ETF',
    '159915.SZ': '创业板ETF',
    '512890.SH': '红利低波ETF',
    '588000.SH': '科创50ETF',
}


def raw_level(score: float) -> int:
    """不含滞回的原始档位。边界取「上沿含、下沿不含」。"""
    if not isfinite(score):
        raise ValueError(f'score 必须是有限数值：{score!r}')
    if score >= 0.60:
        return 4
    if score >= 0.20:
        return 3
    if score > -0.20:
        return 2
    if score > -0.60:
        return 1
    return 0


def level_position(level: int) -> float:
    """档位对应的目标仓位。"""
    if not 0 <= level < len(LEVEL_POSITIONS):
        raise ValueError(f'非法档位：{level!r}')
    return LEVEL_POSITIONS[level]


def level_label(level: int) -> str:
    if not 0 <= level < len(LEVEL_LABELS):
        raise ValueError(f'非法档位：{level!r}')
    return LEVEL_LABELS[level]


def next_thresholds(level: int, hysteresis: float = HYSTERESIS) -> tuple[float | None, float | None]:
    """返回（上调一档所需总分, 下调一档所需总分）；已到顶/底对应项为 None。"""
    if not 0 <= level < len(LEVEL_POSITIONS):
        raise ValueError(f'非法档位：{level!r}')
    up = LEVEL_LOWER_BOUNDS[level + 1] + hysteresis if level < len(LEVEL_POSITIONS) - 1 else None
    down = LEVEL_LOWER_BOUNDS[level] - hysteresis if level > 0 else None
    return up, down


def step_level(level: int, score: float, hysteresis: float = HYSTERESIS) -> int:
    """单日推进一档：最多上调或下调一档，跨越多个边界也不连跳。"""
    up, down = next_thresholds(level, hysteresis)
    if up is not None and score >= up:
        return level + 1
    if down is not None and score <= down:
        return level - 1
    return level


def run_levels(
    scores: Sequence[float],
    hysteresis: float = HYSTERESIS,
    initial_level: int | None = None,
) -> list[int]:
    """从历史起点逐日推进档位；``initial_level`` 缺省时用首个有效总分的原始档位。"""
    levels: list[int] = []
    level = initial_level
    for score in scores:
        if level is None:
            if not isfinite(score):
                continue
            level = raw_level(score)
        elif isfinite(score):
            level = step_level(level, score, hysteresis)
        levels.append(level)
    return levels


def style_bucket(score: float | None, positive: str, negative: str,
                 threshold: float = STYLE_THRESHOLD) -> str:
    """单维度标签：正负阈值外判占优，否则均衡。"""
    if score is None or not isfinite(score):
        return '均衡'
    if score >= threshold:
        return positive
    if score <= -threshold:
        return negative
    return '均衡'


def sleeve_slots(
    scale_score: float | None,
    value_score: float | None,
    vol_score: float | None,
    threshold: float = STYLE_THRESHOLD,
) -> dict[str, str]:
    """三维得分 → 槽位标签（规模 / 价值 / 低波）。"""
    return {
        'size': style_bucket(scale_score, '小盘', '大盘', threshold),
        'value': style_bucket(value_score, '价值', '成长', threshold),
        'vol': style_bucket(vol_score, '高波', '低波', threshold),
    }


def sleeve_allocation(
    slots: dict[str, str],
) -> dict[str, float]:
    """槽位标签 → 各 ETF 在权益内部的目标权重（合计 1.0）。"""
    weights: dict[str, float] = {}

    def add(code: str, weight: float) -> None:
        weights[code] = round(weights.get(code, 0.0) + weight, 10)

    add(CORE_ETF, SLEEVE_WEIGHTS['core'])
    add(SIZE_ETFS[slots['size']], SLEEVE_WEIGHTS['size'])
    add(VALUE_ETFS[slots['value']], SLEEVE_WEIGHTS['value'])
    add(VOL_ETFS[slots['vol']], SLEEVE_WEIGHTS['vol'])
    return weights


def allocation_rows(slots: dict[str, str]) -> list[tuple[str, str, str]]:
    """按槽位返回（代码, 角色, 标签），均衡槽并入核心的不重复列示。"""
    rows: list[tuple[str, str, str]] = [(CORE_ETF, '核心', '沪深300')]
    for key, name in (('size', '规模'), ('value', '价值'), ('vol', '低波')):
        code = {'size': SIZE_ETFS, 'value': VALUE_ETFS, 'vol': VOL_ETFS}[key][slots[key]]
        if code != CORE_ETF:
            rows.append((code, name, slots[key]))
    return rows
