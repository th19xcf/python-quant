#!/usr/bin/env python3

"""
筹码分布（衰减 VAP）正式实现。

口径与 ``tools/chip_prototype.py`` 保持一致，差别只在于数据入口：
- 正式实现不读本地 parquet，由调用方传入已经按当前复权口径处理过的
  ``open/high/low/close/volume`` 行；价格轴与 K 线主图天然同源。
- 换手率来源由 ``turnover_source`` 选择：
  ``'turn'``（A 档）用真实换手率，``'proxy'``（B 档）走
  ``volume / MA20(volume)`` 代理（3%/日基准，夹在 [2%, 10%]）。
  A 档下限由 ``turnover_floor`` 控制（百分数，``None`` 表示不钳制）：
  默认 ``0.2``，可用预设 ``'floor_2'`` 回看旧口径；``turn<=0`` 按
  「当日不衰减」处理，不被下限强撑。``turn`` 缺失的行逐行回退 B 档。
- 结果里记录真实换手率行占比 ``turn_source_ratio`` 与钳制行占比
  ``clamped_ratio``（并拆分为下限抬起 / 上限压住两类）。
- ``turn`` 必须与当日 bar 同行对齐（由调用方按日期先行连接），
  本模块只做同下标运算，不使用 shift/rolling，故不引入未来函数。
- 价格箱默认覆盖调用方给出的“当前可视价格区间”；传入 ``None`` 时覆盖
  该标的历史全区间（对应界面的「全区间」开关）。
"""

from dataclasses import dataclass

import numpy as np
import polars as pl
from scipy.signal import find_peaks

# 衰减参数：与原型一致，量比 1 对应 3%/日
DECAY_MIN = 0.02
DECAY_MAX = 0.10
DECAY_BASE = 0.03
# 真实换手率上限（百分数）；下限由 turnover_floor 按票/档位配置
TURNOVER_CAP = 10.0
# A 档下限预设：raw 不钳制（仅 turn<=0 不衰减），0.2% 为默认推荐档；
# floor_0p05 适配银行等超低换手大盘股；auto_p10 按个股历史换手 p10 自适应。
AUTO_FLOOR_MODE = 'auto_p10'
# auto_p10 的保护区间：低于 0.02% 会让记忆近乎无限，高于 0.2% 又回到旧钳制问题
AUTO_FLOOR_BOUNDS: tuple[float, float] = (0.02, 0.2)
TURNOVER_FLOOR_PRESETS: dict[str, float | None | str] = {
    'raw': None,
    'floor_0p05': 0.05,
    'floor_0p2': 0.2,
    'floor_2': 2.0,
    'auto_p10': AUTO_FLOOR_MODE,
}
TURNOVER_FLOOR_LABELS: dict[str, str] = {
    'raw': '原始换手（仅防零）',
    'floor_0p05': '下限 0.05%',
    'floor_0p2': '下限 0.2%',
    'floor_2': '下限 2%（旧口径）',
    'auto_p10': '自动 p10（0.02~0.2%，默认）',
}
DEFAULT_TURNOVER_FLOOR_MODE = 'auto_p10'
# 钳制行占比超过该阈值时提示"形态仍依赖假设"
CLAMPED_WARN_THRESHOLD = 0.80
DEFAULT_TURNOVER_FLOOR = TURNOVER_FLOOR_PRESETS[DEFAULT_TURNOVER_FLOOR_MODE]
# 默认价格箱数
DEFAULT_BINS = 200
# 主峰之间的最小间隔（箱），避免同一峰被拆成多个相邻尖峰
PEAK_MIN_DISTANCE = 6

_PRICE_COLUMNS = ('open', 'high', 'low', 'close')
_ADJUSTED_PREFIX = {'qfq': 'qfq_', 'hfq': 'hfq_'}


@dataclass(frozen=True)
class ChipPeak:
    """一个筹码峰。"""

    price: float
    share: float
    bin_index: int


@dataclass(frozen=True)
class DecayStats:
    """A 档衰减口径统计（占比分母为有效 turn 行数）。"""

    turn_source_ratio: float = 0.0
    clamped_low_ratio: float = 0.0
    clamped_high_ratio: float = 0.0
    turnover_floor: float | None = None

    @property
    def clamped_ratio(self) -> float:
        """被钳制（下限抬起 + 上限压住）的行占比。"""
        return self.clamped_low_ratio + self.clamped_high_ratio


@dataclass(frozen=True)
class ChipProfile:
    """一次筹码分布计算结果（单位为百分比）。"""

    edges: np.ndarray
    shares: np.ndarray
    centers: np.ndarray
    peaks: list[ChipPeak]
    last_close: float
    total_mass: float
    price_range: tuple[float, float]
    full_range: tuple[float, float]
    bins: int
    warmup_rows: int
    turnover_source: str
    turn_source_ratio: float = 0.0
    turnover_floor: float | None = None
    clamped_ratio: float = 0.0
    clamped_low_ratio: float = 0.0
    clamped_high_ratio: float = 0.0

    @property
    def max_share(self) -> float:
        """最大单箱占比（%），用于确定右栏横向刻度。"""
        return float(self.shares.max()) if self.shares.size else 0.0


def _resolve_adjusted_frame(
    df: pl.DataFrame, adjustment_type: str | None = None
) -> pl.DataFrame:
    """选出可用于计算的列，并在需要时按复权后缀取价。

    正常情况下 DataFetcher 出口的 ``open/high/low/close`` 已是请求的复权口径；
    这里再按 ``qfq_*`` / ``hfq_*`` 兜底一次，保证与主图价格轴同源。
    """
    frame = df
    prefix = _ADJUSTED_PREFIX.get(adjustment_type or '')
    if prefix is not None:
        adjusted = [col for col in _PRICE_COLUMNS if f'{prefix}{col}' in frame.columns]
        if adjusted:
            frame = frame.with_columns(
                [pl.col(f'{prefix}{col}').alias(col) for col in adjusted]
            )

    needed = [*_PRICE_COLUMNS, 'volume']
    missing = [col for col in needed if col not in frame.columns]
    if missing:
        raise ValueError(f'筹码计算缺少列: {missing}')

    valid = pl.all_horizontal([pl.col(col).is_not_null() for col in needed])
    frame = frame.filter(
        valid & (pl.col('volume') > 0) & (pl.col('high') >= pl.col('low'))
    )
    if 'date' in frame.columns:
        frame = frame.sort('date')
    return frame


TURNOVER_SOURCES = ('turn', 'proxy')


def _proxy_decay(df: pl.DataFrame) -> np.ndarray:
    """B 档代理衰减：``clip(3% * volume/MA20, 2%, 10%)``。"""
    vol = df['volume'].to_numpy().astype(float)
    vol_ma = (
        df.select(pl.col('volume').rolling_mean(window_size=20, min_samples=1).alias('ma'))['ma']
        .to_numpy()
        .astype(float)
    )
    ratio = np.divide(vol, vol_ma, out=np.ones_like(vol), where=vol_ma > 0)
    return np.clip(DECAY_BASE * ratio, DECAY_MIN, DECAY_MAX)


def resolve_turnover_floor(
    turnover_floor: float | None | str, turn: np.ndarray
) -> float | None:
    """把档位配置解析成实际下限（百分数）。

    ``auto_p10`` 取该票历史正换手率的 10% 分位，并夹在
    ``AUTO_FLOOR_BOUNDS``（0.02%~0.2%）内：下界防止极低换手票记忆近乎无限，
    上界防止自适应在低换手票上退化成旧的 2% 强钳制。
    """
    if turnover_floor is None:
        return None
    if isinstance(turnover_floor, str):
        if turnover_floor != AUTO_FLOOR_MODE:
            raise ValueError(
                f'未知 turnover_floor={turnover_floor!r}，允许数值、None 或 {AUTO_FLOOR_MODE!r}'
            )
        positive = turn[np.isfinite(turn) & (turn > 0)]
        if positive.size == 0:
            return None
        return float(np.clip(np.percentile(positive, 10), *AUTO_FLOOR_BOUNDS))
    if not 0 <= turnover_floor <= TURNOVER_CAP:
        raise ValueError(
            f'turnover_floor={turnover_floor!r} 越界，允许 None、'
            f'{AUTO_FLOOR_MODE!r} 或 [0, {TURNOVER_CAP}]'
        )
    return float(turnover_floor)


def decay_rates(
    df: pl.DataFrame,
    turnover_source: str = 'turn',
    turnover_floor: float | None | str = DEFAULT_TURNOVER_FLOOR,
) -> tuple[np.ndarray, int, DecayStats]:
    """计算每日衰减率 d_t，返回 ``(d 数组, MA20 预热行数, A 档统计)``。

    Args:
        turnover_source: ``'turn'``（A 档，真实换手率）或 ``'proxy'``（B 档）。
            A 档下 ``turn`` 缺失或非有限的行逐行回退到代理口径。
        turnover_floor: A 档真实换手率下限（百分数）；``None`` 不钳制；
            ``'auto_p10'`` 按个股正换手率 p10 自适应（夹在 0.02%~0.2%）。
            ``turn<=0``（停牌/无成交）统一按“当日不衰减”处理，不受下限影响。
            上限固定 ``TURNOVER_CAP``（10%）。

    Raises:
        ValueError: 口径未知或下限越界

    说明：``turn`` 与行情帧已按日期同行对齐，本函数只做同下标运算，
    ``rolling_mean`` 仅用于代理口径的历史量比，不读取未来行。
    """
    if turnover_source not in TURNOVER_SOURCES:
        raise ValueError(
            f'未知 turnover_source={turnover_source!r}，允许 {TURNOVER_SOURCES}'
        )
    proxy = _proxy_decay(df)
    warmup = min(df.height, 19)
    if turnover_source == 'proxy' or 'turn' not in df.columns:
        # 代理档不使用 A 档下限，但仍校验配置避免静默错档
        resolve_turnover_floor(turnover_floor, np.empty(0))
        return proxy, warmup, DecayStats()

    turn = df['turn'].to_numpy().astype(float)
    valid = np.isfinite(turn)
    valid_count = int(valid.sum())
    if valid_count == 0:
        resolve_turnover_floor(turnover_floor, turn)
        return proxy, warmup, DecayStats()

    floor = resolve_turnover_floor(turnover_floor, turn)
    # turn<=0 视为无成交：d=0（当日不衰减），既不被下限抬起也不被上限压住
    zero = valid & (turn <= 0)
    above = valid & (turn > TURNOVER_CAP)
    below = (valid & (turn > 0) & (turn < floor)
             if floor is not None else np.zeros_like(valid))
    normal = valid & ~zero & ~above & ~below

    d = proxy.copy()
    d[zero] = 0.0
    d[normal] = turn[normal] / 100.0
    if floor is not None:
        d[below] = floor / 100.0
    d[above] = TURNOVER_CAP / 100.0

    stats = DecayStats(
        turn_source_ratio=valid_count / float(valid.size),
        clamped_low_ratio=float(below.sum()) / valid_count,
        clamped_high_ratio=float(above.sum()) / valid_count,
        turnover_floor=floor,
    )
    # 全量真实换手率时没有 MA20 预热问题
    return d, (0 if valid_count == valid.size else warmup), stats


def triangular_mass(
    lo: np.ndarray, hi: np.ndarray, mode: np.ndarray, edges: np.ndarray
) -> np.ndarray:
    """逐日三角分布落在每个价位箱内的质量占比，形状 ``(n_days, bins)``。"""
    a = lo[:, None]
    b = hi[:, None]
    c = mode[:, None]
    span = np.maximum(b - a, 1e-12)
    x = edges[None, :]
    left = (x - a) ** 2 / (span * np.maximum(c - a, 1e-12))
    right = 1.0 - (b - x) ** 2 / (span * np.maximum(b - c, 1e-12))
    cdf = np.where(x <= a, 0.0, np.where(x <= c, left, np.where(x < b, right, 1.0)))
    mass = np.diff(cdf, axis=1)
    return np.clip(mass, 0.0, None)


def decay_accumulate(mass: np.ndarray, volume: np.ndarray, d: np.ndarray) -> np.ndarray:
    """衰减 VAP 递推：``chip_t = chip_{t-1} * (1 - d_t) + 当日新增``。

    停牌日没有行情行，因此不会进入循环，等价于「无成交则不衰减」。
    """
    chips = np.zeros(mass.shape[1], dtype=float)
    for i in range(mass.shape[0]):
        chips *= 1.0 - d[i]
        chips += mass[i] * volume[i]
    return chips


def find_chip_peaks(
    values: np.ndarray,
    edges: np.ndarray,
    total: float | None = None,
    top_n: int = 3,
) -> list[ChipPeak]:
    """按质量占比找主峰/次峰，峰间至少间隔 ``PEAK_MIN_DISTANCE`` 个箱。

    Args:
        values: 每个价格箱的筹码质量
        edges: 价格箱边界
        total: 全历史筹码总量；None 时用 ``values.sum()``
        top_n: 返回的峰数量
    """
    if values.size == 0 or values.sum() <= 0:
        return []
    denominator = float(values.sum()) if total is None else float(total)
    if denominator <= 0:
        return []
    idx, _ = find_peaks(values, distance=PEAK_MIN_DISTANCE, prominence=values.max() * 0.08)
    if idx.size == 0:
        idx = np.array([int(values.argmax())])
    centers = 0.5 * (edges[:-1] + edges[1:])
    order = idx[np.argsort(values[idx])[::-1]][:top_n]
    return [
        ChipPeak(
            price=float(centers[i]),
            share=float(values[i] / denominator),
            bin_index=int(i),
        )
        for i in order
    ]


def resolve_price_range(
    full_range: tuple[float, float],
    visible_range: tuple[float, float] | None,
    bins: int = DEFAULT_BINS,
) -> np.ndarray:
    """把“是否全区间 + 当前可视价格范围”换算成箱边界。

    Args:
        full_range: 数据全区间 ``(min_low, max_high)``
        visible_range: 主图当前价格轴范围；``None`` 表示全区间
        bins: 箱数

    Returns:
        长度为 ``bins + 1`` 的递增边界；退化为单点时保证仍然可计算。
    """
    low, high = float(full_range[0]), float(full_range[1])
    if not np.isfinite(low) or not np.isfinite(high):
        raise ValueError('价格区间包含非有限值，无法生成筹码箱')

    if visible_range is not None:
        vis_low, vis_high = float(visible_range[0]), float(visible_range[1])
        if np.isfinite(vis_low) and np.isfinite(vis_high) and vis_high > vis_low:
            low = max(low, vis_low)
            high = min(high, vis_high)

    if high <= low:
        # 可视区间与历史价格不重叠时回退到全区间，避免空图
        low, high = float(full_range[0]), float(full_range[1])
    if high <= low:
        high = low + max(abs(low) * 1e-6, 1e-6)
    return np.linspace(low, high, bins + 1)


def compute_chip_profile(
    df: pl.DataFrame,
    *,
    price_range: tuple[float, float] | None = None,
    bins: int = DEFAULT_BINS,
    turnover_source: str = 'turn',
    turnover_floor: float | None | str = DEFAULT_TURNOVER_FLOOR,
    adjustment_type: str | None = None,
) -> ChipProfile:
    """计算单标的筹码分布。

    Args:
        df: 行情数据，至少含 ``open/high/low/close/volume``；可选 ``date`` 与 ``turn``
        price_range: 需要覆盖的价格区间；``None`` 表示全历史区间
        bins: 价格箱数
        turnover_source: ``'turn'``（A 档，真实换手率，缺行自动回退）
            或 ``'proxy'``（B 档，vol/MA20 代理）
        turnover_floor: A 档换手率下限（百分数）；``None`` 不钳制，默认 0.2%；
            ``'auto_p10'`` 按个股正换手率 p10 自适应（夹在 0.02%~0.2%）
        adjustment_type: ``qfq``/``hfq``/``None``，用于兜底取复权价列

    Returns:
        ChipProfile: 归一化到 100% 的筹码分布

    Raises:
        ValueError: 输入为空、缺少列、价格区间非法、换手率来源或下限非法
    """
    frame = _resolve_adjusted_frame(df, adjustment_type)
    if frame.height == 0:
        raise ValueError('筹码计算输入为空（过滤缺失价量后无有效行）')

    bins = max(int(bins), 1)
    low = frame['low'].to_numpy().astype(float)
    high = frame['high'].to_numpy().astype(float)
    close = frame['close'].to_numpy().astype(float)
    volume = frame['volume'].to_numpy().astype(float)

    full_range = (float(low.min()), float(high.max()))
    edges = resolve_price_range(full_range, price_range, bins)

    mode = (low + high + close) / 3.0
    d, warmup, decay_stats = decay_rates(frame, turnover_source, turnover_floor)
    mass = triangular_mass(low, high, mode, edges)
    decay = decay_accumulate(mass, volume, d)

    # 全部筹码质量与价格箱无关（每个三角分布积分恒为 1），
    # 用标量递推得到"全历史筹码总量"。裁剪模式下用它做分母，
    # 视窗外老筹码被裁掉后，剩余柱长之和会小于 100%，与通达信一致。
    full_total = 0.0
    for i in range(volume.size):
        full_total *= 1.0 - d[i]
        full_total += float(volume[i])
    if full_total <= 0:
        raise ValueError('筹码总质量为零，无法归一化')

    shares = decay / full_total * 100.0
    centers = 0.5 * (edges[:-1] + edges[1:])
    return ChipProfile(
        edges=edges,
        shares=shares,
        centers=centers,
        peaks=find_chip_peaks(decay, edges, total=full_total),
        last_close=float(close[-1]),
        total_mass=full_total,
        price_range=(float(edges[0]), float(edges[-1])),
        full_range=full_range,
        bins=bins,
        warmup_rows=warmup,
        turnover_source=('turn: real turnover' if turnover_source == 'turn'
                         else 'proxy: volume/MA20'),
        turn_source_ratio=decay_stats.turn_source_ratio,
        turnover_floor=(decay_stats.turnover_floor if turnover_source == 'turn' else None),
        clamped_ratio=(decay_stats.clamped_ratio if turnover_source == 'turn' else 0.0),
        clamped_low_ratio=(decay_stats.clamped_low_ratio
                           if turnover_source == 'turn' else 0.0),
        clamped_high_ratio=(decay_stats.clamped_high_ratio
                            if turnover_source == 'turn' else 0.0),
    )
