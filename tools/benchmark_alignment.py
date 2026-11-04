"""统一回测基准对齐口径。

生产评估原先让基准从测试窗首日 ``d0`` 收盘起算，而策略在 ``d1`` 开盘才建仓，
基准因此多承担一段 ``d0→d1`` 的隔夜跳空收益。本模块把基准统一改为 ``d1``
收盘起算：与策略同起点、同一日期集合、同一交易日计数。

所有评估脚本必须复用本模块，禁止各自实现一套基准口径。
"""

from __future__ import annotations

import numpy as np
import polars as pl

from src.advice.factor_eval import _curve_stats


def equal_weight_daily(frame: pl.DataFrame) -> pl.DataFrame:
    """按日等权收益（列 ``date``/``ret``），只用 ``ret`` 与 ``date`` 两列。"""
    return (frame.select('date', 'ts_code', 'ret')
            .drop_nulls()
            .group_by('date')
            .agg(pl.col('ret').mean().alias('ret'))
            .sort('date'))


def aligned_benchmark_curve(frame: pl.DataFrame) -> pl.DataFrame:
    """从 ``d1`` 收盘起算的等权基准净值曲线。

    ``d0`` 被剔除（策略当日不持仓），首个净值点归一为 1.0，与策略首个
    净值点（``d1`` 收盘）对齐；此后按 ``d1→d2`` 起的收盘收益复利。
    """
    daily = equal_weight_daily(frame)
    if daily.height < 2:
        return pl.DataFrame(schema={'date': pl.Date, 'equity': pl.Float64})
    # 剔除 d0：策略当日不持仓，基准也不应从 d0 收盘起算
    daily = daily.slice(1)
    curve = daily.with_columns((pl.col('ret') + 1.0).cum_prod().alias('gross'))
    base = float(curve['gross'][0])
    return (curve.with_columns((pl.col('gross') / base).alias('equity'))
            .select('date', 'equity'))


def unaligned_benchmark_curve(frame: pl.DataFrame) -> pl.DataFrame:
    """修正前的基准曲线：从 ``d0`` 收盘起算，含 ``d0→d1`` 隔夜跳空。"""
    daily = equal_weight_daily(frame)
    if daily.height < 2:
        return pl.DataFrame(schema={'date': pl.Date, 'equity': pl.Float64})
    return daily.select('date', (pl.col('ret') + 1.0).cum_prod().alias('equity'))


def benchmark_window_stats(frame: pl.DataFrame, n_days: int | None = None,
                           aligned: bool = True
                           ) -> tuple[pl.DataFrame, dict[str, float]]:
    """单窗基准统计，返回 ``(净值曲线, 统计)``。

    ``n_days`` 应与策略统计保持一致（测试窗日历交易日数）；缺省时退回曲线
    自身的点数。``aligned=True`` 用修正后口径，``False`` 保留修正前口径。
    """
    curve = aligned_benchmark_curve(frame) if aligned else unaligned_benchmark_curve(frame)
    if curve.height < 2:
        return curve, {}
    stats = _curve_stats(curve, 1.0, n_days if n_days else curve.height)
    return curve, stats


def daily_excess_stats(strategy_curve: pl.DataFrame,
                       benchmark_curve: pl.DataFrame) -> dict[str, float]:
    """策略与基准同日对齐后的日频超额统计。

    返回日均超额、日频 t 值（``mean/std*sqrt(n)``）、年化日均超额，
    以及策略对基准的 beta 与 R²。
    """
    left = strategy_curve.select(
        'date', (pl.col('equity') / pl.col('equity').shift(1) - 1.0).alias('rs'))
    right = benchmark_curve.select(
        'date', (pl.col('equity') / pl.col('equity').shift(1) - 1.0).alias('rb'))
    joined = (left.join(right, on='date', how='inner')
              .drop_nulls(['rs', 'rb']))
    if joined.height < 3:
        return {'n_days': float(joined.height)}
    rs = joined['rs'].to_numpy()
    rb = joined['rb'].to_numpy()
    diff = rs - rb
    n = diff.size
    mean = float(diff.mean())
    std = float(diff.std(ddof=1))
    t_stat = mean / (std / n ** 0.5) if std > 0 else 0.0
    var_b = float(rb.var(ddof=1))
    beta = float(np.cov(rs, rb, ddof=1)[0, 1] / var_b) if var_b > 0 else 0.0
    corr = float(joined.select(pl.corr('rs', 'rb')).item())
    return {
        'n_days': float(n),
        'mean_daily_excess': mean,
        'annual_mean_excess': mean * 252.0,
        'excess_t': t_stat,
        'beta': beta,
        'r2': corr ** 2,
    }


def same_start_excess_annualized(strategy_stats: dict, benchmark_stats: dict,
                                 n_days: int) -> float:
    """同起点同区间的年化超额：``(1+策略)/(1+基准)`` 再按共同天数年化。"""
    strategy_net = float(strategy_stats['net_return'])
    benchmark_net = float(benchmark_stats['net_return'])
    if benchmark_net <= -1.0:
        return 0.0
    ratio = (1.0 + strategy_net) / (1.0 + benchmark_net)
    return ratio ** (252.0 / max(1, n_days)) - 1.0
