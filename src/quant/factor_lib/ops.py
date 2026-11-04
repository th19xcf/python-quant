#!/usr/bin/env python3

"""
因子算子层 —— 时序算子按 ``ts_code`` 分组、横截面算子按 ``date`` 分组。

所有公开因子公式（Alpha101 / GTJA191 / Alpha158）都只用这里的算子拼装，
保证同一算子在所有因子、所有回测里口径一致：

    - 时序算子统一 ``.over('ts_code')`` 分组，要求面板已按 (ts_code, date) 排序；
    - 滚动窗口一律要求「满窗」（``min_samples=n``），前 n-1 行为 null，
      宁可丢样本也不要用残窗制造不稳定值；
    - ``ts_rank`` 输出 (0, 1] 的百分位（1-based rank / n），与公开实现
      ``rankdata(...)[-1] / n`` 同量纲；
    - 横截面 ``rank`` 输出 (0, 1] 的百分位，与 Alpha101 定义一致；
    - ``scale`` 为 Alpha101 的 ``x * a / sum(|x|)`` 横截面缩放；
    - 除法一律走 ``safe_div``，避免 0 除产生 inf 污染后续 rank。

窗口内极值位置（``ts_argmax`` / ``ts_argmin``）：
    Alpha101 定义为「窗口内最大值/最小值出现在第几天」，本模块返回
    **自窗口最老一天起算的 0-based 索引**（最老 = 0，最新 = n-1），
    与 ``numpy.argmax`` / ``pandas.rolling().apply(np.argmax)`` 一致；
    窗口内出现并列极值时取最早出现的位置。polars 没有窗口 argmax 算子，
    这里用 ``numpy`` 滑窗 + 分块求值（C 层向量化，无逐行 Python 循环），
    并显式按 ``ts_code`` 分段。窗口内任何位置为 null 时结果返回 null，
    与其余「满窗」算子口径一致。

回归斜率（``rolling_slope``）：
    对窗口内固定自变量 x = 0..n-1 做 OLS，闭式解
    ``slope = (n*Σ(x*y) - Σx*Σy) / (n*Σx² - (Σx)²)``，
    Σx / Σx² 是常数，Σ(x*y) 用 ``delay`` 展开后逐项相加，
    全程走 polars 表达式，没有 Python 回调。
"""

from __future__ import annotations

import numpy as np
import polars as pl

#: 时序分组键（单标的的时间序列）
GROUP = 'ts_code'

#: 横截面分组键（同一交易日的全部标的）
CROSS = 'date'


def delay(expr: pl.Expr, n: int = 1) -> pl.Expr:
    """滞后 n 期（按标的内部），等价 Alpha101 的 delay(x, n)。"""
    return expr.shift(n).over(GROUP)


def delta(expr: pl.Expr, n: int = 1) -> pl.Expr:
    """n 期差分，等价 Alpha101 的 delta(x, n)。"""
    return expr - delay(expr, n)


def ts_sum(expr: pl.Expr, n: int) -> pl.Expr:
    """n 期滚动求和（满窗）。"""
    return expr.rolling_sum(window_size=n, min_samples=n).over(GROUP)


def ts_mean(expr: pl.Expr, n: int) -> pl.Expr:
    """n 期滚动均值（满窗）。"""
    return expr.rolling_mean(window_size=n, min_samples=n).over(GROUP)


def ts_std(expr: pl.Expr, n: int) -> pl.Expr:
    """n 期滚动标准差（样本标准差 ddof=1，与 pandas rolling().std() 一致）。"""
    return expr.rolling_std(window_size=n, min_samples=n, ddof=1).over(GROUP)


def ts_min(expr: pl.Expr, n: int) -> pl.Expr:
    """n 期滚动最小值（满窗）。"""
    return expr.rolling_min(window_size=n, min_samples=n).over(GROUP)


def ts_max(expr: pl.Expr, n: int) -> pl.Expr:
    """n 期滚动最大值（满窗）。"""
    return expr.rolling_max(window_size=n, min_samples=n).over(GROUP)


#: 窗口 argmax 分块求值的最大行数（每块需 n 倍布尔临时量，分块控内存）
_ARG_CHUNK_ROWS = 200_000


def _rolling_arg_np(values: np.ndarray, n: int, *, find_max: bool) -> np.ndarray:
    """单标的序列的窗口 argmax/argmin，返回 0-based 窗口内索引（NaN 表示无效）。

    ``np.lib.stride_tricks.sliding_window_view`` 是零拷贝视图，argmax 在 C 层
    完成；分块只为限制 ``isnan`` 产生的临时布尔数组大小。
    """
    length = values.size
    out = np.full(length, np.nan, dtype=np.float64)
    if n < 1 or length < n:
        return out
    arg = np.argmax if find_max else np.argmin
    stop_last = length - n + 1
    for start in range(0, stop_last, _ARG_CHUNK_ROWS):
        stop = min(start + _ARG_CHUNK_ROWS, stop_last)
        block = np.lib.stride_tricks.sliding_window_view(
            values[start : stop + n - 1], n
        )
        valid = ~np.isnan(block).any(axis=1)
        idx = arg(block, axis=1).astype(np.float64)
        out[start + n - 1 : stop + n - 1] = np.where(valid, idx, np.nan)
    return out


def rolling_argmax(values: pl.Series, n: int) -> pl.Series:
    """单标的窗口 argmax（0-based，窗口最老一天为 0），满窗才有效。"""
    raw = values.to_numpy().astype(np.float64, copy=False)
    return pl.Series(
        'ts_argmax', _rolling_arg_np(raw, n, find_max=True), nan_to_null=True
    )


def rolling_argmin(values: pl.Series, n: int) -> pl.Series:
    """单标的窗口 argmin（0-based，窗口最老一天为 0），满窗才有效。"""
    raw = values.to_numpy().astype(np.float64, copy=False)
    return pl.Series(
        'ts_argmin', _rolling_arg_np(raw, n, find_max=False), nan_to_null=True
    )


def _arg_frame(
    frame: pl.DataFrame, expr: pl.Expr, n: int, *, find_max: bool, name: str
) -> pl.DataFrame:
    """把 ``expr`` 落列后按 ``ts_code`` 分段求窗口 argmax/argmin 并写回 ``name``。"""
    src = f'_arg_src_{name}'
    staged = stage(frame, **{src: expr})
    values = staged[src].to_numpy().astype(np.float64, copy=False)
    # 面板按 (ts_code, date) 排序，同一标的连续；rle_id 一次扫完给出连续段编号
    codes = staged[GROUP].rle_id().to_numpy()
    result = np.full(values.size, np.nan, dtype=np.float64)
    starts = np.concatenate(([0], np.flatnonzero(np.diff(codes)) + 1))
    ends = np.concatenate((starts[1:], [codes.size]))
    for start, end in zip(starts, ends, strict=True):
        result[start:end] = _rolling_arg_np(
            values[start:end], n, find_max=find_max
        )
    return staged.with_columns(
        pl.Series(name=name, values=result, nan_to_null=True)
    ).drop(src)


def ts_argmax(
    frame: pl.DataFrame, expr: pl.Expr, n: int, *, name: str = '_ts_argmax'
) -> pl.DataFrame:
    """窗口内最大值位置（0-based，自窗口最老一天起算），需先有面板框架。

    与 Alpha101 ``ts_argmax`` 语义对齐：返回 ``argmax`` 的 0-based 索引，
    并取并列极值中最早出现的位置；窗口不满 n 行或含 null 时结果为 null。
    返回带 ``name`` 列的新 frame，供后续阶段（rank / decay_linear）使用。
    """
    return _arg_frame(frame, expr, n, find_max=True, name=name)


def ts_argmin(
    frame: pl.DataFrame, expr: pl.Expr, n: int, *, name: str = '_ts_argmin'
) -> pl.DataFrame:
    """窗口内最小值位置（0-based，自窗口最老一天起算），其余同 ``ts_argmax``。"""
    return _arg_frame(frame, expr, n, find_max=False, name=name)


def rolling_slope(expr: pl.Expr, n: int) -> pl.Expr:
    """n 期窗口对窗口内序号 0..n-1 的 OLS 回归斜率（闭式解，向量化）。

    ``slope = (n*Σ(x*y) - Σx*Σy) / (n*Σx² - (Σx)²)``，x 固定为 0..n-1，
    分母是常数；Σ(x*y) 用 ``delay`` 展开逐项求和，窗口内 null 沿加法传播。
    满窗才有效（前 n-1 行为 null）。
    """
    if n < 2:  # noqa: PLR2004 —— 单点无法定义斜率
        return pl.lit(None, dtype=pl.Float64)
    sum_t = float(n * (n - 1) / 2)
    sum_t2 = float((n - 1) * n * (2 * n - 1) / 6)
    denom = float(n) * sum_t2 - sum_t**2
    # x 从窗口最老一天起算为 0，故延迟 k 天对应 x = n-1-k；末项权重为 0
    # 仍然保留，用于把窗口内任意位置的 null 传播成 null
    terms = [delay(expr, k) * float(n - 1 - k) for k in range(n)]
    weighted = terms[0]
    for term in terms[1:]:
        weighted = weighted + term
    return safe_div(weighted * float(n) - ts_sum(expr, n) * sum_t, pl.lit(denom))


def ts_rank(expr: pl.Expr, n: int) -> pl.Expr:
    """n 期滚动分位排名，输出 (0, 1]（1-based rank / n）。"""
    if n < 2:  # noqa: PLR2004 —— 窗口为 1 时没有可比较的样本
        return pl.when(expr.is_not_null()).then(1.0).otherwise(None)
    ranked = expr.rolling_rank(window_size=n, min_samples=n)
    return (ranked / float(n)).over(GROUP)


def ts_count(cond: pl.Expr, n: int) -> pl.Expr:
    """n 期条件成立次数（满窗）；null 视为不成立。"""
    flags = cond.fill_null(False).cast(pl.Int32)
    return flags.rolling_sum(window_size=n, min_samples=n).over(GROUP)


def ts_product(expr: pl.Expr, n: int) -> pl.Expr:
    """n 期滚动乘积。

    仅对严格正值有意义（内部走 exp(sum(log))），负值/零会得到 null，
    登记表中依赖该算子的因子标为近似。
    """
    positive = pl.when(expr > 0).then(expr.log()).otherwise(None)
    return ts_sum(positive, n).exp()


def corr(a: pl.Expr, b: pl.Expr, n: int) -> pl.Expr:
    """n 期滚动皮尔逊相关。

    polars 2.x 没有 ``Expr.rolling_corr``，用 ``rolling_cov / (std_a * std_b)``
    等价实现；任一侧方差为 0 时返回 null，避免 inf。
    """
    cov = pl.rolling_cov(a, b, window_size=n, min_samples=n).over(GROUP)
    std_a = ts_std(a, n)
    std_b = ts_std(b, n)
    return safe_div(cov, std_a * std_b)


def covariance(a: pl.Expr, b: pl.Expr, n: int) -> pl.Expr:
    """n 期滚动协方差（ddof=1，与 pandas rolling().cov() 一致）。"""
    return pl.rolling_cov(a, b, window_size=n, min_samples=n).over(GROUP)


def decay_linear(expr: pl.Expr, n: int) -> pl.Expr:
    """线性衰减加权平均，权重 n, n-1, ..., 1（近期权重最高）。

    用 ``sum_horizontal([delay(x, i) * (n - i)])`` 精确展开，避免
    ``rolling_map`` 的 Python 回调开销；不满窗时返回 null。
    """
    divisor = float(n * (n + 1) / 2)
    numerator = pl.sum_horizontal([delay(expr, i) * float(n - i) for i in range(n)])
    return pl.when(delay(expr, n - 1).is_not_null()).then(numerator / divisor).otherwise(None)


def signed_power(expr: pl.Expr, power: float | pl.Expr = 2.0) -> pl.Expr:
    """带符号幂 sign(x) * |x|^power，等价 Alpha101 的 signedpower。

    ``power`` 既可以是常数，也可以是逐行变化的表达式（如 Alpha#84 的
    ``delta(close,5)``）。
    """
    return expr.sign() * expr.abs().pow(power)


def rank_cs(expr: pl.Expr) -> pl.Expr:
    """横截面分位排名，输出 (0, 1]。

    polars ``rank()`` 默认跳过 null，rank 落在 1..count，
    再除以当日非空数量得到 Alpha101 口径的 ``rank(x)``。
    """
    return safe_div(expr.rank().over(CROSS), expr.count().over(CROSS).cast(pl.Float64))


def scale(expr: pl.Expr, a: float = 1.0) -> pl.Expr:
    """横截面缩放 ``x * a / sum(|x|)``，等价 Alpha101 的 scale(x, a)。"""
    return safe_div(expr * a, expr.abs().sum().over(CROSS))


def ind_neutral(expr: pl.Expr) -> pl.Expr:
    """行业中性化的近似实现：横截面去均值。

    项目当前没有行业分类维表，真正的行业中性（行业哑变量回归取残差）
    无法落地；本函数只做全市场 demean，登记表中标为近似。
    """
    return expr - expr.mean().over(CROSS)


def safe_div(numerator: pl.Expr, denominator: pl.Expr) -> pl.Expr:
    """安全除法：分母为 0 / null 时返回 null，不产生 inf。"""
    return pl.when((denominator != 0) & denominator.is_not_null()).then(
        numerator / denominator
    ).otherwise(None)


def log(expr: pl.Expr) -> pl.Expr:
    """自然对数，非正值返回 null（避免 NaN/inf）。"""
    return pl.when(expr > 0).then(expr.log()).otherwise(None)


def sign(expr: pl.Expr) -> pl.Expr:
    """符号函数，null 保持 null。"""
    return expr.sign()


def count_gt(expr: pl.Expr, threshold: float, n: int) -> pl.Expr:
    """n 期 ``expr > threshold`` 成立次数，用于 CNTP / SUMP 类因子。"""
    return ts_count(expr > threshold, n)


# ----------------------------------------------------------------------
# 分阶段求值 —— polars 窗口语义的硬约束
# ----------------------------------------------------------------------
# polars 有两条必须绕开的限制（实测 v2.1）：
#   1. ``rolling_cov`` 的入参不能是带 ``.over()`` 的表达式，否则整列静默变 null；
#   2. 横截面算子（``.over('date')``）套在时序算子（``.over('ts_code')``）
#      外层时同样整列 null；反过来（时序套横截面）是允许的。
# 因此凡是「时序值 → 横截面排名」或「排名 → 滚动相关」的组合，都必须先把
# 中间结果落成真实列，再在列上做下一步。``stage`` / ``out`` 就是这套机制：
#
#     f = stage(frame, x=ts_over_expr)          # 落列
#     f = stage(f, rx=rank_cs(pl.col('x')))     # 横截面排名（作用在列上）
#     out(f, -1 * corr(pl.col('rx'), pl.col('y'), 6))


def stage(frame: pl.DataFrame, **exprs: pl.Expr) -> pl.DataFrame:
    """把若干表达式落成真实列（分阶段求值的唯一入口）。"""
    return frame.with_columns([expr.alias(name) for name, expr in exprs.items()])


def out(frame: pl.DataFrame, expr: pl.Expr) -> pl.Series:
    """求值最终表达式并返回与面板等长的 Series。"""
    return frame.select(expr.alias('_factor_out'))['_factor_out']


__all__ = [
    'CROSS',
    'GROUP',
    'corr',
    'count_gt',
    'covariance',
    'decay_linear',
    'delta',
    'delay',
    'ind_neutral',
    'log',
    'out',
    'rank_cs',
    'rolling_argmax',
    'rolling_argmin',
    'rolling_slope',
    'safe_div',
    'scale',
    'sign',
    'signed_power',
    'stage',
    'ts_argmax',
    'ts_argmin',
    'ts_count',
    'ts_max',
    'ts_mean',
    'ts_min',
    'ts_product',
    'ts_rank',
    'ts_std',
    'ts_sum',
]
