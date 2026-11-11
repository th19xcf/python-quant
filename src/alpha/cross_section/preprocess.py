#!/usr/bin/env python3
"""因子预处理唯一管线：去极值 -> 缺失保留 mask -> 标准化 -> 行业 + lnMV 中性化。

本模块是因子预处理的**唯一正式实现**，替代此前分散在
``src/alpha/cross_section/cross_sectional.py:neutralize``（行业内去均值 + 单暴露
一元回归，两步投影）与 ``tools/research/build_multifactor_signals.py:_neutralize``
（FWL）的两套写法。两者数学上并不等价：前者先按行业去均值、再对 **日期中心化**
的暴露回归，忽略了暴露自身的行业内差异；本模块用 FWL 做
「行业饱和哑变量 + lnMV」的联合 OLS，组内去均值同时作用于因子与暴露，
残差即联合回归残差。

四步顺序固定，可单独调用，也可用 :func:`run_pipeline` 串起来：

1. :func:`winsorize_mad`：``med ± k × 1.4826 × MAD`` 截断，**不改动 null**；
2. :func:`mark_missing`：只标记有效性（``{col}__valid``），**不填 0、不填均值**，
   并把 ``±inf`` 视为缺失转成 null（记录条数）；
3. :func:`standardize`：``zscore``（默认）或 ``rank_gauss``（按日排名 -> 正态分位）；
4. :func:`neutralize_industry_mv`：行业哑变量 + lnMV 联合 OLS 残差。

每一步都返回 ``(frame, StepDiagnostic)``，诊断含逐日样本数、丢弃数、
残差对 lnMV 与行业哑变量的泄漏度、回归 R²、beta；``run_pipeline`` 汇总为
:class:`PipelineResult`。

约定：
- 所有统计量按 ``by``（默认 ``date``）分组，内部用 Polars 窗口表达式实现，
  不需要逐日 Python 循环；
- 输入行序、行数不变，只新增列；新列名 = 原列名 + 阶段后缀
  （``_win`` / ``_std`` / ``_neu``），因此最终列名形如
  ``momentum_1m_win_std_neu``，可从列名直接看出走过的阶段；
- 缺失一律保留 null，不产生任何 0 值填充；下游按 mask 或 null 判定可用性。

示例：
    >>> res = run_pipeline(df, ['momentum_1m'], industry_col='ind_code')
    >>> final = res.columns['momentum_1m']          # 'momentum_1m_win_std_neu'
    >>> res.summary()['neutralize']['totals']       # 丢弃样本数、行业组被剔除数
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final, Literal, cast

import numpy as np
import polars as pl
from scipy.special import ndtri

StandardizeMethod = Literal['rank_gauss', 'zscore']

#: 正态分布下 MAD 与标准差的换算系数（MAD = 0.6745σ）
MAD_SCALE = 1.4826
#: 标准差/方差判零阈值
MIN_STD = 1e-12
#: 默认去极值倍数：med ± 3 × 1.4826 × MAD ≈ ±3σ
DEFAULT_MAD_K = 3.0
#: 默认标准化方式（依据见 docs/因子预处理管线_2026-11-11.md 的 A/B 实测）
DEFAULT_STANDARDIZE: Final[StandardizeMethod] = 'zscore'

WIN_SUFFIX = '_win'
MASK_SUFFIX = '__valid'
STD_SUFFIX = '_std'
NEU_SUFFIX = '_neu'

#: 批量窗口运算的因子分块大小，控制单次物化的内存
_BATCH = 8


@dataclass(frozen=True)
class StepDiagnostic:
    """单步诊断：逐 (by, factor) 明细 + 参数 + 汇总。"""

    step: str
    by: str
    params: dict[str, Any]
    rows: pl.DataFrame

    def summary(self) -> dict[str, Any]:
        """返回可 JSON 序列化的汇总（按 by 求均值，n_ 前缀列另给总和）。"""
        base: dict[str, Any] = {
            'step': self.step,
            'params': dict(self.params),
            'groups': 0,
            'rows': 0,
        }
        if self.rows.is_empty():
            return base
        numeric = [c for c, dt in self.rows.schema.items()
                   if dt.is_numeric() and c != self.by]
        means: dict[str, float | None] = {}
        for c in numeric:
            v = self.rows[c].mean()
            means[c] = None if v is None else float(cast(float, v))
        totals = {c: int(self.rows[c].sum() or 0) for c in numeric if c.startswith('n_')}
        base |= {
            'groups': self.rows[self.by].n_unique(),
            'rows': self.rows.height,
            'per_group_mean': means,
            'totals': totals,
        }
        return base


@dataclass(frozen=True)
class PipelineResult:
    """整链结果：新数据 + 原列到最终列的映射 + 各步诊断。"""

    frame: pl.DataFrame
    columns: dict[str, str]
    diagnostics: tuple[StepDiagnostic, ...]

    def summary(self) -> dict[str, Any]:
        return {d.step: d.summary() for d in self.diagnostics}


def _empty(step: str, by: str, params: dict[str, Any]) -> StepDiagnostic:
    return StepDiagnostic(step=step, by=by, params=params, rows=pl.DataFrame())


def _check(frame: pl.DataFrame, columns: list[str], *, by: str | None = None) -> None:
    if by is not None and by not in frame.columns:
        raise ValueError(f"截面维度列不存在: {by}，可用列: {frame.columns}")
    missing = [c for c in columns if c not in frame.columns]
    if missing:
        raise ValueError(f"因子列不存在: {missing}，可用列: {frame.columns}")


def _industry_expr(frame: pl.DataFrame, industry_col: str) -> pl.Expr:
    """行业有效性表达式：非 null；字符串/分类列额外要求非空串。"""
    e = pl.col(industry_col)
    dtype = frame.schema[industry_col]
    if dtype == pl.String or dtype == pl.Categorical:
        return e.is_not_null() & (e.cast(pl.String) != '')
    return e.is_not_null()


def winsorize_mad(frame: pl.DataFrame,
                  columns: list[str],
                  *,
                  by: str = 'date',
                  k: float = DEFAULT_MAD_K,
                  scale: float = MAD_SCALE,
                  min_obs: int = 30,
                  suffix: str = WIN_SUFFIX,
                  collect_diagnostics: bool = True
                  ) -> tuple[pl.DataFrame, StepDiagnostic]:
    """按 ``med ± k × scale × MAD`` 截断去极值，null 原样保留。

    Args:
        k: 倍数，默认 3（≈ ±3σ）；MAD 取 ``median(|x - med|)``
        scale: MAD 到 σ 的换算系数，默认 1.4826
        min_obs: 单期非空样本下限，不足则**不做截断**（诊断记 ``applied=0``）
        suffix: 新列后缀，产出 ``{col}{suffix}``

    Returns:
        (新增去极值列的数据, 诊断)。``MAD ≈ 0`` 的截面不截断（退化为原值），
        避免「除中位数外全部塌到中位数」。
    """
    _check(frame, columns, by=by)
    if k <= 0:
        raise ValueError(f"k 必须为正: {k}")
    params = {'k': k, 'scale': scale, 'min_obs': min_obs, 'suffix': suffix}
    out = frame
    pieces: list[pl.DataFrame] = []
    for start in range(0, len(columns), _BATCH):
        batch = columns[start:start + _BATCH]
        out = out.with_columns([
            _winsorize_expr(c, by, k, scale, min_obs).alias(f'{c}{suffix}')
            for c in batch
        ])
        if not collect_diagnostics:
            continue
        for c in batch:
            med = pl.col(c).median().over(by)
            mad = (pl.col(c) - med).abs().median().over(by)
            half = k * scale * mad
            lo, hi = med - half, med + half
            pieces.append(frame.select(
                pl.col(by),
                pl.lit(c).alias('factor'),
                pl.len().over(by).alias('n_rows'),
                pl.col(c).is_not_null().sum().over(by).alias('n_obs'),
                ((pl.col(c) < lo) | (pl.col(c) > hi)).sum().over(by).alias('n_clipped'),
                ((mad > MIN_STD) & (pl.col(c).count().over(by) >= min_obs))
                .cast(pl.Int8).alias('applied'),
            ).unique(subset=[by], keep='first'))
    diag = _empty('winsorize_mad', by, params)
    if collect_diagnostics and pieces:
        rows = pl.concat(pieces)
        rows = rows.with_columns(
            (pl.col('n_clipped') / pl.col('n_obs').clip(lower_bound=1))
            .alias('clipped_ratio'))
        diag = StepDiagnostic(step='winsorize_mad', by=by, params=params, rows=rows)
    return out, diag


def _winsorize_expr(c: str, by: str, k: float, scale: float, min_obs: int) -> pl.Expr:
    med = pl.col(c).median().over(by)
    mad = (pl.col(c) - med).abs().median().over(by)
    half = k * scale * mad
    usable = (mad > MIN_STD) & (pl.col(c).count().over(by) >= min_obs)
    return pl.when(usable).then(pl.col(c).clip(med - half, med + half)).otherwise(pl.col(c))


def mark_missing(frame: pl.DataFrame,
                 columns: list[str],
                 *,
                 suffix: str = MASK_SUFFIX,
                 non_finite_to_null: bool = True,
                 collect_diagnostics: bool = True
                 ) -> tuple[pl.DataFrame, StepDiagnostic]:
    """只标记缺失，不填补：新增 ``{col}{suffix}`` 布尔有效性列。

    ``non_finite_to_null=True`` 时把 ``±inf`` 转成 null（inf 不是有效数值，
    参与排名/回归会污染整个截面），并记入 ``n_non_finite``。
    本步**不产生任何 0 值**，也不填补 null。
    """
    _check(frame, columns)
    params = {'suffix': suffix, 'non_finite_to_null': non_finite_to_null}
    numeric = [c for c in columns if frame.schema[c].is_numeric()]
    out = frame
    if non_finite_to_null and numeric:
        out = out.with_columns([
            pl.when(pl.col(c).cast(pl.Float64).is_finite())
            .then(pl.col(c).cast(pl.Float64)).otherwise(None).alias(c) for c in numeric
        ])
    out = out.with_columns([
        pl.when(pl.col(c).is_not_null())
        .then(pl.col(c).cast(pl.Float64).is_finite() if c in numeric else pl.lit(True))
        .otherwise(False).alias(f'{c}{suffix}') for c in columns
    ])
    if not collect_diagnostics:
        return out, _empty('mark_missing', 'factor', params)
    rows = pl.concat([out.select(
        pl.lit(c).alias('factor'),
        pl.len().alias('n_rows'),
        pl.col(c).is_not_null().sum().alias('n_obs'),
        pl.col(c).is_null().sum().alias('n_missing'),
        pl.lit(_non_finite_count(frame, c)).alias('n_non_finite'),
    ) for c in columns])
    diag = StepDiagnostic(step='mark_missing', by='factor', params=params, rows=rows)
    return out, diag


def _non_finite_count(frame: pl.DataFrame, c: str) -> int:
    """入参中 ±inf 的个数（在转 null 之前统计）。"""
    if not frame.schema[c].is_numeric():
        return 0
    s = frame[c].cast(pl.Float64, strict=False)
    return int((s.is_not_null() & ~s.is_finite()).sum())


def _rank_gauss_batch(frame: pl.DataFrame, columns: list[str], by: str,
                      min_obs: int) -> pl.DataFrame:
    """按 by 分组排名 -> (rank-0.5)/n -> 正态分位；null 保持 null。"""
    p = frame.select([
        pl.when(pl.col(c).count().over(by) >= min_obs)
        .then((pl.col(c).rank('average').over(by) - 0.5) / pl.col(c).count().over(by))
        .otherwise(None).alias(c) for c in columns
    ])
    z = ndtri(p.to_numpy())
    return pl.DataFrame(z, schema=columns, nan_to_null=True)


def standardize(frame: pl.DataFrame,
                columns: list[str],
                *,
                by: str = 'date',
                method: StandardizeMethod = DEFAULT_STANDARDIZE,
                min_obs: int = 30,
                suffix: str = STD_SUFFIX,
                collect_diagnostics: bool = True
                ) -> tuple[pl.DataFrame, StepDiagnostic]:
    """按 by 分组标准化：``zscore``（默认）或 ``rank_gauss``。

    Args:
        method: ``zscore`` 为 ``(x-mean)/std(ddof=1)``；``rank_gauss`` 先按截面
            排名再取正态分位，对厚尾/异常值不敏感
        min_obs: 单期非空样本下限，不足则该期置 null

    Returns:
        (新增标准化列的数据, 诊断)。常数截面（std≈0 / 全同秩）一律置 null，
        不返回 0，避免「无区分度」被当成「中位」。
    """
    _check(frame, columns, by=by)
    if method not in ('rank_gauss', 'zscore'):
        raise ValueError(f"未知标准化方式: {method}")
    params = {'method': method, 'min_obs': min_obs, 'suffix': suffix}
    out = frame
    pieces: list[pl.DataFrame] = []
    for start in range(0, len(columns), _BATCH):
        batch = columns[start:start + _BATCH]
        if method == 'rank_gauss':
            z = _rank_gauss_batch(out, batch, by, min_obs)
            out = out.with_columns([z[c].alias(f'{c}{suffix}') for c in batch])
        else:
            out = out.with_columns([
                _zscore_expr(c, by, min_obs).alias(f'{c}{suffix}') for c in batch
            ])
        if not collect_diagnostics:
            continue
        for c in batch:
            res = f'{c}{suffix}'
            pieces.append(out.select(
                pl.col(by),
                pl.lit(c).alias('factor'),
                pl.col(res).is_not_null().sum().over(by).alias('n_obs'),
                pl.col(res).mean().over(by).alias('mean_out'),
                pl.col(res).std(ddof=1).over(by).alias('std_out'),
                (pl.col(c).count().over(by) < min_obs).cast(pl.Int8).alias('short'),
            ).unique(subset=[by], keep='first'))
    diag = _empty('standardize', by, params)
    if collect_diagnostics and pieces:
        diag = StepDiagnostic(step='standardize', by=by, params=params,
                              rows=pl.concat(pieces))
    return out, diag


def _zscore_expr(c: str, by: str, min_obs: int) -> pl.Expr:
    mean = pl.col(c).mean().over(by)
    std = pl.col(c).std(ddof=1).over(by)
    usable = (std > MIN_STD) & (pl.col(c).count().over(by) >= min_obs)
    return pl.when(usable).then((pl.col(c) - mean) / std).otherwise(None)


def _neutralize_parts(c: str, industry_col: str, mv_col: str, ind_ok: pl.Expr,
                      by: str, min_group_size: int
                      ) -> tuple[pl.Expr, pl.Expr, pl.Expr]:
    """返回 (残差, 去均值后的 lnMV, 去均值后的因子) 三个**未按 min_obs 屏蔽**的表达式。"""
    y_raw = pl.col(c).cast(pl.Float64)
    x_raw = pl.col(mv_col).cast(pl.Float64)
    pair_ok = y_raw.is_not_null() & x_raw.is_not_null() & ind_ok
    group_n = pair_ok.sum().over([by, industry_col])
    valid = pair_ok & (group_n >= min_group_size)
    y = pl.when(valid).then(y_raw).otherwise(None)
    x = pl.when(valid).then(x_raw).otherwise(None)
    yt = y - y.mean().over([by, industry_col])
    xt = x - x.mean().over([by, industry_col])
    den = (xt * xt).sum().over(by)
    beta = pl.when(den > MIN_STD).then((xt * yt).sum().over(by) / den).otherwise(0.0)
    return yt - beta * xt, xt, yt


def _neutralize_expr(c: str, industry_col: str, mv_col: str, ind_ok: pl.Expr,
                     by: str, min_group_size: int, min_obs: int) -> pl.Expr:
    """单因子 FWL 残差表达式（行业饱和哑变量 + lnMV 的联合 OLS）。"""
    resid, _xt, _yt = _neutralize_parts(c, industry_col, mv_col, ind_ok, by,
                                        min_group_size)
    enough = (pl.col(c).cast(pl.Float64).is_not_null()
              & pl.col(mv_col).cast(pl.Float64).is_not_null() & ind_ok).sum().over(by) >= min_obs
    return pl.when(enough).then(resid).otherwise(None)


def neutralize_industry_mv(frame: pl.DataFrame,
                           columns: list[str],
                           *,
                           by: str = 'date',
                           industry_col: str = 'ind_code',
                           mv_col: str = 'ln_mv',
                           min_obs: int = 30,
                           min_group_size: int = 5,
                           suffix: str = NEU_SUFFIX,
                           collect_diagnostics: bool = True
                           ) -> tuple[pl.DataFrame, StepDiagnostic]:
    """行业哑变量 + lnMV 联合 OLS 残差（FWL 实现）。

    等价模型：``f = α + Σ_j γ_j·1[行业=j] + β·lnMV + ε``，取 ε。
    实现上先按 ``(by, 行业)`` 同时去均值因子与 lnMV，再对去均值后的 lnMV
    做一元回归，残差即联合回归残差（FWL 定理），不需要展开哑变量矩阵。

    Args:
        min_group_size: 行业内有效样本下限，不足的行业组整体剔除并计数
            （单样本组会让残差恒为 0，等于把该股当作完美拟合）
        min_obs: 单期有效样本下限，不足则该期整体置 null

    Returns:
        (新增残差列的数据, 诊断)。诊断逐日给出：参与回归样本数、四类未入回归数
        （因子缺失 / 市值缺失 / 行业缺失 / 行业组过小；四者**互斥**，按此优先级
        依次判定，因此与 ``n_obs`` 相加恰等于当期行数，可直接对账）、beta、R²、
        残差对 lnMV 的原始相关与组内去均值后相关（后者应≈0，用于自证 FWL）、
        行业组均值残差相对残差标准差的最大倍数（应≈0，用于自证哑变量已被吸收）。
    """
    _check(frame, columns + [mv_col], by=by)
    if industry_col not in frame.columns:
        raise ValueError(f"行业列不存在: {industry_col}")
    params = {'industry_col': industry_col, 'mv_col': mv_col, 'min_obs': min_obs,
              'min_group_size': min_group_size, 'suffix': suffix}
    ind_ok = _industry_expr(frame, industry_col)
    out = frame
    for start in range(0, len(columns), _BATCH):
        batch = columns[start:start + _BATCH]
        out = out.with_columns([
            _neutralize_expr(c, industry_col, mv_col, ind_ok, by,
                             min_group_size, min_obs).alias(f'{c}{suffix}')
            for c in batch
        ])
    diag = _empty('neutralize', by, params)
    if not collect_diagnostics:
        return out, diag
    rows = pl.concat([_neutralize_diag(out, c, industry_col, mv_col, ind_ok, by,
                                       min_group_size, min_obs, suffix)
                      for c in columns])
    diag = StepDiagnostic(step='neutralize', by=by, params=params, rows=rows)
    return out, diag


def _neutralize_diag(out: pl.DataFrame, c: str, industry_col: str, mv_col: str,
                     ind_ok: pl.Expr, by: str, min_group_size: int, min_obs: int,
                     suffix: str) -> pl.DataFrame:
    """单因子逐日中性化诊断。"""
    res = f'{c}{suffix}'
    pair_ok = (pl.col(c).cast(pl.Float64).is_not_null()
               & pl.col(mv_col).cast(pl.Float64).is_not_null() & ind_ok)
    group_n = pair_ok.sum().over([by, industry_col])
    valid = pair_ok & (group_n >= min_group_size)
    resid, xt, yt = _neutralize_parts(c, industry_col, mv_col, ind_ok, by, min_group_size)
    work = out.with_columns([
        pair_ok.alias('__pair_ok'),
        valid.alias('__valid'),
        xt.alias('__xt'),
        yt.alias('__yt'),
        (resid * resid).alias('__ssr'),
        (resid).alias('__resid'),
    ])
    agg = work.group_by([by, industry_col]).agg(
        pl.col('__resid').mean().alias('__gm'),
        pl.col('__resid').std(ddof=1).alias('__gs'),
        pl.col('__resid').is_not_null().sum().alias('__gn'),
    ).filter(pl.col('__gn') >= min_group_size)

    per_date = work.group_by(by).agg(
        pl.col(res).is_not_null().sum().alias('n_obs'),
        pl.col(c).is_null().sum().alias('n_dropped_null_factor'),
        (pl.col(c).cast(pl.Float64).is_not_null()
         & pl.col(mv_col).is_null()).sum().alias('n_dropped_null_mv'),
        (pl.col(c).cast(pl.Float64).is_not_null()
         & pl.col(mv_col).cast(pl.Float64).is_not_null()
         & ~ind_ok).sum().alias('n_dropped_null_industry'),
        (pl.col('__pair_ok') & ~pl.col('__valid')).sum().alias('n_dropped_small_group'),
        (pl.col('__valid').sum() < min_obs).cast(pl.Int8).alias('short'),
        pl.col('__resid').std(ddof=1).alias('resid_std'),
        pl.corr(pl.col('__resid'), pl.col(mv_col).cast(pl.Float64))
        .alias('corr_resid_ln_mv'),
        pl.corr(pl.col('__resid'), pl.col('__xt')).alias('corr_resid_ln_mv_within'),
        (((pl.col('__yt') ** 2).sum() - pl.col('__ssr').sum())
         / (pl.col('__yt') ** 2).sum()).alias('r2'),
        ((pl.col('__xt') * pl.col('__yt')).sum()
         / (pl.col('__xt') ** 2).sum()).alias('beta'),
    ).sort(by)
    leak = per_date.select(by).join(
        agg.with_columns((pl.col('__gm').abs() / pl.col('__gs').fill_null(1.0))
                         .alias('__leak'))
        .group_by(by).agg(pl.col('__leak').max().alias('max_abs_ind_dummy_leak')),
        on=by, how='left')
    return per_date.join(leak, on=by, how='left').with_columns(
        pl.lit(c).alias('factor')).select(
        [by, 'factor', 'n_obs', 'n_dropped_null_factor', 'n_dropped_null_mv',
         'n_dropped_null_industry', 'n_dropped_small_group', 'short', 'beta', 'r2',
         'resid_std', 'corr_resid_ln_mv', 'corr_resid_ln_mv_within',
         'max_abs_ind_dummy_leak'])


def run_pipeline(frame: pl.DataFrame,
                 columns: list[str],
                 *,
                 by: str = 'date',
                 industry_col: str = 'ind_code',
                 mv_col: str = 'ln_mv',
                 winsor_k: float = DEFAULT_MAD_K,
                 standardize_method: StandardizeMethod = DEFAULT_STANDARDIZE,
                 min_obs: int = 30,
                 min_group_size: int = 5,
                 collect_diagnostics: bool = True
                 ) -> PipelineResult:
    """串起四步：去极值 -> 缺失 mask -> 标准化 -> 行业+lnMV 中性化。

    Returns:
        :class:`PipelineResult`，``columns`` 给出「原列 -> 最终残差列」
        （``{col}_win_std_neu``），``diagnostics`` 为四步诊断。

    注意：mask 基于**原始**因子列（``{col}__valid``），因此最终列 null 可以
    区分「原本缺失」（mask=False）与「中性化阶段被剔除」（mask=True 但残差 null）。
    """
    out = frame
    diags: list[StepDiagnostic] = []
    out, d = winsorize_mad(out, columns, by=by, k=winsor_k, min_obs=min_obs,
                           collect_diagnostics=collect_diagnostics)
    diags.append(d)
    out, d = mark_missing(out, columns, collect_diagnostics=collect_diagnostics)
    diags.append(d)
    win_cols = [f'{c}{WIN_SUFFIX}' for c in columns]
    out, d = standardize(out, win_cols, by=by, method=standardize_method,
                         min_obs=min_obs, collect_diagnostics=collect_diagnostics)
    diags.append(d)
    std_cols = [f'{c}{STD_SUFFIX}' for c in win_cols]
    out, d = neutralize_industry_mv(out, std_cols, by=by, industry_col=industry_col,
                                    mv_col=mv_col, min_obs=min_obs,
                                    min_group_size=min_group_size,
                                    collect_diagnostics=collect_diagnostics)
    diags.append(d)
    mapping = {c: f'{c}{WIN_SUFFIX}{STD_SUFFIX}{NEU_SUFFIX}' for c in columns}
    return PipelineResult(frame=out, columns=mapping, diagnostics=tuple(diags))
