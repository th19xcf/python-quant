#!/usr/bin/env python3

"""
因子预处理管线（src/alpha/cross_section/preprocess.py）单测

覆盖四步的核心不变量：
  1. 去极值按 med ± k×1.4826×MAD 截断，null 不被改动，MAD≈0 时不截断；
  2. 缺失只标记不填补：任何一步都不产生 0 值，null 计数在标准化前后守恒；
  3. rank-gauss 等于 ndtri((rank-0.5)/n)，z-score 等于手算 (x-mean)/std；
  4. 中性化与「行业哑变量 + lnMV 的显式 OLS」逐值一致（FWL 等价性），
     且残差对行业哑变量与 lnMV 的泄漏 ≈ 0；
  5. 丢弃口径（行业组过小 / 行业缺失 / 当期样本不足）逐项计数正确；
  6. 整链不改行数、不改行序、不改输入对象，诊断结构齐全。

第 4 条是本次收敛的核心防线：只要 FWL 实现把「组内去均值」错写成
「日期去均值」，与显式哑变量 OLS 的逐值比较必然失败。
"""

import numpy as np
import polars as pl
import pytest
from scipy.special import ndtri

from src.alpha.cross_section.preprocess import (
    MAD_SCALE,
    mark_missing,
    neutralize_industry_mv,
    run_pipeline,
    standardize,
    winsorize_mad,
)


def _panel(n: int = 200, seed: int = 7) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    ind = rng.choice(['A', 'B', 'C', 'D', 'E'], n)
    ln_mv = rng.normal(3.0, 1.0, n)
    alpha = rng.normal(0.0, 1.0, n)
    ind_eff = np.where(ind == 'A', 1.0, np.where(ind == 'B', -1.0, 0.0))
    return pl.DataFrame({
        'date': ['2020-01-31'] * n,
        'ts_code': [f'{i:06d}' for i in range(n)],
        'ind_code': ind,
        'ln_mv': ln_mv,
        'f': 2.0 * ln_mv + ind_eff + alpha,
    })


def _ols_with_dummies(f: np.ndarray, ind: np.ndarray,
                      ln_mv: np.ndarray) -> np.ndarray:
    """显式哑变量 OLS 残差（参照实现，不做任何 FWL 化简）。"""
    cats = sorted(set(ind))
    dummies = np.column_stack([(ind == c).astype(float) for c in cats])
    x = np.column_stack([dummies, ln_mv])
    beta, *_ = np.linalg.lstsq(x, f, rcond=None)
    return f - x @ beta


# ---------------------------------------------------------------- 去极值
def test_winsorize_mad_clips_both_tails():
    vals = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 100.0, -100.0]
    frame = pl.DataFrame({'date': ['d'] * 11, 'f': vals})
    out, diag = winsorize_mad(frame, ['f'], min_obs=5, k=3.0)
    med = float(np.median(vals))
    mad = float(np.median(np.abs(np.array(vals) - med)))
    lo, hi = med - 3.0 * MAD_SCALE * mad, med + 3.0 * MAD_SCALE * mad
    got = out['f_win'].to_numpy()
    assert got[9] == pytest.approx(hi)
    assert got[10] == pytest.approx(lo)
    assert got[:9] == pytest.approx(vals[:9])
    assert diag.rows['n_clipped'][0] == 2
    assert diag.rows['applied'][0] == 1


def test_winsorize_keeps_null_and_skips_degenerate_mad():
    frame = pl.DataFrame({'date': ['d'] * 6, 'f': [1.0, 1.0, 1.0, 1.0, 1.0, None]})
    out, diag = winsorize_mad(frame, ['f'], min_obs=3)
    assert out['f_win'].to_list() == [1.0, 1.0, 1.0, 1.0, 1.0, None]
    assert diag.rows['applied'][0] == 0
    # MAD=0 时不截断，否则除中位数外会全部塌到中位数
    assert diag.rows['n_clipped'][0] == 0


def test_winsorize_min_obs_guard_leaves_values_untouched():
    frame = pl.DataFrame({'date': ['d'] * 8, 'f': [0.0] * 7 + [50.0]})
    out, diag = winsorize_mad(frame, ['f'], min_obs=20)
    assert out['f_win'].to_list() == frame['f'].to_list()
    assert diag.rows['applied'][0] == 0


# ---------------------------------------------------------------- 缺失 mask
def test_mark_missing_marks_without_filling():
    frame = pl.DataFrame({'date': ['d'] * 5, 'f': [1.0, None, 3.0, None, 5.0]})
    out, diag = mark_missing(frame, ['f'])
    assert out['f'].to_list() == [1.0, None, 3.0, None, 5.0]
    assert out['f__valid'].to_list() == [True, False, True, False, True]
    assert out['f'].is_null().sum() == 2
    assert diag.rows['n_missing'][0] == 2
    assert diag.rows['n_non_finite'][0] == 0


def test_mark_missing_turns_inf_into_null_and_counts():
    frame = pl.DataFrame({'date': ['d'] * 4, 'f': [1.0, float('inf'), float('-inf'), 4.0]})
    out, diag = mark_missing(frame, ['f'])
    assert out['f'].to_list() == [1.0, None, None, 4.0]
    assert out['f__valid'].to_list() == [True, False, False, True]
    assert diag.rows['n_non_finite'][0] == 2


def test_no_stage_fills_zero():
    """四步之中任何一步都不能把 null 变成 0。"""
    frame = _panel()
    frame = frame.with_columns(
        pl.when(pl.col('ts_code') == '000000').then(None).otherwise(pl.col('f')).alias('f'))
    res = run_pipeline(frame, ['f'], industry_col='ind_code')
    out = res.frame
    null_rows = frame['f'].is_null()
    for col in ('f_win', 'f_win_std', 'f_win_std_neu'):
        assert out.filter(null_rows)[col].null_count() == null_rows.sum(), col
    assert (out['f_win_std_neu'] == 0.0).sum() == 0


# ---------------------------------------------------------------- 标准化
def test_zscore_matches_manual():
    rng = np.random.default_rng(3)
    vals = rng.normal(5.0, 2.0, 100)
    frame = pl.DataFrame({'date': ['d'] * 100, 'f': vals})
    out, diag = standardize(frame, ['f'], method='zscore', min_obs=10)
    want = (vals - vals.mean()) / vals.std(ddof=1)
    assert out['f_std'].to_numpy() == pytest.approx(want, abs=1e-12)
    assert diag.rows['mean_out'][0] == pytest.approx(0.0, abs=1e-10)
    assert diag.rows['std_out'][0] == pytest.approx(1.0, abs=1e-10)


def test_rank_gauss_matches_ndtri_of_rank():
    vals = np.array([3.0, 1.0, 2.0, 5.0, 4.0])
    frame = pl.DataFrame({'date': ['d'] * 5, 'f': vals})
    out, _ = standardize(frame, ['f'], method='rank_gauss', min_obs=3)
    ranks = np.argsort(np.argsort(vals)).astype(float) + 1.0
    want = ndtri((ranks - 0.5) / len(vals))
    assert out['f_std'].to_numpy() == pytest.approx(want, abs=1e-12)
    assert float(np.mean(want)) == pytest.approx(0.0, abs=1e-12)


def test_rank_gauss_keeps_null_and_is_monotone_invariant():
    frame = pl.DataFrame({'date': ['d'] * 6, 'f': [1.0, 2.0, None, 4.0, 8.0, 16.0]})
    a, _ = standardize(frame, ['f'], method='rank_gauss', min_obs=3)
    b, _ = standardize(frame.with_columns((pl.col('f') ** 3).alias('f')),
                       ['f'], method='rank_gauss', min_obs=3)
    assert a['f_std'].to_list()[2] is None
    keep = a['f_std'].is_not_null()
    assert a.filter(keep)['f_std'].to_numpy() == pytest.approx(
        b.filter(keep)['f_std'].to_numpy(), abs=1e-12)


def test_constant_cross_section_becomes_null_not_zero():
    frame = pl.DataFrame({'date': ['d'] * 10, 'f': [2.0] * 10})
    z, _ = standardize(frame, ['f'], method='zscore')
    r, _ = standardize(frame, ['f'], method='rank_gauss')
    assert z['f_std'].null_count() == 10
    assert r['f_std'].null_count() == 10


# ---------------------------------------------------------------- 中性化
def test_neutralize_equals_explicit_dummy_ols():
    frame = _panel()
    out, diag = neutralize_industry_mv(frame, ['f'], industry_col='ind_code')
    got = out['f_neu'].to_numpy()
    want = _ols_with_dummies(frame['f'].to_numpy(),
                             frame['ind_code'].to_numpy().astype(str),
                             frame['ln_mv'].to_numpy())
    assert got == pytest.approx(want, abs=1e-10)
    assert diag.rows['corr_resid_ln_mv_within'][0] == pytest.approx(0.0, abs=1e-9)
    assert diag.rows['max_abs_ind_dummy_leak'][0] == pytest.approx(0.0, abs=1e-9)
    assert diag.rows['beta'][0] == pytest.approx(2.0, abs=0.5)


def test_neutralize_residual_orthogonal_to_dummies_and_lnmv():
    frame = _panel()
    out, _ = neutralize_industry_mv(frame, ['f'], industry_col='ind_code')
    resid = out['f_neu'].to_numpy()
    ind = frame['ind_code'].to_numpy().astype(str)
    ln_mv = frame['ln_mv'].to_numpy()
    assert np.corrcoef(resid, ln_mv)[0, 1] == pytest.approx(0.0, abs=1e-10)
    for c in sorted(set(ind)):
        assert np.corrcoef(resid, (ind == c).astype(float))[0, 1] == pytest.approx(
            0.0, abs=1e-10)


def test_neutralize_drops_small_group_and_missing_industry():
    n = 40
    ind = ['A'] * 30 + ['B'] * 3 + [''] * 7
    frame = pl.DataFrame({
        'date': ['d'] * n,
        'ind_code': ind,
        'ln_mv': np.linspace(1.0, 3.0, n),
        'f': np.linspace(0.0, 1.0, n),
    })
    out, diag = neutralize_industry_mv(frame, ['f'], industry_col='ind_code',
                                       min_obs=10, min_group_size=5)
    assert out['f_neu'].null_count() == 10
    assert diag.rows['n_dropped_small_group'][0] == 3
    assert diag.rows['n_dropped_null_industry'][0] == 7
    assert diag.rows['n_obs'][0] == 30


def test_neutralize_drop_counters_are_mutually_exclusive():
    """四类丢弃数互斥且相加等于总丢弃数：同一行的多个缺失原因只记一次。"""
    n = 30
    ind = ['A'] * n
    mv = np.linspace(1.0, 2.0, n)
    f = np.linspace(0.0, 1.0, n)
    frame = pl.DataFrame({'date': ['d'] * n, 'ind_code': ind, 'ln_mv': mv, 'f': f})
    frame = frame.with_columns([
        # 因子缺失 + 市值缺失（应只记 factor）
        pl.when(pl.col('ln_mv') > 1.9).then(None)
        .otherwise(pl.col('f')).alias('f'),
        pl.when(pl.col('f').is_null()).then(None).otherwise(pl.col('ln_mv')).alias('ln_mv'),
        # 因子缺失 + 行业缺失（应只记 factor）
        pl.when(pl.col('f').is_null() & pl.col('ind_code').is_not_null()
                & (pl.col('ln_mv').is_null())).then(None)
        .otherwise(pl.col('ind_code')).alias('ind_code'),
    ])
    out, diag = neutralize_industry_mv(frame, ['f'], industry_col='ind_code',
                                       min_obs=5, min_group_size=5)
    r = diag.rows
    dropped = int(r['n_dropped_null_factor'].sum() + r['n_dropped_null_mv'].sum()
                  + r['n_dropped_null_industry'].sum()
                  + r['n_dropped_small_group'].sum())
    n_in = int(r['n_obs'].sum() + dropped)
    assert n_in == n
    assert out['f_neu'].null_count() == dropped
    assert r['n_dropped_null_factor'][0] > 0
    assert r['n_dropped_null_factor'][0] == int(frame['f'].is_null().sum())


def test_neutralize_min_obs_guard_nulls_whole_date():
    frame = pl.DataFrame({
        'date': ['d'] * 12,
        'ind_code': ['A', 'B'] * 6,
        'ln_mv': np.linspace(1.0, 2.0, 12),
        'f': np.linspace(0.0, 1.0, 12),
    })
    out, diag = neutralize_industry_mv(frame, ['f'], industry_col='ind_code',
                                       min_obs=100, min_group_size=3)
    assert out['f_neu'].null_count() == 12
    assert diag.rows['short'][0] == 1


# ---------------------------------------------------------------- 整链
def test_run_pipeline_shapes_columns_and_order():
    frame = _panel()
    before = frame.clone()
    res = run_pipeline(frame, ['f'], industry_col='ind_code', min_obs=20)
    out = res.frame
    assert out.height == frame.height
    assert out['ts_code'].to_list() == frame['ts_code'].to_list()
    assert res.columns == {'f': 'f_win_std_neu'}
    assert out['f_win_std_neu'].is_not_null().all()
    assert out['f__valid'].all()
    # 输入对象不被就地修改
    assert out['f'].to_list() == before['f'].to_list()
    assert set(res.summary()) == {'winsorize_mad', 'mark_missing', 'standardize',
                                  'neutralize'}
    assert res.summary()['neutralize']['totals']['n_obs'] == frame.height


def test_run_pipeline_rejects_unknown_column():
    frame = _panel()
    with pytest.raises(ValueError, match='因子列不存在'):
        run_pipeline(frame, ['nope'], industry_col='ind_code')


def test_pipeline_mask_separates_missing_from_dropped():
    """原缺失 -> mask=False；行业缺失被中性化剔除 -> mask=True 但残差 null。"""
    frame = _panel()
    frame = frame.with_columns([
        pl.when(pl.col('ts_code') == '000000').then(None).otherwise(pl.col('f')).alias('f'),
        pl.when(pl.col('ts_code') == '000001').then(None)
        .otherwise(pl.col('ind_code')).alias('ind_code'),
    ])
    res = run_pipeline(frame, ['f'], industry_col='ind_code', min_obs=20)
    out = res.frame
    r0 = out.filter(pl.col('ts_code') == '000000')
    r1 = out.filter(pl.col('ts_code') == '000001')
    assert r0['f__valid'][0] is False and r0['f_win_std_neu'][0] is None
    assert r1['f__valid'][0] is True and r1['f_win_std_neu'][0] is None
    assert res.summary()['neutralize']['totals']['n_dropped_null_industry'] == 1
