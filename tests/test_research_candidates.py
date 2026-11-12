#!/usr/bin/env python3
"""研究候选因子的独立复算与 PIT 单测。"""
from __future__ import annotations

import math
from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest

from src.alpha.factors.research_candidates import (
    CANDIDATE_METADATA,
    FINANCIAL_CANDIDATES,
    PRICE_CANDIDATES,
    build_pit_reports,
    compute_financial_candidates,
    compute_price_candidates,
)


def _price_panel(n: int = 300, close: list[float] | None = None) -> pl.DataFrame:
    dates = [date(2020, 1, 1) + timedelta(days=i) for i in range(n)]
    if close is None:
        close = [1.0 + i for i in range(n)]
    px = pl.Series('close', close, dtype=pl.Float64)
    return pl.DataFrame({
        'date': dates, 'ts_code': ['000001.SZ'] * n,
        'open': px, 'high': px, 'low': px, 'close': px,
        'pre_close': px.shift(1), 'volume': [1e6] * n,
        'amount': [1e8] * n,
    })


def test_metadata_complete_and_unknown_direction():
    assert set(CANDIDATE_METADATA) == set(PRICE_CANDIDATES + FINANCIAL_CANDIDATES)
    for name, meta in CANDIDATE_METADATA.items():
        assert meta['direction'] == '?', name
        assert meta['pit_start'], name
        assert 'coverage_all' in meta and 'coverage_oos' in meta, name
        assert meta['definition'] and meta['data_deps'], name


def test_price_candidates_hand_computed():
    n = 300
    panel = _price_panel(n)
    mkt = pl.DataFrame({'date': panel['date'], 'mkt_ret': [0.0] * n})
    out = compute_price_candidates(panel, mkt)
    last = out.row(-1, named=True)
    close = [1.0 + i for i in range(n)]

    assert last['m_reversal_5'] == pytest.approx(close[-1] / close[-6] - 1)
    assert last['m_mom_12_1'] == pytest.approx(close[-22] / close[-253] - 1)
    assert last['m_dist_52w_high'] == pytest.approx(close[-1] / max(close[-252:]) - 1)
    ret5 = [close[i] / close[i - 5] - 1 for i in range(5, n)]
    assert last['v_max_5d_21'] == pytest.approx(max(ret5[-21:]))
    rets = [close[i] / close[i - 1] - 1 for i in range(1, n)]
    assert last['l_amihud_20'] == pytest.approx(
        float(np.mean(np.abs(rets[-20:]))) / 1e8)
    assert last['v_ivol_60'] == pytest.approx(float(np.std(rets[-60:], ddof=1)))
    dr2 = [min(r, 0.0) ** 2 for r in rets[-60:]]
    assert last['v_downside_vol_60'] == pytest.approx(math.sqrt(float(np.mean(dr2))))


def test_ivol_zero_when_return_is_beta_times_market():
    n = 200
    mkt_r = [0.001 * math.sin(i / 7.0) for i in range(n)]
    close = [100.0]
    for r in mkt_r[1:]:
        close.append(close[-1] * (1 + 2 * r))
    panel = _price_panel(n, close=close)
    mkt = pl.DataFrame({'date': panel['date'], 'mkt_ret': mkt_r})
    out = compute_price_candidates(panel, mkt)
    assert abs(out['v_ivol_60'][-1]) < 1e-12


def _financial_frames() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """2021Q1~2023Q4，累计值稳定增长，2022 为 2021 的 1.1 倍。"""
    rows = []
    for year, scale in ((2021, 1.0), (2022, 1.1), (2023, 1.21)):
        for q, (month, day) in enumerate(
                ((3, 31), (6, 30), (9, 30), (12, 31)), start=1):
            end = date(year, month, day)
            ann = end + timedelta(days={1: 30, 2: 60, 3: 30, 4: 90}[q])
            rev = 100.0 * q * scale
            rows.append({
                'ts_code': '000001.SZ', 'end_date': end, 'ann_date': ann,
                'revenue': rev, 'gross_profit': rev * 0.4,
                'parent_net_profit': 10.0 * q * scale,
                'total_assets': 1000.0, 'total_equity': 500.0,
                'operating_cf': 8.0 * q * scale,
            })
    base = pl.DataFrame(rows)
    inc = base.select(['ts_code', 'end_date', 'ann_date', 'revenue',
                       'gross_profit', 'parent_net_profit'])
    bal = base.select(['ts_code', 'end_date', 'ann_date', 'total_assets',
                       'total_equity'])
    cf = base.select(['ts_code', 'end_date', 'ann_date', 'operating_cf'])
    return inc, bal, cf


def test_financial_ttm_quality_and_growth_hand_computed():
    inc, bal, cf = _financial_frames()
    rep = build_pit_reports(inc, bal, cf)
    r = rep.filter((pl.col('ts_code') == '000001.SZ')
                   & (pl.col('end_date') == date(2022, 12, 31))).row(0, named=True)
    assert r['revenue__ttm'] == pytest.approx(440.0)
    assert r['parent_net_profit__ttm'] == pytest.approx(44.0)
    assert r['operating_cf__ttm'] == pytest.approx(35.2)
    assert r['q_roe_ttm'] == pytest.approx(44.0 / 500.0)
    assert r['q_roa_ttm'] == pytest.approx(44.0 / 1000.0)
    assert r['q_gross_margin'] == pytest.approx(0.4)
    assert r['q_asset_turnover'] == pytest.approx(0.44)
    assert r['q_accruals'] == pytest.approx((44.0 - 35.2) / 1000.0)
    assert r['q_ocf_to_profit'] == pytest.approx(35.2 / 44.0)
    assert r['g_revenue_yoy'] == pytest.approx(440.0 / 400.0 - 1)
    assert r['g_profit_yoy'] == pytest.approx(44.0 / 40.0 - 1)


def test_sue_needs_six_quarters_and_ignores_unannounced():
    inc, bal, cf = _financial_frames()
    rep = build_pit_reports(inc, bal, cf)
    # 只有 4 个季度时 std 窗口不足
    early = rep.filter(pl.col('end_date') <= date(2021, 12, 31))
    assert early['g_sue'].is_null().all()
    # 到 2023Q4 已有 12 个季度，窗口足够，不再强制为空
    late = rep.filter(pl.col('end_date') == date(2023, 12, 31)).height
    assert late == 1


def test_sue_computable_with_varying_seasonal_diff():
    rows = []
    for i, (year, month, day) in enumerate([
            (2019, 3, 31), (2019, 6, 30), (2019, 9, 30), (2019, 12, 31),
            (2020, 3, 31), (2020, 6, 30), (2020, 9, 30), (2020, 12, 31),
            (2021, 3, 31), (2021, 6, 30), (2021, 9, 30), (2021, 12, 31)]):
        end = date(year, month, day)
        ann = end + timedelta(days=30)
        rows.append({
            'ts_code': '000001.SZ', 'end_date': end, 'ann_date': ann,
            'revenue': 100.0, 'gross_profit': 40.0,
            'parent_net_profit': float(i * i + 1),
            'total_assets': 1000.0, 'total_equity': 500.0,
            'operating_cf': 8.0,
        })
    base = pl.DataFrame(rows)
    inc = base.select(['ts_code', 'end_date', 'ann_date', 'revenue',
                       'gross_profit', 'parent_net_profit'])
    bal = base.select(['ts_code', 'end_date', 'ann_date', 'total_assets',
                       'total_equity'])
    cf = base.select(['ts_code', 'end_date', 'ann_date', 'operating_cf'])
    rep = build_pit_reports(inc, bal, cf)
    assert rep['g_sue'].is_not_null().sum() > 0


def test_pit_asof_does_not_see_future_announcement():
    inc, bal, cf = _financial_frames()
    rep = build_pit_reports(inc, bal, cf)
    q1_2022 = rep.filter(pl.col('end_date') == date(2022, 3, 31)).row(0, named=True)
    ann = q1_2022['ann_date']
    cross = pl.DataFrame({
        'ts_code': ['000001.SZ', '000001.SZ'],
        'date': [ann - timedelta(days=1), ann],
    })
    val = pl.DataFrame({
        'ts_code': ['000001.SZ', '000001.SZ'],
        'trade_date': [ann - timedelta(days=1), ann],
        'total_mv': [1e9, 1e9],
    })
    out = compute_financial_candidates(cross, rep, val).sort('date')
    before, after = out.row(0, named=True), out.row(1, named=True)
    assert before['q_roe_ttm'] == pytest.approx(40.0 / 500.0)  # 2021 年报
    assert after['q_roe_ttm'] == pytest.approx(41.0 / 500.0)   # 2022Q1 TTM
    assert before['v_ep'] == pytest.approx(40.0 / 1e9)
    assert after['v_ep'] == pytest.approx(41.0 / 1e9)


def test_ttm_null_when_annual_report_announced_after_quarter():
    # 上年年报晚于本期季报公告 -> 不允许用未公告的年报凑 TTM
    rows = [
        {'ts_code': '000001.SZ', 'end_date': date(2021, 3, 31),
         'ann_date': date(2021, 4, 30), 'revenue': 100.0, 'gross_profit': 40.0,
         'parent_net_profit': 10.0, 'total_assets': 1000.0,
         'total_equity': 500.0, 'operating_cf': 8.0},
        {'ts_code': '000001.SZ', 'end_date': date(2020, 12, 31),
         'ann_date': date(2021, 5, 30), 'revenue': 400.0, 'gross_profit': 160.0,
         'parent_net_profit': 40.0, 'total_assets': 1000.0,
         'total_equity': 500.0, 'operating_cf': 32.0},
        {'ts_code': '000001.SZ', 'end_date': date(2020, 3, 31),
         'ann_date': date(2020, 4, 30), 'revenue': 90.0, 'gross_profit': 36.0,
         'parent_net_profit': 9.0, 'total_assets': 1000.0,
         'total_equity': 500.0, 'operating_cf': 7.0},
    ]
    base = pl.DataFrame(rows)
    inc = base.select(['ts_code', 'end_date', 'ann_date', 'revenue',
                       'gross_profit', 'parent_net_profit'])
    bal = base.select(['ts_code', 'end_date', 'ann_date', 'total_assets',
                       'total_equity'])
    cf = base.select(['ts_code', 'end_date', 'ann_date', 'operating_cf'])
    rep = build_pit_reports(inc, bal, cf)
    r = rep.filter(pl.col('end_date') == date(2021, 3, 31)).row(0, named=True)
    assert r['revenue__ttm'] is None
    assert r['q_roe_ttm'] is None


# ---------------------------------------------------------------------------
# 财务时效守卫（方案 B24）：陈旧值不得冒充当期
# ---------------------------------------------------------------------------
def _frames_from_rows(rows: list[dict]):
    base = pl.DataFrame(rows)
    inc = base.select(['ts_code', 'end_date', 'ann_date', 'revenue',
                       'gross_profit', 'parent_net_profit'])
    bal = base.select(['ts_code', 'end_date', 'ann_date', 'total_assets',
                       'total_equity'])
    cf = base.select(['ts_code', 'end_date', 'ann_date', 'operating_cf'])
    return inc, bal, cf


def _stale_frames():
    """只给 2009 年报一期：截面日 2021-06-30 时它已陈旧 11 年。"""
    return _frames_from_rows([
        {'ts_code': '000001.SZ', 'end_date': date(2009, 12, 31),
         'ann_date': date(2010, 1, 20), 'revenue': 100.0, 'gross_profit': 40.0,
         'parent_net_profit': 10.0, 'total_assets': 1000.0,
         'total_equity': 500.0, 'operating_cf': 8.0},
    ])


def _fresh_frames():
    """2020 年报，公告日 2021-03-30：截面日 2021-06-30 距报告期 6 个月，窗口内。"""
    return _frames_from_rows([
        {'ts_code': '000001.SZ', 'end_date': date(2020, 12, 31),
         'ann_date': date(2021, 3, 30), 'revenue': 400.0, 'gross_profit': 160.0,
         'parent_net_profit': 40.0, 'total_assets': 1000.0,
         'total_equity': 500.0, 'operating_cf': 32.0},
    ])


def _stale_cross(day: date) -> pl.DataFrame:
    return pl.DataFrame({'ts_code': ['000001.SZ'], 'date': [day]})


def _stale_valuation(day: date) -> pl.DataFrame:
    return pl.DataFrame({'ts_code': ['000001.SZ'], 'trade_date': [day],
                         'total_mv': [1e9]})


def test_staleness_guard_off_propagates_2009_value():
    """反证：关掉守卫时，2010 年的旧值会一路前推到 2021-06-30（红）。"""
    inc, bal, cf = _stale_frames()
    rep = build_pit_reports(inc, bal, cf)
    day = date(2021, 6, 30)
    out = compute_financial_candidates(
        _stale_cross(day), rep, _stale_valuation(day), max_staleness_months=None)
    r = out.row(0, named=True)
    assert r['q_accruals'] == pytest.approx((10.0 - 8.0) / 1000.0)
    assert r['q_roe_ttm'] == pytest.approx(10.0 / 500.0)
    assert r['q_gross_margin'] == pytest.approx(40.0 / 100.0)


def test_staleness_guard_on_nulls_out_2009_value():
    """守卫（24 个月）开启后，同一截面不再拿 2009 年的值（绿）。"""
    inc, bal, cf = _stale_frames()
    rep = build_pit_reports(inc, bal, cf)
    day = date(2021, 6, 30)
    out = compute_financial_candidates(
        _stale_cross(day), rep, _stale_valuation(day), max_staleness_months=24)
    r = out.row(0, named=True)
    for name in FINANCIAL_CANDIDATES:
        assert r[name] is None, name


def test_staleness_guard_keeps_fresh_report():
    """守卫不得误杀：截面距报告期 6 个月时必须保留，且数值可手算。"""
    inc, bal, cf = _fresh_frames()
    rep = build_pit_reports(inc, bal, cf)
    day = date(2021, 6, 30)
    out = compute_financial_candidates(
        _stale_cross(day), rep, _stale_valuation(day), max_staleness_months=24)
    r = out.row(0, named=True)
    assert r['q_roe_ttm'] == pytest.approx(40.0 / 500.0)
    assert r['q_accruals'] == pytest.approx((40.0 - 32.0) / 1000.0)


def test_points_at_guard_off_and_on():
    """build_new_factors.points_at 同一守卫的反证与正证。"""
    bnf = _load_build_new_factors()
    acc = pl.DataFrame({
        'ts_code': ['000001.SZ'], 'end_date': [date(2009, 12, 31)],
        'pub_date': [date(2010, 1, 20)], 'accruals': [0.123]})
    t = date(2021, 6, 30)
    off = bnf.points_at(t, acc, None, None, max_staleness_months=None)
    assert off.row(0, named=True)['accruals'] == pytest.approx(0.123)
    on = bnf.points_at(t, acc, None, None, max_staleness_months=24)
    assert on.row(0, named=True)['accruals'] is None
    # 默认参数即守卫开启，防止调用方漏传
    assert bnf.points_at(t, acc, None, None).row(0, named=True)['accruals'] is None
    # 窗口内（23 个月）必须保留
    near = bnf.points_at(date(2011, 11, 30), acc, None, None,
                         max_staleness_months=24)
    assert near.row(0, named=True)['accruals'] == pytest.approx(0.123)


def _load_build_new_factors():
    import importlib.util
    from pathlib import Path
    path = (Path(__file__).resolve().parents[1]
            / 'tools' / 'research' / 'build_new_factors.py')
    spec = importlib.util.spec_from_file_location('_bnf_staleness', path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
