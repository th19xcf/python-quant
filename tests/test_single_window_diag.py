"""单窗超额归因诊断工具的纯函数自检。

只覆盖不依赖真实数据的计算部件：归因恒等式、Brinson 恒等式、风格截面
z 值、对数收益占比与剔除单窗年化。不加载 parquet，不做网络/数据库访问。
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl

from tools.single_window_diag import (
    STYLE_COLUMNS,
    STYLE_LABELS,
    STYLE_SOURCE,
    VALUE_COMPONENTS,
    _drop_one_annualized,
    _log_shares,
    brinson_contrib,
    build_style_panel,
    decompose_excess,
)


def _synthetic_book(seed: int = 20261105):
    """构造 2 个行业、6 只股票的小截面，返回 (w, b, y, z, industry)。"""
    rng = np.random.default_rng(seed)
    n = 6
    weights = np.array([0.30, 0.20, 0.10, 0.20, 0.15, 0.05])
    bench = np.full(n, 1.0 / n)
    y = rng.normal(0.0, 0.02, n)
    z = rng.normal(0.0, 1.0, (n, len(STYLE_COLUMNS)))
    industry = np.array(['银行'] * 3 + ['电子'] * 3)
    return weights, bench, y, z, industry


def test_decompose_excess_identity():
    weights, bench, y, z, industry = _synthetic_book()
    result = decompose_excess(weights, bench, y, z, industry)
    assert abs(result['_check'] - result['_total']) < 1e-12
    assert abs(result['_total'] - float((weights - bench) @ y)) < 1e-12
    # 六个风格贡献项齐全
    assert all(label in result for label in STYLE_LABELS.values())


def test_decompose_excess_full_weight_has_no_cash_term():
    weights, bench, y, z, industry = _synthetic_book()
    result = decompose_excess(weights, bench, y, z, industry)
    assert abs(result['现金/截距']) < 1e-12


def test_brinson_identity():
    weights, bench, y, z, industry = _synthetic_book()
    result = brinson_contrib(weights, bench, y, industry)
    assert abs(result['_total'] - float((weights - bench) @ y)) < 1e-12
    assert abs(result['行业配置'] + result['行业选股'] - result['_total']) < 1e-12


def test_brinson_handles_zero_port_weight():
    weights = np.array([1.0, 0.0, 0.0, 0.0])
    bench = np.full(4, 0.25)
    y = np.array([0.01, 0.02, -0.01, 0.03])
    industry = np.array(['A', 'A', 'B', 'B'])
    result = brinson_contrib(weights, bench, y, industry)
    # 组合完全放弃 B 行业时，配置项仍须计入（此前跳过 w_port=0 会破坏恒等式）
    assert abs(result['行业配置'] + result['行业选股'] - result['_total']) < 1e-12


def test_build_style_panel_cross_section_zero_mean():
    rows = []
    for day in (dt.date(2024, 1, 2), dt.date(2024, 1, 3)):
        # 管道 min_obs=30：单期样本不足会置 null，故截面取 40 只
        for index in range(40):
            row = {'date': day, 'ts_code': f'{index:06d}.SZ'}
            for offset, column in enumerate(STYLE_SOURCE, start=1):
                row[column] = float(index * offset + 1)
            for offset, column in enumerate(VALUE_COMPONENTS, start=1):
                row[column] = float(index + offset)
            rows.append(row)
    frame = pl.DataFrame(rows)
    panel = build_style_panel(frame)
    assert set(panel.columns) == {'date', 'ts_code', *STYLE_COLUMNS}
    for column in STYLE_COLUMNS:
        means = panel.group_by('date').agg(pl.col(column).mean()).sort('date')
        assert means[column].abs().max() < 1e-9
        assert panel[column].abs().max() <= 3.0 + 1e-12


def test_log_shares_sums_to_one():
    shares = _log_shares([0.10, -0.05, 0.20])
    assert abs(sum(shares) - 1.0) < 1e-12


def test_log_shares_zero_total_returns_zeros():
    shares = _log_shares([0.0, 0.0])
    assert shares == [0.0, 0.0]


def test_drop_one_annualized_matches_compounding():
    returns = [0.30, 0.10, 0.10, 0.10, 0.10, 0.10, -0.05]
    value = _drop_one_annualized(returns, drop=99)
    product = 1.0
    for item in returns:
        product *= 1.0 + item
    assert abs(value - (product ** (252.0 / 875.0) - 1.0)) < 1e-12
    dropped = _drop_one_annualized(returns, drop=0)
    assert dropped < value
