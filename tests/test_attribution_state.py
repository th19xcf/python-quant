"""收益归因（口径降级）：超额统计、行业/风格暴露与界面渲染的最小用例。"""

from __future__ import annotations

import pytest

from src.quant.analysis.attribution_state import (
    BASELINE_PERCENTILE,
    RunSummary,
    annualized_return,
    build_state,
    excess_stats,
    industry_exposure,
    style_exposure,
)


def _series(base: float, step: float, days: int) -> list[float]:
    return [base * (1.0 + step) ** index for index in range(days)]


def test_annualized_return_geometric():
    grows = _series(1.0, 0.001, 253)
    assert annualized_return(grows) == pytest.approx(1.001 ** 252 - 1, rel=1e-6)
    assert annualized_return([1.0]) is None
    assert annualized_return([0.0, 1.0]) is None


def test_excess_stats_beta_and_tracking_error():
    nav = _series(1.0, 0.002, 253)
    bench = _series(1.0, 0.001, 253)
    stats = excess_stats(nav, bench)
    assert stats is not None
    assert stats.excess_annual > 0
    assert stats.beta is not None and stats.beta > 0
    assert stats.tracking_error is not None and stats.tracking_error > 0
    assert stats.information_ratio is not None
    assert excess_stats([1.0, 1.1], [1.0, 1.05]) is None


def test_industry_exposure_deviation_and_ordering():
    weights = {'A': 0.5, 'B': 0.3, 'C': 0.2}
    industry = {'A': '银行', 'B': '电子', 'C': '银行'}
    share = {'银行': 0.10, '电子': 0.20}
    rows = industry_exposure(weights, industry, share)
    assert rows[0].name == '银行'
    assert rows[0].weight == pytest.approx(0.7)
    assert rows[0].diff == pytest.approx(0.6)
    assert {row.name for row in rows} == {'银行', '电子'}


def test_industry_exposure_without_industry_map_is_empty():
    assert industry_exposure({'A': 1.0}, {}) == ()


def test_style_exposure_weighted_percentile():
    weights = {'A': 0.5, 'B': 0.5}
    rows = style_exposure(weights, {'A': 80.0, 'B': 40.0}, {'A': 30.0})
    by_name = {row.name: row for row in rows}
    assert by_name['市值分位'].weight == pytest.approx(60.0)
    assert by_name['市值分位'].diff == pytest.approx(60.0 - BASELINE_PERCENTILE)
    assert by_name['PB 分位'].weight == pytest.approx(30.0)
    assert style_exposure({}, {}, {})[0].diff is None


def test_headless_page_renders_degraded_state(qapp):
    from src.ui.attribution import AttributionPage

    state = build_state(
        asof='2026-11-13',
        run=RunSummary('20261114-110958_v3_monthly', 'v3 月度调仓', '2010-01-04', '2026-11-13'),
        excess=excess_stats(_series(1.0, 0.002, 253), _series(1.0, 0.001, 253)),
        industries=industry_exposure(
            {'A': 0.6, 'B': 0.4}, {'A': '银行', 'B': '电子'}, {'银行': 0.1, '电子': 0.2}),
        styles=style_exposure({'A': 0.6, 'B': 0.4}, {'A': 70.0, 'B': 50.0}, {'A': 40.0, 'B': 60.0}),
        degraded=True,
        missing=('逐期持仓权重', '逐笔成交明细'),
        notes=('口径降级',),
    )
    page = AttributionPage()
    try:
        page.render_state(state)
        assert '口径降级' in page.banner_label.text()
        assert '逐期持仓权重' in page.missing_label.text()
        assert page.excess_table.rowCount() == 6
        assert page.excess_table.showGrid()
        assert page.industry_table.rowCount() == 2
        assert page.industry_table.columnCount() == 4
        assert page.industry_table.showGrid()
        assert page.style_table.rowCount() == 2
        assert page.style_table.showGrid()
        assert '归因完成' in page.status_label.text()
    finally:
        page.deleteLater()
