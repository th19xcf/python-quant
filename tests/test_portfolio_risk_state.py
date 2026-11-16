"""组合仓位与风控：档位映射、波动率缩放、回撤制动与约束校验的最小用例。"""

from __future__ import annotations

import pytest

from src.quant.analysis.portfolio_risk_state import (
    MAX_INDUSTRY_WEIGHT,
    MAX_NAME_WEIGHT,
    baseline_tier,
    check_constraints,
    drawdown_factor,
    drawdown_from_peak,
    evaluate_portfolio_risk,
    evaluate_position,
    realized_volatility,
    vol_scale,
)


def test_tier_mapping_thresholds():
    assert baseline_tier(0.55)[0] == 1.00
    assert baseline_tier(0.20)[0] == 0.60
    assert baseline_tier(0.00)[0] == 0.30
    assert baseline_tier(-0.20) == (0.00, '偏空（≤−0.15）')


def test_vol_scale_never_leverages():
    assert vol_scale(0.30) == pytest.approx(0.5)
    assert vol_scale(0.10) == 1.0
    assert vol_scale(None) == 1.0


def test_drawdown_factor_is_continuous():
    assert drawdown_factor(-0.05) == 1.0
    assert drawdown_factor(-0.125) == pytest.approx(0.5)
    assert drawdown_factor(-0.16) == 0.0
    assert drawdown_factor(None) == 1.0


def test_position_chain_multiplies_three_steps():
    decision = evaluate_position(0.50, '强多', 0.30, -0.12)
    assert decision.tier_position == 1.00
    assert decision.vol_scale == pytest.approx(0.5)
    assert decision.position_after_vol == pytest.approx(0.5)
    assert decision.drawdown_factor == pytest.approx(0.6)
    assert decision.target_position == pytest.approx(0.3)
    assert len(decision.steps) == 3


def test_realized_volatility_and_drawdown_on_synthetic_series():
    closes = [100.0 * (1.001 ** index) for index in range(30)]
    assert realized_volatility(closes) == pytest.approx(0.0, abs=1e-9)
    falling = [100.0 + index for index in range(20)] + [110.0, 99.0]
    assert drawdown_from_peak(falling, window=2) == pytest.approx(99.0 / 110.0 - 1)
    assert realized_volatility([100.0]) is None


def test_constraints_flag_name_and_turnover_violations():
    report = check_constraints(
        {'600000.SH': 0.10, '000001.SZ': 0.05},
        {'600000.SH': '银行', '000001.SZ': '银行'},
        0.30,
        asof='2026-11-13',
    )
    assert report.generated
    by_name = {item.name: item for item in report.items}
    assert by_name['单标的权重'].passed is False
    assert by_name['单行业权重'].actual == pytest.approx(0.15)
    assert by_name['单行业权重'].limit == MAX_INDUSTRY_WEIGHT
    assert by_name['单标的权重'].limit == MAX_NAME_WEIGHT
    assert by_name['单边换手'].passed is False


def test_missing_holdings_is_reported_not_estimated():
    report = check_constraints(None, None, None, source='reports/target_holdings/（未生成）')
    assert not report.generated
    assert report.items == ()
    assert '未找到目标持仓文件' in report.notes[0]


def test_state_requires_no_holdings_to_still_render():
    closes = [100.0 * (1.0005 ** index) for index in range(300)]
    state = evaluate_portfolio_risk(0.10, '中性', closes, notes=('测试',))
    assert 0.0 <= state.decision.target_position <= 1.0
    assert state.budget.realized_vol is not None
    assert not state.constraints.generated
    assert state.notes == ('测试',)


def test_headless_page_renders_tables(qapp):
    from src.ui.portfolio_risk import PortfolioRiskPage

    closes = [100.0 * (1.0005 ** index) for index in range(300)]
    state = evaluate_portfolio_risk(
        0.10, '中性', closes,
        weights={'600000.SH': 0.10, '000001.SZ': 0.05},
        industry_of={'600000.SH': '银行', '000001.SZ': '银行'},
        one_way_turnover=0.30,
        holdings_asof='2026-11-13',
        holdings_source='reports/target_holdings/目标持仓_2026-11-13.csv',
    )
    page = PortfolioRiskPage()
    try:
        page.render_state(state)
        assert page.steps_table.rowCount() == 3
        assert page.steps_table.columnCount() == 3
        assert page.steps_table.showGrid()
        assert page.budget_table.rowCount() == 4
        assert page.budget_table.showGrid()
        assert page.constraint_table.rowCount() == 3
        assert page.constraint_table.showGrid()
        assert '建议仓位' in page.position_label.text()
        assert '计算完成' in page.status_label.text()
    finally:
        page.deleteLater()
