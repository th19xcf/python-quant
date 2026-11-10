#!/usr/bin/env python3

"""每日推荐 UI 契约测试：DTO→表格映射、筛选排序、空状态与页面装配。"""

from datetime import date, datetime

import pytest
from PySide6.QtCore import Qt

from src.core.usecases.get_daily_recommendations import (
    DailyRecommendationBundle,
    DailyRecommendationDTO,
)
from src.ui.advice.table_model import (
    COL_CONFIDENCE,
    COL_FACTORS,
    COL_PRICE,
    COL_RISK,
    COL_SHARES,
    COL_WEIGHT,
    DTO_ROLE,
    AdviceFilterProxyModel,
    AdviceTableModel,
    display_value,
    filter_rows,
    match_filters,
    sort_rows,
    sort_value,
)


def _dto(**overrides) -> DailyRecommendationDTO:
    base = dict(  # noqa: C408
        trade_date='2026-10-19', ts_code='600000.SH', name='浦发银行',
        direction='BUY', target_weight_pct=6.2, shares=12000, ref_price=9.87,
        actionable=overrides.get('direction', 'BUY') in ('BUY', 'SELL'),
        horizon='短线', validity_days=20, confidence=0.86,
        top_factors='momentum_1m +0.42 | volume_momentum_20 +0.31 | atr_pct -0.12',
        risk_notes='行业口径为代码前缀近似；参考价可能跳空', industry='沪主板',
        asset_type='股票', tier='mid', tier_label='中线', score=1.24,
        score_percentile=0.93,
        factor_contributions=[
            {'label': 'momentum_1m', 'value': 0.42},
            {'label': 'volume_momentum_20', 'value': 0.31},
            {'label': 'atr_pct', 'value': -0.12},
        ],
    )
    base.update(overrides)
    return DailyRecommendationDTO(**base)


def _bundle(items: list[DailyRecommendationDTO]) -> DailyRecommendationBundle:
    return DailyRecommendationBundle(
        items=items, trade_date=date(2026, 10, 19),
        generated_at=datetime(2026, 10, 19, 15, 40), data_lag_days=0,
        equity=1_000_000.0,
        summary={'short': {}, 'mid': {'turnover_pct': 8.4}, 'long': {}})


# ---------------------------------------------------------------- mapping
def test_display_value_formatting():
    dto = _dto()
    assert display_value(dto, 0) == '600000.SH'
    assert display_value(dto, COL_WEIGHT) == '6.2%'
    assert display_value(dto, COL_SHARES) == '12,000'
    assert display_value(dto, COL_PRICE) == '9.87'
    assert display_value(dto, COL_CONFIDENCE) == '0.86'
    assert display_value(dto, COL_FACTORS).startswith('momentum_1m')
    risk = display_value(dto, COL_RISK)
    assert '行业口径' in risk and '20日' in risk


def test_model_exposes_dto_and_sort_roles(qapp):
    model = AdviceTableModel([_dto()])
    assert model.rowCount() == 1
    index = model.index(0, COL_CONFIDENCE)
    assert model.data(index, Qt.DisplayRole) == '0.86'
    assert model.data(index, DTO_ROLE).ts_code == '600000.SH'
    assert abs(float(model.data(index, Qt.UserRole + 2)) - 0.86) < 1e-9


# ---------------------------------------------------------------- filters
def test_match_filters_direction_asset_search():
    dto = _dto()
    assert match_filters(dto, 'BUY', '股票', '')
    assert not match_filters(dto, 'SELL')
    assert not match_filters(dto, '全部', 'ETF')
    assert match_filters(dto, '全部', '全部', '浦发')
    assert match_filters(dto, '全部', '全部', 'momentum')


def test_filter_rows_returns_subset():
    items = [_dto(), _dto(ts_code='000001.SZ', name='平安银行', direction='SELL'),
             _dto(ts_code='510300.SH', name='沪深300ETF', asset_type='ETF',
                  direction='HOLD')]
    only_buy = filter_rows(items, 'BUY')
    assert [row.ts_code for row in only_buy] == ['600000.SH']
    etf = filter_rows(items, '全部', 'ETF')
    assert [row.ts_code for row in etf] == ['510300.SH']


def test_sort_rows_by_confidence_and_weight():
    low = _dto(ts_code='000001.SZ', confidence=0.4, target_weight_pct=9.0)
    high = _dto(ts_code='600000.SH', confidence=0.9, target_weight_pct=3.0)
    by_conf = sort_rows([low, high], COL_CONFIDENCE, descending=True)
    assert [row.ts_code for row in by_conf] == ['600000.SH', '000001.SZ']
    by_weight = sort_rows([low, high], COL_WEIGHT, descending=True)
    assert [row.ts_code for row in by_weight] == ['000001.SZ', '600000.SH']


def test_sort_value_direction_order():
    assert sort_value(_dto(direction='BUY'), 2) < sort_value(_dto(direction='SELL'), 2)
    assert sort_value(_dto(direction='SELL'), 2) < sort_value(_dto(direction='AVOID'), 2)


def test_proxy_model_filters_and_sorts(qapp):
    model = AdviceTableModel([
        _dto(direction='BUY', confidence=0.9),
        _dto(ts_code='000001.SZ', name='平安银行', direction='SELL', confidence=0.5),
    ])
    proxy = AdviceFilterProxyModel()
    proxy.setSourceModel(model)
    assert proxy.rowCount() == 2
    proxy.set_direction('SELL')
    assert proxy.rowCount() == 1
    proxy.set_direction('全部')
    proxy.set_search('600000')
    assert proxy.rowCount() == 1


# ---------------------------------------------------------------- page
def test_empty_bundle_shows_empty_state(qapp):
    from src.ui.advice.page import AdvicePage
    page = AdvicePage(host=None)
    page.set_bundle(_bundle([]))
    assert page.stack.currentWidget() is page.empty_state
    assert page.empty_state.title_label.text() == '暂无推荐结果'
    assert page.empty_state.action_button.isVisibleTo(page.empty_state)


def test_page_bundle_populates_table_and_kpi(qapp):
    from src.ui.advice.page import AdvicePage
    page = AdvicePage(host=None)
    page.set_bundle(_bundle([_dto(), _dto(ts_code='000001.SZ',
                                           name='平安银行', direction='SELL',
                                           confidence=0.5)]),
                    select_code='600000.SH')
    assert page.stack.currentWidget() is page.table_card
    assert page.model.rowCount() == 2
    assert page._kpi['count'].value_label.text() == '2'
    assert page._kpi['flow'].value_label.text() == '1 / 1'
    assert page._kpi['turnover'].value_label.text() == '8.4%'
    assert page.current_dto().ts_code == '600000.SH'
    assert page.detail.name_label.text() == '浦发银行'
    assert page.detail.direction_label.text() == 'BUY'


def test_filter_without_result_shows_empty_without_action(qapp):
    from src.ui.advice.page import AdvicePage
    page = AdvicePage(host=None)
    page.set_bundle(_bundle([_dto()]))
    page.search_box.setText('不存在的标的')
    assert page.stack.currentWidget() is page.empty_state
    assert not page.empty_state.action_button.isVisibleTo(page.empty_state)
    page.search_box.setText('')
    assert page.stack.currentWidget() is page.table_card


def test_theme_switch_updates_palette_and_stylesheet(qapp):
    from src.ui.advice.page import AdvicePage
    from src.ui.advice.tokens import DARK, LIGHT
    page = AdvicePage(host=None, theme='dark')
    assert page._pal == DARK
    assert page.theme_button.text() == '浅色'
    page.toggle_theme()
    assert page._pal == LIGHT
    assert '#F3F5F8' in page.styleSheet()
    page.toggle_theme()
    assert page._pal == DARK
    assert '#12151A' in page.styleSheet()
