#!/usr/bin/env python3

"""用例层契约测试：DTO 映射、快照缓存与新鲜度。"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from _advice_synth import build_store

from src.core.usecases.get_daily_recommendations import (
    DailyRecommendationBundle,
    DailyRecommendationDTO,
    _to_dto,
    get_daily_recommendations,
)


def _row(**overrides) -> dict:
    base = {
        'trade_date': '2026-06-30', 'ts_code': '600000.SH', 'name': '浦发银行',
        'direction': 'BUY', 'target_weight_pct': 7.5, 'shares': 5000,
        'ref_price': 15.2, 'horizon': '短线', 'validity_days': 20,
        'confidence': 0.82, 'top_factors': '1月动量 +0.42 | 20日量能 +0.18 | ATR占比 +0.11',
        'risk_notes': 'ST状态未知；次日可能跳空', 'industry': '沪主板',
        'score': 1.8, 'score_percentile': 0.97,
    }
    base.update(overrides)
    return base


def test_dto_mapping_parses_factors_and_asset_type():
    dto = _to_dto(_row(), 'short')
    assert isinstance(dto, DailyRecommendationDTO)
    assert dto.tier == 'short' and dto.tier_label == '短线'
    assert dto.asset_type == '股票'
    assert len(dto.factor_contributions) == 3
    assert dto.factor_contributions[0] == {'label': '1月动量', 'value': 0.42}
    row = dto.to_row()
    assert row['target_weight_pct'] == 7.5
    assert row['horizon'] == '短线'


def test_bundle_filter_and_staleness():
    items = [_to_dto(_row(ts_code='600000.SH'), 'short'),
             _to_dto(_row(ts_code='000001.SZ'), 'mid')]
    bundle = DailyRecommendationBundle(
        items=items, trade_date=date.today() - timedelta(days=3),
        data_lag_days=3, equity=1_000_000)
    assert len(bundle.by_tier('short')) == 1
    assert len(bundle.by_tier('all')) == 2
    assert bundle.is_stale


def test_usecase_generates_and_caches_snapshot(tmp_path):
    root = build_store(tmp_path / 'local_store',
                       [f'6000{index:02d}.SH' for index in range(20)],
                       end=date(2026, 6, 30), days=320)
    out_dir = tmp_path / 'out'
    state_dir = tmp_path / 'state'
    bundle = get_daily_recommendations(
        trade_date=date(2026, 6, 30), horizon='short', equity=1_000_000,
        refresh=True, store_root=root, out_dir=out_dir, state_dir=state_dir,
        tiers=['short'])
    assert bundle.source == 'engine'
    assert bundle.items
    assert all(item.tier == 'short' for item in bundle.items)
    assert (out_dir / 'latest.json').is_file()

    cached = get_daily_recommendations(
        horizon='short', out_dir=out_dir, store_root=root,
        state_dir=state_dir, tiers=['short'])
    assert cached.source == 'snapshot'
    assert len(cached.items) == len(bundle.items)
    assert cached.trade_date == date(2026, 6, 30)
