#!/usr/bin/env python3

"""推荐闭环集成测试：本地列存 → 因子 → 清单 → 状态持久化。"""

from __future__ import annotations

from datetime import date

from _advice_synth import build_store

from src.advice.config import default_config
from src.advice.pipeline import AdviceEngine
from src.advice.universe import list_local_stock_codes


def _codes() -> list[str]:
    return [f'6000{index:02d}.SH' for index in range(20)]


def test_generate_advice_schema_and_directions(tmp_path):
    root = build_store(tmp_path / 'local_store', _codes(),
                       end=date(2026, 6, 30), days=320)
    engine = AdviceEngine(config=default_config('legacy'), store_root=root)
    result = engine.generate(as_of=date(2026, 6, 30), equity=1_000_000,
                             tiers=['short'], codes=_codes(),
                             persist_state=False)
    frame = result.to_frame()
    assert frame.height > 0
    required = {'trade_date', 'ts_code', 'name', 'direction',
                'target_weight_pct', 'shares', 'ref_price', 'horizon',
                'validity_days', 'confidence', 'top_factors', 'risk_notes'}
    assert required <= set(frame.columns)
    directions = set(frame['direction'].to_list())
    assert 'BUY' in directions
    buys = frame.filter(frame['direction'] == 'BUY')
    assert buys.height >= 1
    assert buys['target_weight_pct'].max() <= 8.0 + 1e-6
    assert all(int(value) % 100 == 0 for value in buys['shares'].to_list())
    assert all(text for text in buys['top_factors'].to_list())
    assert result.trade_date == date(2026, 6, 30)
    # 短线档位未通过样本外验证：档位说明与 BUY 风险提示都要显式标注
    short = result.tiers['short']
    assert any('样本外验证' in text for text in short.notes)
    assert any('样本外验证' in text for text in buys['risk_notes'].to_list())


def test_limit_up_candidate_becomes_avoid(tmp_path):
    codes = _codes()
    root = build_store(tmp_path / 'local_store', codes,
                       end=date(2026, 6, 30), days=320,
                       with_limit_up=codes[-1])
    engine = AdviceEngine(config=default_config('legacy'), store_root=root)
    result = engine.generate(as_of=date(2026, 6, 30), equity=2_000_000,
                             tiers=['short'], codes=codes,
                             persist_state=False)
    frame = result.to_frame()
    avoids = frame.filter(frame['direction'] == 'AVOID')
    assert avoids.height >= 1
    assert any('涨停' in text for text in avoids['risk_notes'].to_list())


def test_state_persisted_and_held_next_run(tmp_path):
    codes = _codes()
    root = build_store(tmp_path / 'local_store', codes,
                       end=date(2026, 6, 30), days=320)
    state_dir = tmp_path / 'state'
    engine = AdviceEngine(config=default_config('legacy'), store_root=root)
    first = engine.generate(as_of=date(2026, 6, 30), equity=1_000_000,
                            tiers=['short'], codes=codes,
                            state_dir=state_dir)
    buys = [row for advice in first.tiers.values() for row in advice.rows
            if row.direction == 'BUY']
    assert buys and (state_dir / 'short.json').is_file()

    second = engine.generate(as_of=date(2026, 6, 30), equity=1_000_000,
                             tiers=['short'], codes=codes,
                             state_dir=state_dir)
    holds = [row for advice in second.tiers.values() for row in advice.rows
             if row.direction == 'HOLD']
    assert {row.ts_code for row in holds} == {row.ts_code for row in buys}


def test_all_tiers_have_distinct_horizons(tmp_path):
    root = build_store(tmp_path / 'local_store', _codes(),
                       end=date(2026, 6, 30), days=420)
    engine = AdviceEngine(config=default_config('legacy'), store_root=root)
    result = engine.generate(as_of=date(2026, 6, 30), equity=1_000_000,
                             tiers=['short', 'mid', 'long'], codes=_codes(),
                             persist_state=False)
    labels = {advice.label for advice in result.tiers.values()}
    assert labels == {'短线', '中线', '长线'}
    frame = result.to_frame()
    assert set(frame['horizon'].to_list()) <= {'短线', '中线', '长线'}
