#!/usr/bin/env python3

"""推荐闭环集成测试：本地列存 → 因子 → 清单 → 状态持久化。"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from _advice_synth import build_store, business_dates, write_st_snapshot, write_stock

from src.advice import pipeline as pipeline_module
from src.advice.config import default_config
from src.advice.daily_store import load_daily_snapshot, snapshot_path
from src.advice.models import (
    DIRECTION_AVOID,
    DIRECTION_BUY,
    DIRECTION_HOLD,
    DIRECTION_SELL,
    Recommendation,
)
from src.advice.pipeline import AdviceEngine, merge_duplicate_rows
from src.advice.universe import list_local_stock_codes


def _codes() -> list[str]:
    return [f'6000{index:02d}.SH' for index in range(20)]


def _no_gate_monitor(scored, tier, *, trade_date):  # noqa: ARG001
    """测试注入：IC 门禁不拦截。

    合成行情的方向与 IC 符号跟真实数据无关，本用例只验证与 IC 无关的
    处置逻辑（涨停 → AVOID、换手上限延后），因此显式跳过门禁。
    IC 门禁本身的用例见 ``test_advice_ic_gate.py``。
    """
    return {'tier': tier, 'horizon_days': 5, 'window_days': 20,
            'recent_ic': [0.01, 0.01, 0.01],
            'status': {'failed': False, 'reason': '测试注入：不拦截'}}


def test_generate_advice_schema_and_directions(tmp_path):
    root = build_store(tmp_path / 'local_store', _codes(),
                       end=date(2026, 6, 30), days=320)
    engine = AdviceEngine(config=default_config('legacy'), store_root=root)
    result = engine.generate(as_of=date(2026, 6, 30), equity=1_000_000,
                             tiers=['short'], codes=_codes(),
                             state_dir=tmp_path / 'state',
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


def test_generate_exec_status_tracks_generated_at(tmp_path):
    """exec_status 按生成时刻判定：收盘后生成 pending，次日开盘后 expired。"""
    codes = _codes()
    root = build_store(tmp_path / 'local_store', codes,
                       end=date(2026, 6, 30), days=320)
    engine = AdviceEngine(config=default_config('legacy'), store_root=root)
    pending = engine.generate(
        as_of=date(2026, 6, 30), equity=1_000_000, tiers=['short'],
        codes=codes, state_dir=tmp_path / 'state', persist_state=False,
        generated_at=datetime(2026, 6, 30, 16, 0))
    assert pending.earliest_exec_date == date(2026, 7, 1)
    assert pending.exec_status == 'pending'

    daily_dir = tmp_path / 'daily'
    expired = engine.generate(
        as_of=date(2026, 6, 30), equity=1_000_000, tiers=['short'],
        codes=codes, state_dir=tmp_path / 'state', persist_state=False,
        daily_dir=daily_dir, persist_daily=True,
        generated_at=datetime(2026, 7, 1, 10, 0))
    assert expired.exec_status == 'expired'
    payload = load_daily_snapshot(daily_dir, date(2026, 6, 30))
    assert payload['exec_status'] == 'expired'
    assert expired.summary()['exec_status'] == 'expired'


def test_limit_up_candidate_becomes_avoid(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline_module, 'run_ic_monitor', _no_gate_monitor)
    codes = _codes()
    root = build_store(tmp_path / 'local_store', codes,
                       end=date(2026, 6, 30), days=320)
    # 当前口径动量类因子 direction=-1（动量越低分越高）。合成数据里涨停标的
    # 若走默认上涨趋势会排到最差分位、进不了候选池；改成"下跌趋势 + 最后一日
    # 涨停"，让它成为最低动量 → 最高分位，才能真正验证"涨停候选 → AVOID"。
    write_stock(root, codes[-1],
                business_dates(date(2026, 6, 30), 320),
                base=8.0, trend=-0.02, limit_up_last=True)
    engine = AdviceEngine(config=default_config('legacy'), store_root=root)
    result = engine.generate(as_of=date(2026, 6, 30), equity=2_000_000,
                             tiers=['short'], codes=codes,
                             state_dir=tmp_path / 'state',
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
                             state_dir=tmp_path / 'state',
                             persist_state=False)
    labels = {advice.label for advice in result.tiers.values()}
    assert labels == {'短线', '中线', '长线'}
    frame = result.to_frame()
    assert set(frame['horizon'].to_list()) <= {'短线', '中线', '长线'}


def _recommend(code: str, direction: str) -> Recommendation:
    return Recommendation(
        trade_date=date(2026, 6, 30), ts_code=code, name=code,
        direction=direction, target_weight=0.05, shares=100,
        ref_price=10.0, horizon='短线', validity_days=5, confidence=0.4,
        top_factors=[('动量', 1.0)], risk_notes=[], industry='银行',
        industry_source='snapshot', score=1.0, score_percentile=0.9)


def test_merge_duplicate_rows_priority():
    """同一档位内同码冲突：SELL > HOLD > AVOID，重复行被合并。"""
    rows = [
        _recommend('000001.SZ', DIRECTION_AVOID),
        _recommend('000001.SZ', DIRECTION_HOLD),
        _recommend('000002.SZ', DIRECTION_HOLD),
        _recommend('000002.SZ', DIRECTION_SELL),
        _recommend('000003.SZ', DIRECTION_AVOID),
        _recommend('000003.SZ', DIRECTION_BUY),
    ]
    merged, details = merge_duplicate_rows(rows)
    assert len(merged) == 3
    assert len(details) == 3
    by_code = {rec.ts_code: rec.direction for rec in merged}
    assert by_code == {
        '000001.SZ': DIRECTION_HOLD,
        '000002.SZ': DIRECTION_SELL,
        '000003.SZ': DIRECTION_BUY,
    }


def test_deferred_buys_are_written_to_archive(tmp_path, monkeypatch):
    """换手上限延后的买入必须进入归档，并标 disposition=deferred。"""
    monkeypatch.setattr(pipeline_module, 'run_ic_monitor', _no_gate_monitor)
    # 100 只标的 → 短线档前 8% 约 9 只达买入线，总买入额超过 20% 单边预算。
    # 价格刻意压低在 10~20 元，避免高价导致整手股数向下取整为 0。
    codes = [f'6000{index:03d}.SH' for index in range(100)]
    root = tmp_path / 'local_store'
    dates = business_dates(date(2026, 6, 30), 320)
    for index, code in enumerate(codes):
        write_stock(root, code, dates, base=10.0 + index * 0.1,
                    trend=0.01 + index * 0.0002)
    write_st_snapshot(root, codes, date(2026, 6, 30))
    daily_dir = tmp_path / 'daily'
    engine = AdviceEngine(config=default_config('legacy'), store_root=root)
    result = engine.generate(as_of=date(2026, 6, 30), equity=1_000_000,
                             tiers=['short'], codes=codes,
                             state_dir=tmp_path / 'state',
                             persist_state=False, persist_daily=True,
                             daily_dir=daily_dir)
    payload = load_daily_snapshot(daily_dir, date(2026, 6, 30))
    assert payload is not None
    assert snapshot_path(daily_dir, date(2026, 6, 30)).is_file()
    deferred = [item for tier in payload['tiers'] for item in tier['items']
                if item['disposition'] == 'deferred']
    assert deferred, '换手上限应该产生延后项'
    assert all(item['action'].endswith('(延后)') for item in deferred)
    assert all(item['actionable'] is False for item in deferred)
    assert all(item['defer_reason'] for item in deferred)
    assert all(item['defer_until'] for item in deferred)
    # 归档行数必须与引擎结果一一对应
    engine_rows = sum(len(advice.rows) for advice in result.tiers.values())
    archived_rows = sum(len(tier['items']) for tier in payload['tiers'])
    assert archived_rows == engine_rows


def test_generate_does_not_write_repo_advice_artifacts(tmp_path):
    """generate 未显式传产物目录时，不得把夹具快照写进仓库 reports/。"""
    repo_dir = Path(__file__).resolve().parents[1] / 'reports' / 'advice' / 'ic_monitor'

    def snapshot() -> dict[str, tuple[int, int]]:
        if not repo_dir.is_dir():
            return {}
        return {path.name: (path.stat().st_size, path.stat().st_mtime_ns)
                for path in repo_dir.iterdir() if path.is_file()}

    before = snapshot()
    root = build_store(tmp_path / 'local_store', _codes(),
                       end=date(2026, 6, 30), days=420)
    engine = AdviceEngine(config=default_config('legacy'), store_root=root)
    engine.generate(as_of=date(2026, 6, 30), equity=1_000_000,
                    tiers=['short', 'mid', 'long'], codes=_codes(),
                    state_dir=tmp_path / 'state', persist_state=False)
    assert snapshot() == before
