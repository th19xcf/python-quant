"""每日推荐归档：写盘幂等、字段完整、可读回。"""

from __future__ import annotations

from datetime import date, datetime

from src.advice.daily_store import (
    list_daily_snapshots,
    load_daily_snapshot,
    save_daily_snapshot,
    snapshot_path,
)
from src.advice.models import (
    DIRECTION_AVOID,
    DIRECTION_BUY,
    DIRECTION_HOLD,
    AdviceResult,
    Recommendation,
    TierAdvice,
)


def _rec(ts_code: str, name: str, direction: str,
         score: float, percentile: float) -> Recommendation:
    return Recommendation(
        trade_date=date(2026, 11, 3), ts_code=ts_code, name=name,
        direction=direction, target_weight=0.05, shares=1000,
        ref_price=10.0, horizon='短线', validity_days=5, confidence=0.8,
        top_factors=[('动量', 1.2), ('量能', 0.5)], risk_notes=['波动偏高'],
        industry='银行', industry_source='snapshot', score=score,
        score_percentile=percentile)


def _result() -> AdviceResult:
    result = AdviceResult(trade_date=date(2026, 11, 3), equity=1_000_000.0)
    result.tiers['short'] = TierAdvice(
        tier='short', label='短线', trade_date=date(2026, 11, 3),
        equity=1_000_000.0,
        rows=[
            _rec('000001.SZ', '平安银行', DIRECTION_HOLD, 1.0, 0.60),
            _rec('600000.SH', '浦发银行', DIRECTION_BUY, 1.5, 0.99),
            _rec('000002.SZ', '万科A', DIRECTION_AVOID, 0.2, 0.10),
        ])
    return result


def test_archive_write_twice_is_idempotent(tmp_path):
    result = _result()
    stamp = datetime(2026, 11, 3, 16, 30)
    first = save_daily_snapshot(result, tmp_path, generated_at=stamp)
    second = save_daily_snapshot(result, tmp_path, generated_at=stamp)

    assert first == second == snapshot_path(tmp_path, date(2026, 11, 3))
    files = list(tmp_path.glob('*.json'))
    assert len(files) == 1
    payload = load_daily_snapshot(tmp_path, date(2026, 11, 3))
    assert payload is not None
    items = payload['tiers'][0]['items']
    assert [item['rank'] for item in items] == [1, 2, 3]
    assert [item['ts_code'] for item in items] == [
        '600000.SH', '000001.SZ', '000002.SZ']
    assert items[0]['action'] == '买入'
    assert items[1]['action'] == '持有'
    assert items[2]['action'] == 'AVOID'
    assert items[0]['ref_price_basis'] == 'close'
    assert items[0]['trade_date'] == '2026-11-03'
    assert payload['execution_basis'] == 'next_open'
    assert payload['tiers'][0]['counts'] == {'HOLD': 1, 'BUY': 1, 'AVOID': 1}


def test_archive_list_and_missing(tmp_path):
    save_daily_snapshot(_result(), tmp_path,
                        generated_at=datetime(2026, 11, 3, 16, 0))
    entries = list_daily_snapshots(tmp_path)
    assert [item['trade_date'] for item in entries] == ['2026-11-03']
    assert entries[0]['items'] == {'short': 3}
    assert load_daily_snapshot(tmp_path, date(2026, 11, 4)) is None
