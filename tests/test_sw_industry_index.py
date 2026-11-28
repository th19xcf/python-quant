"""申万 801 行业指数侧车：源端中段缺行登记与本地缺失日剔除。"""

from __future__ import annotations

from datetime import date

import tools.build_sw_industry_index as sw


def _row(day: str, close: float) -> dict:
    return {
        'bargaindate': day, 'openindex': close, 'maxindex': close,
        'minindex': close, 'closeindex': close,
        'bargainamount': 1.0, 'bargainsum': 1.0, 'markup': 0.0,
    }


def test_tail_gap_registers_source_mid_gap() -> None:
    cal = [date(2026, 11, 24), date(2026, 11, 25),
           date(2026, 11, 26), date(2026, 11, 27)]
    gap = sw.tail_gap_for(date(2026, 11, 24), cal)
    assert gap['start'] == '2026-11-25'
    assert gap['rows'] == 3
    assert gap['source_mid_gap'] == ['2026-11-10', '2026-11-17', '2026-11-18']


def test_verify_excludes_source_mid_gap_from_local_missing(monkeypatch) -> None:
    dates = ['2026-11-06', '2026-11-09', '2026-11-11', '2026-11-12',
             '2026-11-13', '2026-11-16', '2026-11-19']
    raw = {'801010': [_row(d, 2500.0 + i) for i, d in enumerate(dates)]}
    frames = {'801010': sw.to_frame('801010', raw['801010'])}
    cal = sorted(date.fromisoformat(d) for d in [
        '2026-11-06', '2026-11-09', '2026-11-10', '2026-11-11', '2026-11-12',
        '2026-11-13', '2026-11-16', '2026-11-17', '2026-11-18', '2026-11-19',
    ])
    monkeypatch.setattr(sw, 'fetch_analysis', lambda day: {})

    rep = sw.verify(frames, raw, cal)
    per = rep['per_index']['801010']

    # 11-10/11-17/11-18 是源端缺行，不得计成本地缺失交易日
    assert per['missing_trade_days'] == 0
    assert per['source_mid_gap_missing'] == [
        '2026-11-10', '2026-11-17', '2026-11-18']
    assert rep['checks']['source_mid_gap_missing_total'] == 3
