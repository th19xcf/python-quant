#!/usr/bin/env python3

"""融资余额增速分位剔除闸门：默认关闭、关闭逐位一致、缺口不误剔除。"""

from __future__ import annotations

from datetime import date

import polars as pl
from _advice_synth import build_store, business_dates

from src.advice.config import MarginFilterConfig, default_config
from src.advice.margin_filter import MarginMomentumFilter
from src.advice.pipeline import AdviceEngine

CODES = [f'6000{index:02d}.SH' for index in range(20)]
DAY = date(2026, 6, 30)


def _write_margin(root, days, grows) -> None:
    rows = []
    for index, code in enumerate(CODES):
        base = 1.0e8
        for step, day in enumerate(days):
            rows.append({'trade_date': day, 'ts_code': code,
                         'rzye': base * (1.0 + grows[index]) ** step})
    frame = pl.DataFrame(rows)
    out = root / 'meta' / 'margin_detail'
    out.mkdir(parents=True, exist_ok=True)
    months = (frame.select(pl.col('trade_date').dt.strftime('%Y%m').alias('m'))
              ['m'].unique().to_list())
    for month in months:
        part = frame.filter(pl.col('trade_date').dt.strftime('%Y%m') == month)
        part.write_parquet(out / f'margin_detail_{month}.parquet')


def _write_completeness(root, days, incomplete=()) -> None:
    out = root / 'meta' / 'margin_detail'
    out.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'trade_date': days,
        'is_complete': [day not in incomplete for day in days],
    }).write_parquet(out / 'completeness.parquet')


def _config(enabled: bool, **kwargs) -> MarginFilterConfig:
    return MarginFilterConfig(enabled=enabled, **kwargs)


def _generate(store, config, state_dir):
    return AdviceEngine(config=config, store_root=store).generate(
        as_of=DAY, equity=1_000_000, tiers=['mid'], codes=CODES,
        state_dir=state_dir, persist_state=False)


def test_excludes_only_top_decile(tmp_path):
    days = business_dates(DAY, 12)
    _write_margin(tmp_path, days, [0.01 * index for index in range(len(CODES))])
    _write_completeness(tmp_path, days)
    flt = MarginMomentumFilter(_config(True, min_universe=10), tmp_path)

    excluded, status = flt.status_for(DAY)

    assert status.applied is True
    assert status.universe == len(CODES)
    # 20 只 × 最高 10% ⇒ 排名 19/20 与 20/20 两只被剔除
    assert excluded == {CODES[-1], CODES[-2]}


def test_incomplete_day_skips_without_excluding(tmp_path):
    days = business_dates(DAY, 12)
    _write_margin(tmp_path, days, [0.01 * index for index in range(len(CODES))])
    _write_completeness(tmp_path, days, incomplete={DAY})
    flt = MarginMomentumFilter(_config(True, min_universe=10), tmp_path)

    excluded, status = flt.status_for(DAY)

    assert excluded == set()
    assert status.applied is False
    assert '不完整' in status.reason


def test_missing_completeness_skips(tmp_path):
    days = business_dates(DAY, 12)
    _write_margin(tmp_path, days, [0.01 * index for index in range(len(CODES))])
    flt = MarginMomentumFilter(_config(True, min_universe=10), tmp_path)

    excluded, status = flt.status_for(DAY)

    assert excluded == set()
    assert status.applied is False


def test_missing_lookback_skips(tmp_path):
    days = business_dates(DAY, 3)
    _write_margin(tmp_path, days, [0.01 * index for index in range(len(CODES))])
    _write_completeness(tmp_path, days)
    flt = MarginMomentumFilter(_config(True, min_universe=10), tmp_path)

    excluded, status = flt.status_for(DAY)

    assert excluded == set()
    assert status.applied is False


def test_default_is_disabled(tmp_path):
    flt = MarginMomentumFilter(MarginFilterConfig(), tmp_path)

    excluded, status = flt.status_for(DAY)

    assert excluded == set()
    assert status.reason == 'disabled'


def test_engine_off_is_bitwise_identical(tmp_path):
    store = build_store(tmp_path / 'store', CODES, end=DAY, days=320)
    base = _generate(store, default_config('legacy'), tmp_path / 'state_a')
    off = _generate(store, default_config(
        'legacy', margin_filter=_config(False)), tmp_path / 'state_b')

    assert base.to_frame().equals(off.to_frame())


def test_engine_on_without_margin_data_is_identical(tmp_path):
    store = build_store(tmp_path / 'store', CODES, end=DAY, days=320)
    base = _generate(store, default_config('legacy'), tmp_path / 'state_a')
    on = _generate(store, default_config(
        'legacy', margin_filter=_config(True)), tmp_path / 'state_b')

    assert base.to_frame().equals(on.to_frame())
    assert any('两融剔除跳过' in text for text in on.tiers['mid'].notes)


def test_engine_on_removes_only_excluded_codes(tmp_path):
    store = build_store(tmp_path / 'store', CODES, end=DAY, days=320)
    days = business_dates(DAY, 12)
    _write_margin(store, days, [0.001 * index for index in range(len(CODES))])
    _write_completeness(store, days)
    config = _config(True, min_universe=10)
    off = _generate(store, default_config('legacy'), tmp_path / 'state_a')
    on = _generate(store, default_config('legacy', margin_filter=config),
                   tmp_path / 'state_b')
    excluded, status = MarginMomentumFilter(config, store).status_for(DAY)

    assert status.applied is True
    buys_off = set(off.to_frame().filter(pl.col('direction') == 'BUY')['ts_code'])
    buys_on = set(on.to_frame().filter(pl.col('direction') == 'BUY')['ts_code'])
    assert buys_off - buys_on <= excluded
