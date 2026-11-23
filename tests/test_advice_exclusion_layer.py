#!/usr/bin/env python3

"""统一排除层：默认全关、逐位一致、三条规则各自生效与跳过。"""

from __future__ import annotations

from datetime import date

import polars as pl
from _advice_synth import build_store, business_dates

from src.advice.config import (
    AtoExcludeConfig,
    ExclusionLayerConfig,
    MarginFilterConfig,
    Rev5ExcludeConfig,
    default_config,
)
from src.advice.exclusion_layer import ExclusionLayer
from src.advice.pipeline import AdviceEngine

CODES = [f'6000{index:02d}.SH' for index in range(20)]
DAY = date(2026, 6, 30)


def _generate(store, config, state_dir):
    return AdviceEngine(config=config, store_root=store).generate(
        as_of=DAY, equity=1_000_000, tiers=['mid'], codes=CODES,
        state_dir=state_dir, persist_state=False)


def _history(days, slopes) -> pl.DataFrame:
    rows = []
    for index, code in enumerate(CODES):
        for step, day in enumerate(days):
            rows.append({'date': day, 'ts_code': code,
                         'close': 100.0 * (1.0 + slopes[index]) ** step})
    return pl.DataFrame(rows).with_columns(pl.col('date').cast(pl.Date))


def _write_turnover(root, days, growth) -> None:
    rows = []
    for index, code in enumerate(CODES):
        for step, day in enumerate(days):
            turn = 1.0 + (growth[index] if step == len(days) - 1 else 0.0)
            rows.append({'trade_date': day, 'ts_code': code, 'turn': turn})
    out = root / 'factors'
    out.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).with_columns(
        pl.col('trade_date').cast(pl.Date)).write_parquet(
            out / 'stock_turnover.parquet')


def test_default_disabled_reads_nothing(tmp_path):
    layer = ExclusionLayer(ExclusionLayerConfig(), tmp_path)

    result = layer.status_for(DAY)

    assert result.codes == frozenset()
    assert result.by_rule == {}
    assert result.statuses == ()


def test_rev5_excludes_top_quintile(tmp_path):
    days = business_dates(DAY, 8)
    history = _history(days, [0.001 * index for index in range(len(CODES))])
    layer = ExclusionLayer(
        ExclusionLayerConfig(rev5=Rev5ExcludeConfig(enabled=True,
                                                     min_universe=10)),
        tmp_path, history=history)

    result = layer.status_for(DAY)

    status = result.statuses[0]
    assert status.applied is True
    assert status.universe == len(CODES)
    assert result.codes == frozenset(CODES[-4:])


def test_rev5_skips_when_universe_too_small(tmp_path):
    days = business_dates(DAY, 8)
    history = _history(days, [0.001 * index for index in range(len(CODES))])
    layer = ExclusionLayer(
        ExclusionLayerConfig(rev5=Rev5ExcludeConfig(enabled=True,
                                                     min_universe=100)),
        tmp_path, history=history)

    result = layer.status_for(DAY)

    assert result.codes == frozenset()
    assert result.statuses[0].applied is False
    assert '截面' in result.statuses[0].reason


def test_ato_excludes_top_quintile(tmp_path):
    days = business_dates(DAY, 30)
    _write_turnover(tmp_path, days,
                    [0.0] * 16 + [1.0, 2.0, 3.0, 4.0])
    history = _history(days, [0.001] * len(CODES))
    layer = ExclusionLayer(
        ExclusionLayerConfig(ato=AtoExcludeConfig(enabled=True,
                                                   min_universe=10)),
        tmp_path, history=history)

    result = layer.status_for(DAY)

    status = result.statuses[0]
    assert status.applied is True
    assert result.codes == frozenset(CODES[-4:])


def test_ato_skips_when_observations_insufficient(tmp_path):
    days = business_dates(DAY, 10)
    _write_turnover(tmp_path, days, [0.0] * len(CODES))
    history = _history(days, [0.001] * len(CODES))
    layer = ExclusionLayer(
        ExclusionLayerConfig(ato=AtoExcludeConfig(enabled=True,
                                                   min_universe=10)),
        tmp_path, history=history)

    result = layer.status_for(DAY)

    assert result.codes == frozenset()
    assert result.statuses[0].applied is False


def test_ato_missing_file_skips_with_warning(tmp_path):
    days = business_dates(DAY, 30)
    history = _history(days, [0.001] * len(CODES))
    layer = ExclusionLayer(
        ExclusionLayerConfig(ato=AtoExcludeConfig(enabled=True,
                                                   min_universe=10)),
        tmp_path, history=history)

    result = layer.status_for(DAY)

    assert result.codes == frozenset()
    assert result.statuses[0].applied is False
    assert 'stock_turnover' in result.statuses[0].reason


def test_margin_rule_is_wrapped(tmp_path):
    days = business_dates(DAY, 30)
    history = _history(days, [0.001] * len(CODES))
    layer = ExclusionLayer(
        ExclusionLayerConfig(margin=MarginFilterConfig(enabled=True,
                                                       min_universe=10)),
        tmp_path, history=history)

    result = layer.status_for(DAY)

    assert result.statuses[0].rule == 'margin'
    assert '两融剔除跳过' in result.notes()[0]


def test_engine_disabled_is_bitwise_identical(tmp_path):
    store = build_store(tmp_path / 'store', CODES, end=DAY, days=320)
    base = _generate(store, default_config('legacy'), tmp_path / 'state_a')
    off = _generate(store, default_config('legacy', exclusion=ExclusionLayerConfig(
        rev5=Rev5ExcludeConfig(enabled=False, exclude_top_pct=0.30),
        ato=AtoExcludeConfig(enabled=False, lookback_days=10, min_obs=5),
        margin=MarginFilterConfig(enabled=False, exclude_top_pct=0.30))),
        tmp_path / 'state_b')

    assert base.to_frame().equals(off.to_frame())


def test_engine_rev5_on_skips_on_small_universe(tmp_path):
    store = build_store(tmp_path / 'store', CODES, end=DAY, days=320)
    base = _generate(store, default_config('legacy'), tmp_path / 'state_a')
    on = _generate(store, default_config('legacy', exclusion=ExclusionLayerConfig(
        rev5=Rev5ExcludeConfig(enabled=True, min_universe=100))),
        tmp_path / 'state_b')

    assert base.to_frame().equals(on.to_frame())
    assert any('REV5剔除跳过' in text for text in on.tiers['mid'].notes)
