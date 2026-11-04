#!/usr/bin/env python3

"""数据契约巡检与已知异常台账测试。"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, timedelta

import polars as pl
import pytest

from src.data.quality.anomalies import (
    KnownAnomaly,
    anomaly_action,
    find_anomalies,
    is_known_anomaly,
    iter_anomalies,
    register_anomaly,
)
from src.data.quality.checker import (
    LocalStoreFrameSource,
    QualityChecker,
    TdxFrameSource,
    coerce_date,
    prepare_frame,
)
from src.data.quality.rules import (
    RuleContext,
    rule_amount_dimension,
    rule_change_mismatch,
    rule_duplicate_key,
    rule_future_trade_date,
    rule_high_lt_low,
    rule_high_lt_max_open_close,
    rule_low_gt_min_open_close,
    rule_pct_chg_mismatch,
    rule_pre_close_mismatch,
    rule_price_positive,
    rule_required_columns,
    rule_volume_invalid,
    rule_weekend_trade_date,
)


def make_frame(rows: int = 5, start: date = date(2024, 1, 2)) -> pl.DataFrame:
    """构造契约完整、数值自洽的行情帧（仅工作日）。"""
    dates: list[date] = []
    current = start
    while len(dates) < rows:
        if current.weekday() < 5:
            dates.append(current)
        current += timedelta(days=1)
    closes = [10.0 + index for index in range(rows)]
    pre_closes = [0.0, *closes[:-1]]
    changes = [
        close - pre if pre > 0 else 0.0
        for close, pre in zip(closes, pre_closes, strict=False)
    ]
    pct_chgs = [
        (close / pre - 1) * 100 if pre > 0 else 0.0
        for close, pre in zip(closes, pre_closes, strict=False)
    ]
    return pl.DataFrame({
        'date': dates,
        'open': closes,
        'high': [close + 0.5 for close in closes],
        'low': [close - 0.5 for close in closes],
        'close': closes,
        'volume': [1000.0 for _ in closes],
        'amount': [close * 1000 * 100 for close in closes],
        'pre_close': pre_closes,
        'change': changes,
        'pct_chg': pct_chgs,
        'qfq_factor': [1.0 for _ in closes],
    })


def run_rule(rule, frame: pl.DataFrame, data_type: str = 'stock',
             latest: date = date(2024, 2, 1)) -> pl.DataFrame:
    return rule(frame, RuleContext(data_type=data_type, latest_trade_date=latest))


def test_clean_frame_has_no_violations() -> None:
    frame = make_frame()
    rules = (
        rule_required_columns, rule_price_positive, rule_high_lt_max_open_close,
        rule_low_gt_min_open_close, rule_high_lt_low, rule_volume_invalid,
        rule_amount_dimension, rule_future_trade_date, rule_weekend_trade_date,
        rule_duplicate_key, rule_pre_close_mismatch, rule_change_mismatch,
        rule_pct_chg_mismatch,
    )
    for rule in rules:
        assert run_rule(rule, frame).is_empty(), rule.__name__


def test_price_and_volume_rules_detect_invalid_values() -> None:
    frame = make_frame(rows=3)
    dates = frame['date'].to_list()
    frame = frame.with_columns([
        pl.when(pl.col('date') == pl.lit(dates[-1]))
        .then(pl.lit(-1.0)).otherwise(pl.col('close')).alias('close'),
        pl.when(pl.col('date') == pl.lit(dates[0]))
        .then(pl.lit(-5.0))
        .when(pl.col('date') == pl.lit(dates[-1]))
        .then(pl.lit(float('nan')))
        .otherwise(pl.col('volume')).alias('volume'),
        pl.when(pl.col('date') == pl.lit(dates[0]))
        .then(pl.lit(float('inf'))).otherwise(pl.col('amount')).alias('amount'),
    ])
    assert run_rule(rule_price_positive, frame).height == 1
    assert run_rule(rule_volume_invalid, frame).height == 2


def test_ohlc_structure_rules() -> None:
    frame = make_frame(rows=3)
    dates = frame['date'].to_list()
    frame = frame.with_columns([
        pl.when(pl.col('date') == pl.lit(dates[0]))
        .then(pl.lit(9.9)).otherwise(pl.col('high')).alias('high'),
        pl.when(pl.col('date') == pl.lit(dates[0]))
        .then(pl.lit(11.2))
        .when(pl.col('date') == pl.lit(dates[-1]))
        .then(pl.lit(9.0))
        .otherwise(pl.col('low')).alias('low'),
    ])
    assert run_rule(rule_high_lt_max_open_close, frame).height == 1
    assert run_rule(rule_low_gt_min_open_close, frame).height == 1
    assert run_rule(rule_high_lt_low, frame).height >= 1


def test_amount_dimension_only_applies_to_stock() -> None:
    frame = make_frame(rows=2).with_columns(
        (pl.col('close') * pl.col('volume') * 100 * 50).alias('amount'))
    assert run_rule(rule_amount_dimension, frame, 'stock').height == 2
    assert run_rule(rule_amount_dimension, frame, 'index').is_empty()


def test_future_and_weekend_dates() -> None:
    dates = [date(2024, 1, 5) + timedelta(days=index) for index in range(4)]
    frame = make_frame(rows=4).with_columns(pl.Series('date', dates))
    violations = run_rule(rule_weekend_trade_date, frame)
    assert violations.height == 2
    future = make_frame(rows=2, start=date(2024, 3, 1))
    assert run_rule(rule_future_trade_date, future,
                    latest=date(2024, 2, 1)).height == 2


def test_duplicate_key_and_required_columns() -> None:
    frame = make_frame(rows=3)
    duplicated = pl.concat([frame, frame.slice(1, 1)])
    assert run_rule(rule_duplicate_key, duplicated).height == 1

    with_null = frame.with_columns(
        pl.when(pl.col('date') == pl.lit(frame['date'].to_list()[0]))
        .then(pl.lit(None, dtype=pl.Float64))
        .otherwise(pl.col('close')).alias('close'))
    assert run_rule(rule_required_columns, with_null).height == 1

    missing = frame.drop('amount')
    violations = run_rule(rule_required_columns, missing)
    assert violations.height == 1
    assert violations['detail'].to_list() == ['缺少必需列']


def test_derived_field_rules() -> None:
    frame = make_frame(rows=4)
    bad_date = frame['date'].to_list()[1]
    frame = frame.with_columns([
        pl.when(pl.col('date') == pl.lit(bad_date))
        .then(pl.lit(999.0)).otherwise(pl.col('pre_close')).alias('pre_close'),
        pl.when(pl.col('date') == pl.lit(bad_date))
        .then(pl.lit(999.0)).otherwise(pl.col('change')).alias('change'),
        pl.when(pl.col('date') == pl.lit(bad_date))
        .then(pl.lit(999.0)).otherwise(pl.col('pct_chg')).alias('pct_chg'),
    ])
    assert run_rule(rule_pre_close_mismatch, frame).height == 1
    assert run_rule(rule_change_mismatch, frame).height == 1
    assert run_rule(rule_pct_chg_mismatch, frame).height == 1


def test_prepare_frame_normalizes_date_and_numeric_dtypes() -> None:
    frame = pl.DataFrame({
        'date': ['2024-01-02', '2024-01-03'],
        'open': [1, 2],
        'high': [1.5, 2.5],
        'low': [0.5, 1.5],
        'close': [1.2, 2.2],
        'volume': [100, 200],
        'amount': [12000, 44000],
    })
    prepared = prepare_frame(frame)
    assert prepared.schema['date'] == pl.Date
    assert prepared.schema['volume'] == pl.Float64
    assert coerce_date('20240102') == date(2024, 1, 2)
    assert coerce_date(date(2024, 1, 2)) == date(2024, 1, 2)
    assert coerce_date(None) is None
    with pytest.raises(ValueError):
        coerce_date('not-a-date')


class FakeSource:
    """测试用巡检源。"""

    def __init__(self, frames: dict[str, dict[str, pl.DataFrame]],
                 name: str = 'local') -> None:
        self.name = name
        self._frames = frames

    def codes(self, data_type: str) -> list[str]:
        return sorted(self._frames.get(data_type, {}))

    def load(self, data_type: str, ts_code: str) -> pl.DataFrame | None:
        return self._frames.get(data_type, {}).get(ts_code)


def test_checker_scope_filters_codes_and_date_range(tmp_path) -> None:
    frame = make_frame(rows=4)
    bad_date = frame['date'].to_list()[-1]
    bad = frame.with_columns(
        pl.when(pl.col('date') == pl.lit(bad_date))
        .then(pl.lit(-1.0)).otherwise(pl.col('close')).alias('close'))
    source = FakeSource({'stock': {'600000.SH': frame, '000001.SZ': bad},
                         'index': {'000001.SH': frame}})
    checker = QualityChecker(source, latest_trade_date=date(2024, 2, 1))

    report = checker.run(codes=['600000.SH'], write_report=False)
    assert report.symbols_scanned == 1
    assert report.rows_scanned == 4
    assert not report.violations

    report = checker.run(codes=['000001.SZ'], start=bad_date,
                         write_report=False)
    assert report.rows_scanned == 4
    assert all(v.trade_date == bad_date for v in report.violations)
    assert report.by_rule['price_non_positive'] == 1

    report = checker.run(write_report=False)
    assert report.symbols_scanned == 3
    assert report.scope['codes'] == 'all'


def test_checker_writes_reports_and_dry_run(tmp_path) -> None:
    frame = make_frame(rows=1).with_columns([
        pl.lit(-1.0).alias('open'),
        pl.lit(-0.5).alias('high'),
        pl.lit(-1.5).alias('low'),
        pl.lit(-1.0).alias('close'),
    ])
    source = FakeSource({'stock': {'600000.SH': frame}, 'index': {}})
    checker = QualityChecker(source)

    report = checker.run(codes=['600000.SH'], write_report=True,
                         output_dir=str(tmp_path))
    assert report.report_files
    payload = json.loads(
        (tmp_path / _basename(report.report_files['json'])).read_text('utf-8'))
    assert payload['summary']['total_violations'] == 1
    assert payload['summary']['unique_violation_rows'] == 1
    assert (tmp_path / _basename(report.report_files['violations_csv'])).exists()
    assert (tmp_path / _basename(report.report_files['summary_csv'])).exists()

    dry = checker.run(codes=['600000.SH'], write_report=False,
                      output_dir=str(tmp_path))
    assert dry.report_files == {}
    assert len(list(tmp_path.glob('data_quality_*'))) == 3


def _basename(path: str) -> str:
    from pathlib import Path

    return Path(path).name


def test_local_store_source_wraps_store() -> None:
    frame = make_frame(rows=2)

    class Store:
        def codes(self, data_type: str) -> list[str]:
            return ['600000.SH']

        def read_file(self, data_type: str, ts_code: str) -> pl.DataFrame | None:
            return frame

    source = LocalStoreFrameSource(Store())
    assert source.name == 'local'
    assert source.codes('stock') == ['600000.SH']
    loaded = source.load('stock', '600000.SH')
    assert loaded is not None
    assert loaded.height == 2


def test_tdx_source_uses_discovery_and_parse(tmp_path, monkeypatch) -> None:
    (tmp_path / 'sh600000.day').write_text('', encoding='utf-8')
    source = TdxFrameSource(tmp_path)
    monkeypatch.setattr(
        'src.data.local_store.build.discover_local_files',
        lambda root, kind: [('stock', '600000.SH', tmp_path / 'sh600000.day')],
    )
    monkeypatch.setattr(
        'src.data.tdx_day_parser.parse_day_file',
        lambda path: make_frame(rows=2),
    )
    assert source.codes('stock') == ['600000.SH']
    assert source.codes('index') == []
    loaded = source.load('stock', '600000.SH')
    assert loaded is not None
    assert loaded.height == 2
    assert source.load('stock', '000001.SZ') is None


def test_known_anomaly_hits_and_misses() -> None:
    assert is_known_anomaly('000001.SH', '1992-06-16')
    assert is_known_anomaly('sh.000001', date(1991, 5, 9))
    assert is_known_anomaly('399001.SZ', '1991-04-06')
    assert is_known_anomaly('000004.SZ', '2010-01-01', context='factor')
    assert is_known_anomaly('600547.SH', '2000-01-01', context='legacy')
    assert not is_known_anomaly('000004.SZ', '2010-01-01', context='local')
    assert not is_known_anomaly('600547.SH', '2000-01-01', context='local')
    assert not is_known_anomaly('600547.SH', '2021-06-23')
    assert not is_known_anomaly('600000.SH', '2010-01-01')
    assert anomaly_action('000001.SH', '1992-06-16') == 'exclude'
    assert anomaly_action('000004.SZ', '2010-01-01', context='factor') == 'flag'


def test_known_anomaly_context_and_extension() -> None:
    # pre_close 缺口只对本地列存上下文生效
    assert is_known_anomaly('000001.SH', '1991-05-10')
    assert not is_known_anomaly('000001.SH', '1991-05-10', context='tdx')

    record = KnownAnomaly(
        anomaly_id='test_extension_only',
        ts_code='600000.SH',
        issue_type='test_issue',
        action='flag',
        reason='扩展接口测试',
        source='unit-test',
        dates=(date(2020, 1, 2),),
    )
    register_anomaly(record)
    try:
        assert is_known_anomaly('600000.SH', '2020-01-02')
        assert not is_known_anomaly('600000.SH', '2020-01-03')
        assert any(item.anomaly_id == 'test_extension_only' for item in iter_anomalies())
        hits = find_anomalies('600000.SH', '2020-01-02')
        assert [item.anomaly_id for item in hits] == ['test_extension_only']
        # 幂等：重复登记同一条记录不报错、不产生新条目
        before = len(list(iter_anomalies()))
        register_anomaly(record)
        assert len(list(iter_anomalies())) == before
        # 同 ID 但内容不同仍报错，避免静默覆盖
        with pytest.raises(ValueError):
            register_anomaly(replace(record, reason='不同内容'))
    finally:
        from src.data.quality import anomalies as module

        module._REGISTRY.pop('test_extension_only', None)
