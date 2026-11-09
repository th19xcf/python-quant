#!/usr/bin/env python3

"""公告事件本地表契约测试：规范化、导入、冷却边界与覆盖语义。"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

from src.data.events import (
    STATUS_CLEAR,
    STATUS_COOLING,
    STATUS_UNKNOWN,
    EventStore,
    _coerce_events,
    cooldown_range,
    dedup_events,
    get_event_status,
    import_file,
    load_coverage,
    load_events,
    normalize_announcements,
)

#: 2025-01-02 起连续 10 个交易日（含周末跳过）
TRADING = [
    date(2025, 1, 2), date(2025, 1, 3),
    date(2025, 1, 6), date(2025, 1, 7), date(2025, 1, 8),
    date(2025, 1, 9), date(2025, 1, 10),
    date(2025, 1, 13), date(2025, 1, 14), date(2025, 1, 15),
]


def _raw_frame() -> pl.DataFrame:
    return pl.DataFrame({
        'keyword': ['并购', '中标', '处罚', '减持', '并购'],
        'stock_code': ['600000', '600000', '600000', '600000', '600000'],
        'art_code': ['a1', 'a2', 'a3', 'a4', 'a5'],
        'title': ['重大资产重组公告', '重大合同中标公告', '行政处罚决定书',
                  '股东减持公告', '重组进展公告'],
        'display_time': [
            datetime(2025, 1, 3, 16, 0),
            datetime(2025, 1, 3, 16, 5),
            datetime(2025, 1, 3, 16, 10),
            datetime(2025, 1, 3, 16, 15),
            datetime(2025, 1, 10, 16, 0),   # 同类型 30 日内重复 → 去重丢弃
        ],
    })


class TestNormalize:
    def test_keyword_and_title_filters(self):
        events = normalize_announcements(_raw_frame())
        assert set(events['event_type'].to_list()) == {
            '并购重组', '重大合同/中标', '监管处罚', '减持'}
        # 第 5 条与第 1 条同类型同标的，30 日内去重丢弃
        assert events.height == 4

    def test_follow_up_announcement_dropped(self):
        events = normalize_announcements(_raw_frame())
        ids = events['event_id'].to_list()
        assert 'a5-600000' not in ids

    def test_ts_code_converted(self):
        events = normalize_announcements(_raw_frame())
        assert set(events['ts_code'].to_list()) == {'600000.SH'}


class TestImport:
    def test_csv_import_and_coverage(self, tmp_path):
        csv = tmp_path / 'events.csv'
        pl.DataFrame({
            'ts_code': ['600000.SH', '000001.SZ'],
            'event_type': ['并购重组', '监管处罚'],
            'publish_time': ['2025-01-03 16:00:00', '2025-03-05 16:00:00'],
            'title': ['重组', '处罚'],
            'event_id': ['e1', 'e2'],
        }).write_csv(csv)
        info = import_file(csv, tmp_path, source='test')
        assert info['rows'] == 2
        frame = load_events(tmp_path)
        assert frame.height == 2
        coverage = load_coverage(tmp_path)
        assert coverage['start'] == '2025-01-03'
        assert coverage['end'] == '2025-03-05'
        assert coverage['complete'] is True

    def test_append_dedup_by_event_id(self, tmp_path):
        csv = tmp_path / 'first.csv'
        pl.DataFrame({
            'ts_code': ['600000.SH'], 'event_type': ['并购重组'],
            'publish_time': ['2025-01-03 16:00:00'],
            'title': ['重组'], 'event_id': ['e1'],
        }).write_csv(csv)
        import_file(csv, tmp_path, source='test')
        import_file(csv, tmp_path, mode='append', source='test')
        assert load_events(tmp_path).height == 1

    def test_raw_file_needs_keyword_columns(self, tmp_path):
        csv = tmp_path / 'bad.csv'
        pl.DataFrame({'a': [1]}).write_csv(csv)
        with pytest.raises(ValueError, match='无法识别'):
            import_file(csv, tmp_path)


def _normalized_csv(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows).write_csv(path)
    return path


class TestAppendSafety:
    """append 语义：既有行零丢失，30 日去重只在本次新批次内部生效。"""

    def test_existing_rows_never_evicted(self, tmp_path):
        seed = _normalized_csv(tmp_path / 'seed.csv', [{
            'ts_code': '600000.SH', 'event_type': '并购重组',
            'publish_time': '2024-01-02 16:00:00',
            'title': '重组', 'event_id': 'e1',
        }])
        import_file(seed, tmp_path, source='test')

        # 新批次里出现同一标的同一类型的更早公告（2023-11-01，距既有行 62 天，
        # 不在 30 日窗口内），旧逻辑会在全表 30 日去重时把 2024-01-02 的既有行挤掉
        batch = _normalized_csv(tmp_path / 'batch.csv', [{
            'ts_code': '600000.SH', 'event_type': '并购重组',
            'publish_time': '2023-11-01 16:00:00',
            'title': '重组预案', 'event_id': 'e0',
        }])
        info = import_file(batch, tmp_path, mode='append', source='test')

        frame = load_events(tmp_path)
        ids = set(frame['event_id'].to_list())
        assert ids == {'e0', 'e1'}
        assert info['existing_affected'] == 0
        # 既有行时间戳与标题一字未改
        kept = frame.filter(pl.col('event_id') == 'e1').to_dicts()[0]
        assert kept['publish_time'] == datetime(2024, 1, 2, 16, 0)
        assert kept['title'] == '重组'

    def test_dedup_only_inside_new_batch(self, tmp_path):
        seed = _normalized_csv(tmp_path / 'seed.csv', [{
            'ts_code': '000001.SZ', 'event_type': '监管处罚',
            'publish_time': '2025-01-03 16:00:00',
            'title': '处罚', 'event_id': 'e9',
        }])
        import_file(seed, tmp_path, source='test')

        batch = _normalized_csv(tmp_path / 'batch.csv', [
            {'ts_code': '600000.SH', 'event_type': '并购重组',
             'publish_time': '2025-02-10 16:00:00',
             'title': '重组', 'event_id': 'n1'},
            {'ts_code': '600000.SH', 'event_type': '并购重组',
             'publish_time': '2025-02-20 16:00:00',
             'title': '重组进展', 'event_id': 'n2'},
        ])
        info = import_file(batch, tmp_path, mode='append', source='test')

        frame = load_events(tmp_path)
        assert frame.height == 2
        assert set(frame['event_id'].to_list()) == {'e9', 'n1'}
        assert info['added'] == 1
        assert info['dropped_in_batch'] == 1

    def test_repeated_import_is_idempotent(self, tmp_path):
        csv = _normalized_csv(tmp_path / 'batch.csv', [{
            'ts_code': '600000.SH', 'event_type': '并购重组',
            'publish_time': '2025-02-10 16:00:00',
            'title': '重组', 'event_id': 'n1',
        }])
        import_file(csv, tmp_path, source='test')
        first = load_events(tmp_path)
        info = import_file(csv, tmp_path, mode='append', source='test')
        second = load_events(tmp_path)
        assert first.equals(second)
        assert info['added'] == 0
        assert info['dropped_existing_conflict'] == 1

    def test_dedup_is_order_independent(self):
        """同一天多份同类型公告时，代表行不能随输入行序漂移。"""
        rows = [
            {'event_id': f'a{i}-600000', 'ts_code': '600000.SH',
             'event_type': '股权激励',
             'publish_time': datetime(2025, 3, 5, 16, 0),
             'title': f'激励计划公告 {i}', 'source': 'test'}
            for i in range(6)
        ]
        base = dedup_events(_coerce_events(pl.DataFrame(rows)))
        for shift in range(1, 6):
            rolled = rows[shift:] + rows[:shift]
            frame = dedup_events(_coerce_events(pl.DataFrame(rolled)))
            assert frame['event_id'].to_list() == base['event_id'].to_list()

    def test_same_day_alternate_not_added_again(self, tmp_path):
        """同日同类型但不同 art_code 的替代件，重复导入不得再次写入。"""
        seed = _normalized_csv(tmp_path / 'seed.csv', [
            {'ts_code': '600000.SH', 'event_type': '股权激励',
             'publish_time': '2025-03-05 16:00:00',
             'title': '激励计划草案', 'event_id': 'a1-600000'},
        ])
        import_file(seed, tmp_path, source='test')

        # 同一事件的律所意见书（不同 art_code，同日同类型），应被 30 日窗口挡掉
        batch = _normalized_csv(tmp_path / 'batch.csv', [
            {'ts_code': '600000.SH', 'event_type': '股权激励',
             'publish_time': '2025-03-05 16:00:00',
             'title': '律所意见书', 'event_id': 'a2-600000'},
        ])
        info = import_file(batch, tmp_path, mode='append', source='test')

        assert info['added'] == 0
        assert info['dropped_near_existing'] == 1
        assert info['existing_affected'] == 0
        frame = load_events(tmp_path)
        assert frame['event_id'].to_list() == ['a1-600000']

    def test_dry_run_writes_nothing(self, tmp_path):
        csv = _normalized_csv(tmp_path / 'batch.csv', [
            {'ts_code': '600000.SH', 'event_type': '并购重组',
             'publish_time': '2025-02-10 16:00:00',
             'title': '重组', 'event_id': 'n1'},
        ])
        info = import_file(csv, tmp_path, mode='append', source='test',
                           dry_run=True)
        assert info['dry_run'] is True
        assert info['added'] == 1
        assert not (tmp_path / 'meta' / 'events' / 'events.parquet').exists()
        assert load_events(tmp_path).is_empty()

        import_file(csv, tmp_path, mode='append', source='test')
        before = (tmp_path / 'meta' / 'events' / 'events.parquet').read_bytes()
        preview = import_file(csv, tmp_path, mode='append', source='test',
                              dry_run=True)
        after = (tmp_path / 'meta' / 'events' / 'events.parquet').read_bytes()
        assert before == after
        assert preview['added'] == 0


class TestCooldown:
    def test_range_starts_at_t_plus_one(self):
        span = cooldown_range(datetime(2025, 1, 3, 16, 0), TRADING, 5)
        assert span == (date(2025, 1, 6), date(2025, 1, 10))

    def test_range_skips_weekend_announcement(self):
        span = cooldown_range(datetime(2025, 1, 4, 10, 0), TRADING, 5)
        assert span == (date(2025, 1, 6), date(2025, 1, 10))

    def test_range_none_when_calendar_exhausted(self):
        assert cooldown_range(datetime(2025, 1, 15), TRADING, 5) is None

    def test_zone_semantics(self, tmp_path):
        pl.DataFrame({
            'ts_code': ['600000.SH'], 'event_type': ['监管处罚'],
            'publish_time': [datetime(2025, 1, 3, 16, 0)],
            'title': ['处罚'], 'event_id': ['e1'],
        }).write_parquet(tmp_path / 'org.parquet')
        import_file(tmp_path / 'org.parquet', tmp_path, source='test',
                    coverage_start='2025-01-02', coverage_end='2025-01-15')
        store = EventStore(tmp_path)

        # T 当天不冷却（区间从 T+1 开始）
        assert store.status('600000.SH', date(2025, 1, 3), TRADING)['status'] \
            == STATUS_CLEAR
        # T+1 进入冷却，T+N 最后一天仍在冷却，T+N+1 解除
        assert store.status('600000.SH', date(2025, 1, 6), TRADING)['status'] \
            == STATUS_COOLING
        assert store.status('600000.SH', date(2025, 1, 10), TRADING)['status'] \
            == STATUS_COOLING
        assert store.status('600000.SH', date(2025, 1, 13), TRADING)['status'] \
            == STATUS_CLEAR

    def test_coverage_outside_is_unknown(self, tmp_path):
        pl.DataFrame({
            'ts_code': ['600000.SH'], 'event_type': ['监管处罚'],
            'publish_time': [datetime(2025, 1, 3, 16, 0)],
            'title': ['处罚'], 'event_id': ['e1'],
        }).write_parquet(tmp_path / 'org.parquet')
        import_file(tmp_path / 'org.parquet', tmp_path, source='test')
        store = EventStore(tmp_path)
        assert store.status('600000.SH', date(2024, 12, 31), TRADING)['status'] \
            == STATUS_UNKNOWN
        assert store.status('600000.SH', date(2025, 2, 1), TRADING)['status'] \
            == STATUS_UNKNOWN

    def test_missing_table_is_unknown(self, tmp_path):
        store = EventStore(tmp_path)
        assert not store.available
        assert store.status('600000.SH', date(2025, 1, 6), TRADING)['status'] \
            == STATUS_UNKNOWN

    def test_get_event_status_requires_calendar(self, tmp_path):
        with pytest.raises(ValueError, match='交易日历'):
            get_event_status('600000.SH', date(2025, 1, 6), trading_dates=[],
                             store_root=tmp_path)

    def test_blocked_map_matches_status(self, tmp_path: Path):
        pl.DataFrame({
            'ts_code': ['600000.SH'], 'event_type': ['监管处罚'],
            'publish_time': [datetime(2025, 1, 3, 16, 0)],
            'title': ['处罚'], 'event_id': ['e1'],
        }).write_parquet(tmp_path / 'org.parquet')
        import_file(tmp_path / 'org.parquet', tmp_path, source='test',
                    coverage_start='2025-01-02', coverage_end='2025-01-15')
        blocked = EventStore(tmp_path).blocked_map(TRADING)
        assert blocked[date(2025, 1, 6)]['600000.SH']['event_type'] == '监管处罚'
        assert date(2025, 1, 3) not in blocked
        assert date(2025, 1, 10) in blocked
        assert date(2025, 1, 13) not in blocked
