"""数据新鲜度自检回归测试。

覆盖判定：
1. 滞后 1 个交易日不报错；
2. 滞后 6 个交易日报 error 且命令行退出码非零；
3. 正式表缺失报 error，而不是静默跳过；
4. 行情有而名录无 / 行情有而估值无；
5. 名录刷新日期陈旧、事件表滞后、ST 区间水位滞后；
6. 正常时只输出一行汇总。

另覆盖基础信息表的「新增标的」口径与权威清单登记校验。
"""

import json
import os
from datetime import date, datetime, time, timedelta

import polars as pl
import pytest

from src.data.quality import freshness_check as fc


def _biz_days(count: int, end: date = date(2026, 11, 9)) -> list[date]:
    days: list[date] = []
    cursor = end
    while len(days) < count:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor -= timedelta(days=1)
    return sorted(days)


def _write_quotes(store, code: str, days: list[date]) -> None:
    path = store / 'quotes' / 'stock' / 'sh' / f'{code}.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'date': days, 'close': [1.0] * len(days)}).write_parquet(path)


def _write_date_table(path, column: str, value: date) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({column: [value]}).write_parquet(path)


def _write_basic(path, codes: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'ts_code': codes}).write_parquet(path)


def _write_fund_basic(path, rows: list[tuple[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'ts_code': [row[0] for row in rows],
                  'fund_type': [row[1] for row in rows]}).write_parquet(path)


def _write_quotes_dir(store, kind: str, codes: list[str]) -> None:
    for code in codes:
        path = store / 'quotes' / kind / 'sh' / f'{code}.parquet'
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({'date': [date(2026, 11, 9)]}).write_parquet(path)


def _write_daily_index(store, rows: list[tuple[str, str, date]]) -> None:
    """数据层全集登记表：行情文件必须在这里，日更缺口计划才看得见。"""
    path = store / 'meta' / 'daily_index.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'ts_code': [row[0] for row in rows],
                  'data_type': [row[1] for row in rows],
                  'end': [row[2] for row in rows]}).write_parquet(path)


def _write_stale_status(store, rows: list[tuple[str, str, bool]]) -> None:
    path = store / 'meta' / 'universe' / 'stale_status.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'ts_code': [row[0] for row in rows],
                  'asset_class': [row[1] for row in rows],
                  'is_stale': [row[2] for row in rows]}).write_parquet(path)


def _write_doc(tmp_path, skip: str = '') -> object:
    paths = [spec.rel_path for spec in fc.TABLE_SPECS if spec.rel_path != skip]
    body = '\n'.join(f'| 语义 | `{path}` | 消费者 | 理由 |' for path in paths)
    doc = tmp_path / '权威清单.md'
    doc.write_text('# 数据权威路径清单\n\n' + body + '\n', encoding='utf-8')
    return doc


def _touch(path, day: date) -> None:
    """把文件刷新日期固定到指定日（名录陈旧判定用）。"""
    stamp = datetime.combine(day, time(12, 0)).timestamp()
    os.utime(path, (stamp, stamp))


def _build_store(tmp_path, *, daily_end: date, coverage_end: date, event_end: date):
    store = tmp_path / 'local_store'
    calendar = _biz_days(12)
    _write_quotes(store, '600000.SH', calendar)
    _write_date_table(store / 'factors' / 'stock_dividend.parquet', 'notice_date', event_end)
    _write_date_table(store / 'factors' / 'stock_adj_factor.parquet', 'trade_date', daily_end)
    _write_date_table(store / 'meta' / 'breadth' / 'breadth_daily.parquet',
                      'trade_date', daily_end)
    _write_date_table(store / 'meta' / 'st' / 'st_coverage.parquet', 'coverage_end', coverage_end)
    pool = store / 'meta' / 'multi_asset' / 'etf_pool.parquet'
    pool.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'ts_code': ['510300.SH'], 'coverage_end': [coverage_end]}).write_parquet(pool)
    valuation = store / 'meta' / 'financials' / 'valuation_daily.parquet'
    valuation.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'ts_code': ['600000.SH'], 'trade_date': [daily_end]}).write_parquet(valuation)
    events = store / 'meta' / 'events' / 'events.parquet'
    events.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'event_id': ['e1'], 'ts_code': ['600000.SH'],
                  'publish_time': [datetime.combine(event_end, time(9, 0))]}).write_parquet(events)
    coverage = store / 'meta' / 'st' / 'coverage.json'
    coverage.write_text(f'{{"snapshot_date": "{coverage_end.isoformat()}"}}', encoding='utf-8')
    pl.DataFrame({'code': ['600000.SH'], 'start_date': [daily_end],
                  'end_date': [None]}).write_parquet(store / 'meta' / 'st' / 'st_intervals.parquet')
    _write_basic(store / 'meta' / 'fund_basic.parquet', ['510300.SH'])
    _write_basic(store / 'meta' / 'index_basic.parquet', ['000001.SH'])
    _write_basic(store / 'meta' / 'stock_basic.parquet', ['600000.SH'])
    for rel in ('meta/fund_basic.parquet', 'meta/index_basic.parquet',
                'meta/stock_basic.parquet'):
        _touch(store / rel, daily_end)
    # 事件表的滞后基准是文件刷新日期，必须把 mtime 固定到 event_end
    for rel in ('factors/stock_dividend.parquet', 'meta/events/events.parquet'):
        _touch(store / rel, event_end)
    _write_quotes_dir(store, 'fund', ['510300.SH'])
    _write_quotes_dir(store, 'index', ['000001.SH'])
    _write_date_table(store / 'factors' / 'fund_dividend.parquet', 'ex_date', daily_end)
    _write_daily_index(store, [('600000.SH', 'stock', daily_end),
                               ('510300.SH', 'fund', daily_end),
                               ('000001.SH', 'index', daily_end)])
    _write_stale_status(store, [])
    return store, calendar


def _row(report, rel_path):
    return next(row for row in report.rows if row.rel_path == rel_path)


def test_lag_one_trading_day_is_ok(tmp_path):
    calendar = _biz_days(12)
    store, calendar = _build_store(tmp_path, daily_end=calendar[-2],
                                   coverage_end=calendar[-1], event_end=calendar[-2])
    report = fc.run_check(store, calendar=calendar, doc_path=_write_doc(tmp_path))

    assert report.ok is True
    assert report.errors == []
    assert _row(report, 'factors/stock_adj_factor.parquet').lag == 1
    assert _row(report, 'factors/stock_adj_factor.parquet').verdict == 'ok'


def test_lag_six_trading_days_reports_error_and_nonzero_exit(tmp_path, monkeypatch):
    calendar = _biz_days(12)
    stale = calendar[-7]
    store, calendar = _build_store(tmp_path, daily_end=stale,
                                   coverage_end=stale, event_end=stale)

    report = fc.run_check(store, calendar=calendar, doc_path=_write_doc(tmp_path))
    row = _row(report, 'factors/stock_adj_factor.parquet')
    assert row.lag == 6
    assert row.verdict == 'error'
    assert report.ok is False

    class _Calendar:
        def authoritative_dates(self):
            return calendar

    monkeypatch.setattr('src.data.trading_calendar.get_calendar',
                        lambda *_args, **_kwargs: _Calendar())
    monkeypatch.setattr('src.data.local_store.paths.resolve_store_root',
                        lambda _config: store)
    monkeypatch.setattr('src.utils.config.get_config', lambda: object())

    assert fc.main([]) == 1


def test_missing_table_reports_error_instead_of_skipping(tmp_path):
    calendar = _biz_days(12)
    store, calendar = _build_store(tmp_path, daily_end=calendar[-1],
                                   coverage_end=calendar[-1], event_end=calendar[-1])
    (store / 'meta' / 'breadth' / 'breadth_daily.parquet').unlink()

    report = fc.run_check(store, calendar=calendar, doc_path=_write_doc(tmp_path))

    row = _row(report, 'meta/breadth/breadth_daily.parquet')
    assert row.verdict == 'error'
    assert '正式表不存在' in row.detail
    assert len(report.rows) == len(fc.TABLE_SPECS) + 1


def test_base_info_uses_new_code_count_not_date_threshold(tmp_path):
    calendar = _biz_days(12)
    store, calendar = _build_store(tmp_path, daily_end=calendar[-1],
                                   coverage_end=calendar[-1], event_end=calendar[-1])
    _write_quotes_dir(store, 'fund', ['510300.SH', '506000.SH'])

    report = fc.run_check(store, calendar=calendar, doc_path=_write_doc(tmp_path))

    row = _row(report, 'meta/fund_basic.parquet')
    assert row.data_end == calendar[-1]
    assert row.lag == 0
    assert row.gap == 1
    assert row.verdict == 'warning'
    assert '506000.SH' in row.detail


def test_base_info_stale_refresh_date_reports_warning(tmp_path):
    calendar = _biz_days(12)
    store, calendar = _build_store(tmp_path, daily_end=calendar[-1],
                                   coverage_end=calendar[-1], event_end=calendar[-1])
    _touch(store / 'meta' / 'index_basic.parquet', calendar[-8])

    report = fc.run_check(store, calendar=calendar, doc_path=_write_doc(tmp_path))

    row = _row(report, 'meta/index_basic.parquet')
    assert row.data_end == calendar[-8]
    assert row.lag == 7
    assert row.verdict == 'warning'
    assert '刷新日期' in row.detail


def test_valuation_missing_active_code_reports_warning(tmp_path):
    calendar = _biz_days(12)
    store, calendar = _build_store(tmp_path, daily_end=calendar[-1],
                                   coverage_end=calendar[-1], event_end=calendar[-1])
    pl.DataFrame({'ts_code': ['600000.SH'], 'trade_date': [calendar[-1]]}).write_parquet(
        store / 'meta' / 'financials' / 'valuation_daily.parquet')
    _write_quotes(store, '000001.SZ', calendar)

    report = fc.run_check(store, calendar=calendar, doc_path=_write_doc(tmp_path))

    row = _row(report, 'meta/financials/valuation_daily.parquet')
    assert row.gap == 1
    assert row.verdict == 'warning'
    assert '000001.SZ' in row.detail


def test_event_stale_refresh_file_reports_error(tmp_path):
    calendar = _biz_days(12)
    old = calendar[-1] - timedelta(days=100)
    store, calendar = _build_store(tmp_path, daily_end=calendar[-1],
                                   coverage_end=calendar[-1], event_end=old)

    report = fc.run_check(store, calendar=calendar, doc_path=_write_doc(tmp_path))

    row = _row(report, 'meta/events/events.parquet')
    assert row.lag == 100
    assert row.verdict == 'error'


def test_event_old_data_but_fresh_file_is_ok(tmp_path):
    """连续无公告是合法状态：事件末日很旧，但表已随日更刷新就不算缺口。"""
    calendar = _biz_days(12)
    old = calendar[-1] - timedelta(days=100)
    store, calendar = _build_store(tmp_path, daily_end=calendar[-1],
                                   coverage_end=calendar[-1], event_end=old)
    _touch(store / 'meta' / 'events' / 'events.parquet', calendar[-1])

    report = fc.run_check(store, calendar=calendar, doc_path=_write_doc(tmp_path))

    row = _row(report, 'meta/events/events.parquet')
    assert row.refresh_date == calendar[-1]
    assert row.lag == 0
    assert row.verdict == 'ok'


def test_universe_covers_full_market_and_counts(tmp_path):
    """数据层按全集：行情目录里每只标的都计入，并报全集只数。"""
    calendar = _biz_days(12)
    store, calendar = _build_store(tmp_path, daily_end=calendar[-1],
                                   coverage_end=calendar[-1], event_end=calendar[-1])

    report = fc.run_check(store, calendar=calendar, doc_path=_write_doc(tmp_path))

    assert report.universe == {'stock': 1, 'fund': 1, 'index': 1}
    assert report.universe_total == 3
    row = _row(report, 'quotes/{stock,fund,index}')
    assert row.kind == fc.KIND_UNIVERSE
    assert row.verdict == 'ok'
    assert '全集 3 只' in row.detail
    assert '合集' not in row.detail


def test_excluded_code_data_break_is_caught(tmp_path):
    """未被池收录的标的一旦行情断裂，数据层全集判据仍要抓到。"""
    calendar = _biz_days(12)
    store, calendar = _build_store(tmp_path, daily_end=calendar[-1],
                                   coverage_end=calendar[-1], event_end=calendar[-1])
    # 159001.SZ 不在候选池，但属于全市场 ETF；其末根停在 9 个交易日前
    _write_quotes_dir(store, 'fund', ['510300.SH', '159001.SZ'])
    _write_daily_index(store, [('600000.SH', 'stock', calendar[-1]),
                               ('510300.SH', 'fund', calendar[-1]),
                               ('159001.SZ', 'fund', calendar[-10]),
                               ('000001.SH', 'index', calendar[-1])])

    report = fc.run_check(store, calendar=calendar, doc_path=_write_doc(tmp_path))

    assert '159001.SZ' not in _row(report, 'meta/multi_asset/etf_pool.parquet').detail
    row = _row(report, 'quotes/{stock,fund,index}')
    assert row.verdict == 'warning'
    assert row.gap == 1
    assert '159001.SZ' in row.detail
    assert '未登记断裂 1 只' in row.detail


def test_registered_stale_is_not_a_break(tmp_path):
    """已登记停更台账（数据源无新增/退市）的断裂属已知合法状态，不计缺口。"""
    calendar = _biz_days(12)
    store, calendar = _build_store(tmp_path, daily_end=calendar[-1],
                                   coverage_end=calendar[-1], event_end=calendar[-1])
    _write_quotes_dir(store, 'fund', ['510300.SH', '159001.SZ'])
    _write_daily_index(store, [('600000.SH', 'stock', calendar[-1]),
                               ('510300.SH', 'fund', calendar[-1]),
                               ('159001.SZ', 'fund', calendar[-10]),
                               ('000001.SH', 'index', calendar[-1])])
    _write_stale_status(store, [('159001.SZ', 'fund', True)])

    report = fc.run_check(store, calendar=calendar, doc_path=_write_doc(tmp_path))

    row = _row(report, 'quotes/{stock,fund,index}')
    assert row.verdict == 'ok'
    assert row.gap == 0
    assert '已知停更 1 只' in row.detail


def test_pool_stale_coverage_reports_warning(tmp_path):
    calendar = _biz_days(12)
    store, calendar = _build_store(tmp_path, daily_end=calendar[-1],
                                   coverage_end=calendar[-5], event_end=calendar[-1])

    report = fc.run_check(store, calendar=calendar, doc_path=_write_doc(tmp_path))

    row = _row(report, 'meta/multi_asset/etf_pool.parquet')
    assert row.lag == 4
    assert row.verdict == 'warning'


def test_pool_member_without_quotes_reports_warning(tmp_path):
    """视图层只判池内标的是否有行情；未入池不算缺口。"""
    calendar = _biz_days(12)
    store, calendar = _build_store(tmp_path, daily_end=calendar[-1],
                                   coverage_end=calendar[-1], event_end=calendar[-1])
    pl.DataFrame({'ts_code': ['510300.SH', '159001.SZ'],
                  'coverage_end': [calendar[-1]] * 2}).write_parquet(
        store / 'meta' / 'multi_asset' / 'etf_pool.parquet')

    report = fc.run_check(store, calendar=calendar, doc_path=_write_doc(tmp_path))

    row = _row(report, 'meta/multi_asset/etf_pool.parquet')
    assert row.gap == 1
    assert row.verdict == 'warning'
    assert '159001.SZ' in row.detail


def test_pool_excluded_by_rule_is_not_a_gap(tmp_path):
    """按入池规则排除的标的只登记原因与评估日期，不计入数据缺口。"""
    calendar = _biz_days(12)
    store, calendar = _build_store(tmp_path, daily_end=calendar[-1],
                                   coverage_end=calendar[-1], event_end=calendar[-1])
    _write_fund_basic(store / 'meta' / 'fund_basic.parquet',
                      [('510300.SH', 'ETF'), ('159001.SZ', 'ETF')])
    _touch(store / 'meta' / 'fund_basic.parquet', calendar[-1])
    _write_quotes_dir(store, 'fund', ['510300.SH', '159001.SZ'])
    excluded = store / 'meta' / 'multi_asset' / 'etf_pool_excluded.json'
    excluded.write_text(json.dumps(
        {'rule': 'equity_watch_no_candidate', 'count': 1, 'codes': ['159001.SZ'],
         'evaluated_at': calendar[-1].isoformat(),
         'items': [{'ts_code': '159001.SZ', 'reason': 'equity_watch_no_candidate',
                    'evaluated_at': calendar[-1].isoformat()}]}), encoding='utf-8')

    report = fc.run_check(store, calendar=calendar, doc_path=_write_doc(tmp_path))

    row = _row(report, 'meta/multi_asset/etf_pool.parquet')
    assert row.gap == 0
    assert row.verdict == 'ok'
    assert '按入池规则排除 1 只' in row.detail
    assert '159001.SZ' not in row.detail.split('按入池规则排除')[0]


def test_st_interval_watermark_lag_reports_error(tmp_path):
    calendar = _biz_days(12)
    stale = calendar[-7]
    store, calendar = _build_store(tmp_path, daily_end=calendar[-1],
                                   coverage_end=calendar[-1], event_end=calendar[-1])
    (store / 'meta' / 'st' / 'coverage.json').write_text(
        f'{{"snapshot_date": "{stale.isoformat()}"}}', encoding='utf-8')

    report = fc.run_check(store, calendar=calendar, doc_path=_write_doc(tmp_path))

    row = _row(report, 'meta/st/st_intervals.parquet')
    assert row.lag == 6
    assert row.verdict == 'error'


def test_summary_is_single_line_when_normal(tmp_path):
    calendar = _biz_days(12)
    store, calendar = _build_store(tmp_path, daily_end=calendar[-1],
                                   coverage_end=calendar[-1], event_end=calendar[-1])
    report = fc.run_check(store, calendar=calendar, doc_path=_write_doc(tmp_path))

    summary = fc.format_summary(report)
    assert '\n' not in summary
    assert summary.startswith('[保鲜]')
    assert '全集 3 只' in summary
    assert '全部正常' in summary


def test_known_exception_is_listed_but_not_counted_as_warning(tmp_path):
    calendar = _biz_days(12)
    store, calendar = _build_store(tmp_path, daily_end=calendar[-1],
                                   coverage_end=calendar[-1], event_end=calendar[-1])
    _write_quotes_dir(store, 'fund', ['510300.SH', '505888.SH'])
    _write_daily_index(store, [('600000.SH', 'stock', calendar[-1]),
                               ('510300.SH', 'fund', calendar[-1]),
                               ('505888.SH', 'fund', calendar[-1]),
                               ('000001.SH', 'index', calendar[-1])])

    report = fc.run_check(store, calendar=calendar, doc_path=_write_doc(tmp_path))

    row = _row(report, 'meta/fund_basic.parquet')
    assert row.verdict == 'known_exception'
    assert '505888.SH' in row.detail
    assert '封闭式基金' in row.detail
    assert report.warnings == []
    assert [item.rel_path for item in report.known_exceptions] == ['meta/fund_basic.parquet']
    assert report.ok is True
    assert 'known_exception 1 条' in fc.format_report(report)


def test_unregistered_spec_raises(tmp_path):
    calendar = _biz_days(12)
    store, calendar = _build_store(tmp_path, daily_end=calendar[-1],
                                   coverage_end=calendar[-1], event_end=calendar[-1])
    doc = _write_doc(tmp_path, skip='meta/stock_basic.parquet')

    with pytest.raises(ValueError, match='未在权威清单登记') as excinfo:
        fc.run_check(store, calendar=calendar, doc_path=doc)

    assert 'meta/stock_basic.parquet' in str(excinfo.value)


@pytest.mark.parametrize(('lag', 'verdict'), [
    (0, 'ok'), (1, 'ok'), (2, 'warning'), (5, 'warning'), (6, 'error'),
])
def test_classify_lag_thresholds(lag, verdict):
    assert fc.classify_lag(lag) == verdict
