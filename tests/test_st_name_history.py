"""补充 ST 名称历史模块的离线单测（不联网）。"""

from datetime import date, datetime, timedelta

import polars as pl

from src.data import st_name_history as N


def test_has_st_token_variants():
    assert N.has_st_token('ST嘉丰')
    assert N.has_st_token('*ST长油')
    assert N.has_st_token('S*ST国农')
    assert N.has_st_token('G*ST国农')
    assert N.has_st_token('GST星源')
    assert N.has_st_token('PT网点')
    assert N.has_st_token('嘉丰股份→ST嘉丰→金丰投资')
    assert N.has_st_token('国农科技→*ST国农→G*ST国农→ST国农')
    assert not N.has_st_token('浦发银行')
    assert not N.has_st_token('G浦发')
    assert not N.has_st_token('深发展A→S深发展A→深发展A')
    assert not N.has_st_token('')


def test_classify_relaxed_accepts_tip_but_rejects_application():
    impl = N.classify_event_title('关于公司股票交易实施其他风险警示暨公司股票停牌的提示性公告')
    assert impl == [('st', 'impl')]
    lift = N.classify_event_title('关于公司股票停牌暨撤销其他风险警示的提示性公告')
    assert lift == [('st', 'lift')]
    assert N.classify_event_title('关于申请公司股票撤销其他风险警示公告') == []
    assert N.classify_event_title('关于股票可能被实施退市风险警示的风险提示公告') == []


def _em(rows):
    return pl.DataFrame(rows, schema={
        'secucode': pl.Utf8, 'name': pl.Utf8, 'former_names': pl.Utf8,
        'org_name': pl.Utf8, 'listing_date': pl.Date})


def _coverage(rows):
    return pl.DataFrame(rows, schema={
        'ts_code': pl.Utf8, 'coverage_start': pl.Date, 'coverage_end': pl.Date,
        'source': pl.Utf8, 'fetched_at': pl.Datetime})


def test_plan_never_defaults_non_st():
    """无名称记录 / 名称含 ST 但无定日事件 → 不写覆盖，保持 unknown。"""
    em = _em([
        {'secucode': '600000.SH', 'name': '浦发银行', 'former_names': '',
         'org_name': '', 'listing_date': date(1999, 11, 10)},
        {'secucode': '600658.SH', 'name': '电子城', 'former_names': '北京天龙→ST京天龙',
         'org_name': '', 'listing_date': date(1993, 5, 24)},
    ])
    coverage = _coverage([])
    windows = {'600000.SH': (date(1999, 11, 10), date(2026, 11, 3)),
               '600658.SH': (date(1993, 5, 24), date(2026, 11, 3))}
    plan = N.plan_supplement(em, coverage, ['600000.SH', '600658.SH', '600001.SH'],
                             windows, {}, today=date(2026, 11, 5))
    assert [row['ts_code'] for row in plan.coverage] == ['600000.SH']
    assert plan.coverage[0]['source'] == N.SOURCE_EM_CLEAN
    reasons = {row['ts_code']: row['reason'] for row in plan.unresolved}
    assert reasons['600658.SH'] == 'st_name_without_dated_event'
    assert reasons['600001.SH'] == 'no_quote_window'


def test_plan_builds_dated_interval_and_point_in_time_window():
    """有定日公告 → 区间从生效日开始；生效日前不在覆盖窗口内。"""
    em = _em([
        {'secucode': '603810.SH', 'name': '丰山集团', 'former_names': '丰山集团→ST丰山',
         'org_name': '', 'listing_date': date(2018, 9, 17)},
    ])
    coverage = _coverage([])
    notices = {'603810.SH': [
        {'notice_id': '1', 'code': '603810.SH',
         'title': '关于公司股票交易实施其他风险警示暨停牌的提示性公告',
         'publish_date': date(2019, 7, 15)},
        {'notice_id': '2', 'code': '603810.SH',
         'title': '关于公司股票停牌暨撤销其他风险警示的提示性公告',
         'publish_date': date(2019, 11, 13)},
    ]}
    plan = N.plan_supplement(
        em, coverage, ['603810.SH'],
        {'603810.SH': (date(2018, 9, 17), date(2026, 11, 3))}, notices,
        today=date(2026, 11, 5))
    assert len(plan.intervals) == 1
    row = plan.intervals[0]
    assert row['start_date'] == date(2019, 7, 16)
    assert row['end_date'] == date(2019, 11, 13)
    assert plan.coverage[0]['coverage_start'] == date(2019, 7, 16)


def test_plan_skips_snapshot_st_codes_with_clean_names():
    """当前快照已判 ST 的标的，不允许用干净名称历史做 non_st 负证据。"""
    em = _em([
        {'secucode': '920575.BJ', 'name': '*ST康乐', 'former_names': '康乐卫士',
         'org_name': '', 'listing_date': date(2023, 3, 15)},
    ])
    plan = N.plan_supplement(
        em, _coverage([]), ['920575.BJ'],
        {'920575.BJ': (date(2023, 3, 15), date(2026, 11, 3))}, {},
        today=date(2026, 11, 5), snapshot_st={'920575.BJ'})
    assert plan.coverage == []
    assert plan.unresolved[0]['reason'] == 'st_name_without_dated_event'


def test_official_coverage_drops_previous_supplement_rows():
    """重建基准不能把上次写入的 supplement 行当成官方覆盖。"""
    coverage = _coverage([
        {'ts_code': '600000.SH', 'coverage_start': date(1999, 11, 10),
         'coverage_end': date(2026, 11, 6), 'source': 'merged', 'fetched_at': None},
        {'ts_code': '600001.SH', 'coverage_start': date(1999, 11, 10),
         'coverage_end': date(2026, 11, 6), 'source': N.SOURCE_EM_CLEAN,
         'fetched_at': None},
        {'ts_code': '600002.SH', 'coverage_start': date(1999, 11, 10),
         'coverage_end': date(2026, 11, 6), 'source': N.SOURCE_CNINFO_DATED,
         'fetched_at': None},
    ])
    kept = N.official_coverage(coverage)
    assert kept['ts_code'].to_list() == ['600000.SH']
    assert N.official_coverage(_coverage([])).height == 0


def test_supplement_rebuild_is_idempotent():
    """规范表已含上次 supplement 行时，重算仍覆盖同一批标的。"""
    em = _em([
        {'secucode': '600000.SH', 'name': '浦发银行', 'former_names': '',
         'org_name': '', 'listing_date': date(1999, 11, 10)},
    ])
    merged = _coverage([
        {'ts_code': '600000.SH', 'coverage_start': date(1999, 11, 10),
         'coverage_end': date(2026, 11, 6), 'source': N.SOURCE_EM_CLEAN,
         'fetched_at': None},
        {'ts_code': '600004.SH', 'coverage_start': date(2003, 4, 28),
         'coverage_end': date(2026, 11, 6), 'source': 'merged', 'fetched_at': None},
    ])
    windows = {'600000.SH': (date(1999, 11, 10), date(2026, 11, 6))}
    plan = N.plan_supplement(em, N.official_coverage(merged), ['600000.SH'],
                             windows, {}, today=date(2026, 11, 8))
    assert [row['ts_code'] for row in plan.coverage] == ['600000.SH']


def test_intervals_clamp_open_interval_to_last_tradable_day():
    segments = [(date(2025, 4, 30), '*st')]
    rows = N._intervals_from_segments(
        '920680.BJ', segments, date(2025, 12, 24),
        fetched_at=datetime(2025, 12, 24), quote_last=date(2026, 11, 11))
    assert rows[0]['end_date'] == date(2025, 12, 24)
    assert rows[0]['fetched_at'] == datetime(2025, 12, 24)


def test_event_date_prefers_title_deadline():
    assert N.st_history._event_date('自2019年7月16日起实施其他风险警示',
                                    date(2019, 7, 15)) == date(2019, 7, 16)
    assert N.st_history._event_date('实施其他风险警示', date(2019, 7, 15)) == \
        date(2019, 7, 15) + timedelta(days=1)
