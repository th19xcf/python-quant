"""财务分析取数层单测：正常 / 缺字段 / 网络异常 / 空数据 / 缓存 / 本地资讯。"""

from __future__ import annotations

from datetime import date

import pandas as pd
import polars as pl
import pytest

from src.data.services import finance_service as fs


@pytest.fixture(autouse=True)
def _clear_cache():
    fs.clear_finance_cache()
    yield
    fs.clear_finance_cache()


def _em_frame(rows: int = 2) -> pd.DataFrame:
    data = [
        {
            'REPORT_DATE': '2026-09-30',
            'REPORT_DATE_NAME': '2026三季报',
            'NOTICE_DATE': '2026-10-30',
            'TOTALOPERATEREVE': 130_971_000_000.0,
            'PARENTNETPROFIT': 64_467_000_000.0,
            'TOTALOPERATEREVETZ': 1.96,
            'PARENTNETPROFITTZ': -0.25,
            'ROEJQ': 18.5,
            'XSJLL': 49.2,
            'XSMLL': 91.3,
            'EPSJB': 51.2,
            'ZCFZL': 21.5,
            'LD': 3.1,
            'SD': 2.4,
            'CHZZL': 0.35,
        },
        {
            'REPORT_DATE': '2026-06-30',
            'REPORT_DATE_NAME': '2026中报',
            'NOTICE_DATE': '2026-08-20',
            'TOTALOPERATEREVE': 88_000_000_000.0,
            'PARENTNETPROFIT': 42_000_000_000.0,
            'TOTALOPERATEREVETZ': 2.1,
            'PARENTNETPROFITTZ': 0.8,
            'ROEJQ': 12.1,
            'XSJLL': 48.0,
            'XSMLL': 91.0,
            'EPSJB': 33.0,
            'ZCFZL': 20.8,
            'LD': 3.2,
            'SD': 2.5,
            'CHZZL': 0.22,
        },
    ][:rows]
    return pd.DataFrame(data)


def _valuation_frame() -> pd.DataFrame:
    return pd.DataFrame([{
        '数据日期': '2026-11-03',
        'PE(TTM)': 22.4,
        'PE(静)': 23.1,
        '市净率': 8.2,
        '总市值': 1_860_000_000_000.0,
        '流通市值': 1_860_000_000_000.0,
    }])


def _news_frame() -> pd.DataFrame:
    return pd.DataFrame([{
        '发布时间': '2026-10-30 18:36:28',
        '新闻标题': '贵州茅台：第三季度净利润同比增长',
        '新闻内容': '公司披露三季报，净利润保持增长。',
        '文章来源': '每日经济新闻',
        '新闻链接': 'http://example.com/news/1',
        '关键词': '600519',
    }])


def _fetchers(**overrides):
    base = {
        'financial_em': lambda code: _em_frame(),
        'financial_ths': lambda code: pd.DataFrame(),
        'valuation': lambda code: _valuation_frame(),
        'news': lambda code: _news_frame(),
    }
    base.update(overrides)
    return base


def test_normal_snapshot_contains_report_metrics_valuation_and_news(tmp_path):
    snapshot = fs.fetch_finance_snapshot(
        '600519.SH', '贵州茅台', store_root=tmp_path,
        fetchers=_fetchers(), use_cache=False)

    assert snapshot['ok'] is True
    assert snapshot['code'] == '600519.SH'
    assert snapshot['report']['period'] == '2026-09-30'
    assert snapshot['report']['notice_date'] == '2026-10-30'
    assert snapshot['report']['source'] == fs.SOURCE_EM
    assert snapshot['metrics']['roe']['display'] == '18.50%'
    assert snapshot['metrics']['debt_ratio']['display'] == '21.50%'
    assert snapshot['valuation']['metrics']['pe_ttm']['display'] == '22.40'
    assert snapshot['valuation']['metrics']['total_mv']['display'] == '18600.00亿'
    assert snapshot['history']['periods'] == ['2026-06-30', '2026-09-30']
    assert len(snapshot['news']) == 1
    assert snapshot['news'][0]['type'] == '新闻'
    assert snapshot['news'][0]['url'].startswith('http://')


def test_missing_fields_degrades_without_exception(tmp_path):
    frame = pd.DataFrame([{
        'REPORT_DATE': '2026-09-30',
        'TOTALOPERATEREVE': 1.0,
    }])
    snapshot = fs.fetch_finance_snapshot(
        '600519.SH', '贵州茅台', store_root=tmp_path,
        fetchers=_fetchers(financial_em=lambda code: frame,
                           financial_ths=lambda code: pd.DataFrame(),
                           valuation=lambda code: pd.DataFrame(),
                           news=lambda code: pd.DataFrame()),
        use_cache=False)

    assert isinstance(snapshot, dict)
    assert snapshot['metrics']['roe']['display'] == '—'
    assert snapshot['metrics']['roe']['value'] is None
    assert any('缺少' in warning for warning in snapshot['warnings'])
    assert snapshot['errors'] == []


def test_network_exception_is_collected_and_degraded(tmp_path):
    def boom(code):
        raise ConnectionError('network down')

    snapshot = fs.fetch_finance_snapshot(
        '600519.SH', '贵州茅台', store_root=tmp_path,
        fetchers={key: boom for key in
                  ('financial_em', 'financial_ths', 'valuation', 'news')},
        use_cache=False)

    assert isinstance(snapshot, dict)
    assert snapshot['ok'] is False
    assert snapshot['report'] is None
    assert snapshot['metrics'] == {}
    assert snapshot['news'] == []
    stages = {error['stage'] for error in snapshot['errors']}
    assert {'financial.financial_em', 'financial.financial_ths',
            'valuation', 'news'} <= stages
    assert all('network down' in error['message'] for error in snapshot['errors'])


def test_empty_frames_produce_readable_empty_state(tmp_path):
    empty = pd.DataFrame()
    snapshot = fs.fetch_finance_snapshot(
        '600519.SH', '贵州茅台', store_root=tmp_path,
        fetchers={key: (lambda code: empty) for key in
                  ('financial_em', 'financial_ths', 'valuation', 'news')},
        use_cache=False)

    assert snapshot['ok'] is False
    assert snapshot['report'] is None
    assert snapshot['metrics'] == {}
    assert snapshot['news'] == []
    assert any('空表' in warning for warning in snapshot['warnings'])
    assert any('无可展示数据' in warning for warning in snapshot['warnings'])


def test_cache_keyed_by_code_and_date_avoids_repeat_requests(tmp_path):
    calls = {'count': 0}

    def counted(code):
        calls['count'] += 1
        return _em_frame()

    fetchers = _fetchers(financial_em=counted)
    first = fs.fetch_finance_snapshot('600519.SH', '贵州茅台',
                                      store_root=tmp_path, fetchers=fetchers)
    second = fs.fetch_finance_snapshot('600519.SH', '贵州茅台',
                                       store_root=tmp_path, fetchers=fetchers)
    assert calls['count'] == 1
    assert first is second

    third = fs.fetch_finance_snapshot('000001.SZ', '平安银行',
                                      store_root=tmp_path, fetchers=fetchers)
    assert calls['count'] == 2
    assert third['code'] == '000001.SZ'


def test_local_events_dividends_and_st_feed_news(tmp_path):
    events_path = tmp_path / 'meta' / 'events' / 'events.parquet'
    events_path.parent.mkdir(parents=True)
    pl.DataFrame([{
        'ts_code': '600519.SH',
        'event_type': '业绩预减',
        'publish_time': '2026-10-25 09:00:00',
        'title': '2026 年三季报业绩预减公告',
        'source': 'cninfo',
    }]).write_parquet(events_path)

    dividend_path = tmp_path / 'factors' / 'stock_dividend.parquet'
    dividend_path.parent.mkdir(parents=True)
    pl.DataFrame([{
        'ts_code': '600519.SH',
        'symbol': '600519',
        'name': '贵州茅台',
        'dividend_year': '2026',
        'report_date': date(2026, 3, 21),
        'record_date': None,
        'ex_date': None,
        'pay_date': None,
        'cash_div': 3.2,
        'share_div': 0.0,
        'total_div': 0.0,
        'rights_issue_price': None,
        'rights_issue_ratio': None,
    }]).write_parquet(dividend_path)

    snapshot = fs.fetch_finance_snapshot(
        '600519.SH', '贵州茅台', store_root=tmp_path, as_of=date(2026, 11, 3),
        fetchers={key: (lambda code: pd.DataFrame()) for key in
                  ('financial_em', 'financial_ths', 'valuation', 'news')},
        use_cache=False)

    types = {item['type'] for item in snapshot['news']}
    assert '业绩预减' in types or '公告' in types
    assert '分红送转' in types
    assert all({'time', 'type', 'title', 'source'} <= set(item) for item in snapshot['news'])


def test_index_code_returns_not_applicable_without_remote_calls():
    calls = []

    def spy(code):
        calls.append(code)
        return pd.DataFrame()

    snapshot = fs.fetch_finance_snapshot(
        '000001.SH', '上证指数', fetchers={key: spy for key in
                                         ('financial_em', 'financial_ths', 'valuation', 'news')},
        use_cache=False)

    assert snapshot['not_applicable'] is True
    assert snapshot['news'] == []
    assert calls == []
