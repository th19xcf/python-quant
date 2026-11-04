"""分红事件表点时正确性单元测试。

覆盖 ``tools/collect_stock_dividend.py`` 的 ``select_as_of``：
公告日（预案公告日，缺失退回最新公告日）之前不可见，公告日当天及以后可见；
``implemented_only`` 额外要求除权除息日已到。
"""

import sys
from datetime import date
from pathlib import Path

import polars as pl
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / 'tools'))

from collect_stock_dividend import DEFAULT_OUT, select_as_of  # noqa: E402


@pytest.fixture
def frame():
    return pl.DataFrame({
        'ts_code': ['600000.SH', '600000.SH', '600001.SH'],
        'ex_date': [date(2024, 5, 21), None, None],
        'plan_notice_date': [date(2024, 3, 15), date(2024, 6, 1), None],
        'notice_date': [date(2024, 4, 10), None, date(2024, 7, 5)],
    })


def test_not_visible_before_announcement(frame):
    out = select_as_of(frame, date(2024, 3, 14))
    assert out.height == 0


def test_visible_on_announcement_date(frame):
    out = select_as_of(frame, date(2024, 3, 15))
    assert out['ts_code'].to_list() == ['600000.SH']
    assert out['ex_date'].to_list() == [date(2024, 5, 21)]


def test_plan_only_visible_after_its_notice(frame):
    out = select_as_of(frame, date(2024, 5, 31))
    assert out.height == 1
    out = select_as_of(frame, date(2024, 6, 1))
    assert out.height == 2


def test_fallback_to_notice_date(frame):
    out = select_as_of(frame, date(2024, 7, 4))
    assert '600001.SH' not in out['ts_code'].to_list()
    out = select_as_of(frame, date(2024, 7, 5))
    assert '600001.SH' in out['ts_code'].to_list()


def test_unknown_announcement_never_visible(frame):
    frame = frame.with_columns(pl.lit(None, dtype=pl.Date).alias('notice_date'))
    out = select_as_of(frame, date(2030, 1, 1))
    assert '600001.SH' not in out['ts_code'].to_list()


def test_implemented_only_requires_ex_date(frame):
    # 公告日已到但除权日未到：点时查询不得把未实施事件当成已实施
    out = select_as_of(frame, date(2024, 5, 20), implemented_only=True)
    assert out.height == 0
    out = select_as_of(frame, date(2024, 5, 21), implemented_only=True)
    assert out.height == 1
    # 预案未实施的行永远不进入 implemented_only
    out = select_as_of(frame, date(2024, 6, 1), implemented_only=True)
    assert out.height == 1


@pytest.mark.skipif(not DEFAULT_OUT.exists(), reason='本地分红表尚未生成')
def test_real_table_920000_point_in_time():
    from datetime import timedelta

    event = (pl.col('ts_code') == '920000.BJ',
             pl.col('ex_date') == date(2026, 5, 25))

    def _count(as_of, **kwargs):
        return select_as_of(pl.read_parquet(DEFAULT_OUT), as_of, **kwargs).filter(
            *event).height

    real = pl.read_parquet(DEFAULT_OUT).filter(*event)
    assert real.height == 1
    notice = real['plan_notice_date'][0]
    assert notice is not None
    assert _count(notice - timedelta(days=1)) == 0
    assert _count(notice) == 1
    assert _count(date(2026, 5, 24), implemented_only=True) == 0
    assert _count(date(2026, 5, 25), implemented_only=True) == 1
