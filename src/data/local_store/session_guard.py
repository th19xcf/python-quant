#!/usr/bin/env python3

"""盘中守卫：交易时段内不把当日未收盘 K 线写进长期列存。

A 股连续竞价 9:30–11:30 / 13:00–15:00，集合竞价 9:15 开始。午休期间
当日 K 线仍只有半个交易日，同样不能落盘，因此守卫窗口取 9:15–15:00
连续区间；收盘后（15:00 起）才允许写入当日记录。

默认行为安全：构建/增量遇到「当前处于守卫窗口」时直接丢弃当日行；
需要人工强制写入时显式传 ``force=True``（CLI ``--force-intraday``）。
"""

from __future__ import annotations

from datetime import date, datetime
from datetime import time as dtime

import polars as pl

GUARD_START = dtime(9, 15)
GUARD_END = dtime(15, 0)


def should_skip_today(moment: datetime | None = None, *, force: bool = False) -> bool:
    """当前时刻是否处于交易时段（含集合竞价与午休，工作日）。"""
    if force:
        return False
    now = moment or datetime.now()
    if now.weekday() >= 5:
        return False
    return GUARD_START <= now.time() < GUARD_END


def guarded_source_date(source_last: date | None,
                        moment: datetime | None = None,
                        *, force: bool = False) -> date | None:
    """把源文件末行日期截到「已收盘的最后一个交易日」。

    盘中源 ``.day`` 会带当日实时 K 线；它不应参与「本地是否落后」的判定，
    否则每轮增量都会把当日尚未收盘的行反复重建。
    """
    if source_last is None or not should_skip_today(moment, force=force):
        return source_last
    now = moment or datetime.now()
    if source_last >= now.date():
        return _previous_day(now.date())
    return source_last


def _previous_day(day: date) -> date:
    from datetime import timedelta  # noqa: PLC0415

    return day - timedelta(days=1)


def filter_intraday_rows(frame: pl.DataFrame,
                         moment: datetime | None = None,
                         *, force: bool = False) -> pl.DataFrame:
    """守卫窗口内丢弃当日行；收盘后或显式 force 时原样返回。"""
    if frame.is_empty() or 'date' not in frame.columns:
        return frame
    if not should_skip_today(moment, force=force):
        return frame
    now = moment or datetime.now()
    return frame.filter(pl.col('date') < now.date())
