#!/usr/bin/env python3

"""落盘守卫：不把「未收盘当日行」与「非交易日行」写进长期列存。

盘中守卫
--------
A 股连续竞价 9:30–11:30 / 13:00–15:00，集合竞价 9:15 开始。午休期间
当日 K 线仍只有半个交易日，同样不能落盘，因此守卫窗口取 9:15–15:00
连续区间；收盘后（15:00 起）才允许写入当日记录。

默认行为安全：构建/增量遇到「当前处于守卫窗口」时直接丢弃当日行；
需要人工强制写入时显式传 ``force=True``（CLI ``--force-intraday``）。

交易日历守卫
------------
源端（通达信 ``.day``）对无成交标的会写 ``close=1.0`` 的占位行，日期口径
是自然日而非交易日，实测含周六/周日与法定假日（2026-11-13 核查：58 只
基金 / 900 行 / 18 个非交易日，见 G-129）。``filter_nontrading_rows``
只挡「日期不在交易日历内」的**源端新帧**行，不 prune 存量、不改
``_merge_source_with_existing`` 的 upsert 语义；日历不可用时原样放行，
避免把整帧静默清空。
"""

from __future__ import annotations

from datetime import date, datetime
from datetime import time as dtime
from pathlib import Path

import polars as pl
from loguru import logger
from polars.exceptions import PolarsError

from src.data.trading_calendar import get_calendar

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


def store_calendar_dates(root: Path | str) -> frozenset[date] | None:
    """取 ``root`` 对应列存的交易日历日期集合；不可用时返回 None。

    ``get_calendar`` 以本地基准指数（``000001.SH`` / ``000300.SH``）为权威
    来源，取不到时才退化为「工作日 − 配置节假日」。两种情况下拿到的都是
    可用集合；真正拿不到（空集合 / 抛错）才返回 None，由调用方决定放行。
    """
    try:
        calendar = get_calendar(root)
    # 兜底理由：日历读取失败不应让整轮构建崩掉，放行比静默清空安全
    except (OSError, ValueError, RuntimeError, PolarsError) as exc:
        logger.warning(f'[calendar] 交易日历不可用，本轮跳过非交易日过滤: {exc}')
        return None
    dates = frozenset(calendar.dates)
    if not dates:
        logger.warning('[calendar] 交易日历为空，本轮跳过非交易日过滤')
        return None
    if calendar.approx_from is not None:
        logger.info(
            f'[calendar] source={calendar.source}，权威段止于 '
            f'{calendar.authoritative_end}，其后为工作日近似')
    return dates


def filter_nontrading_rows(
    frame: pl.DataFrame,
    calendar_dates: frozenset[date] | None,
) -> tuple[pl.DataFrame, tuple[date, ...]]:
    """丢弃日期不在交易日历内的行，返回（过滤后帧, 被挡日期升序元组）。

    只作用于**源端新帧**：存量行由 ``build._merge_source_with_existing`` 的
    upsert 原样保留，本函数不 prune、不删行、不改任何行内取值。

    日历缺失（``None`` / 空集合）时原样返回，且被挡日期为空元组 —— 宁可
    放过也不能把整帧静默清空。
    """
    if frame.is_empty() or 'date' not in frame.columns or not calendar_dates:
        return frame, ()
    allowed = list(calendar_dates)
    keep = pl.col('date').is_in(allowed)
    blocked = frame.filter(~keep)['date'].unique().sort().to_list()
    if not blocked:
        return frame, ()
    return frame.filter(keep), tuple(blocked)
