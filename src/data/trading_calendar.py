#!/usr/bin/env python3

r"""统一 A 股交易日历（本地基准指数为权威来源）。

背景
====
项目里此前至少有四套互不相干的「交易日」口径：

- ``quality/freshness.py``：周一至周五近似，手工在
  ``freshness_config.json`` 的 ``defaults.holidays`` 里排除法定节假日；
- ``quality/clean_set.py``：直接读 ``quotes/index/sh/000001.SH.parquet`` 的
  ``date`` 列；
- ``quality/tradable_universe.py``：读 ``000001.SH`` / ``000300.SH`` 并转成
  ``np.datetime64[D]`` 的 int64 天数；
- ``data/events.py``：要求调用方显式传入交易日历，否则直接抛错。

结果同一天在不同模块里可能是/不是交易日，节假日还要靠人工维护。本模块把
口径收口到一处：**以本地基准指数的实际交易日为权威**，只有在基准文件
缺失/不可读时才退化为「工作日 − 配置节假日」近似，并显式暴露退化状态。

约定
====
- ``count(start, end)`` 统计半开区间 ``(start, end]`` 的交易日数——与
  ``freshness._count_trading_days`` 的历史口径一致；
- ``between(start, end)`` 返回闭区间 ``[start, end]`` 的交易日；
- 权威日历末端之后的部分用工作日近似补齐（否则「今天比数据晚一天」会被
  算成 0 个交易日）。``approx_from`` / ``authoritative_end`` 明确标记哪一段
  是近似值，调用方可据此决定是否拒绝低置信度结论。

只读：不写 ``data/**``，缓存按基准文件 ``st_mtime_ns`` + 观察日 + 节假日集合
失效，不会把过期日历一直留在内存里。
"""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from loguru import logger
from polars.exceptions import PolarsError

from src.data.local_store.paths import default_store_root

#: 权威日历候选（按顺序取第一个有数据的基准指数）。
BENCHMARK_CODES: tuple[str, ...] = ('000001.SH', '000300.SH')
#: 近似日历的起点（覆盖沪深交易所开市）。
FALLBACK_START = date(1990, 1, 1)
#: 近似尾部向前延伸的天数（覆盖未来一年的巡检/回测窗口）。
FUTURE_HORIZON_DAYS = 366
#: 节假日配置沿用 freshness registry，避免维护两份节假日清单。
HOLIDAY_CONFIG = Path(__file__).parent / 'quality' / 'freshness_config.json'

_CACHE: dict[tuple[str, int, int, str], TradingCalendar] = {}


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        with contextlib.suppress(ValueError):
            return date.fromisoformat(value[:10])
    return None


def _weekday_dates(start: date, end: date,
                   holidays: frozenset[date]) -> tuple[date, ...]:
    """闭区间 [start, end] 内的工作日，剔除 holidays。"""
    if end < start:
        return ()
    out: list[date] = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() < 5 and cursor not in holidays:
            out.append(cursor)
        cursor += timedelta(days=1)
    return tuple(out)


def _index_path(root: Path, ts_code: str) -> Path:
    market = ts_code.rsplit('.', 1)[-1].lower() if '.' in ts_code else 'sh'
    return root / 'quotes' / 'index' / market / f'{ts_code}.parquet'


def _read_index_dates(root: Path, ts_code: str) -> tuple[date, ...]:
    """读取基准指数实际交易日；文件缺失/损坏返回空元组。"""
    path = _index_path(root, ts_code)
    if not path.is_file():
        return ()
    try:
        frame = pl.read_parquet(path, columns=['date'])
        values = [_as_date(v) for v in frame['date'].to_list()]
        parsed = sorted({v for v in values if v is not None})
    except (OSError, ValueError, TypeError, PolarsError) as exc:
        logger.warning(f'[calendar] 基准指数日历读取失败 {path}: {exc}')
        return ()
    return tuple(parsed)


def configured_holidays(path: Path | None = None) -> frozenset[date]:
    """从 freshness registry 读取 ``defaults.holidays``（单一节假日来源）。"""
    target = path or HOLIDAY_CONFIG
    try:
        raw = json.loads(target.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        logger.debug(f'[calendar] 节假日配置不可读，按空集合处理: {exc}')
        return frozenset()
    items = raw.get('defaults', {}).get('holidays', []) or []
    parsed = {_as_date(item) for item in items}
    return frozenset(d for d in parsed if d is not None)


@dataclass(frozen=True)
class TradingCalendar:
    """升序交易日序列 + 来源与置信度标记。"""

    dates: tuple[date, ...]
    source: str
    authoritative_end: date | None
    approx_from: date | None
    holidays: frozenset[date] = frozenset()

    @property
    def is_authoritative(self) -> bool:
        """是否至少有一段来自本地基准指数的真实交易日。"""
        return self.authoritative_end is not None

    def authoritative_dates(self) -> tuple[date, ...]:
        """只返回权威段（不含近似尾部）。全退化时返回空元组。"""
        if self.authoritative_end is None:
            return ()
        return tuple(d for d in self.dates if d <= self.authoritative_end)

    def between(self, start: date, end: date) -> tuple[date, ...]:
        """闭区间 [start, end] 内的交易日。"""
        if end < start:
            return ()
        return self.dates[bisect_left(self.dates, start):bisect_right(self.dates, end)]

    def count(self, start: date, end: date) -> int:
        """半开区间 (start, end] 内的交易日数。"""
        if end <= start:
            return 0
        return bisect_right(self.dates, end) - bisect_right(self.dates, start)

    def is_trading_day(self, day: date) -> bool:
        index = bisect_left(self.dates, day)
        return index < len(self.dates) and self.dates[index] == day

    def previous(self, day: date, *, inclusive: bool = False) -> date | None:
        """不晚于 day 的最近交易日（inclusive=True 时允许返回 day 本身）。"""
        index = bisect_right(self.dates, day) if inclusive else bisect_left(self.dates, day)
        return self.dates[index - 1] if index > 0 else None

    def next(self, day: date, *, inclusive: bool = False) -> date | None:
        """不早于 day 的最近交易日（inclusive=True 时允许返回 day 本身）。"""
        index = bisect_left(self.dates, day) if inclusive else bisect_right(self.dates, day)
        return self.dates[index] if index < len(self.dates) else None


def _build(root: Path, as_of: date,
           holidays: frozenset[date]) -> TradingCalendar:
    horizon = as_of + timedelta(days=FUTURE_HORIZON_DAYS)
    index_dates: tuple[date, ...] = ()
    source_code = ''
    for code in BENCHMARK_CODES:
        index_dates = _read_index_dates(root, code)
        if index_dates:
            source_code = code
            break

    if not index_dates:
        logger.warning(
            '[calendar] 本地基准指数日历不可用，退化为「工作日 − 配置节假日」近似；'
            '节假日漏配会直接导致新鲜度/缺口统计偏差'
        )
        return TradingCalendar(
            dates=_weekday_dates(FALLBACK_START, horizon, holidays),
            source='weekday_fallback',
            authoritative_end=None,
            approx_from=FALLBACK_START,
            holidays=holidays,
        )

    tail_start = index_dates[-1] + timedelta(days=1)
    tail = _weekday_dates(tail_start, horizon, holidays)
    if tail:
        logger.debug(
            f'[calendar] 权威日历止于 {index_dates[-1].isoformat()}，'
            f'其后 {len(tail)} 个工作日为近似值（source={source_code}）'
        )
    return TradingCalendar(
        dates=index_dates + tail,
        source=f'local_index:{source_code}',
        authoritative_end=index_dates[-1],
        approx_from=tail_start if tail else None,
        holidays=holidays,
    )


def get_calendar(root: Path | str | None = None, *, as_of: date | None = None,
                 holidays: Iterable[date] | None = None,
                 refresh: bool = False) -> TradingCalendar:
    """取得交易日历（进程内按文件 mtime / 观察日 / 节假日缓存）。"""
    root_path = Path(root) if root is not None else default_store_root()
    today = as_of or date.today()
    hs = (frozenset(holidays) if holidays is not None
          else configured_holidays())
    stamps: list[int] = []
    for code in BENCHMARK_CODES:
        path = _index_path(root_path, code)
        stamps.append(path.stat().st_mtime_ns if path.is_file() else -1)
    key = (str(root_path), max(stamps), today.toordinal(),
           ','.join(sorted(d.isoformat() for d in hs)))
    if not refresh and key in _CACHE:
        return _CACHE[key]
    calendar = _build(root_path, today, hs)
    _CACHE[key] = calendar
    return calendar


def reset_calendar_cache() -> None:
    """清空缓存（测试或基准文件被替换后调用）。"""
    _CACHE.clear()


def count_trading_days(start: date, end: date,
                       root: Path | str | None = None, **kwargs: Any) -> int:
    """``(start, end]`` 的交易日数。"""
    return get_calendar(root, **kwargs).count(start, end)


def trading_dates(start: date, end: date,
                  root: Path | str | None = None, **kwargs: Any) -> tuple[date, ...]:
    """``[start, end]`` 的交易日序列。"""
    return get_calendar(root, **kwargs).between(start, end)


def is_trading_day(day: date, root: Path | str | None = None,
                   **kwargs: Any) -> bool:
    return get_calendar(root, **kwargs).is_trading_day(day)


def previous_trading_day(day: date, root: Path | str | None = None, *,
                         inclusive: bool = False, **kwargs: Any) -> date | None:
    return get_calendar(root, **kwargs).previous(day, inclusive=inclusive)


def next_trading_day(day: date, root: Path | str | None = None, *,
                     inclusive: bool = False, **kwargs: Any) -> date | None:
    return get_calendar(root, **kwargs).next(day, inclusive=inclusive)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='统一交易日历只读查询')
    parser.add_argument('--root', default=None, help='列存根目录（缺省用配置默认值）')
    parser.add_argument('--start', default='', help='起始日期 YYYY-MM-DD')
    parser.add_argument('--end', default='', help='结束日期 YYYY-MM-DD')
    parser.add_argument('--json', action='store_true', help='以 JSON 输出')
    args = parser.parse_args(argv)

    cal = get_calendar(args.root)
    start = _as_date(args.start) or cal.dates[0]
    end = _as_date(args.end) or date.today()
    window = cal.between(start, end)
    payload = {
        'source': cal.source,
        'authoritative_end': (cal.authoritative_end.isoformat()
                              if cal.authoritative_end else None),
        'approx_from': cal.approx_from.isoformat() if cal.approx_from else None,
        'dates_total': len(cal.dates),
        'window_start': start.isoformat(),
        'window_end': end.isoformat(),
        'window_trading_days': len(window),
        'first': window[0].isoformat() if window else None,
        'last': window[-1].isoformat() if window else None,
    }
    if args.json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + '\n')
    else:
        for key, value in payload.items():
            logger.info(f'[calendar] {key}: {value}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
