#!/usr/bin/env python3

"""公告事件本地表：离线导入、增量追加与冷却窗口查询（运行时不联网）。

本模块把 ``research/events/`` 里的公告解析逻辑提升为生产可用的本地表：

- 落盘：``{store_root}/meta/events/events.parquet``（单表，按事件去重）；
- 覆盖声明：``{store_root}/meta/events/coverage.json``（导入区间与完整性）；
- 导入：支持证券公告原始表（``keyword/stock_code/display_time``）与已规范化表
  （``ts_code/event_type/publish_time``），CSV / Parquet 均可；
- 运行时只读本地 parquet，**禁止联网**（项目硬约束）。

事件冷却口径：公告日 T，冷却区间 ``[T+1, T+N]``，N 为该事件类型的交易日数。
覆盖区间之外一律返回 ``unknown``——**不得当成「没有事件」**。
"""

from __future__ import annotations

import argparse
import json
import re
from bisect import bisect_left, bisect_right
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Sequence

import polars as pl
from loguru import logger

RISK_EVENT_COOLDOWNS: dict[str, int] = {
    '并购重组': 20,
    '重大合同/中标': 5,
    '监管处罚': 5,
}

KEYWORD_TO_TYPE = {
    '减持': '减持',
    '增持': '增持',
    '股权激励': '股权激励',
    '中标': '重大合同/中标',
    '重大合同': '重大合同/中标',
    '处罚': '监管处罚',
    '立案': '监管处罚',
    '重组': '并购重组',
    '并购': '并购重组',
    '分红': '分红送转',
}

TITLE_FILTERS: dict[str, tuple[str | None, str | None]] = {
    '减持': (r'减持', r'增持'),
    '增持': (r'增持', r'减持'),
    '股权激励': (r'股权激励|股票期权|限制性股票', None),
    '重大合同/中标': (r'中标|重大合同|框架协议|战略合作', None),
    '监管处罚': (r'处罚|监管措施|立案|警示函|责令改正|问询', None),
    '并购重组': (r'重组|并购|收购|吸收合并', None),
    '分红送转': (r'分红|利润分配|权益分派|派息', None),
}

STATUS_COOLING = 'cooling'
STATUS_CLEAR = 'clear'
STATUS_UNKNOWN = 'unknown'

EVENT_COLUMNS = ('event_id', 'ts_code', 'event_type', 'publish_time',
                 'title', 'source')
DEDUP_DAYS = 30
_EMPTY_SCHEMA = {
    'event_id': pl.String,
    'ts_code': pl.String,
    'event_type': pl.String,
    'publish_time': pl.Datetime,
    'title': pl.String,
    'source': pl.String,
}


def events_path(store_root: Path | str) -> Path:
    return Path(store_root) / 'meta' / 'events' / 'events.parquet'


def coverage_path(store_root: Path | str) -> Path:
    return Path(store_root) / 'meta' / 'events' / 'coverage.json'


def _as_date(value: Any) -> date | None:
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    try:
        return datetime.fromisoformat(text.replace('Z', '')).date()
    except ValueError:
        return None


def _to_ts_code(stock_code: Any) -> str | None:
    from src.utils.code_converter import parse_code  # noqa: PLC0415

    try:
        symbol, market = parse_code(str(stock_code).zfill(6))
    except (ValueError, TypeError):
        return None
    return f'{symbol}.{market}'


def empty_events() -> pl.DataFrame:
    return pl.DataFrame(schema=_EMPTY_SCHEMA)


def normalize_announcements(
    frame: pl.DataFrame, start: Any = None, end: Any = None,
) -> pl.DataFrame:
    """把原始公告表规范化为事件表（同一标的同类型 30 自然日内只留最早一条）。"""
    if frame.is_empty():
        return empty_events()
    need = {'keyword', 'stock_code', 'display_time'}
    missing = need - set(frame.columns)
    if missing:
        raise ValueError(f'原始公告表缺少列: {sorted(missing)}')
    start_day = _as_date(start)
    end_day = _as_date(end)

    rows: list[dict[str, Any]] = []
    for item in frame.to_dicts():
        event_type = KEYWORD_TO_TYPE.get(str(item.get('keyword') or ''))
        if event_type is None:
            continue
        title = str(item.get('title') or '')
        include, exclude = TITLE_FILTERS.get(event_type, (None, None))
        if include and not re.search(include, title):
            continue
        if exclude and re.search(exclude, title):
            continue
        published = item.get('display_time')
        if isinstance(published, str):
            published = _parse_time(published)
        if not isinstance(published, datetime):
            continue
        if start_day is not None and published.date() < start_day:
            continue
        if end_day is not None and published.date() > end_day:
            continue
        ts_code = _to_ts_code(item.get('stock_code'))
        if ts_code is None:
            continue
        rows.append({
            'event_id': f"{item.get('art_code') or ''}-{item.get('stock_code')}",
            'ts_code': ts_code,
            'event_type': event_type,
            'publish_time': published,
            'title': title,
            'source': str(item.get('source') or 'cninfo'),
        })
    return dedup_events(_coerce_events(pl.DataFrame(rows, infer_schema_length=None)
                                       if rows else empty_events()))


def _parse_time(value: Any) -> datetime | None:
    text = str(value).strip()
    if not text:
        return None
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
        try:
            return datetime.strptime(text[:len(fmt)], fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace('Z', '').replace('T', ' '))
    except ValueError:
        return None


def _coerce_events(frame: pl.DataFrame) -> pl.DataFrame:
    """统一列顺序与类型；缺列补空，日期列统一为 Datetime。"""
    if frame.is_empty():
        return empty_events()
    for column in EVENT_COLUMNS:
        if column not in frame.columns:
            frame = frame.with_columns(pl.lit(None).alias(column))
    frame = frame.select(list(EVENT_COLUMNS))
    dtype = frame.schema['publish_time']
    if dtype == pl.Date:
        frame = frame.with_columns(pl.col('publish_time').cast(pl.Datetime))
    elif dtype == pl.String:
        frame = frame.with_columns(
            pl.col('publish_time').str.slice(0, 19)
            .str.to_datetime(strict=False))
    return frame.filter(pl.col('publish_time').is_not_null())


def dedup_events(frame: pl.DataFrame) -> pl.DataFrame:
    """同标的同类型 30 自然日内只保留最早一条（滤掉进展/结果类重复公告）。"""
    if frame.is_empty():
        return frame
    records = sorted(frame.to_dicts(),
                     key=lambda item: (item['ts_code'], item['event_type'],
                                       item['publish_time']))
    kept: list[dict[str, Any]] = []
    markers: dict[tuple[str, str], datetime] = {}
    for item in records:
        key = (item['ts_code'], item['event_type'])
        previous = markers.get(key)
        if previous is not None \
                and item['publish_time'] - previous <= timedelta(days=DEDUP_DAYS):
            continue
        markers[key] = item['publish_time']
        kept.append(item)
    return _coerce_events(pl.DataFrame(kept, infer_schema_length=None))


def load_events(store_root: Path | str) -> pl.DataFrame:
    path = events_path(store_root)
    if not path.is_file():
        return empty_events()
    try:
        return _coerce_events(pl.read_parquet(path))
    except (OSError, ValueError) as exc:
        logger.warning(f'[events] 事件表读取失败，按无数据处理: {path}: {exc}')
        return empty_events()


def load_coverage(store_root: Path | str) -> dict[str, Any]:
    path = coverage_path(store_root)
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        logger.warning(f'[events] 覆盖声明读取失败: {path}: {exc}')
        return {}


def _write_coverage(store_root: Path | str, frame: pl.DataFrame,
                    source: str, complete: bool,
                    coverage_start: Any = None,
                    coverage_end: Any = None) -> dict[str, Any]:
    start_day = _as_date(coverage_start)
    end_day = _as_date(coverage_end)
    if frame.is_empty():
        info: dict[str, Any] = {
            'source': source,
            'start': start_day.isoformat() if start_day else None,
            'end': end_day.isoformat() if end_day else None,
            'rows': 0,
            'complete': complete, 'updated_at': datetime.now().isoformat(timespec='seconds'),
        }
    else:
        data_start = frame['publish_time'].min()
        data_end = frame['publish_time'].max()
        start_value = start_day or _as_date(data_start)
        end_value = end_day or _as_date(data_end)
        info = {
            'source': source,
            'start': start_value.isoformat() if start_value else None,
            'end': end_value.isoformat() if end_value else None,
            'rows': frame.height,
            'complete': complete,
            'declared': bool(start_day and end_day),
            'updated_at': datetime.now().isoformat(timespec='seconds'),
        }
    path = coverage_path(store_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(info, ensure_ascii=False, indent=2),
                    encoding='utf-8')
    return info


def _read_source(path: Path) -> pl.DataFrame:
    if path.suffix.lower() == '.csv':
        return pl.read_csv(path, try_parse_dates=True, infer_schema_length=10000)
    if path.suffix.lower() in ('.parquet', '.pq'):
        return pl.read_parquet(path)
    raise ValueError(f'不支持的事件文件类型: {path.suffix}')


def _detect_format(columns: Iterable[str]) -> str:
    names = set(columns)
    if {'keyword', 'stock_code', 'display_time'} <= names:
        return 'raw'
    if {'ts_code', 'event_type'} & names and 'publish_time' in names:
        return 'normalized'
    raise ValueError(
        '无法识别事件文件格式：需要 (keyword, stock_code, display_time) '
        '或 (ts_code, event_type, publish_time)')


def import_file(path: Path | str, store_root: Path | str | None = None,
                mode: str = 'replace', source: str = 'cninfo',
                complete: bool = True, start: Any = None,
                end: Any = None, coverage_start: Any = None,
                coverage_end: Any = None) -> dict[str, Any]:
    """导入离线事件文件；``mode='replace'`` 全量替换，``'append'`` 增量合并。

    ``coverage_start/end`` 是**覆盖声明**（该区间内已完整抓取），与
    ``start/end``（记录过滤）分开：声明区间之外的日期一律按 ``unknown`` 处理。
    """
    if mode not in ('replace', 'append'):
        raise ValueError(f"未知导入模式: {mode}")
    root = Path(store_root) if store_root else _default_store_root()
    file_path = Path(path)
    raw = _read_source(file_path)
    kind = _detect_format(raw.columns)
    if kind == 'raw':
        incoming = normalize_announcements(raw, start, end)
    else:
        incoming = dedup_events(_coerce_events(raw))
    if incoming.is_empty():
        raise ValueError(f'事件文件没有可用记录: {file_path}')

    if mode == 'append':
        existing = load_events(root)
        if not existing.is_empty():
            duplicate_id_rows = existing.height - existing['event_id'].n_unique()
            if duplicate_id_rows > 0:
                logger.warning(
                    f'[events] 现有事件表存在 {duplicate_id_rows} 行重复 event_id；'
                    'append 会按 event_id 保留首条，重复行将被丢弃——'
                    '若这些行本应是不同事件，说明上游 ID 生成缺少区分维度'
                )
            incoming = pl.concat([existing, incoming], how='vertical_relaxed')
        incoming = incoming.unique(subset=['event_id'], keep='first',
                                   maintain_order=True)
        incoming = dedup_events(incoming)

    path_out = events_path(root)
    path_out.parent.mkdir(parents=True, exist_ok=True)
    incoming.write_parquet(path_out)
    coverage = _write_coverage(root, incoming, source, complete,
                               coverage_start, coverage_end)
    logger.info(f'[events] {mode} 导入 {incoming.height} 条 → {path_out}')
    return {'rows': incoming.height, 'mode': mode, 'coverage': coverage,
            'source_format': kind}


def _default_store_root() -> Path:
    from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
    from src.utils.config import get_config  # noqa: PLC0415

    return resolve_store_root(get_config())


def cooldown_range(publish_time: Any, trading_dates: Sequence[date],
                   days: int) -> tuple[date, date] | None:
    """返回冷却区间 ``[T+1, T+N]``（按交易日计）；日历不足时返回 ``None``。"""
    published = publish_time.date() if isinstance(publish_time, datetime) \
        else _as_date(publish_time)
    if published is None or days <= 0:
        return None
    index = bisect_right(list(trading_dates), published)
    if index >= len(trading_dates):
        return None
    last = min(len(trading_dates) - 1, index + days - 1)
    return trading_dates[index], trading_dates[last]


class EventStore:
    """本地事件表 + 冷却窗口查询（初始化后只读，运行时不联网）。"""

    def __init__(self, store_root: Path | str | None = None,
                 cooldowns: dict[str, int] | None = None) -> None:
        self.store_root = Path(store_root) if store_root else _default_store_root()
        self.cooldowns = dict(cooldowns or RISK_EVENT_COOLDOWNS)
        self.events = load_events(self.store_root)
        self.coverage = load_coverage(self.store_root) or self._derive_coverage()
        self.available = not self.events.is_empty()
        self.risk_events = self.events.filter(
            pl.col('event_type').is_in(list(self.cooldowns)))

    def _derive_coverage(self) -> dict[str, Any]:
        if self.events.is_empty():
            return {}
        return {
            'source': 'derived',
            'start': str(self.events['publish_time'].min())[:10],
            'end': str(self.events['publish_time'].max())[:10],
            'rows': self.events.height,
            'complete': False,
        }

    def trading_dates(self, start: Any = None, end: Any = None) -> tuple[date, ...]:
        """从统一交易日历取 ``[start, end]`` 的交易日，供 ``blocked_map`` 等使用。

        事件模块不自己拼日历：口径与 freshness / clean_set / tradable_universe
        一致（优先本地基准指数真实交易日）。``start`` 缺省取日历首日，
        ``end`` 缺省取今天。
        """
        from src.data.trading_calendar import get_calendar  # noqa: PLC0415

        calendar = get_calendar(self.store_root)
        lo = _as_date(start) or (calendar.dates[0] if calendar.dates else date.today())
        hi = _as_date(end) or date.today()
        return calendar.between(lo, hi)

    @property
    def covered_range(self) -> tuple[date | None, date | None]:
        start = _as_date(self.coverage.get('start'))
        end = _as_date(self.coverage.get('end'))
        return start, end

    def covers(self, as_of: Any) -> bool:
        day = _as_date(as_of)
        start, end = self.covered_range
        if not self.available or day is None or start is None or end is None:
            return False
        return start <= day <= end

    def blocked_map(self, trading_dates: Sequence[date]) -> dict[date, dict[str, dict]]:
        """预计算 ``{交易日: {ts_code: 冷却事件详情}}``，供逐日 O(1) 查询。"""
        blocked: dict[date, dict[str, dict]] = {}
        if not self.available or not trading_dates:
            return blocked
        dates = list(trading_dates)
        for item in self.risk_events.select(
                'ts_code', 'event_type', 'publish_time', 'event_id').to_dicts():
            days = self.cooldowns.get(item['event_type'])
            if not days:
                continue
            span = cooldown_range(item['publish_time'], dates, days)
            if span is None:
                continue
            first, last = span
            for day in dates[bisect_left(dates, first):bisect_right(dates, last)]:
                blocked.setdefault(day, {})[item['ts_code']] = {
                    'event_type': item['event_type'],
                    'publish_date': str(item['publish_time'])[:10],
                    'cooldown_until': last.isoformat(),
                    'event_id': item['event_id'],
                }
        return blocked

    def status(self, code: str, as_of: Any,
               trading_dates: Sequence[date]) -> dict[str, Any]:
        """单标的冷却状态：``cooling`` / ``clear`` / ``unknown``。"""
        day = _as_date(as_of)
        base: dict[str, Any] = {
            'status': STATUS_UNKNOWN, 'event_type': '', 'publish_date': '',
            'cooldown_until': '', 'event_id': '', 'reason': '',
        }
        if day is None:
            base['reason'] = 'as_of_missing'
            return base
        if not self.covers(day):
            base['reason'] = 'no_coverage'
            return base
        target = str(code).upper()
        rows = self.risk_events.filter(pl.col('ts_code') == target)
        hits: list[dict[str, Any]] = []
        for item in rows.select('ts_code', 'event_type', 'publish_time',
                                'event_id').to_dicts():
            days = self.cooldowns.get(item['event_type'])
            span = cooldown_range(item['publish_time'], list(trading_dates), days or 0)
            if span is None:
                continue
            first, last = span
            if first <= day <= last:
                hits.append({**item, 'first': first, 'last': last})
        if not hits:
            base['status'] = STATUS_CLEAR
            return base
        hit = max(hits, key=lambda item: item['last'])
        base.update({
            'status': STATUS_COOLING,
            'event_type': hit['event_type'],
            'publish_date': str(hit['publish_time'])[:10],
            'cooldown_until': hit['last'].isoformat(),
            'event_id': hit['event_id'],
        })
        return base

    def summary(self) -> dict[str, Any]:
        counts = self.events['event_type'].value_counts().to_dicts() \
            if self.available else []
        return {
            'available': self.available,
            'rows': self.events.height,
            'coverage': self.coverage,
            'cooldowns': self.cooldowns,
            'by_type': {item['event_type']: item['count'] for item in counts},
        }


def get_event_status(code: str, as_of: Any, *, trading_dates: Sequence[date],
                     store_root: Path | str | None = None,
                     cooldowns: dict[str, int] | None = None) -> dict[str, Any]:
    """单标的冷却状态查询（``as_of`` 与交易日历必须显式传入）。

    交易日历可用 ``EventStore(store_root).trading_dates(start, end)`` 从统一
    日历模块取得；这里保持强制显式传入，避免静默使用低置信度近似日历。
    """
    if not trading_dates:
        raise ValueError('缺少交易日历，无法按交易日计算冷却窗口')
    store = EventStore(store_root, cooldowns)
    result = store.status(code, as_of, trading_dates)
    result['covered_range'] = list(store.covered_range)
    return result


def _main(args: argparse.Namespace) -> int:
    root = Path(args.store_root) if args.store_root else _default_store_root()
    if args.command in ('import', 'append'):
        result = import_file(
            args.path, root, mode='replace' if args.command == 'import' else 'append',
            source=args.source, complete=not args.partial,
            start=args.start, end=args.end,
            coverage_start=args.coverage_start,
            coverage_end=args.coverage_end)
        logger.info(f'[events] 导入完成: {result["rows"]} 条，'
                    f'覆盖 {result["coverage"].get("start")}~'
                    f'{result["coverage"].get("end")}')
        return 0
    info = EventStore(root).summary()
    logger.info(f'[events] 事件表: {json.dumps(info, ensure_ascii=False, indent=2)}')
    return 0 if info['available'] else 1


def main() -> int:
    parser = argparse.ArgumentParser(description='公告事件本地表（离线导入）')
    parser.add_argument('command', choices=['import', 'append', 'stats'])
    parser.add_argument('--path', default=None, help='CSV / Parquet 事件文件')
    parser.add_argument('--source', default='cninfo', help='数据来源标识')
    parser.add_argument('--start', default=None, help='只导入该日期之后的事件')
    parser.add_argument('--end', default=None, help='只导入该日期之前的事件')
    parser.add_argument('--coverage-start', default=None,
                        help='覆盖声明起始日（区间内已完整抓取）')
    parser.add_argument('--coverage-end', default=None,
                        help='覆盖声明结束日（区间内已完整抓取）')
    parser.add_argument('--partial', action='store_true',
                        help='标记覆盖不完整（区间内仍可能有漏抓）')
    parser.add_argument('--store-root', default=None, help='本地列存根目录')
    parsed = parser.parse_args()
    if parsed.command in ('import', 'append') and not parsed.path:
        parser.error(f'{parsed.command} 需要 --path')
    return _main(parsed)


if __name__ == '__main__':
    raise SystemExit(main())
