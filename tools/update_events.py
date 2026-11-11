#!/usr/bin/env python3

r"""公告事件日更入口：按自然日抓取 → 稳定化收敛 → 追加入库（不改覆盖声明）。

背景
----
``meta/events/events.parquet`` 没有日更任务，只能手工跑
``tools/backfill_events.py`` 按月补；而月级查询会静默丢页（源端间歇返回
HTTP 200 + 空 ``announcements``，``totalRecordNum`` 还计重复行）。本脚本把
已验证过的「按自然日 + 判空重试 + 同日多轮收敛」固化成一条可重复命令：

- 单连接串行、逐关键词带节流，同刻只压一条外部采集；
- 每个自然日的分片原子写，``progress.csv`` / ``failed.csv`` 留断点，
  重跑自动跳过已有分片（``--force`` 可重抓）；
- 入库走 ``src.data.events.import_file(mode='append')``：既有行一条不改，
  30 日去重按既定口径；
- **覆盖声明 ``coverage.json`` 的 start/end 一律原样传递**，绝不由数据
  min/max 反推；跑完只打印「是否需要人工推进 coverage.end」及其依据。

典型用法::

    python tools/update_events.py                       # 补今天
    python tools/update_events.py --date 2026-11-11     # 指定某天
    python tools/update_events.py --start 2026-11-07 --end 2026-11-11
    python tools/update_events.py --days 3              # 含今天往前 3 个自然日
    python tools/update_events.py --skip-fetch          # 只用已有分片入库
    python tools/update_events.py --dry-run             # 只算不落盘

退出码：0 正常；2 没有可用分片；3 抓取失败（已写 failed.csv 并停压）；
4 入库后既有行对拍不通过（需按打印的回滚路径恢复）。
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = PROJECT_ROOT / 'research' / 'events'
DEFAULT_SHARD_DIR = RESEARCH_DIR / 'data' / 'backfill' / 'daily'
DEFAULT_STORE_ROOT = PROJECT_ROOT / 'data' / 'local_store'
KEYWORDS = (
    '减持', '增持', '股权激励', '中标', '重大合同',
    '处罚', '立案', '重组', '并购', '分红',
)
_RAW_SCHEMA = {
    'keyword': pl.Utf8,
    'art_code': pl.Utf8,
    'stock_code': pl.Utf8,
    'short_name': pl.Utf8,
    'title': pl.Utf8,
    'display_time': pl.Datetime('us'),
}
PROGRESS_HEADER = ['day', 'status', 'rounds', 'unique', 'last_round_new',
                   'total_hits', 'elapsed_s', 'finished_at']
FAILED_HEADER = ['day', 'error', 'failed_at']


def _load_fetcher() -> Any:
    if str(RESEARCH_DIR) not in sys.path:
        sys.path.insert(0, str(RESEARCH_DIR))
    import fetch_announcements as module  # noqa: PLC0415

    return module


def _events_api() -> Any:
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    from src.data import events  # noqa: PLC0415

    return events


def _append_row(path: Path, row: dict[str, Any], header: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fresh = not path.is_file()
    with path.open('a', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=header, extrasaction='ignore')
        if fresh:
            writer.writeheader()
        writer.writerow(row)


def _now() -> str:
    return time.strftime('%Y-%m-%d %H:%M:%S')


def _sha256(path: Path) -> str:
    import hashlib  # noqa: PLC0415

    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _read_coverage(store_root: Path) -> dict[str, Any]:
    path = store_root / 'meta' / 'events' / 'coverage.json'
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        logger.warning(f'覆盖声明读取失败: {exc}')
        return {}
    return payload if isinstance(payload, dict) else {}


def _days(start: date, end: date) -> list[date]:
    out: list[date] = []
    cursor = start
    while cursor <= end:
        out.append(cursor)
        cursor += timedelta(days=1)
    return out


def _fetch_day(fetcher: Any, day: date, keywords: tuple[str, ...],
               sleep: float, max_rounds: int
               ) -> tuple[dict[str, Any], int, int, int]:
    """抓单日并做同日收敛：返回唯一行、轮数、首轮命中合计、末轮新增数。

    收敛判据是「本轮没有新增唯一公告」，不用 ``totalRecordNum`` 判等——
    该字段计入重复行，等于它反而会误判。第一轮必然全部为新增，所以
    最少两轮；单日命中通常 ≤30 条（单页），第 1 页空同样由
    ``fetch_cninfo_chunk`` 的判空重试兜住。
    """
    day_text = day.isoformat()
    seen: dict[tuple[str, str], dict[str, Any]] = {}
    hits = 0
    rounds = 0
    last_new = 0
    for round_no in range(1, max_rounds + 1):
        rounds = round_no
        round_new = 0
        for keyword in keywords:
            records, total = fetcher.fetch_cninfo_chunk(keyword, day_text, day_text)
            if round_no == 1:
                # 只累计首轮命中：totalRecordNum 计重复行，多轮累加没有可比含义
                hits += total
            for row in records:
                key = (row.get('art_code') or '', row.get('stock_code') or '')
                if key not in seen:
                    seen[key] = row
                    round_new += 1
            time.sleep(sleep)
        logger.info(f'{day_text} 第{round_no}轮 新增唯一={round_new} 累计={len(seen)}')
        last_new = round_new
        if round_new == 0:
            break
        if round_no == max_rounds:
            logger.warning(f'{day_text} 达到最大轮数 {max_rounds} 仍有新增，可能未收敛')
    return seen, rounds, hits, last_new


def _write_shard(path: Path, rows: list[dict[str, Any]]) -> None:
    from src.data.atomic_write import write_parquet_atomic  # noqa: PLC0415

    frame = pl.from_dicts(rows, schema=_RAW_SCHEMA)
    write_parquet_atomic(frame, path, compression='zstd')


def fetch_days(args: argparse.Namespace, days: list[date],
               shard_dir: Path) -> list[Path]:
    fetcher = _load_fetcher()
    # 单连接串行：关掉页级线程池，并保留关键词之间的节流
    fetcher.WORKERS = 1
    fetcher.REQUEST_SLEEP = max(0.05, args.sleep)
    fetcher.REQUEST_JITTER = 0.0
    shard_dir.mkdir(parents=True, exist_ok=True)
    progress = shard_dir / 'progress.csv'
    failed = shard_dir / 'failed.csv'
    written: list[Path] = []
    for day in days:
        target = shard_dir / f'{day:%Y%m%d}.parquet'
        if target.is_file() and not args.force:
            rows = pl.read_parquet(target).height
            logger.info(f'分片已存在，跳过抓取: {target.name}（{rows} 行）')
            _append_row(progress, {
                'day': day.isoformat(), 'status': 'exists', 'rounds': '',
                'unique': rows, 'last_round_new': '', 'total_hits': '',
                'elapsed_s': '', 'finished_at': '',
            }, PROGRESS_HEADER)
            written.append(target)
            continue
        started = time.time()
        try:
            rows, rounds, hits, last_new = _fetch_day(
                fetcher, day, tuple(args.keywords), args.sleep, args.max_rounds)
            _write_shard(target, list(rows.values()))
        except Exception as exc:  # noqa: BLE001 - 外部采集失败必须停压并留痕
            _append_row(failed, {
                'day': day.isoformat(), 'error': f'{type(exc).__name__}: {exc}',
                'failed_at': _now(),
            }, FAILED_HEADER)
            _append_row(progress, {
                'day': day.isoformat(), 'status': 'failed', 'rounds': '',
                'unique': '', 'last_round_new': '', 'total_hits': '',
                'elapsed_s': round(time.time() - started, 1), 'finished_at': '',
            }, PROGRESS_HEADER)
            logger.error(f'{day} 抓取失败，已写 failed.csv 并停止（不换天硬重试）: {exc}')
            raise SystemExit(3) from exc
        _append_row(progress, {
            'day': day.isoformat(), 'status': 'ok', 'rounds': rounds,
            'unique': len(rows), 'last_round_new': last_new, 'total_hits': hits,
            'elapsed_s': round(time.time() - started, 1), 'finished_at': _now(),
        }, PROGRESS_HEADER)
        logger.info(f'{day} 分片落盘 {target.name}: {len(rows)} 行，'
                    f'{rounds} 轮，命中合计 {hits}，'
                    f'耗时 {round(time.time() - started, 1)}s')
        written.append(target)
    return written


def _merge(shard_dir: Path, paths: list[Path], days: list[date]) -> pl.DataFrame:
    frames = []
    for path in sorted(paths):
        frame = pl.read_parquet(path)
        if not frame.is_empty():
            frames.append(frame)
    if not frames:
        return pl.DataFrame(schema=_RAW_SCHEMA)
    merged = pl.concat(frames, how='vertical_relaxed')
    # maintain_order=True：行序不能由哈希决定，否则 30 日去重会在同日不同
    # art_code 之间摇摆，表现为「同一批分片重复导入还能新增」。
    merged = merged.unique(subset=['art_code', 'stock_code'], keep='first',
                           maintain_order=True)
    target = shard_dir / f'_merged_{days[0]:%Y%m%d}_{days[-1]:%Y%m%d}.parquet'
    merged.write_parquet(target)
    logger.info(f'合并 {len(frames)} 个日分片 → {merged.height} 行 → {target.name}')
    return merged


def _compare_existing(before: pl.DataFrame, after: pl.DataFrame) -> dict[str, int]:
    """既有行逐列对拍（按 event_id 对齐，含 null 相等语义）。"""
    ids = set(before['event_id'].to_list())
    left = before.sort('event_id')
    right = after.filter(pl.col('event_id').is_in(list(ids))).sort('event_id')
    changed = {}
    gained = {}
    for name in left.columns:
        a, b = left[name].cast(pl.Utf8), right[name].cast(pl.Utf8)
        both_null = a.is_null() & b.is_null()
        changed[name] = int(((a != b).fill_null(True) & ~both_null).sum())
        gained[name] = int((b.is_null() & ~a.is_null()).sum())
    return {'changed': sum(v for k, v in changed.items() if k != 'event_id'),
            'null_gain': sum(gained.values()),
            'lost': len(ids - set(after['event_id'].to_list()))}


def _coverage_advice(store_root: Path, table: pl.DataFrame,
                     coverage: dict[str, Any], days: list[date],
                     incomplete: list[str]) -> dict[str, Any]:
    """只给判断与依据，不改声明。"""
    declared_end = coverage.get('end')
    data_end = table['publish_time'].max()
    data_end_date = data_end.date().isoformat() if data_end else None
    beyond = []
    if declared_end and data_end_date and data_end_date > declared_end:
        beyond = [d.isoformat() for d in _days(
            date.fromisoformat(declared_end) + timedelta(days=1),
            date.fromisoformat(data_end_date))]
    rows_beyond = 0
    if beyond:
        rows_beyond = table.filter(
            pl.col('publish_time').dt.date() >= date.fromisoformat(beyond[0])
        ).height
    reasons = []
    if incomplete:
        reasons.append(f'有 {len(incomplete)} 天抓取失败或未覆盖：{incomplete}')
    if beyond:
        reasons.append(f'表内已有 {len(beyond)} 个自然日 / {rows_beyond} 行落在声明终点 '
                       f'{declared_end} 之后（{beyond[0]}~{beyond[-1]}）')
    need = bool(beyond) and not incomplete
    return {
        'declared_start': coverage.get('start'),
        'declared_end': declared_end,
        'data_start': (table['publish_time'].min().date().isoformat()
                       if table.height else None),
        'data_end': data_end_date,
        'beyond_end_days': beyond,
        'rows_beyond_end': rows_beyond,
        'needs_manual_advance': need,
        'reasons': reasons,
        'note': ('覆盖声明必须人工推进：前移等于宣称该区间已完整收口，需先逐日稳定化'
                 '收敛且无失败日；本脚本只追加行，绝不改 start/end'),
        'table_rows': table.height,
        'coverage_rows': coverage.get('rows'),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description='公告事件日更（不改覆盖声明）')
    parser.add_argument('--date', help='单个自然日 YYYY-MM-DD，默认今天')
    parser.add_argument('--start', help='区间起点 YYYY-MM-DD')
    parser.add_argument('--end', help='区间终点 YYYY-MM-DD')
    parser.add_argument('--days', type=int, help='含今天在内往前 N 个自然日')
    parser.add_argument('--max-rounds', type=int, default=3,
                        help='同日最多抓取轮数（默认 3）')
    parser.add_argument('--sleep', type=float, default=0.3,
                        help='每次请求后的节流秒数（默认 0.3）')
    parser.add_argument('--shard-dir', default=str(DEFAULT_SHARD_DIR))
    parser.add_argument('--store-root', default=str(DEFAULT_STORE_ROOT))
    parser.add_argument('--keywords', nargs='+', default=list(KEYWORDS))
    parser.add_argument('--force', action='store_true', help='重抓已有分片')
    parser.add_argument('--skip-fetch', action='store_true',
                        help='不抓取，只用已有分片入库（续跑用）')
    parser.add_argument('--dry-run', action='store_true', help='只计算不落盘')
    args = parser.parse_args()

    today = date.today()
    if args.start or args.end:
        start = date.fromisoformat(args.start) if args.start else today
        end = date.fromisoformat(args.end) if args.end else today
    elif args.date:
        start = end = date.fromisoformat(args.date)
    elif args.days:
        end = today
        start = today - timedelta(days=max(1, args.days) - 1)
    else:
        start = end = today
    if start > end:
        parser.error(f'起始日 {start} 晚于结束日 {end}')
    days = _days(start, end)
    store_root = Path(args.store_root)
    events = _events_api()
    coverage = _read_coverage(store_root)
    if not coverage.get('start') or not coverage.get('end'):
        logger.error('覆盖声明缺失 start/end，拒绝入库（先跑 tools/backfill_events.py）')
        return 2
    shard_dir = Path(args.shard_dir)

    paths: list[Path] = []
    incomplete: list[str] = []
    if args.skip_fetch:
        for day in days:
            path = shard_dir / f'{day:%Y%m%d}.parquet'
            if path.is_file():
                paths.append(path)
            else:
                incomplete.append(day.isoformat())
        logger.info(f'--skip-fetch：命中 {len(paths)} 个分片，缺 {len(incomplete)} 天')
    else:
        paths = fetch_days(args, days, shard_dir)

    merged = _merge(shard_dir, paths, days)
    if merged.is_empty():
        logger.error('没有可导入的公告记录')
        return 2
    if args.dry_run:
        preview = events.import_file(
            shard_dir / f'_merged_{days[0]:%Y%m%d}_{days[-1]:%Y%m%d}.parquet',
            store_root=store_root, mode='append',
            start=days[0].isoformat(), end=days[-1].isoformat(),
            coverage_start=coverage['start'], coverage_end=coverage['end'],
            dry_run=True)
        print(json.dumps({'dry_run': True, **preview}, ensure_ascii=False, indent=1))
        return 0

    formal = events.events_path(store_root)
    before = events.load_events(store_root)
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_dir = PROJECT_ROOT / '_backup' / 'events_daily' / stamp
    backup_dir.mkdir(parents=True, exist_ok=True)
    import shutil  # noqa: PLC0415

    shutil.copy2(formal, backup_dir / 'events.parquet')
    shutil.copy2(events.coverage_path(store_root), backup_dir / 'coverage.json')
    before_sha = _sha256(formal)
    if _sha256(backup_dir / 'events.parquet') != before_sha:
        logger.error('回滚快照 SHA 与正式表不一致，已中止')
        return 4
    logger.info(f'回滚快照 {backup_dir}（SHA {before_sha[:12]}）')

    result = events.import_file(
        shard_dir / f'_merged_{days[0]:%Y%m%d}_{days[-1]:%Y%m%d}.parquet',
        store_root=store_root, mode='append',
        start=days[0].isoformat(), end=days[-1].isoformat(),
        coverage_start=coverage['start'], coverage_end=coverage['end'])
    after = events.load_events(store_root)
    after_sha = _sha256(formal)
    guard = _compare_existing(before, after)

    new_coverage = _read_coverage(store_root)
    advice = _coverage_advice(store_root, after, new_coverage, days, incomplete)
    (backup_dir / 'meta.json').write_text(json.dumps({
        'days': [d.isoformat() for d in days],
        'requested_shards': len(paths),
        'incomplete_days': incomplete,
        'import': result,
        'guard': guard,
        'coverage_advice': advice,
        'sha256_before': before_sha,
        'sha256_after': after_sha,
    }, ensure_ascii=False, indent=2), encoding='utf-8')

    print('=' * 60)
    print(f'本次新增行数: {result["added"]}（源行 {result["source_rows"]}，'
          f'规范化 {result["batch_rows"]}，与既有近重复丢弃 '
          f'{result["dropped_near_existing"]}）')
    print(f'表行数: {before.height} → {after.height}；SHA256 {before_sha[:16]} → '
          f'{after_sha[:16]}')
    print(f'既有行对拍: 改动 {guard["changed"]} 格、非空→null {guard["null_gain"]} 行、'
          f'丢失 {guard["lost"]} 条')
    print(f'当前覆盖边界: start={new_coverage.get("start")} '
          f'end={new_coverage.get("end")}（声明行数 {new_coverage.get("rows")}）')
    print(f'是否需要人工推进 coverage.end: '
          f'{"是" if advice["needs_manual_advance"] else "否"}')
    for reason in advice['reasons']:
        print(f'  依据: {reason}')
    print(f'回滚快照: {backup_dir}')
    print('=' * 60)

    if guard['changed'] or guard['null_gain'] or guard['lost']:
        logger.error(f'既有行被改动，请按回滚快照 {backup_dir} 恢复')
        return 4
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
