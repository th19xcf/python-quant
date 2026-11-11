#!/usr/bin/env python3

r"""公告事件增量补数：把 research 采集脚本接到生产事件表。

问题背景
--------
``data/local_store/meta/events/events.parquet`` 当前只覆盖 2025 年，
新鲜度巡检的唯一 critical 就是这里。项目默认 ``allow_remote_sources=False``，
且巨潮/东财接口有限流，因此**本脚本不会擅自联网、也不会擅自改写事件表**：

- 默认只打印待补区间与分片计划（纯只读）；
- ``--allow-remote`` 才按自然月分片抓取，落到
  ``research/events/data/backfill/YYYYMM.parquet``，已存在的分片自动跳过
  （断点续传）；
- ``--import-only --commit`` 才把分片合并并用
  ``src.data.events.import_file(mode='append')`` 写入生产事件表，
  同时把 ``coverage.json`` 的覆盖终点推进到本次补到的日期。

典型用法::

    python tools/backfill_events.py                         # 看计划
    python tools/backfill_events.py --allow-remote         # 只抓分片，不入库
    python tools/backfill_events.py --import-only --commit # 合并分片并入库

分片是原始公告表（``keyword/stock_code/display_time/title/art_code``），
导入时由生产侧 ``normalize_announcements`` 统一规范化、按 event_id 去重。
"""

from __future__ import annotations

import argparse
import calendar
import csv
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = PROJECT_ROOT / 'research' / 'events'
DEFAULT_SHARD_DIR = RESEARCH_DIR / 'data' / 'backfill'
DEFAULT_STORE_ROOT = PROJECT_ROOT / 'data' / 'local_store'
DEFAULT_KEYWORDS = (
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


def _load_fetcher() -> Any:
    """延迟加载 research 采集模块（该目录不是 Python 包）。"""
    if str(RESEARCH_DIR) not in sys.path:
        sys.path.insert(0, str(RESEARCH_DIR))
    import fetch_announcements as module  # noqa: PLC0415

    return module


def _events_api() -> Any:
    """延迟加载生产事件模块（脚本从 tools/ 启动时项目根不在 sys.path）。"""
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    from src.data import events  # noqa: PLC0415

    return events


def _read_coverage(store_root: Path) -> dict[str, Any]:
    path = store_root / 'meta' / 'events' / 'coverage.json'
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        logger.warning(f'覆盖声明读取失败，按无声明处理: {exc}')
        return {}
    return payload if isinstance(payload, dict) else {}


def _month_chunks(start: date, end: date) -> list[tuple[str, str]]:
    """按自然月切分，单块更小、便于断点续传与限流退避。"""
    chunks: list[tuple[str, str]] = []
    current = date(start.year, start.month, 1)
    while current <= end:
        last_day = calendar.monthrange(current.year, current.month)[1]
        month_end = date(current.year, current.month, last_day)
        chunk_start = max(current, start)
        chunk_end = min(month_end, end)
        chunks.append((chunk_start.isoformat(), chunk_end.isoformat()))
        current = month_end + timedelta(days=1)
    return chunks


def _shard_path(shard_dir: Path, start: str) -> Path:
    return shard_dir / f'{start[:7].replace("-", "")}.parquet'


def _empty_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=_RAW_SCHEMA)


def _append_row(path: Path, row: dict[str, Any], header: list[str]) -> None:
    """向断点日志追加一行（首行写表头，文件不存在时创建）。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fresh = not path.is_file()
    with path.open('a', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=header, extrasaction='ignore')
        if fresh:
            writer.writeheader()
        writer.writerow(row)


PROGRESS_HEADER = ['month', 'start', 'end', 'status', 'rows', 'total_hits',
                   'elapsed_s', 'finished_at']
FAILED_HEADER = ['month', 'start', 'end', 'error', 'failed_at']


def _fetch_shards(args: argparse.Namespace, chunks: list[tuple[str, str]],
                  shard_dir: Path) -> list[Path]:
    """按月抓取分片；分片原子写，逐月记 progress.csv，失败即停压。

    断点语义：分片文件存在即视为该月已完成（``--force`` 可重抓）；
    任何一个月抛错都写 ``failed.csv`` 并**立即停止**，不跳到下个月硬重试——
    巨潮的错误多为限速/封禁，继续加压只会延长恢复时间。
    """
    from src.data.atomic_write import write_parquet_atomic  # noqa: PLC0415

    module = _load_fetcher()
    module.WORKERS = max(1, args.workers)
    module.REQUEST_SLEEP = max(0.05, args.sleep)
    module.REQUEST_JITTER = max(0.0, args.jitter)
    shard_dir.mkdir(parents=True, exist_ok=True)
    progress_path = Path(args.progress_csv) if args.progress_csv \
        else shard_dir / 'progress.csv'
    failed_path = Path(args.failed_csv) if args.failed_csv \
        else shard_dir / 'failed.csv'
    written: list[Path] = []
    for start, end in chunks:
        target = _shard_path(shard_dir, start)
        if target.is_file() and not args.force:
            rows = pl.read_parquet(target).height
            logger.info(f'分片已存在，跳过: {target.name}（{start} ~ {end}）')
            _append_row(progress_path, {
                'month': target.stem, 'start': start, 'end': end,
                'status': 'exists', 'rows': rows, 'total_hits': '',
                'elapsed_s': '', 'finished_at': '',
            }, PROGRESS_HEADER)
            written.append(target)
            continue
        started = time.time()
        try:
            logger.info(f'抓取分片 {start} ~ {end} source={args.source}')
            frame, meta = module.fetch_announcements(
                start, end, tuple(args.keywords), args.source, partial_path=None,
            )
            if frame.is_empty():
                frame = _empty_frame()
            write_parquet_atomic(frame, target, compression='zstd')
        except Exception as exc:
            _append_row(failed_path, {
                'month': target.stem, 'start': start, 'end': end,
                'error': f'{type(exc).__name__}: {exc}',
                'failed_at': _now(),
            }, FAILED_HEADER)
            _append_row(progress_path, {
                'month': target.stem, 'start': start, 'end': end,
                'status': 'failed', 'rows': '', 'total_hits': '',
                'elapsed_s': round(time.time() - started, 1), 'finished_at': '',
            }, PROGRESS_HEADER)
            logger.error(f'分片 {target.name} 失败，已写入 failed.csv 并停止抓取: {exc}')
            raise
        hits = sum(value.get('total_hits', 0) for value in meta.values()
                   if isinstance(value, dict))
        _append_row(progress_path, {
            'month': target.stem, 'start': start, 'end': end, 'status': 'ok',
            'rows': frame.height, 'total_hits': hits,
            'elapsed_s': round(time.time() - started, 1), 'finished_at': _now(),
        }, PROGRESS_HEADER)
        logger.info(f'分片落盘 {target.name}: {frame.height} 行 '
                    f'(命中 {hits}，耗时 {round(time.time() - started, 1)}s)')
        written.append(target)
    return written


def _now() -> str:
    return time.strftime('%Y-%m-%d %H:%M:%S')


def _import_shards(args: argparse.Namespace, shard_dir: Path,
                   coverage_start: str, coverage_end: str) -> int:
    paths = sorted(path for path in shard_dir.glob('*.parquet')
                   if not path.name.startswith('_'))
    if not paths:
        logger.error(f'没有可导入的分片: {shard_dir}')
        return 2
    frames = [pl.read_parquet(path) for path in paths]
    frames = [frame for frame in frames if not frame.is_empty()]
    if not frames:
        logger.error('所有分片均为空，拒绝写入事件表')
        return 2
    merged = pl.concat(frames, how='vertical_relaxed')
    # maintain_order=True 必需：不加时 unique 的结果行序由哈希决定，
    # 同一批分片在不同进程会得到不同的行序，进而让 30 日去重在不同 art_code
    # 之间摇摆，表现为「同一批次重复导入却还能新增」。
    merged = merged.unique(subset=['art_code', 'stock_code'], keep='first',
                           maintain_order=True)
    # 预演与真跑读同一个合并文件，保证预演结果与实跑一致；
    # 该文件是审计产物，名字以 _ 开头，不会被后续 glob 当分片读入。
    merged_path = shard_dir / '_merged_for_import.parquet'
    merged.write_parquet(merged_path)
    logger.info(f'合并 {len(paths)} 个分片 → {merged.height} 行')

    # 补历史区间时，覆盖声明必须取「原声明 ∪ 本次补数区间」的并集。
    # 直接把 args.end（历史年份）当 coverage_end 会写出 start>end 的声明，
    # 让 2025-2026 的数据被误判为 unknown。
    declared_start = min(s for s in (coverage_start, args.start) if s)
    declared_end = max(s for s in (coverage_end, args.end) if s)

    events = _events_api()
    result = events.import_file(
        merged_path,
        store_root=args.store_root,
        mode='append',
        source=args.source,
        complete=True,
        start=args.start,
        end=args.end,
        coverage_start=declared_start,
        coverage_end=declared_end,
        dry_run=args.dry_run,
    )
    logger.info(
        '入库结果: '
        f"dry_run={result['dry_run']} 表行数={result['rows']} "
        f"既有行={result['existing_rows']} 源行={result['source_rows']} "
        f"规范化后={result['batch_rows']} 将新增={result['added']} "
        f"与既有重复跳过={result['dropped_existing_conflict']} "
        f"与既有30日内重复跳过={result['dropped_near_existing']} "
        f"批次内丢弃={result['dropped_in_batch']} "
        f"既有行受影响={result['existing_affected']}"
    )
    if not args.dry_run:
        _annotate_coverage(Path(args.store_root), args.keywords)
    logger.info('事件表更新完成' if not args.dry_run else 'dry-run 完成，未写入')
    return 0


def _annotate_coverage(store_root: Path, keywords: list[str]) -> None:
    """在覆盖声明里写清抓取口径。

    ``complete=true`` 只对本脚本的关键词集合成立：业绩预告走独立的
    ``fetch_yjyg`` 通道，数据源与规范化口径都不同，不能因为本次补数就
    宣称「2026 年事件全量齐全」。把口径写进声明，避免下游误读。
    """
    path = store_root / 'meta' / 'events' / 'coverage.json'
    if not path.is_file():
        return
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        logger.warning(f'覆盖声明回写失败（不影响事件表）: {exc}')
        return
    if not isinstance(payload, dict):
        return
    payload['keyword_scope'] = list(keywords)
    payload['scope_notes'] = (
        '覆盖口径为上述关键词的公告（巨潮 hisAnnouncement）；'
        '业绩预告（业绩预增/预减）走 fetch_yjyg 独立通道，不在本次补数范围内'
    )
    tmp = path.with_suffix('.json.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n',
                   encoding='utf-8')
    tmp.replace(path)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='公告事件增量补数（默认只读计划）')
    parser.add_argument('--start', help='补数起始日 YYYY-MM-DD，缺省取覆盖声明 end+1')
    parser.add_argument('--end', help='补数结束日 YYYY-MM-DD，缺省今天')
    parser.add_argument('--source', choices=('cninfo', 'eastmoney'), default='cninfo')
    parser.add_argument('--keywords', nargs='*', default=list(DEFAULT_KEYWORDS))
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--sleep', type=float, default=0.25)
    parser.add_argument('--jitter', type=float, default=0.0,
                        help='每个 (关键词, 区间) 前的随机抖动上限秒数，默认 0（关闭）')
    parser.add_argument('--shard-dir', default=str(DEFAULT_SHARD_DIR))
    parser.add_argument('--progress-csv', default=None,
                        help='断点日志路径，缺省 <shard-dir>/progress.csv')
    parser.add_argument('--failed-csv', default=None,
                        help='失败清单路径，缺省 <shard-dir>/failed.csv')
    parser.add_argument('--store-root', default=str(DEFAULT_STORE_ROOT))
    parser.add_argument('--allow-remote', action='store_true',
                        help='显式允许联网抓取；不给则只打印计划')
    parser.add_argument('--force', action='store_true', help='重抓已存在的分片')
    parser.add_argument('--import-only', action='store_true',
                        help='不联网，只把现有分片导入生产事件表')
    parser.add_argument('--commit', action='store_true',
                        help='允许写入 events.parquet；--import-only 必须同时给出')
    parser.add_argument('--dry-run', action='store_true',
                        help='预演：只计算将新增/将丢弃/既有行是否受影响，不写事件表')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    store_root = Path(args.store_root)
    shard_dir = Path(args.shard_dir)
    coverage = _read_coverage(store_root)
    declared_end = str(coverage.get('end') or '')
    coverage_start = str(coverage.get('start') or args.start or '')

    if args.start:
        start = date.fromisoformat(args.start)
    elif declared_end:
        start = date.fromisoformat(declared_end) + timedelta(days=1)
    else:
        start = date.today().replace(day=1)
    end = date.fromisoformat(args.end) if args.end else date.today()
    if start > end:
        logger.info('覆盖声明已是最新，无需补数')
        return 0
    args.start, args.end = start.isoformat(), end.isoformat()

    chunks = _month_chunks(start, end)
    logger.info(
        f'当前覆盖声明: {coverage_start} ~ {declared_end or "无"}；'
        f'待补区间: {args.start} ~ {args.end}；分片 {len(chunks)} 个；'
        f'关键词 {len(args.keywords)} 个'
    )

    if args.import_only:
        if not args.commit and not args.dry_run:
            logger.info('dry-run：加 --dry-run 预演统计，或加 --commit 真正写入事件表')
            return 0
        return _import_shards(
            args, shard_dir, coverage_start, declared_end)

    if not args.allow_remote:
        logger.info(
            'dry-run：加 --allow-remote 才会联网抓取；'
            '已有分片可用 --import-only --commit 导入'
        )
        return 0

    _fetch_shards(args, chunks, shard_dir)
    logger.info('分片抓取完成；确认无误后执行 --import-only --commit 入库')
    return 0


if __name__ == '__main__':
    sys.exit(main())
