#!/usr/bin/env python3

r"""跨机同步：只增量复制「代码无法重建」的非 Git 资产，永不删除。

三种范围（``--scope``）：

- ``daily``：日常 17 组每日路径，约 28 MiB，用于每天两台机器互通；
- ``package``：U 盘增量包，= daily + ``meta/research_inputs`` +
  ``external/tdx_hq_cache`` + ``reports`` 下当日新增产物（默认跳过体积大的
  可重建中间件），配合 ``--since`` 使用；
- ``seed``：新机一次性种子（quotes / financials / research_inputs）。

判据（``package`` 与 ``daily`` 一致）：满足任一即复制——
① 源 mtime ≥ ``--since``；② 目标缺失；③ 目标大小不同。
单文件 ``copy2`` 保留 mtime，二次运行天然幂等。

安全约束：脚本内没有任何 ``remove`` / ``unlink`` / ``rmtree`` 调用，
目标端多余文件一律保留。默认 dry-run，加 ``--apply`` 才落盘。

典型用法::

    # 每日增量（双向皆可，先看后传）
    .venv\Scripts\python.exe tools\sync_daily_non_git.py ^
        --source E:\code\python\quant --dest \\HOME-PC\quant\code\python\quant

    # U 盘增量包（分界时点之后）
    .venv\Scripts\python.exe tools\sync_daily_non_git.py --scope package ^
        --since "2026-11-09 07:00" --dest G:\sync\quant --apply ^
        --manifest G:\sync\quant\manifest.csv

    # 校验
    .venv\Scripts\python.exe tools\sync_daily_non_git.py --scope package ^
        --since "2026-11-09 07:00" --dest G:\sync\quant --verify --sample 50
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

#: 每日必传目录（相对项目根，递归）。判定依据见 docs/跨机每日同步清单.md。
DAILY_DIRS: tuple[str, ...] = (
    # 每日一快照、历史日期无法补写
    'data/local_store/meta/st_status',
    'data/local_store/meta/industry',
    'data/local_store/meta/clean',
    # 外部抓取的历史 PIT 证据，重抓只会得到「当前修订」
    'data/local_store/meta/st',
    'data/local_store/meta/industry_pit',
    'data/local_store/meta/index_membership',
    # 指数成分权威表（index_weight_monthly + 派生的 membership_intervals）
    'data/local_store/meta/index_members',
    # 人工维护/校准结论，两边各跑会分叉
    'data/local_store/meta/universe',
    'data/local_store/meta/pools',
    'data/local_store/meta/anomalies',
    'data/local_store/meta/trading_calendar',
    'data/local_store/meta/factors',
    # 派生表，但体积小且重建慢，顺带同步省一次重算
    'data/local_store/meta/suspension',
    'data/local_store/meta/breadth',
    'data/local_store/meta/events',
    'data/local_store/factors/rebuild',
    # 推荐持仓状态，丢了只能空仓重开
    'reports/advice/state',
)

#: 每日必传单文件（相对项目根）。
DAILY_FILES: tuple[str, ...] = (
    'data/local_store/meta/names.json',
    'data/local_store/meta/daily_index.parquet',
    'data/local_store/meta/state.json',
    'data/local_store/meta/stock_basic.parquet',
    'data/local_store/meta/index_basic.parquet',
    'data/local_store/meta/fund_basic.parquet',
    'data/local_store/factors/stock_dividend.parquet',
    'data/local_store/factors/fund_dividend.parquet',
    'data/local_store/factors/fund_share_change.parquet',
    'data/local_store/factors/meta.json',
    'data/local_store/factors/fund_meta.json',
    'data/local_store/factors/covered_codes.txt',
    'data/local_store/factors/fund_covered_codes.txt',
)

#: 每日必传「非递归」目录：只取其直属文件（``reports/advice`` 下的
#: ``calibration/`` 有 397 MB 校准面板，属可重算产物，不能整目录带走）。
DAILY_FLAT_DIRS: tuple[str, ...] = ('reports/advice',)

#: U 盘增量包额外纳入的目录（在 17 组每日路径之外）。
PACKAGE_EXTRA_DIRS: tuple[str, ...] = (
    'data/local_store/meta/research_inputs',
    'external/tdx_hq_cache',
)

#: reports 下永远排除的目录（可重算的大体积校准面板）。
REPORT_SKIP_ALWAYS = frozenset({'reports/advice/calibration'})

#: reports 单个目录「本次需传字节」超过该阈值即视为重体积可重建中间件。
HEAVY_REPORT_BYTES = 50 * 1024 * 1024

#: 包内固定携带的说明/校验文件，回灌时必须跳过。
PACKAGE_AUX_FILES = frozenset({'manifest.csv', 'README.md', 'restore.py'})

#: 一次性种子目录：新机首次灌，日常不传。
SEED_DIRS: tuple[str, ...] = (
    'data/local_store/quotes',
    'data/local_store/meta/financials',
    'data/local_store/meta/research_inputs',
)

#: 任何范围都跳过的目录名 / 文件模式。
#: ``_staging`` 已迁到仓库根（``<仓库根>/_staging``，见 ``src/data/staging.py``），
#: 按目录名跳过同样适用于新位置；``data/local_store/_staging`` 若有历史残留也不再同步。
SKIP_DIR_NAMES = frozenset({'_backup', '_staging', '__pycache__', '.ipynb_checkpoints'})
SKIP_SUFFIXES = ('.tmp', '.temp', '.log')

#: 目标端与源端 mtime 允许的抖动（秒）。FAT/网络盘时间戳精度较粗。
MTIME_TOLERANCE = 2


@dataclass(frozen=True)
class Pending:
    """一个待传输文件。"""

    src: Path
    relative: str
    size: int
    mtime: float


def parse_since(text: str) -> float:
    """解析 ``2026-11-09 07:00`` / ``2026-11-09 07:00:00`` / ``2026-11-09``。"""
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
        try:
            return datetime.strptime(text, fmt).timestamp()
        except ValueError:
            continue
    raise SystemExit(f'无法解析 --since: {text}（示例：2026-11-09 07:00）')


def _make(src: Path, root: Path) -> Pending:
    stat = src.stat()
    return Pending(
        src=src,
        relative=src.relative_to(root).as_posix(),
        size=stat.st_size,
        mtime=stat.st_mtime,
    )


def _iter_dir(root: Path, relative: str, *, recursive: bool) -> list[Pending]:
    base = root / relative
    if not base.is_dir():
        return []
    items: list[Pending] = []
    walker = base.rglob('*') if recursive else base.glob('*')
    for path in walker:
        if not path.is_file():
            continue
        if any(part in SKIP_DIR_NAMES for part in path.relative_to(base).parts[:-1]):
            continue
        if path.suffix.lower() in SKIP_SUFFIXES:
            continue
        items.append(_make(path, root))
    return items


def _iter_file(root: Path, relative: str) -> list[Pending]:
    path = root / relative
    return [_make(path, root)] if path.is_file() else []


def _daily_items(root: Path) -> list[Pending]:
    items: list[Pending] = []
    for relative in DAILY_DIRS:
        items.extend(_iter_dir(root, relative, recursive=True))
    for relative in DAILY_FLAT_DIRS:
        items.extend(_iter_dir(root, relative, recursive=False))
    for relative in DAILY_FILES:
        items.extend(_iter_file(root, relative))
    return items


def _reports_items(
    root: Path, since: float, include_heavy: bool
) -> tuple[list[Pending], list[tuple[str, int, int]]]:
    """只取分界点之后有写入的 reports 目录；返回 (文件, 跳过的重目录)。"""
    reports = root / 'reports'
    if not reports.is_dir():
        return [], []
    items: list[Pending] = []
    skipped: list[tuple[str, int, int]] = []
    for entry in sorted(reports.iterdir()):
        if entry.is_file():
            if entry.suffix.lower() not in SKIP_SUFFIXES and entry.stat().st_mtime >= since:
                items.append(_make(entry, root))
            continue
        relative = entry.relative_to(root).as_posix()
        if relative in REPORT_SKIP_ALWAYS:
            continue
        changed: list[Pending] = []
        for path in entry.rglob('*'):
            if not path.is_file() or path.suffix.lower() in SKIP_SUFFIXES:
                continue
            if any(part in SKIP_DIR_NAMES for part in path.relative_to(entry).parts[:-1]):
                continue
            item = _make(path, root)
            if item.mtime >= since:
                changed.append(item)
        if not changed:
            continue
        size = sum(item.size for item in changed)
        if size > HEAVY_REPORT_BYTES and not include_heavy:
            skipped.append((relative, len(changed), size))
            continue
        items.extend(changed)
    return items, skipped


def collect(
    root: Path,
    scope: str,
    since: float | None = None,
    include_heavy: bool = False,
    skipped_heavy: list[tuple[str, int, int]] | None = None,
) -> list[Pending]:
    """按范围展开待比对文件（尚未比对目标端）。"""
    if scope == 'daily':
        return _daily_items(root)
    if scope == 'seed':
        items: list[Pending] = []
        for relative in SEED_DIRS:
            items.extend(_iter_dir(root, relative, recursive=True))
        return items
    if since is None:
        raise SystemExit('--scope package 必须同时给 --since')
    items = _daily_items(root)
    for relative in PACKAGE_EXTRA_DIRS:
        items.extend(_iter_dir(root, relative, recursive=True))
    report_items, skipped = _reports_items(root, since, include_heavy)
    items.extend(report_items)
    if skipped_heavy is not None:
        skipped_heavy.extend(skipped)
    return items


def needs_copy(item: Pending, dest_root: Path, since: float | None = None) -> bool:
    """目标缺失 / 大小不同 / 源 mtime 晚于 since 与目标 → 需要复制。"""
    target = dest_root / item.relative
    if not target.is_file():
        return True
    stat = target.stat()
    if stat.st_size != item.size:
        return True
    if item.mtime - stat.st_mtime > MTIME_TOLERANCE:
        return True
    return since is not None and item.mtime >= since


def copy_one(item: Pending, dest_root: Path) -> None:
    target = dest_root / item.relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(item.src, target)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _human(size: float) -> str:
    for unit in ('B', 'KB', 'MB', 'GB'):
        if size < 1024 or unit == 'GB':
            return f'{size:.2f} {unit}'
        size /= 1024
    return f'{size:.2f} GB'


def _summarize(items: list[Pending]) -> dict[str, tuple[int, int]]:
    """按其两级父目录汇总 (文件数, 字节)。"""
    table: dict[str, list[int]] = {}
    for item in items:
        key = '/'.join(item.relative.split('/')[:2])
        bucket = table.setdefault(key, [0, 0])
        bucket[0] += 1
        bucket[1] += item.size
    return {key: (value[0], value[1]) for key, value in sorted(table.items())}


def _print_table(items: list[Pending], title: str) -> None:
    print(title)
    print(f'{"目录":<44}{"文件数":>9}{"体积":>16}')
    for key, (count, size) in _summarize(items).items():
        print(f'{key:<44}{count:>9}{_human(size):>16}')
    print(f'{"合计":<44}{len(items):>9}{_human(sum(i.size for i in items)):>16}')


def write_manifest(path: Path, source: Path, entries: list[Pending]) -> None:
    """合并写 manifest.csv：相对路径 / 字节 / 源 mtime / SHA256。"""
    rows: dict[str, list[str]] = {}
    if path.is_file():
        with path.open('r', encoding='utf-8', newline='') as handle:
            for row in csv.DictReader(handle):
                rows[row['relative_path']] = [
                    row['relative_path'], row['bytes'], row['mtime'], row['sha256']
                ]
    for item in entries:
        rows[item.relative] = [
            item.relative,
            str(item.size),
            datetime.fromtimestamp(item.mtime).strftime('%Y-%m-%d %H:%M:%S'),
            _sha256(source / item.relative),
        ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(['relative_path', 'bytes', 'mtime', 'sha256'])
        for key in sorted(rows):
            writer.writerow(rows[key])


def run_sync(
    source: Path,
    dest: Path,
    scope: str,
    since: float | None,
    apply: bool,
    include_heavy: bool,
    manifest: Path | None,
) -> int:
    skipped: list[tuple[str, int, int]] = []
    items = collect(source, scope, since, include_heavy, skipped)
    pending = [item for item in items if needs_copy(item, dest, since)]
    total_bytes = sum(item.size for item in pending)

    print(f'[scope={scope}] 源={source}  目标={dest}')
    if since is not None:
        print(f'分界时点：{datetime.fromtimestamp(since):%Y-%m-%d %H:%M:%S}')
    print(f'范围内 {len(items)} 个文件 / {_human(sum(i.size for i in items))}')
    print(f'需传输 {len(pending)} 个文件 / {_human(total_bytes)}（{total_bytes} B）')
    print('-' * 78)
    _print_table(pending, '按目录拆分：')
    print('-' * 78)
    if skipped:
        print('已跳过的重体积可重建目录（--include-heavy-reports 可放开）：')
        for relative, count, size in skipped:
            print(f'  {relative:<44}{count:>9}{_human(size):>16}')
        print(f'  小计 {sum(s for _, _, s in skipped)} B')

    if not apply:
        for item in sorted(pending, key=lambda i: -i.size)[:10]:
            print(f'  待传 {item.relative}  {_human(item.size)}')
        print('dry-run：未复制任何文件（加 --apply 执行；本脚本从不删除目标端文件）')
        return 0

    for index, item in enumerate(pending, 1):
        copy_one(item, dest)
        if index % 50 == 0 or index == len(pending):
            print(f'  已复制 {index}/{len(pending)}')
    if manifest is not None:
        write_manifest(manifest, source, pending)
        print(f'manifest 已写入 {manifest}')
    print(f'完成：{len(pending)} 个文件 / {_human(total_bytes)}')
    return 0


def run_verify(source: Path, dest: Path, scope: str, since: float | None,
               sample: int) -> int:
    items = collect(source, scope, since)
    if sample > 0:
        step = max(1, len(items) // sample)
        items = items[::step][:sample]
    passed = 0
    missing: list[str] = []
    mismatched: list[str] = []
    for item in items:
        target = dest / item.relative
        if not target.is_file():
            missing.append(item.relative)
            continue
        if target.stat().st_size != item.size or _sha256(item.src) != _sha256(target):
            mismatched.append(item.relative)
            continue
        passed += 1
    print(f'校验 {len(items)} 个文件：通过 {passed}，缺失 {len(missing)}，'
          f'不一致 {len(mismatched)}')
    for relative in missing[:20]:
        print(f'  缺失 {relative}')
    for relative in mismatched[:20]:
        print(f'  不一致 {relative}')
    return 1 if missing or mismatched else 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='跨机同步不可重建资产（只增量复制，不删除）'
    )
    parser.add_argument('--source', default=str(PROJECT_ROOT),
                        help='源机项目根（默认本仓库根）')
    parser.add_argument('--dest', required=True,
                        help='目标根目录，支持 UNC，如 \\\\HOME-PC\\quant 或 G:\\sync\\quant')
    parser.add_argument('--scope', choices=('daily', 'package', 'seed'),
                        default='daily',
                        help='daily=17 组日常；package=U 盘增量包；seed=新机种子')
    parser.add_argument('--since', default='',
                        help='分界时点，如 "2026-11-09 07:00"（package 必填）')
    parser.add_argument('--apply', action='store_true',
                        help='真正复制；缺省为 dry-run')
    parser.add_argument('--verify', action='store_true',
                        help='只做大小+SHA256 校验，不复制')
    parser.add_argument('--sample', type=int, default=0,
                        help='校验时抽样文件数（0=全量）')
    parser.add_argument('--manifest', default='',
                        help='复制后写入/合并 CSV 清单（相对路径/字节/mtime/SHA256）')
    parser.add_argument('--include-heavy-reports', action='store_true',
                        help='package 范围带上超过 50 MB 的 reports 中间件')
    parser.add_argument('--report', default='',
                        help='可选：把本次计划写成 JSON 文件')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    source = Path(args.source).resolve()
    if not source.is_dir():
        print(f'源目录不存在: {source}', file=sys.stderr)
        return 2
    dest = Path(args.dest)
    if not dest.is_dir():
        print(f'目标目录不存在（请先建空目录或检查 UNC 连通）: {dest}',
              file=sys.stderr)
        return 2
    since = parse_since(args.since) if args.since else None

    if args.verify:
        return run_verify(source, dest, args.scope, since, args.sample)

    if args.report:
        items = collect(source, args.scope, since, args.include_heavy_reports)
        pending = [item for item in items if needs_copy(item, dest, since)]
        payload = {
            'generated_at': datetime.now().isoformat(timespec='seconds'),
            'scope': args.scope,
            'since': args.since,
            'source': str(source),
            'dest': str(dest),
            'file_count': len(pending),
            'total_bytes': sum(item.size for item in pending),
            'files': [item.relative for item in pending],
        }
        Path(args.report).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8'
        )
    manifest = Path(args.manifest) if args.manifest else None
    return run_sync(source, dest, args.scope, since, args.apply,
                    args.include_heavy_reports, manifest)


if __name__ == '__main__':
    sys.exit(main())
