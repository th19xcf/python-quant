#!/usr/bin/env python3

r"""归档副本盘点（**严格只读，不删除、不移动、不重命名任何文件**）。

项目里仓库根 ``_backup/`` 约 6.3GB、``reports`` 约 1.8GB，其中大量是
历次重建留下的快照与 pytest 临时目录。直接 ``rm`` 风险很高：名称码表、
分红快照、ST/行业历史快照等都属于**不可重建资产**，必须先确认备份可用。

本工具只做三件事：

1. 扫描归档目录，按目录汇总文件数与体积；
2. 对同体积文件做 SHA256 抽样比对，列出重复组并给出「保留最新、其余可评估」
   的建议清单与可释放体积估算；
3. 列出超过 ``--max-age-days`` 未修改的候选文件。

输出仅为 JSON 报告 + 日志摘要，供人工确认后再另行决策。工具本身**没有**
``--delete`` / ``--apply`` 参数，也不会调用任何删除 API。

典型用法::

    python tools/cleanup_archives.py                         # 摘要打印
    python tools/cleanup_archives.py --output artifacts/archive_audit.json
    python tools/cleanup_archives.py --no-hash               # 跳过哈希，仅统计
    python tools/cleanup_archives.py --scan _backup --scan reports/audit_2026-11-06
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]

#: 默认扫描范围（相对项目根）；``*`` 交给 glob 展开。
DEFAULT_SCANS: tuple[str, ...] = (
    '_backup',
    'reports/audit_*',
    'reports/*_2026*',
)

#: 永不纳入候选清单的路径前缀（相对项目根，POSIX 风格）。
PROTECTED_PREFIXES: tuple[str, ...] = (
    'backups/',                       # P0 不可重建资产备份
    'data/local_store/meta/',         # 元数据与权威快照
    'data/local_store/factors/',
    'data/local_store/quotes/',
    '.git/',
)

#: 单个文件超过该体积就不参与哈希（避免为了查重读 1GB+ 报告）。
DEFAULT_MAX_HASH_MB = 64
#: 哈希总字节预算，超过后停止新文件的哈希（已产生的分组仍然有效）。
DEFAULT_HASH_BUDGET_MB = 512


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _relative(path: Path) -> str:
    try:
        return path.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def _is_protected(relative: str) -> bool:
    # 目录相对路径通常不带尾斜杠（例如 ``backups``），统一补齐后再匹配，
    # 否则 ``backups/critical_assets`` 这种目录会被误当成可清理对象。
    normalized = relative.rstrip('/') + '/'
    return any(normalized.startswith(prefix) for prefix in PROTECTED_PREFIXES)


def expand_scans(specs: Iterable[str], root: Path) -> list[Path]:
    """把扫描表达式展开成实际存在的目录（去重、稳定排序）。"""
    found: set[Path] = set()
    for spec in specs:
        for candidate in sorted(root.glob(spec)) if any(c in spec for c in '*?[') else [root / spec]:
            if candidate.is_dir() and not _is_protected(_relative(candidate)):
                found.add(candidate.resolve())
    return sorted(found)


def scan_files(scan_dirs: Iterable[Path]) -> list[dict[str, Any]]:
    """收集文件元数据；遇到无权限/损坏链接只记录，不中断。"""
    files: list[dict[str, Any]] = []
    seen: set[str] = set()
    for directory in scan_dirs:
        for dirpath, dirnames, filenames in os.walk(directory):
            dirnames.sort()
            for name in sorted(filenames):
                path = Path(dirpath) / name
                try:
                    stat = path.stat()
                except OSError as exc:
                    logger.warning(f'[archive] 跳过无法 stat 的文件 {path}: {exc}')
                    continue
                key = str(path.resolve())
                if key in seen:
                    continue
                seen.add(key)
                relative = _relative(path)
                files.append({
                    'path': relative,
                    'bytes': stat.st_size,
                    'mtime': datetime.fromtimestamp(stat.st_mtime).isoformat(timespec='seconds'),
                    'protected': _is_protected(relative),
                    'group': _relative(directory),
                })
    return files


def find_duplicates(files: list[dict[str, Any]], *, root: Path,
                    max_hash_mb: float, budget_mb: float,
                    enabled: bool = True) -> tuple[list[dict[str, Any]], int]:
    """按「同体积 → SHA256」找重复组；返回 (重复组, 可释放字节数)。

    体积相同的文件才做哈希，且受单文件上限与总字节预算约束；超预算时
    停止新增哈希，宁可少报也不把一次只读盘点变成全盘 IO。
    """
    if not enabled:
        return [], 0
    by_size: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for item in files:
        if not item['protected'] and item['bytes'] > 0:
            by_size[item['bytes']].append(item)

    budget = int(budget_mb * 1024 * 1024)
    max_bytes = int(max_hash_mb * 1024 * 1024)
    used = 0
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    truncated = False
    for size, candidates in sorted(by_size.items()):
        if len(candidates) < 2:
            continue
        if size > max_bytes:
            truncated = True
            continue
        for item in candidates:
            if used + size > budget:
                truncated = True
                break
            try:
                digest = _sha256(root / item['path'])
            except OSError as exc:
                logger.warning(f'[archive] 跳过无法哈希的文件 {item["path"]}: {exc}')
                continue
            used += size
            groups[f'{size}:{digest}'].append(item)

    duplicates: list[dict[str, Any]] = []
    reclaimable = 0
    for key, members in sorted(groups.items(), key=lambda kv: -kv[1][0]['bytes'] * (len(kv[1]) - 1)):
        if len(members) < 2:
            continue
        ordered = sorted(members, key=lambda m: m['mtime'], reverse=True)
        reclaimable += ordered[0]['bytes'] * (len(ordered) - 1)
        duplicates.append({
            'digest': key.split(':', 1)[1],
            'bytes_each': ordered[0]['bytes'],
            'count': len(ordered),
            'keep': ordered[0]['path'],
            'review': [m['path'] for m in ordered[1:]],
        })
    if truncated:
        logger.warning('[archive] 哈希预算/单文件上限已触发，重复组可能不完整（只读盘点，不放宽限制）')
    return duplicates, reclaimable


def build_report(scan_dirs: list[Path], *, root: Path, max_age_days: int,
                 max_hash_mb: float, budget_mb: float,
                 hash_enabled: bool) -> dict[str, Any]:
    files = scan_files(scan_dirs)
    duplicates, reclaimable = find_duplicates(
        files, root=root, max_hash_mb=max_hash_mb,
        budget_mb=budget_mb, enabled=hash_enabled)

    cutoff = datetime.now() - timedelta(days=max_age_days)
    aged = [
        {'path': f['path'], 'bytes': f['bytes'], 'mtime': f['mtime']}
        for f in files
        if not f['protected'] and datetime.fromisoformat(f['mtime']) < cutoff
    ]
    aged.sort(key=lambda item: -item['bytes'])

    by_group: dict[str, dict[str, int]] = defaultdict(lambda: {'files': 0, 'bytes': 0})
    for item in files:
        bucket = by_group[item['group']]
        bucket['files'] += 1
        bucket['bytes'] += item['bytes']

    return {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'project_root': str(root),
        'readonly': True,
        'scan_dirs': [_relative(d) for d in scan_dirs],
        'protected_prefixes': list(PROTECTED_PREFIXES),
        'max_age_days': max_age_days,
        'hash_enabled': hash_enabled,
        'file_count': len(files),
        'total_bytes': sum(f['bytes'] for f in files),
        'by_group': dict(sorted(by_group.items())),
        'protected_file_count': sum(1 for f in files if f['protected']),
        'duplicate_groups': duplicates,
        'duplicate_reclaimable_bytes': reclaimable,
        'aged_candidates': aged,
        'aged_bytes': sum(item['bytes'] for item in aged),
        'notice': '本报告为只读盘点结果；工具不删除/移动/重命名任何文件，删除需人工逐项确认。',
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='归档副本只读盘点（不删除任何文件）')
    parser.add_argument('--root', default=str(PROJECT_ROOT), help='项目根目录')
    parser.add_argument('--scan', action='append', default=None,
                        help='扫描目录（相对项目根，可重复；缺省用内置清单）')
    parser.add_argument('--max-age-days', type=int, default=30,
                        help='超过该天数未修改的文件列入候选（默认 30）')
    parser.add_argument('--max-hash-mb', type=float, default=DEFAULT_MAX_HASH_MB,
                        help='单文件哈希体积上限 MB（默认 64）')
    parser.add_argument('--hash-budget-mb', type=float, default=DEFAULT_HASH_BUDGET_MB,
                        help='哈希总字节预算 MB（默认 512）')
    parser.add_argument('--no-hash', action='store_true', help='跳过哈希，仅做体积/年龄统计')
    parser.add_argument('--output', default='', help='可选：把完整 JSON 报告写到该路径')
    parser.add_argument('--top', type=int, default=10, help='日志里展示的重复组数量')
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    root = Path(args.root).resolve()
    specs = args.scan or DEFAULT_SCANS
    scan_dirs = expand_scans(specs, root)
    if not scan_dirs:
        logger.warning(f'[archive] 没有匹配到任何扫描目录：{list(specs)}')
        return 0

    report = build_report(
        scan_dirs, root=root, max_age_days=args.max_age_days,
        max_hash_mb=args.max_hash_mb, budget_mb=args.hash_budget_mb,
        hash_enabled=not args.no_hash)

    mb = 1024 * 1024
    logger.info(
        f'[archive] 扫描 {len(scan_dirs)} 个目录，{report["file_count"]} 个文件，'
        f'{report["total_bytes"] / mb:.1f} MB；受保护文件 {report["protected_file_count"]} 个'
    )
    for group, stats in report['by_group'].items():
        logger.info(f'[archive]   {group}: {stats["files"]} 个 / {stats["bytes"] / mb:.1f} MB')
    logger.info(
        f'[archive] 重复组 {len(report["duplicate_groups"])} 个，'
        f'理论可释放 {report["duplicate_reclaimable_bytes"] / mb:.1f} MB；'
        f'超期候选 {len(report["aged_candidates"])} 个 / '
        f'{report["aged_bytes"] / mb:.1f} MB'
    )
    for group in report['duplicate_groups'][:args.top]:
        logger.info(
            f'[archive] 重复 {group["count"]} 份 × {group["bytes_each"] / mb:.2f} MB；'
            f'建议保留 {group["keep"]}；待人工确认 {group["review"][:3]}'
        )

    if args.output:
        out = Path(args.output)
        if not out.is_absolute():
            out = root / out
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        logger.info(f'[archive] 报告已写入 {out}')

    logger.warning('[archive] 只读盘点完成：未删除、移动或重命名任何文件；删除须人工逐项确认')
    return 0


if __name__ == '__main__':
    sys.exit(main())
