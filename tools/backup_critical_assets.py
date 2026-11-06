#!/usr/bin/env python3

r"""不可重建数据资产备份 + SHA256 校验。

项目里有一批资产**代码无法重建**（见 ``docs/主机迁移本地数据清单.md``）：
名称码表、分红快照、ST/行业历史日快照、推荐状态，以及 P0-1 新增的
契约巡检基线。它们全部落在被 ``.gitignore`` 排除的目录里，此前只依赖
手工 ``robocopy`` 全量镜像，存在单点丢失风险。

本工具把它们按原相对路径复制到一个独立目录，并生成 ``manifest.json``
（路径 / 字节数 / sha256 / mtime）；``--verify`` 逐项复算哈希。
只做复制与校验，**绝不删除任何文件**。

典型用法::

    python tools/backup_critical_assets.py --dest E:/git/python-quant-critical
    python tools/backup_critical_assets.py --dest E:/git/python-quant-critical --verify
    python tools/backup_critical_assets.py --dry-run   # 只看清单与体积

生产环境请把 ``--dest`` 指向**另一块物理盘或另一台机器**；默认的
``backups/critical_assets`` 只是同盘副本，用于本地快速回滚。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DEST = PROJECT_ROOT / 'backups' / 'critical_assets'

#: 单个文件（相对项目根）
CRITICAL_FILES: tuple[str, ...] = (
    'data/local_store/meta/names.json',
    'data/local_store/factors/stock_dividend.parquet',
    'data/local_store/meta/anomalies/quality_exemptions.csv',
    'data/local_store/meta/events/coverage.json',
    'reports/advice/state/long.json',
    'reports/advice/state/mid.json',
    'reports/advice/state/short.json',
)

#: 整目录（相对项目根），保留目录内相对结构
CRITICAL_DIRS: tuple[str, ...] = (
    'data/local_store/meta/st_status',
    'data/local_store/meta/industry',
    'data/local_store/meta/clean',
)

MANIFEST_NAME = 'manifest.json'


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def collect_assets(root: Path) -> list[Path]:
    """展开清单，返回存在的源文件绝对路径（稳定排序）。"""
    found: set[Path] = set()
    for relative in CRITICAL_FILES:
        path = root / relative
        if path.is_file():
            found.add(path)
        else:
            logger.warning(f'不可重建资产缺失: {relative}')
    for relative in CRITICAL_DIRS:
        directory = root / relative
        if not directory.is_dir():
            logger.warning(f'不可重建资产目录缺失: {relative}')
            continue
        found.update(path for path in directory.rglob('*') if path.is_file())
    return sorted(found)


def _entry(root: Path, path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        'relative_path': path.relative_to(root).as_posix(),
        'bytes': stat.st_size,
        'sha256': _sha256(path),
        'mtime': datetime.fromtimestamp(stat.st_mtime).isoformat(timespec='seconds'),
    }


def backup(root: Path, dest: Path) -> dict[str, Any]:
    """复制资产并写 manifest，返回清单载荷。"""
    assets = collect_assets(root)
    if not assets:
        raise RuntimeError('没有找到任何不可重建资产，拒绝生成空备份')
    dest.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, Any]] = []
    for path in assets:
        relative = path.relative_to(root)
        target = dest / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
        entries.append(_entry(root, path))
    payload = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'source_root': str(root),
        'file_count': len(entries),
        'total_bytes': sum(item['bytes'] for item in entries),
        'files': entries,
    }
    (dest / MANIFEST_NAME).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8'
    )
    return payload


def verify(root: Path, dest: Path) -> tuple[int, int, list[str]]:
    """按 manifest 复算哈希；返回 (通过数, 问题数, 问题描述)。"""
    manifest = dest / MANIFEST_NAME
    if not manifest.is_file():
        return 0, 1, [f'manifest 缺失: {manifest}']
    payload = json.loads(manifest.read_text(encoding='utf-8'))
    problems: list[str] = []
    passed = 0
    for item in payload.get('files', []):
        relative = str(item['relative_path'])
        source = root / relative
        target = dest / relative
        if not target.is_file():
            problems.append(f'备份文件缺失: {relative}')
            continue
        actual = _sha256(target)
        if actual != item['sha256']:
            problems.append(f'备份哈希不符: {relative}')
            continue
        if source.is_file() and _sha256(source) != actual:
            problems.append(f'源与备份不一致（源已更新，需重跑备份）: {relative}')
            continue
        passed += 1
    return passed, len(problems), problems


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='不可重建数据资产备份与校验')
    parser.add_argument('--dest', default=str(DEFAULT_DEST),
                        help='备份目标目录（建议指向另一块盘）')
    parser.add_argument('--source-root', default=str(PROJECT_ROOT))
    parser.add_argument('--verify', action='store_true', help='只校验，不复制')
    parser.add_argument('--dry-run', action='store_true', help='只列出清单与体积')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    root = Path(args.source_root).resolve()
    dest = Path(args.dest).resolve()

    if args.verify:
        passed, failed, problems = verify(root, dest)
        logger.info(f'校验完成: 通过 {passed} 项，问题 {failed} 项')
        for problem in problems:
            logger.error(problem)
        return 1 if failed else 0

    assets = collect_assets(root)
    total = sum(path.stat().st_size for path in assets)
    logger.info(f'不可重建资产 {len(assets)} 个文件，合计 {total / 1e6:.2f} MB → {dest}')
    if args.dry_run:
        for path in assets:
            logger.info(f'  {path.relative_to(root).as_posix()}')
        logger.info('dry-run：未复制')
        return 0

    payload = backup(root, dest)
    logger.info(f'备份完成: {payload["file_count"]} 个文件，manifest: {dest / MANIFEST_NAME}')
    passed, failed, problems = verify(root, dest)
    for problem in problems:
        logger.warning(problem)
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
