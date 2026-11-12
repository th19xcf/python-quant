#!/usr/bin/env python3
"""备份镜像：只增不删。

备份范围四类中的 ② ``data/local_store/**`` 与 ③ ``reports/**`` 按「相对路径集合差」
做镜像，判定口径：

- 仅源侧存在 → ``copy2`` 补齐，并逐文件 SHA256 校验；
- 仅备份侧存在 → **只输出清单，绝不删除**。

字节码产物（``__pycache__`` 目录下的任何文件、``*.pyc``、``*.pyo``）不参与集合差：
两侧都不计入存量与补缺，也不进「仅备份存在」清单。依据是项目规则「比对与 ``copy2``
一律跳过 ``__pycache__/``、``*.pyc``、``*.pyo``」——这类文件由解释器重新生成，进备份
只占体积、并会在完整性核对里制造假差异（实测备份侧曾因此累积 12 个目录 / 66 个 ``.pyc``）。

红线约束：本模块不含任何删除实现（无 ``unlink`` / ``remove`` / ``rmtree``），也不调用
git-clean 之类的外部清理命令。备份侧独有文件意味着「源侧已删、备份侧还在」，
必须由人工核对是否为唯一副本后另行裁定，工具不得替代该判断。

用法::

    python tools/mirror_backup.py --dry-run
    python tools/mirror_backup.py
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DST = Path(r'G:\git\python-quant')
DEFAULT_SUBDIRS = ('data/local_store', 'reports')
EXCLUDED_DIR = '__pycache__'
EXCLUDED_SUFFIXES = ('.pyc', '.pyo')


def is_bytecode_artifact(rel: str) -> bool:
    """相对路径（posix）是否属于字节码产物：``__pycache__`` 目录内，或 ``.pyc`` / ``.pyo``。"""
    if EXCLUDED_DIR in rel.split('/'):
        return True
    return rel.lower().endswith(EXCLUDED_SUFFIXES)


def sha256(path: Path) -> str:
    """分块计算文件 SHA256（大文件不整体读入内存）。"""
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def rel_files(root: Path) -> set[str]:
    """返回 root 下所有文件的相对路径（posix 分隔），跳过字节码产物。"""
    if not root.is_dir():
        return set()
    out: set[str] = set()
    for path in root.rglob('*'):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        if is_bytecode_artifact(rel):
            continue
        out.add(rel)
    return out


@dataclass
class MirrorResult:
    """单个子目录的镜像结果。"""

    subdir: str
    src_count: int = 0
    dst_count: int = 0
    copied: list[str] = field(default_factory=list)
    extra: list[str] = field(default_factory=list)
    mismatch: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.mismatch


def plan(src_root: Path, dst_root: Path) -> tuple[list[str], list[str]]:
    """返回 (仅源侧存在, 仅备份侧存在) 两个相对路径清单，均为排序后的列表。"""
    src_set = rel_files(src_root)
    dst_set = rel_files(dst_root)
    return sorted(src_set - dst_set), sorted(dst_set - src_set)


def mirror_subdir(
    subdir: str, *, src_base: Path, dst_base: Path, dry_run: bool = False
) -> MirrorResult:
    """镜像一个子目录：补缺 + SHA256 校验；备份侧独有文件只登记不删除。"""
    src_root = src_base / subdir
    dst_root = dst_base / subdir
    missing, extra = plan(src_root, dst_root)
    result = MirrorResult(
        subdir=subdir,
        src_count=len(rel_files(src_root)),
        dst_count=len(rel_files(dst_root)),
        extra=extra,
    )

    for rel in missing:
        if dry_run:
            result.copied.append(rel)
            continue
        target = dst_root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_root / rel, target)
        if sha256(src_root / rel) != sha256(target):
            result.mismatch.append(rel)
        else:
            result.copied.append(rel)

    if not dry_run:
        for rel in sorted(rel_files(src_root) & rel_files(dst_root)):
            if sha256(src_root / rel) != sha256(dst_root / rel):
                shutil.copy2(src_root / rel, dst_root / rel)
                if sha256(src_root / rel) != sha256(dst_root / rel):
                    result.mismatch.append(rel)
    return result


def run(
    *, src_base: Path = ROOT, dst_base: Path = DEFAULT_DST,
    subdirs: tuple[str, ...] = DEFAULT_SUBDIRS, dry_run: bool = False,
) -> list[MirrorResult]:
    results = []
    for subdir in subdirs:
        result = mirror_subdir(subdir, src_base=src_base, dst_base=dst_base, dry_run=dry_run)
        results.append(result)
        print(
            f'{subdir}: 源 {result.src_count} 备份 {result.dst_count} '
            f'补缺 {len(result.copied)} 仅备份存在 {len(result.extra)} '
            f'SHA256 不一致 {len(result.mismatch)}'
        )
        for rel in result.extra:
            print(f'  仅备份存在（未删除，需人工裁定）: {subdir}/{rel}')
        for rel in result.mismatch:
            print(f'  MISMATCH {subdir}/{rel}')
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='备份镜像（只增不删）')
    parser.add_argument('--src', type=Path, default=ROOT, help='源仓库根')
    parser.add_argument('--dst', type=Path, default=DEFAULT_DST, help='备份仓库根')
    parser.add_argument('--sub', action='append', dest='subdirs', help='待镜像子目录，可重复')
    parser.add_argument('--dry-run', action='store_true', help='只输出计划，不复制')
    args = parser.parse_args(argv)

    results = run(
        src_base=args.src,
        dst_base=args.dst,
        subdirs=tuple(args.subdirs) if args.subdirs else DEFAULT_SUBDIRS,
        dry_run=args.dry_run,
    )
    return 0 if all(r.ok for r in results) else 1


if __name__ == '__main__':
    sys.exit(main())
