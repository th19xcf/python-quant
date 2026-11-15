#!/usr/bin/env python3
"""备份仓根目录的唯一解析入口（文档与代码一律不写死盘符）。

解析顺序（命中即止，不做候选盘符探测）::

1. 环境变量 ``QUANT_BACKUP_ROOT``；
2. 仓库内本机配置文件 ``.trae/backup.local.json``，内容形如
   ``{"backup_root": "X:/git/python-quant"}``（该文件被 ``.gitignore`` 排除，各机器各配各的）；
3. 两者都没有 → 抛 :class:`BackupRootError`。

解析成功后必须通过三项校验，任一不满足即抛 :class:`BackupRootError` 并停止：

- 目录存在（**不自动创建**，避免把备份写到空目录还以为同步成功）；
- 目录内含 ``.git``；
- ``git -C <root> remote get-url origin`` 指向 ``th19xcf/python-quant``。

校验失败不得降级为「跳过备份」。

用法::

    python tools/backup_root.py            # 打印解析来源、路径与校验详情
    python tools/backup_root.py --quiet    # 只打印路径，供脚本消费
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_VAR = 'QUANT_BACKUP_ROOT'
CONFIG_PATH = ROOT / '.trae' / 'backup.local.json'
CONFIG_KEY = 'backup_root'
EXPECTED_REMOTE = 'th19xcf/python-quant'


class BackupRootError(RuntimeError):
    """备份仓根目录解析或校验失败（不降级、不自动创建目录）。"""


def _candidate_from_env() -> tuple[Path, str] | None:
    raw = os.environ.get(ENV_VAR, '').strip()
    if not raw:
        return None
    return Path(raw), f'环境变量 {ENV_VAR}'


def _candidate_from_config() -> tuple[Path, str] | None:
    if not CONFIG_PATH.is_file():
        return None
    try:
        data = json.loads(CONFIG_PATH.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        raise BackupRootError(f'本机配置文件无法解析：{CONFIG_PATH}（{exc}）') from exc
    raw = data.get(CONFIG_KEY) if isinstance(data, dict) else None
    if not isinstance(raw, str) or not raw.strip():
        raise BackupRootError(f'本机配置文件缺少 {CONFIG_KEY} 字段：{CONFIG_PATH}')
    return Path(raw.strip()), f'配置文件 {CONFIG_PATH}'


def _resolve_candidate() -> tuple[Path, str]:
    """按「环境变量 → 本机配置文件」取候选路径与来源，取不到即报错。"""
    candidate = _candidate_from_env() or _candidate_from_config()
    if candidate is None:
        raise BackupRootError(
            '未找到备份仓路径：请设置环境变量 '
            f'{ENV_VAR}，或创建本机配置文件 {CONFIG_PATH}'
            f'（内容：{{"{CONFIG_KEY}": "<备份仓绝对路径>"}}）'
        )
    return candidate


def _normalize(path: Path) -> Path:
    """展开 ``~``；相对路径按仓库根解析；不创建任何目录。"""
    resolved = path.expanduser()
    if not resolved.is_absolute():
        resolved = ROOT / resolved
    return resolved.resolve()


def validate_backup_root(root: Path) -> str:
    """校验目录存在、含 ``.git``、origin 指向预期仓库；返回 origin URL。"""
    if not root.is_dir():
        raise BackupRootError(f'备份仓根目录不存在：{root}（不会自动创建）')
    if not (root / '.git').exists():
        raise BackupRootError(f'备份仓根目录不是 git 仓库（缺 .git）：{root}')
    try:
        proc = subprocess.run(
            ['git', '-C', str(root), 'remote', 'get-url', 'origin'],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            check=False,
        )
    except OSError as exc:
        raise BackupRootError(f'无法执行 git 读取 origin：{exc}') from exc
    if proc.returncode != 0:
        detail = proc.stderr.strip() or 'git remote get-url origin 失败'
        raise BackupRootError(f'备份仓未配置 origin：{root}（{detail}）')
    url = proc.stdout.strip()
    if EXPECTED_REMOTE.lower() not in url.lower():
        raise BackupRootError(
            f'备份仓 origin 不是 {EXPECTED_REMOTE}：{root} → {url}')
    return url


def resolve_backup_root_with_source() -> tuple[Path, str]:
    """解析并校验备份仓根目录，返回 ``(根目录, 解析来源)``；失败抛 BackupRootError。"""
    path, source = _resolve_candidate()
    root = _normalize(path)
    validate_backup_root(root)
    return root, source


def resolve_backup_root() -> Path:
    """解析并校验备份仓根目录；失败抛 BackupRootError。"""
    return resolve_backup_root_with_source()[0]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='解析并校验备份仓根目录（唯一入口）')
    parser.add_argument('--quiet', action='store_true', help='只输出解析出的路径')
    args = parser.parse_args(argv)

    try:
        path, source = _resolve_candidate()
        root = _normalize(path)
        origin = validate_backup_root(root)
    except BackupRootError as exc:
        print(f'备份仓解析失败：{exc}', file=sys.stderr)
        return 1

    if args.quiet:
        print(root)
        return 0
    print(f'备份仓根目录：{root}')
    print(f'解析来源：{source}')
    print(f'校验：目录存在=是 .git=是 origin={origin}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
