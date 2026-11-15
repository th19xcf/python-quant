#!/usr/bin/env python3
"""提交收尾一条命令：取清单 → 显式路径隔离索引提交 → 备份同步 → 自证。

原先收尾要反复读 ``git status``、判断文件归属、挑路径、隔离索引提交、备份同步、
提交后自证，每轮都在对话里重做一遍，上下文与 token 成本递增。本脚本把这条链固定成
一条命令，默认 dry-run（只打印计划），``--apply`` 才真正写入。

行为要点::

1. 取清单：``git status --porcelain -z --no-renames`` 取已跟踪改动，
   ``git ls-files --others --exclude-standard -z`` 取未跟踪文件，按文件族
   （``.trae/rules`` / ``docs`` / ``tools`` ...）分组展示；
2. 显式提交：只提交 ``--paths`` 命中的路径，未命中的只列出、绝不自动带上；
3. 隔离索引：``GIT_INDEX_FILE`` 临时索引五步——``before = rev-parse HEAD`` →
   ``read-tree before`` 种子化 → ``update-index -z --add --remove --stdin`` →
   ``git diff --cached`` 与预期清单逐条比对 → 提交前复核 HEAD 未变；提交后
   **另起进程**（不带 ``GIT_INDEX_FILE``）执行 ``git read-tree HEAD`` 回填共享索引；
4. 备份同步：经 :func:`tools.backup_root.resolve_backup_root` 解析备份仓，
   ``copy2`` 变更文件、对位删除源端已删文件，逐文件 SHA256 校验，并核对
   「改动清单条数 == 已处理条数」；
5. 自证：``git diff-tree --no-commit-id --name-status -r HEAD`` 与预期清单逐条比对，
   多出或少掉即非零退出；
6. 输出：正常十行以内。

安全边界：不改任何业务逻辑与数据。共享暂存区若已有本次范围外的已暂存条目，
``--apply`` 直接停止——回填 ``read-tree HEAD`` 会清掉它们的暂存状态。

用法::

    python tools/finalize_commit.py --paths tools/finalize_commit.py \
        --message "chore(tools): 收尾脚本"                    # dry-run
    python tools/finalize_commit.py --paths tools/finalize_commit.py \
        --message "chore(tools): 收尾脚本" --apply
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import socket
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.backup_root import BackupRootError, resolve_backup_root  # noqa: E402

FAMILY_ORDER = (
    '.trae/rules',
    '.trae/skills',
    'docs',
    'tools',
    'tests',
    'src',
    'scripts',
    'config',
    'configs',
    'data',
    'reports',
    'logs',
    '.github',
)
PROXY_HOST = '127.0.0.1'
PROXY_PORT = 10808
BOM = b'\xef\xbb\xbf'


class FinalizeError(RuntimeError):
    """收尾流程中的可预期失败（用法、校验、自证、备份），main 捕获后非零退出。"""


@dataclass(frozen=True)
class Change:
    """一条工作区改动：``status`` 取 A/M/D/?，``path`` 为相对仓库根的 posix 路径。"""

    status: str
    path: str


def _git(
    args: list[str],
    *,
    cwd: Path = ROOT,
    env: dict[str, str] | None = None,
    check: bool = True,
    stdin_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """执行 git（关闭路径转义），``check`` 为真时非零退出即抛 FinalizeError。"""
    command = ['git', '-c', 'core.quotepath=false', '-C', str(cwd), *args]
    proc = subprocess.run(
        command,
        input=stdin_text,
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='surrogateescape',
        env=env,
        check=False,
    )
    if check and proc.returncode != 0:
        detail = (proc.stderr or proc.stdout).strip()
        raise FinalizeError(f'git {" ".join(args)} 失败：{detail}')
    return proc


def _nul_items(text: str) -> list[str]:
    """按 NUL 切分 git ``-z`` 输出，丢掉空串。"""
    return [item for item in text.split('\0') if item]


def _status_letter(code: str) -> str:
    """把 porcelain 的两位状态码归一成 A/M/D/?（? 表示未跟踪，与已 add 的 A 区分）。"""
    if code == '??':
        return '?'
    if 'D' in code:
        return 'D'
    if 'A' in code:
        return 'A'
    return 'M'


def list_changes() -> list[Change]:
    """返回工作区全部改动（已跟踪 + 未跟踪），路径不做八进制转义。"""
    tracked = _git(
        ['status', '--porcelain=v1', '-z', '--no-renames', '--untracked-files=no']
    ).stdout
    changes = [Change(_status_letter(item[:2]), item[3:]) for item in _nul_items(tracked)]
    untracked = _git(['ls-files', '--others', '--exclude-standard', '-z']).stdout
    changes.extend(Change('?', path) for path in _nul_items(untracked))
    return changes


def normalize_selectors(raw_paths: list[str]) -> list[str]:
    """把 ``--paths`` 规范化成相对仓库根的 posix 路径；拒绝仓库外与整仓提交。"""
    selectors: list[str] = []
    for raw in raw_paths:
        text = raw.strip()
        if not text:
            raise FinalizeError('--paths 不能为空')
        candidate = Path(text)
        if candidate.is_absolute():
            try:
                relative = candidate.resolve().relative_to(ROOT)
            except ValueError as exc:
                raise FinalizeError(f'--paths 必须位于仓库根 {ROOT} 内：{raw}') from exc
            text = relative.as_posix()
        else:
            text = os.path.normpath(text).replace('\\', '/')
        while text.startswith('./'):
            text = text[2:]
        text = text.rstrip('/')
        if not text or text == '.':
            raise FinalizeError('不允许提交整个仓库，请给出具体文件或子目录')
        if text.startswith('../'):
            raise FinalizeError(f'--paths 不得跳出仓库根：{raw}')
        if text not in selectors:
            selectors.append(text)
    return selectors


def select_changes(changes: list[Change], selectors: list[str]) -> list[Change]:
    """按前缀选中改动：路径相等，或位于给出的目录之下。"""
    return [
        change
        for change in changes
        if any(
            change.path == selector or change.path.startswith(selector + '/')
            for selector in selectors
        )
    ]


def _family(path: str) -> str:
    """文件所属族：命中 FAMILY_ORDER 的取该名，否则取首层目录，根目录文件单列。"""
    for family in FAMILY_ORDER:
        if path == family or path.startswith(family + '/'):
            return family
    return path.split('/', 1)[0] if '/' in path else '(仓库根)'


def _family_key(family: str) -> tuple[int, str]:
    if family in FAMILY_ORDER:
        return FAMILY_ORDER.index(family), family
    return len(FAMILY_ORDER), family


def print_plan(changes: list[Change], chosen: set[str]) -> None:
    """按文件族分组打印清单，标出哪些纳入本次提交。"""
    groups: dict[str, list[Change]] = {}
    for change in changes:
        groups.setdefault(_family(change.path), []).append(change)
    print(f'清单：共 {len(changes)} 项，纳入 {len(chosen)} 项')
    for family in sorted(groups, key=_family_key):
        items = sorted(groups[family], key=lambda change: change.path)
        rendered = [
            f'{change.status} {change.path} [{"提交" if change.path in chosen else "不动"}]'
            for change in items
        ]
        head = '; '.join(rendered[:6])
        more = f'; …另 {len(rendered) - 6} 项' if len(rendered) > 6 else ''
        print(f'  {family}: {head}{more}')


def validate_message(message: str) -> str:
    """校验提交信息：非空、无空行、无 BOM；返回去掉首尾换行的正文。"""
    if message.startswith('\ufeff') or message.encode('utf-8').startswith(BOM):
        raise FinalizeError('提交信息不得带 BOM')
    text = message.replace('\r\n', '\n').replace('\r', '\n').strip('\n')
    if not text.strip():
        raise FinalizeError('提交信息不能为空')
    if any(not line.strip() for line in text.split('\n')):
        raise FinalizeError('提交信息不得有空行（标题与正文、正文各条之间必须连续）')
    return text


def _write_message(path: Path, text: str) -> None:
    """写提交信息文件：UTF-8 无 BOM，LF 换行；写完自检首 3 字节。"""
    with path.open('w', encoding='utf-8', newline='\n') as handle:
        handle.write(text + '\n')
    if path.read_bytes()[:3] == BOM:
        raise FinalizeError(f'提交信息文件带 BOM：{path}')


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _format_paths(paths: list[str]) -> str:
    """把路径清单排成一行，便于直接在报错里逐条核对。"""
    return '、'.join(paths) if paths else '无'


def foreign_staged(expected: set[str]) -> list[str]:
    """返回共享暂存区里本次范围外的已暂存路径；回填 read-tree HEAD 会清掉它们的暂存。"""
    staged = set(_nul_items(_git(['diff', '--cached', '--name-only', '-z']).stdout))
    return sorted(staged - expected)


def _ensure_no_foreign_staged(expected: set[str]) -> None:
    """共享暂存区存在本次范围外的条目时停止，并列出具体路径与可执行提示。"""
    foreign = foreign_staged(expected)
    if foreign:
        raise FinalizeError(
            f'共享暂存区有 {len(foreign)} 项本次范围外的已暂存条目，未提交：'
            f'{_format_paths(foreign)}。'
            '提示：先逐条 git reset -- <path> 取消暂存（不动工作区），'
            '或把这些路径一并写进 --paths，再重跑。'
        )


def commit_paths(expected: list[str], message: str) -> str:
    """用临时索引提交 ``expected`` 这批路径，返回新提交哈希；全程五步校验。"""
    before = _git(['rev-parse', 'HEAD']).stdout.strip()
    with tempfile.TemporaryDirectory(prefix='finalize-index-') as tmp:
        env = dict(os.environ)
        env['GIT_INDEX_FILE'] = str(Path(tmp) / 'index')
        _git(['read-tree', before], env=env)
        _git(
            ['update-index', '-z', '--add', '--remove', '--stdin'],
            env=env,
            stdin_text='\0'.join(expected) + '\0',
        )
        staged = sorted(
            _nul_items(
                _git(['diff', '--cached', '--name-only', '-z', before], env=env).stdout
            )
        )
        if staged != expected:
            extra = sorted(set(staged) - set(expected))
            missing = sorted(set(expected) - set(staged))
            raise FinalizeError(
                '清单与预期不符，未提交：'
                f'多出 {len(extra)} 项（{_format_paths(extra)}）；'
                f'少掉 {len(missing)} 项（{_format_paths(missing)}）。'
                '提示：多出通常是 --paths 前缀把同目录无关文件带进来了，'
                '少掉通常是路径拼错或被 .gitignore 忽略；调整 --paths 后先 dry-run 复核再 --apply。'
            )
        if _git(['rev-parse', 'HEAD']).stdout.strip() != before:
            raise FinalizeError(
                'HEAD 在提交前发生变动（可能有并发提交），本次未提交。'
                '提示：重跑本命令即可（脚本每次都重新取 HEAD）；'
                '若确有并发收尾，先确认对方已收口再动。'
            )
        message_path = Path(tmp) / 'COMMIT_MSG'
        _write_message(message_path, message)
        _git(['commit', '-F', str(message_path), '--cleanup=verbatim'], env=env)
    new_head = _git(['rev-parse', 'HEAD']).stdout.strip()
    if new_head == before:
        raise FinalizeError(
            'git commit 未产生新提交（HEAD 未变化）。'
            '提示：检查 pre-commit hook 是否拦截、工作区是否已无改动；'
            'git status --short 复核后重跑。'
        )
    # 新进程、不带 GIT_INDEX_FILE：回填共享索引到新 HEAD
    _git(['read-tree', 'HEAD'])
    return new_head


def verify_commit(head: str, expected: list[str]) -> list[Change]:
    """用 diff-tree 复核 HEAD 清单，与预期逐条比对；不一致即抛错。"""
    items = _nul_items(
        _git(
            ['diff-tree', '--no-commit-id', '--no-renames', '-r', '-z', '--name-status', head]
        ).stdout
    )
    changes = [
        Change(_status_letter(status), path)
        for status, path in zip(items[0::2], items[1::2], strict=True)
    ]
    actual = sorted(change.path for change in changes)
    if actual != expected:
        extra = sorted(set(actual) - set(expected))
        missing = sorted(set(expected) - set(actual))
        raise FinalizeError(
            '提交后自证不符：HEAD 实际清单与预期不一致：'
            f'多出 {len(extra)} 项（{_format_paths(extra)}）；'
            f'少掉 {len(missing)} 项（{_format_paths(missing)}）。'
            '提示：先 git diff-tree --no-commit-id --name-status -r HEAD 人工核对；'
            '按前向修复处理，不要 amend 或 reset。'
        )
    return changes


def sync_backup(backup_root: Path, changes: list[Change]) -> tuple[int, int, int]:
    """同步本次提交文件到备份仓：复制 + 对位删除 + SHA256；返回 (已处理, 复制, 删除)。"""
    copied = deleted = processed = 0
    for change in changes:
        source = ROOT / change.path
        target = backup_root / change.path
        if change.status == 'D':
            if target.is_file():
                target.unlink()
                deleted += 1
        else:
            if not source.is_file():
                raise FinalizeError(f'备份同步失败：源文件不存在 {change.path}')
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
            if _sha256(source) != _sha256(target):
                raise FinalizeError(f'备份同步失败：SHA256 不一致 {change.path}')
            copied += 1
        processed += 1
    if processed != len(changes):
        raise FinalizeError(
            f'备份条数核对不等：清单 {len(changes)} 条，已处理 {processed} 条，'
            f'差 {len(changes) - processed} 条未同步。'
            '提示：检查备份仓同名路径是否存在且可写，修正后重跑本命令。'
        )
    return processed, copied, deleted


def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


def _push_backup(backup_root: Path) -> str:
    """推送备份仓；失败时按规则先探本地代理端口，未监听才清代理直连重试一次。"""
    branch = _git(['rev-parse', '--abbrev-ref', 'HEAD'], cwd=backup_root).stdout.strip()
    proc = _git(['push', 'origin', branch], cwd=backup_root, check=False)
    if proc.returncode == 0:
        return f'已推送 origin/{branch}'
    if not _port_open(PROXY_HOST, PROXY_PORT):
        retry = _git(
            ['-c', 'http.proxy=', '-c', 'https.proxy=', 'push', 'origin', branch],
            cwd=backup_root,
            check=False,
        )
        if retry.returncode == 0:
            return f'已推送 origin/{branch}（直连重试）'
        raise FinalizeError(
            f'备份仓推送失败（直连重试仍失败）：{(retry.stderr or retry.stdout).strip()}'
        )
    raise FinalizeError(
        f'备份仓推送失败（{PROXY_HOST}:{PROXY_PORT} 在监听，未清代理）：'
        f'{(proc.stderr or proc.stdout).strip()}'
    )


def commit_backup(
    backup_root: Path, changes: list[Change], source_head: str
) -> tuple[str, str]:
    """备份仓按路径限定提交并推送；返回 (短哈希, 推送状态)。"""
    paths = sorted({change.path for change in changes})
    pending = _nul_items(
        _git(['status', '--porcelain', '-z', '--', *paths], cwd=backup_root).stdout
    )
    if not pending:
        short = _git(['rev-parse', '--short', 'HEAD'], cwd=backup_root).stdout.strip()
        return short, '备份仓无新增变更，未推送'
    _git(['add', '-A', '--', *paths], cwd=backup_root)
    today = date.today()
    message = (
        f'chore(backup): {today.year}-{today.month}-{today.day}，'
        f'同步 {len(paths)} 个文件（{source_head[:7]}）'
    )
    with tempfile.TemporaryDirectory(prefix='finalize-backup-msg-') as tmp:
        message_path = Path(tmp) / 'COMMIT_MSG'
        _write_message(message_path, message)
        _git(
            ['commit', '-F', str(message_path), '--cleanup=verbatim', '--', *paths],
            cwd=backup_root,
        )
    short = _git(['rev-parse', '--short', 'HEAD'], cwd=backup_root).stdout.strip()
    return short, _push_backup(backup_root)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='提交收尾一条命令（默认 dry-run）')
    parser.add_argument(
        '--paths', action='append', required=True, help='显式提交路径，可重复；目录按前缀展开'
    )
    parser.add_argument('--message', required=True, help='中文提交信息（不得有空行/BOM）')
    parser.add_argument('--apply', action='store_true', help='真正提交并同步备份（缺省只打印计划）')
    args = parser.parse_args(argv)

    try:
        selectors = normalize_selectors(args.paths)
        message = validate_message(args.message)
        changes = list_changes()
        chosen = select_changes(changes, selectors)
        chosen_paths = {change.path for change in chosen}
        backup_root = resolve_backup_root()

        expected = sorted(chosen_paths)
        if not args.apply:
            print_plan(changes, chosen_paths)
            print(f'提交信息：通过校验（{len(message.split(chr(10)))} 行，无空行，无 BOM）')
            print(f'备份仓：{backup_root}')
        if not expected:
            raise FinalizeError(
                '--paths 没有命中任何改动，未提交。'
                '提示：先跑 git status --short 核对路径拼写；被 .gitignore 忽略的文件不会进入清单，'
                '确认确实要提交时先 git add -f <path> 或调整忽略规则。'
            )
        _ensure_no_foreign_staged(set(expected))
        if not args.apply:
            print('[dry-run] 未写入任何内容；确认无误后加 --apply 执行')
            return 0
        head = commit_paths(expected, message)
        actual = verify_commit(head, expected)
        short = _git(['rev-parse', '--short', 'HEAD']).stdout.strip()
        print(f'提交 {short}｜{len(actual)} 个文件')
        print(f'自证：diff-tree {len(actual)} 项 == 预期 {len(expected)} 项（一致）')
        processed, copied, deleted = sync_backup(backup_root, actual)
        print(f'备份同步：{processed}/{len(actual)}（复制 {copied}，对位删除 {deleted}）')
        backup_short, push_state = commit_backup(backup_root, actual, head)
        print(f'备份仓 {backup_short}｜{push_state}')
        print(f'工作区未纳入：{len(changes) - len(chosen)} 项（保持原样）')
        return 0
    except BackupRootError as exc:
        print(f'备份仓解析失败：{exc}', file=sys.stderr)
        return 2
    except FinalizeError as exc:
        print(f'错误：{exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
