#!/usr/bin/env python3

"""原子写盘：同目录临时文件 → ``fsync`` → ``os.replace`` 覆盖目标。

支持 eager / lazy parquet、文本与二进制四类写入；写一半失败（断电、进程
被杀、磁盘满）时目标文件保持原样，不会留下半截 parquet / json / csv；
临时文件以 ``.<目标文件名>.tmp`` 命名，失败即清理，不在数据目录里遗留
``.tmp`` 垃圾。全仓正式数据写入必须走本模块（DG-9），禁止手写
「临时文件 + ``os.replace``」——那条路径没有 fsync。

**单文件原子，不等于多文件事务**：多个文件逐个原子写时，中途失败仍会
出现「前几个已更新、后几个还是旧值」的跨文件不一致，``os.replace`` 无法
保证这一层；调用方需自行评估该残留风险。

**变更台账自动埋点（2026-11-12）**：目标路径落在 ``data/local_store/**`` 时，
写盘成功后自动向 ``logs/data_changelog.csv`` 追加一条 ``create`` / ``update``
记录（写前不存在 → ``create``，存在 → ``update``，判定基于 ``os.replace``
之前的实际状态）。边界与坑：

- ``_staging/``、``_backup/``、``reports/``、``logs/``、``research/`` 一律不记；
  这些路径每天几百次写，记进去会淹掉台账，且本来就不是正式数据。
- ``delete`` / ``move`` 不做自动推断，仍由调用方显式调用
  :func:`src.data.changelog.record_delete` / :func:`record_move`。
- 写台账自身会用 :func:`suspend_changelog_recording` 显式关掉埋点，不依赖
  「日志路径恰好不在正式库」这种偶然正确。
- ``prev_sha256`` 优先取同目录（或祖先目录）``manifest.json`` 登记值，但必须
  ``bytes`` 与 ``mtime`` 同时与磁盘一致才敢用——manifest 是旁路文件，可能滞后
  于磁盘；两项任一不符或没有 manifest 就退回流式重算。exchanges 类 manifest
  只记 ``bytes`` / ``sha256`` 不记 ``mtime`` 的，按「无法校验」处理，仍然重算。
- 记账失败**不影响数据**：数据已原子落盘，记录失败时写
  ``logs/data_changelog_failures.csv`` 并向 stderr 告警，不抛异常——否则调用方
  会误判写盘失败而重试。``tools/check_changelog.py`` 会把该文件非空判为失败。
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
    import polars as pl


# ---------------------------------------------------------------------------
# 变更台账埋点
# ---------------------------------------------------------------------------

#: 埋点暂停深度；>0 时所有写入都不记账（台账自身写盘走这里）
_suspend_depth = 0

#: 记账失败记录（只增不改；非空即由 check_changelog 判红）
FAILURES_FIELDS = ['timestamp', 'target', 'kind', 'error_type', 'error_message']


@contextmanager
def suspend_changelog_recording() -> Iterator[None]:
    """显式关闭台账埋点（可重入）。

    ``logs/data_changelog.csv`` 本身也是通过本模块写盘的，必须用它把「写台账」
    这一步排除在记账之外；不能依赖「日志路径不在 data/local_store 下」这种
    路径判断的偶然性。
    """
    global _suspend_depth
    _suspend_depth += 1
    try:
        yield
    finally:
        _suspend_depth -= 1


def _formal_store_root() -> Path:
    from src.data.changelog import repo_root  # 延迟导入，避免与 changelog 循环依赖

    return Path(repo_root()) / 'data' / 'local_store'


def _is_formal_target(target: Path, store_root: Path) -> bool:
    """目标是否落在 ``data/local_store/**`` 的正式层（排除快照与 staging）。"""
    try:
        rel = os.path.relpath(os.path.abspath(target), os.path.abspath(store_root))
    except ValueError:  # Windows 跨盘符
        return False
    if rel == os.curdir or rel.startswith(os.pardir + os.sep) or os.path.isabs(rel):
        return False
    return not any(part in ('_backup', '_staging') for part in Path(rel).parts)


def _hash_file(path: Path, chunk: int = 4 << 20) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for block in iter(lambda: handle.read(chunk), b''):
            digest.update(block)
    return digest.hexdigest()


def _manifest_sha(target: Path, store_root: Path) -> str:
    """从 manifest.json 取 ``target`` 的 sha256，仅当 bytes + mtime 都对得上。

    manifest 是写完数据后另行生成的旁路文件，可能滞后或只记部分字段，所以
    不能只信 ``sha256``；``bytes`` 与 ``mtime`` 任一不符即视为不可用，调用方
    退回流式重算。
    """
    stat = target.stat()
    size = stat.st_size
    mtime = datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%dT%H:%M:%S')
    parent = target.parent
    target_abs = os.path.normcase(os.path.abspath(target))
    while True:
        manifest = parent / 'manifest.json'
        try:
            if manifest.is_file():
                payload = json.loads(manifest.read_text(encoding='utf-8'))
                raw_files = payload.get('files')
                entries = raw_files.values() if isinstance(raw_files, dict) else (raw_files or [])
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    if entry.get('bytes') != size or entry.get('mtime') != mtime:
                        continue
                    sha = entry.get('sha256')
                    if not sha:
                        continue
                    declared = entry.get('path')
                    if declared and not _entry_matches(declared, parent, store_root, target_abs):
                        continue
                    return str(sha).lower()
        except (OSError, ValueError, TypeError):
            pass
        if parent == store_root or parent.parent == parent:
            break
        parent = parent.parent
    return ''


def _entry_matches(declared: str, manifest_dir: Path, store_root: Path, target_abs: str) -> bool:
    for base in (manifest_dir, store_root):
        candidate = os.path.normcase(os.path.abspath(os.path.join(base, declared)))
        if candidate == target_abs:
            return True
    return False


def _prev_sha256(target: Path, store_root: Path) -> str:
    cached = _manifest_sha(target, store_root)
    return cached or _hash_file(target)


def _task_name() -> str:
    named = os.environ.get('QUANT_CHANGELOG_TASK', '').strip()
    if named:
        return named
    script = Path(sys.argv[0]).stem if sys.argv and sys.argv[0] else ''
    return script or 'unattributed'


def _prepare_record(target: Path, kind: str) -> dict[str, Any] | None:
    """写盘前抓「写前状态」；不在正式层或已暂停埋点时返回 None。"""
    if _suspend_depth:
        return None
    try:
        store_root = _formal_store_root()
        if not _is_formal_target(target, store_root):
            return None
        existed = target.exists()
        prev = _prev_sha256(target, store_root) if existed else ''
        return {'existed': existed, 'prev_sha256': prev, 'kind': kind}
    except Exception as exc:  # noqa: BLE001 - 记账不得影响数据写入
        _record_failure(target, kind, exc)
        return None


def _finish_record(
    context: dict[str, Any] | None,
    target: Path,
    *,
    payload: bytes | None = None,
) -> None:
    """写盘成功后补记台账；任何异常都只落失败清单，不向调用方抛。"""
    if context is None:
        return
    try:
        from src.data import changelog  # 延迟导入，避免循环依赖

        if payload is not None:
            sha = hashlib.sha256(payload).hexdigest()
        else:
            sha = _hash_file(target)
        changelog.append_event(
            action='create' if not context['existed'] else 'update',
            path=str(target),
            task=_task_name(),
            reason=f"自动记账（{context['kind']} 原子写盘）",
            sha256=sha,
            size_bytes=target.stat().st_size,
            prev_sha256=context['prev_sha256'],
            operator=os.environ.get('QUANT_CHANGELOG_OPERATOR', 'system'),
            dedupe_window_seconds=0,
        )
    except Exception as exc:  # noqa: BLE001 - 数据已落盘，记账失败不能反噬
        _record_failure(target, context['kind'], exc)


def _record_failure(target: Path, kind: str, exc: BaseException) -> None:
    """记账失败落 ``logs/data_changelog_failures.csv`` 并告警，不抛异常。"""
    try:
        from src.data.changelog import repo_root

        path = Path(repo_root()) / 'logs' / 'data_changelog_failures.csv'
        path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not path.exists() or path.stat().st_size == 0
        with open(path, 'a', encoding='utf-8', newline='') as handle:
            writer = csv.writer(handle, lineterminator='\n')
            if write_header:
                writer.writerow(FAILURES_FIELDS)
            writer.writerow([
                datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
                str(target), kind, type(exc).__name__, str(exc)[:500],
            ])
    except Exception:  # noqa: BLE001 - 连失败清单都写不了，只能退到 stderr
        sys.stderr.write(f'[changelog] 记账失败且失败清单不可写：{target}\n')
    sys.stderr.write(
        f'[changelog] 记账失败（数据已落盘）：{target}：{type(exc).__name__}: {exc}\n')


def _tmp_path(path: Path) -> Path:
    return path.with_name(f'.{path.name}.tmp')


def _fsync(path: Path) -> None:
    # Windows 上 fsync 需要可写句柄，故用 ``rb+`` 而不是 ``rb``
    with open(path, 'rb+') as handle:
        os.fsync(handle.fileno())


#: path 位收到字符串时的长度上限；超过即判「文本写到了 path 位」
_GUARD_PATH_MAX = 512


def _require_write_args(kind: str, payload: object, path: object) -> None:
    """写盘前防呆：参数顺序写反时立刻抛错，不建目录、不落临时文件。

    只做 O(1) 的形态判定，不猜内容语义，因此对既有调用点零影响：

    - 载荷位类型不对（``write_text_atomic`` 首位收到 ``Path``、
      ``write_bytes_atomic`` 首位收到 ``str``/``Path``）即判顺序写反；
    - 目标位收到 ``str`` 但含换行或超长，说明「文本被写到了 path 位」。
    """
    expected = 'str' if kind == 'text' else 'bytes'
    ok_payload = (isinstance(payload, str) if kind == 'text'
                  else isinstance(payload, (bytes, bytearray, memoryview)))
    if not ok_payload:
        raise TypeError(
            f'write_{kind}_atomic 参数顺序疑似颠倒：第 1 个参数应为 {expected}，'
            f'实际收到 {type(payload).__name__}；签名是 '
            f'write_{kind}_atomic(payload, path)')
    if not isinstance(path, (str, Path)):
        raise TypeError(
            f'write_{kind}_atomic 参数顺序疑似颠倒：第 2 个参数应为 Path/str，'
            f'实际收到 {type(path).__name__}（像是把载荷写到了 path 位）')
    if isinstance(path, str) and ('\n' in path or len(path) > _GUARD_PATH_MAX):
        raise TypeError(
            f'write_{kind}_atomic 参数顺序疑似颠倒：path 位收到的是文本'
            f'（长度 {len(path)}，含换行={chr(10) in path}）')


def write_parquet_atomic(
    frame: pl.DataFrame | pl.LazyFrame, path: Path | str, *,
    compression: Any = 'zstd',
    compression_level: int | None = None,
    statistics: Any = True,
    row_group_size: int | None = None,
    allow_extra_columns: bool = False,
) -> None:
    """原子写 parquet：落临时文件、``fsync`` 后替换目标。

    ``frame`` 为 ``LazyFrame`` 时走 ``sink_parquet`` 流式落盘，避免把千万行
    中间结果 collect 回内存；两者走同一套临时文件与 fsync 收尾。

    目标落在正式库 ``data/local_store/quotes/**`` 时，写盘前强制校验行情
    schema（列集合 / 顺序 / dtype）：这是绕过 store API 的修复脚本唯一会被
    拦住的地方。确需额外列的调用点显式传 ``allow_extra_columns=True`` 并
    在调用处写明理由，不得整体关闭闸门。

    ``polars`` 延迟导入：只用 :func:`write_text_atomic` /
    :func:`write_bytes_atomic` 的轻量脚本（如巡检）不必为此拉起 polars。
    """
    import polars as pl

    from src.data.quote_schema import assert_formal_quote_schema

    assert_formal_quote_schema(
        path, frame, allow_extra_columns=allow_extra_columns)
    target = Path(path)
    record = _prepare_record(target, 'parquet')
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(target)
    try:
        if isinstance(frame, pl.LazyFrame):
            frame.sink_parquet(
                str(tmp), compression=compression,
                compression_level=compression_level,
                statistics=statistics, row_group_size=row_group_size)
        else:
            frame.write_parquet(
                tmp, compression=compression,
                compression_level=compression_level,
                statistics=statistics, row_group_size=row_group_size)
        _fsync(tmp)
        os.replace(tmp, target)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    _finish_record(record, target)


def write_bytes_atomic(payload: bytes, path: Path | str) -> None:
    """原子写二进制：落临时文件、``fsync`` 后替换目标。"""
    _require_write_args('bytes', payload, path)
    target = Path(path)
    record = _prepare_record(target, 'bytes')
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(target)
    try:
        with open(tmp, 'wb') as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, target)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    _finish_record(record, target, payload=bytes(payload))


def write_text_atomic(
    text: str, path: Path | str, *, encoding: str = 'utf-8',
) -> None:
    """原子写文本：落临时文件、``fsync`` 后替换目标。"""
    _require_write_args('text', text, path)
    target = Path(path)
    record = _prepare_record(target, 'text')
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(target)
    try:
        with open(tmp, 'w', encoding=encoding, newline='') as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, target)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    _finish_record(record, target, payload=text.encode(encoding))
