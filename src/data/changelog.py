"""数据变更台账（logs/data_changelog.csv）的唯一写入入口。

台账此前没有任何代码写入点：`data/local_store/**` 的变更记录靠任务收口时人工追加，
`reports/**` 则从未被记账，导致「某份报告何时被删、删的是哪一版」无法回答。
本模块把写入收敛到一个函数，并提供删除事件的便捷入口，避免再各写各的。

字段：run_id,timestamp,task,action,path,bytes,sha256,prev_sha256,operator,reason
- `path` 一律用**仓库相对路径**（`data/local_store/...`、`reports/...`），新增行不再使用
  「相对 data/local_store」的旧写法；历史行保持原样，不做回写。
- `reason` 是 2026-11-10 新增的末位列，用于记录删除/覆盖的成因；历史行该列为空。

约定（与 `logs/data_changelog.README.md` 一致）：只追加，不改历史行；`action` 只取
`create|update|delete`；`delete` 必须给出删除前的 `sha256`，`prev_sha256` 可留空。
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import sys
import time
from pathlib import Path

FIELDS = [
    "run_id",
    "timestamp",
    "task",
    "action",
    "path",
    "bytes",
    "sha256",
    "prev_sha256",
    "operator",
    "reason",
]
ACTIONS = ("create", "update", "delete")


def repo_root() -> Path:
    """仓库根：src/data/changelog.py 往上两级。"""
    return Path(__file__).resolve().parents[2]


def changelog_path() -> Path:
    return repo_root() / "logs" / "data_changelog.csv"


def file_sha256(path: str | os.PathLike[str], chunk: int = 4 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            block = f.read(chunk)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def _upgrade_header(csv_path: Path) -> None:
    """把 9 列旧表头原地升级为 10 列（补空 reason），历史值一个不改。

    走「临时文件 + os.replace」，失败时原文件保持不动；行数不守恒直接抛错。
    """
    with open(csv_path, encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    if not rows or rows[0] == FIELDS:
        return
    if rows[0] != FIELDS[:-1]:
        raise ValueError(f"表头与预期不符，拒绝改写：{rows[0]}")
    body = [r + [""] for r in rows[1:] if r]
    tmp = csv_path.with_suffix(".csv.tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(FIELDS)
        w.writerows(body)
    if len(body) != len(rows) - 1:
        tmp.unlink(missing_ok=True)
        raise ValueError("行数不守恒，已放弃升级")
    os.replace(tmp, csv_path)


def append_event(
    *,
    action: str,
    path: str,
    task: str,
    reason: str,
    sha256: str,
    size_bytes: int,
    prev_sha256: str = "",
    operator: str = "system",
    run_id: str | None = None,
    timestamp: str | None = None,
    csv_file: str | os.PathLike[str] | None = None,
) -> dict[str, str]:
    """追加一条台账记录并返回该行。`path` 用仓库相对路径。"""
    if action not in ACTIONS:
        raise ValueError(f"action 只能是 {ACTIONS}，收到 {action!r}")
    if not path or os.path.isabs(path):
        raise ValueError("path 必须是仓库内相对路径")
    target = Path(csv_file) if csv_file else changelog_path()

    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        _upgrade_header(target)
    else:
        with open(target, "w", encoding="utf-8", newline="\n") as f:
            csv.writer(f, lineterminator="\n").writerow(FIELDS)

    row = {
        "run_id": run_id or f"run_{time.strftime('%Y%m%d_%H%M%S')}",
        "timestamp": timestamp or time.strftime("%Y-%m-%dT%H:%M:%S"),
        "task": task,
        "action": action,
        "path": path.replace("\\", "/"),
        "bytes": str(size_bytes),
        "sha256": sha256,
        "prev_sha256": prev_sha256,
        "operator": operator,
        "reason": reason,
    }
    with open(target, "a", encoding="utf-8", newline="\n") as f:
        csv.writer(f, lineterminator="\n").writerow([row[k] for k in FIELDS])
    return row


def record_delete(
    *,
    path: str,
    task: str,
    reason: str,
    sha256: str | None = None,
    size_bytes: int | None = None,
    **kwargs: str,
) -> dict[str, str]:
    """删除事件入口：删之前调用，自动取当前字节数与 SHA256。"""
    abs_path = repo_root() / path
    if size_bytes is None:
        size_bytes = os.path.getsize(abs_path)
    if sha256 is None:
        sha256 = file_sha256(abs_path)
    return append_event(
        action="delete",
        path=path,
        task=task,
        reason=reason,
        sha256=sha256,
        size_bytes=size_bytes,
        **kwargs,
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="向 logs/data_changelog.csv 追加一条变更记录")
    ap.add_argument("--action", required=True, choices=ACTIONS)
    ap.add_argument("--path", required=True, help="仓库相对路径，如 reports/xxx/panel.parquet")
    ap.add_argument("--task", required=True)
    ap.add_argument("--reason", required=True)
    ap.add_argument("--sha256", default="")
    ap.add_argument("--prev-sha256", default="")
    ap.add_argument("--bytes", type=int, default=None)
    ap.add_argument("--operator", default="system")
    args = ap.parse_args(argv)

    size_bytes = args.bytes
    sha = args.sha256
    if size_bytes is None:
        size_bytes = os.path.getsize(repo_root() / args.path)
    if not sha:
        sha = file_sha256(repo_root() / args.path)
    row = append_event(
        action=args.action,
        path=args.path,
        task=args.task,
        reason=args.reason,
        sha256=sha,
        size_bytes=size_bytes,
        prev_sha256=args.prev_sha256,
        operator=args.operator,
    )
    sys.stdout.write(",".join(row[k] for k in FIELDS) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
