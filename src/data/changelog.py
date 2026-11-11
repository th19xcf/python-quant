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

三条硬约束（2026-11-10 修复）：
1. **列升级只改首行表头**：历史行按字节原样保留，列数不足的旧行由读侧 `read_events`
   补空容错，不允许把 9,821 行历史整行重写。
2. **同事件幂等**：同一 `action + path + task` 在 `DEFAULT_DEDUPE_WINDOW_SECONDS` 内重复调用
   只落一条，返回里带 `duplicate="true"`；窗口传 0 或 None 可关闭。
3. **哈希格式校验**：`sha256` 必须 64 位十六进制，落库统一小写；`delete` 不允许留空。
   截断哈希（如 16 位）会被拒绝，避免写入永远对不上任何文件的记录。
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import re
import sys
import time
from pathlib import Path

from src.data.atomic_write import write_bytes_atomic

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
SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
DEFAULT_DEDUPE_WINDOW_SECONDS = 300.0
TS_FORMATS = ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S")


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


def normalize_sha256(value: str, *, action: str) -> str:
    """校验 sha256：必须 64 位十六进制，落库统一小写；`delete` 必须给出哈希。

    长度不符 / 含非十六进制字符 / 截断一律抛错，避免写入永远对不上文件的记录。
    """
    text = (value or "").strip()
    if not text:
        if action == "delete":
            raise ValueError(
                f"delete 必须给出删除前的 sha256（期望 64 位十六进制，实际 {len(text)} 位）"
            )
        return ""
    if not SHA256_RE.match(text):
        raise ValueError(f"sha256 期望 64 位十六进制，实际 {len(text)} 位：{text!r}")
    return text.lower()


def _header_fields(csv_path: Path) -> list[str]:
    """只读首行，取当前表头的列名（文件不存在或为空时返回标准表头）。"""
    if not csv_path.exists():
        return list(FIELDS)
    with open(csv_path, encoding="utf-8", newline="") as f:
        first = f.readline()
    if not first.strip():
        return list(FIELDS)
    return next(csv.reader([first.rstrip("\r\n")]))


def _upgrade_header(csv_path: Path) -> None:
    """只改写首行表头（9 列 → 10 列），其后的历史行按字节原样拷贝。

    走 ``write_bytes_atomic``（临时文件 → fsync → 替换），失败时原文件保持不动。
    """
    raw = csv_path.read_bytes()
    if not raw:
        return
    pos = raw.find(b"\n")
    first, rest = (raw, b"") if pos < 0 else (raw[: pos + 1], raw[pos + 1 :])
    header = next(csv.reader([first.decode("utf-8").rstrip("\r\n")]))
    if header == FIELDS:
        return
    if header != FIELDS[:-1]:
        raise ValueError(f"表头与预期不符，拒绝改写：{header}")
    ending = b"\r\n" if first.endswith(b"\r\n") else b"\n"
    write_bytes_atomic(
        ",".join(FIELDS).encode("utf-8") + ending + rest, csv_path)


def read_events(csv_file: str | os.PathLike[str] | None = None) -> list[dict[str, str]]:
    """读取台账全部记录；列数不足的旧行按当前表头补空串，不抛错。"""
    target = Path(csv_file) if csv_file else changelog_path()
    if not target.exists():
        return []
    events: list[dict[str, str]] = []
    with open(target, encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            return []
        for cells in reader:
            if not cells:
                continue
            cells = (cells + [""] * len(header))[: len(header)]
            events.append(dict(zip(header, cells, strict=True)))
    return events


def _parse_ts(text: str) -> float | None:
    text = (text or "").strip()
    if not text:
        return None
    for fmt in TS_FORMATS:
        try:
            return time.mktime(time.strptime(text, fmt))
        except ValueError:
            continue
    return None


def _find_recent_duplicate(
    target: Path,
    *,
    action: str,
    path: str,
    task: str,
    window_seconds: float,
    ref_ts: float,
) -> dict[str, str] | None:
    if window_seconds <= 0 or not target.exists():
        return None
    for event in read_events(target):
        if (event.get("action"), event.get("path"), event.get("task")) != (
            action,
            path,
            task,
        ):
            continue
        ts = _parse_ts(event.get("timestamp", ""))
        if ts is not None and abs(ref_ts - ts) <= window_seconds:
            return event
    return None


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
    dedupe_window_seconds: float | None = DEFAULT_DEDUPE_WINDOW_SECONDS,
) -> dict[str, str]:
    """追加一条台账记录并返回该行；窗口内重复事件不落盘，返回已有行 + `duplicate="true"`。"""
    if action not in ACTIONS:
        raise ValueError(f"action 只能是 {ACTIONS}，收到 {action!r}")
    if not path or os.path.isabs(path):
        raise ValueError("path 必须是仓库内相对路径")
    sha256 = normalize_sha256(sha256, action=action)
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

    if dedupe_window_seconds is not None and dedupe_window_seconds > 0:
        ref_ts = _parse_ts(row["timestamp"]) or time.time()
        dup = _find_recent_duplicate(
            target,
            action=row["action"],
            path=row["path"],
            task=row["task"],
            window_seconds=dedupe_window_seconds,
            ref_ts=ref_ts,
        )
        if dup is not None:
            return {**dup, "duplicate": "true"}

    width = len(_header_fields(target))
    values = [row[k] for k in FIELDS][:width]
    values += [""] * (width - len(values))
    with open(target, "a", encoding="utf-8", newline="\n") as f:
        csv.writer(f, lineterminator="\n").writerow(values)
    return {**row, "duplicate": "false"}


def record_delete(
    *,
    path: str,
    task: str,
    reason: str,
    sha256: str | None = None,
    size_bytes: int | None = None,
    **kwargs: object,
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
        **kwargs,  # type: ignore[arg-type]
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
    ap.add_argument(
        "--dedupe-window",
        type=float,
        default=DEFAULT_DEDUPE_WINDOW_SECONDS,
        help="同 action+path+task 的幂等窗口（秒），0 表示关闭",
    )
    ap.add_argument("--no-dedupe", action="store_true", help="等价于 --dedupe-window 0")
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
        dedupe_window_seconds=0 if args.no_dedupe else args.dedupe_window,
    )
    sys.stdout.write(",".join(row[k] for k in FIELDS) + "\n")
    if row.get("duplicate") == "true":
        sys.stdout.write("duplicate=true（窗口内已存在，未重复写入）\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
