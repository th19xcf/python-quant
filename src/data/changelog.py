"""数据变更台账（logs/data_changelog.csv）的唯一写入入口。

台账此前没有任何代码写入点：`data/local_store/**` 的变更记录靠任务收口时人工追加，
`reports/**` 则从未被记账，导致「某份报告何时被删、删的是哪一版」无法回答。
本模块把写入收敛到一个函数，并提供删除事件的便捷入口，避免再各写各的。

字段：run_id,timestamp,task,action,path,bytes,sha256,prev_sha256,operator,reason
- `path` 一律用**仓库相对路径 + 正斜杠**（`data/local_store/...`、`reports/...`）。绝对路径
  若落在仓库内会被相对化，落在仓库外直接抛错；反斜杠会被归一为正斜杠。无法相对化时
  绝不写半截。历史行保持原样，不做回写。
- `bytes` 为**数值字符串**；大小未知时写**空字段**，禁止写 `-1` 之类的哨兵值（会让
  `sum()` / `mean()` 得到负数污染）。
- `action` 只取 `create|update|delete`，越界值直接抛错。`move` 不作为一种落库取值：
  用 :func:`record_move` 拆成 `delete`（源）+ `create`（目标）两行，`create` 行的
  `prev_sha256` 填源文件哈希，从而保留「同一内容从哪移到哪」的 linkage。
  选择拆行而不是新增 `dst_path` 列的理由：不改变 10 列结构，读侧无需为旧文件做
  列数分支，每条记录仍然只有**一个**可校验的 64 位哈希。
- `reason` 是 2026-11-10 新增的末位列，用于记录删除/覆盖的成因；历史行该列为空。
- 行尾固定 LF、UTF-8 无 BOM。

约定（与 `logs/data_changelog.README.md` 一致）：只追加，不改历史行；`delete` 必须给出
删除前的 `sha256`，`prev_sha256` 可留空。

四条硬约束：
1. **列升级只改首行表头**：历史行按字节原样保留，列数不足的旧行由读侧 `read_events`
   补空容错，不允许把 9,821 行历史整行重写。
2. **同事件幂等**：同一 `action + path + task` 在 `DEFAULT_DEDUPE_WINDOW_SECONDS` 内重复调用
   只落一条，返回里带 `duplicate="true"`；窗口传 0 或 None 可关闭。
3. **哈希格式校验**：`sha256` 必须 64 位十六进制，落库统一小写；`delete` 不允许留空。
   截断哈希（如 16 位）会被拒绝，避免写入永远对不上任何文件的记录。
4. **追加前校验末行完整**：文件最后一个字节必须是 `\n`，否则拒绝追加（防止在上次
   写一半的残行后面继续拼接，造出永远无法解析的混合行）。
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import os
import re
import sys
import time
from pathlib import Path

from src.data.atomic_write import suspend_changelog_recording, write_bytes_atomic

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


def normalize_repo_path(path: str | os.PathLike[str]) -> str:
    """把路径规范成「仓库相对 + 正斜杠」。

    - 反斜杠归一为正斜杠；
    - 仓库内的绝对路径转成相对路径；
    - 仓库外 / 指向仓库根 / 含换行的路径直接抛错，绝不落半截记录。
    """
    raw = str(path).strip()
    if not raw:
        raise ValueError("path 不能为空")
    if "\n" in raw or "\r" in raw:
        raise ValueError(f"path 含换行，疑似把描述写进了 path：{raw!r}")
    root = os.path.abspath(str(repo_root()))
    candidate = raw if os.path.isabs(raw) else os.path.join(root, raw)
    try:
        rel = os.path.relpath(os.path.abspath(candidate), root)
    except ValueError as exc:  # Windows 跨盘符
        raise ValueError(f"path 无法相对化为仓库内路径：{raw!r}") from exc
    if rel in (os.curdir, os.pardir) or rel.startswith(os.pardir + os.sep) or os.path.isabs(rel):
        raise ValueError(f"path 无法相对化为仓库内路径：{raw!r}")
    return rel.replace(os.sep, "/")


def _assert_tail_intact(csv_path: Path) -> None:
    """追加前确认末行完整：文件最后一个字节必须是 ``\\n``。"""
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return
    with open(csv_path, "rb") as f:
        f.seek(-1, os.SEEK_END)
        last = f.read(1)
    if last != b"\n":
        raise ValueError(
            "台账末行未以换行结尾（疑似上次写入被截断），拒绝追加；"
            "请先人工核对并补全末行，不得在残行后继续拼接"
        )


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
    with suspend_changelog_recording():
        write_bytes_atomic(
            ",".join(FIELDS).encode("utf-8") + b"\n" + rest, csv_path)


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
    size_bytes: int | None,
    prev_sha256: str = "",
    operator: str = "system",
    run_id: str | None = None,
    timestamp: str | None = None,
    csv_file: str | os.PathLike[str] | None = None,
    dedupe_window_seconds: float | None = DEFAULT_DEDUPE_WINDOW_SECONDS,
) -> dict[str, str]:
    """追加一条台账记录并返回该行；窗口内重复事件不落盘，返回已有行 + `duplicate="true"`。"""
    if action not in ACTIONS:
        hint = "；move 请用 record_move（拆成 delete + create 两行）" if action == "move" else ""
        raise ValueError(f"action 只能是 {ACTIONS}，收到 {action!r}{hint}")
    path = normalize_repo_path(path)
    if size_bytes is not None:
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int):
            raise TypeError(f"size_bytes 必须是 int 或 None，收到 {type(size_bytes).__name__}")
        if size_bytes < 0:
            raise ValueError("bytes 大小未知时传 None（写空字段），不要传负数哨兵值")
    sha256 = normalize_sha256(sha256, action=action)
    target = Path(csv_file) if csv_file else changelog_path()

    target.parent.mkdir(parents=True, exist_ok=True)
    # 台账写盘全程关闭埋点：即便将来把追加改成 atomic 写，也不会自我递归记账
    with suspend_changelog_recording():
        if target.exists():
            _assert_tail_intact(target)
            _upgrade_header(target)
        else:
            with open(target, "w", encoding="utf-8", newline="\n") as f:
                csv.writer(f, lineterminator="\n").writerow(FIELDS)

        row = {
            "run_id": run_id or f"run_{time.strftime('%Y%m%d_%H%M%S')}",
            "timestamp": timestamp or time.strftime("%Y-%m-%dT%H:%M:%S"),
            "task": task,
            "action": action,
            "path": path,
            "bytes": "" if size_bytes is None else str(size_bytes),
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
        # 先序列化再校验再落盘：字段含逗号/换行/引号时 csv.writer 会自动加引号，
        # 这里回读一遍确认列数等于表头宽度，避免留下「11 列的坏行」等下次核对方发现。
        buffer = io.StringIO()
        csv.writer(buffer, lineterminator="\n").writerow(values)
        line = buffer.getvalue()
        cells = next(csv.reader([line]))
        if len(cells) != width:
            raise ValueError(
                f"台账行结构异常：表头 {width} 列，序列化后 {len(cells)} 列；"
                f"字段含未转义的逗号/换行（task={task!r}）"
            )
        with open(target, "a", encoding="utf-8", newline="\n") as f:
            f.write(line)
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
    path = normalize_repo_path(path)
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


def record_move(
    *,
    src_path: str,
    dst_path: str,
    task: str,
    reason: str,
    src_sha256: str | None = None,
    dst_sha256: str | None = None,
    src_size_bytes: int | None = None,
    dst_size_bytes: int | None = None,
    **kwargs: object,
) -> list[dict[str, str]]:
    """移动/改名事件入口：拆成 `delete`（源）+ `create`（目标）两行。

    两条记录不是跨行事务（``os.replace`` 只保证单文件原子）：若第二行写入失败，
    第一行的 delete 记录已落盘；调用方应保留产物并重试 `create` 一行，不得回滚历史行。
    """
    src = normalize_repo_path(src_path)
    dst = normalize_repo_path(dst_path)
    if src == dst:
        raise ValueError(f"move 的源与目标相同：{src}")
    src_abs = repo_root() / src
    dst_abs = repo_root() / dst
    if src_size_bytes is None:
        src_size_bytes = os.path.getsize(src_abs)
    if src_sha256 is None:
        src_sha256 = file_sha256(src_abs)
    if dst_size_bytes is None:
        dst_size_bytes = os.path.getsize(dst_abs)
    if dst_sha256 is None:
        dst_sha256 = file_sha256(dst_abs)

    kwargs.pop("prev_sha256", None)
    deleted = append_event(
        action="delete",
        path=src,
        task=task,
        reason=reason,
        sha256=src_sha256,
        size_bytes=src_size_bytes,
        **kwargs,  # type: ignore[arg-type]
    )
    created = append_event(
        action="create",
        path=dst,
        task=task,
        reason=reason,
        sha256=dst_sha256,
        size_bytes=dst_size_bytes,
        prev_sha256=src_sha256,
        **kwargs,  # type: ignore[arg-type]
    )
    return [deleted, created]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="向 logs/data_changelog.csv 追加一条变更记录")
    ap.add_argument(
        "--action",
        required=True,
        choices=[*ACTIONS, "move"],
        help="move 表示移动/改名，会拆成 delete（源）+ create（目标）两行",
    )
    ap.add_argument("--path", required=True, help="仓库相对路径，如 reports/xxx/panel.parquet")
    ap.add_argument("--dst-path", default="", help="--action move 时的目标路径")
    ap.add_argument("--task", required=True)
    ap.add_argument("--reason", required=True)
    ap.add_argument("--sha256", default="")
    ap.add_argument("--prev-sha256", default="")
    ap.add_argument("--bytes", type=int, default=None, help="字节数；未知时留空，不要传 -1")
    ap.add_argument("--operator", default="system")
    ap.add_argument(
        "--dedupe-window",
        type=float,
        default=DEFAULT_DEDUPE_WINDOW_SECONDS,
        help="同 action+path+task 的幂等窗口（秒），0 表示关闭",
    )
    ap.add_argument("--no-dedupe", action="store_true", help="等价于 --dedupe-window 0")
    args = ap.parse_args(argv)

    dedupe = 0 if args.no_dedupe else args.dedupe_window
    if args.action == "move":
        if not args.dst_path:
            ap.error("--action move 必须同时给出 --dst-path")
        rows = record_move(
            src_path=args.path,
            dst_path=args.dst_path,
            task=args.task,
            reason=args.reason,
            prev_sha256=args.prev_sha256,
            operator=args.operator,
            dedupe_window_seconds=dedupe,
        )
    else:
        rel = normalize_repo_path(args.path)
        size_bytes = args.bytes
        sha = args.sha256
        if size_bytes is None:
            size_bytes = os.path.getsize(repo_root() / rel)
        if not sha:
            sha = file_sha256(repo_root() / rel)
        rows = [
            append_event(
                action=args.action,
                path=rel,
                task=args.task,
                reason=args.reason,
                sha256=sha,
                size_bytes=size_bytes,
                prev_sha256=args.prev_sha256,
                operator=args.operator,
                dedupe_window_seconds=dedupe,
            )
        ]

    for row in rows:
        sys.stdout.write(",".join(row[k] for k in FIELDS) + "\n")
        if row.get("duplicate") == "true":
            sys.stdout.write("duplicate=true（窗口内已存在，未重复写入）\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
