#!/usr/bin/env python3
"""纯本地巡检：只读系统状态与文件元数据，不调用任何模型、不写 `data/`。

默认由人工手动执行一轮（`python tools/patrol_local.py`），不注册任何定时任务。

**判据是多信号、按可靠性排序的，任一单条都不足以下结论**：

1. `progress.json` / `manifest.json` 的数值计数变化（最强；DG-7 本就要求 `_staging/` 下有断点进度文件）；
2. 登记产物的 mtime 与大小；
3. 任意子进程（`python` / `git` / `node` / `powershell` / `cmd` / `schtasks` 等，不限 python）的 CPU 时间增长；
4. 会话最后更新时间（读得到就报，读不到标 `unavailable`，不伪造）。

判定规则：
- `stalled` **只允许**在「第 1、2 项都可用，且三项硬信号连续两轮都不动」时给出；
- 第 1、2 项缺登记或读不到时，即使进程状态异常也只能判 `unknown`；
- 任一硬信号本轮有变化即为 `healthy`；
- 其余为 `unknown`，并在 `reasons` 里写清本轮实际用了哪几条信号。

输出：
- `logs/patrol_status.json` —— 每轮覆盖写，结构固定（缺数据也保留键）；
- `logs/patrol_alerts.log` —— **只在 `stalled` 时追加**，正常时文件都不创建。

输入 `config/patrol_targets.json`：每个在跑任务登记 `name` / `artifacts`（路径或 glob，可绝对路径）/
`timeout_minutes`，可选 `process_match`（命令行子串）。清单缺失或 `tasks` 为空时退化：
扫 `_staging/**` 与 `data/local_store/**` 的最近 mtime，并在结果里标 `degraded=true`。

边界：不读交易数据内容（只读元数据与 progress/manifest 计数）、不删除任何文件。
"""
from __future__ import annotations

import argparse
import ctypes
import json
import os
import subprocess
import sys
import time
from ctypes import wintypes
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.atomic_write import write_text_atomic  # noqa: E402

DEFAULT_CONFIG = ROOT / "config" / "patrol_targets.json"
DEFAULT_STATUS = ROOT / "logs" / "patrol_status.json"
DEFAULT_ALERTS = ROOT / "logs" / "patrol_alerts.log"
PENDING_PATH = Path(
    r"c:\Users\Administrator\.trae-cn\memory\projects"
    r"\-e-code-python-quant--p2-7445926e270c0ad3e9a7\pending_confirmations.md"
)
SESSION_DIR = Path(
    r"c:\Users\Administrator\.trae-cn\memory\projects"
    r"\-e-code-python-quant--p2-7445926e270c0ad3e9a7"
)
SCHEMA = 2
STALL_ROUNDS = 2
COUNTER_FILES = {"progress.json", "manifest.json"}
# 子进程名白名单（不含 .exe 后缀，小写）：覆盖 python / git / node / powershell / cmd / schtasks
PROC_STEMS = {
    "python", "pythonw", "python3", "py",
    "git", "git-remote-https", "ssh",
    "node", "npm", "npx",
    "powershell", "pwsh", "cmd", "conhost",
    "schtasks", "wmic", "curl",
}

# ---------------------------------------------------------------- 进程采集

TH32CS_SNAPPROCESS = 0x00000002
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
FILETIME_EPOCH = 116444736000000000  # 1601-01-01 -> 1970-01-01，单位 100ns


class PROCESSENTRY32W(ctypes.Structure):
    _fields_ = [
        ("dwSize", wintypes.DWORD),
        ("cntUsage", wintypes.DWORD),
        ("th32ProcessID", wintypes.DWORD),
        ("th32DefaultHeapID", ctypes.POINTER(ctypes.c_ulong)),
        ("th32ModuleID", wintypes.DWORD),
        ("cntThreads", wintypes.DWORD),
        ("th32ParentProcessID", wintypes.DWORD),
        ("pcPriClassBase", ctypes.c_long),
        ("dwFlags", wintypes.DWORD),
        ("szExeFile", wintypes.WCHAR * 260),
    ]


class FILETIME(ctypes.Structure):
    _fields_ = [("dwLowDateTime", wintypes.DWORD), ("dwHighDateTime", wintypes.DWORD)]


def _ft_to_unix(ft: FILETIME) -> float:
    ticks = (ft.dwHighDateTime << 32) | ft.dwLowDateTime
    return (ticks - FILETIME_EPOCH) / 1e7


def _win_process_snapshot() -> list[dict[str, Any]]:
    """枚举白名单进程的 pid / 启动时间 / CPU 秒数（ctypes 直读，不依赖第三方库）。"""
    if os.name != "nt":
        return []
    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    k32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    k32.Process32FirstW.argtypes = [wintypes.HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
    k32.Process32NextW.argtypes = [wintypes.HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
    k32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    k32.OpenProcess.restype = wintypes.HANDLE
    k32.GetProcessTimes.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(FILETIME),
        ctypes.POINTER(FILETIME),
        ctypes.POINTER(FILETIME),
        ctypes.POINTER(FILETIME),
    ]
    k32.CloseHandle.argtypes = [wintypes.HANDLE]
    snap = k32.CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0)
    if snap == wintypes.HANDLE(-1).value:
        return []
    out: list[dict[str, Any]] = []
    try:
        entry = PROCESSENTRY32W()
        entry.dwSize = ctypes.sizeof(PROCESSENTRY32W)
        ok = k32.Process32FirstW(snap, ctypes.byref(entry))
        while ok:
            name = entry.szExeFile
            stem = name.lower().removesuffix(".exe")
            if stem in PROC_STEMS:
                h = k32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, entry.th32ProcessID)
                if h:
                    created, exited, kernel, user = FILETIME(), FILETIME(), FILETIME(), FILETIME()
                    got = k32.GetProcessTimes(
                        wintypes.HANDLE(h),
                        ctypes.byref(created),
                        ctypes.byref(exited),
                        ctypes.byref(kernel),
                        ctypes.byref(user),
                    )
                    k32.CloseHandle(wintypes.HANDLE(h))
                    if got:
                        cpu = (
                            (kernel.dwHighDateTime << 32 | kernel.dwLowDateTime)
                            + (user.dwHighDateTime << 32 | user.dwLowDateTime)
                        ) / 1e7
                        out.append(
                            {
                                "pid": int(entry.th32ProcessID),
                                "name": name,
                                "start_time": datetime.fromtimestamp(
                                    _ft_to_unix(created), tz=UTC
                                ).isoformat(),
                                "cpu_seconds": round(cpu, 3),
                                "cmdline": None,
                            }
                        )
            ok = k32.Process32NextW(snap, ctypes.byref(entry))
    finally:
        k32.CloseHandle(snap)
    return out


def _cmdlines() -> dict[int, str]:
    """可选：取命令行用于 process_match 过滤。取不到就返回空表（匹配退化为全部进程）。"""
    if os.name != "nt":
        return {}
    ps = (
        "Get-CimInstance Win32_Process | Select-Object ProcessId,Name,CommandLine | "
        "ConvertTo-Json -Compress"
    )
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError):
        return {}
    if r.returncode != 0 or not r.stdout.strip():
        return {}
    try:
        data = json.loads(r.stdout)
    except json.JSONDecodeError:
        return {}
    if isinstance(data, dict):
        data = [data]
    return {int(x["ProcessId"]): (x.get("CommandLine") or "") for x in data if "ProcessId" in x}


def collect_processes(*, need_cmdline: bool = False) -> list[dict[str, Any]]:
    """进程采集入口（测试可替换本函数以模拟「有子进程 / 无子进程」）。"""
    procs = _win_process_snapshot()
    if need_cmdline and procs:
        cm = _cmdlines()
        for p in procs:
            p["cmdline"] = cm.get(p["pid"])
    return sorted(procs, key=lambda p: p["pid"])


# ---------------------------------------------------------------- 产物与计数


def _expand(root: Path, pattern: str) -> list[Path]:
    p = Path(pattern)
    base = p if p.is_absolute() else root / pattern
    if any(ch in pattern for ch in "*?["):
        return sorted(x for x in base.parent.glob(base.name) if x.is_file())
    return [base] if base.is_file() else []


def _flatten(obj: Any, prefix: str = "", depth: int = 0) -> dict[str, float]:
    out: dict[str, float] = {}
    if depth > 3 or len(out) >= 200:
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.update(_flatten(v, f"{prefix}{k}.", depth + 1))
    elif isinstance(obj, (int, float)) and not isinstance(obj, bool):
        out[prefix.rstrip(".")] = float(obj)
    return out


def read_counters(path: Path) -> dict[str, float]:
    """从 progress.json / manifest.json 提取数值计数（只读，不解析交易数据）。"""
    if path.name not in COUNTER_FILES:
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return {f"{path.name}:{k}": v for k, v in _flatten(data).items()}


def collect_artifacts(root: Path, patterns: list[str]) -> dict[str, Any] | None:
    files: list[dict[str, Any]] = []
    counters: dict[str, float] = {}
    for pat in patterns:
        for p in _expand(root, pat):
            try:
                st = p.stat()
            except OSError:
                continue
            rel = p.relative_to(root).as_posix() if p.is_relative_to(root) else str(p)
            files.append({"path": rel, "mtime_ns": st.st_mtime_ns, "size": st.st_size})
            counters.update(read_counters(p))
    if not files:
        return None
    max_ns = max(f["mtime_ns"] for f in files)
    return {
        "files": files,
        "file_count": len(files),
        "total_size": sum(f["size"] for f in files),
        "max_mtime_ns": max_ns,
        "max_mtime": datetime.fromtimestamp(max_ns / 1e9, tz=UTC).isoformat(),
        "counters": counters,
    }


def degrade_scan(root: Path) -> dict[str, Any]:
    """清单缺失/为空时的退化口径：只取最近 mtime 的若干文件。"""
    newest: list[dict[str, Any]] = []
    scanned = 0
    for sub in ("_staging", "data/local_store"):
        base = root / sub
        if not base.exists():
            continue
        for dirpath, _dirnames, filenames in os.walk(base):
            for fn in filenames:
                fp = Path(dirpath) / fn
                try:
                    st = fp.stat()
                except OSError:
                    continue
                scanned += 1
                newest.append(
                    {
                        "path": fp.relative_to(root).as_posix(),
                        "mtime_ns": st.st_mtime_ns,
                        "size": st.st_size,
                    }
                )
    newest.sort(key=lambda x: x["mtime_ns"], reverse=True)
    top = newest[:5]
    return {
        "scanned_files": scanned,
        "newest": top,
        "newest_mtime": (
            datetime.fromtimestamp(top[0]["mtime_ns"] / 1e9, tz=UTC).isoformat()
            if top
            else None
        ),
    }


# ---------------------------------------------------------------- 会话与台账


def session_activity(session_dir: Path = SESSION_DIR) -> dict[str, Any]:
    """信号 4：会话最后更新时间。读不到就标 unavailable，不猜。"""
    if not session_dir.exists():
        return {"state": "unavailable", "last_update": None, "path": str(session_dir)}
    latest: tuple[float, str] | None = None
    for p in session_dir.rglob("*"):
        if not p.is_file() or p.suffix not in {".jsonl", ".md"}:
            continue
        try:
            m = p.stat().st_mtime
        except OSError:
            continue
        if latest is None or m > latest[0]:
            latest = (m, p.as_posix())
    if latest is None:
        return {"state": "unavailable", "last_update": None, "path": str(session_dir)}
    return {
        "state": "available",
        "last_update": datetime.fromtimestamp(latest[0], tz=UTC).isoformat(),
        "last_update_ns": int(latest[0] * 1e9),
        "path": latest[1],
    }


def read_pending(path: Path = PENDING_PATH) -> dict[str, Any]:
    if not path.exists():
        return {"available": False, "path": str(path), "items": []}
    items: list[str] = []
    in_section = False
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        s = line.strip()
        if s.startswith("## "):
            in_section = s.startswith("## 待确认")
            continue
        if in_section and s.startswith("- "):
            items.append(s[2:].strip()[:300])
    return {"available": True, "path": str(path), "items": items}


# ---------------------------------------------------------------- 判定


def classify(
    *,
    counters_state: str,
    artifact_state: str,
    process_state: str,
    unchanged_rounds: int,
    prev_round_exists: bool,
    timeout_minutes: int,
) -> tuple[str, list[str]]:
    """多信号判定：单条信号不足以判 stalled，第 1、2 项缺失时一律 unknown。"""
    used = f"信号1(counters)={counters_state} / 信号2(artifacts)={artifact_state} / 信号3(process)={process_state}"
    if counters_state == "unavailable" and artifact_state == "unavailable":
        return "unknown", [used, "信号1、2 均不可用，按规则不得判 stalled"]
    if "changed" in (counters_state, artifact_state) or process_state == "active":
        return "healthy", [used, "本轮至少一条硬信号有变化"]
    if not prev_round_exists:
        return "unknown", [used, "首轮无上一轮快照，无法累计停滞轮次"]
    if unchanged_rounds >= STALL_ROUNDS:
        return "stalled", [
            used,
            f"三项硬信号连续 {unchanged_rounds} 轮均无变化",
            f"超时阈值 {timeout_minutes} 分钟",
        ]
    return "unknown", [used, f"硬信号本轮无变化，但仅 {unchanged_rounds} 轮，未达 {STALL_ROUNDS} 轮"]


def _load_previous(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def run(
    *,
    root: Path = ROOT,
    config_path: Path = DEFAULT_CONFIG,
    status_path: Path = DEFAULT_STATUS,
    alerts_path: Path = DEFAULT_ALERTS,
    pending_path: Path = PENDING_PATH,
    session_dir: Path = SESSION_DIR,
) -> dict[str, Any]:
    checked_at = datetime.now(tz=UTC).isoformat()
    prev = _load_previous(status_path)
    prev_tasks = {t.get("name"): t for t in prev.get("tasks", []) if isinstance(t, dict)}
    prev_procs = {p.get("pid"): p for p in prev.get("processes", []) if isinstance(p, dict)}

    tasks_cfg: list[dict[str, Any]] = []
    degraded, degraded_reason = False, None
    if config_path.exists():
        try:
            cfg = json.loads(config_path.read_text(encoding="utf-8"))
            tasks_cfg = [t for t in cfg.get("tasks", []) if isinstance(t, dict)]
        except Exception as exc:  # noqa: BLE001
            degraded, degraded_reason = True, f"config 解析失败: {type(exc).__name__}"
    else:
        degraded, degraded_reason = True, "config 不存在"
    if not tasks_cfg:
        degraded = True
        degraded_reason = degraded_reason or "config 中 tasks 为空"

    need_cmd = any(t.get("process_match") for t in tasks_cfg)
    procs = collect_processes(need_cmdline=need_cmd)
    proc_rows = []
    for p in procs:
        pv = prev_procs.get(p["pid"])
        delta = round(p["cpu_seconds"] - pv["cpu_seconds"], 3) if pv else p["cpu_seconds"]
        proc_rows.append({**p, "cpu_delta": delta})
    cpu_growth = round(sum(r["cpu_delta"] for r in proc_rows), 3) if proc_rows else None
    sess = session_activity(session_dir)

    results: list[dict[str, Any]] = []
    alerts: list[str] = []
    for t in tasks_cfg:
        name = str(t.get("name") or "unnamed")
        patterns = [str(x) for x in (t.get("artifacts") or [])]
        timeout_minutes = int(t.get("timeout_minutes") or 15)
        match = t.get("process_match")
        matched = procs if not match else [
            p for p in procs if p.get("cmdline") and match in p["cmdline"]
        ]

        art = collect_artifacts(root, patterns)
        prev_task = prev_tasks.get(name) or {}
        prev_art = prev_task.get("signals", {}).get("artifacts", {})
        prev_counters = prev_task.get("signals", {}).get("counters", {}).get("values", {}) or {}

        # 信号 1：计数变化
        if art is None or not art["counters"]:
            counters_state = "unavailable"
            counters_changed: list[str] = []
        else:
            if not prev_counters:
                counters_state = "unchanged"
                counters_changed = []
            else:
                changed = [
                    k for k, v in art["counters"].items() if prev_counters.get(k) != v
                ]
                counters_state = "changed" if changed else "unchanged"
                counters_changed = changed

        # 信号 2：产物 mtime / 大小
        if art is None:
            artifact_state = "unavailable"
        elif not isinstance(prev_art, dict) or "max_mtime_ns" not in prev_art:
            artifact_state = "changed"  # 首轮无基线，视为有活动，避免误判停滞
        else:
            same = (
                art["max_mtime_ns"] == prev_art.get("max_mtime_ns")
                and art["total_size"] == prev_art.get("total_size")
                and art["file_count"] == prev_art.get("file_count")
            )
            artifact_state = "unchanged" if same else "changed"

        # 信号 3：进程 CPU 增长
        if cpu_growth is None:
            process_state = "unavailable"
        elif matched and cpu_growth > 0:
            process_state = "active"
        else:
            process_state = "idle"

        idle_now = (
            counters_state in ("unchanged", "unavailable")
            and artifact_state in ("unchanged", "unavailable")
            and process_state in ("idle", "unavailable")
        )
        unchanged_rounds = (prev_task.get("unchanged_rounds", 0) + 1) if idle_now else 0

        status, reasons = classify(
            counters_state=counters_state,
            artifact_state=artifact_state,
            process_state=process_state,
            unchanged_rounds=unchanged_rounds,
            prev_round_exists=bool(prev_task),
            timeout_minutes=timeout_minutes,
        )
        results.append(
            {
                "name": name,
                "status": status,
                "reasons": reasons,
                "timeout_minutes": timeout_minutes,
                "process_match": match,
                "unchanged_rounds": unchanged_rounds,
                "signals": {
                    "counters": {
                        "state": counters_state,
                        "changed_keys": counters_changed,
                        "values": art["counters"] if art else {},
                    },
                    "artifacts": {
                        "state": artifact_state,
                        "max_mtime": art["max_mtime"] if art else None,
                        "max_mtime_ns": art["max_mtime_ns"] if art else None,
                        "silent_minutes": (
                            round((time.time() - art["max_mtime_ns"] / 1e9) / 60, 1)
                            if art
                            else None
                        ),
                        "total_size": art["total_size"] if art else None,
                        "file_count": art["file_count"] if art else None,
                        "files": art["files"] if art else [],
                    },
                    "processes": {
                        "state": process_state,
                        "matched_count": len(matched),
                        "pids": [p["pid"] for p in matched],
                        "cpu_growth": cpu_growth,
                    },
                    "session": {
                        "state": sess["state"],
                        "last_update": sess["last_update"],
                    },
                },
            }
        )
        if status == "stalled":
            alerts.append(f"{checked_at} [stalled] {name}: " + "；".join(reasons))

    status_doc = {
        "schema": SCHEMA,
        "checked_at": checked_at,
        "previous_checked_at": prev.get("checked_at"),
        "degraded": degraded,
        "degraded_reason": degraded_reason,
        "processes": proc_rows,
        "process_cpu_growth_total": cpu_growth,
        "session_activity": sess,
        "tasks": results,
        "degraded_scan": degrade_scan(root) if degraded else None,
        "pending_confirmations": read_pending(pending_path),
        "alert_count": len(alerts),
        "alerts": alerts,
    }

    write_text_atomic(
        json.dumps(status_doc, ensure_ascii=False, indent=2), status_path)

    if alerts:
        alerts_path.parent.mkdir(parents=True, exist_ok=True)
        with alerts_path.open("a", encoding="utf-8", newline="\n") as fh:
            for line in alerts:
                fh.write(line + "\n")
    return status_doc


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="纯本地巡检（不调用模型）")
    ap.add_argument("--root", default=str(ROOT))
    ap.add_argument("--config", default=None)
    ap.add_argument("--status-out", default=None)
    ap.add_argument("--alerts-out", default=None)
    ap.add_argument("--pending", default=str(PENDING_PATH))
    ap.add_argument("--session-dir", default=str(SESSION_DIR))
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args(argv)
    root = Path(a.root)
    doc = run(
        root=root,
        config_path=Path(a.config) if a.config else root / "config" / "patrol_targets.json",
        status_path=Path(a.status_out) if a.status_out else root / "logs" / "patrol_status.json",
        alerts_path=Path(a.alerts_out) if a.alerts_out else root / "logs" / "patrol_alerts.log",
        pending_path=Path(a.pending),
        session_dir=Path(a.session_dir),
    )
    if not a.quiet:
        summary = {
            "checked_at": doc["checked_at"],
            "degraded": doc["degraded"],
            "tasks": {t["name"]: t["status"] for t in doc["tasks"]},
            "processes": len(doc["processes"]),
            "cpu_growth": doc["process_cpu_growth_total"],
            "pending": len(doc["pending_confirmations"]["items"]),
            "alerts": doc["alert_count"],
        }
        print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
