"""`tools/patrol_local.py` 的可重复验收用例（不依赖真实进程与真实时间间隔）。

覆盖四个场景：
1. 结构稳定：连跑三轮，`patrol_status.json` 可被 json 解析且顶层键一致；
2. 产物不动：无子进程 + 计数与 mtime 都不动，第三轮判 `stalled` 并写 alert；
3. 产物在动：子进程 CPU 增长 + 计数递增，三轮都 `healthy` 且不写 alert；
4. 信号 1、2 均不可用：只允许 `unknown`，不得判 `stalled`。
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools import patrol_local as pl_mod

TOP_KEYS = {
    "schema",
    "checked_at",
    "previous_checked_at",
    "degraded",
    "degraded_reason",
    "processes",
    "process_cpu_growth_total",
    "session_activity",
    "tasks",
    "degraded_scan",
    "pending_confirmations",
    "alert_count",
    "alerts",
}


def _setup(tmp_path: Path, artifacts: list[str], *, timeout: int = 15) -> dict[str, Path]:
    (tmp_path / "_staging" / "task").mkdir(parents=True, exist_ok=True)
    cfg = tmp_path / "config" / "patrol_targets.json"
    cfg.parent.mkdir(parents=True, exist_ok=True)
    cfg.write_text(
        json.dumps(
            {
                "schema": 2,
                "tasks": [
                    {
                        "name": "demo",
                        "artifacts": artifacts,
                        "timeout_minutes": timeout,
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return {
        "root": tmp_path,
        "config": cfg,
        "status": tmp_path / "logs" / "patrol_status.json",
        "alerts": tmp_path / "logs" / "patrol_alerts.log",
        "pending": tmp_path / "pending.md",
        "session": tmp_path / "session",
    }


def _run(paths: dict[str, Path]) -> dict:
    return pl_mod.run(
        root=paths["root"],
        config_path=paths["config"],
        status_path=paths["status"],
        alerts_path=paths["alerts"],
        pending_path=paths["pending"],
        session_dir=paths["session"],
    )


@pytest.fixture
def no_processes(monkeypatch):
    monkeypatch.setattr(pl_mod, "collect_processes", lambda need_cmdline=False: [])


def test_status_structure_stable_across_three_rounds(tmp_path, no_processes):
    paths = _setup(tmp_path, ["_staging/task/progress.json"])
    art = tmp_path / "_staging" / "task" / "progress.json"
    art.write_text(json.dumps({"done": 1, "total": 10}), encoding="utf-8")

    keysets = []
    for _ in range(3):
        doc = _run(paths)
        reloaded = json.loads(paths["status"].read_text(encoding="utf-8"))
        assert reloaded == doc
        keysets.append(set(reloaded))
    assert all(k == TOP_KEYS for k in keysets)
    assert doc["schema"] == 2
    assert doc["checked_at"] and doc["previous_checked_at"]


def test_stalled_after_two_idle_rounds_and_alert(tmp_path, no_processes):
    paths = _setup(tmp_path, ["_staging/task/progress.json"], timeout=1)
    art = tmp_path / "_staging" / "task" / "progress.json"
    art.write_text(json.dumps({"done": 1}), encoding="utf-8")

    r1 = _run(paths)
    r2 = _run(paths)
    r3 = _run(paths)

    assert r1["tasks"][0]["status"] == "healthy"  # 首轮无基线，产物视为有活动
    assert r2["tasks"][0]["status"] == "unknown"
    assert r2["tasks"][0]["unchanged_rounds"] == 1
    assert r3["tasks"][0]["status"] == "stalled"
    assert r3["tasks"][0]["unchanged_rounds"] == 2
    assert r3["alert_count"] == 1
    assert "不得判 stalled" not in " ".join(r3["tasks"][0]["reasons"])

    lines = paths["alerts"].read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    assert "[stalled] demo" in lines[0]
    # 前两轮未产生 alert：文件只有第三轮那一条
    assert paths["alerts"].exists()


def test_active_artifacts_never_alert(tmp_path, monkeypatch):
    paths = _setup(tmp_path, ["_staging/task/progress.json"])
    art = tmp_path / "_staging" / "task" / "progress.json"

    counter = {"n": 0}

    def fake_procs(need_cmdline=False):
        counter["n"] += 1
        return [
            {
                "pid": 4242,
                "name": "python.exe",
                "start_time": "2026-11-10T00:00:00+00:00",
                "cpu_seconds": 10.0 * counter["n"],
                "cmdline": "python demo_task.py",
            }
        ]

    monkeypatch.setattr(pl_mod, "collect_processes", fake_procs)

    statuses = []
    for i in range(1, 4):
        art.write_text(json.dumps({"done": i}), encoding="utf-8")
        doc = _run(paths)
        statuses.append(doc["tasks"][0]["status"])
        assert doc["alert_count"] == 0
    assert statuses == ["healthy", "healthy", "healthy"]
    assert not paths["alerts"].exists()
    doc = json.loads(paths["status"].read_text(encoding="utf-8"))
    assert doc["tasks"][0]["signals"]["counters"]["state"] == "changed"
    assert doc["tasks"][0]["signals"]["processes"]["state"] == "active"


def test_unknown_when_signals_one_and_two_unavailable(tmp_path, no_processes):
    paths = _setup(tmp_path, ["_staging/missing/progress.json"])
    docs = [_run(paths) for _ in range(3)]
    for d in docs:
        assert d["tasks"][0]["status"] == "unknown"
        assert d["alert_count"] == 0
    assert "不得判 stalled" in " ".join(docs[-1]["tasks"][0]["reasons"])
    assert not paths["alerts"].exists()


def test_degraded_when_config_missing(tmp_path, no_processes):
    root = tmp_path
    (root / "_staging" / "x").mkdir(parents=True)
    (root / "_staging" / "x" / "a.parquet").write_bytes(b"x")
    doc = pl_mod.run(
        root=root,
        config_path=root / "config" / "none.json",
        status_path=root / "logs" / "s.json",
        alerts_path=root / "logs" / "a.log",
        pending_path=root / "pending.md",
        session_dir=root / "session",
    )
    assert doc["degraded"] is True
    assert doc["tasks"] == []
    assert doc["degraded_scan"]["scanned_files"] >= 1
    assert doc["degraded_scan"]["newest"][0]["path"] == "_staging/x/a.parquet"


def test_pending_confirmations_are_listed(tmp_path, no_processes):
    paths = _setup(tmp_path, ["_staging/task/progress.json"])
    (tmp_path / "_staging" / "task" / "progress.json").write_text("{}", encoding="utf-8")
    paths["pending"].write_text(
        "# 台账\n\n## 待确认\n\n- 条目甲：需要点头\n- 条目乙：需要点头\n\n## 历史记录\n\n- 旧条目\n",
        encoding="utf-8",
    )
    doc = _run(paths)
    assert doc["pending_confirmations"]["available"] is True
    assert doc["pending_confirmations"]["items"] == ["条目甲：需要点头", "条目乙：需要点头"]


def test_module_has_no_delete_calls():
    src = Path(pl_mod.__file__).read_text(encoding="utf-8")
    for bad in ("unlink(", "rmtree(", "os.remove(", ".unlink("):
        assert bad not in src, f"巡检脚本不得包含删除调用: {bad}"
