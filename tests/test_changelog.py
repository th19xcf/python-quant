"""`src/data/changelog.py` 的写入契约测试。"""

from __future__ import annotations

import csv

import pytest

from src.data.changelog import (
    FIELDS,
    append_event,
    file_sha256,
    read_events,
    record_delete,
)

OLD_HEADER = FIELDS[:-1]
OLD_ROWS = [
    ["r1", "2026-11-05T01:00:00", "t1", "create", "quotes/a.parquet", "10", "h", "", "system"],
    ["r2", "2026-11-05T02:00:00", "t1", "update", "quotes/a.parquet", "12", "h2", "h", "system"],
]


def _rows(path):
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _write_old_format(csv_file) -> None:
    with open(csv_file, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(OLD_HEADER)
        w.writerows(OLD_ROWS)


def test_append_event_writes_all_fields(tmp_path):
    csv_file = tmp_path / "data_changelog.csv"
    row = append_event(
        action="delete",
        path="reports/demo/panel.parquet",
        task="cleanup_demo",
        reason="一次性临时产物",
        sha256="a" * 64,
        size_bytes=123,
        csv_file=csv_file,
    )
    assert row["reason"] == "一次性临时产物"
    assert row["duplicate"] == "false"
    rows = _rows(csv_file)
    assert len(rows) == 1
    assert list(rows[0].keys()) == FIELDS
    assert rows[0]["action"] == "delete"
    assert rows[0]["bytes"] == "123"


def test_upgrade_keeps_history_bytes_identical(tmp_path):
    """列升级只改首行：历史行必须逐字节不变，diff 里只允许出现表头与新行。"""
    csv_file = tmp_path / "data_changelog.csv"
    _write_old_format(csv_file)
    before = csv_file.read_bytes().splitlines(keepends=True)
    history_bytes = before[1:]

    append_event(
        action="delete",
        path="reports/x.parquet",
        task="t2",
        reason="r",
        sha256="b" * 64,
        size_bytes=1,
        csv_file=csv_file,
    )

    after = csv_file.read_bytes().splitlines(keepends=True)
    assert after[1 : 1 + len(history_bytes)] == history_bytes
    assert after[0].decode("utf-8").rstrip("\r\n") == ",".join(FIELDS)
    # 历史行仍是 9 列，只有新追加的一行为 10 列
    assert len(next(csv.reader([after[1].decode("utf-8")]))) == 9
    assert len(next(csv.reader([after[-1].decode("utf-8")]))) == len(FIELDS)


def test_read_events_pads_short_rows(tmp_path):
    csv_file = tmp_path / "data_changelog.csv"
    _write_old_format(csv_file)
    csv_file.write_bytes(
        (",".join(FIELDS) + "\n").encode()
        + csv_file.read_bytes().split(b"\n", 1)[1]
    )
    events = read_events(csv_file)
    assert len(events) == 2
    assert events[0]["path"] == "quotes/a.parquet"
    assert events[0]["reason"] == ""
    assert events[1]["prev_sha256"] == "h"


def test_duplicate_within_window_written_once(tmp_path):
    csv_file = tmp_path / "data_changelog.csv"
    kwargs = {
        "action": "delete",
        "path": "data/local_store/x.parquet",
        "task": "cleanup",
        "reason": "残留",
        "sha256": "c" * 64,
        "size_bytes": 7,
        "csv_file": csv_file,
        "timestamp": "2026-11-10T15:50:38",
    }
    first = append_event(**kwargs)
    second = append_event(**kwargs)

    assert first["duplicate"] == "false"
    assert second["duplicate"] == "true"
    assert second["sha256"] == kwargs["sha256"]
    assert len(read_events(csv_file)) == 1


def test_duplicate_outside_window_written_again(tmp_path):
    csv_file = tmp_path / "data_changelog.csv"
    base = {
        "action": "delete",
        "path": "data/local_store/x.parquet",
        "task": "cleanup",
        "reason": "残留",
        "sha256": "d" * 64,
        "size_bytes": 7,
        "csv_file": csv_file,
    }
    append_event(**base, timestamp="2026-11-10T10:00:00")
    later = append_event(**base, timestamp="2026-11-10T10:06:00")
    assert later["duplicate"] == "false"
    assert len(read_events(csv_file)) == 2


def test_dedupe_can_be_disabled(tmp_path):
    csv_file = tmp_path / "data_changelog.csv"
    base = {
        "action": "delete",
        "path": "data/local_store/x.parquet",
        "task": "cleanup",
        "reason": "残留",
        "sha256": "e" * 64,
        "size_bytes": 7,
        "csv_file": csv_file,
    }
    append_event(**base, dedupe_window_seconds=0)
    append_event(**base, dedupe_window_seconds=0)
    assert len(read_events(csv_file)) == 2


def test_rejects_bad_action_and_absolute_path(tmp_path):
    csv_file = tmp_path / "c.csv"
    with pytest.raises(ValueError):
        append_event(action="remove", path="reports/a", task="t", reason="r", sha256="x", size_bytes=1, csv_file=csv_file)
    with pytest.raises(ValueError):
        append_event(action="delete", path=r"E:\code\python\quant\reports\a", task="t", reason="r", sha256="x", size_bytes=1, csv_file=csv_file)


def test_record_delete_reads_size_and_hash(tmp_path, monkeypatch):
    target = tmp_path / "reports" / "gone.parquet"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"payload")

    monkeypatch.setattr("src.data.changelog.repo_root", lambda: tmp_path)
    csv_file = tmp_path / "logs" / "data_changelog.csv"
    row = record_delete(path="reports/gone.parquet", task="cleanup", reason="临时", csv_file=csv_file)

    assert row["bytes"] == str(len(b"payload"))
    assert row["sha256"] == file_sha256(target)
    assert row["path"] == "reports/gone.parquet"


def test_rejects_short_sha256(tmp_path):
    """16 位短哈希必须被拒，且不落盘——否则写进去的记录永远对不上任何文件。"""
    csv_file = tmp_path / "c.csv"
    with pytest.raises(ValueError, match="期望 64 位十六进制，实际 16 位"):
        append_event(
            action="update",
            path="reports/a",
            task="t",
            reason="r",
            sha256="0" * 16,
            size_bytes=1,
            csv_file=csv_file,
        )
    assert not csv_file.exists()


def test_rejects_non_hex_sha256(tmp_path):
    csv_file = tmp_path / "c.csv"
    with pytest.raises(ValueError, match="期望 64 位十六进制，实际 64 位"):
        append_event(
            action="create",
            path="reports/a",
            task="t",
            reason="r",
            sha256="g" * 64,
            size_bytes=1,
            csv_file=csv_file,
        )
    assert not csv_file.exists()


def test_uppercase_sha256_normalized_to_lower(tmp_path):
    csv_file = tmp_path / "c.csv"
    row = append_event(
        action="create",
        path="reports/a",
        task="t",
        reason="r",
        sha256="A" * 64,
        size_bytes=1,
        csv_file=csv_file,
    )
    assert row["sha256"] == "a" * 64
    assert _rows(csv_file)[0]["sha256"] == "a" * 64
