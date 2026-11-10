"""`src/data/changelog.py` 的写入契约测试。"""

from __future__ import annotations

import csv

import pytest

from src.data.changelog import FIELDS, append_event, file_sha256, record_delete


def _rows(path):
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


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
    rows = _rows(csv_file)
    assert len(rows) == 1
    assert list(rows[0].keys()) == FIELDS
    assert rows[0]["action"] == "delete"
    assert rows[0]["bytes"] == "123"


def test_upgrade_old_header_keeps_rows_and_values(tmp_path):
    csv_file = tmp_path / "data_changelog.csv"
    old = ["run_id", "timestamp", "task", "action", "path", "bytes", "sha256", "prev_sha256", "operator"]
    with open(csv_file, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(old)
        w.writerow(["r1", "2026-11-05T01:00:00", "t1", "create", "quotes/a.parquet", "10", "h", "", "system"])
        w.writerow(["r2", "2026-11-05T02:00:00", "t1", "update", "quotes/a.parquet", "12", "h2", "h", "system"])

    append_event(
        action="delete",
        path="reports/x.parquet",
        task="t2",
        reason="r",
        sha256="b" * 64,
        size_bytes=1,
        csv_file=csv_file,
    )

    rows = _rows(csv_file)
    assert len(rows) == 3
    assert rows[0]["path"] == "quotes/a.parquet"
    assert rows[0]["reason"] == ""
    assert rows[1]["prev_sha256"] == "h"
    assert rows[2]["reason"] == "r"


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
