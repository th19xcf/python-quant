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
    record_move,
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


def test_rejects_bad_action_and_out_of_repo_absolute_path(tmp_path, monkeypatch):
    monkeypatch.setattr("src.data.changelog.repo_root", lambda: tmp_path)
    csv_file = tmp_path / "c.csv"
    with pytest.raises(ValueError, match="action 只能是"):
        append_event(action="remove", path="reports/a", task="t", reason="r", sha256="x", size_bytes=1, csv_file=csv_file)
    with pytest.raises(ValueError, match="无法相对化为仓库内路径"):
        append_event(
            action="delete",
            path=r"C:\Windows\System32\drivers\etc\hosts",
            task="t", reason="r", sha256="x" * 64, size_bytes=1, csv_file=csv_file,
        )


def test_move_action_rejected_with_hint(tmp_path):
    csv_file = tmp_path / "c.csv"
    with pytest.raises(ValueError, match="record_move"):
        append_event(
            action="move", path="reports/a", task="t", reason="r",
            sha256="", size_bytes=None, csv_file=csv_file,
        )
    assert not csv_file.exists()


def test_path_normalized_to_repo_relative_forward_slash(tmp_path, monkeypatch):
    monkeypatch.setattr("src.data.changelog.repo_root", lambda: tmp_path)
    csv_file = tmp_path / "c.csv"
    row = append_event(
        action="create",
        path=str(tmp_path / "data" / "local_store" / "x.parquet"),
        task="t", reason="r", sha256="a" * 64, size_bytes=1, csv_file=csv_file,
    )
    assert row["path"] == "data/local_store/x.parquet"

    row2 = append_event(
        action="update",
        path=r"data\local_store\y.parquet",
        task="t", reason="r", sha256="b" * 64, size_bytes=2, csv_file=csv_file,
        dedupe_window_seconds=0,
    )
    assert row2["path"] == "data/local_store/y.parquet"


def test_unknown_bytes_written_as_empty_not_minus_one(tmp_path):
    csv_file = tmp_path / "c.csv"
    row = append_event(
        action="update", path="reports/a", task="t", reason="r",
        sha256="a" * 64, size_bytes=None, csv_file=csv_file,
    )
    assert row["bytes"] == ""
    assert _rows(csv_file)[0]["bytes"] == ""
    with pytest.raises(ValueError, match="不要传负数哨兵值"):
        append_event(
            action="update", path="reports/b", task="t", reason="r",
            sha256="b" * 64, size_bytes=-1, csv_file=csv_file,
        )
    assert len(_rows(csv_file)) == 1


def test_record_move_splits_into_delete_and_create(tmp_path, monkeypatch):
    monkeypatch.setattr("src.data.changelog.repo_root", lambda: tmp_path)
    src = tmp_path / "data" / "local_store" / "old.parquet"
    dst = tmp_path / "data" / "local_store" / "new.parquet"
    src.parent.mkdir(parents=True)
    src.write_bytes(b"same-content")
    dst.write_bytes(b"same-content")
    csv_file = tmp_path / "logs" / "data_changelog.csv"

    rows = record_move(
        src_path="data/local_store/old.parquet",
        dst_path="data/local_store/new.parquet",
        task="move_task",
        reason="改名为语义口径",
        csv_file=csv_file,
    )

    assert [r["action"] for r in rows] == ["delete", "create"]
    stored = _rows(csv_file)
    assert [r["action"] for r in stored] == ["delete", "create"]
    assert {r["action"] for r in stored} <= {"create", "update", "delete"}
    assert stored[0]["path"] == "data/local_store/old.parquet"
    assert stored[1]["path"] == "data/local_store/new.parquet"
    assert stored[1]["prev_sha256"] == stored[0]["sha256"]
    assert stored[1]["sha256"] == file_sha256(dst)


def test_refuses_to_append_after_truncated_tail(tmp_path):
    csv_file = tmp_path / "c.csv"
    csv_file.write_bytes((",".join(FIELDS) + "\n").encode() + b"r1,2026-11-12T00:00:00,t,create,reports/a,1,")
    before = csv_file.read_bytes()
    with pytest.raises(ValueError, match="末行未以换行结尾"):
        append_event(
            action="create", path="reports/a", task="t", reason="r",
            sha256="a" * 64, size_bytes=1, csv_file=csv_file,
        )
    assert csv_file.read_bytes() == before


def test_output_is_lf_utf8_without_bom(tmp_path):
    csv_file = tmp_path / "c.csv"
    append_event(
        action="create", path="reports/中文.parquet", task="t", reason="r",
        sha256="a" * 64, size_bytes=1, csv_file=csv_file,
    )
    raw = csv_file.read_bytes()
    assert not raw.startswith(b"\xef\xbb\xbf")
    assert b"\r" not in raw
    assert raw.endswith(b"\n")
    raw.decode("utf-8")


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


def test_reason_with_comma_newline_quote_roundtrips(tmp_path):
    """reason 含逗号/换行/双引号时落盘再读回必须逐字相等，且每行列数恒等于表头。

    背景：2026-11-13 台账第 10365 行（task=b2s2_breadth_rebuild）的 reason 里
    「6,819 天」的逗号未加引号，整行被解析成 11 列，check_changelog 直接判 FAIL。
    """
    csv_file = tmp_path / "data_changelog.csv"
    reason = '全量重建：口径 6,819 天；备注 "双引号" 与 单引号\n第二行内容'
    append_event(
        action="update",
        path="factors/breadth_daily.parquet",
        task="roundtrip",
        reason=reason,
        sha256="b" * 64,
        size_bytes=4096,
        csv_file=csv_file,
    )

    rows = _rows(csv_file)
    assert len(rows) == 1
    assert rows[0]["reason"] == reason

    with open(csv_file, encoding="utf-8", newline="") as f:
        parsed = list(csv.reader(f))
    assert all(len(cells) == len(FIELDS) for cells in parsed), parsed
    assert len(parsed) == 2  # 表头 + 1 条记录，字段内换行不得额外成行
