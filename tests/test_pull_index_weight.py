"""`tools/pull_index_weight.py` 的 manifest 合并语义用例。

背景：`phase_build()` 过去整文件重写 manifest，会把人工补登的 `files` 段
（文件级 rows/bytes/sha256/registered_at）直接冲掉。改为合并语义后，重跑
只刷新工具自己算得出的字段，工具未管理的键原样保留。

`test_same_assertion_goes_red_*` 两条用于自证断言能失败：同一条断言套在
旧实现（整文件重写）与手工篡改的样例上必须报错。
"""

from __future__ import annotations

import copy
import json

import pytest

from tools import pull_index_weight as piw

PREVIOUS = {
    "dataset": "index_members",
    "generated_at": "2026-11-11T23:00:00+08:00",
    "source": "tushare.index_weight",
    "semantics": "旧语义文本",
    "fields": {"index_code": "旧字段说明"},
    "raw_cache": "tmp/index_weight_raw",
    "coverage": {"000300.SH": {"periods": 1}},
    "files": [
        {
            "path": "meta/index_members/index_weight_monthly.parquet",
            "rows": 375727,
            "bytes": 841723,
            "sha256": "3b079cd44fa191dd2bf48bec6885d9aaaa64bc6a50bacb42838284f9e3487395",
            "registered_at": "2026-11-12",
            "registered_reason": "人工补登",
        }
    ],
    "total_rows": 375727,
    "total_periods": 250,
}


def _fresh_manifest() -> dict:
    """模拟一次 `phase_build()` 产出的工具字段。"""
    return {
        "dataset": "index_members",
        "generated_at": "2026-11-12T14:00:00+08:00",
        "source": "tushare.index_weight",
        "semantics": "新语义文本",
        "fields": {"index_code": "新字段说明"},
        "raw_cache": "tmp/index_weight_raw",
        "coverage": {"000300.SH": {"periods": 2}},
        "membership_intervals": {"codes": 3},
        "total_rows": 400000,
        "total_periods": 251,
    }


def _assert_manual_preserved(merged: dict, previous: dict) -> None:
    """工具未管理的键必须逐字段原样保留；缺失或改动即失败。"""
    assert "files" in merged, "files 段被整文件重写抹掉"
    assert merged["files"] == previous["files"], "files 段被改动"
    tool_keys = set(_fresh_manifest())
    for key, value in previous.items():
        if key in tool_keys:
            continue
        assert key in merged, f"{key} 被抹掉"
        assert merged[key] == value, f"{key} 被改动"


def test_merge_refreshes_tool_fields_and_keeps_manual() -> None:
    merged = piw._merge_manifest(PREVIOUS, _fresh_manifest())
    _assert_manual_preserved(merged, PREVIOUS)
    assert merged["generated_at"] == "2026-11-12T14:00:00+08:00"
    assert merged["total_rows"] == 400000
    assert merged["total_periods"] == 251
    assert merged["coverage"] == {"000300.SH": {"periods": 2}}
    assert merged["membership_intervals"] == {"codes": 3}


def test_rerun_twice_keeps_manual_fields(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(piw, "MANIFEST_PATH", tmp_path / "manifest.json")
    piw.MANIFEST_PATH.write_text(
        json.dumps(PREVIOUS, ensure_ascii=False, indent=2), encoding="utf-8")
    previous = piw._load_existing_manifest()
    for _ in range(2):
        merged = piw._merge_manifest(previous, _fresh_manifest())
        piw.MANIFEST_PATH.write_text(
            json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
        previous = piw._load_existing_manifest()
    _assert_manual_preserved(previous, PREVIOUS)
    assert previous["total_rows"] == 400000


def test_same_assertion_goes_red_on_old_overwrite_behaviour() -> None:
    """旧实现整文件重写、只剩 fresh —— 同一条断言必须失败。"""
    old_style = dict(_fresh_manifest())
    with pytest.raises(AssertionError):
        _assert_manual_preserved(old_style, PREVIOUS)


def test_same_assertion_goes_red_on_tampered_files() -> None:
    """手工篡改 files 里一个 sha256 —— 同一条断言必须失败。"""
    merged = piw._merge_manifest(PREVIOUS, _fresh_manifest())
    tampered = copy.deepcopy(merged)
    tampered["files"][0]["sha256"] = "0" * 64
    with pytest.raises(AssertionError):
        _assert_manual_preserved(tampered, PREVIOUS)


def test_load_existing_manifest_missing_file_returns_empty(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(piw, "MANIFEST_PATH", tmp_path / "manifest.json")
    assert piw._load_existing_manifest() == {}


def test_load_existing_manifest_refuses_corrupt(tmp_path, monkeypatch) -> None:
    path = tmp_path / "manifest.json"
    path.write_text("{not json", encoding="utf-8")
    monkeypatch.setattr(piw, "MANIFEST_PATH", path)
    with pytest.raises(RuntimeError):
        piw._load_existing_manifest()
