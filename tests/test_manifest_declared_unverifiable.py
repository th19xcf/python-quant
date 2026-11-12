"""非 parquet 台账的 rows 核对口径：CSV 按数据行真核，取不到行数才允许声明。

判定句：
  ① CSV 登记了 ``rows`` 时，必须按「首行表头之外的数据行」真核；登记值与磁盘不符
     判 ``rows`` 不一致，相符则零 issue（不再进「不可核对」列）；
  ② 取不到行数的文件（JSON 注册表等）登记了 ``rows`` 且没有
     ``rows_unverifiable_reason`` 时，产生 ``rows_unverifiable``（不判不一致，但每轮
     都出现在「不可核对」列）；写了原因改为 ``rows_unverifiable_declared``，
     ``bad == 0`` 且不计入「不可核对」；
  ③ 声明原因只停用行数核对，``bytes`` / ``sha256`` 仍必须核对——登记值错了照样判不一致。

改动前 CSV 一律按「不可核对」处理，故本文件第一个用例（CSV 行数真核）会对旧实现失败。
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from tools.manifest_tool import FactsCache, ManifestResult, Record, check_record


def _make_csv(tmp_path: Path, body: str = "ts_code,trade_date\n588810.SH,2025-02-12\n") -> tuple[Path, dict]:
    path = tmp_path / "meta" / "anomalies" / "fund_fake_bar_bars.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path, {
        "rows": 1,
        "bytes": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def _make_json(tmp_path: Path) -> tuple[Path, dict]:
    path = tmp_path / "meta" / "anomalies" / "fund_pseudo_registry.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"spans": [{"ts_code": "159001.SZ"}]}', encoding="utf-8")
    return path, {
        "rows": 1,
        "bytes": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def _check(tmp_path: Path, path: Path, fields: dict) -> ManifestResult:
    record = Record("files", f"meta/anomalies/{path.name}", path, fields)
    result = ManifestResult(rel="meta/anomalies/manifest.json", data={})
    result.issues.extend(check_record(record, tmp_path, FactsCache()))
    return result


def test_csv_rows_are_verified_against_data_rows(tmp_path: Path) -> None:
    path, fields = _make_csv(tmp_path)

    result = _check(tmp_path, path, dict(fields))

    assert result.issues == [], [i.__dict__ for i in result.issues]
    assert result.unverifiable == 0
    assert result.unverifiable_declared == 0
    assert result.bad == 0


def test_csv_wrong_row_count_is_inconsistent(tmp_path: Path) -> None:
    path, fields = _make_csv(tmp_path)
    fields["rows"] = 2

    result = _check(tmp_path, path, dict(fields))

    assert [i.kind for i in result.issues] == ["rows"]
    assert result.bad == 1


def test_csv_embedded_newline_counts_one_record(tmp_path: Path) -> None:
    path, fields = _make_csv(tmp_path, body='ts_code,note\n588810.SH,"第一行\n第二行"\n')

    result = _check(tmp_path, path, dict(fields))

    assert result.issues == [], [i.__dict__ for i in result.issues]


def test_undeclared_rows_are_flagged_as_unverifiable(tmp_path: Path) -> None:
    path, fields = _make_json(tmp_path)

    result = _check(tmp_path, path, dict(fields))

    assert [i.kind for i in result.issues] == ["rows_unverifiable"]
    assert result.unverifiable == 1
    assert result.unverifiable_declared == 0
    assert result.bad == 0


def test_declared_reason_is_not_noise(tmp_path: Path) -> None:
    path, fields = _make_json(tmp_path)
    fields["rows_unverifiable_reason"] = "JSON 注册表，rows=登记条目数"

    result = _check(tmp_path, path, dict(fields))

    assert [i.kind for i in result.issues] == ["rows_unverifiable_declared"]
    assert result.unverifiable == 0
    assert result.unverifiable_declared == 1
    assert result.bad == 0


def test_declared_reason_does_not_disable_sha_check(tmp_path: Path) -> None:
    path, fields = _make_json(tmp_path)
    fields["rows_unverifiable_reason"] = "JSON 注册表，rows=登记条目数"
    fields["sha256"] = "0" * 64

    result = _check(tmp_path, path, dict(fields))

    kinds = [i.kind for i in result.issues]
    assert "sha256" in kinds, kinds
    assert result.bad >= 1
