"""非 parquet 台账的 rows 不可核对：未声明原因要照明，声明原因后不计噪声。

判定句：
  ① 登记了 ``rows`` 但文件不是 parquet、且没有 ``rows_unverifiable_reason`` 时，
     必须产生 ``rows_unverifiable``（不判不一致，但每轮都出现在「不可核对」列）；
  ② 条目写了 ``rows_unverifiable_reason`` 时，改为 ``rows_unverifiable_declared``，
     ``bad == 0`` 且不再计入「不可核对」；
  ③ 声明原因只停用行数核对，``bytes`` / ``sha256`` 仍必须核对——登记值错了照样判不一致。

本文件第二个用例在改动前会拿到 ``rows_unverifiable``（声明的键被忽略），故能对
「已声明不可核对仍被当噪声」这一缺陷失败。
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from tools.manifest_tool import FactsCache, ManifestResult, Record, check_record


def _make_csv(tmp_path: Path) -> tuple[Path, dict]:
    path = tmp_path / "meta" / "anomalies" / "fund_fake_bar_bars.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("ts_code,trade_date\n588810.SH,2025-02-12\n", encoding="utf-8")
    return path, {
        "rows": 1,
        "bytes": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def _check(tmp_path: Path, path: Path, fields: dict) -> ManifestResult:
    record = Record("files", "meta/anomalies/fund_fake_bar_bars.csv", path, fields)
    result = ManifestResult(rel="meta/anomalies/manifest.json", data={})
    result.issues.extend(check_record(record, tmp_path, FactsCache()))
    return result


def test_undeclared_rows_are_flagged_as_unverifiable(tmp_path: Path) -> None:
    path, fields = _make_csv(tmp_path)

    result = _check(tmp_path, path, dict(fields))

    assert [i.kind for i in result.issues] == ["rows_unverifiable"]
    assert result.unverifiable == 1
    assert result.unverifiable_declared == 0
    assert result.bad == 0


def test_declared_reason_is_not_noise(tmp_path: Path) -> None:
    path, fields = _make_csv(tmp_path)
    fields["rows_unverifiable_reason"] = "CSV 台账，无 parquet 行数可核"

    result = _check(tmp_path, path, dict(fields))

    assert [i.kind for i in result.issues] == ["rows_unverifiable_declared"]
    assert result.unverifiable == 0
    assert result.unverifiable_declared == 1
    assert result.bad == 0


def test_declared_reason_does_not_disable_sha_check(tmp_path: Path) -> None:
    path, fields = _make_csv(tmp_path)
    fields["rows_unverifiable_reason"] = "CSV 台账，无 parquet 行数可核"
    fields["sha256"] = "0" * 64

    result = _check(tmp_path, path, dict(fields))

    kinds = [i.kind for i in result.issues]
    assert "sha256" in kinds, kinds
    assert result.bad >= 1
