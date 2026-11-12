"""sha256 大小写：小写比较 + 大写告警，格式问题不静默也不误判不一致（DG-31）。

判定句：
  ① 登记值是大写 hex 且与磁盘同值时，必须**不判不一致**（``ManifestResult.bad == 0``），
     同时**产生一条** ``sha256_format`` 格式告警；
  ② 登记值与磁盘不同值时，无论大小写都必须判 ``sha256`` 不一致。

改动前 ``check_record`` 用 ``actual != want_sha`` 直接比较，① 会得到一条
``kind='sha256'`` 的不一致，本文件第一个用例即失败，故它能对「大写写法被静默
判成不一致 / 格式告警缺失」这一缺陷失败。
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import polars as pl

from tools.manifest_tool import FactsCache, ManifestResult, Record, check_record


def _make_parquet(tmp_path: Path) -> tuple[Path, str]:
    path = tmp_path / "quotes" / "stock" / "sh" / "AAA.SH.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"date": ["2026-11-11"], "close": [1.0]}).write_parquet(path)
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def _issues(tmp_path: Path, path: Path, sha: str) -> tuple[list, ManifestResult]:
    record = Record(
        "files",
        path.relative_to(tmp_path).as_posix(),
        path,
        {"bytes": path.stat().st_size, "sha256": sha},
    )
    result = ManifestResult(rel="quotes/stock/sh/AAA.SH.manifest.json", data={})
    result.issues.extend(check_record(record, tmp_path, FactsCache()))
    return result.issues, result


def test_uppercase_same_hex_passes_with_format_warning(tmp_path: Path) -> None:
    path, real = _make_parquet(tmp_path)

    issues, result = _issues(tmp_path, path, real.upper())

    assert [i.kind for i in issues] == ["sha256_format"], [i.__dict__ for i in issues]
    assert result.format_alerts == 1
    assert result.bad == 0


def test_wrong_hex_fails_even_when_uppercase(tmp_path: Path) -> None:
    path, real = _make_parquet(tmp_path)
    wrong = ("0" if real[0] != "0" else "1") * 64

    issues, result = _issues(tmp_path, path, wrong)
    assert [i.kind for i in issues] == ["sha256"]
    assert result.bad == 1

    issues_upper, result_upper = _issues(tmp_path, path, wrong.upper())
    assert [i.kind for i in issues_upper] == ["sha256"]
    assert result_upper.bad == 1


def test_lowercase_same_hex_is_silent(tmp_path: Path) -> None:
    path, real = _make_parquet(tmp_path)

    issues, result = _issues(tmp_path, path, real)

    assert issues == []
    assert result.format_alerts == 0
    assert result.bad == 0
