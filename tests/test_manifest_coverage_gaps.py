"""G-106：coverage_gaps 不得对 quotes/ 整段豁免。

判定句：``quotes/`` 下存在一个没有侧车 manifest 的 parquet 时，
``coverage_gaps(...)['no_manifest']`` 的计数必须从 0 增到 1；目录里补上任意
manifest 后同一文件应移入 ``not_located``；补上指向它的侧车记录后应归零。

改动前 ``coverage_gaps`` 有一行 ``if rel.startswith('quotes/'): continue``，
本文件的三个用例都会失败（计数恒为 0），故它能对「quotes 被整段跳过」这一
缺陷失败（DG-31）。
"""

from __future__ import annotations

from pathlib import Path

from tools.manifest_tool import ManifestResult, Record, coverage_gaps


def _touch(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'')
    return path


def _total(gaps: dict[str, dict[str, list[str]]], bucket: str) -> int:
    return sum(len(files) for files in gaps.get(bucket, {}).values())


def test_quotes_parquet_without_manifest_is_counted(tmp_path: Path) -> None:
    baseline = coverage_gaps([], tmp_path)
    assert _total(baseline, 'no_manifest') == 0

    _touch(tmp_path / 'quotes' / 'stock' / 'sh' / 'AAA.SH.parquet')
    gaps = coverage_gaps([], tmp_path)

    assert _total(gaps, 'no_manifest') == 1
    assert gaps['no_manifest']['quotes/stock/sh'] == ['quotes/stock/sh/AAA.SH.parquet']


def test_directory_manifest_moves_gap_to_not_located(tmp_path: Path) -> None:
    _touch(tmp_path / 'quotes' / 'index' / 'sz' / 'BBB.SZ.parquet')
    (tmp_path / 'quotes' / 'index' / 'sz' / 'manifest.json').write_text(
        '{}', encoding='utf-8')

    gaps = coverage_gaps([], tmp_path)

    assert _total(gaps, 'no_manifest') == 0
    assert _total(gaps, 'not_located') == 1


def test_sidecar_record_clears_gap(tmp_path: Path) -> None:
    shard = _touch(tmp_path / 'quotes' / 'fund' / 'sh' / '510300.SH.parquet')
    sidecar = tmp_path / 'quotes' / 'fund' / 'sh' / '510300.SH.manifest.json'
    sidecar.write_text('{}', encoding='utf-8')

    result = ManifestResult(rel='quotes/fund/sh/510300.SH.manifest.json', data={})
    result.records.append(
        Record('top', 'quotes/fund/sh/510300.SH.parquet', shard, {'rows': 0, 'bytes': 0}))

    gaps = coverage_gaps([result], tmp_path)

    assert _total(gaps, 'no_manifest') == 0
    assert _total(gaps, 'not_located') == 0
