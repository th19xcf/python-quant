"""``--no-write`` 语义回归：只算候选与对拍，磁盘不得新增任何文件。

背景：2026-11-13 复现文档口径时，``--no-write`` 仍在
``_staging/ca_rebuild_<日期>/`` 下落了 ``old_vs_new_diff.csv`` 与
``summary.json``——触发字面是「不落盘」而实际建了目录、写了两个文件。
本用例把「整棵树前后文件集合相同」固定成断言。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from tools import build_fund_corporate_actions as bca


def _table() -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': ['150008.SZ', '150009.SZ'],
        'date': [date(2020, 1, 2), date(2020, 3, 4)],
        'prev_close': [1.0, 1.0],
        'close': [1.0, 1.0],
        'gap': [0.0, 0.0],
        'adj_prev': [1.0, 1.0],
        'adj_now': [1.0, 1.0],
        'basis': ['test', 'test'],
    }, schema=bca.SCHEMA)


def _files(root: Path) -> set[str]:
    return {str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}


def test_no_write_keeps_file_set_unchanged(tmp_path, monkeypatch):
    store = tmp_path / 'local_store'
    target = store / bca.REL_PATH
    target.parent.mkdir(parents=True)
    _table().write_parquet(target)
    (store / 'meta' / bca.MANIFEST_NAME).write_text('{}', encoding='utf-8')

    stage_root = tmp_path / 'staging'
    stage_root.mkdir()
    monkeypatch.setattr(bca, 'get_config', lambda: object())
    monkeypatch.setattr(bca, 'resolve_store_root', lambda _cfg: store)
    monkeypatch.setattr(
        bca, 'staging_dir', lambda name, with_date=True: stage_root / name)
    monkeypatch.setattr(bca, 'build_events', lambda _root, _codes: _table())
    monkeypatch.setattr(
        bca, 'sub_threshold_stats', lambda *_a, **_k: {'count': 0})

    before = _files(tmp_path)
    assert bca.main(['--no-write']) == 0
    assert _files(tmp_path) == before
    assert not (stage_root / 'ca_rebuild').exists()
