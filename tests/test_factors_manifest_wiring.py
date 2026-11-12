"""factors 写盘收尾必须回写 manifest：真实写入口跑在临时 root，不碰正式数据。

判定句：
  ① ``_assemble`` 写入 ``factors/stock_turnover.parquet`` 后，``factors/manifest.json``
     的 rows / bytes / sha256 必须与磁盘一致，``--check`` 等价核对零不一致；
  ② 同一张表在「写盘但没回写」时（直接追加一行模拟），核对必须报不一致——
     这正是接线前每次写盘后的形态；
  ③ ``write_shares`` 写 ``meta/factors/stock_shares.parquet`` 后，同目录 manifest
     的统计字段必须被回写（``required=True``，登记缺失直接抛错而不是静默跳过）。
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl

from src.data.factors_manifest import write_factors_manifest
from src.data.local_store.share_capital import (
    SHARES_FILENAME,
    shares_path,
    write_shares,
)
from src.data.local_store.turnover import _assemble
from tools.manifest_tool import FactsCache, check_manifest


def _part(tmp_path: Path, rows: int = 3) -> Path:
    path = tmp_path / '_staging' / 'turnover_parts' / 'part_00000.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        'ts_code': ['510300.SH'] * rows,
        'trade_date': [date(2026, 1, day + 1) for day in range(rows)],
        'turn': [0.01] * rows,
        'source': ['baostock'] * rows,
        'fetched_at': [None] * rows,
    }).write_parquet(path)
    return path


def test_assemble_refreshes_factors_manifest(tmp_path: Path) -> None:
    root = tmp_path / 'store'
    target = root / 'factors' / 'stock_turnover.parquet'
    target.parent.mkdir(parents=True)

    total = _assemble([_part(tmp_path)], target, set())

    assert total == 3
    manifest = json.loads((root / 'factors' / 'manifest.json').read_text(encoding='utf-8'))
    entry = manifest['files']['stock_turnover.parquet']
    assert entry['rows'] == 3, entry
    assert entry['bytes'] == target.stat().st_size, entry
    assert check_manifest(root / 'factors' / 'manifest.json', root, FactsCache()).bad == 0


def test_write_without_refresh_is_reported_as_inconsistent(tmp_path: Path) -> None:
    root = tmp_path / 'store'
    target = root / 'factors' / 'stock_turnover.parquet'
    target.parent.mkdir(parents=True)
    _assemble([_part(tmp_path)], target, set())
    manifest_path = root / 'factors' / 'manifest.json'

    # 模拟「写盘但没回写」：直接追加一行，manifest 停在旧值
    combined = pl.concat([
        pl.read_parquet(target),
        pl.DataFrame({'ts_code': ['510300.SH'], 'trade_date': [date(2026, 1, 9)],
                      'turn': [0.02], 'source': ['baostock'], 'fetched_at': [None]}),
    ], how='vertical_relaxed')
    combined.write_parquet(target)

    stale = check_manifest(manifest_path, root, FactsCache())
    kinds = {issue.kind for issue in stale.issues}
    assert {'rows', 'bytes', 'sha256'} & kinds, sorted(kinds)
    assert stale.bad > 0

    # 回写后恢复一致：证明不一致确实来自「没回写」而不是别的缺陷
    write_factors_manifest(root)
    assert check_manifest(manifest_path, root, FactsCache()).bad == 0


def test_write_shares_refreshes_meta_factors_manifest(tmp_path: Path) -> None:
    root = tmp_path / 'store'
    manifest_path = root / 'meta' / 'factors' / 'manifest.json'
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps({
        'dataset': '股本区间事实表',
        'files': [{'path': f'meta/factors/{SHARES_FILENAME}', 'rows': 0,
                   'bytes': 0, 'sha256': '0' * 64}],
    }, ensure_ascii=False), encoding='utf-8')

    frame = pl.DataFrame({
        'ts_code': ['600000.SH', '600000.SH'],
        'effective_date': [date(2020, 1, 1), date(2021, 1, 1)],
        'total_share': [1_000_000, 2_000_000],
    })
    written = write_shares(frame, root, write_meta=False)

    assert written == shares_path(root)
    entry = json.loads(manifest_path.read_text(encoding='utf-8'))['files'][0]
    assert entry['rows'] == 2, entry
    assert entry['bytes'] == written.stat().st_size, entry
    assert check_manifest(manifest_path, root, FactsCache()).bad == 0
