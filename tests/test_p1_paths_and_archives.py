#!/usr/bin/env python3

"""P1：本地数据路径收口 + 归档只读盘点的回归测试。"""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.data.local_store.paths import (
    DEFAULT_STORE_PATH,
    default_store_root,
    freshness_artifacts_dir,
    project_root,
    resolve_store_root,
)
from src.data.local_store.store import resolve_store_root as legacy_resolve_store_root
from src.data.staging import STAGING_ROOT, staging_dir
from tools import cleanup_archives as archives
from tools import sync_daily_non_git as daily_sync


def test_paths_are_absolute_and_cwd_independent(tmp_path: Path,
                                                 monkeypatch: pytest.MonkeyPatch) -> None:
    root = project_root()
    assert root.is_absolute()
    assert default_store_root() == root / DEFAULT_STORE_PATH
    assert freshness_artifacts_dir() == root / 'artifacts' / 'data_freshness'

    # 换 cwd 后结果不变（旧实现用裸 Path('data/local_store') 会漂移）
    monkeypatch.chdir(tmp_path)
    assert default_store_root() == root / DEFAULT_STORE_PATH
    assert freshness_artifacts_dir().is_absolute()


def test_resolve_store_root_relative_and_absolute(tmp_path: Path) -> None:
    relative = SimpleNamespace(data=SimpleNamespace(local_store_path='data/local_store'))
    assert resolve_store_root(relative) == project_root() / 'data/local_store'

    absolute = SimpleNamespace(data=SimpleNamespace(local_store_path=str(tmp_path / 'store')))
    assert resolve_store_root(absolute) == tmp_path / 'store'

    # store.py 保留的同名入口必须委托到同一实现
    assert legacy_resolve_store_root(relative) == resolve_store_root(relative)

    # 缺字段的轻量配置安全退回默认目录
    assert resolve_store_root(SimpleNamespace()) == default_store_root()


def test_share_capital_work_dir_uses_project_root() -> None:
    from src.data.local_store import share_capital

    assert share_capital.default_store_root() == default_store_root()
    assert share_capital.default_work_dir() == project_root() / 'tmp' / 'stock_shares'


def test_archive_report_is_readonly_and_detects_duplicates(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(archives, 'PROJECT_ROOT', tmp_path)

    backup = tmp_path / 'data' / 'local_store' / '_backup'
    backup.mkdir(parents=True)
    (backup / 'a.parquet').write_bytes(b'same-content' * 8)
    (backup / 'b.parquet').write_bytes(b'same-content' * 8)
    (backup / 'c.parquet').write_bytes(b'other-content' * 5)

    scan_dirs = archives.expand_scans(['data/local_store/_backup'], tmp_path)
    assert len(scan_dirs) == 1

    report = archives.build_report(
        scan_dirs, root=tmp_path, max_age_days=30,
        max_hash_mb=64, budget_mb=512, hash_enabled=True)

    assert report['readonly'] is True
    assert report['file_count'] == 3
    assert len(report['duplicate_groups']) == 1
    group = report['duplicate_groups'][0]
    assert group['count'] == 2
    assert report['duplicate_reclaimable_bytes'] == group['bytes_each']

    # 盘点绝不能删文件
    assert (backup / 'a.parquet').exists()
    assert (backup / 'b.parquet').exists()
    assert (backup / 'c.parquet').exists()
    assert '不删除' in report['notice']


def test_archive_protected_dir_is_never_scanned(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(archives, 'PROJECT_ROOT', tmp_path)
    protected = tmp_path / 'backups' / 'critical_assets'
    protected.mkdir(parents=True)
    (protected / 'names.json').write_text('{}', encoding='utf-8')

    assert archives.expand_scans(['backups'], tmp_path) == []


def test_archive_aged_candidates(tmp_path: Path,
                                  monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(archives, 'PROJECT_ROOT', tmp_path)
    old = tmp_path / 'reports' / 'audit_old'
    old.mkdir(parents=True)
    target = old / 'snapshot.parquet'
    target.write_bytes(b'x' * 32)
    stale = (datetime.now() - timedelta(days=90)).timestamp()
    os.utime(target, (stale, stale))

    scan_dirs = archives.expand_scans(['reports/audit_*'], tmp_path)
    report = archives.build_report(
        scan_dirs, root=tmp_path, max_age_days=30,
        max_hash_mb=64, budget_mb=512, hash_enabled=False)

    assert report['hash_enabled'] is False
    assert report['duplicate_groups'] == []
    assert [item['path'] for item in report['aged_candidates']] == [
        'reports/audit_old/snapshot.parquet']
    assert target.exists()
    # 确保没有引入"时间"相关的隐式删除
    assert time.time() > stale


def test_staging_root_is_outside_data_and_gitignored() -> None:
    """staging 根必须在 data/ 之外，且被 .gitignore 覆盖。"""
    root = project_root()
    assert STAGING_ROOT == root / '_staging'
    assert STAGING_ROOT.parent == root
    assert not STAGING_ROOT.is_relative_to(root / 'data')
    assert staging_dir('demo_20261110') == root / '_staging' / 'demo_20261110'
    assert staging_dir('demo', day=date(2026, 11, 10), with_date=True) == \
        root / '_staging' / 'demo_20261110'
    assert '/_staging/' in (root / '.gitignore').read_text(encoding='utf-8')


def test_sync_scopes_never_touch_staging_root(tmp_path: Path) -> None:
    """每日同步清单不得收进仓库根 ``_staging/``，否则候选数据会被当正式数据同步。"""
    store = tmp_path / 'data' / 'local_store'
    (store / 'meta' / 'st_status').mkdir(parents=True)
    (store / 'meta' / 'st_status' / 'a.parquet').write_bytes(b'x' * 8)
    (store / 'meta' / 'names.json').write_text('{}', encoding='utf-8')
    (store / 'quotes').mkdir(parents=True)
    (store / 'quotes' / 'q.parquet').write_bytes(b'q' * 8)
    staging = tmp_path / '_staging' / 'job_20261110'
    staging.mkdir(parents=True)
    (staging / 'candidate.parquet').write_bytes(b'y' * 8)
    (tmp_path / 'reports' / 'audit').mkdir(parents=True)
    (tmp_path / 'reports' / 'audit' / 'r.md').write_text('r', encoding='utf-8')

    # 排除已不再依赖「按目录名跳过」，靠范围常量本身把仓库根挡在外面
    assert '_staging' not in daily_sync.SKIP_DIR_NAMES

    daily_rel = {item.relative for item in daily_sync.collect(tmp_path, 'daily')}
    assert 'data/local_store/meta/st_status/a.parquet' in daily_rel
    package_rel = {item.relative
                   for item in daily_sync.collect(tmp_path, 'package', since=0.0)}
    seed_rel = {item.relative for item in daily_sync.collect(tmp_path, 'seed')}
    for scope, relatives in (('daily', daily_rel), ('package', package_rel),
                             ('seed', seed_rel)):
        assert relatives, scope
        assert not [rel for rel in relatives if '_staging' in rel.split('/')], scope
        assert not [rel for rel in relatives if rel.startswith('_staging/')], scope
    assert 'data/local_store/quotes/q.parquet' in seed_rel
