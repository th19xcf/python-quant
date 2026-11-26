"""manifest 哈希事实缓存（``src/data/hash_cache.py`` + 日更 manifest 步）用例。

覆盖复用判据：未变复用、size 变重算、mtime 变重算、路径消失不命中、跨进程持久化、
强制全量开关；以及「门禁不放宽」——复用命中时 manifest 登记值不符仍照样报出。
"""

from __future__ import annotations

import json
import os

import pytest

from src.data.hash_cache import FactsCache
from src.data.manifest_refresh import file_facts
from tools import daily_update as du


def _csv(tmp_path, name: str = 'data.csv', text: str = 'a,b\n1,2\n'):
    path = tmp_path / name
    path.write_text(text, encoding='utf-8')
    return path


def test_unchanged_file_reuses_cached_facts(tmp_path):
    path = _csv(tmp_path)
    cache = FactsCache(tmp_path / 'cache.json')
    first = file_facts(path, cache)
    assert cache.recomputed == 1 and cache.hits == 0

    second = file_facts(path, cache)
    assert cache.recomputed == 1 and cache.hits == 1
    assert second == first
    assert second['sha256'] == first['sha256']


def test_size_change_recomputes(tmp_path):
    path = _csv(tmp_path)
    cache = FactsCache(tmp_path / 'cache.json')
    before = file_facts(path, cache)

    path.write_text('a,b\n1,2\n3,4\n', encoding='utf-8')
    after = file_facts(path, cache)
    assert cache.recomputed == 2 and cache.hits == 0
    assert after['bytes'] != before['bytes']
    assert after['sha256'] != before['sha256']


def test_mtime_change_recomputes_even_if_size_same(tmp_path):
    path = _csv(tmp_path)
    cache = FactsCache(tmp_path / 'cache.json')
    first = file_facts(path, cache)

    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 10 ** 9))
    second = file_facts(path, cache)
    assert cache.recomputed == 2 and cache.hits == 0
    # 内容没变，重算结果与原值一致，但确实重算了一次
    assert second['sha256'] == first['sha256']
    assert second['bytes'] == first['bytes']


def test_missing_path_is_not_served_from_cache(tmp_path):
    path = _csv(tmp_path)
    cache = FactsCache(tmp_path / 'cache.json')
    file_facts(path, cache)
    path.unlink()

    with pytest.raises(FileNotFoundError):
        file_facts(path, cache)
    assert cache.hits == 0


def test_cache_persists_across_runs(tmp_path):
    path = _csv(tmp_path)
    cache_file = tmp_path / 'cache.json'
    first = FactsCache(cache_file)
    facts = file_facts(path, first)
    first.save()

    second = FactsCache(cache_file)
    assert file_facts(path, second) == facts
    assert second.recomputed == 0 and second.hits == 1


def test_full_recompute_switch_ignores_cache(tmp_path):
    path = _csv(tmp_path)
    cache_file = tmp_path / 'cache.json'
    warm = FactsCache(cache_file)
    file_facts(path, warm)
    warm.save()

    forced = FactsCache(cache_file, read_cached=False)
    file_facts(path, forced)
    assert forced.recomputed == 1 and forced.hits == 0


def _store_with_manifest(tmp_path, sha256: str):
    root = tmp_path / 'store'
    root.mkdir()
    table = root / 'data.csv'
    table.write_text('a,b\n1,2\n', encoding='utf-8')
    sidecar = root / 'data.manifest.json'
    sidecar.write_text(json.dumps({
        'path': 'data.csv', 'rows': 1, 'bytes': table.stat().st_size, 'sha256': sha256,
    }, ensure_ascii=False), encoding='utf-8')
    return root, {'data.csv': table.stat().st_size}


def test_verify_still_flags_manifest_mismatch_with_warm_cache(tmp_path):
    """缓存只复用事实，不参与判据：登记 sha256 不符必须照样报出。"""
    root, changed = _store_with_manifest(tmp_path, sha256='deadbeef')
    cache = FactsCache(tmp_path / 'cache.json')
    for _ in range(2):  # 第二轮全部命中缓存
        report = du._verify(root, changed, set(changed), {}, {}, cache)
        assert any('data.csv:sha256' in item for item in report['problems'])
    assert cache.hits >= 1


def test_verify_passes_when_manifest_matches_and_cache_reused(tmp_path):
    root = tmp_path / 'store'
    root.mkdir()
    table = root / 'data.csv'
    table.write_text('a,b\n1,2\n', encoding='utf-8')
    facts = file_facts(table)
    (root / 'data.manifest.json').write_text(json.dumps({
        'path': 'data.csv', 'rows': facts['rows'],
        'bytes': facts['bytes'], 'sha256': facts['sha256'],
    }, ensure_ascii=False), encoding='utf-8')
    changed = {'data.csv': table.stat().st_size}

    cache = FactsCache(tmp_path / 'cache.json')
    first = du._verify(root, changed, set(changed), {}, {}, cache)
    second = du._verify(root, changed, set(changed), {}, {}, cache)
    assert first['problems'] == [] and second['problems'] == []
    assert cache.hits >= 1
