"""分析页落盘缓存的单元测试：键、命中、绕过、dv 缺失、损坏与保留版本。"""

from __future__ import annotations

import pickle
import time
from pathlib import Path

import pytest

from src.quant.analysis import analysis_cache as ac


@pytest.fixture()
def cache_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / 'analysis_cache'
    monkeypatch.setattr(ac, 'CACHE_ROOT', root)
    return root


def test_build_key_stable_and_sensitive() -> None:
    base = ac.build_key('market_trend', '2026-11-13', 'dv-1', {'panel_end': '2026-11-13'})
    assert base == ac.build_key(
        'market_trend', '2026-11-13', 'dv-1', {'panel_end': '2026-11-13'})
    assert base != ac.build_key(
        'market_trend', '2026-11-14', 'dv-1', {'panel_end': '2026-11-13'})
    assert base != ac.build_key(
        'market_trend', '2026-11-13', 'dv-2', {'panel_end': '2026-11-13'})
    assert base != ac.build_key(
        'market_trend', '2026-11-13', 'dv-1', {'panel_end': '2026-11-14'})
    assert base != ac.build_key(
        'style_rotation', '2026-11-13', 'dv-1', {'panel_end': '2026-11-13'})


def test_cached_hit_returns_original_time(cache_root: Path) -> None:
    calls: list[int] = []

    def compute() -> dict[str, int]:
        calls.append(1)
        return {'x': 1}

    first = ac.cached('market_trend', 'state', asof=None,
                      fingerprint={'panel_end': '2026-11-13'},
                      compute=compute, dv='dv-1')
    second = ac.cached('market_trend', 'state', asof=None,
                       fingerprint={'panel_end': '2026-11-13'},
                       compute=compute, dv='dv-1')
    assert first.meta.hit is False
    assert second.meta.hit is True
    assert len(calls) == 1
    assert second.value == {'x': 1}
    assert second.meta.computed_at == first.meta.computed_at
    assert second.meta.dv == 'dv-1'


def test_refresh_bypasses_cache(cache_root: Path) -> None:
    calls: list[int] = []

    def compute() -> int:
        calls.append(1)
        return len(calls)

    ac.cached('market_trend', 'state', asof=None, fingerprint={},
              compute=compute, dv='dv-1')
    out = ac.cached('market_trend', 'state', asof=None, fingerprint={},
                    compute=compute, dv='dv-1', refresh=True)
    assert len(calls) == 2
    assert out.meta.hit is False
    assert out.value == 2


def test_dv_missing_skips_cache(cache_root: Path) -> None:
    calls: list[int] = []

    def compute() -> str:
        calls.append(1)
        return 'v'

    ac.cached('market_trend', 'state', asof=None, fingerprint={},
              compute=compute, dv='')
    ac.cached('market_trend', 'state', asof=None, fingerprint={},
              compute=compute, dv='')
    assert len(calls) == 2
    assert not list(cache_root.rglob('*.pkl'))


def test_corrupt_file_is_miss(cache_root: Path) -> None:
    calls: list[int] = []

    def compute() -> str:
        calls.append(1)
        return 'ok'

    first = ac.cached('industry_heat', 'state', asof=None, fingerprint={},
                      compute=compute, dv='dv-1')
    assert first.meta.path is not None
    Path(first.meta.path).write_bytes(b'not a pickle')
    second = ac.cached('industry_heat', 'state', asof=None, fingerprint={},
                       compute=compute, dv='dv-1')
    assert second.meta.hit is False
    assert len(calls) == 2


def test_prune_keeps_three_and_no_tmp(cache_root: Path) -> None:
    for index in range(5):
        ac.save_object('market_trend', 'state', f'key{index}', index,
                       dv='dv-1', asof=None, computed_at='2026-11-16 10:00:00')
    directory = cache_root / 'market_trend'
    assert len(list(directory.glob('state-*.pkl'))) == ac.KEEP_VERSIONS
    assert not list(directory.glob('.tmp-*'))


def test_refresh_overwrites_cache(cache_root: Path) -> None:
    calls: list[int] = []

    def compute() -> dict[str, int]:
        calls.append(1)
        return {'n': len(calls)}

    args = {'page': 'market_trend', 'namespace': 'state', 'asof': None,
            'fingerprint': {'panel_end': '2026-11-13'}, 'compute': compute,
            'dv': 'dv-1'}
    first = ac.cached(**args)
    path = Path(first.meta.path)
    before_mtime, before_bytes = path.stat().st_mtime_ns, path.read_bytes()
    time.sleep(0.01)
    refreshed = ac.cached(**args, refresh=True)
    after_mtime, after_bytes = path.stat().st_mtime_ns, path.read_bytes()
    assert refreshed.meta.hit is False
    assert after_mtime != before_mtime
    assert after_bytes != before_bytes
    again = ac.cached(**args)
    assert again.meta.hit is True
    assert again.meta.computed_at == refreshed.meta.computed_at


def test_describe_texts() -> None:
    hit = ac.CacheMeta(page='p', namespace='state', key='k', dv='dv-1', asof=None,
                       computed_at='2026-11-16 10:00:00', hit=True)
    assert '缓存命中' in ac.describe(hit)
    assert '2026-11-16 10:00:00' in ac.describe(hit)
    miss = ac.CacheMeta(page='p', namespace='state', key='k', dv='dv-1', asof=None,
                        computed_at='2026-11-16 10:00:00', hit=False)
    assert '缓存命中' not in ac.describe(miss)
    assert '数据版本 dv-1' in ac.describe(miss)
    no_dv = ac.CacheMeta(page='p', namespace='state', key='k', dv=None, asof=None,
                         computed_at='2026-11-16 10:00:00', hit=False)
    assert '未写缓存' in ac.describe(no_dv)
    assert ac.describe(None) == ''


def test_pickle_roundtrip_value(cache_root: Path) -> None:
    value = {'a': [1, 2, 3], 'b': '中文'}
    path = ac.save_object('portfolio_risk', 'state', 'k', value,
                          dv='dv-1', asof=None, computed_at='2026-11-16 10:00:00')
    assert path is not None
    with path.open('rb') as handle:
        payload = pickle.load(handle)  # noqa: S301 - 本地自建缓存，非外部输入
    assert payload['value'] == value
    assert payload['schema'] == ac.CACHE_SCHEMA
