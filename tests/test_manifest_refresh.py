"""写盘后必须回写 manifest 统计字段：不回写要能被 --check 判出，回写后必须 PASS。

覆盖 ``src/data/manifest_refresh.py`` 的三种现有布局与两种失败形态：

* 布局 A：``files`` 是 list，条目自带相对 store 根的 ``path``（meta/factors）；
* 布局 B：条目的 ``path`` 只有文件名、目录由顶层 ``dir`` 给出（meta/anomalies）；
* 布局 C：顶层自述 + ``sibling_tables.files``（meta/breadth）。

判「不一致」用 ``tools/manifest_tool.py`` 自己的核对函数，不另写一套口径——
否则测试与生产核对标准可能各说各话。
"""

import hashlib
import json
from datetime import date

import polars as pl
import pytest

from src.data.manifest_refresh import refresh_manifest_files
from tools.manifest_tool import FactsCache, check_manifest

STALE = {'rows': 1, 'bytes': 1, 'sha256': '0' * 64}


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _check(root, manifest_rel):
    return check_manifest(root / manifest_rel, root, FactsCache())


def _write_manifest(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def _frame(days):
    return pl.DataFrame({
        'ts_code': ['000001.SZ'] * days,
        'effective_date': [f'2020-01-{day:02d}' for day in range(1, days + 1)],
        'value': list(range(days)),
    }).with_columns(pl.col('effective_date').str.to_date('%Y-%m-%d'))


def test_factors_layout_detects_drift_then_refresh_passes(tmp_path):
    """布局 A：写盘不回写 → --check 报不一致；回写后 PASS，人工字段不动。"""
    root = tmp_path / 'store'
    target = root / 'meta' / 'factors' / 'stock_shares.parquet'
    target.parent.mkdir(parents=True)
    manifest = target.parent / 'manifest.json'
    _write_manifest(manifest, {
        'dataset': '股本区间事实表',
        'authority': '清单 L39',
        'files': [{
            'path': 'meta/factors/stock_shares.parquet', **STALE,
            'columns': ['ts_code', 'effective_date', 'value'],
            'date_column': 'effective_date',
            'date_range': ['2020-01-01', '2020-01-01'],
            'note': '人工登记项不得被回写覆盖',
        }],
    })
    _frame(3).write_parquet(target)

    before = _check(root, 'meta/factors/manifest.json')
    assert before.bad >= 2, before.issues
    assert any(issue.kind == 'rows' for issue in before.issues)
    assert any(issue.kind == 'sha256' for issue in before.issues)

    refreshed = refresh_manifest_files(manifest, root, [target], required=True)
    assert refreshed == ['meta/factors/stock_shares.parquet']

    after = _check(root, 'meta/factors/manifest.json')
    assert after.bad == 0, after.issues
    assert after.ok_records == 1

    entry = json.loads(manifest.read_text(encoding='utf-8'))['files'][0]
    assert entry['rows'] == 3
    assert entry['bytes'] == target.stat().st_size
    assert entry['sha256'] == _sha(target)
    assert entry['date_range'] == ['2020-01-01', '2020-01-03']
    assert entry['note'] == '人工登记项不得被回写覆盖'


def test_csv_entry_with_top_level_dir(tmp_path):
    """布局 B：``dir`` + 裸文件名条目（meta/anomalies 的 CSV 台账）。"""
    root = tmp_path / 'store'
    target = root / 'meta' / 'anomalies' / 'fund_fake_bar_bars.csv'
    target.parent.mkdir(parents=True)
    manifest = target.parent / 'manifest.json'
    _write_manifest(manifest, {
        'dir': 'data/local_store/meta/anomalies',
        'dataset': '异常台账',
        'files': [{
            'path': 'fund_fake_bar_bars.csv', **STALE,
            'columns': ['ts_code', 'trade_date'],
            'level': 'A',
        }],
    })
    target.write_text('ts_code,trade_date\n588810.SH,2025-02-12\n', encoding='utf-8')

    before = _check(root, 'meta/anomalies/manifest.json')
    assert before.bad >= 2, before.issues

    assert refresh_manifest_files(manifest, root, [target], required=True) == [
        'meta/anomalies/fund_fake_bar_bars.csv']
    after = _check(root, 'meta/anomalies/manifest.json')
    assert after.bad == 0, after.issues
    entry = json.loads(manifest.read_text(encoding='utf-8'))['files'][0]
    assert entry['rows'] == 1
    assert entry['level'] == 'A'


def test_self_describing_and_sibling_entries(tmp_path):
    """布局 C：顶层自述 + ``sibling_tables.files`` 两条记录一并重算。"""
    root = tmp_path / 'store'
    breadth = root / 'meta' / 'breadth'
    breadth.mkdir(parents=True)
    ma = breadth / 'breadth_ma_daily.parquet'
    daily = breadth / 'breadth_daily.parquet'
    pl.DataFrame({'date': [date(2020, 1, 2)], 'pct': [1.0]}).write_parquet(ma)
    pl.DataFrame({'trade_date': [date(2020, 1, 2), date(2020, 1, 3)],
                  'up_count': [1, 2]}).write_parquet(daily)
    manifest = breadth / 'manifest.json'
    _write_manifest(manifest, {
        'path': 'data/local_store/meta/breadth/breadth_ma_daily.parquet',
        'dataset': '宽度两表',
        **STALE,
        'columns': ['date', 'pct'],
        'date_range': ['2020-01-01', '2020-01-01'],
        'sibling_tables': {'note': '同目录另一张表', 'files': [{
            'path': 'data/local_store/meta/breadth/breadth_daily.parquet',
            **STALE, 'cols': 1, 'columns': ['trade_date', 'up_count'],
            'date_column': 'trade_date', 'date_range': ['2020-01-01', '2020-01-01'],
        }]},
    })

    assert _check(root, 'meta/breadth/manifest.json').bad >= 2
    refreshed = refresh_manifest_files(manifest, root, [ma, daily], required=True)
    assert set(refreshed) == {'meta/breadth/breadth_ma_daily.parquet',
                              'meta/breadth/breadth_daily.parquet'}
    after = _check(root, 'meta/breadth/manifest.json')
    assert after.bad == 0, after.issues
    assert after.ok_records >= 2  # 顶层自述 1 条 + sibling 1 条（另含容器节点）

    payload = json.loads(manifest.read_text(encoding='utf-8'))
    assert payload['rows'] == 1
    assert payload['date_range'] == ['2020-01-02', '2020-01-02']
    sibling = payload['sibling_tables']['files'][0]
    assert sibling['rows'] == 2
    assert sibling['cols'] == 2
    assert sibling['date_range'] == ['2020-01-02', '2020-01-03']


def test_refresh_is_idempotent(tmp_path):
    """同一份磁盘内容重复回写不再产生字节变化（否则会刷出空台账记录）。"""
    root = tmp_path / 'store'
    target = root / 'meta' / 'events' / 'events.parquet'
    target.parent.mkdir(parents=True)
    manifest = target.parent / 'manifest.json'
    _write_manifest(manifest, {
        'files': [{'path': 'meta/events/events.parquet', **STALE,
                   'columns': ['event_id']}],
    })
    pl.DataFrame({'event_id': ['a', 'b']}).write_parquet(target)

    refresh_manifest_files(manifest, root, [target], required=True)
    first = manifest.read_bytes()
    refresh_manifest_files(manifest, root, [target], required=True)
    assert manifest.read_bytes() == first


def test_required_raises_when_entry_missing(tmp_path):
    """漏登记不是「无需回写」：required=True 必须报错，不静默跳过。"""
    root = tmp_path / 'store'
    target = root / 'meta' / 'events' / 'events.parquet'
    target.parent.mkdir(parents=True)
    manifest = target.parent / 'manifest.json'
    _write_manifest(manifest, {'files': []})
    pl.DataFrame({'event_id': ['a']}).write_parquet(target)

    with pytest.raises(RuntimeError, match='没有 meta/events/events.parquet 的登记条目'):
        refresh_manifest_files(manifest, root, [target], required=True)


def test_missing_manifest_is_silent(tmp_path):
    """临时 store 没有 manifest 时直接跳过，不凭空造登记文件。"""
    root = tmp_path / 'store'
    target = root / 'meta' / 'events' / 'events.parquet'
    target.parent.mkdir(parents=True)
    pl.DataFrame({'event_id': ['a']}).write_parquet(target)

    assert refresh_manifest_files(target.parent / 'manifest.json', root,
                                  [target], required=True) == []
    assert not (target.parent / 'manifest.json').exists()
