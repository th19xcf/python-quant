"""build_store 落盘后必须回写 ``meta/st/manifest.json`` 的 files 段。

历史缺陷（2026-11-13 实测）：parquet 已被 2026-11-12 23:59 的重建换掉，
manifest 仍停在旧 rows / bytes / sha256，``manifest_tool --check`` 报 5 条不一致。
根因是 build_store 只写 parquet 与 coverage.json，从未调用任何 manifest 刷新。
"""

import hashlib
import json
from datetime import date

import polars as pl

from src.data import st_history

LISTING = {'000005.SZ': date(2010, 1, 1), '000006.SZ': date(2014, 6, 1)}
STALE = {'rows': 1, 'bytes': 1, 'sha256': '0' * 64}


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _manifest(root):
    return json.loads(
        (root / 'meta' / 'st' / 'manifest.json').read_text(encoding='utf-8'))


def _seed(root):
    raw = root / 'meta' / 'st' / 'raw'
    raw.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([
        {'code': '000005.SZ', 'change_date': date(2015, 1, 5),
         'name_prev': '正常', 'name_new': 'ST戊'},
        {'code': '000006.SZ', 'change_date': date(2019, 3, 4),
         'name_prev': '正常', 'name_new': '*ST己'},
    ], schema=st_history._RAW_SZ_SCHEMA).write_parquet(raw / 'sz_change_name.parquet')
    pl.DataFrame([{'ts_code': code, 'list_date': day}
                  for code, day in LISTING.items()]).write_parquet(
        root / 'meta' / 'stock_basic.parquet')
    # 故意写成过期登记：模拟「parquet 已换、manifest 没回写」
    manifest = {
        'dataset': 'ST 三表',
        'authority': '清单 L11（ST 区间）/ L12（ST 覆盖窗口）',
        'files': [
            {'path': 'meta/st/st_intervals.parquet', **STALE},
            {'path': 'meta/st/st_coverage.parquet', **STALE},
            {'path': 'meta/st/st_price_inferred.parquet',
             'rows': 7, 'bytes': 7, 'sha256': 'f' * 64},
        ],
    }
    (root / 'meta' / 'st' / 'manifest.json').write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')


def _drift(root):
    """files 段与磁盘不一致的路径（空列表 = 同步）；磁盘不存在的条目不参与。"""
    out = []
    for entry in _manifest(root)['files']:
        path = root / entry['path']
        if not path.is_file():
            continue
        frame = pl.read_parquet(path) if path.suffix == '.parquet' else None
        rows = frame.height if frame is not None else entry.get('rows')
        if (entry.get('bytes'), entry.get('rows'), entry.get('sha256')) != (
                path.stat().st_size, rows, _sha(path)):
            out.append(entry['path'])
    return out


def test_build_store_refreshes_manifest_files(tmp_path):
    _seed(tmp_path)
    st_history.build_store(tmp_path, apply_snapshot=False, as_of=date(2026, 11, 10))

    assert _drift(tmp_path) == []
    payload = _manifest(tmp_path)
    # files 段以外的人工登记不被构建覆盖
    assert payload['dataset'] == 'ST 三表'
    assert payload['authority'].startswith('清单 L11')
    entries = {entry['path']: entry for entry in payload['files']}
    # 本次未写盘的表保持原样
    assert entries['meta/st/st_price_inferred.parquet']['rows'] == 7
    # 回写条目按磁盘实测，带列清单与日期区间说明
    intervals = entries['meta/st/st_intervals.parquet']
    assert intervals['rows'] == pl.read_parquet(
        st_history.intervals_path(tmp_path)).height
    assert intervals['columns'] == list(st_history._INTERVAL_SCHEMA)
    assert '候选实测' in entries['meta/st/st_coverage.parquet']['date_range_note']


def test_manifest_drift_is_detected_when_parquet_written_without_refresh(tmp_path):
    """反证：绕开 build_store 直接改写 parquet，同一判据必须报不一致。"""
    _seed(tmp_path)
    st_history.build_store(tmp_path, apply_snapshot=False, as_of=date(2026, 11, 10))
    assert _drift(tmp_path) == []

    coverage = pl.read_parquet(st_history.coverage_path(tmp_path))
    pl.concat([coverage, coverage]).write_parquet(st_history.coverage_path(tmp_path))

    assert _drift(tmp_path) == ['meta/st/st_coverage.parquet']


def test_output_dir_run_does_not_touch_source_manifest(tmp_path):
    _seed(tmp_path)
    st_history.build_store(tmp_path, apply_snapshot=False,
                           output_dir=tmp_path / 'rebuild', as_of=date(2026, 11, 10))

    # 对拍运行写的是另一个目录：本目录 manifest 保持原样，未被回写
    entries = {entry['path']: entry for entry in _manifest(tmp_path)['files']}
    assert entries['meta/st/st_intervals.parquet']['rows'] == STALE['rows']
    assert entries['meta/st/st_coverage.parquet']['sha256'] == STALE['sha256']
    assert not st_history.intervals_path(tmp_path).exists()
