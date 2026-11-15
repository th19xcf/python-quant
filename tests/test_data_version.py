"""数据版本台账回归测试：版本历史只增、派生表标记与落后判定。"""

import json
from datetime import date

import polars as pl

from src.backtest.runner import records as run_records
from src.data import data_version as dv


def _write_json(path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')


def _build_store(tmp_path):
    store = tmp_path / 'local_store'
    index = store / 'meta' / 'daily_index.parquet'
    index.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'ts_code': ['600000.SH'], 'end': [date(2026, 11, 13)]}).write_parquet(index)
    _write_json(store / 'factors' / 'manifest.json',
                {'files': {'stock_adj_factor.parquet': {'rows': 100, 'sha256': 'aa' * 32}}})
    _write_json(store / 'meta' / 'events' / 'manifest.json',
                {'files': {'events.parquet': {'rows': 10, 'sha256': 'bb' * 32}}})
    return store


def _bump_factors(store, sha: str) -> None:
    _write_json(store / 'factors' / 'manifest.json',
                {'files': {'stock_adj_factor.parquet': {'rows': 100, 'sha256': sha * 32}}})


def test_version_id_uses_quotes_end_and_is_stable(tmp_path):
    store = _build_store(tmp_path)
    first = dv.compute_version(store)
    second = dv.compute_version(store)

    assert first['version'] == second['version']
    assert first['version'].startswith('dv-20261113-')
    assert first['as_of'] == '2026-11-13'
    assert first['classes']['basic']['meta/stock_basic.manifest.json'] == 'missing'


def test_record_keeps_history_and_never_overwrites(tmp_path):
    store = _build_store(tmp_path)
    first = dv.record_version(store)
    again = dv.record_version(store)
    assert first['changed'] is True
    assert again['changed'] is False
    assert again['history'] == 1

    _bump_factors(store, 'cc')
    third = dv.record_version(store)
    assert third['changed'] is True
    assert third['history'] == 2
    assert third['version'] != first['version']

    ledger = json.loads((store / dv.VERSION_FILE).read_text(encoding='utf-8'))
    assert ledger['schema'] == dv.SCHEMA
    assert ledger['current']['digest'] == third['digest']
    assert [item['digest'] for item in ledger['versions']][1] == first['digest']
    assert ledger['current']['seen_count'] == 1


def test_history_is_capped_at_keep(tmp_path):
    store = _build_store(tmp_path)
    for sha in ('11', '22', '33'):
        _bump_factors(store, sha)
        dv.record_version(store, keep=2)
    ledger = json.loads((store / dv.VERSION_FILE).read_text(encoding='utf-8'))
    assert len(ledger['versions']) == 2
    assert ledger['versions'][0]['digest'] == ledger['current']['digest']


def test_stamp_then_stale_after_base_changes(tmp_path):
    store = _build_store(tmp_path)
    dv.record_version(store)
    pool = store / 'meta' / 'multi_asset' / 'etf_pool.parquet'
    pool.parent.mkdir(parents=True, exist_ok=True)
    pool.write_bytes(b'placeholder')

    entry = dv.stamp_derived(store, 'meta/multi_asset/etf_pool.parquet', files=['etf_pool.parquet'])
    assert entry['based_on_version'].startswith('dv-20261113-')
    audit = dv.audit_derived(store)
    status = {row['table']: row['status'] for row in audit['rows']}
    assert status['meta/multi_asset/etf_pool.parquet'] == 'ok'
    assert status['meta/breadth/breadth_daily.parquet'] == 'missing'

    _bump_factors(store, 'dd')
    dv.record_version(store)
    audit = dv.audit_derived(store)
    status = {row['table']: row['status'] for row in audit['rows']}
    assert status['meta/multi_asset/etf_pool.parquet'] == 'stale'
    assert audit['counts']['stale'] == 1


def test_unstamped_derived_is_reported_not_backfilled(tmp_path):
    store = _build_store(tmp_path)
    dv.record_version(store)
    turnover = store / 'factors' / 'stock_turnover.parquet'
    turnover.parent.mkdir(parents=True, exist_ok=True)
    turnover.write_bytes(b'placeholder')

    audit = dv.audit_derived(store)
    row = next(item for item in audit['rows'] if item['table'] == 'factors/stock_turnover.parquet')
    assert row['status'] == 'unstamped'
    assert row['based_on_version'] is None
    assert not (store / dv.DERIVED_FILE).exists()


def test_stamp_rebuilt_only_marks_changed_tables(tmp_path):
    store = _build_store(tmp_path)
    dv.record_version(store)
    pool = store / 'meta' / 'multi_asset' / 'etf_pool.parquet'
    pool.parent.mkdir(parents=True, exist_ok=True)
    pool.write_bytes(b'placeholder')

    stamped = dv.stamp_rebuilt(store, {'meta/multi_asset/etf_pool.parquet', 'quotes/stock/x.parquet'})
    assert stamped == ['meta/multi_asset/etf_pool.parquet']
    payload = json.loads((store / dv.DERIVED_FILE).read_text(encoding='utf-8'))
    assert list(payload['tables']) == ['meta/multi_asset/etf_pool.parquet']


def test_store_data_basis_unknown_when_ledger_missing(tmp_path):
    store = _build_store(tmp_path)
    basis = run_records.store_data_basis(store)
    assert basis['status'] == 'unknown'
    assert basis['version'] is None

    dv.record_version(store)
    basis = run_records.store_data_basis(store)
    assert basis['status'] == 'recorded'
    assert basis['version'].startswith('dv-20261113-')
    assert basis['data_as_of'] == '2026-11-13'


def test_read_run_data_basis_priority_and_unknown(tmp_path):
    run_dir = tmp_path / 'run'
    run_dir.mkdir()
    empty = run_records.read_run_data_basis(run_dir)
    assert empty['status'] == 'unknown' and empty['source'] == 'none'

    (run_dir / 'progress.json').write_text(
        json.dumps({'data_basis': {'status': 'recorded', 'version': 'dv-p'}}), encoding='utf-8')
    assert run_records.read_run_data_basis(run_dir)['source'] == 'progress.json'

    (run_dir / 'data_basis.json').write_text(
        json.dumps({'status': 'bound', 'version': 'dv-b', 'evidence': 'e'}), encoding='utf-8')
    assert run_records.read_run_data_basis(run_dir)['source'] == 'data_basis.json'

    metrics = {'data_basis': {'status': 'recorded', 'version': 'dv-m'}}
    assert run_records.read_run_data_basis(run_dir, metrics)['version'] == 'dv-m'


def test_bind_run_requires_evidence_and_records_ledger_check(tmp_path):
    store = _build_store(tmp_path)
    recorded = dv.record_version(store)
    run_dir = tmp_path / 'run'
    run_dir.mkdir()

    try:
        run_records.bind_run_data_basis(run_dir, version=recorded['version'],
                                        evidence='', store_root=store)
    except ValueError:
        pass
    else:
        raise AssertionError('缺证据必须拒绝绑定')

    entry = run_records.bind_run_data_basis(
        run_dir, version=recorded['version'],
        evidence='metrics.repro.data_version.parquet_sha256 与当日 manifest 一致',
        store_root=store)
    assert entry['ledger_check'] == 'current'
    assert entry['digest'] == recorded['digest']
    assert (run_dir / run_records.RUN_BASIS_FILE).is_file()
    assert run_records.read_run_data_basis(run_dir)['source'] == run_records.RUN_BASIS_FILE
