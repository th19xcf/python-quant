"""turnover 模块单测：读取缓存、来源分级、断点续跑、增量合并与本地派生。

全部使用临时目录 + 合成 parquet + 注入 fake baostock，不访问网络、不读
``data/`` 正式表。重点覆盖三条易错边界：来源优先级、分块合并不得清历史、
本地派生的缺口 anti-join 与行数守恒。
"""

from __future__ import annotations

import hashlib
import json
import sys
import types
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

import src.data.local_store.turnover as turn_module
import src.utils.exception_handler as exc_module
from src.data.local_store import share_capital as share_module
from src.data.local_store.store import LocalQuoteStore
from src.data.local_store.turnover import (
    TURN_DERIVED_SOURCE,
    TURN_SCHEMA,
    _assemble,
    _build_tasks,
    _canonical_code,
    _cleanup_stale_parts,
    _ensure_login,
    _existing_last_dates,
    _fetch_code,
    _frame_to_records,
    _load_json,
    _load_resume_state,
    _normalize_date_col,
    _query_turn_range,
    _run_chunk,
    _run_chunk_inline,
    _run_turnover_chunks,
    _select_universe,
    _TurnoverRun,
    _worker_init,
    _worker_task,
    _write_failed_csv,
    _write_json,
    _write_part,
    _write_progress_csv,
    attach_turn,
    backfill_turnover,
    derive_turnover_local,
    read_float_share_series,
    read_turnover,
    shares_path,
    turnover_path,
)
from src.utils.exceptions import DataSourceConnectionError

_DATES = [date(2024, 1, 2) + timedelta(days=i) for i in range(5)]


@pytest.fixture(autouse=True)
def _reset_session():
    turn_module._SESSION.clear()
    yield
    turn_module._SESSION.clear()


def _quote_frame(code: str, volume: float = 1000.0) -> pl.DataFrame:
    closes = [10.0 + i * 0.1 for i in range(len(_DATES))]
    return pl.DataFrame({
        'date': _DATES,
        'ts_code': [code] * len(_DATES),
        'open': closes,
        'high': closes,
        'low': closes,
        'close': closes,
        'pre_close': closes,
        'volume': [volume] * len(_DATES),
        'amount': [1e4] * len(_DATES),
        'pct_chg': [0.0] * len(_DATES),
        'change': [0.0] * len(_DATES),
        'qfq_factor': [1.0] * len(_DATES),
        'hfq_factor': [1.0] * len(_DATES),
    })


def _store_root(tmp_path) -> Path:
    """两级深的临时列存根，保证 staging 落点留在 tmp_path 内。"""
    return tmp_path / 'data' / 'local_store'


def _write_quotes(root, codes: list[str], volume: float = 1000.0) -> LocalQuoteStore:
    store = LocalQuoteStore(root, enabled=True)
    for code in codes:
        store.write_daily('stock', code, _quote_frame(code, volume))
    store.flush_meta()
    return store


def _turn_frame(rows: list[tuple[str, date, float]]) -> pl.DataFrame:
    return pl.DataFrame({
        'ts_code': [r[0] for r in rows],
        'trade_date': [r[1] for r in rows],
        'turn': [r[2] for r in rows],
        'source': ['baostock'] * len(rows),
        'fetched_at': [datetime(2026, 11, 4)] * len(rows),
    }, schema=TURN_SCHEMA)


def _write_turnover(root, rows: list[tuple[str, date, float]]) -> None:
    path = turnover_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    _turn_frame(rows).write_parquet(path)


def _write_shares(root, rows: list[tuple[str, date, int]]) -> None:
    frame = pl.DataFrame({
        'ts_code': [r[0] for r in rows],
        'effective_date': [r[1] for r in rows],
        'total_share': [r[2] for r in rows],
        'float_share': [r[2] for r in rows],
        'free_float_share': [r[2] for r in rows],
        'change_reason': ['test'] * len(rows),
        'source': ['test'] * len(rows),
        'fetched_at': [datetime(2026, 11, 4)] * len(rows),
    })
    share_module.write_shares(frame, root, write_meta=False)


def _install_baostock(monkeypatch, **attrs) -> types.SimpleNamespace:
    fake = types.SimpleNamespace(**attrs)
    monkeypatch.setitem(sys.modules, 'baostock', fake)
    return fake


# ----------------------------------------------------------------------
# 读取与来源分级
# ----------------------------------------------------------------------
def test_turnover_and_shares_paths(tmp_path):
    assert turnover_path(tmp_path) == tmp_path / 'factors' / 'stock_turnover.parquet'
    assert shares_path(tmp_path) == tmp_path / 'meta' / 'factors' / 'stock_shares.parquet'


def test_read_turnover_missing_cache_and_lru(tmp_path):
    assert read_turnover('000001.SZ', tmp_path) is None

    _write_turnover(tmp_path, [('000001.SZ', d, float(i)) for i, d in enumerate(_DATES)])
    frame = read_turnover('000001.SZ', tmp_path)
    assert frame is not None
    assert frame['trade_date'].to_list() == _DATES
    # 第二次命中缓存；未知代码的 None 也会缓存
    assert read_turnover('000001.SZ', tmp_path).equals(frame)
    assert read_turnover('999999.SZ', tmp_path) is None

    # 超过 LRU 上限后淘汰最旧条目
    many = [(f'{i:06d}.SZ', _DATES[0], 1.0) for i in range(140)]
    other_root = tmp_path / 'many'
    _write_turnover(other_root, many)
    for code, _, _ in many:
        read_turnover(code, other_root)
    assert len(turn_module._TURN_CACHE) <= turn_module._LRU_MAX


def test_read_turnover_cache_key_includes_columns(tmp_path):
    """同一标的先窄列后宽列必须各取各的列（缓存键含 columns）。"""
    _write_turnover(
        tmp_path, [('000001.SZ', d, float(i)) for i, d in enumerate(_DATES)])

    narrow = read_turnover('000001.SZ', tmp_path, columns=('trade_date', 'turn'))
    assert narrow is not None
    assert narrow.columns == ['trade_date', 'turn']

    wide = read_turnover(
        '000001.SZ', tmp_path, columns=('trade_date', 'turn', 'source'))
    assert wide is not None
    assert wide.columns == ['trade_date', 'turn', 'source']

    # 反向再取窄列：不能被宽列缓存污染
    again = read_turnover('000001.SZ', tmp_path, columns=('trade_date', 'turn'))
    assert again is not None
    assert again.columns == ['trade_date', 'turn']


def test_read_float_share_series_paths(tmp_path, monkeypatch):
    assert read_float_share_series('000001.SZ', tmp_path) is None

    _write_shares(tmp_path, [('000001.SZ', date(2023, 1, 1), 100000000)])
    frame = read_float_share_series('000001.SZ', tmp_path)
    assert frame is not None
    assert frame.columns == ['effective_date', 'float_share']
    assert read_float_share_series('999999.SZ', tmp_path) is None

    monkeypatch.setattr(turn_module, '_share_module_names', lambda: None)
    assert read_float_share_series('000001.SZ', tmp_path) is None

    def boom(*_a, **_k):
        raise ValueError('bad')

    monkeypatch.setattr(
        turn_module, '_share_module_names', lambda: {'shares_frame': boom})
    assert read_float_share_series('000001.SZ', tmp_path) is None


def test_normalize_date_col_variants():
    base = pl.DataFrame({'date': ['2024-01-02'], 'v': [1]})
    assert _normalize_date_col(base.lazy(), 'date').collect()['date'].dtype == pl.Date
    as_dt = base.with_columns(pl.col('date').str.to_datetime()).lazy()
    assert _normalize_date_col(as_dt, 'date').collect()['date'].dtype == pl.Date
    as_date = pl.DataFrame({
        'date': [date(2024, 1, 2)], 'v': [1]}).lazy()
    assert _normalize_date_col(as_date, 'date').collect()['date'].dtype == pl.Date


def test_canonical_code_keeps_unparseable():
    assert _canonical_code('000001.SZ') == '000001.SZ'
    assert _canonical_code('') == ''


def test_attach_turn_requires_columns_and_skips_existing(tmp_path):
    root = _store_root(tmp_path)
    frame = pl.DataFrame({'volume': [1.0] * 5})
    out = attach_turn(frame, '000001.SZ', root).collect()
    assert 'turn' not in out.columns

    ready = _quote_frame('000001.SZ').with_columns(pl.lit(1.23).alias('turn'))
    out = attach_turn(ready, '000001.SZ', root).collect()
    assert out['turn'].to_list() == [1.23] * 5


def test_attach_turn_stored_source(tmp_path):
    root = _store_root(tmp_path)
    _write_quotes(root, ['000001.SZ'])
    _write_turnover(root, [('000001.SZ', d, 1.5) for d in _DATES])
    out = attach_turn(_quote_frame('000001.SZ'), '000001.SZ', root).collect()
    assert out['turn'].to_list() == [1.5] * 5
    assert out['turn_source'].unique().to_list() == ['baostock']
    assert 'stored_source' not in out.columns


def test_attach_turn_without_any_source(tmp_path):
    root = _store_root(tmp_path)
    _write_quotes(root, ['000001.SZ'])
    out = attach_turn(_quote_frame('000001.SZ'), '000001.SZ', root).collect()
    assert out['turn'].to_list() == [None] * 5
    assert out['turn_source'].to_list() == [None] * 5


def test_attach_turn_static_and_daily_float_share(tmp_path):
    root = _store_root(tmp_path)
    _write_quotes(root, ['000001.SZ'])
    _write_shares(root, [('000001.SZ', date(2023, 1, 1), 100000000)])
    out = attach_turn(
        _quote_frame('000001.SZ'), '000001.SZ', root,
        keep_float_share=True).collect()
    # volume=1000 手 → 100000 股 → /1e8 → 0.1%
    assert out['turn'].to_list() == pytest.approx([0.1] * 5)
    assert out['turn_source'].unique().to_list() == ['float_share_static']
    assert 'float_share' in out.columns

    daily_root = _store_root(tmp_path) / 'daily'
    _write_quotes(daily_root, ['000001.SZ'])
    _write_shares(daily_root, [
        ('000001.SZ', date(2023, 1, 1), 100000000),
        ('000001.SZ', _DATES[2], 200000000),
    ])
    out = attach_turn(
        _quote_frame('000001.SZ'), '000001.SZ', daily_root,
        keep_float_share=True).collect()
    assert out['turn_source'].to_list() == [
        'float_share', 'float_share', 'float_share', 'float_share', 'float_share']
    assert out['turn'][0] == pytest.approx(0.1)
    assert out['turn'][-1] == pytest.approx(0.05)


# ----------------------------------------------------------------------
# 上游拉取
# ----------------------------------------------------------------------
def test_worker_init_and_ensure_login(monkeypatch):
    monkeypatch.setattr(turn_module.time, 'sleep', lambda _s: None)
    calls = {'login': 0}

    def login():
        calls['login'] += 1
        return types.SimpleNamespace(error_code='0', error_msg='')

    _install_baostock(monkeypatch, login=login)
    _worker_init()
    assert turn_module._SESSION['login'] is True

    turn_module._SESSION.clear()
    _ensure_login()
    assert calls['login'] == 2
    _ensure_login()  # 已登录直接返回
    assert calls['login'] == 2


def test_ensure_login_failures_and_block(monkeypatch):
    monkeypatch.setattr(turn_module.time, 'sleep', lambda _s: None)
    _install_baostock(
        monkeypatch,
        login=lambda: types.SimpleNamespace(error_code='1', error_msg='denied'))
    for attempt in range(1, 3):
        with pytest.raises(DataSourceConnectionError, match='登录失败'):
            _ensure_login()
        assert turn_module._SESSION['login_fails'] == attempt
    with pytest.raises(DataSourceConnectionError):
        _ensure_login()
    assert turn_module._SESSION['blocked'] is True
    with pytest.raises(RuntimeError, match='熔断'):
        _ensure_login()


def test_query_turn_range_success_and_errors(monkeypatch):
    monkeypatch.setattr(exc_module.time, 'sleep', lambda _s: None)
    monkeypatch.setattr(
        turn_module.schema, 'result_to_frame',
        lambda _rs: pl.DataFrame({'date': ['2024-01-02'], 'turn': ['1.5']}))
    good_rs = types.SimpleNamespace(error_code='0', error_msg='')
    _install_baostock(
        monkeypatch,
        login=lambda: types.SimpleNamespace(error_code='0', error_msg=''),
        query_history_k_data_plus=lambda *_a, **_k: good_rs)
    frame = _query_turn_range('000001.SZ', date(2024, 1, 2), date(2024, 1, 5))
    assert frame.height == 1

    _install_baostock(
        monkeypatch,
        login=lambda: types.SimpleNamespace(error_code='0', error_msg=''),
        query_history_k_data_plus=lambda *_a, **_k: None)
    with pytest.raises(DataSourceConnectionError, match='返回 None'):
        _query_turn_range('000001.SZ', date(2024, 1, 2), date(2024, 1, 5))

    _install_baostock(
        monkeypatch,
        login=lambda: types.SimpleNamespace(error_code='0', error_msg=''),
        query_history_k_data_plus=lambda *_a, **_k: types.SimpleNamespace(
            error_code='10001', error_msg='bad'))
    with pytest.raises(DataSourceConnectionError, match='10001'):
        _query_turn_range('000001.SZ', date(2024, 1, 2), date(2024, 1, 5))


def test_frame_to_records_filters_bad_rows():
    assert _frame_to_records(None, 'A.SZ', datetime(2026, 11, 4)).is_empty()
    assert _frame_to_records(
        pl.DataFrame(), 'A.SZ', datetime(2026, 11, 4)).is_empty()
    raw = pl.DataFrame({
        'date': ['2024-01-02', 'bad-date', '2024-01-04', '2024-01-05'],
        'turn': ['1.5', '2.0', '', '-1'],
    })
    out = _frame_to_records(raw, 'A.SZ', datetime(2026, 11, 4))
    assert out.height == 1
    assert out['turn'][0] == 1.5
    assert out['source'][0] == 'baostock'


def test_fetch_code_success_and_year_fallback(monkeypatch):
    raw = pl.DataFrame({'date': ['2024-01-02'], 'turn': ['1.0']})
    calls: list[tuple[date, date]] = []

    def full_ok(code, start, end):
        calls.append((start, end))
        return raw

    monkeypatch.setattr(turn_module, '_query_turn_range', full_ok)
    out = _fetch_code('A.SZ', date(2024, 1, 2), date(2024, 1, 5))
    assert out.height == 1
    assert calls == [(date(2024, 1, 2), date(2024, 1, 5))]

    # 整段返回空 → 按自然年分段；每段都有数据
    calls.clear()

    def per_year(code, start, end):
        calls.append((start, end))
        if (start, end) == (date(2022, 6, 1), date(2024, 3, 1)):
            return pl.DataFrame({'date': [], 'turn': []})
        return pl.DataFrame({'date': [start.isoformat()], 'turn': ['1.0']})

    monkeypatch.setattr(turn_module, '_query_turn_range', per_year)
    out = _fetch_code('A.SZ', date(2022, 6, 1), date(2024, 3, 1))
    assert calls == [
        (date(2022, 6, 1), date(2024, 3, 1)),
        (date(2022, 6, 1), date(2022, 12, 31)),
        (date(2023, 1, 1), date(2023, 12, 31)),
        (date(2024, 1, 1), date(2024, 3, 1)),
    ]
    assert out.height == 3


def test_fetch_code_connection_failures(monkeypatch):
    def denied(*_a, **_k):
        raise DataSourceConnectionError(source='baostock', message='down')

    monkeypatch.setattr(turn_module, '_query_turn_range', denied)
    out = _fetch_code('A.SZ', date(2024, 1, 2), date(2024, 1, 5))
    assert out.is_empty()

    def dropped(*_a, **_k):
        raise ConnectionError('reset')

    monkeypatch.setattr(turn_module, '_query_turn_range', dropped)
    out = _fetch_code('A.SZ', date(2022, 6, 1), date(2024, 3, 1))
    assert out.is_empty()


def test_worker_task_results(monkeypatch):
    payload = {'code': 'A.SZ', 'start': '2024-01-02', 'end': '2024-01-05'}
    monkeypatch.setattr(
        turn_module, '_fetch_code',
        lambda *_a, **_k: _turn_frame([('A.SZ', _DATES[-1], 1.0)]))
    result = _worker_task(payload)
    assert result['rows'] == 1
    assert result['last_date'] == _DATES[-1].isoformat()

    monkeypatch.setattr(
        turn_module, '_fetch_code', lambda *_a, **_k: pl.DataFrame(schema=TURN_SCHEMA))
    assert _worker_task(payload)['error'] == 'empty'

    def boom(*_a, **_k):
        raise RuntimeError('crash')

    monkeypatch.setattr(turn_module, '_fetch_code', boom)
    assert _worker_task(payload)['error'] == 'RuntimeError: crash'


# ----------------------------------------------------------------------
# 分块、断点与合并
# ----------------------------------------------------------------------
def test_write_helpers(tmp_path):
    part = tmp_path / 'part_00000.parquet'
    _write_part(part, _turn_frame([('A.SZ', _DATES[0], 1.0)]))
    assert part.exists()

    meta = tmp_path / 'manifest.json'
    _write_json(meta, {'status': 'running'})
    assert _load_json(meta) == {'status': 'running'}
    assert _load_json(tmp_path / 'missing.json') == {}
    meta.write_text('{bad', encoding='utf-8')
    assert _load_json(meta) == {}

    progress = tmp_path / 'progress.csv'
    _write_progress_csv(progress, {'B.SZ': '2024-01-05', 'A.SZ': '2024-01-04'}, [
        {'code': 'C.SZ', 'start': 'a', 'end': 'b',
         'error': 'x"y', 'retries_used': 3},
    ])
    lines = progress.read_text(encoding='utf-8').splitlines()
    assert lines[0] == 'code,last_date'
    assert lines[1] == 'A.SZ,2024-01-04'
    failed = (tmp_path / 'failed.csv').read_text(encoding='utf-8').splitlines()
    assert failed[0] == 'code,start,end,error,retries_used'
    assert "x'y" in failed[1]
    assert failed[1].endswith(',3')

    header_only = tmp_path / 'empty.csv'
    _write_failed_csv(header_only, [])
    assert header_only.read_text(encoding='utf-8').splitlines() == [
        'code,start,end,error,retries_used']


def test_assemble_keeps_history_when_touched_empty(tmp_path):
    """复查缺陷 4：target 存在 + touched 为空时不得整体替换正式表。"""
    target = tmp_path / 'stock_turnover.parquet'
    history = _turn_frame([('A.SZ', d, 1.0) for d in _DATES])
    history.write_parquet(target)
    part = tmp_path / 'part_00000.parquet'
    _turn_frame([
        ('A.SZ', _DATES[-1], 9.9),          # 同日更新
        ('A.SZ', _DATES[-1] + timedelta(days=1), 2.0),  # 新增
        ('B.SZ', _DATES[0], 3.0),           # 新增标的历史
    ]).write_parquet(part)

    total = _assemble([part], target, set())
    out = pl.read_parquet(target).sort(['ts_code', 'trade_date'])
    assert total == 7
    assert out.height == 7
    assert out.filter(pl.col('ts_code') == 'A.SZ').height == 6
    assert out.filter(
        (pl.col('ts_code') == 'A.SZ') & (pl.col('trade_date') == _DATES[-1])
    )['turn'][0] == 9.9
    assert out.filter(pl.col('ts_code') == 'A.SZ')['turn'].head(4).to_list() == [
        1.0, 1.0, 1.0, 1.0]


def test_assemble_without_target(tmp_path):
    target = tmp_path / 'stock_turnover.parquet'
    part = tmp_path / 'part_00000.parquet'
    _turn_frame([('A.SZ', _DATES[0], 1.0)]).write_parquet(part)
    assert _assemble([part], target, {'A.SZ'}) == 1
    assert _assemble([], target, set()) is None


def test_existing_last_dates(tmp_path):
    target = tmp_path / 'stock_turnover.parquet'
    assert _existing_last_dates(target) == {}
    _turn_frame([
        ('A.SZ', _DATES[0], 1.0),
        ('A.SZ', _DATES[2], 1.0),
        ('B.SZ', _DATES[0], 1.0),
    ]).write_parquet(target)
    assert _existing_last_dates(target) == {
        'A.SZ': _DATES[2].isoformat(), 'B.SZ': _DATES[0].isoformat()}

    target.write_bytes(b'not a parquet')
    assert _existing_last_dates(target) == {}


def test_build_tasks_branches(tmp_path):
    store = _write_quotes(_store_root(tmp_path), ['A.SZ', 'B.SZ'])
    tasks, skipped = _build_tasks(store, ['A.SZ', 'B.SZ', 'C.SZ'], {})
    assert skipped == 1
    assert len(tasks) == 2
    assert tasks[0]['start'] == _DATES[0].isoformat()

    # 已有末日期 → 从次日开始
    tasks, _ = _build_tasks(
        store, ['A.SZ'], {'A.SZ': _DATES[1].isoformat()})
    assert tasks[0]['start'] == _DATES[2].isoformat()

    # 已是最新 → 不生成任务
    tasks, _ = _build_tasks(
        store, ['A.SZ'], {'A.SZ': _DATES[-1].isoformat()})
    assert tasks == []


def test_select_universe_and_cleanup(tmp_path):
    store = _write_quotes(_store_root(tmp_path), ['A.SZ', 'B.SZ', 'C.SZ'])
    assert _select_universe(store, None, None) == ['A.SZ', 'B.SZ', 'C.SZ']
    assert _select_universe(store, ['a.sz'], None) == ['A.SZ']
    assert _select_universe(store, None, 2) == ['A.SZ', 'B.SZ']

    parts_dir = tmp_path / 'parts'
    parts_dir.mkdir()
    (parts_dir / 'part_00000.parquet').write_bytes(b'x')
    (parts_dir / 'keep.txt').write_text('keep', encoding='utf-8')
    _cleanup_stale_parts(parts_dir)
    assert not (parts_dir / 'part_00000.parquet').exists()
    assert (parts_dir / 'keep.txt').exists()


def test_load_resume_state(tmp_path):
    parts_dir = tmp_path / 'parts'
    parts_dir.mkdir()
    manifest = parts_dir / 'manifest.json'
    state = {
        'status': 'running', 'codes_hash': 'abc', 'completed_parts': [0, 1],
        'completed': {'A.SZ': '2024-01-05'}, 'failed': [{'code': 'B.SZ'}],
    }
    manifest.write_text(json.dumps(state), encoding='utf-8')

    # 非续跑：清理残留分块并返回空状态
    (parts_dir / 'part_00000.parquet').write_bytes(b'x')
    resuming, parts, done, failed, _ = _load_resume_state(
        parts_dir, manifest, 'abc', resume=False, total_chunks=2)
    assert resuming is False
    assert parts == set() and done == {} and failed == []
    assert not (parts_dir / 'part_00000.parquet').exists()

    # 续跑：只认实际存在的分块
    (parts_dir / 'part_00001.parquet').write_bytes(b'x')
    resuming, parts, done, failed, _ = _load_resume_state(
        parts_dir, manifest, 'abc', resume=True, total_chunks=2)
    assert resuming is True
    assert parts == {1}
    assert done == {'A.SZ': '2024-01-05'}
    assert failed == [{'code': 'B.SZ'}]

    # hash 不匹配 → 不续跑
    resuming, _, done, _, _ = _load_resume_state(
        parts_dir, manifest, 'other', resume=True, total_chunks=2)
    assert resuming is False and done == {}


def test_run_chunk_inline_success_failure_and_block(tmp_path, monkeypatch):
    monkeypatch.setattr(turn_module.time, 'sleep', lambda _s: None)
    monkeypatch.setattr(turn_module, '_worker_init',
                        lambda: turn_module._SESSION.update(login=True))

    def fake_task(task):
        if task['code'] == 'EMPTY.SZ':
            return {'code': 'EMPTY.SZ', 'records': None, 'error': 'empty'}
        if task['code'] == 'BOOM.SZ':
            return {'code': 'BOOM.SZ', 'records': None, 'error': 'x'}
        return {'code': task['code'], 'records': _turn_frame(
            [(task['code'], _DATES[0], 1.0)]), 'error': None,
            'rows': 1, 'last_date': _DATES[0].isoformat()}

    monkeypatch.setattr(turn_module, '_worker_task', fake_task)
    chunk = [
        {'code': 'A.SZ', 'start': 'x', 'end': 'y'},
        {'code': 'EMPTY.SZ', 'start': 'x', 'end': 'y'},
        {'code': 'BOOM.SZ', 'start': 'x', 'end': 'y'},
    ]
    frames, done, failures = _run_chunk_inline(chunk)
    assert len(frames) == 1
    assert done == {'A.SZ': _DATES[0].isoformat()}
    assert [f['retries_used'] for f in failures] == [0, 3]

    turn_module._SESSION['blocked'] = True
    frames, done, _ = _run_chunk_inline(chunk)
    assert frames == [] and done == {}


def test_run_chunk_with_fake_executor(monkeypatch):
    class FakeFuture:
        def __init__(self, result=None, exc=None):
            self._result = result
            self._exc = exc

        def result(self):
            if self._exc is not None:
                raise self._exc
            return self._result

    class FakeExecutor:
        def __init__(self, *a, **k):
            self.futures = []

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def submit(self, _fn, task):
            future = FakeFuture(
                {'code': task['code'], 'records': _turn_frame([
                    (task['code'], _DATES[0], 1.0)]), 'error': None,
                 'last_date': _DATES[0].isoformat()})
            self.futures.append(future)
            return future

    monkeypatch.setattr(turn_module, 'ProcessPoolExecutor', FakeExecutor)
    monkeypatch.setattr(turn_module, 'as_completed', lambda futures: list(futures))

    chunk = [{'code': 'A.SZ', 'start': 'x', 'end': 'y'},
             {'code': 'B.SZ', 'start': 'x', 'end': 'y'}]
    frames, done, failures = _run_chunk(chunk, workers=1)
    assert len(frames) == 2 and failures == []
    assert set(done) == {'A.SZ', 'B.SZ'}


def test_run_turnover_chunks_writes_progress(tmp_path, monkeypatch):
    monkeypatch.setattr(turn_module.time, 'sleep', lambda _s: None)
    monkeypatch.setattr(
        turn_module, '_run_chunk_inline',
        lambda chunk: ([_turn_frame([(chunk[0]['code'], _DATES[0], 1.0)])],
                       {chunk[0]['code']: _DATES[0].isoformat()}, []))
    parts_dir = tmp_path / 'parts'
    run = _TurnoverRun(
        chunks=[[{'code': 'A.SZ', 'start': 'x', 'end': 'y'}]],
        parts_dir=parts_dir, manifest_path=tmp_path / 'manifest.json',
        progress_path=tmp_path / 'progress.csv', codes_hash='h',
        chunk_size=200, total_codes=1, started=turn_module.time.perf_counter(),
        completed_parts=set(), completed_codes={}, failed_items=[],
        resuming=False, state_started_at=None)
    _run_turnover_chunks(run, workers=1)
    assert (parts_dir / 'part_00000.parquet').exists()
    assert run.completed_parts == {0}
    manifest = json.loads(
        (tmp_path / 'manifest.json').read_text(encoding='utf-8'))
    assert manifest['status'] == 'running'
    assert manifest['completed'] == {'A.SZ': _DATES[0].isoformat()}
    assert (tmp_path / 'progress.csv').exists()

    # 熔断：写入分块但不标记完成，并停止后续分块
    turn_module._SESSION['blocked'] = True
    blocked_run = _TurnoverRun(
        chunks=[[{'code': 'B.SZ', 'start': 'x', 'end': 'y'}],
                [{'code': 'C.SZ', 'start': 'x', 'end': 'y'}]],
        parts_dir=parts_dir, manifest_path=tmp_path / 'm2.json',
        progress_path=tmp_path / 'p2.csv', codes_hash='h',
        chunk_size=200, total_codes=2, started=turn_module.time.perf_counter(),
        completed_parts=set(), completed_codes={}, failed_items=[],
        resuming=False, state_started_at=None)
    _run_turnover_chunks(blocked_run, workers=1)
    assert blocked_run.completed_parts == set()
    assert not (parts_dir / 'part_00001.parquet').exists()


def test_backfill_turnover_dry_run_and_no_tasks(tmp_path):
    root = _store_root(tmp_path)
    store = _write_quotes(root, ['A.SZ', 'B.SZ'])
    out = backfill_turnover(store, root, dry_run=True)
    assert out['codes'] == 2
    assert out['planned_rows'] == 10
    assert out['skipped_no_quotes'] == 0

    empty_root = tmp_path / 'data' / 'empty_store'
    empty_store = LocalQuoteStore(empty_root, enabled=True)
    out = backfill_turnover(empty_store, empty_root, dry_run=True)
    assert out == {'codes': 0, 'planned_rows': 0,
                   'skipped_no_quotes': 0, 'dry_run': True}

    _write_turnover(root, [
        ('A.SZ', _DATES[-1], 1.0), ('B.SZ', _DATES[-1], 1.0)])
    out = backfill_turnover(store, root)
    assert out == {'codes': 0, 'rows': 0, 'failed': 0, 'elapsed_s': 0.0}


def test_backfill_turnover_full_run(tmp_path, monkeypatch):
    root = _store_root(tmp_path)
    store = _write_quotes(root, ['A.SZ', 'B.SZ'])
    monkeypatch.setattr(turn_module.time, 'sleep', lambda _s: None)
    monkeypatch.setattr(
        turn_module, '_run_chunk_inline',
        lambda chunk: ([_turn_frame([(t['code'], _DATES[0], 1.0) for t in chunk])],
                       {t['code']: _DATES[0].isoformat() for t in chunk}, []))
    out = backfill_turnover(store, root, chunk_size=1)
    assert out['codes'] == 2
    assert out['codes_written'] == 2
    assert out['total_rows'] == 2
    assert out['failed'] == 0
    assert turnover_path(root).exists()
    assert (tmp_path / '_staging' / 'turnover_parts' / 'manifest.json').exists()
    assert (tmp_path / '_staging' / 'turnover_parts' / 'progress.csv').exists()
    assert (tmp_path / '_staging' / 'turnover_parts' / 'failed.csv').exists()
    # 合并后分块删除
    assert not list((tmp_path / '_staging' / 'turnover_parts').glob('part_*.parquet'))


def test_backfill_turnover_resume_skips_completed_parts(tmp_path, monkeypatch):
    root = _store_root(tmp_path)
    store = _write_quotes(root, ['A.SZ'])
    parts_dir = tmp_path / '_staging' / 'turnover_parts'
    parts_dir.mkdir(parents=True)
    task = {'code': 'A.SZ', 'start': _DATES[0].isoformat(),
            'end': _DATES[-1].isoformat()}
    codes_hash = hashlib.sha1(task['code'].encode('utf-8')).hexdigest()
    _write_part(parts_dir / 'part_00000.parquet',
                _turn_frame([('A.SZ', _DATES[0], 1.0)]))
    (parts_dir / 'manifest.json').write_text(json.dumps({
        'status': 'running', 'codes_hash': codes_hash,
        'completed_parts': [0], 'completed': {'A.SZ': _DATES[0].isoformat()},
        'failed': [], 'started_at': '2026-11-01T00:00:00',
    }), encoding='utf-8')

    def explode(_chunk):
        raise AssertionError('续跑不应重抓已完成分块')

    monkeypatch.setattr(turn_module, '_run_chunk_inline', explode)
    out = backfill_turnover(store, root, resume=True)
    assert out['codes_written'] == 1
    assert out['total_rows'] == 1
    assert turnover_path(root).exists()


# ----------------------------------------------------------------------
# 本地派生
# ----------------------------------------------------------------------
def test_derive_requires_share_snapshot(tmp_path):
    root = _store_root(tmp_path)
    _write_quotes(root, ['A.SZ'])
    with pytest.raises(FileNotFoundError, match='股本快照缺失'):
        derive_turnover_local(root)


def test_derive_dry_run_and_filters(tmp_path):
    root = _store_root(tmp_path)
    _write_quotes(root, ['A.SZ', 'B.SZ'])
    _write_shares(root, [('A.SZ', date(2023, 1, 1), 100000000),
                         ('B.SZ', date(2023, 1, 1), 100000000)])
    out = derive_turnover_local(root)
    assert out['gap_rows'] == 10
    assert out['derived_rows'] == 10
    assert out['no_share_rows'] == 0
    assert out['dropped_nonpositive'] == 0
    assert out['codes'] == 2
    assert out['written'] is False
    assert out['date_min'] == str(_DATES[0])
    assert out['date_max'] == str(_DATES[-1])
    assert out['by_date_tail'][str(_DATES[-1])] == 2

    filtered = derive_turnover_local(root, codes=['A.SZ'], start=_DATES[2])
    assert filtered['codes'] == 1
    assert filtered['gap_rows'] == 3
    assert filtered['date_min'] == str(_DATES[2])

    with pytest.raises(ValueError, match='codes 过滤后为空'):
        derive_turnover_local(root, codes=['  '])


def test_derive_no_gap_and_no_share(tmp_path):
    root = _store_root(tmp_path)
    _write_quotes(root, ['A.SZ'])
    _write_shares(root, [('A.SZ', _DATES[0], 100000000)])
    _write_turnover(root, [('A.SZ', d, 1.0) for d in _DATES])
    out = derive_turnover_local(root)
    assert out == {'gap_rows': 0, 'derived_rows': 0,
                   'written': False, 'target': str(turnover_path(root))}

    later = tmp_path / 'later'
    _write_quotes(later, ['A.SZ'])
    _write_shares(later, [('A.SZ', _DATES[-1] + timedelta(days=30), 100000000)])
    out = derive_turnover_local(later)
    assert out['gap_rows'] == 5
    assert out['no_share_rows'] == 5
    assert out['derived_rows'] == 0
    assert out['written'] is False


def test_derive_writes_and_is_idempotent(tmp_path):
    root = _store_root(tmp_path)
    _write_quotes(root, ['A.SZ'])
    _write_shares(root, [('A.SZ', date(2023, 1, 1), 100000000)])
    _write_turnover(root, [('A.SZ', _DATES[0], 1.0)])

    out = derive_turnover_local(root, dry_run=False)
    assert out['gap_rows'] == 4
    assert out['written'] is True
    assert out['before_rows'] == 1
    assert out['after_rows'] == 5
    assert out['rows_added'] == 4

    written = pl.read_parquet(turnover_path(root)).sort('trade_date')
    assert written['turn'].to_list() == pytest.approx([1.0, 0.1, 0.1, 0.1, 0.1])
    assert written['source'][1] == TURN_DERIVED_SOURCE

    again = derive_turnover_local(root, dry_run=False)
    assert again['gap_rows'] == 0
    assert again['written'] is False
