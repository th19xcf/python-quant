#!/usr/bin/env python3
"""补齐本地行情库的「内部历史缺口」（数据源：本地通达信 .day）。

只做一件事：TDX 有、本地 parquet 没有的历史交易日，按本地 schema 追加行。
- 不改任何已有行；TDX 比本地短的标的（tdx_truncated）一律跳过；
- 同一只票重复执行不产生重复行（按 (ts_code, date) 去重，重跑写入 0 行）；
- 执行前把原分片备份到 ``data/local_store/_backup/internal_gap_fix/<日期>/``；
- 每 50 只落一次进度，中断后从断点续跑。

用法::

    .venv\\Scripts\\python.exe tools\\fill_internal_gaps.py inventory
    .venv\\Scripts\\python.exe tools\\fill_internal_gaps.py fill [--reset-progress]
    .venv\\Scripts\\python.exe tools\\fill_internal_gaps.py verify
    .venv\\Scripts\\python.exe tools\\fill_internal_gaps.py report-jumps
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.data.local_store.build import discover_local_files  # noqa: E402
from src.data.local_store.store import LocalFactorStore, LocalQuoteStore  # noqa: E402
from src.data.tdx_day_parser import parse_day_file  # noqa: E402

STORE = ROOT / 'data' / 'local_store'
TDX = Path(r'd:\zxzq\vipdoc')
UNIVERSE = STORE / 'meta' / 'universe'
EXCLUSIONS = UNIVERSE / 'universe_exclusions.parquet'
JUMPS = UNIVERSE / 'universe_price_jumps.parquet'
INVENTORY = UNIVERSE / 'internal_gap_inventory.csv'
PROGRESS = UNIVERSE / 'internal_gap_fix_progress.json'
FAILED = UNIVERSE / 'internal_gap_fix_failed.csv'
BACKUP_ROOT = ROOT / '_backup' / 'internal_gap_fix'
BATCH = 50
# 因子比对容差（file 内嵌副本 vs 因子快照）
FACTOR_TOL = 1e-9
# 成交量比对容差（只允许 float32 舍入）
VOLUME_TOL = 0.01
AMOUNT_TOL = 1e-6


def _local_path(kind: str, ts_code: str) -> Path:
    market = ts_code.split('.')[1].lower()
    return STORE / 'quotes' / kind / market / f'{ts_code}.parquet'


def _tdx_paths() -> dict[tuple[str, str], Path]:
    return {(kind, code): path for kind, code, path in discover_local_files(TDX, 'all')}


def _load_gap_codes() -> pl.DataFrame:
    return pl.read_parquet(EXCLUSIONS).filter(
        pl.col('reason_code') == 'internal_gap').sort(['asset_class', 'ts_code'])


def _original_gap_codes() -> pl.DataFrame:
    """盘点清单里的 279 只（排除清单刷新后仍可复验）。"""
    return pl.read_csv(INVENTORY).select('ts_code', 'asset_class').sort(
        ['asset_class', 'ts_code'])


def _segments(dates: list[date]) -> list[tuple[date, date]]:
    """把交易日集合切成连续片段（按自然日相邻不算断，交易日间隔 > 7 天算断）。"""
    if not dates:
        return []
    out: list[tuple[date, date]] = []
    start = prev = dates[0]
    for cur in dates[1:]:
        if (cur - prev).days > 7:
            out.append((start, prev))
            start = cur
        prev = cur
    out.append((start, prev))
    return out


def _existing_hash(frame: pl.DataFrame) -> str:
    """对已有数据做内容哈希（与文件物理字节无关）。"""
    if frame.is_empty():
        return ''
    rows = frame.sort('date').select(sorted(frame.columns)).to_dicts()
    payload = json.dumps(
        [[str(r[k]) for k in sorted(frame.columns)] for r in rows],
        ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(payload.encode()).hexdigest()


# ----------------------------------------------------------------------
# 盘点
# ----------------------------------------------------------------------
def inventory() -> pl.DataFrame:
    gaps = _load_gap_codes()
    tdx_map = _tdx_paths()
    factor_store = LocalFactorStore(STORE)
    rows: list[dict[str, Any]] = []
    for item in gaps.iter_rows(named=True):
        kind, code = item['asset_class'], item['ts_code']
        path = _local_path(kind, code)
        tdx_path = tdx_map.get((kind, code))
        rec: dict[str, Any] = {
            'ts_code': code, 'asset_class': kind,
            'listed_missing_rows': int(item['missing_rows']),
            'tdx_file_exists': bool(tdx_path is not None and tdx_path.is_file()),
            'tdx_rows': 0, 'local_rows': 0,
            'local_start': None, 'local_end': None,
            'tdx_start': None, 'tdx_end': None,
            'missing_count': 0, 'missing_start': None, 'missing_end': None,
            'missing_segments': 0,
            'leading_missing': 0, 'internal_missing': 0,
            'local_only_count': 0, 'overlap_rows': 0,
            'close_diff_rows': 0, 'volume_diff_rows': 0, 'amount_diff_rows': 0,
            'tdx_dup_dates': 0, 'missing_zero_volume': 0, 'missing_bad_ohlc': 0,
            'factor_snapshot_rows': 0, 'factor_mismatch_rows': 0,
            'factor_change_on_missing': 0,
            'eligible': False, 'skip_reason': '',
            'existing_hash': '',
        }
        if not path.is_file():
            rec['skip_reason'] = 'local_file_missing'
            rows.append(rec)
            continue
        local = pl.read_parquet(path).sort('date')
        rec['local_rows'] = local.height
        rec['local_start'] = local['date'][0]
        rec['local_end'] = local['date'][-1]
        rec['existing_hash'] = _existing_hash(local)
        if tdx_path is None or not tdx_path.is_file():
            rec['skip_reason'] = 'tdx_file_missing'
            rows.append(rec)
            continue
        tdx = parse_day_file(tdx_path)
        if tdx.is_empty():
            rec['skip_reason'] = 'tdx_empty'
            rows.append(rec)
            continue
        rec['tdx_dup_dates'] = tdx.height - tdx['date'].n_unique()
        tdx = tdx.unique(subset=['date'], keep='last').sort('date')
        rec['tdx_rows'] = tdx.height
        rec['tdx_start'] = tdx['date'][0]
        rec['tdx_end'] = tdx['date'][-1]

        half_open = pl.read_parquet(path, columns=['date', 'close', 'volume', 'amount'])
        rec['local_only_count'] = local.height - int(local.join(
            tdx.select('date'), on='date', how='semi').height)

        overlap = half_open.sort('date').join(
            tdx.select('date', 'close', 'volume', 'amount'), on='date',
            how='inner', suffix='_tdx')
        rec['overlap_rows'] = overlap.height
        if overlap.height:
            rec['close_diff_rows'] = overlap.filter(
                (pl.col('close') - pl.col('close_tdx')).abs() > 1e-9).height
            rec['volume_diff_rows'] = overlap.filter(
                (pl.col('volume') - pl.col('volume_tdx')).abs() > VOLUME_TOL).height
            rec['amount_diff_rows'] = overlap.filter(
                (pl.col('amount') - pl.col('amount_tdx')).abs() > AMOUNT_TOL).height

        missing = tdx.filter(
            (pl.col('date') <= local['date'][-1]) &
            ~pl.col('date').is_in(local['date'].to_list()))
        rec['missing_count'] = missing.height
        if missing.height:
            mdates = missing['date'].to_list()
            rec['missing_start'] = mdates[0]
            rec['missing_end'] = mdates[-1]
            rec['missing_segments'] = len(_segments(mdates))
            first_local = local['date'][0]
            rec['leading_missing'] = sum(1 for d in mdates if d < first_local)
            rec['internal_missing'] = rec['missing_count'] - rec['leading_missing']
            rec['missing_zero_volume'] = missing.filter(pl.col('volume') <= 0).height
            rec['missing_bad_ohlc'] = missing.filter(
                (pl.col('open') <= 0) | (pl.col('high') <= 0) | (pl.col('low') <= 0) |
                (pl.col('close') <= 0) | (pl.col('high') < pl.col('low'))).height

        snap = factor_store.factors(code)
        if snap is not None and not snap.is_empty():
            rec['factor_snapshot_rows'] = snap.height
            j = local.select('date', 'qfq_factor', 'hfq_factor').join(
                snap, left_on='date', right_on='trade_date', how='inner',
                suffix='_snap')
            if j.height:
                rec['factor_mismatch_rows'] = j.filter(
                    (pl.col('qfq_factor') - pl.col('qfq_factor_snap')).abs() > FACTOR_TOL).height \
                    + j.filter(
                        (pl.col('hfq_factor') - pl.col('hfq_factor_snap')).abs() > FACTOR_TOL).height
            if missing.height:
                prev = snap.filter(pl.col('trade_date') < missing['date'][0])
                prior = prev.tail(1) if prev.height else snap.head(1)
                base_q = prior['qfq_factor'][0] if prior.height else 1.0
                inj = missing.select('date').join(
                    snap, left_on='date', right_on='trade_date', how='left')
                change = inj.filter(
                    (pl.col('qfq_factor').is_not_null()) &
                    ((pl.col('qfq_factor') - base_q).abs() > FACTOR_TOL)).height
                rec['factor_change_on_missing'] = change

        rec['eligible'] = rec['tdx_rows'] > rec['local_rows']
        if not rec['eligible']:
            rec['skip_reason'] = 'tdx_not_longer'
        elif rec['missing_count'] == 0:
            rec['skip_reason'] = 'no_missing'
        rows.append(rec)

    frame = pl.DataFrame(rows, infer_schema_length=None)
    INVENTORY.parent.mkdir(parents=True, exist_ok=True)
    frame.write_csv(INVENTORY)
    amp = frame.filter(pl.col('eligible'))
    print(f'盘点: {frame.height} 只; 可补 {amp.height} 只 / '
          f'{int(amp["missing_count"].sum())} 行; '
          f'跳过 {frame.height - amp.height} 只')
    print(frame.filter(~pl.col('eligible')).group_by('skip_reason').agg(
        pl.len().alias('codes')).sort('codes', descending=True))
    print('分类统计:')
    print(amp.group_by('asset_class').agg(
        pl.len().alias('codes'),
        pl.col('missing_count').sum().alias('rows'),
        pl.col('leading_missing').sum().alias('leading'),
        pl.col('internal_missing').sum().alias('internal'),
        pl.col('missing_segments').sum().alias('segments')).sort('asset_class'))
    print(f'清单已写: {INVENTORY}')
    return frame


# ----------------------------------------------------------------------
# 补数
# ----------------------------------------------------------------------
def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(text, encoding='utf-8', newline='\n')
    os.replace(tmp, path)


def _backup(kind: str, code: str, path: Path, day_tag: str) -> Path:
    market = code.split('.')[1].lower()
    dst = BACKUP_ROOT / day_tag / 'quotes' / kind / market / f'{code}.parquet'
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dst.exists():
        shutil.copy2(path, dst)
    return dst


def _tdx_frame(tdx: pl.DataFrame, snap: pl.DataFrame | None) -> pl.DataFrame:
    """按 build 口径（因子前向填充 + 因子比调整昨收）构造 TDX 全序列。"""
    n = tdx.height
    if snap is not None and not snap.is_empty():
        joined = pl.DataFrame({'trade_date': tdx['date']}).join(
            snap, on='trade_date', how='left')
        qfq = joined['qfq_factor'].fill_null(strategy='forward').fill_null(1.0)
        hfq = joined['hfq_factor'].fill_null(strategy='forward').fill_null(1.0)
    else:
        qfq = pl.Series('qfq_factor', [1.0] * n, dtype=pl.Float64)
        hfq = pl.Series('hfq_factor', [1.0] * n, dtype=pl.Float64)
    close = tdx['close'].cast(pl.Float64)
    prev_close = close.shift(1)
    pre_close = prev_close * (qfq.shift(1) / qfq)
    frame = pl.DataFrame({
        'date': tdx['date'].cast(pl.Date),
        'open': tdx['open'].cast(pl.Float64),
        'high': tdx['high'].cast(pl.Float64),
        'low': tdx['low'].cast(pl.Float64),
        'close': close,
        'pre_close': pre_close,
        'volume': tdx['volume'].cast(pl.Float64),
        'amount': tdx['amount'].cast(pl.Float64),
        'qfq_factor': qfq,
        'hfq_factor': hfq,
    })
    frame = frame.with_columns([
        (pl.col('close') - pl.col('pre_close')).alias('change'),
        pl.when(pl.col('pre_close').is_not_null() & (pl.col('pre_close') != 0))
        .then((pl.col('close') / pl.col('pre_close') - 1) * 100)
        .otherwise(None).alias('pct_chg'),
    ])
    return frame.with_columns([
        pl.col(c).fill_null(0.0)
        for c in ('pre_close', 'change', 'pct_chg')
    ])


def _patch_chain(merged: pl.DataFrame, inserted: list[date]) -> pl.DataFrame:
    """补入行按「合并后序列」的前一行重算 pre_close/change/pct_chg。

    仅当 TDX 文件首行本身就是缺口行时（本地有更早的历史）才会与 TDX 内部的
    链式结果不同：此时 TDX 侧无前一行，按合并序列取本地前一行更合理；
    其余补入行与 TDX 链式结果完全一致。已有行一律不动。
    """
    ctx = merged.with_columns([
        pl.col('close').shift(1).alias('_prev_close'),
        pl.col('qfq_factor').shift(1).alias('_prev_qfq'),
    ])
    prev = ctx.filter(pl.col('date').is_in(inserted)).select(
        'date',
        (pl.col('_prev_close') * (pl.col('_prev_qfq') / pl.col('qfq_factor'))
         ).alias('_pre_close'),
    )
    patch = prev.filter(pl.col('_pre_close').is_not_null())
    if patch.is_empty():
        return merged
    out = merged.join(patch, on='date', how='left')
    return out.with_columns([
        pl.when(pl.col('_pre_close').is_not_null())
        .then(pl.col('_pre_close')).otherwise(pl.col('pre_close')).alias('pre_close'),
        pl.when(pl.col('_pre_close').is_not_null())
        .then(pl.col('close') - pl.col('_pre_close')).otherwise(pl.col('change')).alias('change'),
        pl.when(pl.col('_pre_close').is_not_null() & (pl.col('_pre_close') != 0))
        .then((pl.col('close') / pl.col('_pre_close') - 1) * 100)
        .otherwise(pl.col('pct_chg')).alias('pct_chg'),
    ]).drop('_pre_close')


def _fill_one(
    kind: str, code: str, tdx_path: Path, local: pl.DataFrame,
    factor_store: LocalFactorStore, store: LocalQuoteStore, day_tag: str,
) -> dict[str, Any]:
    tdx = parse_day_file(tdx_path)
    tdx = tdx.unique(subset=['date'], keep='last').sort('date')
    if tdx.is_empty() or tdx.height <= local.height:
        return {'status': 'skipped', 'rows': 0, 'reason': 'tdx_not_longer',
                'tdx_rows': tdx.height, 'local_rows': local.height}
    local_end = local['date'][-1]
    local_dates = local['date'].to_list()
    missing = [d for d in tdx['date'].to_list() if d <= local_end and d not in local_dates]
    if not missing:
        return {'status': 'skipped', 'rows': 0, 'reason': 'no_missing',
                'tdx_rows': tdx.height, 'local_rows': local.height}

    snap = factor_store.factors(code)
    full = _tdx_frame(tdx, snap)

    # 口径自检：既有重叠日价格/因子必须同口径（允许既有源的末位舍入差异，
    # 但相对偏差超过 1% 视为量纲/口径错位，拒绝写入）
    overlap = full.join(local.select('date', 'close', 'qfq_factor', 'hfq_factor'),
                        on='date', how='inner', suffix='_loc')
    rel = (pl.col('close') - pl.col('close_loc')).abs() / pl.col('close_loc').abs()
    bad_close = overlap.filter(rel.gt(0.01).fill_null(False)).height
    close_diff_rows = overlap.filter(
        (pl.col('close') - pl.col('close_loc')).abs().gt(1e-9).fill_null(False)).height
    bad_factor = overlap.filter(
        (pl.col('qfq_factor') - pl.col('qfq_factor_loc')).abs().gt(FACTOR_TOL).fill_null(False) |
        (pl.col('hfq_factor') - pl.col('hfq_factor_loc')).abs().gt(FACTOR_TOL).fill_null(False)
    ).height
    if bad_close or bad_factor:
        return {'status': 'failed', 'rows': 0, 'overlap_close_diff': bad_close,
                'overlap_factor_diff_codes': bad_factor,
                'reason': 'overlap_mismatch'}

    new = full.filter(pl.col('date').is_in(missing)).with_columns(
        pl.lit(code).alias('ts_code'))
    new = new.select(local.columns).cast(local.schema)
    merged = pl.concat([local, new], how='vertical').unique(
        subset=['date'], keep='first').sort('date')
    merged = _patch_chain(merged, missing)
    assert merged.height == local.height + len(missing), '合并后行数不等于原行数+缺口'

    _backup(kind, code, _local_path(kind, code), day_tag)
    store.write_daily(kind, code, merged, full=True,
                      source_mtime_ns=tdx_path.stat().st_mtime_ns)
    return {'status': 'ok', 'rows': len(missing), 'tdx_rows': tdx.height,
            'local_rows_before': local.height,
            'segments_detail': [f'{a.isoformat()}~{b.isoformat()}'
                                for a, b in _segments(missing)][:6],
            'segments': len(_segments(missing)),
            'pre_existing_close_diff_rows': close_diff_rows}


def _load_progress() -> dict[str, Any]:
    if PROGRESS.is_file():
        return json.loads(PROGRESS.read_text(encoding='utf-8'))
    return {'started_at': datetime.now().isoformat(timespec='seconds'),
            'completed': {}, 'rows_inserted': 0, 'batches_flushed': 0}


def _save_progress(state: dict[str, Any]) -> None:
    state['updated_at'] = datetime.now().isoformat(timespec='seconds')
    _atomic_write(PROGRESS, json.dumps(state, ensure_ascii=False, indent=2))


def _log_failed(kind: str, code: str, error: str) -> None:
    new = not FAILED.is_file()
    FAILED.parent.mkdir(parents=True, exist_ok=True)
    with FAILED.open('a', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        if new:
            writer.writerow(['ts_code', 'asset_class', 'error', 'occurred_at'])
        writer.writerow([code, kind, error, datetime.now().isoformat(timespec='seconds')])


def fill(reset: bool = False, codes: list[str] | None = None) -> dict[str, Any]:
    if reset and PROGRESS.is_file():
        PROGRESS.unlink()
    if reset and FAILED.is_file():
        FAILED.unlink()
    state = _load_progress()
    gaps = _load_gap_codes()
    if codes:
        wanted = {c.strip().upper() for c in codes}
        gaps = gaps.filter(pl.col('ts_code').is_in(sorted(wanted)))
        state = {'started_at': datetime.now().isoformat(timespec='seconds'),
                 'completed': {}, 'rows_inserted': 0, 'batches_flushed': 0}
    tdx_map = _tdx_paths()
    factor_store = LocalFactorStore(STORE)
    store = LocalQuoteStore(STORE, factor_store=factor_store)
    day_tag = datetime.now().strftime('%Y%m%d')
    if not FAILED.is_file():
        FAILED.parent.mkdir(parents=True, exist_ok=True)
        with FAILED.open('w', encoding='utf-8', newline='') as f:
            csv.writer(f).writerow(['ts_code', 'asset_class', 'error', 'occurred_at'])
    pending = [r for r in gaps.iter_rows(named=True)
               if state['completed'].get(r['ts_code'], {}).get('status')
               not in ('ok', 'skipped')]
    print(f'待处理 {len(pending)} / {gaps.height} 只（已完成 {len(state["completed"])}）')

    processed = 0
    run_rows = 0
    for item in pending:
        kind, code = item['asset_class'], item['ts_code']
        path = _local_path(kind, code)
        tdx_path = tdx_map.get((kind, code))
        try:
            if not path.is_file() or tdx_path is None or not tdx_path.is_file():
                result = {'status': 'skipped', 'rows': 0,
                          'reason': 'file_missing'}
            else:
                local = pl.read_parquet(path).sort('date')
                result = _fill_one(kind, code, tdx_path, local, factor_store,
                                   store, day_tag)
                if result['status'] == 'failed':
                    _log_failed(kind, code, json.dumps(result, ensure_ascii=False))
        except Exception as exc:  # noqa: BLE001 - 单只失败不阻断整批
            result = {'status': 'failed', 'rows': 0, 'reason': repr(exc)}
            _log_failed(kind, code, repr(exc))
        state['completed'][code] = result
        state['rows_inserted'] += int(result.get('rows', 0))
        run_rows += int(result.get('rows', 0))
        processed += 1
        if processed % BATCH == 0:
            store.flush_meta()
            state['batches_flushed'] += 1
            _save_progress(state)
            done = sum(1 for v in state['completed'].values() if v['status'] == 'ok')
            failed = sum(1 for v in state['completed'].values() if v['status'] == 'failed')
            print(f'进度 {len(state["completed"])}/{gaps.height} 只'
                  f'（本轮补入 {run_rows} 行，ok={done}，failed={failed}）')
    store.flush_meta()
    _save_progress(state)
    ok = sum(1 for v in state['completed'].values() if v['status'] == 'ok')
    failed = sum(1 for v in state['completed'].values() if v['status'] == 'failed')
    skipped = sum(1 for v in state['completed'].values() if v['status'] == 'skipped')
    print(f'\n本轮写入 {run_rows} 行；ok={ok} failed={failed} skipped={skipped}')
    print(f'进度文件: {PROGRESS}')
    return state


def _backup_dir_for(kind: str, code: str) -> Path | None:
    market = code.split('.')[1].lower()
    if not BACKUP_ROOT.is_dir():
        return None
    for run_dir in sorted(BACKUP_ROOT.iterdir(), reverse=True):
        path = run_dir / 'quotes' / kind / market / f'{code}.parquet'
        if path.is_file():
            return path
    return None


def _date_set(code: str, tdx: pl.DataFrame, cur: pl.DataFrame) -> dict[str, Any]:
    local_end = cur['date'][-1]
    tdx_only = tdx.filter((pl.col('date') <= local_end) &
                          ~pl.col('date').is_in(cur['date'].to_list()))
    local_only = cur.filter(~pl.col('date').is_in(tdx['date'].to_list()))
    return {'tdx_only': tdx_only.height, 'local_only': local_only.height,
            'dup_local': cur.height - cur['date'].n_unique()}


def verify() -> dict[str, Any]:
    """验收：日期集合、逐值比对、既有行不变、is_tradable 重跑、跳变交叉校验。"""
    from src.data.tdx_day_parser import parse_day_file

    gaps = _original_gap_codes()
    inv = pl.read_csv(INVENTORY)
    eligible = set(inv.filter(pl.col('tdx_rows') > pl.col('local_rows'))['ts_code'].to_list())
    tdx_map = _tdx_paths()

    rows: list[dict[str, Any]] = []
    for item in gaps.iter_rows(named=True):
        kind, code = item['asset_class'], item['ts_code']
        path = _local_path(kind, code)
        cur = pl.read_parquet(path).sort('date')
        rec: dict[str, Any] = {'ts_code': code, 'asset_class': kind,
                              'local_rows': cur.height}
        bk = _backup_dir_for(kind, code)
        old = pl.read_parquet(bk).sort('date') if bk is not None else cur
        inserted = cur.filter(~pl.col('date').is_in(old['date'].to_list())) \
            if bk is not None else cur.clear()
        rec['inserted_rows'] = inserted.height
        rec['added_by_this_task'] = inserted.height if code in eligible else 0

        tdx_path = tdx_map.get((kind, code))
        tdx = parse_day_file(tdx_path).unique(subset=['date'], keep='last').sort('date') \
            if tdx_path and tdx_path.is_file() else None

        # 1) 日期集合
        if tdx is not None:
            ds = _date_set(code, tdx, cur)
            rec.update(ds)
            # 2) 补入行逐值 vs TDX
            j = inserted.join(tdx.select('date', 'open', 'high', 'low', 'close',
                                         'volume', 'amount'), on='date',
                              how='inner', suffix='_tdx')
            rec['inserted_matched'] = j.height
            diffs = {}
            for c in ('open', 'high', 'low', 'close', 'amount'):
                diffs[c] = j.filter(
                    (pl.col(c) - pl.col(f'{c}_tdx')).abs().gt(1e-9).fill_null(False)).height
            diffs['volume'] = j.filter(
                (pl.col('volume') - pl.col('volume_tdx')).abs().gt(VOLUME_TOL)
                .fill_null(False)).height
            rec['inserted_diff_close'] = diffs['close']
            rec['inserted_diff_amount'] = diffs['amount']
            rec['inserted_diff_volume'] = diffs['volume']
            rec['inserted_diff_ohlc'] = diffs['open'] + diffs['high'] + diffs['low']
            rec['inserted_zero_volume'] = inserted.filter(pl.col('volume') <= 0).height

            # 3) 既有行全列不变（对照备份）
            if bk is not None:
                chk = old.join(cur, on='date', how='inner', suffix='_cur')
                changed = 0
                for c in old.columns:
                    if c in ('date', 'ts_code'):
                        continue
                    changed = max(changed, chk.filter(
                        (pl.col(c) - pl.col(f'{c}_cur')).abs().gt(1e-12)
                        .fill_null(False)).height)
                rec['existing_rows_changed'] = changed
                rec['existing_rows_compared'] = chk.height
                rec['existing_hash_before'] = _existing_hash(old)
                rec['existing_hash_after'] = _existing_hash(
                    cur.filter(pl.col('date').is_in(old['date'].to_list())))
                rec['hash_ok'] = rec['existing_hash_before'] == rec['existing_hash_after']

            # 4) 补入行链式 pre_close 自洽
            ctx = cur.with_columns([
                pl.col('close').shift(1).alias('_pc'),
                pl.col('qfq_factor').shift(1).alias('_pq')])
            bad = ctx.filter(pl.col('date').is_in(inserted['date'].to_list())).filter(
                (pl.col('_pc') * (pl.col('_pq') / pl.col('qfq_factor')) -
                 pl.col('pre_close')).abs().gt(1e-9).fill_null(False))
            rec['chain_mismatch'] = bad.height

            # 5) 仍然缺口的行（is_tradable 口径）
            rec['still_missing'] = ds['tdx_only']
        else:
            rec['still_missing'] = -1
        rows.append(rec)

    table = pl.DataFrame(rows, infer_schema_length=None)
    print('\n=== 验收汇总 ===')
    print(table.select(
        pl.len().alias('codes'),
        pl.col('added_by_this_task').sum().alias('inserted_rows'),
        pl.col('still_missing').sum().alias('tdx_only_now'),
        pl.col('inserted_diff_close').sum().alias('ins_close_diff'),
        pl.col('inserted_diff_amount').sum().alias('ins_amount_diff'),
        pl.col('inserted_diff_volume').sum().alias('ins_volume_diff'),
        pl.col('inserted_diff_ohlc').sum().alias('ins_ohlc_diff'),
        pl.col('chain_mismatch').sum().alias('chain_mismatch'),
    ))
    print('既有行全列比对（全部 279 只，非抽样）:',
          f'{int(table["existing_rows_compared"].sum() if "existing_rows_compared" in table.columns else 0)} 行，'
          f'变化行 {int(table["existing_rows_changed"].sum() if "existing_rows_changed" in table.columns else 0)}')
    print('既有行内容哈希一致:',
          f'{int(table.filter(pl.col("hash_ok") == True).height) if "hash_ok" in table.columns else 0}'  # noqa: E712
          f'/{int(table["hash_ok"].is_not_null().sum()) if "hash_ok" in table.columns else 0}')
    left = table.filter(pl.col('still_missing') > 0).sort('still_missing', descending=True)
    print(f'仍有缺口(应为跳过标的): {left.height} 只 / {int(left["still_missing"].sum())} 行')
    print(left.select('ts_code', 'asset_class', 'still_missing', 'tdx_only', 'local_only').head(8))
    print('本地独有日期(未改动):',
          f'{int(table["local_only"].sum())} 行 / '
          f'{int(table.filter(pl.col("local_only") > 0).height)} 只')
    return {'table': table}


def _baseline_short_rows(di: pl.DataFrame, prep: dict[str, int],
                         snapshot: date) -> list[dict[str, Any]]:
    """按补数前的本地行数重建 ``tdx_truncated`` 基线（该类本轮不在范围内）。"""
    from src.data.quality import tradable_universe as tu

    local_rows = {r['ts_code']: int(r['rows']) for r in di.iter_rows(named=True)}
    local_end = {r['ts_code']: r['end'] for r in di.iter_rows(named=True)}
    local_codes = {(r['data_type'], r['ts_code']) for r in di.iter_rows(named=True)}
    out: list[dict[str, Any]] = []
    for asset_class, ts_code, path in discover_local_files(TDX, 'all'):
        if (asset_class, ts_code) not in local_codes:
            continue
        tdx = tu._read_tdx_days(path)
        before = prep.get(ts_code, local_rows.get(ts_code, 0))
        if tdx.size < before:
            out.append({
                'ts_code': ts_code, 'asset_class': asset_class,
                'reason_code': 'tdx_truncated',
                'reason_text': tu.REASON_TEXT['tdx_truncated'],
                'last_trade_date': local_end[ts_code],
                'expected_rows': int(before), 'actual_rows': int(tdx.size),
                'missing_rows': int(before - tdx.size),
                'severity': tu.REASON_SEVERITY['tdx_truncated'],
                'detected_at': snapshot,
            })
    return out


def refresh_exclusions() -> dict[str, Any]:
    """只刷新 internal_gap；tdx_truncated 按补数前口径保留（本轮不纳入范围）。"""
    from src.data.quality import tradable_universe as tu

    di = pl.read_parquet(STORE / 'meta' / 'daily_index.parquet')
    snapshot = max(di['end'].drop_nulls().to_list())
    gap, _short_now = tu._tdx_rows(di, STORE, TDX, snapshot)
    inv = pl.read_csv(INVENTORY)
    prep = {r['ts_code']: int(r['local_rows']) for r in inv.iter_rows(named=True)}
    short = _baseline_short_rows(di, prep, snapshot)
    print(f'基线 tdx_truncated 重建: {len(short)} 只（原清单 133 只）')

    old = pl.read_parquet(EXCLUSIONS)
    keep = old.filter(~pl.col('reason_code').is_in(['internal_gap', 'tdx_truncated']))
    fresh = pl.DataFrame(gap + short, schema=old.schema)
    frame = pl.concat([keep, fresh], how='vertical').sort(
        ['asset_class', 'reason_code', 'ts_code'])

    # pre_close<=0 行数：直接按当前文件实测（幂等，避免增量累加）
    summary_path = UNIVERSE / 'universe_exclusions.json'
    summary = json.loads(summary_path.read_text(encoding='utf-8'))
    zpc = dict.fromkeys(summary['price_jumps']['pre_close_zero_rows'], 0)
    for cls in zpc:
        folder = STORE / 'quotes' / cls
        for path in folder.rglob('*.parquet'):
            f = pl.read_parquet(path, columns=['date', 'pre_close'])
            if f.height >= 2:
                zpc[cls] += f.filter(pl.col('pre_close') <= 0).height
    summary['price_jumps']['pre_close_zero_rows'] = zpc

    sources = dict(summary['sources'])
    new_summary = tu._summarize(frame, di, snapshot, sources,
                                summary['price_jumps'])
    tu._write_atomic(EXCLUSIONS, lambda p: frame.write_parquet(p))
    tu._write_atomic(summary_path, lambda p: p.write_text(
        json.dumps(new_summary, ensure_ascii=False, indent=2, sort_keys=True),
        encoding='utf-8'))
    print('排除清单已刷新（未重算跳变明细）:')
    print('  internal_gap:', new_summary['reasons'].get('internal_gap'))
    print('  tdx_truncated:', new_summary['reasons'].get('tdx_truncated'))
    print('  bad_price_jump:', new_summary['reasons'].get('bad_price_jump'))
    print('  total_codes:', new_summary['total_codes'])
    print('  price_jumps total:', new_summary['price_jumps']['total_rows'])
    return {'frame': frame, 'summary': new_summary}


def verify_jumps() -> dict[str, Any]:
    """用既有跳变清单做交叉校验：补入行不得新增 data_error 类跳变。"""
    from src.data.quality import tradable_universe as tu

    detail = pl.read_parquet(JUMPS)
    calendar = tu._benchmark_calendar(STORE)
    dividends, pending = tu._load_dividend_events(STORE)
    wide: set[date] = set()
    counts: dict[tuple[date, int], int] = {}
    for item in detail.iter_rows(named=True):
        if item['asset_class'] != 'stock':
            continue
        key = (item['date'], 1 if item['pct_chg'] > 0 else -1)
        counts[key] = counts.get(key, 0) + 1
    for (when, _s), number in counts.items():
        if number >= 5:
            wide.add(when)

    gaps = _original_gap_codes()
    inserted_map: dict[str, list[date]] = {}
    for item in gaps.iter_rows(named=True):
        cur = pl.read_parquet(_local_path(item['asset_class'], item['ts_code']),
                              columns=['date']).sort('date')
        bk = _backup_dir_for(item['asset_class'], item['ts_code'])
        old_dates = pl.read_parquet(bk, columns=['date'])['date'].to_list() \
            if bk is not None else []
        inserted_map[item['ts_code']] = sorted(
            set(cur['date'].to_list()) - set(old_dates)) if bk is not None else []

    records: list[dict[str, Any]] = []
    for item in gaps.iter_rows(named=True):
        kind, code = item['asset_class'], item['ts_code']
        frame = pl.read_parquet(
            _local_path(kind, code),
            columns=['date', 'close', 'pre_close', 'qfq_factor', 'hfq_factor']).sort('date')
        if frame.height < 2:
            continue
        dates = frame['date'].to_list()
        close = frame['close'].to_numpy()
        pre_close = frame['pre_close'].to_numpy()
        qfq = frame['qfq_factor'].to_numpy()
        hfq = frame['hfq_factor'].to_numpy()
        valid = np.isfinite(pre_close) & (pre_close > 0)
        ratio = np.zeros_like(close)
        np.divide(close, pre_close, out=ratio, where=valid)
        ratio -= 1
        for i in np.flatnonzero(valid & (np.abs(ratio) > 0.5)):
            records.append(tu._jump_record(kind, code, dates, close, pre_close,
                                           qfq, hfq, int(i), float(ratio[i]),
                                           calendar, dividends, pending))

    for rec in records:
        rec['label'], rec['evidence'] = tu._classify_jump(rec, wide)
    now = pl.DataFrame(records) if records else tu._empty_jump_frame()

    ins = set()
    for code, dates in inserted_map.items():
        for d in dates:
            ins.add(f'{code}|{d}')
    ins_series = sorted(ins)

    def _keyed(frame: pl.DataFrame) -> pl.DataFrame:
        return frame.with_columns(
            pl.format('{}|{}', pl.col('ts_code'), pl.col('date').cast(pl.Utf8))
            .alias('_key'))

    at_inserted = _keyed(now).filter(pl.col('_key').is_in(ins_series)).drop('_key') \
        if ins else now.clear()
    print('\n=== 跳变交叉校验（基于既有清单，未重算落盘）===')
    print('既有清单:', detail.height, '行 /', detail['ts_code'].n_unique(), '只')
    print('补入行上的跳变记录:', at_inserted.height)
    if at_inserted.height:
        print(at_inserted.group_by('label').agg(pl.len().alias('rows')))
        print(at_inserted.filter(pl.col('label') != 'index_rebalance')
              .select('ts_code', 'date', 'pre_close', 'close', 'pct_chg', 'label'))
    pre = _keyed(now).filter(~pl.col('_key').is_in(ins_series)).drop('_key') \
        if ins else now
    old_here = detail.filter(pl.col('ts_code').is_in(
        gaps['ts_code'].to_list()))
    old_keys = {(r['ts_code'], r['date'], r['label'])
                for r in old_here.iter_rows(named=True)}
    new_keys = {(r['ts_code'], r['date'], r['label'])
                for r in pre.iter_rows(named=True)}
    print('既有日期跳变标签一致:',
          f'{len(old_keys & new_keys)}/{len(old_keys)}；'
          f'新出现 {len(new_keys - old_keys)}，消失 {len(old_keys - new_keys)}')
    if new_keys - old_keys or old_keys - new_keys:
        diff = pl.DataFrame(
            [(c, d, lab, 'new') for c, d, lab in sorted(new_keys - old_keys)] +
            [(c, d, lab, 'gone') for c, d, lab in sorted(old_keys - new_keys)],
            schema=['ts_code', 'date', 'label', 'kind'], orient='row')
        print(diff.group_by(['label', 'kind']).agg(pl.len().alias('rows')))
    return {'at_inserted': at_inserted, 'label_diff': new_keys - old_keys}


def main() -> int:
    parser = argparse.ArgumentParser(description='内部历史缺口补数工具')
    sub = parser.add_subparsers(dest='mode', required=True)
    sub.add_parser('inventory')
    p_fill = sub.add_parser('fill')
    p_fill.add_argument('--reset-progress', action='store_true')
    p_fill.add_argument('--codes', default=None, help='只处理指定标的（逗号分隔，用于试点）')
    sub.add_parser('verify')
    sub.add_parser('verify-jumps')
    sub.add_parser('refresh-exclusions')
    args = parser.parse_args()
    if args.mode == 'inventory':
        inventory()
    elif args.mode == 'fill':
        codes = args.codes.split(',') if args.codes else None
        fill(reset=args.reset_progress, codes=codes)
    elif args.mode == 'verify':
        verify()
    elif args.mode == 'verify-jumps':
        verify_jumps()
    elif args.mode == 'refresh-exclusions':
        refresh_exclusions()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
