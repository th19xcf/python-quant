"""分红日频增量刷新（基金年度批量 + 个股近两报告期），供日更非致命步调用。

口径与边界::

    - 基金：``ak.fund_fh_em(year)`` 年度批量（1 月首周补前一年），只取除息日落在
      ``[上次成功日 + 1, 目标日]`` 的事件；首次运行（无状态台账）回溯 ``--bootstrap-days``。
    - 个股：``ak.stock_fhps_em(date=报告期)`` 最近两个报告期，经
      ``tools.collect_stock_dividend.merge_frames`` 合并：只补新事件、回填除息日，
      同日金额冲突只记录不改写。
    - 只追加/补缺，不删除任何行；单次空返不判「无分红」，也不改写既有记录。
    - 写正式表按 DG-35：候选落 ``_staging/`` → 逐值对拍 → ``_backup/snapshots/``
      快照 → ``write_parquet_atomic`` 原子替换 → 受影响标的复权因子重建（含行情
      内嵌副本刷新）→ 写状态台账 ``meta/dividend_status/daily_refresh_state.json``。
    - 单源失败只告警、不写该源，另一源照常；进程退出码 0/1 表示本步整体成败，
      由日更入口按非致命步处理。

用法::

    python tools/refresh_dividend_daily.py --date 2026-11-18 --dry-run
    python tools/refresh_dividend_daily.py --date 2026-11-18
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.atomic_write import write_parquet_atomic, write_text_atomic  # noqa: E402
from src.data.dataset_contracts import write_dataset  # noqa: E402
from src.data.local_store.fund_factors import (  # noqa: E402
    DIVIDEND_COLUMNS,
    FUND_DIVIDEND_FILENAME,
    FUND_SHARE_CHANGE_FILENAME,
    fetch_dividend_year,
    rebuild_fund_factors,
)
from src.data.local_store.store import LocalFactorStore, LocalQuoteStore  # noqa: E402
from src.data.staging import staging_dir  # noqa: E402
from tools.collect_stock_dividend import (  # noqa: E402
    FINAL_SCHEMA,
    merge_frames,
    normalize_period,
)

STORE = ROOT / 'data' / 'local_store'
RULE = 'daily-increment-window-v1'
#: 首次运行（无状态台账）回溯天数：覆盖「上次手工刷新」到今天的空档。
DEFAULT_BOOTSTRAP_DAYS = 30


# ----------------------------------------------------------------------
# 日期与状态
# ----------------------------------------------------------------------
def _quarter_periods(target: date) -> list[str]:
    """目标日之前（含）最近的两个季末报告期，格式 ``YYYYMMDD``。"""
    ends: list[date] = []
    for year in (target.year, target.year - 1):
        for month, day in ((12, 31), (9, 30), (6, 30), (3, 31)):
            value = date(year, month, day)
            if value <= target:
                ends.append(value)
    return [item.strftime('%Y%m%d') for item in sorted(ends, reverse=True)[:2]]


def _state_path() -> Path:
    return STORE / 'meta' / 'dividend_status' / 'daily_refresh_state.json'


def _load_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _window_start(target: date, state: dict[str, Any], source: str,
                  bootstrap_days: int) -> date:
    """增量窗口起点：上次成功日的次日；无状态时回溯 ``bootstrap_days``。"""
    last = (state.get(source) or {}).get('last_success')
    if last:
        try:
            # 同日重跑时不把窗口写成「起点晚于终点」，回落到当天（幂等重查）
            return min(date.fromisoformat(str(last)[:10]) + timedelta(days=1), target)
        except ValueError:
            pass
    return target - timedelta(days=max(1, bootstrap_days) - 1)


# ----------------------------------------------------------------------
# 基金分红
# ----------------------------------------------------------------------
def _fund_universe() -> set[str]:
    """本地有行情/因子的基金代码（``fund_covered_codes.txt``，缺失时退回因子表）。"""
    path = STORE / 'factors' / 'fund_covered_codes.txt'
    if path.is_file():
        return {line.strip() for line in path.read_text(encoding='utf-8').splitlines()
                if line.strip()}
    factor_path = STORE / 'factors' / 'fund_adj_factor.parquet'
    if not factor_path.is_file():
        return set()
    return set(pl.read_parquet(factor_path, columns=['ts_code'])['ts_code'].unique().to_list())


def refresh_fund(target: date, start: date, *, dry_run: bool) -> dict[str, Any]:
    """基金分红增量：年度批量 → 只追加缺失 ``(ts_code, ex_date)`` → 重建因子。"""
    started = time.perf_counter()
    years = [target.year - 1, target.year] if (target.month == 1 and target.day <= 7) \
        else [target.year]
    frames: list[pl.DataFrame] = []
    for year in years:
        frame, error = fetch_dividend_year(year)
        if frame is None:
            return {'ok': False, 'degraded': True, 'error': f'{year} 年度抓取失败:{error}',
                    'elapsed_s': round(time.perf_counter() - started, 2)}
        frames.append(frame)
    fetched = pl.concat(frames, how='vertical_relaxed') if len(frames) > 1 else frames[0]
    universe = _fund_universe()
    in_window = fetched.filter(pl.col('ex_date') >= start, pl.col('ex_date') <= target)
    new = in_window.filter(pl.col('ts_code').is_in(universe)) if universe else in_window

    table = STORE / 'factors' / FUND_DIVIDEND_FILENAME
    old = pl.read_parquet(table).select(DIVIDEND_COLUMNS)
    key = ['ts_code', 'ex_date']
    conflicts = new.join(
        old.select(['ts_code', 'ex_date', pl.col('cash_amount').alias('old_cash')]),
        on=key, how='inner').filter(
        (pl.col('cash_amount') - pl.col('old_cash')).abs() > 1e-9)
    add = new.join(old.select(key), on=key, how='anti').sort(key)
    stats: dict[str, Any] = {
        'ok': True, 'window': f'{start}~{target}',
        'fetched_rows': fetched.height, 'window_rows': in_window.height,
        'out_of_universe_rows': in_window.height - new.height,
        'universe_codes': len(universe),
        'added_rows': add.height, 'added_codes': add['ts_code'].n_unique(),
        'amount_conflicts': conflicts.height,
        'skipped_conflict_sample': sorted({
            f'{rec["ts_code"]}|{rec["ex_date"]}' for rec in conflicts.head(5).to_dicts()}),
    }
    if add.height == 0 or dry_run:
        stats['written'] = False
        stats['elapsed_s'] = round(time.perf_counter() - started, 2)
        return stats

    candidate = pl.concat([old, add], how='vertical_relaxed').select(DIVIDEND_COLUMNS)
    stage = staging_dir('dividend_daily', with_date=True)
    stage.mkdir(parents=True, exist_ok=True)
    stage_path = stage / 'fund_dividend_candidate.parquet'
    # 候选暂存文件，非数据集
    write_parquet_atomic(candidate, stage_path, compression='zstd', statistics=True)

    back = pl.read_parquet(stage_path)
    assert back.height == old.height + add.height, '基金分红候选行数不守恒'
    back_new = back.join(add.select(key), on=key, how='semi').sort(key)
    back_old = back.join(add.select(key), on=key, how='anti').sort(key)
    assert back_new.equals(add.sort(key)), '基金分红新增行逐值对拍不一致'
    assert back_old.equals(old.sort(key)), '基金分红既有行被改动'

    _snapshot(table, 'fund_dividend', old)
    write_dataset('fund_dividend', candidate,
                  rel_path=table.relative_to(STORE).as_posix(), root=STORE,
                  writer='tools/refresh_dividend_daily.py')
    assert pl.read_parquet(table).height == candidate.height, '基金分红替换后行数不一致'

    codes = sorted(add['ts_code'].unique().to_list())
    splits = pl.read_parquet(STORE / 'factors' / FUND_SHARE_CHANGE_FILENAME)
    factor_stats = rebuild_fund_factors(STORE, candidate, splits, codes=codes)
    stats.update({
        'written': True, 'stage': str(stage_path),
        'affected_codes': codes,
        'factor_rows': int(factor_stats.get('factor_rows') or 0),
        'factor_codes': int(factor_stats.get('factor_codes') or 0),
        'quote_factor_refresh': factor_stats.get('quote_factor_refresh'),
        'mismatch_codes': _quote_factor_mismatch('fund', codes),
        'elapsed_s': round(time.perf_counter() - started, 2),
    })
    return stats


# ----------------------------------------------------------------------
# 个股分红
# ----------------------------------------------------------------------
def _fetch_stock_period(period: str, start: date,
                        retries: int = 3) -> tuple[pl.DataFrame | None, str]:
    import akshare as ak  # noqa: PLC0415

    last = ''
    for attempt in range(1, retries + 1):
        try:
            raw = ak.stock_fhps_em(date=period)
            if raw is None or len(raw) == 0:
                return pl.DataFrame(schema=FINAL_SCHEMA), ''
            return normalize_period(raw, period, start), ''
        except Exception as exc:  # noqa: BLE001 - 东财接口异常类型不稳定
            last = f'{type(exc).__name__}: {exc}'
            time.sleep(1.0 * attempt)
    return None, last


def _content_changed(old: pl.DataFrame, final: pl.DataFrame) -> bool:
    """除行序外内容是否变化（``merge_frames`` 会重排并回填同名值，需去噪）。"""
    if old.height != final.height:
        return True
    cols = [name for name in old.columns if name in final.columns]
    if len(cols) != len(old.columns):
        return True
    return not old.select(cols).sort(cols).equals(final.select(cols).sort(cols))


def _amount_fingerprint(frame: pl.DataFrame) -> Counter:
    return Counter(
        (rec['ts_code'], rec.get('dividend_year'), round(float(rec.get('cash_div') or 0.0), 6),
         round(float(rec.get('share_div') or 0.0), 6))
        for rec in frame.to_dicts())


def refresh_stock(target: date, start: date, *, dry_run: bool) -> dict[str, Any]:
    """个股分红增量：最近两报告期 → 合并（只补新事件/回填除息日）→ 重建因子。"""
    started = time.perf_counter()
    periods = _quarter_periods(target)
    frames: list[pl.DataFrame] = []
    errors: list[str] = []
    for period in periods:
        frame, error = _fetch_stock_period(period, start)
        if frame is None:
            errors.append(f'{period}:{error}')
        else:
            frames.append(frame)
    if not frames:
        return {'ok': False, 'degraded': True,
                'error': '; '.join(errors) or '两个报告期均无返回',
                'elapsed_s': round(time.perf_counter() - started, 2)}

    fetched = pl.concat(frames, how='vertical_relaxed')
    table = STORE / 'factors' / 'stock_dividend.parquet'
    old = pl.read_parquet(table)
    final, merge_stats, conflicts = merge_frames(old, fetched)
    appended = int(merge_stats['appended_same_day']) + int(merge_stats['appended_plan'])
    stats: dict[str, Any] = {
        'ok': True, 'window': f'{start}~{target}', 'periods': periods,
        'fetched_rows': fetched.height, 'appended_rows': appended,
        'enriched_rows': int(merge_stats.get('enriched_rows') or 0),
        'amount_conflicts': len(conflicts),
        'fetch_errors': errors,
        'elapsed_s': round(time.perf_counter() - started, 2),
    }
    changed = _content_changed(old, final)
    if appended == 0 and not changed:
        stats['written'] = False
        return stats

    # 逐值对拍：行数守恒 + 既有金额指纹必须是最终表的子多重集 + 既有除息日不丢
    assert final.height == old.height + appended, '个股分红候选行数不守恒'
    old_fp, new_fp = _amount_fingerprint(old), _amount_fingerprint(final)
    missing = {key: value - new_fp.get(key, 0) for key, value in old_fp.items()
               if new_fp.get(key, 0) < value}
    assert not missing, f'个股分红既有金额被改动 {len(missing)} 组'
    old_events = {(rec['ts_code'], rec['ex_date'])
                  for rec in old.filter(pl.col('ex_date').is_not_null()).to_dicts()}
    new_events = {(rec['ts_code'], rec['ex_date'])
                  for rec in final.filter(pl.col('ex_date').is_not_null()).to_dicts()}
    assert old_events <= new_events, '个股分红既有除息事件丢失'

    if dry_run:
        stats['written'] = False
        return stats

    stage = staging_dir('dividend_daily', with_date=True)
    stage.mkdir(parents=True, exist_ok=True)
    stage_path = stage / 'stock_dividend_candidate.parquet'
    # 候选暂存文件，非数据集
    write_parquet_atomic(final, stage_path, compression='zstd', statistics=True)
    assert pl.read_parquet(stage_path).height == final.height, '个股分红候选回读行数不一致'

    _snapshot(table, 'stock_dividend', old)
    write_dataset('stock_dividend', final,
                  rel_path=table.relative_to(STORE).as_posix(), root=STORE,
                  writer='tools/refresh_dividend_daily.py')
    assert pl.read_parquet(table).height == final.height, '个股分红替换后行数不一致'

    codes = sorted({code for code, _ in set(new_events) - set(old_events)})
    if codes:
        from src.data.local_store.factor_rebuild import (  # noqa: PLC0415
            rebuild_factors_from_local,
        )

        rebuild = rebuild_factors_from_local(
            LocalQuoteStore(STORE), LocalFactorStore(STORE),
            codes=codes, include_existing=True, resume=False)
    else:
        # 仅公告字段/进度被回填时不动因子，但仍要让 factors manifest 跟进事件表
        from src.data.factors_manifest import write_factors_manifest  # noqa: PLC0415

        write_factors_manifest(STORE)
        rebuild = {}
    stats.update({
        'written': True, 'stage': str(stage_path), 'affected_codes': codes,
        'factor_rebuild': {k: v for k, v in rebuild.items() if k != 'quote_factor_refresh'},
        'quote_factor_refresh': rebuild.get('quote_factor_refresh'),
        'mismatch_codes': _quote_factor_mismatch('stock', codes) if codes else 0,
        'elapsed_s': round(time.perf_counter() - started, 2),
    })
    return stats


# ----------------------------------------------------------------------
# 治理与校验
# ----------------------------------------------------------------------
def _snapshot(table: Path, label: str, old: pl.DataFrame) -> Path:
    """改前快照（DG-11）：进仓库根 ``_backup/snapshots/``，不动正式目录。"""
    snap = ROOT / '_backup' / 'snapshots' / f'{label}_{date.today():%Y-%m-%d}'
    snap.mkdir(parents=True, exist_ok=True)
    shutil.copy2(table, snap / f'{table.stem}.bak.parquet')
    old.write_csv(snap / f'{table.stem}_before.csv')
    return snap


def _quote_factor_mismatch(kind: str, codes: list[str]) -> int:
    """受影响标的的行情内嵌因子副本 vs 因子快照，返回不一致只数。"""
    name = 'fund_adj_factor.parquet' if kind == 'fund' else 'stock_adj_factor.parquet'
    factor_path = STORE / 'factors' / name
    if not factor_path.is_file():
        return len(codes)
    factors = pl.read_parquet(
        factor_path, columns=['ts_code', 'trade_date', 'qfq_factor', 'hfq_factor'])
    quote_store = LocalQuoteStore(STORE)
    bad = 0
    for code in codes:
        path = quote_store.quote_path(kind, code)
        if not path.is_file():
            bad += 1
            continue
        quote = pl.read_parquet(
            path, columns=['date', 'qfq_factor', 'hfq_factor']).rename(
            {'date': 'trade_date'})
        joined = quote.join(
            factors.filter(pl.col('ts_code') == code), on='trade_date',
            how='inner', suffix='_snap')
        if joined.height == 0:
            bad += 1
            continue
        diff = joined.filter(
            ((pl.col('qfq_factor') - pl.col('qfq_factor_snap')).abs() > 1e-9)
            | ((pl.col('hfq_factor') - pl.col('hfq_factor_snap')).abs() > 1e-9))
        bad += 1 if diff.height else 0
    return bad


def _write_state(state: dict[str, Any]) -> None:
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(json.dumps(state, ensure_ascii=False, indent=2) + '\n', path)


def _latest_local_date() -> date | None:
    path = STORE / 'meta' / 'daily_index.parquet'
    if not path.is_file():
        return None
    ends = pl.read_parquet(path, columns=['end'])['end']
    values = [item for item in ends.to_list() if item is not None]
    return max(values) if values else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='分红日频增量刷新（基金 + 个股）')
    parser.add_argument('--date', default=None, help='目标日（默认取本地 daily_index 末日）')
    parser.add_argument('--store-root', default=None, help='local_store 根（默认仓库内路径）')
    parser.add_argument('--bootstrap-days', type=int, default=DEFAULT_BOOTSTRAP_DAYS,
                        help='无状态台账时回溯天数（默认 30）')
    parser.add_argument('--skip-fund', action='store_true')
    parser.add_argument('--skip-stock', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args(argv)

    global STORE
    if args.store_root:
        STORE = Path(args.store_root)
    target = date.fromisoformat(args.date) if args.date else _latest_local_date()
    if target is None:
        raise SystemExit('无法确定目标日：请给 --date 或先构建 local_store')

    state = _load_state(_state_path())
    result: dict[str, Any] = {'target': target.isoformat(), 'rule': RULE}
    failed: list[str] = []

    if args.skip_fund:
        result['fund'] = {'ok': True, 'skipped': True}
    else:
        start = _window_start(target, state, 'fund', args.bootstrap_days)
        try:
            result['fund'] = refresh_fund(target, start, dry_run=args.dry_run)
        except Exception as exc:  # noqa: BLE001 - 单源失败不得中断另一源
            result['fund'] = {'ok': False, 'degraded': True,
                              'error': f'{type(exc).__name__}: {exc}'}
        if not result['fund'].get('ok'):
            failed.append('fund')

    if args.skip_stock:
        result['stock'] = {'ok': True, 'skipped': True}
    else:
        start = _window_start(target, state, 'stock', args.bootstrap_days)
        try:
            result['stock'] = refresh_stock(target, start, dry_run=args.dry_run)
        except Exception as exc:  # noqa: BLE001
            result['stock'] = {'ok': False, 'degraded': True,
                              'error': f'{type(exc).__name__}: {exc}'}
        if not result['stock'].get('ok'):
            failed.append('stock')

    for source in ('fund', 'stock'):
        item = result[source]
        if item.get('ok') and not args.dry_run:
            state[source] = {
                'last_success': target.isoformat(),
                'window': item.get('window'),
                'added_rows': item.get('added_rows', item.get('appended_rows', 0)),
                'affected_codes': item.get('affected_codes') or [],
                'updated_at': datetime.now().isoformat(timespec='seconds'),
            }
        elif not item.get('ok'):
            state[source] = {**(state.get(source) or {}),
                             'degraded_at': datetime.now().isoformat(timespec='seconds'),
                             'error': item.get('error', '')}

    result.update({
        'rule': RULE, 'updated_at': datetime.now().isoformat(timespec='seconds'),
        'failed_sources': failed,
    })
    if not args.dry_run:
        state.update({'rule': RULE,
                      'last_run': datetime.now().isoformat(timespec='seconds'),
                      'last_target': target.isoformat(),
                      'failed_sources': failed,
                      # 本次运行真实结果（成功/失败都写），供日更入口汇报本轮而非上一次
                      'last_result': {
                          'target': target.isoformat(),
                          'finished_at': datetime.now().isoformat(timespec='seconds'),
                          'failed_sources': failed,
                          'sources': {
                              source: {
                                  key: result[source].get(key)
                                  for key in ('ok', 'skipped', 'added_rows', 'appended_rows',
                                              'affected_codes', 'error', 'degraded')
                                  if key in result[source]
                              }
                              for source in ('fund', 'stock')
                          },
                      }})
        _write_state(state)
    result['state_path'] = str(_state_path())
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0 if not failed else 1


if __name__ == '__main__':
    raise SystemExit(main())
