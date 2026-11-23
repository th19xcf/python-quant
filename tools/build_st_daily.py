#!/usr/bin/env python3

"""重建 A 股逐日 ST/*ST 状态面板（2005 年起）。

数据源与推导口径
================
1. 主源 ``meta/name_history.parquet``（tushare ``namechange``，逐段带
   ``start_date`` 生效日 / ``end_date`` 失效日 / ``ann_date`` 公告日）。
   按证券简称前缀判定：含 ``*ST`` → ``*st``；含 ``ST`` → ``st``；
   ``PT`` 开头或 ``*`` 开头（如 ``*金亚``）→ ``st``（其他风险警示）。
   切换一律用 **start_date（生效日）**，``ann_date`` 只留证，不参与判定，
   因此不存在「公告日未到就提前戴帽」的未来信息。
2. 兜底 ``meta/st/st_intervals.parquet``：tushare 未覆盖到的区间（交易所
   简称变更表、巨潮公告定日、人工裁定）按 ``flag`` 补 ST。
3. 非 ST 只有两种来源：命中名称段且该段非风险警示；或落在
   ``meta/st/st_coverage.parquet`` 的可信窗口内且无 ST 区间。其余一律
   ``unknown``，不默认非 ST。

落盘
====
``data/local_store/meta/st/daily/st_daily_YYYYMM.parquet``（自然月分区，
列：trade_date / ts_code / status / source），外加：

- ``st_daily_intervals.parquet``：逐段证据表，带 ``ann_date`` 便于审计；
- ``manifest.json``：逐文件 rows / bytes / sha256 / 起止日 + 总行数 +
  「生效日晚于公告日」的滞后统计；
- ``comparison.json``：与 ``st_intervals.parquet`` 的对拍（覆盖率、双向
  冲突条数与样例）。

幂等：输出全部由输入纯函数决定，同输入重跑逐字节一致；写盘一律走
:func:`write_parquet_atomic`（临时文件 + fsync + os.replace）。

用法::

    python tools/build_st_daily.py              # 全量重建
    python tools/build_st_daily.py --since 2015-01-01
    python tools/build_st_daily.py --since 2026-11-01 --incremental  # 日更增量

``--incremental`` 只重建 ``--since`` 起的月分区并保留其余分区（不删除），
manifest 的 ``files`` 与 ``rows`` 按「历史登记 + 本次重建」合并成全表口径，
另记 ``mode`` / ``window`` / ``rebuilt`` 说明本次只动了哪些月。
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.atomic_write import write_parquet_atomic, write_text_atomic  # noqa: E402
from src.data.dataset_contracts import write_dataset  # noqa: E402
from src.data.trading_calendar import trading_dates  # noqa: E402
from src.data.dataset_contracts import write_dataset_at

STORE = ROOT / 'data' / 'local_store'
OUT_DIR = STORE / 'meta' / 'st' / 'daily'
NAME_HISTORY = STORE / 'meta' / 'name_history.parquet'
ST_INTERVALS = STORE / 'meta' / 'st' / 'st_intervals.parquet'
ST_COVERAGE = STORE / 'meta' / 'st' / 'st_coverage.parquet'
DAILY_INDEX = STORE / 'meta' / 'daily_index.parquet'

SOURCE_NAME = 'tushare_namechange'
DEFAULT_START = date(2005, 1, 1)

PANEL_SCHEMA: dict[str, Any] = {
    'trade_date': pl.Date, 'ts_code': pl.Utf8,
    'status': pl.Utf8, 'source': pl.Utf8,
}
EVIDENCE_SCHEMA: dict[str, Any] = {
    'ts_code': pl.Utf8, 'name': pl.Utf8, 'start_date': pl.Date,
    'end_date': pl.Date, 'ann_date': pl.Date, 'flag': pl.Utf8,
    'source': pl.Utf8,
}


def classify_name(name: Any) -> str | None:
    """按简称判定风险警示类型；非风险警示返回 ``None``（与既有工具同口径）。"""
    if name is None:
        return None
    text = str(name).strip().upper()
    if not text:
        return None
    body = text[1:] if text.startswith('G') else text  # 股改标记 G
    if 'ST' in body:
        return '*st' if '*ST' in body else 'st'
    if body.startswith('PT'):
        return 'st'
    if text.startswith('*'):
        return 'st'
    return None


def _close_open_ends(frame: pl.DataFrame) -> pl.DataFrame:
    """同一标的后一段存在时，把前一段的空 end 收窄到后一段起点前一日。"""
    rows: list[dict[str, Any]] = []
    for code, group in frame.group_by('ts_code', maintain_order=True):
        items = sorted(group.to_dicts(), key=lambda r: (r['start_date'], r['name'] or ''))
        for index, item in enumerate(items):
            end = item['end_date']
            if end is None and index + 1 < len(items):
                end = items[index + 1]['start_date'] - timedelta(days=1)
            rows.append({**item, 'end_date': end})
    if not rows:
        return frame.clear()
    return pl.DataFrame(rows, schema=frame.schema, strict=False)


def _name_evidence(nh: pl.DataFrame, as_of: date) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in nh.iter_rows(named=True):
        start = item['start_date']
        if start is None:
            continue
        rows.append({
            'ts_code': item['ts_code'], 'name': item['name'],
            'start_date': start, 'end_date': item['end_date'],
            'ann_date': item['ann_date'],
            'flag': classify_name(item['name']),
            'source': SOURCE_NAME,
        })
    if not rows:
        return pl.DataFrame(schema=EVIDENCE_SCHEMA)
    frame = pl.DataFrame(rows, schema=EVIDENCE_SCHEMA, strict=False)
    return _close_open_ends(frame).sort(['ts_code', 'start_date', 'name'])


def _official_evidence(intervals: pl.DataFrame | None) -> pl.DataFrame:
    if intervals is None or intervals.height == 0:
        return pl.DataFrame(schema=EVIDENCE_SCHEMA)
    frame = intervals.select([
        pl.col('ts_code'),
        pl.lit(None, dtype=pl.Utf8).alias('name'),
        pl.col('start_date'),
        pl.col('end_date'),
        pl.lit(None, dtype=pl.Date).alias('ann_date'),
        pl.when(pl.col('flag') == '*st').then(pl.lit('*st'))
        .otherwise(pl.lit('st')).alias('flag'),
        pl.col('source'),
    ])
    return frame.sort(['ts_code', 'start_date'])


def _month_spans(start: date, end: date) -> Iterable[tuple[date, date]]:
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        nxt = date(cursor.year + (cursor.month == 12),
                   1 if cursor.month == 12 else cursor.month + 1, 1)
        yield max(start, cursor), min(end, nxt - timedelta(days=1))
        cursor = nxt


def _hits(pairs: pl.DataFrame, evidence: pl.DataFrame) -> pl.DataFrame:
    joined = (pairs.join(evidence, on='ts_code', how='inner')
              .filter((pl.col('start_date') <= pl.col('trade_date'))
                      & (pl.col('end_date').is_null()
                         | (pl.col('end_date') >= pl.col('trade_date')))))
    if joined.height == 0:
        return pl.DataFrame(schema={'ts_code': pl.Utf8, 'trade_date': pl.Date,
                                    'flag': pl.Utf8, 'source': pl.Utf8})
    # 升序下 '*st' 排在 'st' 前，取 first 即让 *ST 优先
    return (joined.sort(['ts_code', 'trade_date', 'flag'])
            .group_by(['ts_code', 'trade_date'], maintain_order=True)
            .agg(pl.col('flag').first(), pl.col('source').first()))


def _covered(pairs: pl.DataFrame, evidence: pl.DataFrame,
             start_col: str, end_col: str) -> pl.DataFrame:
    joined = pairs.join(evidence.select(['ts_code', start_col, end_col]),
                        on='ts_code', how='inner')
    joined = joined.filter((pl.col(start_col) <= pl.col('trade_date'))
                           & (pl.col(end_col).is_null()
                              | (pl.col(end_col) >= pl.col('trade_date'))))
    return joined.select(['ts_code', 'trade_date']).unique()


def _status_frame(pairs: pl.DataFrame, name_ev: pl.DataFrame,
                  off_ev: pl.DataFrame) -> pl.DataFrame:
    name_st = name_ev.filter(pl.col('flag').is_not_null())
    off_st = off_ev.filter(pl.col('flag').is_not_null())
    name_hit = _hits(pairs, name_st).rename(
        {'flag': 'nh_flag', 'source': 'nh_source'})
    off_hit = _hits(pairs, off_st).rename(
        {'flag': 'off_flag', 'source': 'off_source'})
    name_any = _covered(pairs, name_ev, 'start_date', 'end_date').with_columns(
        pl.lit(True).alias('nh_seg'))
    coverage = pl.read_parquet(ST_COVERAGE) if ST_COVERAGE.is_file() else None
    off_cov = (_covered(pairs, coverage, 'coverage_start', 'coverage_end')
               .with_columns(pl.lit(True).alias('off_cov'))
               if coverage is not None else
               pl.DataFrame(schema={'ts_code': pl.Utf8, 'trade_date': pl.Date,
                                    'off_cov': pl.Boolean}))

    out = (pairs.join(name_hit, on=['ts_code', 'trade_date'], how='left')
           .join(off_hit, on=['ts_code', 'trade_date'], how='left')
           .join(name_any, on=['ts_code', 'trade_date'], how='left')
           .join(off_cov, on=['ts_code', 'trade_date'], how='left'))
    status = (pl.when(pl.col('nh_flag').is_not_null()).then(pl.col('nh_flag'))
              .when(pl.col('off_flag').is_not_null()).then(pl.col('off_flag'))
              .when(pl.col('nh_seg').fill_null(False)).then(pl.lit('non_st'))
              .when(pl.col('off_cov').fill_null(False)).then(pl.lit('non_st'))
              .otherwise(pl.lit('unknown')))
    source = (pl.when(pl.col('nh_flag').is_not_null()).then(pl.col('nh_source'))
              .when(pl.col('off_flag').is_not_null()).then(pl.col('off_source'))
              .when(pl.col('nh_seg').fill_null(False)).then(pl.lit(SOURCE_NAME))
              .when(pl.col('off_cov').fill_null(False)).then(pl.lit('st_coverage'))
              .otherwise(pl.lit('')))
    return out.with_columns(status.alias('status'),
                            source.alias('source')).select(list(PANEL_SCHEMA))


def _universe() -> pl.DataFrame:
    index = pl.read_parquet(DAILY_INDEX).filter(pl.col('data_type') == 'stock')
    return (index.select(['ts_code', pl.col('start'), pl.col('end')])
            .filter(pl.col('start').is_not_null()))


def _session_days(start: date, end: date) -> list[date]:
    """真实交易日取 breadth 日频（全历史、只含开市日），缺失时退回日历模块。"""
    breadth = STORE / 'meta' / 'breadth' / 'breadth_daily.parquet'
    if breadth.is_file():
        dates = pl.read_parquet(breadth, columns=['trade_date'])['trade_date']
        days = sorted({day for day in dates.to_list() if start <= day <= end})
        if days:
            return days
    return list(trading_dates(start, end, ROOT))


def _lag_stats(ev: pl.DataFrame) -> dict[str, Any]:
    dated = ev.filter(pl.col('ann_date').is_not_null()
                      & (pl.col('source') == SOURCE_NAME))
    if dated.height == 0:
        return {'rows_with_ann_date': 0}
    lag = (pl.col('start_date') - pl.col('ann_date')).dt.total_days()
    frame = dated.with_columns(lag.alias('lag_days'))
    return {
        'rows_with_ann_date': frame.height,
        'effective_after_announcement': int((frame['lag_days'] > 0).sum()),
        'effective_same_day': int((frame['lag_days'] == 0).sum()),
        'effective_before_announcement': int((frame['lag_days'] < 0).sum()),
        'lag_days_median': float(frame['lag_days'].median()),
        'lag_days_p95': float(frame['lag_days'].quantile(0.95)),
        'lag_days_max': int(frame['lag_days'].max()),
        'note': '面板切换用 start_date（生效日）；ann_date 仅留证，不参与判定',
    }


def _comparison(out_dir: Path, intervals: pl.DataFrame | None) -> dict[str, Any]:
    panel = pl.scan_parquet(str(out_dir / 'st_daily_[0-9][0-9][0-9][0-9][0-9][0-9].parquet'))
    total = panel.select(pl.len()).collect().item()
    known = panel.filter(pl.col('status') != 'unknown').select(pl.len()).collect().item()
    result: dict[str, Any] = {
        'panel_rows': total,
        'known_rows': known,
        'known_ratio': round(known / total, 6) if total else 0.0,
        'by_status': dict(panel.group_by('status').agg(pl.len())
                          .collect().iter_rows()),
    }
    if intervals is None or intervals.height == 0:
        result['intervals'] = None
        return result
    st = intervals.select(['ts_code', 'start_date', 'end_date', 'source']).lazy()
    matched = (panel.join(st, on='ts_code', how='inner')
               .filter((pl.col('start_date') <= pl.col('trade_date'))
                       & (pl.col('end_date').is_null()
                          | (pl.col('end_date') >= pl.col('trade_date')))))
    missed = matched.filter(pl.col('status') == 'non_st')
    panel_st = panel.filter(pl.col('status').is_in(['st', '*st']))
    interval_days = matched.select(['ts_code', 'trade_date']).unique()
    extra = panel_st.join(interval_days, on=['ts_code', 'trade_date'],
                          how='anti')
    result['intervals'] = {
        'interval_stock_days': interval_days.select(pl.len()).collect().item(),
        'panel_stock_days_status_st': panel_st.select(pl.len()).collect().item(),
        'conflict_interval_st_panel_non_st':
            missed.select(pl.len()).collect().item(),
        'conflict_panel_st_interval_absent':
            extra.select(pl.len()).collect().item(),
        'conflict_interval_sample': missed.sort(['ts_code', 'trade_date'])
        .head(10).collect().to_dicts(),
        'conflict_panel_sample': extra.sort(['ts_code', 'trade_date'])
        .head(10).collect().to_dicts(),
    }
    return result


def _merge_files(prev: dict[str, Any], rebuilt: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """增量重建时把「历史 manifest 登记 + 本次重建」合并成全表文件清单。"""
    merged = {item['path']: item for item in (prev.get('files') or [])
              if isinstance(item, dict) and 'path' in item}
    merged.update({item['path']: item for item in rebuilt})
    return [merged[name] for name in sorted(merged)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='重建逐日 ST 状态面板')
    parser.add_argument('--since', default=DEFAULT_START.isoformat())
    parser.add_argument('--incremental', action='store_true',
                        help='增量：保留 --since 之前的月分区，manifest 合并成全表口径')
    args = parser.parse_args(argv)

    nh = pl.read_parquet(NAME_HISTORY)
    intervals = pl.read_parquet(ST_INTERVALS) if ST_INTERVALS.is_file() else None
    universe = _universe()
    as_of = universe['end'].max()
    start = max(date.fromisoformat(args.since), universe['start'].min())
    universe = universe.filter(pl.col('end') >= start)

    name_ev = _name_evidence(nh, as_of)
    off_ev = _official_evidence(intervals)
    days = _session_days(start, as_of)
    print(f'区间 {start} ~ {as_of}，交易日 {len(days)}，标的 {universe.height}',
          flush=True)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    files: list[dict[str, Any]] = []
    for month_start, month_end in _month_spans(start, as_of):
        month_days = [day for day in days if month_start <= day <= month_end]
        if not month_days:
            continue
        codes = universe.filter((pl.col('start') <= month_end)
                                & (pl.col('end') >= month_start))
        day_frame = pl.DataFrame({'trade_date': month_days},
                                 schema={'trade_date': pl.Date})
        pairs = (codes.select(['ts_code', 'start', 'end'])
                 .join(day_frame, how='cross')
                 .filter((pl.col('start') <= pl.col('trade_date'))
                         & (pl.col('end') >= pl.col('trade_date')))
                 .select(['ts_code', 'trade_date']))
        panel = _status_frame(pairs, name_ev, off_ev).sort(['trade_date', 'ts_code'])
        path = OUT_DIR / f'st_daily_{month_start:%Y%m}.parquet'
        write_dataset('st_daily', panel, rel_path=f'meta/st/daily/{path.name}',
                      root=STORE, writer='tools/build_st_daily.py')
        files.append({
            'path': path.name, 'rows': panel.height,
            'start': str(panel['trade_date'].min()),
            'end': str(panel['trade_date'].max()),
            'bytes': path.stat().st_size,
            'status_counts': dict(panel.group_by('status').agg(pl.len())
                                  .iter_rows()),
        })
        print(f'  {path.name}: {panel.height} 行', flush=True)

    evidence = pl.concat([name_ev, off_ev], how='vertical_relaxed')
    # TODO(write_dataset): st_daily_intervals 未登记契约，暂保留裸写
    write_dataset_at(evidence.sort(['ts_code', 'start_date', 'name']),
                     OUT_DIR / 'st_daily_intervals.parquet', root=STORE,
                     writer='tools/build_st_daily.py')

    produced = {item['path'] for item in files}
    if not args.incremental:
        for stale in OUT_DIR.glob('st_daily_[0-9][0-9][0-9][0-9][0-9][0-9].parquet'):
            if stale.name not in produced:
                stale.unlink()

    comparison = _comparison(OUT_DIR, intervals)
    prev_manifest: dict[str, Any] = {}
    manifest_path = OUT_DIR / 'manifest.json'
    if args.incremental and manifest_path.is_file():
        try:
            prev_manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            prev_manifest = {}
    panel_files = _merge_files(prev_manifest, files) if args.incremental else files
    range_start = min(item['start'] for item in panel_files)
    range_end = max(item['end'] for item in panel_files)
    manifest = {
        'mode': 'incremental' if args.incremental else 'full',
        'built_at': datetime.now().isoformat(timespec='seconds'),
        'range': {'start': range_start, 'end': range_end},
        'window': {'since': str(start), 'end': str(as_of)},
        'rebuilt': [item['path'] for item in files],
        'trading_days': len(_session_days(date.fromisoformat(range_start), as_of)),
        'universe_codes': (prev_manifest.get('universe_codes')
                           if prev_manifest else universe.height),
        'rows': sum(item['rows'] for item in panel_files),
        'files': panel_files,
        'sources': {
            'primary': f'{NAME_HISTORY.relative_to(ROOT)}（tushare namechange，'
                       'start_date=生效日）',
            'fallback': f'{ST_INTERVALS.relative_to(ROOT)}（交易所简称变更/公告定日/人工裁定）',
            'non_st_evidence': f'{ST_COVERAGE.relative_to(ROOT)}',
        },
        'effective_lag': _lag_stats(evidence),
        'note': ('逐日面板按 (trade_date, ts_code) 唯一；status ∈ '
                 'st/*st/non_st/unknown。切换用生效日 start_date，'
                 'ann_date 只留证。非 ST 只认名称段或可信窗口，其余 unknown。'),
    }
    write_text_atomic(json.dumps(manifest, ensure_ascii=False, indent=2),
                      OUT_DIR / 'manifest.json')
    write_text_atomic(json.dumps(comparison, ensure_ascii=False, indent=2,
                                 default=str),
                      OUT_DIR / 'comparison.json')
    print(json.dumps({'rows': manifest['rows'],
                      'known_ratio': comparison['known_ratio'],
                      'conflicts': comparison.get('intervals')},
                     ensure_ascii=False, indent=2, default=str)[:2000])
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
