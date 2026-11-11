#!/usr/bin/env python3
"""基金份额折算因子修复 + 全 ETF 复权极值扫描（只改基金侧）。

背景与四个实测样本
----------------
1. ``159930.SZ`` 2013-12-12：事件表有折算记录（1:0.4074），但 ``quotes/fund``
   原始价**没有**跳空（2013-12-11 close 0.948 → 2013-12-12 close 0.945，
   原始收益 -0.32%）；因子台阶 2.588957→1.054741 反而造出 -59.4% 的复权收益。
   同标的 2025-09-22 亦然（原始 -1.19%，报表 pct_chg -6.31%，pre_close 1.4239
   为未复权价）。判定：行情源已对该基金做过前复权，因子属二次复权 → 置 qfq=1。
2. ``512890.SH`` 2021-10-25：原始 1.639→0.801（-51.13%），事件表缺失；
   上交所公告拆分比例 1:2（拆分前净值 1.6004 → 拆分后 0.8002）。
3. ``515260.SH`` 2025-09-15：原始 1.269→0.636（-49.88%），事件表缺失；
   上交所公告 1:2（拆分日 2025-09-12）。
4. ``561380.SH`` 2026-06-25：原始 2.373→0.961（-59.50%），事件表缺失；
   上交所公告 1:2.5（权益登记日 2026-06-24）。

修法
----
* 缺事件：补进 ``factors/fund_share_change.parquet``（source 记交易所公告）；
* 因子：只重算上述 4 只，其余行原样保留后整表原子替换；
* 二次复权：``src.data.local_store.fund_factors.QUOTE_SPLIT_ADJUSTED_CODES``
  在重建时同样跳过，保证快照与代码口径一致；
* 改前备份到 ``data/local_store/_backup/``，逐行旧值→新值写
  ``reports/etf_audit/fund_split_repair_rows.csv``。

用法::

    .venv\\Scripts\\python.exe tools/fund_split_repair.py scan --tag before
    .venv\\Scripts\\python.exe tools/fund_split_repair.py apply
    .venv\\Scripts\\python.exe tools/fund_split_repair.py scan --tag after
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Sequence

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data.adj_factor_calculator import AdjFactorCalculator  # noqa: E402
from src.data.local_store.fund_factors import (  # noqa: E402
    FUND_FACTOR_MAX,
    FUND_SHARE_CHANGE_FILENAME,
    SHARE_CHANGE_COLUMNS,
    prepare_events_for_code,
)

STORE = PROJECT_ROOT / 'data' / 'local_store'
QUOTES = STORE / 'quotes' / 'fund'
FACTORS = STORE / 'factors'
BACKUP = STORE / '_backup'
ANOMALIES = STORE / 'meta' / 'anomalies'
OUT_DIR = PROJECT_ROOT / 'reports' / 'etf_audit'
OUT_DIR.mkdir(parents=True, exist_ok=True)

FACTOR_PATH = FACTORS / 'fund_adj_factor.parquet'
SHARE_PATH = FACTORS / FUND_SHARE_CHANGE_FILENAME
DIVIDEND_PATH = FACTORS / 'fund_dividend.parquet'

#: 事件表缺失、由交易所公告补录的份额拆分。
MISSING_SPLIT_EVENTS: tuple[dict[str, Any], ...] = (
    {'ts_code': '512890.SH', 'ex_date': date(2021, 10, 22), 'split_type': '份额拆分',
     'share_ratio': 2.0,
     'source': 'sse:512890_20211025 份额拆分结果公告（1:2，拆分前净值1.6004→0.8002）'},
    {'ts_code': '515260.SH', 'ex_date': date(2025, 9, 12), 'split_type': '份额拆分',
     'share_ratio': 2.0,
     'source': 'sse:515260_20250915 份额拆分结果公告（1:2，拆分日2025-09-12）'},
    {'ts_code': '561380.SH', 'ex_date': date(2026, 6, 24), 'split_type': '份额拆分',
     'share_ratio': 2.5,
     'source': 'sse:561380_20260625 份额拆分结果公告（1:2.5，登记日2026-06-24）'},
    {'ts_code': '510950.SH', 'ex_date': date(2026, 4, 24), 'split_type': '份额拆分',
     'share_ratio': 1.28775,
     'source': ('gffunds:510950 份额拆分结果公告（78,450,000份→101,020,838份，'
                '净值1.2651→0.9825，比例1:1.28775，拆分日2026-04-24）')},
)

#: 需要重算因子的代码（缺事件 4 只 + 二次复权 1 只）。
REPAIR_CODES: tuple[str, ...] = (
    '159930.SZ', '512890.SH', '515260.SH', '561380.SH', '510950.SH')

#: 复权极值扫描阈值。
JUMP_THRESHOLD = 0.20

#: 非股票权益类 ETF 关键字（与动量回测口径一致，仅用于样例标注）。
NON_EQUITY_KEYWORDS = ('债', '黄金', '白银', '豆粕', '原油', '商品', '货币',
                       '日利', '添益', '现金', '存单', '短融', '政金', '信用')


def market_of(code: str) -> str:
    return 'sh' if code.endswith('SH') else 'sz'


def load_quote(code: str) -> pl.DataFrame | None:
    path = QUOTES / market_of(code) / f'{code}.parquet'
    if not path.exists():
        return None
    frame = pl.read_parquet(path)
    keep = [c for c in ('date', 'close', 'pre_close', 'amount') if c in frame.columns]
    return frame.select(keep).sort('date').with_columns(
        pl.col('date').cast(pl.Date).alias('trade_date'))


def load_etf_universe() -> tuple[list[str], dict[str, str]]:
    """MySQL ``fund_basic`` ETF ∩ 本地行情 − 占位桩。"""
    from sqlalchemy import create_engine, text

    from src.utils.config import get_config

    d = get_config().database
    engine = create_engine(
        f'mysql+pymysql://{d.username}:{d.password}@{d.host}:{d.port}/{d.database}',
        connect_args={'connect_timeout': 10})
    with engine.connect() as session:
        rows = session.execute(text(
            "select ts_code, name from fund_basic where fund_type='ETF'")).fetchall()
    names = {r[0]: r[1] for r in rows}
    placeholder_path = ANOMALIES / 'placeholder_quote_codes.csv'
    placeholders = set(pl.read_csv(placeholder_path)['ts_code'].to_list()) \
        if placeholder_path.exists() else set()
    codes = []
    for code in sorted(names):
        if code in placeholders:
            continue
        if (QUOTES / market_of(code) / f'{code}.parquet').exists():
            codes.append(code)
    return codes, names


def _dividends_for(code: str) -> pl.DataFrame | None:
    if not DIVIDEND_PATH.exists():
        return None
    frame = pl.read_parquet(DIVIDEND_PATH).filter(pl.col('ts_code') == code)
    return frame if not frame.is_empty() else None


def _share_changes_for(code: str) -> pl.DataFrame | None:
    frame = pl.read_parquet(SHARE_PATH).filter(pl.col('ts_code') == code)
    return frame if not frame.is_empty() else None


def recompute_code_factors(code: str, quotes: pl.DataFrame) -> pl.DataFrame:
    """按事件表重算单只基金因子（内部会跳过 QUOTE_SPLIT_ADJUSTED_CODES）。"""
    prices = quotes.select([
        pl.lit(code).alias('ts_code'),
        pl.col('trade_date'),
        pl.col('close').cast(pl.Float64),
    ]).sort('trade_date')
    events = prepare_events_for_code(
        code, prices['trade_date'].to_list(),
        _dividends_for(code), _share_changes_for(code))
    result = AdjFactorCalculator.calculate_factors(
        prices, events, max_factor=FUND_FACTOR_MAX)
    return result.select(['ts_code', 'trade_date', 'qfq_factor', 'hfq_factor'])


def _event_dates(code: str, quotes: pl.DataFrame) -> dict[str, set[date]]:
    """折算/分红的实际生效交易日（与 fund_factors 对齐规则一致）。"""
    dates = quotes['trade_date'].to_list()
    date_set = set(dates)
    share_dates: set[date] = set()
    div_dates: set[date] = set()
    sc = _share_changes_for(code)
    if sc is not None:
        for ex in sc['ex_date'].to_list():
            nxt = next((d for d in dates if d > ex), None)
            if nxt is not None:
                share_dates.add(nxt)
    dv = _dividends_for(code)
    if dv is not None:
        for ex in dv['ex_date'].to_list():
            # 扫描标的均为 ETF：除息日当天生效
            if ex in date_set:
                div_dates.add(ex)
    return {'split': share_dates, 'dividend': div_dates}


def scan(tag: str, threshold: float = JUMP_THRESHOLD) -> dict[str, Any]:
    """全 ETF 复权单日极值扫描 + 四类归因。"""
    codes, names = load_etf_universe()
    factors = pl.read_parquet(FACTOR_PATH)
    rows: list[dict[str, Any]] = []
    scanned = 0
    for code in codes:
        quotes = load_quote(code)
        if quotes is None or quotes.height < 3:
            continue
        factors_code = factors.filter(pl.col('ts_code') == code).select(
            ['trade_date', 'qfq_factor'])
        if factors_code.is_empty():
            continue
        frame = (quotes.join(factors_code, on='trade_date', how='left')
                 .with_columns(pl.col('qfq_factor').fill_null(1.0))
                 .sort('trade_date'))
        if frame.height < 2:
            continue
        scanned += 1
        frame = frame.with_columns([
            (pl.col('close') * pl.col('qfq_factor')).alias('adj_close'),
            (pl.col('close') / pl.col('close').shift(1) - 1.0).alias('raw_ret'),
            (pl.col('qfq_factor') / pl.col('qfq_factor').shift(1) - 1.0)
            .alias('factor_step'),
        ])
        frame = frame.with_columns(
            (pl.col('adj_close') / pl.col('adj_close').shift(1) - 1.0).alias('adj_ret'))
        hits = frame.filter(pl.col('adj_ret').abs() > threshold)
        if hits.is_empty():
            continue
        ev = _event_dates(code, quotes)
        for row in hits.iter_rows(named=True):
            d = row['trade_date']
            if d in ev['split']:
                category = '折算'
            elif d in ev['dividend']:
                category = '分红'
            elif abs(row['factor_step'] or 0.0) < 1e-9:
                category = '真实大跌'
            else:
                category = '数据错误'
            rows.append({
                'ts_code': code,
                'name': names.get(code, ''),
                'date': str(d),
                'close': row['close'],
                'raw_ret': round(row['raw_ret'], 6),
                'qfq_factor': round(row['qfq_factor'], 6),
                'factor_step': round(row['factor_step'] or 0.0, 6),
                'adj_ret': round(row['adj_ret'], 6),
                'category': category,
            })
    counts: dict[str, int] = dict.fromkeys(('折算', '分红', '数据错误', '真实大跌'), 0)
    for row in rows:
        counts[row['category']] += 1
    samples: dict[str, list[dict[str, Any]]] = {}
    for category in counts:
        samples[category] = [r for r in rows if r['category'] == category][:8]
    detail = pl.DataFrame(rows) if rows else pl.DataFrame(schema={
        'ts_code': pl.Utf8, 'name': pl.Utf8, 'date': pl.Utf8, 'close': pl.Float64,
        'raw_ret': pl.Float64, 'qfq_factor': pl.Float64, 'factor_step': pl.Float64,
        'adj_ret': pl.Float64, 'category': pl.Utf8})
    detail.write_csv(OUT_DIR / f'fund_extreme_scan_{tag}.csv')
    summary = {
        'tag': tag,
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'threshold': threshold,
        'etf_universe': len(codes),
        'etf_scanned': scanned,
        'extreme_rows': len(rows),
        'category_counts': counts,
        'samples': samples,
    }
    (OUT_DIR / f'fund_extreme_scan_{tag}.json').write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    print(json.dumps({k: summary[k] for k in
                      ('tag', 'etf_universe', 'etf_scanned', 'extreme_rows',
                       'category_counts')}, ensure_ascii=False, indent=2))
    return summary


def _append_missing_events() -> dict[str, Any]:
    frame = pl.read_parquet(SHARE_PATH)
    existing = set(zip(frame['ts_code'].to_list(), frame['ex_date'].to_list(), strict=False))
    now = datetime.now()
    new_rows = [row for row in MISSING_SPLIT_EVENTS
                if (row['ts_code'], row['ex_date']) not in existing]
    if new_rows:
        addition = pl.DataFrame([{**row, 'fetched_at': now} for row in new_rows])
        addition = addition.select(
            [c for c in SHARE_CHANGE_COLUMNS if c in addition.columns])
        merged = pl.concat([frame, addition], how='vertical_relaxed').sort(
            ['ts_code', 'ex_date'])
        tmp = SHARE_PATH.with_suffix('.parquet.tmp')
        merged.write_parquet(tmp, compression='zstd', statistics=True)
        tmp.replace(SHARE_PATH)
    return {'added_events': len(new_rows),
            'events': [{k: str(v) for k, v in row.items()} for row in new_rows]}


def apply_repair() -> dict[str, Any]:
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_files = {}
    for path in (FACTOR_PATH, SHARE_PATH, DIVIDEND_PATH):
        target = BACKUP / f'{path.stem}.{stamp}_pre_splitfix{path.suffix}'
        shutil.copy2(path, target)
        backup_files[path.name] = str(target)

    events_info = _append_missing_events()

    old_factors = pl.read_parquet(FACTOR_PATH)
    new_parts: list[pl.DataFrame] = []
    detail_rows: list[dict[str, Any]] = []
    code_summary: dict[str, Any] = {}
    for code in REPAIR_CODES:
        quotes = load_quote(code)
        if quotes is None:
            raise RuntimeError(f'{code} 本地行情缺失，无法重算因子')
        old = old_factors.filter(pl.col('ts_code') == code).select(
            ['trade_date', 'qfq_factor', 'hfq_factor']).sort('trade_date')
        new = recompute_code_factors(code, quotes).sort('trade_date')
        merged = old.join(new, on='trade_date', how='full', suffix='_new')
        merged = merged.with_columns([
            pl.col('qfq_factor_new').fill_null(pl.col('qfq_factor')),
            pl.col('hfq_factor_new').fill_null(pl.col('hfq_factor')),
        ])
        for field in ('qfq_factor', 'hfq_factor'):
            diff = merged.filter(
                (pl.col(field) - pl.col(f'{field}_new')).abs() > 1e-12)
            for row in diff.iter_rows(named=True):
                detail_rows.append({
                    'ts_code': code,
                    'trade_date': str(row['trade_date']),
                    'field': field,
                    'old': row[field],
                    'new': row[f'{field}_new'],
                })
        segments = []
        for field in ('qfq_factor', 'hfq_factor'):
            groups = merged.group_by(
                [pl.col(field).round(9).alias('old'),
                 pl.col(f'{field}_new').round(9).alias('new')],
                maintain_order=True).agg([
                pl.len().alias('rows'),
                pl.col('trade_date').min().alias('start'),
                pl.col('trade_date').max().alias('end')])
            segments.append({
                'field': field,
                'transitions': [
                    {'old': row['old'], 'new': row['new'], 'rows': row['rows'],
                     'start': str(row['start']), 'end': str(row['end'])}
                    for row in groups.iter_rows(named=True)
                    if abs((row['old'] or 0.0) - (row['new'] or 0.0)) > 1e-12
                ],
            })
        code_summary[code] = {
            'clip_rows': old.height,
            'changed_qfq_rows': int(merged.filter(
                (pl.col('qfq_factor') - pl.col('qfq_factor_new')).abs() > 1e-12).height),
            'changed_hfq_rows': int(merged.filter(
                (pl.col('hfq_factor') - pl.col('hfq_factor_new')).abs() > 1e-12).height),
            'segments': segments,
        }
        new_parts.append(new.with_columns(pl.lit(code).alias('ts_code')).select(
            ['ts_code', 'trade_date', 'qfq_factor', 'hfq_factor']))

    kept = old_factors.filter(~pl.col('ts_code').is_in(REPAIR_CODES))
    merged_all = pl.concat([kept, *new_parts], how='vertical_relaxed').sort(
        ['ts_code', 'trade_date'])
    tmp = FACTOR_PATH.with_suffix('.parquet.tmp')
    merged_all.write_parquet(
        tmp, compression='zstd', compression_level=3,
        statistics=True, row_group_size=100_000)
    tmp.replace(FACTOR_PATH)

    detail = pl.DataFrame(detail_rows, schema={
        'ts_code': pl.Utf8, 'trade_date': pl.Utf8, 'field': pl.Utf8,
        'old': pl.Float64, 'new': pl.Float64})
    detail.write_csv(OUT_DIR / 'fund_split_repair_rows.csv')

    manifest = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'backup_files': backup_files,
        'events_upserted': events_info,
        'codes': code_summary,
        'factor_rows_total': merged_all.height,
        'changed_rows': len(detail_rows),
        'rows_csv': str(OUT_DIR / 'fund_split_repair_rows.csv'),
        'note': ('quotes/fund 下的 qfq_factor/hfq_factor 副本未改（该目录由另一条'
                 '任务写入，本轮只读）；以下游 factors 快照为准'),
    }
    (OUT_DIR / 'fund_split_repair_manifest.json').write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    print(json.dumps({'changed_rows': len(detail_rows),
                      'factor_rows_total': merged_all.height,
                      'events_added': events_info['added_events']},
                     ensure_ascii=False, indent=2))
    return manifest


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['scan', 'apply'])
    parser.add_argument('--tag', default='current', help='scan 产物后缀')
    parser.add_argument('--threshold', type=float, default=JUMP_THRESHOLD,
                        help='复权单日收益绝对值阈值（默认 0.20）')
    args = parser.parse_args(argv)
    if args.action == 'apply':
        apply_repair()
    else:
        scan(args.tag, args.threshold)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
