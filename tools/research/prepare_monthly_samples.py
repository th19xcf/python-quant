#!/usr/bin/env python3
"""阶段 1/3：构建 PIT 月末价量样本（归档自 E:\\www\\tmp\\factor_eval_20261105\\prepare_samples.py）。

只读 ``data/local_store``，把中间件写入工作目录（默认 ``reports/research_tmp``）：

- ``pv_monthly.parquet``       月末价量因子面板（后复权 OHLC + 原始收盘/成交额）
- ``eligible_monthly.parquet`` 逐月末实时求值的 strict 准入集合（禁止用单一快照）
- ``universe_counts.json``     每月合格只数

下游：``build_analysis_monthly.py`` → ``build_new_factors.py``。
"""
from __future__ import annotations

import argparse
import gc
import json
import sys
from datetime import date
from pathlib import Path

import polars as pl
from loguru import logger

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.advice.factor_panel import SCORE_COLUMNS, compute_factor_panel  # noqa: E402
from src.data.quality.clean_set import get_engine  # noqa: E402

DATA_DIR = REPO_ROOT / 'data' / 'local_store'
DEFAULT_WORK_DIR = REPO_ROOT / 'reports' / 'research_tmp'
DEFAULT_CALENDAR = DATA_DIR / 'quotes' / 'index' / 'sh' / '000001.SH.parquet'

logger.remove()
logger.add(sys.stderr, level='WARNING')


def _require(path: Path, hint: str) -> Path:
    if not path.exists():
        raise SystemExit(f'缺少输入: {path}\n{hint}')
    return path


def month_end_dates(calendar: Path, start: date, end: date) -> list[date]:
    cal = pl.read_parquet(calendar, columns=['date'])['date'].unique().sort()
    out: list[date] = []
    cur = None
    last = None
    for d in cal.to_list():
        if d is None or d < start or d > end:
            continue
        key = (d.year, d.month)
        if cur != key:
            if last is not None:
                out.append(last)
            cur = key
        last = d
    if last is not None:
        out.append(last)
    return out


def load_one(path: Path) -> pl.DataFrame:
    raw = pl.read_parquet(path, columns=[
        'date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close',
        'volume', 'amount', 'hfq_factor'])
    hfq = pl.col('hfq_factor').cast(pl.Float64, strict=False).fill_null(1.0)
    return raw.select([
        pl.col('date').cast(pl.Date, strict=False),
        pl.col('ts_code').cast(pl.Utf8),
        (pl.col('open').cast(pl.Float64, strict=False) * hfq).alias('open'),
        (pl.col('high').cast(pl.Float64, strict=False) * hfq).alias('high'),
        (pl.col('low').cast(pl.Float64, strict=False) * hfq).alias('low'),
        (pl.col('close').cast(pl.Float64, strict=False) * hfq).alias('close'),
        pl.col('volume').cast(pl.Float64, strict=False).alias('volume'),
        pl.col('amount').cast(pl.Float64, strict=False).alias('raw_amount'),
        pl.col('pre_close').cast(pl.Float64, strict=False).alias('raw_prev_close'),
        pl.col('close').cast(pl.Float64, strict=False).alias('raw_close'),
        hfq.alias('hfq_factor'),
    ]).sort('date')


def suspension_aware_forward_returns(daily: pl.DataFrame) -> pl.DataFrame:
    """计算与 ``daily`` 行对齐的 ``fwd_ret`` / ``fwd_ret_21c``。

    窗口到期仍在停牌时（第 21 根 bar 的 ``volume`` 为空或 0），不把窗口内的
    0 收益占位 bar 当作收益，改用其后第 21 根**真实成交** bar 的累计涨跌补足；
    不足 21 根真实 bar 时保留原值（无后续可观测路径，不臆造收益）。

    判据取 ``volume == 0`` 这一层：T8 实测 62 行全部命中 ``volume==0`` 且四价相等，
    而 ``meta/suspension/implied_suspension.parquet`` 只覆盖其中 18 行，不足以
    单独作为判据。
    """
    base = daily.select([
        pl.col('ts_code'),
        pl.col('date'),
        pl.col('open'),
        pl.col('close'),
        (pl.col('volume').is_null() | (pl.col('volume') <= 0)).alias('_ph'),
    ]).with_columns([
        pl.col('open').shift(-1).over('ts_code').alias('_entry'),
        pl.col('close').shift(-1).over('ts_code').alias('_entry_c'),
        pl.col('close').shift(-21).over('ts_code').alias('_close21'),
        pl.col('_ph').shift(-21).over('ts_code').alias('_end_ph'),
        pl.col('_ph').not_().cast(pl.Int32).cum_sum().over('ts_code')
        .alias('_rank'),
    ])
    real = (base.filter(~pl.col('_ph'))
            .select(['ts_code', pl.col('_rank').alias('_target'),
                     pl.col('close').alias('_exit')]))
    base = base.with_columns((pl.col('_rank') + 21).alias('_target'))
    base = base.join(real, on=['ts_code', '_target'], how='left',
                     maintain_order='left')
    fixed = pl.col('_end_ph').fill_null(False) & pl.col('_exit').is_not_null()
    return base.select([
        'ts_code', 'date',
        pl.when(fixed & pl.col('_entry').is_not_null())
        .then(pl.col('_exit') / pl.col('_entry') - 1.0)
        .otherwise(pl.col('_close21') / pl.col('_entry') - 1.0)
        .alias('fwd_ret'),
        pl.when(fixed & pl.col('_entry_c').is_not_null())
        .then(pl.col('_exit') / pl.col('_entry_c') - 1.0)
        .otherwise(pl.col('_close21') / pl.col('_entry_c') - 1.0)
        .alias('fwd_ret_21c'),
    ])


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='构建 PIT 月末价量样本（阶段 1/3）')
    p.add_argument('--store', type=Path, default=DATA_DIR, help='本地数据根目录')
    p.add_argument('--work-dir', type=Path, default=DEFAULT_WORK_DIR,
                   help='中间件输出目录')
    p.add_argument('--calendar', type=Path, default=DEFAULT_CALENDAR,
                   help='交易日历（默认上证指数日线）')
    p.add_argument('--start', default='2015-01-01')
    p.add_argument('--end', default='2026-10-31')
    p.add_argument('--batch', type=int, default=300)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    store = Path(args.store).resolve()
    work_dir = Path(args.work_dir).resolve()
    work_dir.mkdir(parents=True, exist_ok=True)

    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    mdates = month_end_dates(Path(args.calendar), start, end)
    if not mdates:
        raise SystemExit(f'交易日历 {args.calendar} 在 {start}~{end} 内没有月末样本')
    mset = set(mdates)
    print(f'月末样本 {len(mdates)} 个: {mdates[0]} ~ {mdates[-1]}', flush=True)

    files = sorted((store / 'quotes/stock').glob('*/*.parquet'))
    if not files:
        raise SystemExit(f'未找到股票行情文件: {store / "quotes/stock"}')
    print(f'股票行情文件 {len(files)} 个', flush=True)

    parts: list[pl.DataFrame] = []
    keep = list(dict.fromkeys([
        'ts_code', 'date', 'fwd_ret', 'fwd_ret_21c', 'raw_close', 'volume',
        'history_rows', 'hist_seq', *SCORE_COLUMNS]))
    for i in range(0, len(files), args.batch):
        chunk = files[i:i + args.batch]
        frames = []
        for fp in chunk:
            try:
                frames.append(load_one(fp))
            except Exception as exc:  # noqa: BLE001
                print(f'[WARN] 跳过 {fp.name}: {exc}', flush=True)
        daily = pl.concat(frames, how='vertical_relaxed').sort(['ts_code', 'date'])
        labels = suspension_aware_forward_returns(daily)
        daily = daily.hstack(labels.select(['fwd_ret', 'fwd_ret_21c']))
        daily = daily.with_columns(
            (pl.int_range(pl.len()).over('ts_code') + 1).alias('hist_seq'))
        fac = compute_factor_panel(daily, extended=False)
        fac = fac.filter(pl.col('date').is_in(list(mset)))
        fac = fac.select([c for c in keep if c in fac.columns])
        parts.append(fac)
        if (i // args.batch) % 5 == 0:
            print(f'  batch {i // args.batch + 1}/'
                  f'{(len(files) - 1) // args.batch + 1}, '
                  f'累计 {sum(p.height for p in parts)} 行', flush=True)
        del daily, fac, frames
        gc.collect()

    monthly = pl.concat(parts, how='vertical_relaxed').sort(['ts_code', 'date'])
    monthly.write_parquet(work_dir / 'pv_monthly.parquet')
    print(f'价量月末样本: {monthly.height} 行 × {monthly.width} 列', flush=True)

    engine = get_engine()
    elig = []
    counts = {}
    for d in mdates:
        codes = engine.eligible_set(d, tier='strict', data_type='stock')
        counts[str(d)] = len(codes)
        elig.append(pl.DataFrame({'date': [d] * len(codes), 'ts_code': codes}))
    elig_df = pl.concat(elig, how='vertical_relaxed')
    elig_df.write_parquet(work_dir / 'eligible_monthly.parquet')
    with open(work_dir / 'universe_counts.json', 'w', encoding='utf-8') as fh:
        json.dump(counts, fh, ensure_ascii=False, indent=2)
    vals = list(counts.values())
    print(f'strict 月末只数范围: {min(vals)} ~ {max(vals)}，'
          f'合计 {len(elig_df)} 条', flush=True)
    for d in mdates:
        if d.month == 12:
            print(f'  {d}: {counts[str(d)]}', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
