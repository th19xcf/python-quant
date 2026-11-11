#!/usr/bin/env python
"""按生产口径（T+1 开盘 -> T+1+h 开盘）重算 fwd 标签，与面板 c2c 标签对照。

只读：data/local_store/quotes/stock/**（个股日线，复权走官方出口
DataFetcher._apply_adjustment(frame,'hfq')）、纯日历取自 000001.SH 指数。
输出：labels_oo_h{h}.parquet（date, ts_code, fwd_{h}_oo），供 run_l1.py 评估使用。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from src.data.managers.data_fetcher import DataFetcher  # noqa: E402

# 输入（只读，不是产物）
BASE = ROOT / '_staging/factor_candidates_20261111/base_rebal.parquet'
CAL = ROOT / 'data/local_store/quotes/index/sh/000001.SH.parquet'
STOCK_GLOB = str(ROOT / 'data/local_store/quotes/stock/*/*.parquet')

# 输出（可重建的中间缓存，供 run_l1.py / turnover_curve.py 读取）
OUT = ROOT / '_staging' / 'l1_equal_weight_20261111'

HORIZONS = (20, 5, 60)


def trade_calendar() -> list:
    d = pl.read_parquet(CAL, columns=['date'])['date'].unique().sort().to_list()
    return [x for x in d if x >= __import__('datetime').date(2014, 11, 1)]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--out-dir', default=None,
                    help='标签缓存目录（默认 _staging/l1_equal_weight_20261111）')
    args = ap.parse_args()
    global OUT
    if args.out_dir:
        OUT = Path(args.out_dir).resolve()
    OUT.mkdir(parents=True, exist_ok=True)

    cal = trade_calendar()
    idx_of = {d: i for i, d in enumerate(cal)}

    weekly = sorted(pl.scan_parquet(BASE).select('date').unique().collect()['date'].to_list())
    plan: dict[int, pl.DataFrame] = {}
    needed: list = []
    for h in HORIZONS:
        step = max(1, h // 5)
        rebal = weekly[::step]
        rows = []
        for d in rebal:
            i = idx_of[d]
            if i + 1 + h >= len(cal):
                continue
            rows.append({'date': d, 'entry': cal[i + 1], 'exit': cal[i + 1 + h]})
        plan[h] = pl.DataFrame(rows)
        needed.extend(plan[h]['entry'].to_list())
        needed.extend(plan[h]['exit'].to_list())
    needed = sorted(set(needed))
    print(f'调仓日需求 {sum(p.height for p in plan.values())} 行，'
          f'需要行情日 {len(needed)} 个', flush=True)

    px = (pl.scan_parquet(STOCK_GLOB)
          .select(['date', 'ts_code', 'open', 'high', 'low', 'close', 'hfq_factor'])
          .filter(pl.col('date').is_in(needed))
          .collect())
    print('读取个股行情', px.shape, flush=True)
    px = DataFetcher._apply_adjustment(px, 'hfq').select(['date', 'ts_code', 'open'])
    px = px.rename({'open': 'open_hfq'})
    print('复权完成', px.shape, flush=True)

    for h in HORIZONS:
        p = plan[h]
        print(f'h={h}: 计划 {p.height} 期，开始合并', flush=True)
        entry = px.rename({'date': 'entry', 'open_hfq': 'open_in'})
        exit_ = px.rename({'date': 'exit', 'open_hfq': 'open_out'})
        lab = (p.join(entry, on='entry', how='inner')
               .join(exit_, on=['exit', 'ts_code'], how='inner')
               .select(['date', 'ts_code', 'open_in', 'open_out'])
               .with_columns((pl.col('open_out') / pl.col('open_in') - 1.0)
                             .alias(f'fwd_{h}_oo'))
               .select(['date', 'ts_code', f'fwd_{h}_oo']))
        print(f'h={h}: 标签 {lab.shape}', flush=True)
        # 与面板 c2c 标签对拍：同一 (date, ts_code) 的覆盖率与相关性
        base = (pl.scan_parquet(BASE).filter(pl.col('date').is_in(p['date'].to_list()))
                .select(['date', 'ts_code', f'fwd_{h}']).collect())
        chk = base.join(lab, on=['date', 'ts_code'], how='left')
        n = chk.height
        nn = int(chk[f'fwd_{h}_oo'].is_null().sum())
        both = chk.drop_nulls()
        corr = float(pl.DataFrame(both).select(
            pl.corr(f'fwd_{h}', f'fwd_{h}_oo')).item()) if both.height else float('nan')
        print(f'h={h}: 标签行 {n:,}，oo 缺失 {nn:,}（{nn / n:.2%}），'
              f'两口径相关系数 {corr:.6f}', flush=True)
        lab.write_parquet(OUT / f'labels_oo_h{h}.parquet')


if __name__ == '__main__':
    main()
