#!/usr/bin/env python3
"""「不买名单」收窄版前向记录（幂等重跑）。

每周最后一个交易日收盘后运行：把可交易池按 R1'（BIAS12 > +10%）与
R3（20 日均换手池内最高 20%）分成 drop / keep 两组，记录每只股票下一周的持有收益。

用法（项目根目录、venv）::

    .venv\\Scripts\\python.exe work/不买名单剔除_2026-11-26/前向记录.py

口径与回测一致（见同目录 收窄版重跑.py / run.py）：信号日收盘出信号，次一交易日
开盘买入，下一信号日后首个可卖 bar 开盘卖出；出场停牌/一字跌停顺延，退市按最后收盘价。
幂等：每次重算所有已成熟周，按 (date, ts_code) 覆盖同键旧行，不动其它周。
"""
from __future__ import annotations

import importlib.util
import os
import sys
from datetime import date
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location('excl_run', HERE / 'run.py')
run = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(run)  # type: ignore[union-attr]

FORWARD_START = date(2026, 12, 1)
OUT_DIR = HERE / 'forward'
REC = OUT_DIR / 'records.csv'
COLS = ['date', 'ts_code', 'group', 'ret']
MIN_COVER = 0.90


def forward_weeks(cal: list[date]) -> list[date]:
    """每周最后一个交易日（ISO 周），取 FORWARD_START 之后。"""
    wk = (pl.DataFrame({'date': cal}).filter(pl.col('date') >= FORWARD_START)
          .with_columns(pl.col('date').dt.strftime('%G-%V').alias('wk')))
    return wk.group_by('wk').agg(pl.col('date').max()).sort('date')['date'].to_list()


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df, _weeks, cal = run.load()
    df = run.features(df)
    weeks = forward_weeks(cal)
    print(f'[forward] 待记录周末 {len(weeks)} 个：{weeks[:1]} … {weeks[-1:]}', flush=True)
    new = pl.DataFrame(schema={'date': pl.Date, 'ts_code': pl.String,
                               'group': pl.String, 'ret': pl.Float64})
    if weeks:
        sig = run.signal_frame(df, weeks)
        bias = (df.filter(pl.col('date').is_in(weeks)).select(['date', run.G, 'bias12'])
                .unique(['date', run.G], keep='last'))
        sig = sig.join(bias, on=['date', run.G], how='left').with_columns(
            (pl.col('bias12') > 0.10).fill_null(False).alias('r1b'))
        sig = sig.with_columns(
            pl.when(pl.col('r1b') | pl.col('r3')).then(pl.lit('drop'))
            .otherwise(pl.lit('keep')).alias('group'))
        hold = run.holding_returns(df, weeks)
        m = sig.select(['date', run.G, 'group']).join(hold, on=['date', run.G], how='inner')
        pool = sig.group_by('date').agg(pl.len().alias('n_pool'))
        cnt = m.group_by('date').agg(pl.len().alias('n_hit'))
        ready = (pool.join(cnt, on='date', how='left').fill_null(0)
                 .filter(pl.col('n_hit') >= MIN_COVER * pl.col('n_pool'))['date'].to_list())
        new = (m.filter(pl.col('date').is_in(ready))
               .rename({run.G: 'ts_code', 'r': 'ret'}).select(COLS).sort(['date', 'ts_code']))
        print(f'[forward] 成熟周 {len(ready)} 个，新增/覆盖 {new.height:,} 行', flush=True)
    if REC.exists():
        old = pl.read_csv(REC, try_parse_dates=True).select(COLS)
        old = old.with_columns(pl.col('date').cast(pl.Date), pl.col('ret').cast(pl.Float64))
        out = (pl.concat([old.filter(~pl.col('date').is_in(new['date'].unique().to_list())), new])
               .unique(['date', 'ts_code'], keep='last').sort(['date', 'ts_code']))
    else:
        out = new
    tmp = REC.with_suffix('.csv.tmp')
    out.write_csv(tmp)
    os.replace(tmp, REC)
    print(f'[forward] 写入 {REC}：{out.height:,} 行 / {out["date"].n_unique()} 周', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
