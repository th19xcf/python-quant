#!/usr/bin/env python3
"""阶段 3/3：4 个新信息源因子的 PIT 月末值（归档自
E:\\www\\tmp\\factor_eval2_20261105\\compute.py）。

因子：
- ``turnover_20``  : 20 日真实换手率均值取负（越高换手、未来收益越差）
- ``accruals``     : (归母净利TTM - 经营现金流TTM) / 总资产
- ``roe_stability``: 过去 8 季 ROE_TTM 的均值 / 标准差
- ``sue``          : (Q_t - Q_{t-4}) / std(过去 8 季季节差分)

财报可见性一律 ``ann_date <= T``；同一报告期多版本时取**当时可见的最新公告版**
（更正/重述以最新一版为准），与 ``store.latest_announced_as_of``、
``fundamental_factors._dedupe_period`` 口径一致。

输入：``<output-dir>/analysis_monthly.parquet``（可用 ``--base`` 覆盖）。
输出：``<output-dir>/new_factors.parquet``，覆盖信息写工作目录 ``coverage.json``。
"""
from __future__ import annotations

import argparse
import gc
import json
import sys
from datetime import date
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / 'data' / 'local_store'
DEFAULT_WORK_DIR = REPO_ROOT / 'reports' / 'research_tmp'
DEFAULT_OUTPUT_DIR = DATA_DIR / 'meta' / 'research_inputs'


def _require(path: Path, hint: str) -> Path:
    if not path.exists():
        raise SystemExit(f'缺少输入: {path}\n{hint}')
    return path


# ---------------------------------------------------------------- 财务公用
def first_period(frame: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    """同一 (ts_code, end_date) 只保留 ann_date 最新的一版（更正/重述覆盖旧版）。

    PIT 口径：任意时点 T 可见的是 ``ann_date <= T`` 中公告日最新的一版；
    ``ann_date`` 同时是可见性门槛，故取最新版不会引入前视。与
    ``store.latest_announced_as_of``、``fundamental_factors._dedupe_period`` 一致。

    函数名保留 ``first_period``（按报告期取一行），语义已从「最早公告版」
    统一为「最新公告版」。
    """
    return (
        frame.filter(pl.col('ann_date').is_not_null())
        .sort(['ts_code', 'end_date', 'ann_date'])
        .group_by(['ts_code', 'end_date'], maintain_order=True)
        .agg([pl.col('ann_date').last(),
              *(pl.col(c).last() for c in cols)])
    )


def ttm(frame: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    """年初至今累计 → TTM；年报期直接用年报值。

    严格 PIT：上年年报 / 上年同期累计的公告日必须不晚于本期公告日，
    否则该期 TTM 视为当时不可得（返回 null），禁止用未来才披露的数据。
    """
    cur = first_period(frame, cols)
    prev_q = cur.select([
        'ts_code', pl.col('end_date').alias('_prev_end'),
        pl.col('ann_date').alias('_prev_ann'),
        *[pl.col(c).alias(f'{c}__prev_q') for c in cols]])
    merged = cur.with_columns(
        pl.col('end_date').dt.offset_by('-1y').alias('_prev_end')
    ).join(prev_q, on=['ts_code', '_prev_end'], how='left')

    annual = (
        cur.filter(pl.col('end_date').dt.month() == 12)
        .select(['ts_code', pl.col('end_date').alias('_annual_end'),
                 pl.col('ann_date').alias('_annual_ann'),
                 *[pl.col(c).alias(f'{c}__annual') for c in cols]])
        .with_columns((pl.col('_annual_end').dt.year() + 1).alias('_fy'))
        .select(['ts_code', '_fy', '_annual_ann',
                 *[f'{c}__annual' for c in cols]]))
    merged = merged.with_columns(pl.col('end_date').dt.year().alias('_fy')) \
        .join(annual, on=['ts_code', '_fy'], how='left')

    usable = (pl.col('_prev_ann') <= pl.col('ann_date')) \
        & (pl.col('_annual_ann') <= pl.col('ann_date'))
    expr = [
        pl.when(pl.col('end_date').dt.month() == 12).then(pl.col(c))
        .otherwise(pl.when(usable).then(
            pl.col(c) + pl.col(f'{c}__annual') - pl.col(f'{c}__prev_q'))
            .otherwise(None))
        .alias(f'{c}__ttm') for c in cols]
    return merged.with_columns(expr).select(
        ['ts_code', 'ann_date', 'end_date', *[f'{c}__ttm' for c in cols]])


def max_pub(cols: list[str]) -> pl.Expr:
    return pl.max_horizontal(
        [pl.col(c).cast(pl.Int32).fill_null(-719162) for c in cols]
    ).cast(pl.Date)


# ---------------------------------------------------------------- 换手率
def build_turnover(store: Path, mdates: list[date], turn_start: date,
                   batch: int) -> pl.DataFrame:
    shares = pl.read_parquet(
        store / 'meta/factors/stock_shares.parquet',
        columns=['ts_code', 'effective_date', 'float_share'])
    shares = shares.filter(
        pl.col('float_share').is_not_null() & (pl.col('float_share') > 0)
    ).unique(subset=['ts_code', 'effective_date'], keep='last') \
     .sort(['ts_code', 'effective_date'])
    files = sorted((store / 'quotes/stock').glob('*/*.parquet'))
    if not files:
        raise SystemExit(f'未找到股票行情文件: {store / "quotes/stock"}')
    mset = set(mdates)
    parts = []
    for i in range(0, len(files), batch):
        chunk = files[i:i + batch]
        frames, codes = [], []
        for fp in chunk:
            try:
                df = pl.read_parquet(fp, columns=['date', 'ts_code', 'volume'])
                df = df.filter(pl.col('date') >= turn_start)
                if df.height:
                    frames.append(df)
                    codes.append(df['ts_code'][0])
            except Exception as exc:  # noqa: BLE001
                print(f'[WARN] {fp.name}: {exc}', flush=True)
        if not frames:
            continue
        daily = pl.concat(frames, how='vertical_relaxed') \
            .sort(['ts_code', 'date'])
        daily = daily.join_asof(
            shares.filter(pl.col('ts_code').is_in(codes)),
            left_on='date', right_on='effective_date', by='ts_code',
            strategy='backward')
        # volume 单位手 → 股；/ 流通股本（股）= 换手率（小数）
        daily = daily.with_columns(
            (pl.col('volume').cast(pl.Float64) * 100.0
             / pl.col('float_share').cast(pl.Float64)).alias('_turn'))
        daily = daily.with_columns(
            pl.col('_turn').rolling_mean(window_size=20, min_samples=15)
            .over('ts_code').alias('_turn20'))
        parts.append(daily.filter(pl.col('date').is_in(list(mset)))
                     .select(['ts_code', 'date',
                              (-pl.col('_turn20')).alias('turnover_20')]))
        print(f'  turnover batch {i // batch + 1}/'
              f'{(len(files) - 1) // batch + 1}', flush=True)
        del daily, frames
        gc.collect()
    return pl.concat(parts, how='vertical_relaxed').sort(['ts_code', 'date'])


# ---------------------------------------------------------------- 财务因子
def accrual_table(income: pl.DataFrame, cash: pl.DataFrame,
                  bal: pl.DataFrame) -> pl.DataFrame:
    inc_t = ttm(income, ['parent_net_profit']).rename({'ann_date': 'ann_inc'})
    cf_t = ttm(cash, ['operating_cf']).rename({'ann_date': 'ann_cf'})
    bal_f = first_period(bal, ['total_assets', 'total_equity']) \
        .rename({'ann_date': 'ann_bal'})
    return inc_t.join(cf_t, on=['ts_code', 'end_date'], how='inner') \
        .join(bal_f, on=['ts_code', 'end_date'], how='inner') \
        .filter(pl.col('ann_inc').is_not_null()
                & pl.col('ann_cf').is_not_null()
                & pl.col('ann_bal').is_not_null()) \
        .with_columns(
            max_pub(['ann_inc', 'ann_cf', 'ann_bal']).alias('pub_date'),
            ((pl.col('parent_net_profit__ttm') - pl.col('operating_cf__ttm'))
             / pl.col('total_assets')).alias('accruals')) \
        .select(['ts_code', 'end_date', 'pub_date', 'accruals'])


def roe_table(income: pl.DataFrame, bal: pl.DataFrame) -> pl.DataFrame:
    """逐报告期的 ROE_TTM（不做滚动；滚动在月末 as-of 时按「当时已知的 8 期」做）。"""
    inc_t = ttm(income, ['parent_net_profit']).rename({'ann_date': 'ann_inc'})
    bal_f = first_period(bal, ['total_assets', 'total_equity']) \
        .rename({'ann_date': 'ann_bal'})
    return inc_t.join(bal_f, on=['ts_code', 'end_date'], how='inner') \
        .filter(pl.col('ann_inc').is_not_null()
                & pl.col('ann_bal').is_not_null()) \
        .with_columns(
            max_pub(['ann_inc', 'ann_bal']).alias('pub_date'),
            pl.when(pl.col('total_equity') > 0).then(
                pl.col('parent_net_profit__ttm') / pl.col('total_equity')
            ).otherwise(None).alias('_roe')) \
        .select(['ts_code', 'end_date', 'pub_date', '_roe'])


def sue_table(income: pl.DataFrame) -> pl.DataFrame:
    """逐报告期的单季归母净利与季节差分（不做滚动）。"""
    q = first_period(income, ['parent_net_profit']) \
        .rename({'ann_date': 'pub_date'}) \
        .sort(['ts_code', 'end_date']) \
        .with_columns([
            pl.col('end_date').dt.month().alias('_m'),
            pl.col('parent_net_profit').shift(1).over('ts_code')
            .alias('_prev_cum'),
            pl.col('end_date').shift(1).over('ts_code').alias('_prev_end'),
            pl.col('pub_date').shift(1).over('ts_code').alias('_prev_ann')])
    q = q.with_columns(
        pl.when(pl.col('_m') == 3).then(pl.col('parent_net_profit'))
        .when((pl.col('_m') > 3)
              & (pl.col('_prev_end').dt.month() == pl.col('_m') - 3)
              & (pl.col('_prev_end').dt.year() == pl.col('end_date').dt.year())
              & (pl.col('_prev_ann') <= pl.col('pub_date')))
        .then(pl.col('parent_net_profit') - pl.col('_prev_cum'))
        .otherwise(None).alias('_single'))
    prev = q.select(['ts_code', pl.col('end_date').alias('_yago'),
                     pl.col('pub_date').alias('_ann_yago'),
                     pl.col('_single').alias('_single_yago')])
    q = q.with_columns(pl.col('end_date').dt.offset_by('-1y').alias('_yago')) \
        .join(prev, on=['ts_code', '_yago'], how='left') \
        .with_columns(
            pl.when(pl.col('_ann_yago') <= pl.col('pub_date'))
            .then(pl.col('_single') - pl.col('_single_yago'))
            .otherwise(None).alias('_d')) \
        .sort(['ts_code', 'end_date'])
    return q.select(['ts_code', 'end_date', 'pub_date', '_single', '_d'])


def points_at(t: date, acc: pl.DataFrame | None, roe: pl.DataFrame | None,
              sue: pl.DataFrame | None) -> pl.DataFrame:
    """在月末 T 上取「截至 T 已公告」的三因子截面值。

    - 应计：T 时已公告的最近一期；
    - ROE 稳定性：T 时已公告的最近 8 期，至少 6 期有效；
    - SUE：T 时已公告的最近 8 个季节差分（至少 6 个有效），分子为最近一期。

    生产与截断审计共用本函数，保证两条路径口径完全一致。
    """
    parts: list[pl.DataFrame] = []

    if acc is not None:
        parts.append(
            acc.filter(pl.col('pub_date') <= t)
            .sort(['ts_code', 'end_date'])
            .group_by('ts_code', maintain_order=True)
            .agg(pl.col('accruals').drop_nulls().last()))

    if roe is not None:
        r8 = roe.filter(pl.col('pub_date') <= t) \
            .sort(['ts_code', 'end_date']) \
            .group_by('ts_code', maintain_order=True) \
            .agg(pl.col('_roe').drop_nulls().tail(8).alias('_v'))
        parts.append(
            r8.explode('_v').group_by('ts_code', maintain_order=True).agg([
                pl.col('_v').count().alias('_n'),
                pl.col('_v').mean().alias('_m'),
                pl.col('_v').std().alias('_s')])
            .with_columns(
                pl.when((pl.col('_n') >= 6) & (pl.col('_s') > 0))
                .then(pl.col('_m') / pl.col('_s')).otherwise(None)
                .alias('roe_stability'))
            .select(['ts_code', 'roe_stability']))

    if sue is not None:
        s8 = sue.filter(pl.col('pub_date') <= t) \
            .sort(['ts_code', 'end_date']) \
            .group_by('ts_code', maintain_order=True) \
            .agg(pl.col('_d').drop_nulls().tail(8).alias('_v'),
                 pl.col('_d').drop_nulls().last().alias('_cur'))
        parts.append(
            s8.explode('_v').group_by('ts_code', maintain_order=True).agg([
                pl.col('_v').count().alias('_n'),
                pl.col('_v').std().alias('_s'),
                pl.col('_cur').first().alias('_cur')])
            .with_columns(
                pl.when((pl.col('_n') >= 6) & (pl.col('_s') > 0)
                        & (pl.col('_cur').is_not_null()))
                .then(pl.col('_cur') / pl.col('_s')).otherwise(None)
                .alias('sue'))
            .select(['ts_code', 'sue']))

    out = parts[0]
    for nxt in parts[1:]:
        out = out.join(nxt, on='ts_code', how='full', coalesce=True)
    return out.with_columns(pl.lit(t).cast(pl.Date).alias('date'))


def build_financials(store: Path, base: pl.DataFrame,
                     mdates: list[date]) -> pl.DataFrame:
    income = pl.read_parquet(store / 'meta/financials/income.parquet',
                             columns=['ts_code', 'end_date', 'ann_date',
                                      'parent_net_profit'])
    cash = pl.read_parquet(store / 'meta/financials/cashflow.parquet',
                           columns=['ts_code', 'end_date', 'ann_date',
                                    'operating_cf'])
    bal = pl.read_parquet(store / 'meta/financials/balance.parquet',
                          columns=['ts_code', 'end_date', 'ann_date',
                                   'total_assets', 'total_equity'])

    acc = accrual_table(income, cash, bal)
    roe = roe_table(income, bal)
    sue = sue_table(income)

    # 逐月末取「截至 T 已公告」的值；生产与审计共用 points_at。
    frames = [points_at(t, acc, roe, sue) for t in mdates]
    fin = pl.concat(frames, how='vertical_relaxed')

    return base.join(fin, on=['ts_code', 'date'], how='left') \
        .select(['ts_code', 'date', 'accruals', 'roe_stability', 'sue'])


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='构建 new_factors 面板（阶段 3/3）')
    p.add_argument('--store', type=Path, default=DATA_DIR, help='本地数据根目录')
    p.add_argument('--work-dir', type=Path, default=DEFAULT_WORK_DIR,
                   help='评估辅助文件输出目录')
    p.add_argument('--output-dir', type=Path, default=DEFAULT_OUTPUT_DIR,
                   help='new_factors.parquet 输出目录')
    p.add_argument('--base', type=Path, default=None,
                   help='analysis_monthly.parquet 路径；默认取 output-dir 下同名文件')
    p.add_argument('--turnover-start', default='2014-11-01',
                   help='换手率计算的日线起点（需早于首个样本月 20 个交易日）')
    p.add_argument('--batch', type=int, default=300)
    p.add_argument('--output-name', default='new_factors.parquet')
    return p.parse_args()


def main() -> int:
    args = parse_args()
    store = Path(args.store).resolve()
    work_dir = Path(args.work_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    base_path = (Path(args.base).resolve() if args.base
                 else output_dir / 'analysis_monthly.parquet')
    _require(base_path, '请先运行 tools/research/build_analysis_monthly.py')

    base = pl.read_parquet(base_path, columns=['ts_code', 'date']) \
        .sort(['ts_code', 'date'])
    mdates = base['date'].unique().sort().to_list()
    if not mdates:
        raise SystemExit(f'{base_path} 的 date 列为空')
    print(f'月末数: {len(mdates)}  {mdates[0]} ~ {mdates[-1]}', flush=True)

    to = build_turnover(store, mdates, date.fromisoformat(args.turnover_start),
                        args.batch)
    print(f'turnover_20 非空 {to["turnover_20"].is_not_null().sum()}/{to.height}',
          flush=True)
    fin = build_financials(store, base, mdates)
    out = base.join(to, on=['ts_code', 'date'], how='left') \
        .join(fin, on=['ts_code', 'date'], how='left') \
        .sort(['ts_code', 'date'])
    out_path = output_dir / args.output_name
    out.write_parquet(out_path)
    print(f'new_factors: {out.height:,} 行 × {out.width} 列 → {out_path}', flush=True)

    cov = {}
    for f in ['turnover_20', 'accruals', 'roe_stability', 'sue']:
        d = out.filter(pl.col(f).is_not_null())
        cov[f] = {
            'rows': d.height,
            'first': str(d['date'].min()),
            'last': str(d['date'].max()),
            'coverage': round(d.height / out.height, 6),
            'months': d['date'].n_unique(),
            'by_year': {int(k): int(v) for k, v in
                        d.group_by(pl.col('date').dt.year()).len()
                        .sort('date').iter_rows()},
        }
    (work_dir / 'coverage.json').write_text(
        json.dumps(cov, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(cov, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
