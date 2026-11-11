#!/usr/bin/env python3

"""880/881 段通达信自编指数：判定「价格序列」还是「统计量序列」，输出可过滤白/黑名单。

背景
----
σ 实测把「|月收益| > 50pp」的 217 只 880/881 指数整只剔除，但其中多数并非数据
错误：有的是涨跌家数/停板家数这类**根本不是价格**的统计量序列，有的是月末尾部门控
（``date == CAL[ym]``）遇上交易日缺口，把跨月收益算成了单月。本工具按结构特征
逐条定性，给出可直接用于过滤的名单。

判定规则（按顺序命中即定档）
--------------------------
R1 ``int_frac >= 0.95``        点位整数占比≥95% → 家数/只数序列，不是价格
R2 名称命中统计口径关键词       停板/涨停/跌停/连板/多板/涨跌/家数/复牌/换手/振荡/
                               强势/较弱/较强/活跃/次新/首板/曾/回购/新低/新高/异动/
                               预高送转/均价 → 通达信统计口径板块
R3 ``corr_mkt < 0.20``         与 000300.SH 日收益相关<0.2 → 与价格体系无关联
R4 ``rows < 250`` 或无重叠      历史不足一年 → 无法验证
R5 连续 ≥10 个交易日单边       累计 >50% 的单调涨跌段 → 疑合成/回填段
R6 ``max_abs_daily >= 15%``    单日涨跌≥15% → 编制断点或行情断点
R7 其余                        干净价格序列

用法::

    python tools/index_pool_classify.py
    # → reports/data_audit/index_880_881_pool_classification_20261111.csv

CSV 列 ``class`` 取值：``white`` / ``defect`` / ``synthetic`` / ``not_price`` /
``short_history``，供下游按 ``class == 'white'`` 直接过滤。
"""

from __future__ import annotations

import argparse
import glob
import os
from typing import cast

import polars as pl

STAT_NAME_KEYS = ('停板', '涨停', '跌停', '连板', '多板', '涨跌', '家数', '复牌', '换手',
                  '振荡', '强势', '较弱', '较强', '活跃', '次新', '首板', '曾', '回购',
                  '新低', '新高', '异动', '预高送转', '均价')

INT_BLACK = 0.95
CORR_BLACK = 0.20
JUMP_DEFECT = 15.0
MIN_ROWS = 250
BAD_MONTH = 50.0


def classify(r: dict) -> tuple[str, str]:
    name = r['name'] or ''
    if r['int_frac'] >= INT_BLACK:
        return 'not_price', 'R1 点位整数占比≥95%（家数/只数序列）'
    hit = [k for k in STAT_NAME_KEYS if k in name]
    if hit:
        return 'not_price', f'R2 名称命中统计口径关键词「{hit[0]}」'
    if r['corr_mkt'] is not None and r['corr_mkt'] < CORR_BLACK:
        return 'not_price', f'R3 与000300相关{r["corr_mkt"]:.3f}<0.2'
    if r['rows'] < MIN_ROWS or r['corr_mkt'] is None:
        return 'short_history', f'R4 行数{r["rows"]}<250 或无重叠，无法验证'
    if r['up_streak'] >= 10 and (r['up_cum'] or 0) > 50:
        return 'synthetic', (f'R5 连续{r["up_streak"]}个交易日每日>+2%、累计'
                             f'+{r["up_cum"]:.0f}%，真实行业篮子不可能')
    if r['dn_streak'] >= 10 and (r['dn_cum'] or 0) < -50:
        return 'synthetic', (f'R5 连续{r["dn_streak"]}个交易日每日<-2%、累计'
                             f'{r["dn_cum"]:.0f}%，真实行业篮子不可能')
    if r['max_abs_daily'] >= JUMP_DEFECT:
        return 'defect', (f'R6 单日{("+" if r["jump_sign"] > 0 else "")}{r["max_abs_daily"]:.2f}%'
                          f'（{r["jump_date"]}），共{r["jump_days_15"]}天≥15%')
    return 'white', 'R7 非整数点位+无合成段+无≥15%单日断点+与大盘正相关'


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--store', default='data/local_store')
    ap.add_argument('--out', default='reports/data_audit/'
                                    'index_880_881_pool_classification_20261111.csv')
    args = ap.parse_args()
    quote_dir = os.path.join(args.store, 'quotes', 'index', 'sh')
    names = (pl.read_parquet(os.path.join(args.store, 'meta', 'index_basic.parquet'))
             .select(['ts_code', 'name']).unique('ts_code'))

    bench = pl.read_parquet(os.path.join(quote_dir, '000300.SH.parquet')).sort('date')
    ends = (bench.with_columns(pl.col('date').dt.strftime('%Y-%m').alias('ym'))
            .group_by('ym').agg(pl.col('date').max()).sort('ym'))
    ends_map = dict(zip(ends['ym'].to_list(), ends['date'].to_list(), strict=False))
    b = (bench.select(['date', 'close'])
         .with_columns((pl.col('close') / pl.col('close').shift(1)).log().alias('rb'))
         .select(['date', 'rb']))

    rows = []
    for path in sorted(glob.glob(os.path.join(quote_dir, '88[01]*.parquet'))):
        code = os.path.basename(path).replace('.parquet', '')
        q = pl.read_parquet(path).select(['date', 'close', 'volume']).sort('date')
        close = q['close']
        ret = (close / close.shift(1) - 1) * 100
        gaps = q.select(pl.col('date').diff().dt.total_days().alias('g'))['g'].drop_nulls()
        j = q.select(['date', 'close']).join(b, on='date', how='inner')
        corr = None
        if len(j) > 100:
            j = (j.with_columns((pl.col('close') / pl.col('close').shift(1)).log().alias('r'))
                 .drop_nulls())
            if j['r'].std() and j['rb'].std():
                corr = round(float(j.select(pl.corr('r', 'rb')).item()), 4)

        # 月收益：按 000300.SH 的每月最后交易日门控，与 σ 实测口径一致
        w = (q.select(['date', 'close'])
             .with_columns(pl.col('date').dt.strftime('%Y-%m').alias('ym'), ret.alias('r'))
             .join(pl.DataFrame({'ym': list(ends_map), 'me': list(ends_map.values())})
                   .with_columns(pl.col('me').cast(pl.Date)), on='ym', how='left'))
        mr = (w.filter(pl.col('date') == pl.col('me')).drop_nulls('r')
              .with_columns((pl.col('close') / pl.col('close').shift(1) - 1).mul(100).alias('rm'),
                            pl.col('me').shift(1).alias('prev_me'))
              .drop_nulls('rm')
              .with_columns((pl.col('me') - pl.col('prev_me')).dt.total_days().alias('span')))
        bad = mr.filter(pl.col('rm').abs() > BAD_MONTH)
        top_bad = bad.sort(pl.col('rm').abs(), descending=True)

        # 单边段：连续 ≥10 个交易日同向超 2%，用对数收益累计
        rw = (pl.DataFrame({'ret': ret})
              .with_columns(up=(pl.col('ret') > 2).fill_null(False),
                            dn=(pl.col('ret') < -2).fill_null(False)))
        rw = rw.with_columns(gu=(pl.col('up') != pl.col('up').shift(1)).cum_sum(),
                             gd=(pl.col('dn') != pl.col('dn').shift(1)).cum_sum(),
                             lr=(pl.col('ret') / 100 + 1).log())
        # 同长度段按累计幅度定序，避免并列时取到不确定的一段
        gu = (rw.filter(pl.col('up')).group_by('gu')
              .agg(n=pl.len(), cum=(pl.col('lr').sum().exp() - 1).mul(100))
              .sort(['n', 'cum'], descending=[True, True]))
        gd = (rw.filter(pl.col('dn')).group_by('gd')
              .agg(n=pl.len(), cum=(pl.col('lr').sum().exp() - 1).mul(100))
              .sort(['n', 'cum'], descending=[True, False]))
        jump_raw = ret.abs()
        jmax = float(cast(float, jump_raw.max()))
        idx = int(cast(int, jump_raw.arg_max())) if jmax >= JUMP_DEFECT else None

        rows.append({
            'ts_code': code, 'rows': len(q), 'start': str(q['date'].min()),
            'end': str(q['date'].max()),
            'close_min': round(float(cast(float, close.min())), 2),
            'close_max': round(float(cast(float, close.max())), 2),
            'int_frac': round(float(cast(float, (close == close.round(0)).mean())), 4),
            'max_abs_daily': round(jmax, 2),
            'jump_days_15': int(cast(int, (jump_raw >= JUMP_DEFECT).sum())),
            'jump_date': str(q['date'][idx]) if idx is not None else None,
            'jump_sign': (1 if float(cast(float, ret[idx])) > 0 else -1) if idx is not None else 0,
            'max_gap_days': int(cast(int, gaps.max())) if len(gaps) else 0,
            'corr_mkt': corr,
            'up_streak': int(cast(int, gu['n'][0])) if gu.height else 0,
            'up_cum': round(float(cast(float, gu['cum'][0])), 2) if gu.height else None,
            'dn_streak': int(cast(int, gd['n'][0])) if gd.height else 0,
            'dn_cum': round(float(cast(float, gd['cum'][0])), 2) if gd.height else None,
            'bad_month_n': bad.height,
            'bad_month_gap_n': bad.filter(pl.col('span') > 45).height,
            'max_month': top_bad['ym'][0] if bad.height else None,
            'max_month_ret': round(float(cast(float, top_bad['rm'][0])), 2) if bad.height else None,
            'max_month_span': int(cast(int, top_bad['span'][0])) if bad.height else None,
        })

    df = pl.DataFrame(rows).join(names, on='ts_code', how='left').with_columns(
        (pl.col('bad_month_n') > 0).alias('in_anomaly_50pp'),
        pl.col('ts_code').str.slice(0, 3).alias('segment'))
    cls, why = [], []
    for r in df.to_dicts():
        c, reason = classify(r)
        cls.append(c)
        why.append(reason)
    result = (df.with_columns(pl.Series('class', cls), pl.Series('reason', why))
              .select(['ts_code', 'name', 'segment', 'class', 'reason', 'rows', 'start', 'end',
                       'close_min', 'close_max', 'int_frac', 'max_abs_daily', 'jump_days_15',
                       'jump_date', 'up_streak', 'up_cum', 'dn_streak', 'dn_cum',
                       'max_gap_days', 'corr_mkt', 'bad_month_n', 'bad_month_gap_n',
                       'max_month', 'max_month_ret', 'max_month_span', 'in_anomaly_50pp'])
              .sort(['class', 'ts_code']))
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    result.write_csv(args.out)
    print(result.group_by('class').len().sort('class'))
    print('in_anomaly_50pp:', result.filter(pl.col('in_anomaly_50pp')).height)
    print('CSV:', args.out)


if __name__ == '__main__':
    main()
