"""正式因子面板构建入口。

把本地正式表（valuation_daily / income / balance / cashflow /
stock_shares / industry_pit_sw）合成为一张长面板，写入
``data/local_store/factors/factor_panel.parquet``。

CLI：
    python tools/factor_panel.py --dry-run           # 只打印行数/因子覆盖
    python tools/factor_panel.py --apply --date 2026-11-19   # 单日信号日
    python tools/factor_panel.py --apply             # 全量（valuation_daily 驱动）

输出 schema：
    ts_code / signal_date / ep / bp / dy / b_ocf_ttm / roe_ttm / accruals
    / gross_margin / industry_l1 / industry_l1_q (行业内 ep 分位示例) / ...

所有财务字段均按 ``ann_date`` PIT 口径，绝不按 ``end_date`` 对齐。
"""
from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import polars as pl
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]

# ---- 正式表路径（全量本地权威，无 staging 依赖） ----
VALUATION = ROOT / 'data/local_store/meta/financials/valuation_daily.parquet'
INCOME = ROOT / 'data/local_store/meta/financials/income.parquet'
BALANCE = ROOT / 'data/local_store/meta/financials/balance.parquet'
CASHFLOW = ROOT / 'data/local_store/meta/financials/cashflow.parquet'
INDUSTRY_PIT = ROOT / 'data/local_store/meta/industry_pit/industry_pit_sw.parquet'

OUT_PATH = ROOT / 'data/local_store/factors/factor_panel.parquet'

# ---- 因子字段注册 ----
# (输出列名, 计算公式描述, 来源表列表)
FACTOR_SPEC = [
    ('ep',          '1 / pe (pe > 0)',                      ['valuation']),
    ('bp',          '1 / pb (pb > 0)',                      ['valuation']),
    ('dy',          'dv_ratio 直接取（估值快照）',           ['valuation']),
    ('b_ocf_ttm',   '经营现金流TTM / 总市值',                ['cashflow', 'valuation']),
    ('roe_ttm',     '归母净利TTM / 所有者权益（asof）',      ['income', 'balance']),
    ('accruals',    '(净利单季 − 经营CF单季) / 总资产 asof', ['income', 'cashflow', 'balance']),
    ('gross_margin','gross_profit / revenue * 100（asof）',  ['income']),
]


# ---------------------------------------------------------------------------
# 单季差分 + TTM 滚动（累计财报 → 单季 → 近 4 期和）
# ---------------------------------------------------------------------------

def _cum_to_quarterly(df: pl.DataFrame, value_col: str,
                      period_col: str = 'end_date') -> pl.DataFrame:
    """累计财报列 → 单季值（polars 向量化，跨年自动重置）。

    规则：
    - Q1 (month=3) 直接取原值
    - H1 (month=6) = H1 − Q1 同年
    - 9M (month=9) = 9M − H1 同年
    - 年报 (month=12) = 年报 − 9M 同年
    - 缺一期时 gap 自然为 null，由上游补齐逻辑处理

    返回新增 ``<value_col>_q`` 列的 df。
    """
    work = df.select(['ts_code', period_col, value_col]).with_columns([
        pl.col(period_col).dt.month().alias('month'),
        pl.col(period_col).dt.year().alias('year'),
    ])
    # 同年同代码，上一累计期（month 差 3 且 month 更小）
    prev = work.with_columns([
        pl.col(value_col).shift(1).over(['ts_code', 'year']).alias('_prev_val'),
        pl.col('month').shift(1).over(['ts_code', 'year']).alias('_prev_month'),
    ])
    prev = prev.with_columns(
        pl.when(pl.col('month') == 3)
        .then(pl.col(value_col))          # Q1
        .when((pl.col('month') - pl.col('_prev_month') == 3)
              & pl.col('_prev_val').is_not_null())
        .then(pl.col(value_col) - pl.col('_prev_val'))   # H1 / 9M / 年报
        .otherwise(None)
        .alias(f'{value_col}_q')
    )
    return prev.select(['ts_code', period_col, 'year', 'month', value_col, f'{value_col}_q'])


def ttm_from_quarterly(quarterly: pl.DataFrame, value_col: str,
                       date_col: str = 'ann_date',
                       end_col: str = 'end_date') -> pl.DataFrame:
    """单季值 → TTM 滚动（按 ann_date PIT 取最近 4 期单季求和）。

    输入 quarterly 必须含 ts_code / end_date / ann_date / ``<value_col>_q``。
    返回 ts_code / ann_date / ttm / end_date（TTM 的「最新报告期 end_date」）。
    """
    q = quarterly.filter(pl.col(f'{value_col}_q').is_not_null()).sort(['ts_code', date_col])
    ttm = q.with_columns(
        pl.col(f'{value_col}_q').rolling_sum(window_size=4).over('ts_code').alias('ttm')
    ).select(['ts_code', date_col, 'ttm', end_col])
    return ttm


# ---------------------------------------------------------------------------
# as-of 合并（按 ann_date / trade_date 对齐到信号日面板）
# ---------------------------------------------------------------------------

def asof_merge(panel: pl.DataFrame, factor: pl.DataFrame,
               value_cols: list[str],
               on: str = 'ts_code',
               panel_date: str = 'signal_date',
               factor_date: str = 'ann_date') -> pl.DataFrame:
    """对 panel 按 (ts_code, signal_date) 左连接 factor，取 factor_date <= signal_date 的最近一条。

    polars 原生 ``asof_join`` 按 key 分组 + 最近日期匹配，比 Python 循环快几个数量级。
    factor_date 只作匹配键，不作为值列带出（显式 drop 避免 ``_right`` 后缀冲突）。
    """
    factor = factor.select([on, factor_date, *value_cols]).sort([on, factor_date])
    panel = panel.sort([on, panel_date])
    out = panel.join_asof(
        factor, left_on=panel_date, right_on=factor_date, by=on,
    )
    # join_asof 会把 right 的 date 列带进来（自动加后缀），显式删掉避免累计冲突
    drop = [c for c in out.columns if c.endswith('_right')]
    if drop:
        out = out.drop(drop)
    # 恢复 panel 原顺序（按 signal_date, ts_code）
    return out.sort(['signal_date', 'ts_code'])


# ---------------------------------------------------------------------------
# 主构建流程
# ---------------------------------------------------------------------------

def build_panel(signal_dates: Optional[list[date]] = None) -> pl.DataFrame:
    """读正式表 → 合因子 → 输出长面板。

    signal_dates: 给定则只保留这些日期；None 则全量（valuation_daily 覆盖）。
    """
    logger.info('读估值驱动表 valuation_daily ...')
    val = pl.scan_parquet(VALUATION).select([
        pl.col('ts_code'),
        pl.col('trade_date').cast(pl.Date).alias('signal_date'),
        pl.col('pe').cast(pl.Float64),
        pl.col('pb').cast(pl.Float64),
        pl.col('dv_ratio').cast(pl.Float64),
        pl.col('total_mv').cast(pl.Float64),
    ])
    if signal_dates:
        val = val.filter(pl.col('signal_date').is_in(signal_dates))
    val = val.collect()
    logger.info('valuation_daily 驱动面板：{} 行 / {} 只 / {} 日',
                val.height, val['ts_code'].n_unique(), val['signal_date'].n_unique())

    # ---- 1. 纯估值因子（直接算） ----
    panel = val.with_columns([
        pl.when(pl.col('pe') > 0).then(pl.lit(1.0) / pl.col('pe'))
        .otherwise(None).alias('ep'),
        pl.when(pl.col('pb') > 0).then(pl.lit(1.0) / pl.col('pb'))
        .otherwise(None).alias('bp'),
        pl.col('dv_ratio').alias('dy'),
    ]).drop(['pe', 'pb', 'dv_ratio'])

    # ---- 2. TTM 财务表预计算 ----
    # income
    logger.info('预计算 income 单季/TTM ...')
    inc_raw = pl.read_parquet(INCOME, columns=[
        'ts_code', 'end_date', 'ann_date',
        'parent_net_profit', 'net_profit', 'revenue', 'gross_profit',
    ]).filter(pl.col('ann_date').is_not_null())
    inc_q = _cum_to_quarterly(inc_raw, 'parent_net_profit')
    inc_q = inc_q.join(_cum_to_quarterly(inc_raw, 'net_profit').select(
        ['ts_code', 'end_date', 'net_profit_q']),
        on=['ts_code', 'end_date'], how='left')
    inc_q = inc_q.join(_cum_to_quarterly(inc_raw, 'revenue').select(
        ['ts_code', 'end_date', 'revenue_q']),
        on=['ts_code', 'end_date'], how='left')
    inc_q = inc_q.join(_cum_to_quarterly(inc_raw, 'gross_profit').select(
        ['ts_code', 'end_date', 'gross_profit_q']),
        on=['ts_code', 'end_date'], how='left')
    # 加 ann_date 回关联
    inc_q = inc_q.join(inc_raw.select(['ts_code', 'end_date', 'ann_date']),
                       on=['ts_code', 'end_date'], how='left')
    inc_ttm = ttm_from_quarterly(inc_q, 'parent_net_profit').rename({'ttm': 'parent_net_profit_ttm'})
    net_profit_ttm = ttm_from_quarterly(inc_q, 'net_profit').rename({'ttm': 'net_profit_ttm'})

    # cashflow
    logger.info('预计算 cashflow 单季/TTM ...')
    cf_raw = pl.read_parquet(CASHFLOW, columns=['ts_code', 'end_date', 'ann_date', 'operating_cf'])
    cf_q = _cum_to_quarterly(cf_raw, 'operating_cf')
    cf_q = cf_q.join(cf_raw.select(['ts_code', 'end_date', 'ann_date']),
                     on=['ts_code', 'end_date'], how='left')
    cf_ttm = ttm_from_quarterly(cf_q, 'operating_cf').rename({'ttm': 'operating_cf_ttm'})

    # balance（直接 asof 取，无需差分）
    logger.info('读 balance ...')
    bal = pl.read_parquet(BALANCE, columns=['ts_code', 'end_date', 'ann_date', 'total_assets', 'total_equity'])

    # ---- 3. as-of 合并到面板 ----
    logger.info('asof 合并 income TTM ...')
    panel = asof_merge(panel, inc_ttm, ['parent_net_profit_ttm'],
                       panel_date='signal_date', factor_date='ann_date')
    logger.info('asof 合并 net_profit TTM ...')
    panel = asof_merge(panel, net_profit_ttm, ['net_profit_ttm'],
                       panel_date='signal_date', factor_date='ann_date')
    logger.info('asof 合并 cashflow TTM ...')
    panel = asof_merge(panel, cf_ttm, ['operating_cf_ttm'],
                       panel_date='signal_date', factor_date='ann_date')
    logger.info('asof 合并 balance ...')
    panel = asof_merge(panel, bal, ['total_assets', 'total_equity'],
                       panel_date='signal_date', factor_date='ann_date')

    # ---- 4. 合成 TTM 比率因子 ----
    logger.info('合成 TTM 比率因子 ...')
    panel = panel.with_columns([
        # b_ocf_ttm = 经营现金流TTM / 总市值（total_mv 单位是万元，operating_cf_ttm 是元）
        (pl.col('operating_cf_ttm') / (pl.col('total_mv') * 1e4)).alias('b_ocf_ttm'),
        # roe_ttm = 归母净利TTM / 所有者权益
        (pl.col('parent_net_profit_ttm') / pl.col('total_equity') * 100.0).alias('roe_ttm'),
    ])

    # ---- 5. accruals + gross_margin（单季口径，需 income_q/cashflow_q asof 最近一期）----
    logger.info('asof 合并单季口径 ...')
    # accruals 需要单季值，取 ann_date <= signal_date 的最近一期
    cf_single = cf_q.filter(pl.col('operating_cf_q').is_not_null()).select(
        ['ts_code', 'ann_date', 'operating_cf_q']).rename({'operating_cf_q': 'ocf_q'})
    inc_single = inc_q.filter(pl.col('net_profit_q').is_not_null()).select(
        ['ts_code', 'ann_date', 'net_profit_q', 'gross_profit_q', 'revenue_q'])
    panel = asof_merge(panel, cf_single, ['ocf_q'],
                       panel_date='signal_date', factor_date='ann_date')
    panel = asof_merge(panel, inc_single, ['net_profit_q', 'gross_profit_q', 'revenue_q'],
                       panel_date='signal_date', factor_date='ann_date')
    panel = panel.with_columns([
        # accruals = (Net Income − Operating CF) / Total Assets
        ((pl.col('net_profit_q') - pl.col('ocf_q')) / pl.col('total_assets')).alias('accruals'),
        # gross_margin = gross_profit / revenue * 100
        (pl.col('gross_profit_q') / pl.col('revenue_q') * 100.0).alias('gross_margin'),
    ])

    # ---- 6. 行业 PIT ----
    logger.info('asof 合并行业 PIT ...')
    ind = pl.read_parquet(INDUSTRY_PIT, columns=[
        'ts_code', 'industry_l1', 'in_date', 'out_date'])
    # 把行业区间展开为每日映射（in_date ≤ signal_date < out_date）
    # 用 conditional asof_join：取 in_date <= signal_date 的最近一条，再校验 out_date
    ind_joinable = ind.with_columns(pl.col('in_date').alias('_id_anchor')).sort(['ts_code', 'in_date'])
    panel = panel.sort(['ts_code', 'signal_date'])
    panel = panel.join_asof(
        ind_joinable.select(['ts_code', 'in_date', 'out_date', 'industry_l1']),
        left_on='signal_date', right_on='in_date', by='ts_code',
    )
    # 校验 out_date（不是 null 且 < signal_date → 置空）
    panel = panel.with_columns(
        pl.when(pl.col('out_date').is_null()
                | (pl.col('out_date') >= pl.col('signal_date')))
        .then(pl.col('industry_l1'))
        .otherwise(None).alias('industry_l1')
    ).drop(['out_date'])
    panel = panel.sort(['signal_date', 'ts_code'])

    # ---- 7. 行业内分位示例（ep 行业内分位，用于 PIT 分层列示范）----
    logger.info('计算行业内 ep 分位 ...')
    panel = panel.with_columns([
        pl.col('ep').rank().over(['signal_date', 'industry_l1']).alias('_ep_rank'),
        pl.col('ep').count().over(['signal_date', 'industry_l1']).alias('_ep_n'),
    ]).with_columns(
        (pl.col('_ep_rank') / pl.col('_ep_n')).alias('ep_ind_pct')
    ).drop(['_ep_rank', '_ep_n'])

    # ---- 8. 只保留输出列 ----
    keep = ['ts_code', 'signal_date',
            'ep', 'bp', 'dy',
            'b_ocf_ttm', 'roe_ttm', 'accruals', 'gross_margin',
            'industry_l1', 'ep_ind_pct']
    out = panel.select([c for c in keep if c in panel.columns])
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description='正式因子面板构建')
    ap.add_argument('--apply', action='store_true', help='写入 parquet（默认 dry-run）')
    ap.add_argument('--dry-run', action='store_true', help='只打印 stats（不改盘）')
    ap.add_argument('--date', type=str, default=None, help='单日信号日 YYYY-MM-DD')
    ap.add_argument('--start', type=str, default=None, help='起始信号日 YYYY-MM-DD')
    ap.add_argument('--end', type=str, default=None, help='结束信号日 YYYY-MM-DD')
    args = ap.parse_args()

    signal_dates: Optional[list[date]] = None
    if args.date:
        signal_dates = [date.fromisoformat(args.date)]
    elif args.start and args.end:
        lo = date.fromisoformat(args.start)
        hi = date.fromisoformat(args.end)
        signal_dates = [lo + timedelta(days=i)
                        for i in range((hi - lo).days + 1)]

    logger.info('开始构建：dry_run={} apply={} dates={}',
                args.dry_run, args.apply,
                len(signal_dates) if signal_dates else '全量')

    t0 = datetime.now()
    panel = build_panel(signal_dates)
    dt = (datetime.now() - t0).total_seconds()

    logger.info('=== 构建完成（{:.1f}s）===', dt)
    logger.info('行数={} 代码={} 日期={}',
                panel.height,
                panel['ts_code'].n_unique(),
                panel['signal_date'].n_unique())
    logger.info('列：{}', panel.columns)

    # 各因子覆盖率（非空占比）
    total = panel.height
    for col in panel.columns:
        if col in ('ts_code', 'signal_date', 'industry_l1'):
            continue
        n_null = panel[col].null_count()
        n_non = total - n_null
        pct = n_non / total * 100 if total else 0
        logger.info('  {}: {} / {} ({:.1f}%)', col, n_non, total, pct)

    if args.apply:
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        panel.write_parquet(OUT_PATH, compression='zstd')
        logger.info('已写入：{}', OUT_PATH)
        # 写 manifest
        manifest = {
            'table': 'factor_panel',
            'rows': panel.height,
            'cols': panel.columns,
            'created_at': datetime.now().isoformat(),
            'sources': [
                'meta/financials/valuation_daily.parquet',
                'meta/financials/income.parquet',
                'meta/financials/balance.parquet',
                'meta/financials/cashflow.parquet',
                'meta/industry_pit/industry_pit_sw.parquet',
            ],
            'note': '日频因子面板；财报按 ann_date PIT，估值按 trade_date asof',
        }
        mf = OUT_PATH.with_suffix('.manifest.json')
        mf.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
        logger.info('manifest：{}', mf)
    else:
        logger.info('dry-run：未写盘（加 --apply 生效）')


if __name__ == '__main__':
    main()
