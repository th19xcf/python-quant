#!/usr/bin/env python3

r"""A 股市场状态向量日频表（``state_vector_daily``）。

只做特征层：六个维度各给原值 + 0~100 滚动分位，不涉回测、不做相似态检索。

六个维度（详见 manifest 的 ``dimensions``）
==========================================
1. 估值位 ``val_earn_yield``：全 A 整体法盈利收益率 = Σ净利润TTM / Σ总市值（%）。
   本地**没有**全 A 整体法 PE 表，故由 ``meta/financials/valuation_daily.parquet``
   逐只 ``pe``（TTM）反推净利润再整体相加（降级口径，逐条写在 manifest）。
   2026-11-11 起（``state_vector_v2``）对每日横截面的 ``ey = 1/pe`` 先做
   ``VAL_EY_WINSOR_LOWER`` / ``VAL_EY_WINSOR_UPPER`` 分位缩尾再加权聚合：
   ``earn_yield = Σ(total_mv × clip(1/pe, q_lo, q_hi)) / Σtotal_mv × 100``。
   缩尾只截断「近零 pe」造成的极端 ``1/pe``（例如 pe=0.01 时 ey=100），
   不剔除亏损股（``pe<0`` 的负收益仍按原值参与），也不改变覆盖率门槛。
   未缩尾的旧口径保留在 ``val_earn_yield_unwinsor`` 列供对照，主列语义只表示缩尾口径。
2. 情绪位 ``sent_ff_turnover``：全市场自由流通换手率 = Σ成交额 / Σ自由流通市值（%）。
   自由流通股本取 ``meta/factors/stock_shares.parquet`` 的 ``free_float_share_filled``，
   三态（raw / ffill_250d / excluded）分别处理，见 ``FREE_FLOAT_RULE``。
3. 宽度位 ``breadth_ma20``：``meta/breadth/breadth_ma_daily.parquet`` 的 ``pct_above_ma20``。
4. 杠杆位 ``lev_margin_buy_ratio``：沪深两市融资买入额 / 沪深两市成交额（%）。
5. 趋势位 ``trend_er20``：沪深300 的 20 日考夫曼效率系数（0~1，只度量位移效率）。
6. 风格位 ``style_large_small_20d``：沪深300 与中证1000 的 20 日收益率之差（pp）。

分位
====
滚动 ``QUANTILE_WINDOW``（1250 交易日 ≈ 5 年）窗口内「有效值 ≤ 当日值」的占比 ×100。
窗口内有效值不足 ``QUANTILE_WINDOW`` 个即写 null，**不插值、不向前填充、不退回全样本**。
窗口长度的敏感性（750 日 / 扩张窗口对照）在 ``--apply`` 时算进 manifest。

产物
====
- ``data/local_store/meta/state/state_vector_daily.parquet``
- ``data/local_store/meta/state/manifest.json``

候选先写 ``_staging/state_vector_<YYYYMMDD>/``（``parts/`` 逐块、``progress.csv`` 断点、
``failed.csv`` 失败清单），``--apply`` 才原子替换正式路径。

用法::

    .venv\\Scripts\\python.exe tools\\build_state_vector.py            # 只构建候选
    .venv\\Scripts\\python.exe tools\\build_state_vector.py --apply    # 构建 + 落正式路径
    .venv\\Scripts\\python.exe tools\\build_state_vector.py --rebuild  # 忽略断点重算
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import polars as pl
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STORE = ROOT / 'data' / 'local_store'

#: 口径版本；任一口径项变化都必须 +1（写进 manifest，不写进文件名）
METHOD_VERSION = 'state_vector_v2'

#: 估值位缩尾参数：逐日横截面对 ``ey = 1/pe`` 做分位缩尾（1% / 99%）
#: 目的：pe 趋近 0 时 ``1/pe`` 会放大到 ±数十倍，单只票即可支配整体法分子；
#: 缩尾只截断这些极端值，不剔除亏损股、不改覆盖率门槛（2026-11-11 用户裁定「切」）
VAL_EY_WINSOR_LOWER = 0.01
VAL_EY_WINSOR_UPPER = 0.99
#: 口径变更影响显著阈值（pp）：|缩尾 − 未缩尾| ≥ 该值的交易日打 ``val_earn_yield_winsor``
VAL_WINSOR_FLAG_PP = 0.5

#: 滚动分位窗口（有效交易日）≈ 5 年
QUANTILE_WINDOW = 1250
#: 窗口内至少要这么多个有效值才给分位（= 满窗，不足即 null）
QUANTILE_MIN_COUNT = QUANTILE_WINDOW
#: 敏感性对照窗口（只写进 manifest，不落列）
SENSITIVITY_WINDOWS = (750,)
#: 扩张窗口对照的最小样本
EXPANDING_MIN_COUNT = 250

#: 效率系数 / 风格差的回看窗口
ER_WINDOW = 20

#: 估值位的最低 pe 覆盖率：低于此比例说明当日不是「全 A」，该维置 null（2011-01~04 的换源爬坡期）
VAL_MIN_PE_COVERAGE = 0.80

#: 已知需剔除的盈利收益率跳变日（2018-01-02 为 local→akshare 换源；2015-01-05 为已知单日跳变）
KNOWN_SWITCH_DATES = ('2015-01-05', '2018-01-02')

#: 分块大小（按文件数）
DEFAULT_CHUNK_SIZE = 500

_PRICE_COLUMNS = ['date', 'ts_code', 'close', 'volume', 'amount']
_SHARE_COLUMNS = ['ts_code', 'effective_date', 'free_float_share_filled', 'free_float_source']

_PART_COLUMNS = [
    'date',
    'amount_all', 'amount_shsz', 'amount_ff',
    'ff_mv', 'ff_mv_ffill', 'ff_mv_raw',
    'n_ff', 'n_bar',
]

_OUTPUT_COLUMNS = [
    'date',
    'val_earn_yield', 'val_earn_yield_q',
    'valuation_erp', 'valuation_erp_q',
    'sent_ff_turnover', 'sent_ff_turnover_q',
    'breadth_ma20', 'breadth_ma20_q',
    'lev_margin_buy_ratio', 'lev_margin_buy_ratio_q',
    'trend_er20', 'trend_er20_q',
    'style_large_small_20d', 'style_large_small_20d_q',
    'sent_ff_coverage', 'sent_ff_ffill_share',
    'val_n_codes', 'val_pe_coverage', 'sent_n_codes',
    'dims_available', 'dims_q_available',
    'val_earn_yield_unwinsor', 'valuation_erp_unwinsor',
    'quality_flags',
]

#: 自由流通三态处理口径（manifest 与文档共用这一段字符串）
FREE_FLOAT_RULE = {
    'raw': '源端直接披露的自由流通股本，原样计入分母，不做事后修正',
    'ffill_250d': '该记录为空、上一非空记录在 250 个交易日内，沿用上一非空值；'
                  '计入分母但在 sent_ff_ffill_share 中单独披露其占自由流通市值的比重，'
                  '用于评估陈旧股本的污染幅度',
    'excluded': '无前一非空值或超过 250 交易日，free_float_share_filled 为空；'
                '该标的当日**同时**从分子（成交额）与分母（自由流通市值）中剔除，'
                '即本维口径为「自由流通股本可得标的的自由流通换手率」，不用流通市值或总市值顶替',
}

_DIM_META = [
    ('val', 'val_earn_yield', '估值位：全A整体法盈利收益率(%)'),
    ('sent', 'sent_ff_turnover', '情绪位：全市场自由流通换手率(%)'),
    ('breadth', 'breadth_ma20', '宽度位：站上 MA20 的个股占比(%)'),
    ('lev', 'lev_margin_buy_ratio', '杠杆位：两市融资买入额/两市成交额(%)'),
    ('trend', 'trend_er20', '趋势位：沪深300 20日考夫曼效率系数(0~1)'),
    ('style', 'style_large_small_20d', '风格位：沪深300 − 中证1000 20日收益率差(pp)'),
]

#: 附带列（同样给滚动分位，但不计入 dims_available）：估值位的第二种表述 ERP
_EXTRA_Q_META = [
    ('val_erp', 'valuation_erp', '估值位附属：ERP = 盈利收益率 − 10Y 国债收益率(pp)'),
]

_Q_META = _DIM_META + _EXTRA_Q_META

#: 数据质量标记（供相似态检索剔除异常日）；null 表示当日无已知质量问题
QUALITY_FLAGS = {
    'val_pe_coverage_low': '当日估值表无数据或 pe 覆盖率 < 80%，整体法盈利收益率不可比，'
                           '该维与 ERP 均置 null（2011-01-04~2011-04-20 换源爬坡期整段命中）',
    'val_source_switch': '盈利收益率存在单日口径跳变的换源日：自动派生（当日多数个股 pe 来源与上一'
                         '交易日不同）∪ 已知跳变日 2015-01-05 / 2018-01-02。原值保留不调整，'
                         '但相似态检索应剔除',
    'val_earn_yield_winsor': '估值位口径变更标记：2026-11-11 起 val_earn_yield 改为逐日 ey=1/pe '
                             '1%/99% 缩尾口径（state_vector_v2）。该标记只落在「缩尾使当日值与'
                             '未缩尾旧口径相差 ≥ 0.5pp」的交易日，提示该日估值位读数对口径选择敏感；'
                             '原始数值见对照列 val_earn_yield_unwinsor',
}


# ----------------------------------------------------------------------
# 路径与工具
# ----------------------------------------------------------------------
def staging_dir(day: date | None = None) -> Path:
    return ROOT / '_staging' / f'state_vector_{(day or date.today()):%Y%m%d}'


def target_path(store: Path) -> Path:
    return store / 'meta' / 'state' / 'state_vector_daily.parquet'


def manifest_path(store: Path) -> Path:
    return store / 'meta' / 'state' / 'manifest.json'


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for block in iter(lambda: handle.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def _write_json(payload: dict[str, Any], path: Path) -> None:
    tmp = path.with_name(f'.{path.name}.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str),
                   encoding='utf-8')
    tmp.replace(path)


# ----------------------------------------------------------------------
# 分块扫个股：成交额、自由流通市值
# ----------------------------------------------------------------------
def _read_chunk(
    files: Iterable[Path], failed: list[tuple[str, str]],
) -> pl.DataFrame | None:
    frames: list[pl.DataFrame] = []
    for path in files:
        try:
            frames.append(pl.read_parquet(path, columns=_PRICE_COLUMNS))
        except Exception as exc:  # noqa: BLE001 - 单只坏文件不应中断全量
            failed.append((path.name.removesuffix('.parquet'), f'{type(exc).__name__}: {exc}'))
    if not frames:
        return None
    return pl.concat(frames, how='vertical_relaxed')


def load_shares(store: Path) -> pl.DataFrame:
    """PIT 自由流通股本（区间事实，靠 join_asof 展开到日）。"""
    shares = pl.read_parquet(
        store / 'meta' / 'factors' / 'stock_shares.parquet', columns=_SHARE_COLUMNS)
    shares = shares.with_columns(pl.col('effective_date').cast(pl.Date))
    return shares.sort(['ts_code', 'effective_date'])


def process_chunk(frame: pl.DataFrame, shares: pl.DataFrame) -> pl.DataFrame:
    """一个块 → 逐日成交额 / 自由流通市值汇总（未跨块合并）。"""
    frame = frame.with_columns(pl.col('date').cast(pl.Date))
    frame = frame.filter(
        pl.col('volume').is_not_null() & (pl.col('volume') > 0)
        & pl.col('amount').is_not_null() & (pl.col('amount') > 0)
        & pl.col('close').is_not_null() & (pl.col('close') > 0)
    )
    if frame.height == 0:
        return _empty_part()

    frame = frame.sort(['ts_code', 'date']).join_asof(
        shares, left_on='date', right_on='effective_date',
        by='ts_code', strategy='backward')

    frame = frame.with_columns([
        (pl.col('free_float_share_filled').cast(pl.Float64) * pl.col('close'))
        .alias('ffc_mv'),
        pl.col('ts_code').str.ends_with('.BJ').alias('_is_bj'),
    ])
    frame = frame.with_columns([
        pl.when(pl.col('ffc_mv') > 0).then(pl.col('ffc_mv')).otherwise(None).alias('ff_mv'),
        pl.when(pl.col('ffc_mv') > 0).then(pl.col('amount')).otherwise(None).alias('_amt_ff'),
        pl.when((pl.col('ffc_mv') > 0) & (pl.col('free_float_source') == 'ffill_250d'))
        .then(pl.col('ffc_mv')).otherwise(0.0).alias('ff_mv_ffill'),
        pl.when((pl.col('ffc_mv') > 0) & (pl.col('free_float_source') == 'raw'))
        .then(pl.col('ffc_mv')).otherwise(0.0).alias('ff_mv_raw'),
        pl.when(~pl.col('_is_bj')).then(pl.col('amount')).otherwise(0.0).alias('amount_shsz'),
    ])
    return frame.group_by('date').agg([
        pl.col('amount').sum().alias('amount_all'),
        pl.col('amount_shsz').sum().alias('amount_shsz'),
        pl.col('_amt_ff').sum().alias('amount_ff'),
        pl.col('ff_mv').sum().alias('ff_mv'),
        pl.col('ff_mv_ffill').sum().alias('ff_mv_ffill'),
        pl.col('ff_mv_raw').sum().alias('ff_mv_raw'),
        pl.col('ff_mv').is_not_null().sum().alias('n_ff'),
        pl.len().alias('n_bar'),
    ]).sort('date')


def _empty_part() -> pl.DataFrame:
    schema = {name: (pl.Date if name == 'date' else pl.Float64) for name in _PART_COLUMNS}
    for name in ('n_ff', 'n_bar'):
        schema[name] = pl.Int64
    return pl.DataFrame(schema=schema)


# ----------------------------------------------------------------------
# 其余五维
# ----------------------------------------------------------------------
def build_calendar(store: Path) -> list[date]:
    """交易日历 = 个股行情里实际出现过的全部日期（升序去重），与 breadth_ma 同口径。"""
    glob = str(store / 'quotes' / 'stock' / '**' / '*.parquet')
    # 只取 date：投影子集读取，多余列不应让日历构建失败
    frame = (pl.scan_parquet(glob, extra_columns='ignore')
             .select(pl.col('date').cast(pl.Date))
             .unique().sort('date').collect())
    return frame['date'].to_list()


def load_valuation(store: Path) -> pl.DataFrame:
    """逐日汇总估值表 → 整体法盈利收益率（逐日 ey 缩尾口径 + 未缩尾对照）。

    ``earnings_ttm = total_mv / pe``；只用 ``pe`` 非空的行，且分子分母取同一集合，
    避免「市值算进来、利润没算」的错配。``pe`` 含负值（亏损股），整体法按原值累加，
    不做剔除——剔除负值会系统性地把整体估值压低。

    缩尾：逐日在横截面上对 ``ey = 1/pe`` 取 ``VAL_EY_WINSOR_LOWER`` /
    ``VAL_EY_WINSOR_UPPER`` 分位，把 ``ey`` 截断到 ``[q_lo, q_hi]`` 后按总市值加权：
    ``earn_w = total_mv × clip(ey, q_lo, q_hi)``，主列用 ``Σearn_w / Σtotal_mv``。
    未缩尾的 ``Σ(total_mv/pe) / Σtotal_mv`` 写进对照列，供新旧口径逐日比较。
    """
    base = (pl.scan_parquet(store / 'meta' / 'financials' / 'valuation_daily.parquet')
            .select(['ts_code', 'trade_date', 'pe', 'total_mv', 'circ_mv'])
            .filter(pl.col('pe').is_not_null() & (pl.col('pe') != 0)
                    & pl.col('total_mv').is_not_null())
            .with_columns([
                (pl.col('total_mv') / pl.col('pe')).alias('earn'),
                (1.0 / pl.col('pe')).alias('ey'),
                (pl.col('pe').abs() < 1.0).alias('_near_zero_pe'),
            ]))
    wins = base.with_columns([
        pl.col('ey').quantile(VAL_EY_WINSOR_LOWER).over('trade_date').alias('_ey_lo'),
        pl.col('ey').quantile(VAL_EY_WINSOR_UPPER).over('trade_date').alias('_ey_hi'),
    ]).with_columns(
        (pl.col('total_mv') * pl.col('ey').clip(pl.col('_ey_lo'), pl.col('_ey_hi')))
        .alias('earn_w'))
    frame = (wins.group_by('trade_date')
             .agg([
                 pl.col('total_mv').sum().alias('mv'),
                 pl.col('earn').sum().alias('earn'),
                 pl.col('earn_w').sum().alias('earn_w'),
                 pl.col('_near_zero_pe').sum().alias('n_near_zero_pe'),
                 pl.len().alias('n'),
             ])
             .sort('trade_date')
             .rename({'trade_date': 'date'}))
    rows = (pl.scan_parquet(store / 'meta' / 'financials' / 'valuation_daily.parquet')
            .group_by('trade_date').agg(pl.len().alias('n_row'))
            .rename({'trade_date': 'date'}))
    # 覆盖率诊断用的流通市值不受 pe 过滤影响（pe 缺失的标的同样有流通市值）
    circ = (pl.scan_parquet(store / 'meta' / 'financials' / 'valuation_daily.parquet')
            .select(['trade_date', 'circ_mv'])
            .filter(pl.col('circ_mv').is_not_null())
            .group_by('trade_date').agg(pl.col('circ_mv').sum().alias('circ_mv'))
            .rename({'trade_date': 'date'}))
    # 逐只 pe 的来源分段：source 列有两种写法（'pe=xxx|pb=...' 与裸来源名），统一抽 pe 段
    pe_src = (pl.scan_parquet(store / 'meta' / 'financials' / 'valuation_daily.parquet')
              .select(['trade_date', 'source', 'pe'])
              .filter(pl.col('pe').is_not_null())
              .pipe(_pe_source_column)
              .group_by(['trade_date', 'pe_src']).agg(pl.len().alias('n_src'))
              .sort(['trade_date', 'n_src', 'pe_src'], descending=[False, True, False])
              .group_by('trade_date', maintain_order=True)
              .agg(pl.col('pe_src').first().alias('pe_src'))
              .rename({'trade_date': 'date'}))
    return (frame.collect().join(rows.collect(), on='date', how='left')
            .join(circ.collect(), on='date', how='left')
            .join(pe_src.collect(), on='date', how='left'))


def _pe_source_column(frame: pl.LazyFrame) -> pl.LazyFrame:
    """从 ``source`` 抽出 pe 的来源名（无 ``pe=`` 前缀时整串即来源）。"""
    return frame.with_columns(
        pl.when(pl.col('source').str.starts_with('pe='))
        .then(pl.col('source').str.extract(r'pe=([^|]*)', 1))
        .otherwise(pl.col('source'))
        .alias('pe_src'))


def load_margin(store: Path) -> pl.DataFrame:
    frame = pl.read_parquet(
        store / 'meta' / 'macro' / 'cn_market_margin_daily.parquet',
        columns=['date', 'market', 'fin_buy'])
    return frame.group_by('date').agg(
        pl.col('fin_buy').sum().alias('fin_buy'),
        pl.col('market').n_unique().alias('n_market'),
    ).sort('date')


def load_index(store: Path, code: str) -> pl.DataFrame:
    frame = pl.read_parquet(
        store / 'quotes' / 'index' / 'sh' / f'{code}.parquet', columns=['date', 'close'])
    frame = frame.with_columns(pl.col('date').cast(pl.Date))
    return frame.rename({'close': code}).sort('date')


def add_index_features(panel: pl.DataFrame, calendar: pl.DataFrame,
                       store: Path) -> pl.DataFrame:
    """沪深300 效率系数 + 沪深300/中证1000 20 日收益差。"""
    c300 = load_index(store, '000300.SH')
    c1000 = load_index(store, '000852.SH')
    missing = {}
    for name, frame in (('000300.SH', c300), ('000852.SH', c1000)):
        miss = calendar.join(frame, on='date', how='anti').height
        missing[name] = miss

    idx = calendar.join(c300, on='date', how='left').join(c1000, on='date', how='left')
    idx = idx.sort('date').with_columns([
        (pl.col('000300.SH') - pl.col('000300.SH').shift(1)).abs().alias('_d300'),
        (pl.col('000852.SH') - pl.col('000852.SH').shift(1)).abs().alias('_d1000'),
    ])
    idx = idx.with_columns([
        pl.col('_d300').rolling_sum(ER_WINDOW, min_samples=ER_WINDOW).alias('_sum_d300'),
        (pl.col('000300.SH') / pl.col('000300.SH').shift(ER_WINDOW) - 1).alias('_r300'),
        (pl.col('000852.SH') / pl.col('000852.SH').shift(ER_WINDOW) - 1).alias('_r1000'),
        pl.col('000300.SH').shift(ER_WINDOW).alias('_p300_lag'),
    ])
    idx = idx.with_columns([
        pl.when((pl.col('_sum_d300') > 0) & pl.col('_p300_lag').is_not_null())
        .then((pl.col('000300.SH') - pl.col('_p300_lag')).abs() / pl.col('_sum_d300'))
        .otherwise(None).alias('trend_er20'),
        pl.when(pl.col('_r300').is_not_null() & pl.col('_r1000').is_not_null())
        .then((pl.col('_r300') - pl.col('_r1000')) * 100.0)
        .otherwise(None).alias('style_large_small_20d'),
    ])
    idx = idx.select(['date', 'trend_er20', 'style_large_small_20d'])
    logger.info(f'[state_vector] 指数缺口（交易日历有、指数无）：{missing}')
    return panel.join(idx, on='date', how='left')


def attach_dimensions(panel: pl.DataFrame, store: Path) -> pl.DataFrame:
    """把六维原值挂到交易日历上（不做任何填充）。"""
    val = load_valuation(store)
    margin = load_margin(store)
    treasury = pl.read_parquet(
        store / 'meta' / 'macro' / 'cn_treasury_yield_10y.parquet',
        columns=['date', 'y10']).rename({'y10': 'y10_pct'})
    breadth = pl.read_parquet(
        store / 'meta' / 'breadth' / 'breadth_ma_daily.parquet',
        columns=['date', 'pct_above_ma20'])
    breadth = breadth.rename({'pct_above_ma20': 'breadth_ma20'})

    panel = panel.join(val, on='date', how='left')
    panel = panel.join(margin, on='date', how='left').join(breadth, on='date', how='left')
    panel = panel.join(treasury, on='date', how='left')

    panel = panel.with_columns([
        # 整体法盈利收益率(%)：Σ利润 / Σ市值；两处均为正且 pe 覆盖达标才算
        # 主列 = 逐日 ey 缩尾口径（VALID 门槛同旧口径：覆盖率 + 聚合盈利为正）
        pl.when((pl.col('mv') > 0) & (pl.col('earn_w') > 0)
                & (pl.col('n') >= pl.col('n_row') * VAL_MIN_PE_COVERAGE))
        .then(pl.col('earn_w') / pl.col('mv') * 100.0).otherwise(None)
        .alias('val_earn_yield'),
        # 对照列 = 未缩尾旧口径（state_vector_v1），门控与旧口径完全一致，
        # 使新旧差额只来自缩尾这一项，不混入门槛变化
        pl.when((pl.col('mv') > 0) & (pl.col('earn') > 0)
                & (pl.col('n') >= pl.col('n_row') * VAL_MIN_PE_COVERAGE))
        .then(pl.col('earn') / pl.col('mv') * 100.0).otherwise(None)
        .alias('val_earn_yield_unwinsor'),
        pl.col('n').alias('val_n_codes'),
        pl.when(pl.col('n_row') > 0)
        .then(pl.col('n') / pl.col('n_row') * 100.0).otherwise(None)
        .alias('val_pe_coverage'),
    ])
    panel = panel.with_columns([
        pl.when(pl.col('ff_mv') > 0).then(pl.col('sent_amt_ff') / pl.col('ff_mv') * 100.0)
        .otherwise(None).alias('sent_ff_turnover'),
        pl.when(pl.col('ff_mv') > 0)
        .then(pl.col('ff_mv_ffill') / pl.col('ff_mv') * 100.0).otherwise(None)
        .alias('sent_ff_ffill_share'),
        pl.when((pl.col('ff_mv') > 0) & (pl.col('circ_mv') > 0))
        .then(pl.col('ff_mv') / pl.col('circ_mv') * 100.0).otherwise(None)
        .alias('sent_ff_coverage'),
        pl.col('n_ff').alias('sent_n_codes'),
        # 融资买入额 / 沪深两市成交额
        pl.when((pl.col('fin_buy') > 0) & (pl.col('amount_shsz') > 0))
        .then(pl.col('fin_buy') / pl.col('amount_shsz') * 100.0).otherwise(None)
        .alias('lev_margin_buy_ratio'),
    ])
    panel = panel.with_columns([
        # ERP = 盈利收益率 − 10Y 国债收益率（两者都非空才算，国债缺失不填）
        pl.when(pl.col('val_earn_yield').is_not_null() & pl.col('y10_pct').is_not_null())
        .then(pl.col('val_earn_yield') - pl.col('y10_pct')).otherwise(None)
        .alias('valuation_erp'),
        pl.when(pl.col('val_earn_yield_unwinsor').is_not_null()
                & pl.col('y10_pct').is_not_null())
        .then(pl.col('val_earn_yield_unwinsor') - pl.col('y10_pct')).otherwise(None)
        .alias('valuation_erp_unwinsor'),
    ])
    return attach_quality_flags(panel)


def attach_quality_flags(panel: pl.DataFrame) -> pl.DataFrame:
    """标记需要下游剔除的异常日（换源跳变 / 估值覆盖率不足）。

    缺失维本身由 ``dims_available`` / ``dims_q_available`` 表达，这里不重复。
    """
    val_start = panel.filter(pl.col('val_pe_coverage').is_not_null())['date'].min()
    panel = panel.with_columns([
        # 自动派生：当日多数个股的 pe 来源与上一交易日不同
        (pl.col('pe_src') != pl.col('pe_src').shift(1)).fill_null(False).alias('_src_switch'),
        # 已知跳变日：2015-01-05 来源名未变但盈利收益率单日 −3.17%（与当日指数 +3.58% 同向较难分离），
        # 2018-01-02 为 local→akshare 换源日；两天都按用户要求标记，下游可剔除
        pl.col('date').is_in([date.fromisoformat(d) for d in KNOWN_SWITCH_DATES])
        .alias('_known_switch'),
        # pe 覆盖率不可用：当天估值表无行，或覆盖率低于阈值（2011-01-04 起的换源爬坡期）
        ((pl.col('date') >= pl.lit(val_start))
         & (pl.col('val_pe_coverage').is_null()
            | (pl.col('val_pe_coverage') < VAL_MIN_PE_COVERAGE * 100.0)))
        .alias('_coverage_low'),
    ])
    conditions = [
        ('val_source_switch', pl.col('_src_switch') | pl.col('_known_switch')),
        ('val_pe_coverage_low', pl.col('_coverage_low')),
        ('val_earn_yield_winsor',
         pl.col('val_earn_yield').is_not_null()
         & pl.col('val_earn_yield_unwinsor').is_not_null()
         & ((pl.col('val_earn_yield') - pl.col('val_earn_yield_unwinsor')).abs()
            >= VAL_WINSOR_FLAG_PP)),
    ]
    expr = pl.concat_str(
        [pl.when(cond).then(pl.lit(code)).otherwise(None) for code, cond in conditions],
        separator='|', ignore_nulls=True)
    panel = panel.with_columns(
        pl.when(expr.is_null() | (expr == '')).then(None).otherwise(expr).alias('quality_flags'))
    return panel.drop(['_src_switch', '_known_switch', '_coverage_low'])


# ----------------------------------------------------------------------
# 滚动分位
# ----------------------------------------------------------------------
def rolling_quantile(values: np.ndarray, window: int, min_count: int | None = None) -> np.ndarray:
    """滚动分位：窗口内「有效值 ≤ 当日值」的占比 ×100。

    窗口按**有效观测数**回溯 ``window`` 个（不是按自然行数）：某维中间断几天时，
    窗口向前多取几天补齐，不会因为个别缺失把整个窗口判成无效。
    有效样本不足 ``window``（或 ``min_count``）或当日值为空 → NaN（不插值、不向前填充）。
    """
    required = window if min_count is None else min_count
    total = values.shape[0]
    out = np.full(total, np.nan)
    valid_mask = ~np.isnan(values)
    cum_valid = np.cumsum(valid_mask)
    for i in range(total):
        current = values[i]
        if np.isnan(current) or cum_valid[i] < required:
            continue
        # 窗口下界：使 [lo, i] 内恰好含 window 个有效观测
        target = cum_valid[i] - window
        lo = int(np.searchsorted(cum_valid, target, side='right')) if target > 0 else 0
        window_values = values[lo:i + 1]
        valid = window_values[~np.isnan(window_values)]
        out[i] = float((valid <= current).sum()) / valid.size * 100.0
    return out


def add_quantiles(panel: pl.DataFrame) -> pl.DataFrame:
    exprs = []
    for _key, raw_col, _desc in _Q_META:
        values = panel[raw_col].to_numpy().astype(float)
        q = rolling_quantile(values, QUANTILE_WINDOW, QUANTILE_MIN_COUNT)
        # NaN 是「不可用」的载体，必须落成 null；否则下游 is_not_null() 会把 NaN 当有效值
        exprs.append(pl.Series(f'{raw_col}_q', q, dtype=pl.Float64).fill_nan(None))
    panel = panel.with_columns(exprs)
    raw_cols = [raw for _k, raw, _d in _DIM_META]
    q_cols = [f'{raw}_q' for raw in raw_cols]
    panel = panel.with_columns([
        pl.sum_horizontal([pl.col(c).is_not_null().cast(pl.Int32) for c in raw_cols])
        .alias('dims_available'),
        pl.sum_horizontal([pl.col(c).is_not_null().cast(pl.Int32) for c in q_cols])
        .alias('dims_q_available'),
    ])
    return panel.select(_OUTPUT_COLUMNS)


def sensitivity(panel: pl.DataFrame) -> list[dict[str, Any]]:
    """分位窗口长度的敏感性：主窗口 1250 日 vs 750 日 / 扩张窗口。"""
    rows: list[dict[str, Any]] = []
    for _key, raw_col, _desc in _Q_META:
        values = panel[raw_col].to_numpy().astype(float)
        base = panel[f'{raw_col}_q'].to_numpy().astype(float)
        for window in SENSITIVITY_WINDOWS:
            alt = rolling_quantile(values, window, window)
            rows.append(_compare(raw_col, f'rolling_{window}d', base, alt))
        alt = rolling_quantile(values, values.size, EXPANDING_MIN_COUNT)
        rows.append(_compare(raw_col, 'expanding_full_sample', base, alt))
    return rows


def _compare(raw_col: str, label: str, base: np.ndarray, alt: np.ndarray) -> dict[str, Any]:
    mask = ~np.isnan(base) & ~np.isnan(alt)
    if mask.sum() == 0:
        return {'dim': raw_col, 'alt_window': label, 'common_days': 0}
    diff = np.abs(base[mask] - alt[mask])
    sign_agree = float(np.mean(np.sign(base[mask] - 50.0) == np.sign(alt[mask] - 50.0)))
    frame = pl.DataFrame({'a': base[mask], 'b': alt[mask]})
    return {
        'dim': raw_col,
        'alt_window': label,
        'common_days': int(mask.sum()),
        'mean_abs_diff_pp': round(float(diff.mean()), 3),
        'p90_abs_diff_pp': round(float(np.percentile(diff, 90)), 3),
        'max_abs_diff_pp': round(float(diff.max()), 3),
        'spearman': round(float(frame.select(
            pl.corr('a', 'b', method='spearman')).item()), 4),
        'sign_of_centered_agree': round(sign_agree, 4),
    }


# ----------------------------------------------------------------------
# 构建 / 落盘
# ----------------------------------------------------------------------
def _read_progress(path: Path) -> dict[int, dict[str, Any]]:
    if not path.is_file():
        return {}
    done: dict[int, dict[str, Any]] = {}
    with open(path, encoding='utf-8', newline='') as handle:
        for row in csv.DictReader(handle):
            try:
                done[int(row['chunk'])] = row
            except (KeyError, ValueError):
                continue
    return done


def _append_progress(path: Path, row: dict[str, Any]) -> None:
    header = ['chunk', 'part', 'codes', 'rows', 'status', 'elapsed_s', 'updated_at']
    write_header = not path.is_file()
    with open(path, 'a', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def _append_failed(path: Path, rows: list[tuple[str, str]]) -> None:
    if not rows:
        return
    write_header = not path.is_file()
    with open(path, 'a', encoding='utf-8', newline='') as handle:
        writer = csv.writer(handle)
        if write_header:
            writer.writerow(['ts_code', 'reason', 'logged_at'])
        stamp = datetime.now().isoformat(timespec='seconds')
        for code, reason in rows:
            writer.writerow([code, reason, stamp])


def build(store: Path, *, chunk_size: int = DEFAULT_CHUNK_SIZE,
          rebuild: bool = False) -> tuple[pl.DataFrame, Path]:
    from src.data.atomic_write import write_parquet_atomic  # noqa: PLC0415

    stage = staging_dir()
    parts = stage / 'parts'
    parts.mkdir(parents=True, exist_ok=True)
    progress_path = stage / 'progress.csv'
    failed_path = stage / 'failed.csv'

    files = sorted((store / 'quotes' / 'stock').glob('**/*.parquet'))
    if not files:
        raise SystemExit(f'[state_vector] 未找到个股行情: {store / "quotes" / "stock"}')
    if rebuild:
        for stale in parts.glob('part_*.parquet'):
            stale.unlink()
        progress_path.unlink(missing_ok=True)
        failed_path.unlink(missing_ok=True)

    calendar_list = build_calendar(store)
    calendar = pl.DataFrame({'date': calendar_list}, schema={'date': pl.Date})
    logger.info(f'[state_vector] 交易日历 {len(calendar_list)} 天：'
                f'{calendar_list[0]} ~ {calendar_list[-1]}')

    shares = load_shares(store)
    src = shares.filter(pl.col('free_float_share_filled').is_not_null())
    logger.info(f'[state_vector] 自由流通股本 {shares.height} 行，'
                f'三态分布 {src["free_float_source"].value_counts().to_dicts()}')

    chunks = [files[i:i + chunk_size] for i in range(0, len(files), chunk_size)]
    done = _read_progress(progress_path)
    for index, chunk in enumerate(chunks):
        part = parts / f'part_{index:04d}.parquet'
        record = done.get(index)
        if record is not None and record.get('status') == 'ok' and part.is_file():
            logger.info(f'[state_vector] 块 {index} 已完成，跳过（断点续跑）')
            continue
        started = time.time()
        failures: list[tuple[str, str]] = []
        frame = _read_chunk(chunk, failures)
        rows = frame.height if frame is not None else 0
        result = process_chunk(frame, shares) if frame is not None else None
        if result is None or result.height == 0:
            result = _empty_part()
        write_parquet_atomic(result.select(_PART_COLUMNS), part)
        _append_failed(failed_path, failures)
        _append_progress(progress_path, {
            'chunk': index,
            'part': part.name,
            'codes': len(chunk),
            'rows': rows,
            'status': 'ok' if not failures else f'ok_with_{len(failures)}_failed',
            'elapsed_s': f'{time.time() - started:.1f}',
            'updated_at': datetime.now().isoformat(timespec='seconds'),
        })
        logger.info(f'[state_vector] 块 {index + 1}/{len(chunks)}：{len(chunk)} 只 / '
                    f'{rows} 行 / 失败 {len(failures)} 只')

    part_files = sorted(parts.glob('part_*.parquet'))
    if len(part_files) != len(chunks):
        raise SystemExit(f'[state_vector] 分块不足：{len(part_files)}/{len(chunks)}')
    merged = pl.read_parquet(part_files)
    daily = (merged.group_by('date').agg([
        pl.col('amount_all').sum(), pl.col('amount_shsz').sum(), pl.col('amount_ff').sum(),
        pl.col('ff_mv').sum(), pl.col('ff_mv_ffill').sum(), pl.col('ff_mv_raw').sum(),
        pl.col('n_ff').sum(), pl.col('n_bar').sum(),
    ]).sort('date'))

    panel = calendar.join(daily, on='date', how='left')
    panel = panel.with_columns([
        pl.col('amount_all').fill_null(0.0),
        pl.col('amount_shsz').fill_null(0.0),
        pl.col('amount_ff').fill_null(0.0),
        pl.col('ff_mv').fill_null(0.0),
        pl.col('ff_mv_ffill').fill_null(0.0),
        pl.col('ff_mv_raw').fill_null(0.0),
        pl.col('n_ff').fill_null(0),
        pl.col('n_bar').fill_null(0),
    ])
    panel = panel.rename({'amount_ff': 'sent_amt_ff'})
    panel = attach_dimensions(panel, store)
    panel = add_index_features(panel, calendar, store)
    panel = add_quantiles(panel)

    out = stage / 'state_vector_daily.parquet'
    write_parquet_atomic(panel, out)
    logger.info(f'[state_vector] 候选完成 {panel.height} 行 -> {out}')
    return panel, stage


def write_manifest(panel: pl.DataFrame, store: Path, stage: Path, *,
                   chunk_size: int, codes: int, sens: list[dict[str, Any]],
                   caliber_change: dict[str, Any] | None = None) -> Path:
    target = target_path(store)
    q_valid = panel.filter(pl.col('dims_q_available') > 0)
    all_six = panel.filter(pl.col('dims_q_available') == 6)
    dims = []
    for key, raw_col, desc in _Q_META:
        series = panel[raw_col]
        q_series = panel[f'{raw_col}_q']
        first_raw = panel.filter(series.is_not_null())['date'].min()
        first_q = panel.filter(q_series.is_not_null())['date'].min()
        dims.append({
            'key': key,
            'desc': desc,
            'supplementary': key in {k for k, _c, _d in _EXTRA_Q_META},
            'raw_column': raw_col,
            'quantile_column': f'{raw_col}_q',
            'raw_nonnull_rows': int(series.is_not_null().sum()),
            'raw_null_rows': int(series.is_null().sum()),
            'raw_first_date': str(first_raw) if first_raw else None,
            'quantile_first_date': str(first_q) if first_q else None,
        })
    payload = {
        'path': str(target.relative_to(ROOT)).replace('\\', '/'),
        'semantics': 'A 股市场状态向量日频表（六维原值 + 0~100 滚动分位），供相似态回溯作特征输入；'
                     '不做回测、不做检索',
        'method_version': METHOD_VERSION,
        'caliber_change': caliber_change,
        'produced_by': 'tools/build_state_vector.py',
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'rows': panel.height,
        'date_range': [str(panel['date'].min()), str(panel['date'].max())],
        'columns': _OUTPUT_COLUMNS,
        'units': {
            'val_earn_yield': '%（Σ净利润TTM / Σ总市值 × 100；逐日 ey=1/pe 1%/99% 缩尾口径）',
            'val_earn_yield_unwinsor': '%（同式但不对 ey 缩尾，state_vector_v1 旧口径对照）',
            'sent_ff_turnover': '%（Σ成交额 / Σ自由流通市值 × 100）',
            'breadth_ma20': '%（站上 MA20 的个股占比，原表已是 0~100）',
            'lev_margin_buy_ratio': '%（两市融资买入额 / 两市成交额 × 100）',
            'trend_er20': '0~1 无量纲（考夫曼效率系数）',
            'style_large_small_20d': 'pp（沪深300 20日收益率 − 中证1000 20日收益率，×100）',
            'valuation_erp': 'pp（缩尾口径盈利收益率 − 10Y 国债收益率，两者均已对齐为 %）',
            'valuation_erp_unwinsor': 'pp（未缩尾旧口径盈利收益率 − 10Y 国债收益率，仅作对照）',
            '*_q': '0~100 分位（滚动窗口内「有效值 ≤ 当日值」的占比 ×100）',
            'sent_ff_coverage': '%（自由流通市值 / 流通市值，同日同集合）',
            'val_pe_coverage': '%（当日 pe 非空标的数 / 当日估值表标的数）',
            'sent_ff_ffill_share': '%（ffill_250d 贡献的自由流通市值占全部自由流通市值）',
            'dims_available': '当日原值非空的维度数（0~6，只数六维主列，不含 valuation_erp）',
            'dims_q_available': '当日分位非空的维度数（0~6）',
            'quality_flags': '数据质量标记，多标记用 | 连接；null = 当日无已知质量问题',
        },
        'quality_flags': {
            'column': 'quality_flags',
            'usage': 'pl.col("quality_flags").is_null() 即保留；非空行建议在相似态检索中直接排除',
            'values': QUALITY_FLAGS,
            'flagged_days': _flag_spans(panel),
            'counts': {code: int(panel.filter(
                pl.col('quality_flags').str.contains(code, literal=True)).height)
                for code in QUALITY_FLAGS},
        },
        'quantile': {
            'window_trading_days': QUANTILE_WINDOW,
            'window_approx_years': round(QUANTILE_WINDOW / 250.0, 2),
            'min_valid_in_window': QUANTILE_MIN_COUNT,
            'definition': 'pct = (#{窗口内有效值 ≤ 当日值} / #{窗口内有效值}) × 100',
            'insufficient': '窗口内有效值不足 min_valid_in_window → 该维分位写 null，'
                            '并计入 dims_q_available 时为不可用；禁止插值、禁止向前填充、'
                            '禁止退回全样本',
            'why_5y': '5 年覆盖一轮完整的 A 股风格轮动（牛熊各约 2~3 年），窗口再短会被单一'
                      '行情阶段主导，再长则把 2015 年前的结构性变化混进来；敏感性见 sensitivity',
            'sensitivity': sens,
            'sensitivity_read': 'mean_abs_diff_pp 为主窗口与对照窗口在同一交易日上的分位差均值；'
                                'sign_of_centered_agree 为「相对 50 中位线的方向」一致率',
        },
        'dimensions': dims,
        'dimension_sources': {
            'val': {
                'source': 'meta/financials/valuation_daily.parquet',
                'degraded': True,
                'degrade_reason': '本地没有「全 A 整体法 PE」现成表；改为逐只 pe(TTM) 反推 '
                                  'earnings_ttm = total_mv / pe 再整体相加，等价于整体法 PE 的倒数，'
                                  '但依赖逐只 pe 的准确性',
                'formula': 'earn_yield = Σ(total_mv × clip(1/pe, q_lo, q_hi)) / Σtotal_mv × 100；'
                           'q_lo/q_hi 为当日横截面 1%/99% 分位，只对 pe 非空且非 0 的行求和，'
                           '分子分母同集合',
                'winsor': {
                    'target': 'ey = 1/pe（逐日横截面）',
                    'lower_quantile': VAL_EY_WINSOR_LOWER,
                    'upper_quantile': VAL_EY_WINSOR_UPPER,
                    'why': 'pe 趋近 0 时 1/pe 放大到 ±数十倍，单只票即可支配整体法分子；'
                           '2026-11-10 A/B 实测近零 pe（|pe|<1）在 3,357/5,053 天出现，'
                           '平均 9.15 行/天、最多 33 行，贡献 Σearn 均值 −1.41%、最低 −6.68%，'
                           '故对 ey 做分位缩尾，不改覆盖率门槛、不剔除亏损股',
                    'comparison_column': 'val_earn_yield_unwinsor',
                    'comparison_note': '对照列沿用 state_vector_v1 的未缩尾口径与同一门控，'
                                       '主列语义只表示缩尾口径，不得混用',
                    'flag': f'缩尾使当日值与未缩尾口径相差 ≥ {VAL_WINSOR_FLAG_PP}pp 的交易日'
                            f'打 quality_flags=val_earn_yield_winsor',
                },
                'negative_earnings': '亏损股（pe<0）按原值参与累加，不剔除；缩尾只截断 ey 的'
                                     '极端分位，不改变正负号',
                'coverage_guard': f'当日 pe 覆盖率（pe 非空数 / 估值表标的数）低于 '
                                  f'{VAL_MIN_PE_COVERAGE:.0%} 时该维置 null——2011-01-04~2011-04-20 '
                                  f'是 baostock→本地重算的换源爬坡期，pe 覆盖从 0 升到 100%，'
                                  f'此段不构成「全 A」，一律不给值。逐日覆盖率见 val_pe_coverage',
                'pe_source_segments': [
                    '2006-01-04~2010-12-31 baostock:peTTM',
                    '2011-01-04~2014-12-31 local:shares_x_close+fin_ttm',
                    '2015-01-05~2017-12-29 local:shares_x_close+fin_ttm',
                    '2018-01-02~ akshare:stock_value_em（PE TTM）',
                ],
                'pe_source_note': '逐只 pe 是混源字段，按年份分段；整体法聚合后需关注换源日的跳变，'
                                  '跳变幅度见 source_switch_jumps',
                'erp': {
                    'column': 'valuation_erp',
                    'desc': '盈利收益率 − 10Y 国债收益率（两者同为 %，差值单位为 pp）',
                    'source': 'meta/macro/cn_treasury_yield_10y.parquet 的 y10（%）',
                    'formula': 'valuation_erp = val_earn_yield − y10，任一为空即 null',
                    'no_fill': '国债当日无值即 null，不做前填、不用相邻日顶替',
                    'start_rule': '起点取两者交集：盈利收益率起点与国债收益率起点取较晚者，'
                                  '且仍需满 1250 个有效观测才给分位',
                },
            },
            'sent': {
                'source': [
                    'quotes/stock/**/*.parquet（close / volume / amount）',
                    'meta/factors/stock_shares.parquet（free_float_share_filled / free_float_source）',
                    'meta/financials/valuation_daily.parquet（circ_mv，仅用于覆盖率诊断）',
                ],
                'formula': 'sent_ff_turnover = Σamount / Σ(free_float_share_filled × close) × 100',
                'universe': '沪深京全市场，剔除当日无成交（volume<=0 或无 bar）的标的；'
                            '分子分母同集合，不做「市值算进来、成交额不算」的错配',
                'free_float_three_states': FREE_FLOAT_RULE,
                'free_float_ffill_share_note': 'ffill_250d 是上游股本表自带的填充（窗口 250 交易日），'
                                               '不是本表对状态向量做的填充；其污染幅度逐日披露在 '
                                               'sent_ff_ffill_share',
            },
            'breadth': {
                'source': 'meta/breadth/breadth_ma_daily.parquet 的 pct_above_ma20',
                'formula': '直接取原列，不做二次计算',
            },
            'lev': {
                'source': [
                    'meta/macro/cn_market_margin_daily.parquet（fin_buy，单位元，market ∈ {sh,sz}）',
                    'quotes/stock/**/*.parquet（amount，仅沪深两市）',
                ],
                'formula': 'lev_margin_buy_ratio = Σ_market fin_buy / Σ_{SH,SZ} amount × 100',
                'universe': '分母剔除北交所（.BJ），因为两融表只覆盖沪深两市，含 BJ 会稀释比例',
            },
            'trend': {
                'source': 'quotes/index/sh/000300.SH.parquet 的 close',
                'formula': 'ER20 = |P_t − P_{t−20}| / Σ_{i=t−19..t}|P_i − P_{i−1}|',
                'note': '只度量位移效率、不含方向；分母为 0 时写 null',
            },
            'style': {
                'source': ['quotes/index/sh/000300.SH.parquet', 'quotes/index/sh/000852.SH.parquet'],
                'formula': 'style_large_small_20d = (P300_t/P300_{t−20} − 1 − (P1000_t/P1000_{t−20} − 1)) × 100',
                'note': '正值=大盘风格占优，负值=小盘风格占优',
            },
        },
        'source_switch_jumps': _source_switch_jumps(panel),
        'coverage': {
            'rows_total': panel.height,
            'rows_with_any_quantile': q_valid.height,
            'first_date_any_quantile': str(q_valid['date'].min()) if q_valid.height else None,
            'rows_all_six_quantiles': all_six.height,
            'first_date_all_six_quantiles': str(all_six['date'].min()) if all_six.height else None,
            'dims_available_distribution': {
                str(k): v for k, v in panel['dims_available'].value_counts().sort('dims_available')
                .iter_rows()
            },
            'dims_q_available_distribution': {
                str(k): v for k, v in panel['dims_q_available'].value_counts()
                .sort('dims_q_available').iter_rows()
            },
            'last_row': panel.tail(1).to_dicts()[0],
        },
        'gaps': [],  # 由 _gaps(panel) 填
        'rebuild': 'python tools/build_state_vector.py --rebuild --apply',
        'read': 'pl.read_parquet(store/"meta"/"state"/"state_vector_daily.parquet")',
        'notes': [
            '本表不改任何既有正式表，全部输入只读',
            '任一维数据不足时该维原值与分位均为 null，不做插值/向前填充/全样本回退',
            '北交所不进杠杆位分母；自由流通 excluded 的标的在情绪位同时退出分子分母',
            'state_vector_v2（2026-11-11）：估值位主列改为逐日 ey=1/pe 1%/99% 缩尾口径，'
            '分位随之重算；未缩尾旧口径保留在 val_earn_yield_unwinsor / valuation_erp_unwinsor '
            '对照列，主列语义不得回退为未缩尾值',
            'valuation_erp 是估值位的第二种表述（盈利收益率 − 10Y 国债），同样给分位，'
            '但不计入 dims_available / dims_q_available，避免同一维被计两次',
            'quality_flags 非空的行建议在相似态检索中直接排除，含义见 quality_flags.values',
        ],
        'universe': {'quote_files': codes, 'chunk_size': chunk_size},
        'sha256': _sha256(target),
        'bytes': target.stat().st_size,
    }
    payload['gaps'] = _gaps(panel)
    path = manifest_path(store)
    _write_json(payload, path)
    return path


def _flag_spans(panel: pl.DataFrame) -> dict[str, list[dict[str, Any]]]:
    """把每个质量标记的连续交易日合并成区间（供 manifest 直接引用）。"""
    dates = panel['date'].to_list()
    out: dict[str, list[dict[str, Any]]] = {}
    for code in QUALITY_FLAGS:
        hit = panel['quality_flags'].str.contains(code, literal=True).fill_null(False).to_numpy()
        positions = np.flatnonzero(hit)
        spans: list[dict[str, Any]] = []
        if positions.size:
            start = prev = int(positions[0])
            for index in positions[1:]:
                index = int(index)
                if index == prev + 1:
                    prev = index
                    continue
                spans.append({'start': str(dates[start]), 'end': str(dates[prev]),
                              'trading_days': prev - start + 1})
                start = prev = index
            spans.append({'start': str(dates[start]), 'end': str(dates[prev]),
                          'trading_days': prev - start + 1})
        out[code] = spans
    return out


def _source_switch_jumps(panel: pl.DataFrame) -> list[dict[str, Any]]:
    """逐只 pe 换源日的整体 PE 跳变，用于量化混源影响。"""
    frame = panel.filter(pl.col('val_earn_yield').is_not_null())
    frame = frame.with_columns(
        (pl.col('val_earn_yield') / pl.col('val_earn_yield').shift(1) - 1).alias('_chg'))
    dates = ['2011-01-04', '2015-01-05', '2018-01-02']
    out = []
    for item in frame.filter(pl.col('date').is_in(
            [date.fromisoformat(d) for d in dates])).iter_rows(named=True):
        out.append({'date': str(item['date']),
                    'earn_yield_pct': round(item['val_earn_yield'], 4),
                    'day_over_day_change_pct': round(item['_chg'] * 100.0, 3)
                    if item['_chg'] is not None else None})
    return out


def _gaps(panel: pl.DataFrame) -> list[dict[str, Any]]:
    """缺维与待补项：每维的可用区间与不可用天数。"""
    out = []
    for key, raw_col, desc in _Q_META:
        nonnull = panel.filter(pl.col(raw_col).is_not_null())
        nulls = panel.height - nonnull.height
        out.append({
            'dim': key,
            'desc': desc,
            'raw_available_days': nonnull.height,
            'raw_null_days': nulls,
            'raw_first_date': str(nonnull['date'].min()) if nonnull.height else None,
            'raw_last_date': str(nonnull['date'].max()) if nonnull.height else None,
            'quantile_available_days': int(panel[f'{raw_col}_q'].is_not_null().sum()),
        })
    return out


def _caliber_change_report(old: pl.DataFrame, new: pl.DataFrame, *,
                           old_sha256: str | None,
                           old_method_version: str | None) -> dict[str, Any]:
    """新旧口径逐日对照（写进 manifest，作为 DG-11/DG-22 的留证）。

    只在主列 ``val_earn_yield`` 两侧都非空的交易日上统计差值，避免把「门控差异
    造成的 null」混进数值差；null 的增减单独列在 ``null_counts`` / ``null_days``。
    """
    keys = ['date', 'val_earn_yield', 'valuation_erp', 'val_earn_yield_q',
            'valuation_erp_q', 'quality_flags']
    common = [c for c in keys if c in old.columns and c in new.columns]
    joined = old.select(common).join(
        new.select(common), on='date', how='full', suffix='_new', coalesce=True)
    both = joined.filter(pl.col('val_earn_yield').is_not_null()
                         & pl.col('val_earn_yield_new').is_not_null())
    diff = both.select(
        (pl.col('val_earn_yield_new') - pl.col('val_earn_yield')).alias('d'))
    abs_diff = diff.select(pl.col('d').abs())['d']
    signed = diff['d']

    def _q(series: pl.Series, p: float) -> float | None:
        if series.len() == 0:
            return None
        value = series.quantile(p)
        assert isinstance(value, (int, float)), f'分位结果非数值: {type(value)}'
        return round(float(value), 6)

    def _mean(series: pl.Series) -> float | None:
        if series.len() == 0:
            return None
        value = series.mean()
        assert isinstance(value, (int, float)), f'均值结果非数值: {type(value)}'
        return round(float(value), 6)

    # 最大差对应日期：在 both 上直接取
    max_date = (both.with_columns(
        (pl.col('val_earn_yield_new') - pl.col('val_earn_yield')).abs().alias('a'))
        .sort('a', descending=True)['date'][0]) if both.height else None

    qb = joined.filter(pl.col('val_earn_yield_q').is_not_null()
                       & pl.col('val_earn_yield_q_new').is_not_null())
    qdiff = (qb.select((pl.col('val_earn_yield_q_new') - pl.col('val_earn_yield_q'))
                       .alias('q'))['q'])
    qflip = int(((qb['val_earn_yield_q'] - 50.0)
                 * (qb['val_earn_yield_q_new'] - 50.0) < 0).sum())
    eq = joined.filter(pl.col('valuation_erp_q').is_not_null()
                       & pl.col('valuation_erp_q_new').is_not_null())
    eqdiff = eq.select((pl.col('valuation_erp_q_new') - pl.col('valuation_erp_q'))
                       .alias('q'))['q']

    null_counts = {}
    for col in sorted(set(old.columns) | set(new.columns)):
        before = int(old[col].is_null().sum()) if col in old.columns else None
        after = int(new[col].is_null().sum()) if col in new.columns else None
        null_counts[col] = {
            'before': before,
            'after': after,
            'delta': (after - before) if before is not None and after is not None else None,
        }

    def _null_days(col: str) -> dict[str, list[str]]:
        before = set(old.filter(pl.col(col).is_null())['date'].to_list())
        after = set(new.filter(pl.col(col).is_null())['date'].to_list())
        return {'gained_null': sorted(str(d) for d in after - before),
                'lost_null': sorted(str(d) for d in before - after)}

    return {
        'from_method_version': old_method_version,
        'to_method_version': METHOD_VERSION,
        'winsor': {'target': 'ey = 1/pe（逐日横截面）',
                   'lower_quantile': VAL_EY_WINSOR_LOWER,
                   'upper_quantile': VAL_EY_WINSOR_UPPER},
        'rows_before': old.height,
        'rows_after': new.height,
        'sha256_before': old_sha256,
        'common_days': both.height,
        'old_mean_pct': _mean(both['val_earn_yield']),
        'new_mean_pct': _mean(both['val_earn_yield_new']),
        'signed_mean_diff_pp': _mean(signed),
        'abs_mean_diff_pp': _mean(abs_diff),
        'abs_p50_diff_pp': _q(abs_diff, 0.50),
        'abs_p90_diff_pp': _q(abs_diff, 0.90),
        'abs_max_diff_pp': _q(abs_diff, 1.0),
        'abs_max_diff_date': str(max_date) if max_date is not None else None,
        'quantile_impact': {
            'common_days': int(qb.height),
            'val_earn_yield_q_abs_mean_pp': _mean(qdiff.abs()),
            'val_earn_yield_q_abs_p90_pp': _q(qdiff.abs(), 0.90),
            'val_earn_yield_q_abs_max_pp': _q(qdiff.abs(), 1.0),
            'median_line_cross_days': qflip,
            'median_line_cross_rate': (round(qflip / qb.height, 4) if qb.height else None),
            'valuation_erp_q_abs_mean_pp': _mean(eqdiff.abs()),
        },
        'null_counts': null_counts,
        'null_days': {'val_earn_yield': _null_days('val_earn_yield'),
                      'valuation_erp': _null_days('valuation_erp')},
        'flag_counts': {
            'val_earn_yield_winsor': int(new.filter(
                pl.col('quality_flags').fill_null('').str.contains(
                    'val_earn_yield_winsor', literal=True)).height),
        },
    }


def apply(panel: pl.DataFrame, store: Path, stage: Path, *,
          chunk_size: int, codes: int, sens: list[dict[str, Any]]) -> None:
    import sys

    sys.path.insert(0, str(ROOT))
    from src.data.atomic_write import write_parquet_atomic

    target = target_path(store)
    old: pl.DataFrame | None = None
    old_sha: str | None = None
    old_version: str | None = None
    if target.is_file():
        old = pl.read_parquet(target)
        old_sha = _sha256(target)
        if manifest_path(store).is_file():
            try:
                old_version = json.loads(
                    manifest_path(store).read_text(encoding='utf-8')).get('method_version')
            except json.JSONDecodeError:
                old_version = None
        backup_dir = (ROOT / '_backup'
                      / f'state_vector_{datetime.now():%Y%m%d_%H%M%S}')
        backup = backup_dir / 'state_vector_daily.parquet'
        # 备份位唯一化：同一路径被两次快照复用会互相覆盖，已存在即报错（DG-11 执行细则）
        assert not backup_dir.exists(), f'备份目录已存在，禁止复用: {backup_dir}'
        backup.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(target, backup)
        # DG-11：口径变更时把旧 manifest 一并留档，回滚可还原整套证据
        if manifest_path(store).is_file():
            shutil.copy2(manifest_path(store), backup_dir / 'manifest.json')
        logger.info(f'[state_vector] 旧表与旧 manifest 已备份 -> {backup_dir}')

    write_parquet_atomic(panel, target)
    written = pl.read_parquet(target)
    assert written.height == panel.height, '落盘行数与候选不一致'
    assert written.equals(panel), '落盘内容与候选不一致'
    caliber = (_caliber_change_report(old, panel, old_sha256=old_sha,
                                      old_method_version=old_version)
               if old is not None else None)
    path = write_manifest(panel, store, stage, chunk_size=chunk_size, codes=codes,
                          sens=sens, caliber_change=caliber)
    logger.info(f'[state_vector] 已落正式路径 {target}（SHA256 {_sha256(target)[:16]}…）')
    logger.info(f'[state_vector] manifest -> {path}')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='构建 state_vector_daily（六维市场状态向量）')
    parser.add_argument('--store-root', default=str(DEFAULT_STORE))
    parser.add_argument('--chunk-size', type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument('--rebuild', action='store_true', help='忽略断点，清空本次分块重算')
    parser.add_argument('--apply', action='store_true', help='校验通过后原子写正式路径')
    args = parser.parse_args(argv)

    store = Path(args.store_root)
    panel, stage = build(store, chunk_size=args.chunk_size, rebuild=args.rebuild)
    codes = len(sorted((store / 'quotes' / 'stock').glob('**/*.parquet')))
    sens = sensitivity(panel)
    if args.apply:
        apply(panel, store, stage, chunk_size=args.chunk_size, codes=codes, sens=sens)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
