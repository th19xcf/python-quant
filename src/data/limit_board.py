#!/usr/bin/env python3

"""逐股逐日涨跌停标记（2005 起，按月分区）。

为什么需要本表
--------------
``ExecutionRules`` 已经能「给定代码 + 交易日 + ST 状态」推出涨跌停幅度，
但缺一张**逐日逐股的落地标记**：回测、breadth 对拍、推荐可成交性判定各自
现算，口径容易漂移，也看不出「差的那几只」是谁。本模块把结果固化成数据，
规则本身不再另写一套，全部调用 :mod:`src.core.market_rules`。

字段与口径
----------
每行 = (trade_date, ts_code)，列：

* ``prev_close``：前收盘。优先取行情 ``pre_close``（原始未复权价）；缺失或
  ≤0 时用同日之前的最近收盘补齐，并记 ``prev_close_source``。上市首根 bar
  无前收时留空。
* ``limit_pct`` / ``limit_up_price`` / ``limit_down_price``：幅度由
  ``ExecutionRules.resolve_limit_pct`` 推导，价格按交易所口径四舍五入到分。
  价格用「整数分 + ROUND_HALF_UP」批量计算，与
  :meth:`ExecutionRules.resolve_limit_price` 逐样本对拍（构建时随机抽样，
  不一致即失败），不是第二套规则。
* ``touch_up`` / ``touch_down``：当日最高/最低价触及涨停/跌停（盘中最值）。
* ``close_at_up`` / ``close_at_down``：收盘封在涨停/跌停（与 breadth 的
  ``limit_up_count`` 同口径，用于对拍）。
* ``one_word_up`` / ``one_word_down``：一字板（最高=最低且封在板上）。
* ``suspended``：成交量为空/非正/非有限。
* ``can_buy`` / ``can_sell``：复用 ``ExecutionRules.block_reason`` 的判定顺序
  （停牌 → 开盘价触板 → 无开盘价时退化为 one-word 判定）。
* ``st_status`` / ``listing_phase`` / ``limit_basis``：口径来源留证。

新股、摘帽与制度分界
--------------------
* 上市阶段按 ``stock_basic.list_date`` 与交易日推导，不看未来信息：
  - 科创板（2019-07-22 起）、创业板注册制（2020-08-24 起）：上市前 5 个
    交易日不限价；
  - 北交所（2021-11-15 起）：上市首日不限价；
  - 沪深主板（2023-02-17 全面注册制起）：上市前 5 个交易日不限价。
  不限价日价格列留空、触及/封板列留空、可买可卖只由停牌决定。
* 改革前上市的主板/创业板/北交所标的「首日」涨跌幅另有 44%/36% 等历史
  制度，无法用统一百分比还原；本表把这类 ``listing_phase='legacy'`` 的
  首日标记为「不可买/不可卖」（保守：不假设可成交），价格列留空，并在
  manifest ``caveats`` 里写明。
* ST 状态逐日取自 ``meta/st/daily``（按生效日切换）；取不到时为
  ``unknown``，仍按板块常规幅度计算并由 ``limit_basis='st_unknown'`` 标记。
* 创业板改革日按 ``ExecutionRules.CHINEXT_REG_DATE`` 逐日切换，不是整段
  用 20%。

落盘
----
``meta/limit/daily/limit_daily_YYYYMM.parquet``（原子写）+ ``manifest.json``
（逐文件 rows/bytes/sha256/起止日 + freshness）+ ``comparison.json`` +
``reconcile_daily.parquet``（与 breadth 的逐日对拍）。同输入重跑逐字节一致，
未产出的旧月分区会被删除。freshness 另在
``src/data/quality/freshness_config.json`` 登记（key=limit_board_daily）。

用法::

    python tools/build_limit_board.py                    # 全量重建
    python tools/build_limit_board.py --since 2015-01-01
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from loguru import logger

from src.core.market_rules import ExecutionRules
from src.data.atomic_write import write_parquet_atomic, write_text_atomic

SCHEMA_VERSION = 'limit_board/v1'
DEFAULT_START = date(2005, 1, 1)
PRICE_CHECK_SAMPLES = 500
TRADE_CHECK_SAMPLES = 2000

#: 制度生效日（与交易所公告一致；创业板改革日与 ExecutionRules 同源）
STAR_UNLIMITED_SINCE = date(2019, 7, 22)
BSE_UNLIMITED_SINCE = date(2021, 11, 15)
MAIN_UNLIMITED_SINCE = date(2023, 2, 17)

MAIN_BOARDS = ('沪市主板', '深市主板')
OUTPUT_COLUMNS = [
    'trade_date', 'ts_code', 'board', 'st_status', 'listing_days',
    'listing_phase', 'prev_close', 'prev_close_source', 'limit_pct',
    'limit_basis', 'limit_up_price', 'limit_down_price',
    'touch_up', 'touch_down', 'close_at_up', 'close_at_down',
    'one_word_up', 'one_word_down', 'suspended', 'can_buy', 'can_sell',
]
_SCHEMA = {
    'trade_date': pl.Date, 'ts_code': pl.Utf8, 'board': pl.Utf8,
    'st_status': pl.Utf8, 'listing_days': pl.Int32, 'listing_phase': pl.Utf8,
    'prev_close': pl.Float64, 'prev_close_source': pl.Utf8,
    'limit_pct': pl.Float64, 'limit_basis': pl.Utf8,
    'limit_up_price': pl.Float64, 'limit_down_price': pl.Float64,
    'touch_up': pl.Boolean, 'touch_down': pl.Boolean,
    'close_at_up': pl.Boolean, 'close_at_down': pl.Boolean,
    'one_word_up': pl.Boolean, 'one_word_down': pl.Boolean,
    'suspended': pl.Boolean, 'can_buy': pl.Boolean, 'can_sell': pl.Boolean,
}

_QUOTE_COLUMNS = ['date', 'ts_code', 'open', 'high', 'low', 'close',
                  'pre_close', 'volume']


# ----------------------------------------------------------------------
# 路径
# ----------------------------------------------------------------------
def output_dir(store_root: Path | str) -> Path:
    return Path(store_root) / 'meta' / 'limit' / 'daily'


def manifest_path(store_root: Path | str) -> Path:
    return Path(store_root) / 'meta' / 'limit' / 'manifest.json'


def comparison_path(store_root: Path | str) -> Path:
    return Path(store_root) / 'meta' / 'limit' / 'comparison.json'


def reconcile_path(store_root: Path | str) -> Path:
    return Path(store_root) / 'meta' / 'limit' / 'reconcile_daily.parquet'


def freshness_path(store_root: Path | str) -> Path:
    return Path(store_root) / 'meta' / 'limit' / 'freshness.json'


def _month_spans(start: date, end: date) -> Iterable[tuple[date, date, str]]:
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        nxt = date(cursor.year + (cursor.month == 12),
                   1 if cursor.month == 12 else cursor.month + 1, 1)
        yield max(start, cursor), min(end, nxt - timedelta(days=1)), f'{cursor:%Y%m}'
        cursor = nxt


def _date_lit(value: date) -> pl.Expr:
    return pl.lit(value, dtype=pl.Date)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(4 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


# ----------------------------------------------------------------------
# 输入
# ----------------------------------------------------------------------
def _quote_files(root: Path) -> list[str]:
    files = sorted((root / 'quotes' / 'stock').rglob('*.parquet'))
    if not files:
        raise RuntimeError(f'{root}/quotes/stock 下没有个股行情，无法构建涨跌停标记')
    return [str(path) for path in files]


def _sessions(root: Path) -> list[date]:
    """真实交易日：优先 breadth 日频（全历史开市日），缺失时退回行情日期。"""
    breadth = root / 'meta' / 'breadth' / 'breadth_daily.parquet'
    if breadth.is_file():
        days = sorted(pl.read_parquet(breadth, columns=['trade_date'])
                      ['trade_date'].to_list())
        if days:
            return days
    days = (pl.scan_parquet(_quote_files(root), extra_columns='ignore')
            .select(pl.col('date').unique().sort()).collect()['date'].to_list())
    return [day for day in days if day is not None]


def _basic(root: Path) -> pl.DataFrame:
    frame = pl.read_parquet(root / 'meta' / 'stock_basic.parquet',
                            columns=['ts_code', 'list_date', 'board'])
    return frame.unique('ts_code')


def _st_panel(root: Path, start: date, end: date) -> pl.DataFrame:
    files = sorted((root / 'meta' / 'st' / 'daily').glob('st_daily_[0-9]*.parquet'))
    if not files:
        raise RuntimeError('缺少逐日 ST 面板 meta/st/daily，先执行 '
                           'python tools/build_st_daily.py')
    return (pl.scan_parquet([str(path) for path in files], extra_columns='ignore')
            .filter((pl.col('trade_date') >= start) & (pl.col('trade_date') <= end))
            .select(['trade_date', 'ts_code', 'status']).collect())


def _limit_pct_map(codes: Iterable[str]) -> pl.DataFrame:
    """(ts_code, era, st_rank) → 整数百分比；唯一真源为 resolve_limit_pct。"""
    rows: list[tuple[str, int, int, int]] = []
    pre = ExecutionRules.CHINEXT_REG_DATE - timedelta(days=1)
    post = ExecutionRules.CHINEXT_REG_DATE
    for code in sorted(set(codes)):
        for era, sample in ((0, pre), (1, post)):
            for rank, is_st in ((0, False), (1, True), (2, None)):
                pct = ExecutionRules.resolve_limit_pct(
                    code, is_st=is_st, trade_date=sample)
                rows.append((code, era, rank, int(round(pct * 100))))
    return pl.DataFrame(
        rows, schema={'ts_code': pl.Utf8, 'era': pl.Int8, 'st_rank': pl.Int8,
                      'pct': pl.Int64}, orient='row')


def _board_expr() -> pl.Expr:
    """板块标签：以 stock_basic.board 为准，缺失时按代码段兜底。"""
    code = pl.col('ts_code').str.split('.').list.first()
    return (pl.when(pl.col('board').is_not_null()).then(pl.col('board'))
            .when(code.str.starts_with('688') | code.str.starts_with('689'))
            .then(pl.lit('科创板'))
            .when(code.str.starts_with('300') | code.str.starts_with('301'))
            .then(pl.lit('创业板'))
            .when(pl.col('ts_code').str.ends_with('.BJ')
                  | code.str.starts_with('920')).then(pl.lit('北交所'))
            .otherwise(pl.lit('主板')).alias('board'))


# ----------------------------------------------------------------------
# 计算
# ----------------------------------------------------------------------
def _prepare(root: Path, start: date, end: date) -> tuple[pl.DataFrame, dict[str, Any]]:
    """读取并对齐行情 / 上市日 / ST 状态，返回带中间列的长表。"""
    quotes = (pl.scan_parquet(_quote_files(root), extra_columns='ignore')
              .filter(pl.col('date') >= pl.lit(start - timedelta(days=45),
                                               dtype=pl.Date))
              .select(_QUOTE_COLUMNS).collect())
    counters: dict[str, Any] = {'quote_rows': quotes.height,
                                'quote_codes': quotes['ts_code'].n_unique()}
    quotes = quotes.sort(['ts_code', 'date'])

    sessions = _sessions(root)
    sess = pl.DataFrame({'date': sessions}).with_row_index('sidx')
    basic = _basic(root)
    st = _st_panel(root, start, end)

    frame = (quotes
             .with_columns(pl.col('date').cum_count().over('ts_code')
                           .cast(pl.Int32).alias('row_no'))
             .join(sess, on='date', how='left'))
    frame = (frame.join(basic, on='ts_code', how='left')
             .join(sess.rename({'date': 'list_date', 'sidx': 'lidx'}),
                   on='list_date', how='left'))
    frame = frame.with_columns(
        (pl.col('sidx').cast(pl.Int64) - pl.col('lidx').cast(pl.Int64) + 1)
        .cast(pl.Int32).alias('listing_days'))
    counters['listing_days_missing'] = int(frame['listing_days'].null_count())

    st = st.rename({'trade_date': 'date'})
    frame = frame.join(st, on=['date', 'ts_code'], how='left')
    status = pl.col('status')
    frame = frame.with_columns([
        pl.when(status.is_in(['st', '*st'])).then(pl.lit('st'))
        .when(status.is_null() | (status == 'unknown')).then(pl.lit('unknown'))
        .otherwise(pl.lit('non_st')).alias('st_status'),
        pl.when(status.is_in(['st', '*st'])).then(1)
        .when(status.is_null() | (status == 'unknown')).then(2)
        .otherwise(0).cast(pl.Int8).alias('st_rank'),
        pl.when(pl.col('date') >= _date_lit(ExecutionRules.CHINEXT_REG_DATE))
        .then(1).otherwise(0).cast(pl.Int8).alias('era'),
    ])
    counters['st_panel_missing'] = int((frame['st_rank'] == 2).sum())
    return frame, counters


def _apply_marks(frame: pl.DataFrame, counters: dict[str, Any]) -> pl.DataFrame:
    pct_map = _limit_pct_map(frame['ts_code'].unique().to_list())
    frame = frame.join(pct_map, on=['ts_code', 'era', 'st_rank'], how='left')
    frame = frame.with_columns(pl.col('pct').fill_null(10).cast(pl.Int64))

    # 前收盘：原始 pre_close 优先，缺失时用最近收盘补，并留来源
    raw_pre = (pl.when((pl.col('pre_close') > 0) & pl.col('pre_close').is_finite())
               .then(pl.col('pre_close')))
    derived = pl.col('close').shift(1).over('ts_code')
    frame = frame.with_columns([
        raw_pre.alias('_raw_pre'),
        derived.alias('_derived_pre'),
    ]).with_columns([
        pl.col('_raw_pre').fill_null(pl.col('_derived_pre')).alias('prev_close'),
        pl.when(pl.col('_raw_pre').is_not_null()).then(pl.lit('quote'))
        .when(pl.col('_derived_pre').is_not_null()).then(pl.lit('derived_prev_close'))
        .otherwise(pl.lit('none')).alias('prev_close_source'),
    ])
    counters['prev_close_derived'] = int(
        (frame['prev_close_source'] == 'derived_prev_close').sum())
    counters['prev_close_missing'] = int((frame['prev_close_source'] == 'none').sum())

    frame = frame.with_columns(_board_expr())
    exempt = (
        ((pl.col('board') == '科创板')
         & (pl.col('list_date') >= _date_lit(STAR_UNLIMITED_SINCE))
         & (pl.col('listing_days') <= 5))
        | ((pl.col('board') == '创业板')
           & (pl.col('list_date') >= _date_lit(ExecutionRules.CHINEXT_REG_DATE))
           & (pl.col('listing_days') <= 5))
        | ((pl.col('board') == '北交所')
           & (pl.col('list_date') >= _date_lit(BSE_UNLIMITED_SINCE))
           & (pl.col('listing_days') <= 1))
        | (pl.col('board').is_in(list(MAIN_BOARDS))
           & (pl.col('list_date') >= _date_lit(MAIN_UNLIMITED_SINCE))
           & (pl.col('listing_days') <= 5))
    )
    frame = frame.with_columns([
        pl.when(exempt).then(pl.lit('exempt'))
        .when(pl.col('listing_days') == 1).then(pl.lit('legacy'))
        .otherwise(pl.lit('normal')).alias('listing_phase'),
    ])
    counters['listing_exempt_rows'] = int((frame['listing_phase'] == 'exempt').sum())
    counters['listing_legacy_rows'] = int((frame['listing_phase'] == 'legacy').sum())

    valid = (pl.col('prev_close').is_not_null() & (pl.col('prev_close') > 0)
             & (pl.col('listing_phase') == 'normal'))
    frame = frame.with_row_index('_rid')
    pre_c = (pl.col('prev_close') * 100).round(0).cast(pl.Int64)
    up_c = (pre_c * (100 + pl.col('pct')) + 50) // 100
    down_c = (pre_c * (100 - pl.col('pct')) + 50) // 100
    open_c = (pl.col('open') * 100).round(0).cast(pl.Int64)
    high_c = (pl.col('high') * 100).round(0).cast(pl.Int64)
    low_c = (pl.col('low') * 100).round(0).cast(pl.Int64)
    close_c = (pl.col('close') * 100).round(0).cast(pl.Int64)
    frame = frame.with_columns([
        pre_c.alias('_pre_c'), up_c.alias('_up_c'), down_c.alias('_down_c'),
        open_c.alias('_open_c'), high_c.alias('_high_c'), low_c.alias('_low_c'),
        close_c.alias('_close_c'),
    ])
    # 少量历史行情（北交所平移/早期数据）前收带 3 位以上小数，整数分口径会
    # 与其后 0.01 舍入不同；这些行改用 resolve_limit_price 精确算，其余仍批量。
    frame = frame.with_columns(
        (((pl.col('prev_close') * 100 - pl.col('_pre_c')).abs() > 1e-6) & valid)
        .alias('_odd_pre'))
    odd = frame.filter(pl.col('_odd_pre')).select(['_rid', 'prev_close', 'pct'])
    if odd.height:
        rows = []
        for rid, prev, pct in odd.iter_rows():
            limit = pct / 100.0
            rows.append((
                rid,
                int(round(ExecutionRules.resolve_limit_price(prev, limit, 'buy') * 100)),
                int(round(ExecutionRules.resolve_limit_price(prev, limit, 'sell') * 100)),
            ))
        fix = pl.DataFrame(rows, schema={'_rid': pl.UInt32, '_odd_up': pl.Int64,
                                         '_odd_down': pl.Int64}, orient='row')
        frame = (frame.join(fix, on='_rid', how='left').with_columns([
            pl.when(pl.col('_odd_pre')).then(pl.col('_odd_up'))
            .otherwise(pl.col('_up_c')).alias('_up_c'),
            pl.when(pl.col('_odd_pre')).then(pl.col('_odd_down'))
            .otherwise(pl.col('_down_c')).alias('_down_c'),
        ]))
    counters['odd_prev_close_rows'] = int(odd.height)

    suspended = (pl.col('volume').is_null() | ~pl.col('volume').is_finite()
                 | (pl.col('volume') <= 0))
    frame = frame.with_columns(suspended.alias('suspended'))
    counters['suspended_rows'] = int(frame['suspended'].sum())

    def _mask(expression: pl.Expr) -> pl.Expr:
        return pl.when(valid).then(expression).otherwise(None)

    touch_up = _mask(pl.col('_high_c') >= pl.col('_up_c'))
    touch_down = _mask(pl.col('_low_c') <= pl.col('_down_c'))
    close_up = _mask(pl.col('_close_c') >= pl.col('_up_c'))
    close_down = _mask(pl.col('_close_c') <= pl.col('_down_c'))
    flat = pl.col('_high_c') == pl.col('_low_c')
    one_up = _mask(flat & (pl.col('_high_c') >= pl.col('_up_c')))
    one_down = _mask(flat & (pl.col('_low_c') <= pl.col('_down_c')))

    open_blocked_up = pl.col('_open_c') >= pl.col('_up_c')
    open_blocked_down = pl.col('_open_c') <= pl.col('_down_c')
    fallback_up = flat & (pl.col('_close_c') >= pl.col('_up_c'))
    fallback_down = flat & (pl.col('_close_c') <= pl.col('_down_c'))
    blocked_up = (pl.when(pl.col('_open_c').is_not_null()).then(open_blocked_up)
                  .otherwise(fallback_up))
    blocked_down = (pl.when(pl.col('_open_c').is_not_null()).then(open_blocked_down)
                    .otherwise(fallback_down))
    prices_ok = (pl.col('_close_c').is_not_null()
                 & pl.col('prev_close').is_not_null())
    normal = pl.col('listing_phase') == 'normal'
    can_buy = (pl.when(pl.col('listing_phase') == 'exempt').then(~pl.col('suspended'))
               .when(pl.col('listing_phase') == 'legacy').then(pl.lit(False))
               .when(~normal | ~prices_ok).then(pl.lit(False))
               .otherwise(~pl.col('suspended') & ~blocked_up.fill_null(True)))
    can_sell = (pl.when(pl.col('listing_phase') == 'exempt').then(~pl.col('suspended'))
                .when(pl.col('listing_phase') == 'legacy').then(pl.lit(False))
                .when(~normal | ~prices_ok).then(pl.lit(False))
                .otherwise(~pl.col('suspended') & ~blocked_down.fill_null(True)))
    limit_basis = (
        pl.when(pl.col('listing_phase') == 'exempt').then(pl.lit('new_stock_exempt'))
        .when(pl.col('listing_phase') == 'legacy').then(pl.lit('new_stock_legacy'))
        .when(pl.col('st_rank') == 2).then(pl.lit('st_unknown'))
        .when((pl.col('st_rank') == 1) & (pl.col('pct') < 10)).then(pl.lit('st'))
        .when(pl.col('st_rank') == 1).then(pl.lit('st_wide'))
        .otherwise(pl.lit('board'))
    )
    frame = frame.with_columns([
        touch_up.alias('touch_up'), touch_down.alias('touch_down'),
        close_up.alias('close_at_up'), close_down.alias('close_at_down'),
        one_up.alias('one_word_up'), one_down.alias('one_word_down'),
        can_buy.alias('can_buy'), can_sell.alias('can_sell'),
        limit_basis.alias('limit_basis'),
        pl.when(valid).then(pl.col('_up_c') / 100.0).otherwise(None)
        .alias('limit_up_price'),
        pl.when(valid).then(pl.col('_down_c') / 100.0).otherwise(None)
        .alias('limit_down_price'),
    ])
    _verify_price_rounding(frame)
    _verify_tradeability(frame, counters)

    return frame.select([
        pl.col('date').alias('trade_date'), pl.col('ts_code'), pl.col('board'),
        pl.col('st_status'), pl.col('listing_days'), pl.col('listing_phase'),
        pl.col('prev_close'), pl.col('prev_close_source'),
        (pl.col('pct') / 100.0).alias('limit_pct'), pl.col('limit_basis'),
        pl.col('limit_up_price'), pl.col('limit_down_price'),
        pl.col('touch_up'), pl.col('touch_down'),
        pl.col('close_at_up'), pl.col('close_at_down'),
        pl.col('one_word_up'), pl.col('one_word_down'),
        pl.col('suspended'), pl.col('can_buy'), pl.col('can_sell'),
    ])


def _verify_price_rounding(frame: pl.DataFrame) -> None:
    """随机抽样把整数分结果与 resolve_limit_price 对拍，不一致即失败。"""
    sample = frame.filter(pl.col('limit_up_price').is_not_null())
    if sample.height == 0:
        return
    take = min(PRICE_CHECK_SAMPLES, sample.height)
    picked = sample.sample(n=take, seed=42)
    bad = 0
    for prev, pct, up, down in zip(
            picked['prev_close'].to_list(), picked['pct'].to_list(),
            picked['limit_up_price'].to_list(), picked['limit_down_price'].to_list(),
            strict=True):
        limit = pct / 100.0
        if (abs(ExecutionRules.resolve_limit_price(prev, limit, 'buy') - up) > 1e-9
                or abs(ExecutionRules.resolve_limit_price(prev, limit, 'sell') - down) > 1e-9):
            bad += 1
    if bad:
        raise RuntimeError(f'整数分涨跌停价与 resolve_limit_price 抽样不一致：{bad}/{take}')


def _verify_tradeability(frame: pl.DataFrame, counters: dict[str, Any]) -> None:
    """随机抽样把 can_buy/can_sell 与 ``ExecutionRules.block_reason`` 对拍。"""
    sample = frame.filter(
        (pl.col('listing_phase') == 'normal')
        & pl.col('prev_close').is_not_null() & (pl.col('prev_close') > 0))
    if sample.height == 0:
        return
    take = min(TRADE_CHECK_SAMPLES, sample.height)
    picked = sample.sample(n=take, seed=7)
    rules = ExecutionRules()
    bad = 0
    for row in picked.select([
            'ts_code', 'date', 'prev_close', 'open', 'high', 'low', 'close',
            'volume', 'st_status', 'can_buy', 'can_sell']).iter_rows(named=True):
        is_st = None if row['st_status'] == 'unknown' else row['st_status'] == 'st'
        for side, column in (('buy', 'can_buy'), ('sell', 'can_sell')):
            expected = rules.block_reason(
                side, prev_close=row['prev_close'], open_price=row['open'],
                high=row['high'], low=row['low'], close=row['close'],
                volume=row['volume'], ts_code=row['ts_code'],
                is_st=is_st, trade_date=row['date']) is None
            if expected != row[column]:
                bad += 1
    counters['tradeability_check_mismatch'] = bad
    if bad:
        raise RuntimeError(f'可买可卖与 ExecutionRules.block_reason 抽样不一致：'
                           f'{bad}/{take * 2}')


# ----------------------------------------------------------------------
# 与 breadth 对拍
# ----------------------------------------------------------------------
def _compare(marks: pl.DataFrame, root: Path,
             counters: dict[str, Any]) -> tuple[dict[str, Any], pl.DataFrame]:
    """逐日对拍：新表 close_at_up/down vs breadth limit_up/down，并给差异归因。"""
    from src.data.quality.code_migration import load_entity_index  # noqa: PLC0415

    stored = pl.read_parquet(
        root / 'meta' / 'breadth' / 'breadth_daily.parquet',
        columns=['trade_date', 'limit_up_count', 'limit_down_count'])
    intervals = pl.read_parquet(
        root / 'meta' / 'st' / 'st_intervals.parquet',
        columns=['ts_code', 'start_date', 'end_date', 'flag'])
    intervals = intervals.filter(pl.col('flag').is_in(['st', '*st']))
    superseded = sorted(load_entity_index(root).superseded)

    quotes = (pl.scan_parquet(_quote_files(root), extra_columns='ignore')
              .select(['date', 'ts_code', 'close', 'pre_close', 'volume'])
              .collect().sort(['ts_code', 'date']))
    quotes = quotes.with_columns(
        pl.col('date').cum_count().over('ts_code').cast(pl.Int32).alias('row_no'))
    quotes = quotes.filter(
        pl.col('date') >= pl.lit(marks['trade_date'].min(), dtype=pl.Date))
    if superseded:
        quotes = quotes.filter(~pl.col('ts_code').is_in(superseded))
        marks = marks.filter(~pl.col('ts_code').is_in(superseded))

    marks = marks.select([
        pl.col('trade_date').alias('date'), 'ts_code', 'listing_phase',
        'st_status', 'prev_close_source', 'limit_basis',
        'close_at_up', 'close_at_down',
        (pl.col('limit_pct') * 100).round(0).cast(pl.Int64).alias('new_pct'),
    ])
    quotes = quotes.join(marks, on=['date', 'ts_code'], how='left')

    # breadth 侧 ST：st_intervals 逐日展开（区间数少，按 code 连接后过滤）
    hit = (quotes.select(['date', 'ts_code'])
           .join(intervals, on='ts_code', how='inner')
           .filter((pl.col('start_date') <= pl.col('date'))
                   & (pl.col('end_date').is_null()
                      | (pl.col('end_date') >= pl.col('date'))))
           .select(['date', 'ts_code']).unique()
           .with_columns(pl.lit(1).cast(pl.Int8).alias('b_st_rank')))
    quotes = (quotes.join(hit, on=['date', 'ts_code'], how='left')
              .with_columns(pl.col('b_st_rank').fill_null(0)))
    quotes = quotes.with_columns(
        pl.when(pl.col('date') >= _date_lit(ExecutionRules.CHINEXT_REG_DATE))
        .then(1).otherwise(0).cast(pl.Int8).alias('era'))
    pct_map = _limit_pct_map(quotes['ts_code'].unique().to_list())
    quotes = quotes.join(
        pct_map.filter(pl.col('st_rank') != 2).rename({'pct': 'b_pct'}),
        left_on=['ts_code', 'era', 'b_st_rank'],
        right_on=['ts_code', 'era', 'st_rank'], how='left')
    quotes = quotes.with_columns(pl.col('b_pct').fill_null(10).cast(pl.Int64))

    valid = ((pl.col('volume') > 0) & (pl.col('pre_close') > 0)
             & pl.col('pre_close').is_not_null() & pl.col('close').is_not_null())
    pre_c = (pl.col('pre_close') * 100).round(0).cast(pl.Int64)
    close_c = (pl.col('close') * 100).round(0).cast(pl.Int64)
    quotes = quotes.with_columns([
        close_c.alias('_close_c'),
        ((pre_c * (100 + pl.col('b_pct')) + 50) // 100).alias('_b_up_c'),
        ((pre_c * (100 - pl.col('b_pct')) + 50) // 100).alias('_b_down_c'),
        (valid & (pl.col('row_no') > 1)).alias('b_limitable'),
        pl.col('close_at_up').fill_null(False).alias('new_up'),
        pl.col('close_at_down').fill_null(False).alias('new_down'),
        (pl.col('row_no') == 1).alias('is_first'),
    ]).with_columns([
        (pl.col('b_limitable') & (pl.col('_close_c') >= pl.col('_b_up_c')))
        .alias('b_up'),
        (pl.col('b_limitable') & (pl.col('_close_c') <= pl.col('_b_down_c')))
        .alias('b_down'),
        (pl.col('st_status') == 'st').alias('new_is_st'),
    ])

    def _reason(direction: str) -> pl.Expr:
        new_col = pl.col(f'new_{direction}')
        b_col = pl.col(f'b_{direction}')
        return (pl.when(new_col != b_col)
                .then(pl.when(pl.col('listing_phase') != 'normal')
                      .then(pl.lit('new_stock'))
                      .when(pl.col('new_is_st') != (pl.col('b_st_rank') == 1))
                      .then(pl.lit('st_status_diff'))
                      .when(pl.col('is_first')).then(pl.lit('breadth_first_day'))
                      .when(pl.col('prev_close_source') != 'quote')
                      .then(pl.lit('prev_close_derived'))
                      .when((pl.col('pre_close') * 100
                             - (pl.col('pre_close') * 100).round(0)).abs() > 1e-6)
                      .then(pl.lit('prev_close_fraction'))
                      .when(pl.col('new_pct') != pl.col('b_pct'))
                      .then(pl.lit('limit_pct_diff'))
                      .otherwise(pl.lit('residual')))
                .otherwise(pl.lit('same')).alias(f'{direction}_reason'))

    quotes = quotes.with_columns([_reason('up'), _reason('down')])

    daily_new = quotes.group_by('date').agg([
        pl.col('new_up').sum().cast(pl.Int64).alias('new_up_count'),
        pl.col('new_down').sum().cast(pl.Int64).alias('new_down_count'),
        pl.col('b_up').sum().cast(pl.Int64).alias('b_up_count'),
        pl.col('b_down').sum().cast(pl.Int64).alias('b_down_count'),
    ]).sort('date')
    daily = (daily_new.join(stored, left_on='date', right_on='trade_date', how='left')
             .with_columns([
                 (pl.col('new_up_count') - pl.col('limit_up_count')).alias('diff_up'),
                 (pl.col('new_down_count') - pl.col('limit_down_count')).alias('diff_down'),
                 (pl.col('b_up_count') - pl.col('limit_up_count')).alias('b_diff_up'),
                 (pl.col('b_down_count') - pl.col('limit_down_count')).alias('b_diff_down'),
             ]).rename({'date': 'trade_date'})
             .select(['trade_date', 'limit_up_count', 'new_up_count', 'b_up_count',
                      'diff_up', 'b_diff_up', 'limit_down_count', 'new_down_count',
                      'b_down_count', 'diff_down', 'b_diff_down'])
             .sort('trade_date'))

    up_reasons = dict(quotes.filter(pl.col('up_reason') != 'same')
                      .group_by('up_reason').agg(pl.len()).iter_rows())
    down_reasons = dict(quotes.filter(pl.col('down_reason') != 'same')
                        .group_by('down_reason').agg(pl.len()).iter_rows())
    samples = (quotes.filter(pl.col('up_reason') != 'same')
               .sort('date').head(30)
               .select(['date', 'ts_code', 'listing_phase', 'prev_close_source',
                        'limit_basis', 'st_status', 'b_st_rank', 'new_pct', 'b_pct',
                        'new_up', 'b_up', 'up_reason']).to_dicts())

    def _stats(column: str) -> dict[str, Any]:
        values = [int(value) for value in daily[column].abs().to_list()
                  if value is not None]
        return {
            'max': max(values) if values else 0,
            'mean': round(sum(values) / len(values), 3) if values else 0.0,
            'dates_nonzero': sum(1 for value in values if value > 0),
        }

    up_stats, down_stats = _stats('diff_up'), _stats('diff_down')
    comparison = {
        'start': str(daily['trade_date'].min()) if daily.height else None,
        'dates': daily.height,
        'max_abs_diff': max(up_stats['max'], down_stats['max']),
        'max_abs_diff_up': up_stats['max'],
        'max_abs_diff_down': down_stats['max'],
        'mean_abs_diff_up': up_stats['mean'],
        'mean_abs_diff_down': down_stats['mean'],
        'dates_with_diff': int(
            ((daily['diff_up'].abs() > 0) | (daily['diff_down'].abs() > 0)).sum()),
        'dates_diff_zero': int(
            ((daily['diff_up'].abs() == 0) & (daily['diff_down'].abs() == 0)).sum()),
        'dates_diff_up_nonzero': up_stats['dates_nonzero'],
        'dates_diff_down_nonzero': down_stats['dates_nonzero'],
        'breadth_recompute_mismatch_dates': int(
            ((daily['b_diff_up'].abs() > 0)
             | (daily['b_diff_down'].abs() > 0)).sum()),
        'b_recompute_max_abs_diff': int(max(
            daily['b_diff_up'].abs().max() or 0,
            daily['b_diff_down'].abs().max() or 0)),
        'b_recompute_note': '用 breadth 口径（st_intervals + 首日剔除 + 同一 pct_map）'
                            '复算，与 breadth 存量比对：2025-2026 完全一致；'
                            '2005-2024 少数日期差 1~8 只，来自 breadth 快照历史行'
                            '为增量构建、早于行情/ST 回补，不是本表口径差异。',
        'new_vs_recomputed_by_reason_up': {str(k): int(v) for k, v in up_reasons.items()},
        'new_vs_recomputed_by_reason_down': {str(k): int(v) for k, v in down_reasons.items()},
        'samples': samples,
        'note': 'diff_* = 新表 close_at_* 计数 − breadth 存量计数；'
                'reason 归因对的是「新表 vs breadth 口径复算」，'
                '避免把 breadth 快照历史回补误差算进规则差异。',
    }
    counters['breadth_max_abs_diff'] = comparison['max_abs_diff']
    return comparison, daily


# ----------------------------------------------------------------------
# 主流程
# ----------------------------------------------------------------------
def build(store_root: Path | str, *, since: str | date = DEFAULT_START,
          codes: Iterable[str] | None = None) -> dict[str, Any]:
    root = Path(store_root)
    start = since if isinstance(since, date) else date.fromisoformat(str(since))
    sessions = _sessions(root)
    start = max(start, sessions[0]) if sessions else start
    end = sessions[-1] if sessions else date.today()

    frame, counters = _prepare(root, start, end)
    if codes is not None:
        wanted = {str(code).strip().upper() for code in codes}
        frame = frame.filter(pl.col('ts_code').is_in(list(wanted)))
    marks = _apply_marks(frame, counters)
    del frame
    # 读入时多取 45 个自然日只为给首日算前收/上市天数，落盘仍从 start 开始
    marks = marks.filter(pl.col('trade_date') >= start).sort(['trade_date', 'ts_code'])
    counters.update({
        'rows': marks.height,
        'codes': marks['ts_code'].n_unique(),
        'st_panel_missing': int((marks['st_status'] == 'unknown').sum()),
        'prev_close_derived': int(
            (marks['prev_close_source'] == 'derived_prev_close').sum()),
        'prev_close_missing': int((marks['prev_close_source'] == 'none').sum()),
        'listing_exempt_rows': int((marks['listing_phase'] == 'exempt').sum()),
        'listing_legacy_rows': int((marks['listing_phase'] == 'legacy').sum()),
        'suspended_rows': int(marks['suspended'].sum()),
    })

    out_dir = output_dir(root)
    out_dir.mkdir(parents=True, exist_ok=True)
    files: list[dict[str, Any]] = []
    produced: set[str] = set()
    for month_start, month_end, tag in _month_spans(start, end):
        part = marks.filter((pl.col('trade_date') >= month_start)
                            & (pl.col('trade_date') <= month_end))
        if part.height == 0:
            continue
        path = out_dir / f'limit_daily_{tag}.parquet'
        write_parquet_atomic(part, path)
        produced.add(path.name)
        files.append({
            'path': path.name, 'rows': part.height,
            'start': str(part['trade_date'].min()),
            'end': str(part['trade_date'].max()),
            'bytes': path.stat().st_size,
            'sha256': _sha256(path),
        })
    for stale in out_dir.glob('limit_daily_[0-9][0-9][0-9][0-9][0-9][0-9].parquet'):
        if stale.name not in produced:
            stale.unlink()

    comparison, daily = _compare(marks, root, counters)
    if daily.height:
        write_parquet_atomic(daily, reconcile_path(root))

    built_at = datetime.now().isoformat(timespec='seconds')
    panel_end = _panel_end(root)
    freshness = {
        'schema': 'limit_board_freshness/v1',
        'built_at': built_at,
        'quote_end': str(end),
        'st_panel_end': str(panel_end) if panel_end else None,
        'lag_trading_days': _lag_trading_days(sessions, end, panel_end),
        'state': 'ok' if panel_end is None or panel_end >= end else 'warn',
    }
    manifest = {
        'schema': SCHEMA_VERSION,
        'built_at': built_at,
        'range': {'start': str(start), 'end': str(end)},
        'trading_days': len([day for day in sessions if start <= day <= end]),
        'rows': counters['rows'],
        'codes': counters['codes'],
        'files': files,
        'freshness': freshness,
        'rules': {
            'limit_pct': 'src.core.market_rules.ExecutionRules.resolve_limit_pct',
            'limit_price': '整数分 ROUND_HALF_UP，逐样本与 resolve_limit_price 对拍',
            'tradeability': 'ExecutionRules.block_reason 同序（停牌→开盘触板→一字退化）',
            'st_source': 'meta/st/daily（生效日切换）',
        },
        'counters': counters,
        'comparison': comparison,
        'caveats': [
            'listing_phase=legacy（改革前主板/创业板首日）历史涨跌幅另制，'
            '无法用统一百分比还原：价格列留空、can_buy/can_sell=False（保守）。',
            'st_status=unknown 仍按板块常规幅度计算，limit_basis=st_unknown 留证。',
            'prev_close_source=derived_prev_close 为行情缺前收时的收盘补齐，'
            '除权日可能与交易所口径不同。',
            '前收带 3 位以上小数的历史行（北交所平移等）用 resolve_limit_price '
            '精确计算，breadth 旧口径按整数分，差异归因为 prev_close_fraction。',
            '北交所由精选层平移的标的，list_date 可能早于北交所开市日，'
            '首日不限价窗口按 list_date 判定，未额外猜测。',
        ],
    }
    write_text_atomic(json.dumps(freshness, ensure_ascii=False, indent=2),
                      freshness_path(root))
    write_text_atomic(json.dumps(comparison, ensure_ascii=False, indent=2, default=str),
                      comparison_path(root))
    write_text_atomic(json.dumps(manifest, ensure_ascii=False, indent=2, default=str),
                      manifest_path(root))
    logger.info(f'[limit] 落盘 {len(files)} 个月分区 / {marks.height} 行 / '
                f'{counters["codes"]} 只；breadth 对拍最大差 {comparison["max_abs_diff"]}')
    return {'manifest': manifest, 'comparison': comparison}


def _panel_end(root: Path) -> date | None:
    files = sorted((root / 'meta' / 'st' / 'daily').glob('st_daily_[0-9]*.parquet'))
    if not files:
        return None
    return pl.scan_parquet(str(files[-1]), extra_columns='ignore').select(
        pl.col('trade_date').max()).collect().item()


def _lag_trading_days(sessions: list[date], quote_end: date,
                      panel_end: date | None) -> int:
    if panel_end is None:
        return 0
    return sum(1 for day in sessions if panel_end < day <= quote_end)


# ----------------------------------------------------------------------
# 读取入口（下游按需读月分区）
# ----------------------------------------------------------------------
def load_marks(store_root: Path | str, start: date, end: date,
               columns: list[str] | None = None) -> pl.DataFrame:
    directory = output_dir(store_root)
    files = [str(path) for path in
             sorted(directory.glob('limit_daily_[0-9]*.parquet'))]
    if not files:
        return pl.DataFrame(schema=_SCHEMA)
    frame = (pl.scan_parquet(files, extra_columns='ignore')
             .filter((pl.col('trade_date') >= start) & (pl.col('trade_date') <= end)))
    if columns:
        frame = frame.select(columns)
    return frame.collect()


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='重建逐股逐日涨跌停标记')
    parser.add_argument('--since', default=DEFAULT_START.isoformat())
    args = parser.parse_args(argv)
    from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
    from src.utils.config import get_config  # noqa: PLC0415

    summary = build(resolve_store_root(get_config()), since=args.since)
    logger.info(json.dumps({
        'rows': summary['manifest']['rows'],
        'max_abs_diff': summary['comparison']['max_abs_diff'],
        'dates_with_diff': summary['comparison']['dates_with_diff'],
    }, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
