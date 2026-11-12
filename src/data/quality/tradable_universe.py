#!/usr/bin/env python3

"""机器可读的「不可用标的」排除清单。

本模块只读本地列存与可选的 TDX 源、基础表，产出统一清单：
``meta/universe/universe_exclusions.parquet`` 与同目录 JSON 汇总。

设计目标
--------
1. 一份清单回答"哪些标的、因为什么、影响多大"，下游回测/选股/推荐不再各自
   重写过滤规则。
2. 一个标的可以命中多条原因（例如既是占位行情又是停更），清单按
   ``(ts_code, reason_code)`` 存行，不做合并，避免信息丢失。
3. 纯只读：不写 ``quotes/**``、不动 MySQL、不依赖 ``src/advice``。
4. 生成幂等：``detected_at`` 取数据快照日而非当前时钟，重跑字节一致。

原因码
------
- ``placeholder_price``：价格恒为 1.0 的占位行情（全段 / 尾部），零收益假样本
- ``stale_feed``：本地末行早于最近交易日（源端停更 / 退市）
- ``no_quotes``：基础表有记录但本地无行情文件
- ``internal_gap``：本地缺历史交易日；含 TDX 对账缺口，以及指数在交易日历上
  连续 ≥ 20 个交易日无 bar 的大段空洞（源端同样缺失时也登记，避免被
  ``tdx_truncated`` 掩盖）
- ``tdx_truncated``：TDX 源历史比本地短，禁止用 TDX 重建
- ``no_basic_info``：本地有行情但基础表无记录（名称/行业等元数据缺失）
- ``bad_price_jump``：未识别的孤立价格跳变（单日 |涨跌幅| > ``JUMP_THRESHOLD``）
- ``bad_ohlc``：该行 OHLC 非法（high<low / high<max(开,收) / low>min(开,收) /
  非正价格），下游 ATR、筹码峰等计算需跳过该行；按行登记，不整只排除

读侧用法::

    from src.data.quality.tradable_universe import is_tradable, load_exclusions

    is_tradable('600000.SH', '2025-06-30')
    is_tradable('000004.SZ', '2026-11-03', strict=True)
"""

from __future__ import annotations

import contextlib
import json
import threading
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
from loguru import logger

from src.data.atomic_write import write_parquet_atomic, write_text_atomic
from src.utils.code_converter import to_ts_code

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ROOT = PROJECT_ROOT / 'data' / 'local_store'
UNIVERSE_DIR = ('meta', 'universe')
EXCLUSIONS_FILE = 'universe_exclusions.parquet'
SUMMARY_FILE = 'universe_exclusions.json'
PRICE_JUMPS_FILE = 'universe_price_jumps.parquet'

ASSET_CLASSES: tuple[str, ...] = ('stock', 'index', 'fund')

EXCLUSION_SCHEMA: dict[str, Any] = {
    'ts_code': pl.Utf8,
    'asset_class': pl.Utf8,
    'reason_code': pl.Utf8,
    'reason_text': pl.Utf8,
    'last_trade_date': pl.Date,
    'expected_rows': pl.Int64,
    'actual_rows': pl.Int64,
    'missing_rows': pl.Int64,
    'severity': pl.Utf8,
    'detected_at': pl.Date,
}

REASON_SEVERITY: dict[str, str] = {
    'placeholder_price': 'block',
    'no_quotes': 'block',
    'stale_feed': 'warn',
    'internal_gap': 'warn',
    'tdx_truncated': 'warn',
    'no_basic_info': 'warn',
    'bad_price_jump': 'warn',
    'bad_ohlc': 'warn',
}

#: 价格跳变判定阈值（单日 |close/pre_close - 1| 超过该值即进入分类流程）。
#: 扫描判据、报告 threshold 文案、REASON_TEXT 文案统一引用本常量。
JUMP_THRESHOLD = 0.5

REASON_TEXT: dict[str, str] = {
    'placeholder_price': '价格恒为 1.0 的占位行情，零收益假样本',
    'stale_feed': '本地末行早于最近交易日（源端停更/退市）',
    'no_quotes': '基础表有记录但本地无行情文件（按基础表取数会静默少截面）',
    'internal_gap': '本地缺历史交易日（TDX 对账缺口或指数连续 ≥20 日空洞）',
    'tdx_truncated': 'TDX 源历史比本地短，重建会截断历史（禁止用 TDX 重建）',
    'no_basic_info': '本地有行情但基础表无记录（名称/行业等元数据缺失）',
    'bad_price_jump': (
        f'未识别的孤立价格跳变（单日涨跌幅绝对值 > {JUMP_THRESHOLD:.0%}，疑似源端错误）'
    ),
    'bad_ohlc': '该行 OHLC 非法，下游计算需跳过该行',
}

#: 价格跳变分类（只有 data_error 会进入排除清单）
JUMP_LABELS: tuple[str, ...] = (
    'corporate_action_missed',
    'resumption_gap',
    'data_error',
    'index_rebalance',
    'market_wide_event',
    'new_listing',
    'pre_price_limit_era',
)

JUMP_LABEL_TEXT: dict[str, str] = {
    'corporate_action_missed': '除权除息/送转未复权导致的跳变（因子或分红事件可解释）',
    'resumption_gap': '长期停牌复牌后的跳空',
    'data_error': '无事件、无长期停牌、因子无变动的孤立跳变（疑似源端错误）',
    'index_rebalance': '指数点位跳变（成分调整/基期重置，仅指数）',
    'market_wide_event': '同日多只股票同向跳变（市场制度/报价单位切换等共性事件）',
    'new_listing': '新股上市初期无涨跌幅限制（文件前 5 行内的跳变）',
    'pre_price_limit_era': '1996-12-16 涨跌幅限制实施前的真实大幅波动',
}

#: A 股涨跌幅限制实施日；之前单日 >50% 属真实行情
PRICE_LIMIT_DATE = date(1996, 12, 16)

#: ``bad_ohlc`` 整只停用阈值：非法行占该标的全历史比例超过该值时才登记汇总行。
#: 依据：非法行会让 ATR/筹码峰等逐行计算失真，但个别早期错值不影响整只票可用；
#: 超过 20% 意味着每 5 个交易日就有 1 行不可用，波动率与筹码分布已无法可靠估计。
#: 实测全量最高占比仅 5.62%（880006.SH），当前不会触发整只停用。
BAD_OHLC_CODE_SHARE_LIMIT = 0.2

#: 指数连续空洞判定阈值：本地指数序列在交易日历上连续缺失 >= 该交易日数。
#: 只对指数启用：个股/基金的日历空洞大量来自长期停牌与退市，与数据缺失
#: 不可区分；而指数每个交易日都应有 bar，连续大段缺失必为数据问题。
CONTINUOUS_GAP_MIN_DAYS = 20

#: 无论 ``as_of`` 都直接判定不可用的原因
_BLOCKING_REASONS: frozenset[str] = frozenset({'placeholder_price', 'no_quotes'})
#: ``strict=True`` 时额外剔除的历史缺口/元数据原因（tdx_truncated 只约束重建，
#: 不影响本地数据可用性；bad_price_jump 只在跳变当天生效，不按标的长期剔除）
_STRICT_WARNINGS: frozenset[str] = frozenset({'internal_gap', 'no_basic_info'})
#: 通达信自编指数代码前缀（``880`` 板块指数 / ``881`` 行业指数）。这类指数不可投资，
#: 历史序列在源端本身就不完整，``internal_gap`` 只作为记录保留，不参与 strict 剔除；
#: 判定同时要求 ``asset_class='index'``，避免误伤同前缀的非指数标的。
TDX_SYNTHETIC_INDEX_PREFIXES: tuple[str, ...] = ('880', '881')

_LOCK = threading.RLock()
_CACHE: dict[str, tuple[int, pl.DataFrame]] = {}

_DAY_DTYPE = np.dtype([
    ('date', '<u4'),
    ('open', '<u4'),
    ('high', '<u4'),
    ('low', '<u4'),
    ('close', '<u4'),
    ('amount', '<f4'),
    ('volume', '<u4'),
    ('reserved', '<u4'),
])


def _strict_ignorable(reason: Mapping[str, Any]) -> bool:
    """strict 口径下可忽略的原因行（仅通达信自编指数的 ``internal_gap``）。

    判定依据全部取自清单自身的字段，可追溯、不依赖代码白名单：
    ``reason_code == 'internal_gap'`` 且 ``asset_class == 'index'`` 且
    ``ts_code`` 前三位命中 :data:`TDX_SYNTHETIC_INDEX_PREFIXES`。
    """
    return (
        reason.get('reason_code') == 'internal_gap'
        and reason.get('asset_class') == 'index'
        and str(reason.get('ts_code', ''))[:3] in TDX_SYNTHETIC_INDEX_PREFIXES
    )


# ----------------------------------------------------------------------
# 路径与通用小工具
# ----------------------------------------------------------------------
def _resolve_root(root: Path | str) -> Path:
    path = Path(root)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def _universe_dir(store_root: Path | str) -> Path:
    return _resolve_root(store_root).joinpath(*UNIVERSE_DIR)


def _exclusions_path(root: Path | str) -> Path:
    """``root`` 可为列存根目录，也可直接指向 ``meta/universe``。"""
    base = _resolve_root(root)
    if (base / EXCLUSIONS_FILE).is_file():
        return base / EXCLUSIONS_FILE
    return base.joinpath(*UNIVERSE_DIR) / EXCLUSIONS_FILE


def _as_date(value: Any) -> date | None:
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        with contextlib.suppress(ValueError):
            return datetime.strptime(text[:10] if '-' in text else text[:8], fmt).date()
    return None


def _quote_path(store_root: Path, asset_class: str, ts_code: str) -> Path:
    market = ts_code[-2:].lower()
    return store_root / 'quotes' / asset_class / market / f'{ts_code}.parquet'


def _epoch_days(values: np.ndarray) -> np.ndarray:
    """``datetime64`` 数组 -> 1970-01-01 起的天数。"""
    return values.astype('datetime64[D]').astype(np.int64)


def _date_of_epoch(day: int) -> date:
    """1970-01-01 起的天数 -> ``datetime.date``。"""
    return (np.datetime64('1970-01-01', 'D') + np.timedelta64(int(day), 'D')).item()


def _ymd_to_epoch_days(dates: np.ndarray) -> np.ndarray:
    """``YYYYMMDD`` 整数数组 -> 1970-01-01 起的天数（纯 numpy 向量化）。"""
    if dates.size == 0:
        return np.array([], dtype=np.int64)
    y = dates.astype(np.int64) // 10000
    m = (dates.astype(np.int64) // 100) % 100
    d = dates.astype(np.int64) % 100
    a = (14 - m) // 12
    yy = y + 4800 - a
    mm = m + 12 * a - 3
    jdn = d + (153 * mm + 2) // 5 + 365 * yy + yy // 4 - yy // 100 + yy // 400 - 32045
    return jdn - 2440588  # 2440588 = 1970-01-01 的儒略日


def _read_tdx_days(path: Path) -> np.ndarray:
    raw = np.fromfile(path, dtype=_DAY_DTYPE)
    if raw.size == 0:
        return np.array([], dtype=np.int64)
    values = raw['date']
    values = values[(values >= 19900101) & (values <= 21000101)]
    return np.unique(_ymd_to_epoch_days(values))


def _read_local_days(store_root: Path, asset_class: str, ts_code: str) -> np.ndarray:
    path = _quote_path(store_root, asset_class, ts_code)
    if not path.is_file():
        return np.array([], dtype=np.int64)
    frame = pl.read_parquet(path, columns=['date']).sort('date')
    return _epoch_days(frame['date'].to_numpy())


def _empty_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=EXCLUSION_SCHEMA)


# ----------------------------------------------------------------------
# 生成：各类原因
# ----------------------------------------------------------------------
def _placeholder_rows(store_root: Path, as_of: date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    fund_dir = store_root / 'quotes' / 'fund'
    if not fund_dir.is_dir():
        return rows
    for path in sorted(fund_dir.rglob('*.parquet')):
        ts_code = path.stem
        frame = pl.read_parquet(path, columns=['date', 'close']).sort('date')
        if frame.is_empty():
            continue
        closes = frame['close'].to_numpy()
        real = np.flatnonzero(closes != 1.0)
        if real.size == closes.size:
            continue
        dates = frame['date'].to_list()
        if real.size == 0:
            mode = '全段'
            last_trade = None
        else:
            trailing = closes.size - 1 - int(real[-1])
            if trailing == 0:
                continue
            mode = f'尾部 {trailing} 行'
            last_trade = dates[int(real[-1])]
        rows.append({
            'ts_code': ts_code,
            'asset_class': 'fund',
            'reason_code': 'placeholder_price',
            'reason_text': f'{REASON_TEXT["placeholder_price"]}（{mode}）',
            'last_trade_date': last_trade,
            'expected_rows': int(closes.size),
            'actual_rows': 0,
            'missing_rows': int(closes.size),
            'severity': REASON_SEVERITY['placeholder_price'],
            'detected_at': as_of,
        })
    return rows


def _benchmark_calendar(store_root: Path) -> np.ndarray | None:
    """基准交易日（1970 起的天数），统一由 ``src.data.trading_calendar`` 提供。

    只取权威段（真实指数交易日），不含日历末端的近似尾部；基准文件缺失时
    返回 ``None``，与旧实现「无数据即不判定缺口」的语义保持一致。
    """
    from src.data.trading_calendar import get_calendar  # noqa: PLC0415

    calendar = get_calendar(store_root)
    days = calendar.authoritative_dates()
    if not days:
        return None
    return np.array(
        [np.datetime64(d.isoformat(), 'D').astype(np.int64) for d in days],
        dtype=np.int64,
    )


def _stale_rows(
    di: pl.DataFrame, store_root: Path, as_of: date,
) -> list[dict[str, Any]]:
    calendar = _benchmark_calendar(store_root)
    rows: list[dict[str, Any]] = []
    stale = di.filter(pl.col('end').is_not_null() & (pl.col('end') < as_of))
    for item in stale.iter_rows(named=True):
        end = item['end']
        missing = 0
        if calendar is not None and calendar.size:
            end_day = np.datetime64(end.isoformat(), 'D').astype(np.int64)
            as_of_day = np.datetime64(as_of.isoformat(), 'D').astype(np.int64)
            missing = int(np.count_nonzero((calendar > end_day) & (calendar <= as_of_day)))
        actual = int(item['rows'] or 0)
        rows.append({
            'ts_code': item['ts_code'],
            'asset_class': item['data_type'],
            'reason_code': 'stale_feed',
            'reason_text': REASON_TEXT['stale_feed'],
            'last_trade_date': end,
            'expected_rows': actual + missing,
            'actual_rows': actual,
            'missing_rows': missing,
            'severity': REASON_SEVERITY['stale_feed'],
            'detected_at': as_of,
        })
    return rows


def _basic_rows(
    di: pl.DataFrame, basic_tables: Mapping[str, Iterable[str]], as_of: date,
) -> list[dict[str, Any]]:
    local_by_class = {
        cls: set(di.filter(pl.col('data_type') == cls)['ts_code'].to_list())
        for cls in ASSET_CLASSES
    }
    row_count = {item['ts_code']: int(item['rows'] or 0) for item in di.iter_rows(named=True)}
    rows: list[dict[str, Any]] = []
    for cls in ASSET_CLASSES:
        basic = set(basic_tables.get(cls, ()))
        if not basic:
            continue
        for ts_code in sorted(basic - local_by_class[cls]):
            rows.append({
                'ts_code': ts_code,
                'asset_class': cls,
                'reason_code': 'no_quotes',
                'reason_text': REASON_TEXT['no_quotes'],
                'last_trade_date': None,
                'expected_rows': 0,
                'actual_rows': 0,
                'missing_rows': 0,
                'severity': REASON_SEVERITY['no_quotes'],
                'detected_at': as_of,
            })
        for ts_code in sorted(local_by_class[cls] - basic):
            rows.append({
                'ts_code': ts_code,
                'asset_class': cls,
                'reason_code': 'no_basic_info',
                'reason_text': REASON_TEXT['no_basic_info'],
                'last_trade_date': None,
                'expected_rows': row_count.get(ts_code, 0),
                'actual_rows': row_count.get(ts_code, 0),
                'missing_rows': 0,
                'severity': REASON_SEVERITY['no_basic_info'],
                'detected_at': as_of,
            })
    return rows


def _continuous_gap_rows(
    store_root: Path, as_of: date,
) -> tuple[list[dict[str, Any]], dict[str, set[int]]]:
    """登记指数在交易日历上连续大段缺失的区间（每段一行）。

    TDX 对账只能看到「TDX 有、本地缺」；当源端同样整段没有数据时，缺口会被
    ``tdx_truncated`` 掩盖。这里改用权威交易日历直接比对本地指数序列，把
    连续缺失 >= :data:`CONTINUOUS_GAP_MIN_DAYS` 个交易日、且两侧都有数据的
    区间单独登记为 ``internal_gap``。

    Returns:
        ``(rows, covered)``：``covered`` 为 ``{ts_code: {缺失日}}``，供
        :func:`_tdx_rows` 去重，避免同一空洞被登记两次。
    """
    calendar = _benchmark_calendar(store_root)
    rows: list[dict[str, Any]] = []
    covered: dict[str, set[int]] = {}
    if calendar is None or calendar.size == 0:
        return rows, covered
    index_dir = store_root / 'quotes' / 'index'
    if not index_dir.is_dir():
        return rows, covered
    for path in sorted(index_dir.rglob('*.parquet')):
        ts_code = path.stem
        local = _read_local_days(store_root, 'index', ts_code)
        if local.size < 2:
            continue
        expected = calendar[(calendar >= local.min()) & (calendar <= local.max())]
        missing = np.setdiff1d(expected, local)
        if missing.size < CONTINUOUS_GAP_MIN_DAYS:
            continue
        positions = np.searchsorted(calendar, missing)
        start = 0
        for end in range(1, missing.size + 1):
            if end < missing.size and positions[end] - positions[end - 1] == 1:
                continue
            length = end - start
            if length >= CONTINUOUS_GAP_MIN_DAYS:
                days = missing[start:end]
                first = _date_of_epoch(int(days[0]))
                last = _date_of_epoch(int(days[-1]))
                before = _date_of_epoch(int(calendar[max(positions[start] - 1, 0)]))
                rows.append({
                    'ts_code': ts_code,
                    'asset_class': 'index',
                    'reason_code': 'internal_gap',
                    'reason_text': (
                        f'指数连续大段缺失：{first}~{last} 共 {length} 个交易日'
                        '无 bar，缺口两侧均有数据'),
                    'last_trade_date': before,
                    'expected_rows': int(local.size + length),
                    'actual_rows': int(local.size),
                    'missing_rows': int(length),
                    'severity': REASON_SEVERITY['internal_gap'],
                    'detected_at': as_of,
                })
                covered.setdefault(ts_code, set()).update(int(x) for x in days)
            start = end
    return rows, covered


def _tdx_rows(
    di: pl.DataFrame, store_root: Path, tdx_root: Path, as_of: date,
    covered: Mapping[str, set[int]] | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    from src.data.local_store.build import discover_local_files

    calendar = _benchmark_calendar(store_root)
    local_codes = {(item['data_type'], item['ts_code']) for item in di.iter_rows(named=True)}
    gap_rows: list[dict[str, Any]] = []
    short_rows: list[dict[str, Any]] = []
    for asset_class, ts_code, path in discover_local_files(tdx_root, 'all'):
        if (asset_class, ts_code) not in local_codes:
            continue
        tdx = _read_tdx_days(path)
        local = _read_local_days(store_root, asset_class, ts_code)
        if local.size == 0 or tdx.size == 0:
            continue
        local_end = int(local.max())
        tdx_upto = tdx[tdx <= local_end]
        if calendar is not None and calendar.size:
            # 非交易日的 TDX bar 不构成「本地缺口」。通达信早期历史的日期字段存在错位：
            # 1993-05-01（周六）/ 1993-05-02（周日）会写出 bar，同时漏掉 05-03（周一）、
            # 05-06（周四）。不过滤会把本地正确的缺失判成 internal_gap，再经 strict 口径
            # 把 000001.SZ、000002.SZ 这类主板标的误移出可交易池。
            tdx_upto = tdx_upto[np.isin(tdx_upto, calendar)]
        missing = np.setdiff1d(tdx_upto, local)
        if missing.size and covered and covered.get(ts_code):
            # 已由连续空洞登记过的缺失日不再重复计入 TDX 对账行
            missing = missing[~np.isin(missing, list(covered[ts_code]))]
        if missing.size:
            gap_rows.append({
                'ts_code': ts_code,
                'asset_class': asset_class,
                'reason_code': 'internal_gap',
                'reason_text': REASON_TEXT['internal_gap'],
                'last_trade_date': _date_of_epoch(local_end),
                'expected_rows': int(local.size + missing.size),
                'actual_rows': int(local.size),
                'missing_rows': int(missing.size),
                'severity': REASON_SEVERITY['internal_gap'],
                'detected_at': as_of,
            })
        if tdx.size < local.size:
            short_rows.append({
                'ts_code': ts_code,
                'asset_class': asset_class,
                'reason_code': 'tdx_truncated',
                'reason_text': REASON_TEXT['tdx_truncated'],
                'last_trade_date': _date_of_epoch(int(local.max())),
                'expected_rows': int(local.size),
                'actual_rows': int(tdx.size),
                'missing_rows': int(local.size - tdx.size),
                'severity': REASON_SEVERITY['tdx_truncated'],
                'detected_at': as_of,
            })
    return gap_rows, short_rows


def _load_dividend_events(
    store_root: Path,
) -> tuple[set[tuple[str, date]], dict[str, list[date]]]:
    """返回 (精确除权日集合, 仅 report_date 的送转记录)。

    部分历史分红记录 ``ex_date`` 为空，但 ``share_div > 0`` 仍可解释腰斩式跳变，
    按 ``report_date`` 起一年内匹配。
    """
    path = store_root / 'factors' / 'stock_dividend.parquet'
    if not path.is_file():
        return set(), {}
    frame = pl.read_parquet(
        path, columns=['ts_code', 'ex_date', 'report_date', 'share_div'])
    exact: set[tuple[str, date]] = set()
    missing: dict[str, list[date]] = {}
    for item in frame.iter_rows(named=True):
        ex_date = _as_date(item['ex_date'])
        if ex_date is not None:
            exact.add((item['ts_code'], ex_date))
            continue
        report_date = _as_date(item['report_date'])
        if (report_date is not None and item['share_div']
                and float(item['share_div']) > 0):
            missing.setdefault(item['ts_code'], []).append(report_date)
    return exact, missing


def _scan_jump_records(
    store_root: Path, calendar: np.ndarray | None,
    dividends: set[tuple[str, date]], pending_splits: Mapping[str, list[date]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    records: list[dict[str, Any]] = []
    zero_pre_close: dict[str, int] = dict.fromkeys(ASSET_CLASSES, 0)
    for asset_class in ASSET_CLASSES:
        folder = store_root / 'quotes' / asset_class
        if not folder.is_dir():
            continue
        for path in sorted(folder.rglob('*.parquet')):
            frame = pl.read_parquet(
                path, columns=['date', 'close', 'pre_close', 'qfq_factor', 'hfq_factor']
            ).sort('date')
            if frame.height < 2:
                continue
            dates = frame['date'].to_list()
            close = frame['close'].to_numpy()
            pre_close = frame['pre_close'].to_numpy()
            qfq = frame['qfq_factor'].to_numpy()
            hfq = frame['hfq_factor'].to_numpy()
            code = path.stem
            valid = np.isfinite(pre_close) & (pre_close > 0)
            zero_pre_close[asset_class] += int(
                np.count_nonzero(np.isfinite(pre_close) & (pre_close <= 0)))
            ratio_all = np.zeros_like(close)
            np.divide(close, pre_close, out=ratio_all, where=valid)
            ratio_all -= 1
            candidates = np.flatnonzero(valid & (np.abs(ratio_all) > JUMP_THRESHOLD))
            for i in candidates:
                records.append(_jump_record(
                    asset_class, code, dates, close, pre_close, qfq, hfq,
                    int(i), float(ratio_all[i]), calendar, dividends, pending_splits))
    return records, zero_pre_close


def _jump_record(
    asset_class: str, code: str, dates: list[date],
    close: np.ndarray, pre_close: np.ndarray,
    qfq: np.ndarray, hfq: np.ndarray, index: int, ratio: float,
    calendar: np.ndarray | None, dividends: set[tuple[str, date]],
    pending_splits: Mapping[str, list[date]],
) -> dict[str, Any]:
    current, previous = dates[index], dates[index - 1]
    gap_days = 0
    if calendar is not None and calendar.size:
        cur_day = int(np.datetime64(current.isoformat(), 'D').astype(np.int64))
        prev_day = int(np.datetime64(previous.isoformat(), 'D').astype(np.int64))
        gap_days = int(np.searchsorted(calendar, cur_day, 'left')
                       - np.searchsorted(calendar, prev_day, 'right'))
    factor_evidence = ''
    for name, values in (('qfq', qfq), ('hfq', hfq)):
        before, after = values[index - 1], values[index]
        if (np.isfinite(before) and np.isfinite(after) and before > 0
                and abs(after / before - 1) > 1e-6):
            factor_evidence = f'{name}_factor {before:.6g}→{after:.6g}'
            break
    has_dividend = (code, current) in dividends or (code, previous) in dividends
    split_evidence = ''
    if not has_dividend:
        for report_date in pending_splits.get(code, []):
            if 0 <= (current - report_date).days <= 365:
                has_dividend = True
                split_evidence = f'送转记录 report_date={report_date.isoformat()}（ex_date 缺失）'
                break
    return {
        'asset_class': asset_class, 'ts_code': code,
        'date': current, 'prev_date': previous,
        'pre_close': float(pre_close[index]), 'close': float(close[index]),
        'pct_chg': ratio * 100, 'gap_trading_days': gap_days,
        'row_pos': index,
        'factor_changed': bool(factor_evidence), 'has_dividend': has_dividend,
        'factor_evidence': factor_evidence, 'split_evidence': split_evidence,
    }


def _price_jumps(
    store_root: Path, as_of: date,
) -> tuple[list[dict[str, Any]], pl.DataFrame, dict[str, Any]]:
    """扫描全历史日线，分类 ``|close/pre_close - 1| > JUMP_THRESHOLD`` 的跳变行。

    Returns:
        (仅 data_error 的排除行, 全部跳变明细, 报告汇总)
    """
    calendar = _benchmark_calendar(store_root)
    dividends, pending_splits = _load_dividend_events(store_root)
    records, zero_pre_close = _scan_jump_records(
        store_root, calendar, dividends, pending_splits)

    wide_events = set()
    counts: dict[tuple[date, int], int] = {}
    for item in records:
        if item['asset_class'] != 'stock':
            continue
        key = (item['date'], 1 if item['pct_chg'] > 0 else -1)
        counts[key] = counts.get(key, 0) + 1
    for (when, _sign), number in counts.items():
        if number >= 5:
            wide_events.add(when)

    for item in records:
        item['label'], item['evidence'] = _classify_jump(item, wide_events)

    detail = pl.DataFrame(records) if records else _empty_jump_frame()
    detail = detail.sort(['asset_class', 'label', 'ts_code', 'date'])
    excluded = [
        {
            'ts_code': item['ts_code'],
            'asset_class': item['asset_class'],
            'reason_code': 'bad_price_jump',
            'reason_text': (
                f'{REASON_TEXT["bad_price_jump"]}'
                f'（{item["date"].isoformat()} {item["pre_close"]:.2f}→'
                f'{item["close"]:.2f}，{item["pct_chg"]:+.1f}%）'),
            'last_trade_date': item['date'],
            'expected_rows': 1,
            'actual_rows': 0,
            'missing_rows': 1,
            'severity': REASON_SEVERITY['bad_price_jump'],
            'detected_at': as_of,
        }
        for item in records if item['label'] == 'data_error'
    ]
    report = _jump_report(detail, zero_pre_close, as_of)
    return excluded, detail, report


def _bad_ohlc_rows(
    store_root: Path, as_of: date,
) -> list[dict[str, Any]]:
    """逐行扫描非法 OHLC，登记 ``bad_ohlc``（只标记，不改行情数值）。

    非法判定（任一成立即非法）：``high < low``、``high < max(open, close)``、
    ``low > min(open, close)``、``close <= 0``、``open <= 0``。空值行不参与比较，
    与 polars 过滤语义一致。

    粒度：默认按行登记（``last_trade_date`` 即该行日期），不整只排除；仅当非法行
    占比超过 :data:`BAD_OHLC_CODE_SHARE_LIMIT` 时额外登记一条汇总行（``expected_rows``
    > 1 为可识别的整只停用标记），由 :func:`is_tradable` 判为不可用。
    """
    rows: list[dict[str, Any]] = []
    for asset_class in ASSET_CLASSES:
        folder = store_root / 'quotes' / asset_class
        if not folder.is_dir():
            continue
        for path in sorted(folder.rglob('*.parquet')):
            ts_code = path.stem
            frame = pl.read_parquet(
                path, columns=['date', 'open', 'high', 'low', 'close']).sort('date')
            if frame.is_empty():
                continue
            opens = frame['open'].to_numpy()
            highs = frame['high'].to_numpy()
            lows = frame['low'].to_numpy()
            closes = frame['close'].to_numpy()
            masks: list[tuple[str, np.ndarray]] = [
                ('high<low', highs < lows),
                ('high<max(open,close)', highs < np.maximum(opens, closes)),
                ('low>min(open,close)', lows > np.minimum(opens, closes)),
                ('close<=0', closes <= 0),
                ('open<=0', opens <= 0),
            ]
            illegal = np.any(np.vstack([mask for _, mask in masks]), axis=0)
            indices = np.flatnonzero(illegal)
            if indices.size == 0:
                continue
            dates = frame['date'].to_list()
            for index in indices:
                labels = '/'.join(name for name, mask in masks if bool(mask[index]))
                when = dates[int(index)]
                rows.append({
                    'ts_code': ts_code,
                    'asset_class': asset_class,
                    'reason_code': 'bad_ohlc',
                    'reason_text': (
                        f'{REASON_TEXT["bad_ohlc"]}（{when.isoformat()} {labels}）'),
                    'last_trade_date': when,
                    'expected_rows': 1,
                    'actual_rows': 0,
                    'missing_rows': 1,
                    'severity': REASON_SEVERITY['bad_ohlc'],
                    'detected_at': as_of,
                })
            share = indices.size / frame.height
            if share > BAD_OHLC_CODE_SHARE_LIMIT:
                rows.append({
                    'ts_code': ts_code,
                    'asset_class': asset_class,
                    'reason_code': 'bad_ohlc',
                    'reason_text': (
                        f'{REASON_TEXT["bad_ohlc"]}（非法行 {indices.size}/{frame.height}'
                        f'，占比 {share:.2%}，超过 {BAD_OHLC_CODE_SHARE_LIMIT:.0%}，整只停用）'),
                    'last_trade_date': dates[int(indices[-1])],
                    'expected_rows': frame.height,
                    'actual_rows': frame.height - int(indices.size),
                    'missing_rows': int(indices.size),
                    'severity': REASON_SEVERITY['bad_ohlc'],
                    'detected_at': as_of,
                })
    return rows


def _classify_jump(
    item: dict[str, Any], wide_events: set[date],
) -> tuple[str, str]:
    if item['asset_class'] == 'index':
        return 'index_rebalance', '指数点位跳变（成分调整/基期重置）'
    if item['factor_changed'] or item['has_dividend']:
        evidence = (item['factor_evidence'] or item['split_evidence']
                    or '分红除权事件命中')
        return 'corporate_action_missed', evidence
    if item['gap_trading_days'] >= 5:
        return ('resumption_gap',
                f'前置停牌 {item["gap_trading_days"]} 个交易日')
    if item['date'] in wide_events:
        return 'market_wide_event', '同日多只股票同向跳变（市场共性事件）'
    if item['row_pos'] <= 4:
        return 'new_listing', '上市前 5 个交易日无涨跌幅限制'
    if item['date'] < PRICE_LIMIT_DATE:
        return 'pre_price_limit_era', '1996-12-16 前无涨跌幅限制，大幅波动属真实行情'
    return 'data_error', '无分红/因子变动，无长期停牌，孤立跳变'


def _empty_jump_frame() -> pl.DataFrame:
    return pl.DataFrame(schema={
        'asset_class': pl.Utf8, 'ts_code': pl.Utf8,
        'date': pl.Date, 'prev_date': pl.Date,
        'pre_close': pl.Float64, 'close': pl.Float64,
        'pct_chg': pl.Float64, 'gap_trading_days': pl.Int64,
        'row_pos': pl.Int64,
        'factor_changed': pl.Boolean, 'has_dividend': pl.Boolean,
        'factor_evidence': pl.Utf8, 'split_evidence': pl.Utf8,
        'label': pl.Utf8, 'evidence': pl.Utf8,
    })


def _jump_report(
    detail: pl.DataFrame, zero_pre_close: Mapping[str, int], as_of: date,
) -> dict[str, Any]:
    by_label: dict[str, Any] = {}
    samples: dict[str, Any] = {}
    for label in JUMP_LABELS:
        sub = detail.filter(pl.col('label') == label) if detail.height else detail
        by_label[label] = {
            'rows': sub.height,
            'codes': sub['ts_code'].n_unique() if sub.height else 0,
            'by_asset_class': {
                cls: sub.filter(pl.col('asset_class') == cls).height
                for cls in ASSET_CLASSES
            },
            'description': JUMP_LABEL_TEXT[label],
        }
        if sub.height:
            sample_rows = sub.sort(
                [pl.col('pct_chg').abs(), 'ts_code'], descending=[True, False]).head(5)
            samples[label] = [
                {
                    'ts_code': item['ts_code'],
                    'asset_class': item['asset_class'],
                    'date': item['date'].isoformat(),
                    'prev_date': item['prev_date'].isoformat(),
                    'pre_close': round(item['pre_close'], 4),
                    'close': round(item['close'], 4),
                    'pct_chg': round(item['pct_chg'], 2),
                    'gap_trading_days': item['gap_trading_days'],
                    'evidence': item['evidence'],
                }
                for item in sample_rows.iter_rows(named=True)
            ]
        else:
            samples[label] = []
    data_error = detail.filter(pl.col('label') == 'data_error') if detail.height else detail
    return {
        'generated_as_of': as_of.isoformat(),
        'threshold': (
            f'|close/pre_close - 1| > {JUMP_THRESHOLD}（排除 pre_close<=0 的首日行）'
        ),
        'total_rows': detail.height,
        'total_codes': detail['ts_code'].n_unique() if detail.height else 0,
        'by_label': by_label,
        'samples': samples,
        'pre_close_zero_rows': dict(zero_pre_close),
        'data_error_impact': {
            'codes': data_error['ts_code'].n_unique() if data_error.height else 0,
            'rows': data_error.height,
            'note': '仅 data_error 写入排除清单（reason_code=bad_price_jump，warn）',
        },
    }


# ----------------------------------------------------------------------
# 汇总
# ----------------------------------------------------------------------
def _coverage(di: pl.DataFrame, excluded: set[str]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for cls in ASSET_CLASSES:
        sub = di.filter(pl.col('data_type') == cls)
        total_codes = sub.height
        total_rows = int(sub['rows'].sum())
        if excluded:
            remain = sub.filter(~pl.col('ts_code').is_in(sorted(excluded)))
        else:
            remain = sub
        result[cls] = {
            'total_codes': total_codes,
            'total_rows': total_rows,
            'remaining_codes': remain.height,
            'remaining_rows': int(remain['rows'].sum()) if remain.height else 0,
            'code_retention': round(remain.height / total_codes, 4) if total_codes else 0.0,
        }
    result['all'] = {
        'total_codes': di.height,
        'total_rows': int(di['rows'].sum()),
        'remaining_codes': sum(v['remaining_codes'] for k, v in result.items() if k != 'all'),
        'remaining_rows': sum(v['remaining_rows'] for k, v in result.items() if k != 'all'),
        'code_retention': 0.0,
    }
    total = result['all']['total_codes']
    result['all']['code_retention'] = (
        round(result['all']['remaining_codes'] / total, 4) if total else 0.0)
    return result


def _strict_excluded_codes(rows: pl.DataFrame) -> set[str]:
    """按 :func:`is_tradable` 的 ``strict=True`` 口径汇总会被剔除的标的。"""
    codes: set[str] = set()
    for item in rows.iter_rows(named=True):
        reason = item['reason_code']
        if (reason in _BLOCKING_REASONS
                or reason in ('stale_feed', 'bad_price_jump')
                or (reason == 'bad_ohlc' and item['expected_rows'] > 1)
                or (reason in _STRICT_WARNINGS and not _strict_ignorable(item))):
            codes.add(item['ts_code'])
    return codes


def _summarize(
    rows: pl.DataFrame, di: pl.DataFrame, as_of: date,
    sources: Mapping[str, Any],
    price_jumps: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    total_codes = di.height
    reasons: dict[str, Any] = {}
    for reason in REASON_SEVERITY:
        sub = rows.filter(pl.col('reason_code') == reason)
        if sub.height == 0:
            continue
        codes = sub['ts_code'].n_unique()
        by_class = {
            cls: sub.filter(pl.col('asset_class') == cls)['ts_code'].n_unique()
            for cls in ASSET_CLASSES
        }
        by_market = {
            market: sub.filter(pl.col('ts_code').str.ends_with(market))['ts_code'].n_unique()
            for market in ('SH', 'SZ', 'BJ')
        }
        reasons[reason] = {
            'severity': REASON_SEVERITY[reason],
            'codes': codes,
            'code_share': round(codes / total_codes, 4) if total_codes else 0.0,
            'rows_missing': int(sub['missing_rows'].sum()),
            'by_asset_class': {k: v for k, v in by_class.items() if v},
            'by_market': {k: v for k, v in by_market.items() if v},
        }
    blocked = set(rows.filter(pl.col('severity') == 'block')['ts_code'].to_list())
    all_codes = set(rows['ts_code'].to_list())
    multi = int(rows.group_by('ts_code').len().filter(pl.col('len') > 1).height)
    return {
        'generated_as_of': as_of.isoformat(),
        'detected_at_rule': 'detected_at 取数据快照日，保证生成幂等',
        'total_codes': total_codes,
        'total_rows': int(di['rows'].sum()),
        'reasons': reasons,
        'excluded_union': {
            'blocking_codes': len(blocked),
            'all_reason_codes': len(all_codes),
            'multi_reason_codes': multi,
        },
        'coverage_after_exclusion': {
            'block_only': _coverage(di, blocked),
            'all_reasons': _coverage(di, all_codes),
            'strict': _coverage(di, _strict_excluded_codes(rows)),
        },
        'price_jumps': dict(price_jumps) if price_jumps is not None else None,
        'sources': dict(sources),
    }


# ----------------------------------------------------------------------
# 生成入口
# ----------------------------------------------------------------------
def build_exclusions(
    store_root: Path | str = DEFAULT_ROOT,
    *,
    tdx_root: Path | str | None = None,
    basic_tables: Mapping[str, Iterable[str]] | None = None,
    output_dir: Path | str | None = None,
    as_of: date | str | None = None,
) -> dict[str, Any]:
    """生成排除清单（parquet + json），返回汇总字典。

    Args:
        store_root: 本地列存根目录。
        tdx_root: 通达信 ``vipdoc`` 目录；``None`` 时跳过缺口/截断两类。
        basic_tables: ``{'stock': [...], 'index': [...], 'fund': [...]}``；
            ``None`` 时跳过 ``no_quotes`` / ``no_basic_info`` 两类。
        output_dir: 输出目录，默认 ``<store_root>/meta/universe``。
        as_of: 数据快照日，默认取 ``daily_index`` 的最大末日。

    Returns:
        汇总字典（与写入的 JSON 内容一致）。
    """
    root = _resolve_root(store_root)
    di = pl.read_parquet(root / 'meta' / 'daily_index.parquet')
    snapshot = _as_date(as_of) or max(di['end'].drop_nulls().to_list())

    rows: list[dict[str, Any]] = []
    rows.extend(_placeholder_rows(root, snapshot))
    rows.extend(_stale_rows(di, root, snapshot))
    if basic_tables is not None:
        rows.extend(_basic_rows(di, basic_tables, snapshot))
    if tdx_root is not None:
        gap, covered = _continuous_gap_rows(root, snapshot)
        rows.extend(gap)
        gap, short = _tdx_rows(di, root, _resolve_root(tdx_root), snapshot, covered)
        rows.extend(gap)
        rows.extend(short)
    rows.extend(_bad_ohlc_rows(root, snapshot))
    jump_rows, jump_detail, jump_report = _price_jumps(root, snapshot)
    rows.extend(jump_rows)

    frame = pl.DataFrame(rows, schema=EXCLUSION_SCHEMA) if rows else _empty_frame()
    frame = frame.sort(
        ['asset_class', 'reason_code', 'ts_code', 'last_trade_date'], nulls_last=True)
    out_dir = Path(output_dir) if output_dir is not None else _universe_dir(root)
    out_dir.mkdir(parents=True, exist_ok=True)
    sources = {
        'store_root': str(root),
        'tdx_root': str(_resolve_root(tdx_root)) if tdx_root is not None else None,
        'basic_tables': 'provided' if basic_tables is not None else 'skipped',
        'price_jumps_detail': str(out_dir / PRICE_JUMPS_FILE),
    }
    summary = _summarize(frame, di, snapshot, sources, jump_report)
    write_parquet_atomic(frame, out_dir / EXCLUSIONS_FILE)
    write_parquet_atomic(jump_detail, out_dir / PRICE_JUMPS_FILE)
    write_text_atomic(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True),
        out_dir / SUMMARY_FILE)
    # 写盘后按磁盘实测回写同目录 manifest；required=True 要求三项都已登记，
    # 漏登记即报错（DG-3），不再静默跳过。
    from src.data.manifest_refresh import refresh_manifest_files  # noqa: PLC0415

    refresh_manifest_files(
        out_dir / 'manifest.json', root,
        [out_dir / EXCLUSIONS_FILE, out_dir / PRICE_JUMPS_FILE, out_dir / SUMMARY_FILE],
        required=True)
    # 审计参考值，非口径：2026-11-10 起对外口径统一为 strict，该值只用于与
    # 历史报告（healthy_by_audit_definition，旧值 8,903/9,770）对照，不写入 JSON。
    audit_codes = set(frame.filter(
        pl.col('reason_code').is_in(['placeholder_price', 'stale_feed', 'internal_gap'])
    )['ts_code'].to_list())
    logger.info(
        '排除清单已生成: {} 行 / {} 只标的，strict 可交易 {}/{} 只；'
        '审计参考值（非口径，不写 JSON）{} 只',
        frame.height, summary['excluded_union']['all_reason_codes'],
        summary['coverage_after_exclusion']['strict']['all']['remaining_codes'],
        summary['coverage_after_exclusion']['strict']['all']['total_codes'],
        _coverage(di, audit_codes)['all']['remaining_codes'])
    return summary


# ----------------------------------------------------------------------
# 读侧：纯读、无副作用
# ----------------------------------------------------------------------
def load_exclusions(
    root: Path | str = DEFAULT_ROOT, *, use_cache: bool = True,
) -> pl.DataFrame:
    """读取排除清单。

    Args:
        root: 列存根目录或 ``meta/universe`` 目录；文件不存在时返回空表。
        use_cache: 同一进程内按文件 mtime 缓存。

    Returns:
        列为 :data:`EXCLUSION_SCHEMA` 的 DataFrame。
    """
    path = _exclusions_path(root)
    if not path.is_file():
        return _empty_frame()
    key = str(path)
    mtime = path.stat().st_mtime_ns
    with _LOCK:
        cached = _CACHE.get(key)
        if use_cache and cached is not None and cached[0] == mtime:
            return cached[1]
    frame = pl.read_parquet(path)
    missing_cols = set(EXCLUSION_SCHEMA) - set(frame.columns)
    if missing_cols:
        raise ValueError(f'排除清单缺少列: {sorted(missing_cols)}')
    with _LOCK:
        _CACHE[key] = (mtime, frame)
    return frame


def exclusion_reasons(
    ts_code: str,
    as_of: date | str | None = None,
    *,
    root: Path | str = DEFAULT_ROOT,
    exclusions: pl.DataFrame | None = None,
) -> list[dict[str, Any]]:
    """返回该标的在 ``as_of`` 当天仍然成立的原因行（纯读）。"""
    code = to_ts_code(ts_code)
    frame = load_exclusions(root) if exclusions is None else exclusions
    if frame.height == 0:
        return []
    hits = frame.filter(pl.col('ts_code') == code)
    if hits.height == 0:
        return []
    target = _as_date(as_of)
    result: list[dict[str, Any]] = []
    for item in hits.iter_rows(named=True):
        last_trade = item['last_trade_date']
        if (item['reason_code'] == 'stale_feed' and target is not None
                and last_trade is not None and target < last_trade):
            continue
        if (item['reason_code'] == 'bad_price_jump'
                and (target is None or last_trade is None or target != last_trade)):
            continue
        if item['reason_code'] == 'bad_ohlc':
            # 行级登记只在当天成立；``expected_rows > 1`` 是整只停用汇总行，始终生效
            if item['expected_rows'] <= 1 and (
                    target is None or last_trade is None or target != last_trade):
                continue
        result.append(item)
    return result


def is_tradable(
    ts_code: str,
    as_of: date | str | None = None,
    *,
    root: Path | str = DEFAULT_ROOT,
    strict: bool = False,
    exclusions: pl.DataFrame | None = None,
) -> bool:
    """该标的在 ``as_of`` 是否可用。

    规则：
    - ``placeholder_price`` / ``no_quotes`` 永久不可用；
    - ``stale_feed`` 仅在 ``as_of >= last_trade_date`` 时不可用（历史日期仍可用）；
    - ``bad_ohlc`` 默认只标记非法行、不影响整只票；仅当非法行占比超过
      :data:`BAD_OHLC_CODE_SHARE_LIMIT` 登记了整只停用汇总行时才不可用；
    - ``internal_gap`` / ``no_basic_info`` 仅 ``strict=True`` 时不可用；其中通达信
      自编指数（``880*`` / ``881*`` 板块与行业指数）的 ``internal_gap`` 只记录、
      不剔除——这类指数不可投资、历史本身不完整；
    - ``tdx_truncated`` 只约束"禁止用 TDX 重建"，不影响本地数据可用性。

    Args:
        ts_code: 标准代码或可转换的代码。
        as_of: 查询日期；``None`` 视为最新。
        root: 列存根目录或清单目录。
        strict: 是否按严格口径剔除历史缺口与元数据缺失。
        exclusions: 预加载的清单，避免重复读盘。
    """
    reasons = exclusion_reasons(ts_code, as_of, root=root, exclusions=exclusions)
    if not reasons:
        return True
    if any(item['reason_code'] in _BLOCKING_REASONS for item in reasons):
        return False
    if any(item['reason_code'] in ('stale_feed', 'bad_price_jump') for item in reasons):
        return False
    if any(item['reason_code'] == 'bad_ohlc' and item['expected_rows'] > 1
           for item in reasons):
        return False
    strict_hits = [
        item for item in reasons
        if item['reason_code'] in _STRICT_WARNINGS and not _strict_ignorable(item)
    ]
    return not (strict and strict_hits)
