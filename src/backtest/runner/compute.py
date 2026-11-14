"""规格驱动层的数据装载、因子面板与剔除条件实现（只读消费正式数据）。

本模块不包含撮合、成本计算或下单腿逻辑：行情读取走 ``DataFetcher`` 的既有
接口，撮合与成本全部在 ``src/backtest`` 既有引擎内完成，这里只产出
「调仓日 → 成分」与「调仓日 → 信号」。

因子与条件的实现口径逐条沿用 ``tools/factor_lowvol_quantile.py``、
``tools/factor_exclusion_list.py``、``tools/factor_value_quantile.py`` 的构造，
以便与已冻结基线逐值对齐。
"""
from __future__ import annotations

import glob
import math
from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import polars as pl
from loguru import logger

from src.data.managers.data_fetcher import DataFetcher
from src.utils.config import Config

from .errors import SpecError

ROOT = Path(__file__).resolve().parents[3]
STOCK_QUOTES_GLOB = 'data/local_store/quotes/stock/**/*.parquet'
VALUATION = ROOT / 'data/local_store/meta/financials/valuation_daily.parquet'
STOCK_BASIC = ROOT / 'data/local_store/meta/stock_basic.parquet'

FREQUENCY_MONTHS: Dict[str, Tuple[int, ...]] = {
    'monthly': tuple(range(1, 13)),
    'quarterly': (3, 6, 9, 12),
    'semi': (6, 12),
}


# ---------------------------------------------------------------------------
# 行情与日历
# ---------------------------------------------------------------------------

def local_stock_codes() -> List[str]:
    """本地列存里的全部 A 股代码（按文件名去重排序）。"""
    files = glob.glob(str(ROOT / STOCK_QUOTES_GLOB), recursive=True)
    return sorted({Path(p).stem for p in files})


def load_universe(period: Dict[str, str], universe: Dict[str, Any]
                  ) -> Dict[str, pl.DataFrame]:
    """按规格读取后复权行情；``explicit`` 模式只读指定清单。"""
    available = set(local_stock_codes())
    mode = universe['mode']
    if mode == 'explicit':
        codes = list(universe['codes'])
        missing = sorted(set(codes) - available)
        if missing:
            raise SpecError(
                f'股票池指定的代码在本地列存中不存在：{missing[:20]}'
                f'（清单共 {len(codes)} 只，本地 {len(available)} 只）')
    else:
        codes = sorted(available)
    logger.info('读取行情：{} 只 / {}~{}（后复权，data_start={}）', len(codes),
                period['start'], period['end'], period['warmup_start'])
    batch = DataFetcher(Config(), db_manager=None).get_stock_batch(
        codes, period['warmup_start'], period['end'], '1d', 'hfq')
    data = {c: df for c, df in batch.items() if df is not None and not df.is_empty()}
    empty = sorted(set(codes) - set(data))
    if mode == 'explicit' and empty:
        raise SpecError(f'股票池指定代码无可用行情：{empty[:20]}')
    logger.info('行情读取完成：{} 只 / {} 行', len(data),
                sum(df.height for df in data.values()))
    return data


def drop_contract_violations(data: Dict[str, pl.DataFrame]
                             ) -> Tuple[Dict[str, pl.DataFrame], List[Dict[str, Any]]]:
    """沿用引擎行情契约做预筛；不改契约、不改引擎，只剔除并记录。"""
    from src.backtest.engine.input_contract import (
        BacktestInputContractError,
        validate_universe,
    )

    kept: Dict[str, pl.DataFrame] = {}
    violations: List[Dict[str, Any]] = []
    for code, df in data.items():
        try:
            validate_universe({code: df})
        except BacktestInputContractError as exc:
            violations.append({
                'code': code, 'rows': df.height,
                'start': str(df['date'].min())[:10],
                'end': str(df['date'].max())[:10],
                'reason': str(exc).replace(code, '').strip()[:120],
            })
            continue
        kept[code] = df
    if violations:
        logger.warning('行情契约预筛剔除 {} 只：{}', len(violations),
                       [v['code'] for v in violations[:20]])
    return kept, violations


def build_calendar(data: Dict[str, pl.DataFrame]
                   ) -> Tuple[List[date], List[date], Dict[date, date]]:
    all_dates = pl.concat([df.select('date') for df in data.values()])
    calendar: List[date] = all_dates.unique().sort('date')['date'].to_list()
    months: Dict[Tuple[int, int], date] = {}
    for day in calendar:
        months[(day.year, day.month)] = day
    month_ends = sorted(months.values())
    prev_day = {day: calendar[i - 1] for i, day in enumerate(calendar) if i}
    return calendar, month_ends, prev_day


def rebalance_days(month_ends: List[date], frequency: str,
                   first_date: date) -> List[date]:
    months = FREQUENCY_MONTHS[frequency]
    return [d for d in month_ends if d.month in months and d >= first_date]


def quoted_sets(data: Dict[str, pl.DataFrame],
                days: List[date]) -> Dict[date, Set[str]]:
    """每个调仓日有行情（当日有 bar）的标的集合。"""
    me = np.array(days, dtype='datetime64[D]')
    out: Dict[date, Set[str]] = {d: set() for d in days}
    for code, df in data.items():
        dates = df['date'].to_numpy()
        raw = np.searchsorted(dates, me)
        idx = np.minimum(raw, dates.size - 1)
        for j in np.nonzero((raw < dates.size) & (dates[idx] == me))[0]:
            out[days[j]].add(code)
    return out


def load_trading_days() -> List[date]:
    """全历史 A 股交易日（估值表 trade_date 即交易日），供上市天数与预热换算。"""
    df = pl.scan_parquet(VALUATION).select('trade_date').unique().collect()
    return sorted(df['trade_date'].to_list())


# ---------------------------------------------------------------------------
# 计算上下文与缓存
# ---------------------------------------------------------------------------

@dataclass
class Context:
    """一次运行的只读上下文；因子面板、估值/上市日等按需懒加载并缓存。"""

    data: Dict[str, pl.DataFrame]
    days: List[date]
    quoted: Dict[date, Set[str]]
    st_status: Dict[str, Any]
    allowed: Set[str] | None = None
    _factors: Dict[Tuple[str, Tuple[Tuple[str, Any], ...]], Dict[date, Dict[str, float]]] = field(
        default_factory=dict)
    _valuation: Dict[str, Tuple[np.ndarray, np.ndarray, np.ndarray]] | None = None
    _list_dates: Dict[str, date] | None = None
    _trading_days: List[date] | None = None
    _amount_arrays: Dict[str, Tuple[np.ndarray, np.ndarray]] | None = None

    def factor(self, name: str, params: Dict[str, Any]) -> Dict[date, Dict[str, float]]:
        key = (name, tuple(sorted(params.items())))
        if key not in self._factors:
            self._factors[key] = _FACTOR_FUNCS[name](self, params)
        return self._factors[key]

    def valuation(self) -> Dict[str, Tuple[np.ndarray, np.ndarray, np.ndarray]]:
        if self._valuation is None:
            df = pl.scan_parquet(VALUATION).select(
                ['ts_code', 'trade_date', 'pe', 'pb']).filter(
                pl.col('pe').is_not_null() | pl.col('pb').is_not_null()).collect()
            df = df.sort(['ts_code', 'trade_date'])
            codes = df['ts_code'].to_numpy()
            starts = np.flatnonzero(np.r_[True, codes[1:] != codes[:-1]])
            ends = np.r_[starts[1:], len(codes)]
            dts = df['trade_date'].to_numpy().astype('datetime64[D]')
            pe = df['pe'].to_numpy().astype('float64')
            pb = df['pb'].to_numpy().astype('float64')
            self._valuation = {
                str(codes[a]): (dts[a:b], pe[a:b], pb[a:b])
                for a, b in zip(starts, ends, strict=True)}
            logger.info('估值表：{} 只 / {} 行', len(self._valuation), df.height)
        return self._valuation

    def list_dates(self) -> Dict[str, date]:
        if self._list_dates is None:
            basic = pl.read_parquet(STOCK_BASIC, columns=['ts_code', 'list_date'])
            self._list_dates = dict(basic.filter(
                pl.col('list_date').is_not_null()).select(
                ['ts_code', 'list_date']).iter_rows())
        return self._list_dates

    def trading_days(self) -> List[date]:
        if self._trading_days is None:
            self._trading_days = load_trading_days()
        return self._trading_days

    def amount_arrays(self) -> Dict[str, Tuple[np.ndarray, np.ndarray]]:
        if self._amount_arrays is None:
            out: Dict[str, Tuple[np.ndarray, np.ndarray]] = {}
            for code, df in self.data.items():
                if 'amount' not in df.columns:
                    continue
                out[code] = (df['date'].to_numpy().astype('datetime64[D]'),
                             df['amount'].to_numpy().astype('float64'))
            self._amount_arrays = out
        return self._amount_arrays


# ---------------------------------------------------------------------------
# 因子面板
# ---------------------------------------------------------------------------

def vol_panel(ctx: Context, params: Dict[str, Any]) -> Dict[date, Dict[str, float]]:
    """信号日为止最近 ``window`` 个交易日 hfq 对数收益的样本标准差。

    窗口末端必须恰好是调仓日、窗口内全部收益有效；不足则当日无因子值。
    """
    window = int(params['window'])
    me = np.array(ctx.days, dtype='datetime64[D]')
    panel: Dict[date, Dict[str, float]] = {d: {} for d in ctx.days}
    skipped = 0
    for code, df in ctx.data.items():
        dates = df['date'].to_numpy()
        close = df['close'].to_numpy().astype('float64')
        m = close.size
        if m <= window:
            skipped += 1
            continue
        with np.errstate(divide='ignore', invalid='ignore'):
            ret = np.diff(np.log(np.where(close > 0, close, np.nan)))
        bad = ~np.isfinite(ret)
        r = np.nan_to_num(ret, nan=0.0, posinf=0.0, neginf=0.0)
        cs1 = np.concatenate(([0.0], np.cumsum(r)))
        cs2 = np.concatenate(([0.0], np.cumsum(r * r)))
        csb = np.concatenate(([0], np.cumsum(bad)))
        pos = np.searchsorted(dates, me)
        idx = np.minimum(pos, m - 1)
        hit = np.nonzero((pos < m) & (dates[idx] == me))[0]
        for j in hit:
            k = int(idx[j])
            a = k - window
            if a < 0 or csb[k] - csb[a] != 0:
                continue
            n = window
            mean = (cs1[k] - cs1[a]) / n
            var = (cs2[k] - cs2[a] - n * mean * mean) / (n - 1)
            if var <= 0:
                continue
            panel[ctx.days[j]][code] = float(np.sqrt(var))
    logger.info('因子 vol(窗口 {} 日)：{} 个调仓日，行数不足跳过 {} 只',
                window, len(ctx.days), skipped)
    return panel


def _value_panel(ctx: Context, column: str) -> Dict[date, Dict[str, float]]:
    """估值因子截面（1/pe 或 1/pb），只取当日有行情且估值有限为正的标的。"""
    val = ctx.valuation()
    out: Dict[date, Dict[str, float]] = {d: {} for d in ctx.days}
    col = 1 if column == 'pe' else 2
    for day in ctx.days:
        me = np.datetime64(day)
        for code in ctx.quoted[day]:
            pair = val.get(code)
            if pair is None:
                continue
            dts, pes, pbs = pair
            k = bisect_right(dts, me) - 1
            if k < 0:
                continue
            raw = pes[k] if col == 1 else pbs[k]
            if np.isfinite(raw) and raw > 0:
                out[day][code] = float(1.0 / raw)
    return out


def bp_panel(ctx: Context, params: Dict[str, Any]) -> Dict[date, Dict[str, float]]:
    del params
    panel = _value_panel(ctx, 'pb')
    logger.info('因子 bp：{} 个调仓日', len(panel))
    return panel


def ep_panel(ctx: Context, params: Dict[str, Any]) -> Dict[date, Dict[str, float]]:
    del params
    panel = _value_panel(ctx, 'pe')
    logger.info('因子 ep：{} 个调仓日', len(panel))
    return panel


# ---------------------------------------------------------------------------
# 分位切分
# ---------------------------------------------------------------------------

def unit_fraction_buckets(pct: float, label: str) -> int:
    """把「最高/最低 pct」换算成档数 n（pct 必须是 1/n，n 取 2~20）。"""
    if not (0.0 < pct < 1.0):
        raise SpecError(f'{label} 的 pct={pct} 越界，必须在 (0, 1) 内')
    n = int(round(1.0 / pct))
    if n < 2 or n > 20 or abs(n * pct - 1.0) > 1e-9:
        raise SpecError(
            f'{label} 的 pct={pct} 无法换算成整档：请写成 1/n（n=2~20），'
            f'例如 20% → 5 档、10% → 10 档')
    return n


def bucket_slices(values: Dict[str, float], buckets: int,
                  min_names: int, direction: str = 'asc') -> List[List[str]]:
    # 同值按代码升序打破平局，保证跨进程（不同 hash 种子）结果一致
    items = sorted(values.items(), key=lambda kv: (kv[1], kv[0]))
    if direction == 'desc':
        items.reverse()
    n = len(items)
    if n < min_names:
        return []
    out: List[List[str]] = [[] for _ in range(buckets)]
    for rank, (code, _) in enumerate(items):
        out[min(buckets - 1, rank * buckets // n)].append(code)
    return out


# ---------------------------------------------------------------------------
# 剔除条件（返回当日要剔除的集合；返回 None 表示当日横截面不足、跳过该调仓日）
# ---------------------------------------------------------------------------

def exclude_vol_high_pct(ctx: Context, params: Dict[str, Any], day: date,
                         base: Set[str]) -> Set[str] | None:
    nq = unit_fraction_buckets(params['pct'], '剔除波动率最高 pct')
    panel = ctx.factor('vol', {'window': params['window']})
    if day not in panel:
        return None
    buckets = bucket_slices(panel[day], nq, params['min_names'])
    if not buckets:
        return None
    return set(buckets[-1]) & base


def exclude_bp_expensive_pct(ctx: Context, params: Dict[str, Any], day: date,
                             base: Set[str]) -> Set[str] | None:
    nq = unit_fraction_buckets(params['pct'], '剔除 BP 最贵 pct')
    panel = ctx.factor('bp', {})
    if day not in panel:
        return None
    buckets = bucket_slices(panel[day], nq, params['min_names'])
    if not buckets:
        return None
    return set(buckets[0]) & base


def exclude_new_listing(ctx: Context, params: Dict[str, Any], day: date,
                        base: Set[str]) -> Set[str]:
    list_dates = ctx.list_dates()
    all_days = ctx.trading_days()
    idx = bisect_right(all_days, day)
    out: Set[str] = set()
    for code in base:
        ld = list_dates.get(code)
        if ld is None:
            arr = ctx.data[code]['date'].to_numpy()
            ld = date.fromisoformat(str(arr[0])[:10])
        if idx - bisect_right(all_days, ld) < params['min_trading_days']:
            out.add(code)
    return out


def exclude_st(ctx: Context, params: Dict[str, Any], day: date,
               base: Set[str]) -> Set[str]:
    del params
    stamp = day.isoformat()
    return {code for code in base if _st_at(ctx.st_status.get(code), stamp)}


def exclude_low_liquidity_pct(ctx: Context, params: Dict[str, Any], day: date,
                              base: Set[str]) -> Set[str]:
    window = int(params['window'])
    arrays = ctx.amount_arrays()
    day_np = np.datetime64(day)
    amt: List[Tuple[str, float]] = []
    for code in base:
        pair = arrays.get(code)
        if pair is None:
            continue
        dts, vals = pair
        k = int(np.searchsorted(dts, day_np, side='right')) - 1
        if k < window - 1:
            continue
        window_vals = vals[k - window + 1:k + 1]
        window_vals = window_vals[np.isfinite(window_vals)]
        if len(window_vals) == window:
            amt.append((code, float(window_vals.mean())))
    amt.sort(key=lambda kv: (kv[1], kv[0]))
    return {code for code, _ in amt[:int(len(amt) * params['pct'])]}


def exclude_pe_negative_or_expensive_pct(ctx: Context, params: Dict[str, Any], day: date,
                                         base: Set[str]) -> Set[str]:
    val = ctx.valuation()
    me = np.datetime64(day)
    values: List[Tuple[str, float]] = []
    for code in base:
        pair = val.get(code)
        if pair is None:
            continue
        dts, pes, _ = pair
        k = bisect_right(dts, me) - 1
        if k < 0:
            continue
        pe = pes[k]
        if np.isfinite(pe):
            values.append((code, float(pe)))
    positive = np.array([x for _, x in values if x > 0])
    cut = float(np.quantile(positive, params['upper_quantile'])) if positive.size else math.inf
    return {code for code, x in values if x < 0 or x > cut}


def _st_at(entry: Any, day: str) -> bool:
    if not entry:
        return False
    if isinstance(entry, bool):
        return entry
    for seg in entry:
        if seg['start_date'] <= day <= seg['end_date']:
            return bool(seg['is_st'])
    return False


_FACTOR_FUNCS = {'vol': vol_panel, 'bp': bp_panel, 'ep': ep_panel}
