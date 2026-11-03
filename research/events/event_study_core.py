#!/usr/bin/env python3

"""事件研究核心：行情装载、市场模型、CAR、统计与 Bootstrap。

设计约束（与项目规则一致）::

- 只读本地列存 ``data/local_store``，不发起任何远程行情查询；
- 量化口径统一使用后复权价 ``close * hfq_factor``，避免除权跳空污染收益；
- 事件日 ``day 0`` 定义为「信息可交易的第一天」：统一取公告/新闻发布日期的
  下一个交易日，建仓价取该日开盘价（保守口径，盘后/夜间事件自然满足）；
- 估计窗 ``[-120, -20]``，事件窗 ``[-5, +20]``，主口径为市场模型
  （alpha/beta 在估计窗 OLS 估计），同时给出 beta=1 的市场调整口径作对照。
"""

from __future__ import annotations

import math
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[2]
STORE_ROOT = ROOT / 'data' / 'local_store'
HORIZONS = (0, 1, 5, 20)
EST_START = -120
EST_END = -20
MIN_EST_OBS = 60
RNG_SEED = 42
BOOTSTRAP_ROUNDS = 1000


@dataclass
class PriceSeries:
    """单标的按日期升序的复权价格序列。"""

    ts_code: str
    dates: list[date]
    adj_open: np.ndarray
    adj_close: np.ndarray


def load_price_series(ts_code: str, kind: str = 'stock') -> PriceSeries | None:
    """从本地列存读取后复权价格序列，缺失或有效样本不足时返回 None。"""
    symbol, market = ts_code.split('.')[0], ts_code.split('.')[-1].lower()
    path = STORE_ROOT / 'quotes' / kind / market / f'{symbol}.{market.upper()}.parquet'
    if not path.exists():
        return None
    frame = pl.read_parquet(path, columns=['date', 'open', 'close', 'hfq_factor'])
    frame = frame.drop_nulls(['date', 'open', 'close', 'hfq_factor'])
    frame = frame.filter((pl.col('open') > 0) & (pl.col('close') > 0))
    if frame.height < 40:
        return None
    frame = frame.sort('date')
    factor = frame['hfq_factor'].to_numpy().astype(float)
    return PriceSeries(
        ts_code=ts_code,
        dates=frame['date'].to_list(),
        adj_open=frame['open'].to_numpy().astype(float) * factor,
        adj_close=frame['close'].to_numpy().astype(float) * factor,
    )


def _returns(close: np.ndarray) -> np.ndarray:
    out = np.full(close.shape, np.nan)
    out[1:] = close[1:] / close[:-1] - 1.0
    return out


class Benchmark:
    """基准指数序列，用于市场模型与牛熊状态判定。"""

    def __init__(self, ts_code: str = '000300.SH') -> None:
        series = load_price_series(ts_code, kind='index')
        if series is None:
            raise RuntimeError(f'基准指数缺失: {ts_code}')
        self.ts_code = ts_code
        self.dates = series.dates
        self.open = series.adj_open
        self.close = series.adj_close
        self.returns = _returns(self.close)
        self.ret_by_date = {d: float(r) for d, r in zip(self.dates, self.returns, strict=False)}
        self.index_by_date = {d: i for i, d in enumerate(self.dates)}

    def next_trading_day(self, publish: datetime | date) -> date | None:
        """严格晚于发布日期（自然日）的第一个交易日。"""
        day = publish.date() if isinstance(publish, datetime) else publish
        idx = bisect_right(self.dates, day)
        return self.dates[idx] if idx < len(self.dates) else None

    def close_on(self, day: date) -> float | None:
        idx = bisect_left(self.dates, day)
        if idx < len(self.dates) and self.dates[idx] == day:
            return float(self.close[idx])
        return None

    def regime(self, day: date, ma_window: int = 250) -> str:
        """牛熊状态：收盘价高于前 ma_window 日均值记为 bull。"""
        idx = self.index_by_date.get(day)
        if idx is None:
            idx = bisect_left(self.dates, day) - 1
        if idx < ma_window or idx >= len(self.close):
            return 'unknown'
        ma = float(np.mean(self.close[idx - ma_window:idx]))
        return 'bull' if self.close[idx] > ma else 'bear'


def _ols_market_model(stock_ret: np.ndarray, mkt_ret: np.ndarray) -> tuple[float, float] | None:
    if len(stock_ret) < MIN_EST_OBS:
        return None
    var = float(np.var(mkt_ret, ddof=1))
    if var <= 0:
        return None
    beta = float(np.cov(stock_ret, mkt_ret, ddof=1)[0, 1] / var)
    alpha = float(np.mean(stock_ret) - beta * np.mean(mkt_ret))
    return alpha, beta


def compute_event_car(
    series: PriceSeries,
    benchmark: Benchmark,
    publish: datetime | date,
    horizons: tuple[int, ...] = HORIZONS,
    trade_day: date | None = None,
) -> dict[str, Any] | None:
    """计算单个事件的 CAR；样本不足或日期无法对齐时返回 None。

    ``trade_day`` 显式指定时直接作为事件日 0（用于两会等「开盘前已知」事件），
    否则统一取发布日期的下一个交易日。
    """
    if trade_day is None:
        trade_day = benchmark.next_trading_day(publish)
    if trade_day is None:
        return None
    i0 = bisect_left(series.dates, trade_day)
    if i0 >= len(series.dates) or series.dates[i0] != trade_day:
        return None
    if i0 < abs(EST_START) + 1 or i0 + max(horizons) >= len(series.dates):
        return None

    stock_ret = _returns(series.adj_close)
    est_lo = i0 + EST_START
    est_hi = i0 + EST_END + 1
    mkt_est = np.array([benchmark.ret_by_date.get(d, np.nan) for d in series.dates[est_lo:est_hi]])
    stock_est = stock_ret[est_lo:est_hi]
    mask = ~(np.isnan(stock_est) | np.isnan(mkt_est))
    model = _ols_market_model(stock_est[mask], mkt_est[mask])
    if model is None:
        return None
    alpha, beta = model

    bench_idx = benchmark.index_by_date[trade_day]
    if bench_idx >= len(benchmark.close) or bench_idx + max(horizons) >= len(benchmark.close):
        return None
    mkt_open_ret = float(benchmark.close[bench_idx] / benchmark.open[bench_idx] - 1.0)
    stock_open_ret = float(series.adj_close[i0] / series.adj_open[i0] - 1.0)
    ar0 = stock_open_ret - (alpha + beta * mkt_open_ret)
    ar_market_adj0 = stock_open_ret - mkt_open_ret

    cars = {0: ar0}
    cars_market_adj = {0: ar_market_adj0}
    cumulative = ar0
    cumulative_adj = ar_market_adj0
    for horizon in range(1, max(horizons) + 1):
        mkt_ret = float(benchmark.returns[bench_idx + horizon])
        stock_r = float(stock_ret[i0 + horizon])
        cumulative += stock_r - (alpha + beta * mkt_ret)
        cumulative_adj += stock_r - mkt_ret
        if horizon in horizons:
            cars[horizon] = cumulative
            cars_market_adj[horizon] = cumulative_adj

    return {
        'trade_day': trade_day,
        'alpha': alpha,
        'beta': beta,
        'n_est': int(mask.sum()),
        'regime': benchmark.regime(trade_day),
        **{f'car{h}': cars[h] for h in horizons},
        **{f'car_ma{h}': cars_market_adj[h] for h in horizons},
    }


def bootstrap_ci(values: np.ndarray, rounds: int = BOOTSTRAP_ROUNDS) -> tuple[float, float]:
    if len(values) < 2:
        return (float('nan'), float('nan'))
    rng = np.random.default_rng(RNG_SEED)
    means = np.array([np.mean(rng.choice(values, size=len(values), replace=True)) for _ in range(rounds)])
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def _t_stat(values: np.ndarray) -> tuple[float, float, float]:
    n = len(values)
    mean = float(np.mean(values))
    if n < 2:
        return mean, float('nan'), float('nan')
    std = float(np.std(values, ddof=1))
    if std == 0:
        return mean, float('nan'), float('nan')
    t_value = mean / (std / math.sqrt(n))
    try:
        from scipy import stats

        p_value = float(2 * stats.t.sf(abs(t_value), df=n - 1))
    except Exception:  # noqa: BLE001 - scipy 不可用时仅返回 t 值
        p_value = float('nan')
    return mean, t_value, p_value


def summarize(
    frame: pl.DataFrame,
    group_cols: list[str],
    horizons: tuple[int, ...] = HORIZONS,
    value_template: str = 'car{h}',
    regime_col: str = 'regime',
) -> pl.DataFrame:
    """按事件类型 × 窗口汇总：均值、中位数、t 值、胜率、Bootstrap 区间、牛熊分段。"""
    rows: list[dict[str, Any]] = []
    if frame.height == 0:
        return pl.DataFrame()
    for keys, group in frame.group_by(group_cols, maintain_order=True):
        key_map = dict(zip(group_cols, keys if isinstance(keys, tuple) else (keys,), strict=False))
        for horizon in horizons:
            col = value_template.format(h=horizon)
            values = group[col].drop_nulls().to_numpy().astype(float)
            values = values[~np.isnan(values)]
            mean, t_value, p_value = _t_stat(values)
            winsor = np.clip(values, np.percentile(values, 1), np.percentile(values, 99)) if len(values) >= 20 else values
            mean_winsor, t_winsor, _ = _t_stat(winsor)
            ci_low, ci_high = bootstrap_ci(values)
            date_means = group.group_by('trade_day').agg(pl.col(col).mean()).sort('trade_day')[col]
            date_values = date_means.drop_nulls().to_numpy().astype(float)
            date_values = date_values[~np.isnan(date_values)]
            mean_date, t_date, p_date = _t_stat(date_values)
            ci_low_date, ci_high_date = bootstrap_ci(date_values)
            row = {**key_map, 'horizon': horizon, 'n': int(len(values)),
                   'mean': mean, 'mean_winsor': mean_winsor, 'median': float(np.median(values)) if len(values) else float('nan'),
                   'std': float(np.std(values, ddof=1)) if len(values) >= 2 else float('nan'),
                   't': t_value, 'p': p_value, 't_winsor': t_winsor,
                   'win_rate': float(np.mean(values > 0)) if len(values) else float('nan'),
                   'ci_low': ci_low, 'ci_high': ci_high,
                   'n_dates': int(len(date_values)), 'mean_date': mean_date,
                   't_date': t_date, 'p_date': p_date,
                   'ci_low_date': ci_low_date, 'ci_high_date': ci_high_date}
            for regime in ('bull', 'bear'):
                subset = group.filter(pl.col(regime_col) == regime)[col].drop_nulls().to_numpy().astype(float)
                subset = subset[~np.isnan(subset)]
                r_mean, r_t, _ = _t_stat(subset)
                row[f'n_{regime}'] = int(len(subset))
                row[f'mean_{regime}'] = r_mean
                row[f't_{regime}'] = r_t
            row['significant'] = bool(len(date_values) >= 10 and abs(t_date) >= 1.96)
            row['sample_flag'] = 'ok' if len(values) >= 30 else 'insufficient'
            rows.append(row)
    return pl.DataFrame(rows)


def write_csv(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_csv(path)
