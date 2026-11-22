#!/usr/bin/env python3
"""筹码分布逐日信号（因果版，批量向量化）。

与 `src/tech_analysis/chip.py` 的关系
--------------------------------------
`chip.py` 只算「截至最新一日」的一张分布（整段历史一次递推），
用于界面展示可以，但信号普查需要**每一交易日**都能复现当天的分布。
本模块把它改写成逐日输出，口径保持同一族（衰减 VAP + 三角分布落箱）：

    chip_t = chip_{t-1} * (1 - d_t) + volume_t * triangular_t

差别（均为可计算性/因果性所做的必要取舍，报告中如实标注）：

1. **价格箱按票固定、锚点只取上市后前 250 个交易日的收盘中位数**，
   覆盖 `anchor * exp([-2.0, +3.2])`（约 0.135x ~ 24.5x），220 箱
   （对数步长 ≈ 2.4%）。不用全样本 min/max 作区间，避免把「该股未来
   波动幅度」泄漏进箱宽。极端涨跌超出区间时质量被压到边缘箱，
   `grid_cover_ratio` 会给出被压住的交易日占比。
2. **换手率用 B 档代理**（本地日线没有 `turn` 列）：
   `d = clip(3% * volume / MA20(volume), 2%, 10%)`，与 `chip.py` 的
   `turnover_source='proxy'` 完全一致；`volume<=0` 当日不衰减。
3. 主峰占比用「3 箱滑动均值取最大」，替代 `find_peaks`（220 箱下两者
   等价度高，但前者可对全部交易日一次性向量化）。

算法：把递推展开成前缀和，避免逐日 Python 循环
------------------------------------------------
    chip_T = Σ_{t<=T} vol_t * mass_t * Π_{s=t+1..T}(1-d_s)
           = exp(C_T) * Σ_{t<=T} vol_t * mass_t * exp(-C_t),  C_T = Σ ln(1-d_s)

一次 `np.cumsum` 即得全部交易日，复杂度 O(交易日 × 箱数)，无 Python 日循环。

输出列（逐日，与 `chip.py` 命名对齐）
------------------------------------
- `chip_main_share`      主峰占比(%)，3 箱滑动均值的最大值
- `chip_main_price`      主峰价格（复权价）
- `chip_peak_dist`       收盘相对主峰的距离(%)，正=收盘高于主峰
- `chip_profit_ratio`    获利盘占比(%)：成本低于当日收盘的筹码比例
- `chip_width90`         90% 成本区间宽度 / 收盘价 × 100（5%~95% 分位）
- `chip_width80`         80% 成本区间宽度 / 收盘价 × 100（10%~90% 分位）
- `chip_grid_cover_ratio` 收盘价落在价格箱内的交易日占比（<1 表示有截断）

`chip_main_share` / `chip_profit_ratio` / `chip_width90` 即三个「集中度」维度；
龙虎榜/持仓/逐笔类集中度（主力控盘度、股东户数、席位集中度）需要本地没有的
数据，本模块不做、也不造数。

用法：
    python tools/chip_signals.py                 # 冒烟：前 5 只，打印耗时
    python tools/chip_signals.py --limit 200     # 抽样 200 只
"""

from __future__ import annotations

import glob
import os
import sys
import time

import numpy as np
import pandas as pd
import polars as pl

ROOT = r"e:\code\python\quant"
STORE = os.path.join(ROOT, "data", "local_store")

BINS = 220
LOG_LO, LOG_HI = -2.0, 3.2            # 相对锚点的对数区间
ANCHOR_WINDOW = 250                   # 锚点取上市后前 250 日的收盘中位数
TURN_BASE, TURN_MIN, TURN_MAX = 0.03, 0.02, 0.10
DECAY_WARMUP = 20
PEAK_WIN = 3


def _triangular_mass(low: np.ndarray, high: np.ndarray, mode: np.ndarray,
                     edges: np.ndarray) -> np.ndarray:
    """逐日三角分布落到每个价格箱的质量占比，形状 (n_days, bins)。"""
    a = low[:, None]
    b = high[:, None]
    c = mode[:, None]
    span = np.maximum(b - a, 1e-12)
    x = edges[None, :]
    left = (x - a) ** 2 / (span * np.maximum(c - a, 1e-12))
    right = 1.0 - (b - x) ** 2 / (span * np.maximum(b - c, 1e-12))
    cdf = np.where(x <= a, 0.0, np.where(x <= c, left, np.where(x < b, right, 1.0)))
    return np.clip(np.diff(cdf, axis=1), 0.0, None)


def compute_chip_series(low: np.ndarray, high: np.ndarray, close: np.ndarray,
                        volume: np.ndarray, bins: int = BINS) -> dict[str, np.ndarray]:
    """逐日筹码集中度指标。输入已是同一复权口径的价与原始成交量。

    Returns:
        dict，含 chip_main_share / chip_main_price / chip_peak_dist /
        chip_profit_ratio / chip_width90 / chip_width80 / chip_grid_cover_ratio
        （均为长度 n 的 float64 数组；无效行为 NaN）
    """
    low = np.asarray(low, float).copy()
    high = np.asarray(high, float).copy()
    close = np.asarray(close, float).copy()
    volume = np.asarray(volume, float).copy()
    n = close.size
    out = {k: np.full(n, np.nan) for k in (
        "chip_main_share", "chip_main_price", "chip_peak_dist",
        "chip_profit_ratio", "chip_width90", "chip_width80",
    )}
    out["chip_grid_cover_ratio"] = 0.0
    ok = np.isfinite(close) & np.isfinite(low) & np.isfinite(high) & (close > 0)
    if ok.sum() < 60:
        return out
    low = np.where(ok, low, close)
    high = np.where(ok, np.maximum(high, low), close)
    volume = np.where(ok & (volume > 0), volume, 0.0)

    head = close[:min(ANCHOR_WINDOW, n)]
    head = head[np.isfinite(head) & (head > 0)]
    if head.size == 0:
        return out
    anchor = float(np.median(head))
    edges = anchor * np.exp(np.linspace(LOG_LO, LOG_HI, bins + 1))
    centers = 0.5 * (edges[:-1] + edges[1:])

    # 衰减率：B 档代理（与 chip.py turnover_source='proxy' 同式）
    vol_s = pd.Series(volume)
    vol_ma = vol_s.rolling(DECAY_WARMUP, min_periods=1).mean().to_numpy()
    ratio = np.divide(volume, vol_ma, out=np.ones_like(volume), where=vol_ma > 0)
    d = np.clip(TURN_BASE * ratio, TURN_MIN, TURN_MAX)
    d = np.where(volume > 0, d, 0.0)
    # 低于一个完整箱宽的价格波动会落进同一箱，这里不需要额外保护

    mass = _triangular_mass(low, high, (low + high + close) / 3.0, edges)
    C = np.cumsum(np.log1p(-np.clip(d, 0.0, 0.5)))
    scaled = mass * (volume * np.exp(-C))[:, None]
    chips = np.exp(C)[:, None] * np.cumsum(scaled, axis=0)
    tot = chips.sum(axis=1)
    good = np.isfinite(tot) & (tot > 0)
    if not good.any():
        return out
    shares = np.zeros_like(chips)
    shares[good] = chips[good] / tot[good, None]
    cum = np.cumsum(shares, axis=1)

    # 主峰：3 箱滑动均值的最大处
    sm = shares.copy()
    if bins > 1:
        sm[:, 1:] += shares[:, :-1]
    if bins > 2:
        sm[:, 2:] += shares[:, :-2]
    denom = np.full(bins, float(min(PEAK_WIN, bins)))
    denom[0] = 1.0
    if bins > 1:
        denom[1] = float(min(2, bins))
    sm = sm / denom[None, :]
    peak_idx = sm.argmax(axis=1)
    main_share = sm[np.arange(n), peak_idx] * 100.0
    main_price = centers[peak_idx]

    # 获利盘：收盘价之前（含所在箱）的累计筹码占比
    k = np.clip(np.searchsorted(edges, close, side="right") - 1, 0, bins - 1)
    profit = cum[np.arange(n), k] * 100.0

    c05 = (cum >= 0.05).argmax(axis=1)
    c95 = (cum >= 0.95).argmax(axis=1)
    c10 = (cum >= 0.10).argmax(axis=1)
    c90 = (cum >= 0.90).argmax(axis=1)
    w90 = (centers[c95] - centers[c05]) / close * 100.0
    w80 = (centers[c90] - centers[c10]) / close * 100.0

    bad = ~good
    for key, arr in (("chip_main_share", main_share), ("chip_main_price", main_price),
                     ("chip_profit_ratio", profit), ("chip_width90", w90),
                     ("chip_width80", w80)):
        arr = np.where(bad, np.nan, arr)
        out[key][ok] = arr[ok]
    dist = (close - main_price) / close * 100.0
    out["chip_peak_dist"][ok] = np.where(bad, np.nan, dist)[ok]
    cover = ((close >= edges[0]) & (close <= edges[-1]) & ok)
    out["chip_grid_cover_ratio"] = float(cover.sum() / max(int(ok.sum()), 1))
    return out


def load_stock(path: str) -> tuple[str, pd.DataFrame] | None:
    try:
        raw = pl.read_parquet(path)
    except Exception:
        return None
    if raw.height < 120:
        return None
    d = raw.select(
        pl.col("date").cast(pl.Date), "ts_code", "open", "high", "low", "close",
        "volume", "hfq_factor",
    ).to_pandas().sort_values("date").reset_index(drop=True)
    return str(d["ts_code"].iloc[0]), d


def main() -> int:
    limit = 5
    if "--limit" in sys.argv:
        limit = int(sys.argv[sys.argv.index("--limit") + 1])
    files = []
    for mkt in ("sh", "sz", "bj"):
        files.extend(sorted(glob.glob(os.path.join(STORE, "quotes", "stock", mkt, "*.parquet"))))
    picked = files[:: max(len(files) // max(limit, 1), 1)][:limit]
    t0 = time.time()
    rows = 0
    covers = []
    for f in picked:
        got = load_stock(f)
        if got is None:
            continue
        code, d = got
        hf = d["hfq_factor"].fillna(1.0).to_numpy(float)
        res = compute_chip_series(d["low"].to_numpy(float) * hf,
                                  d["high"].to_numpy(float) * hf,
                                  d["close"].to_numpy(float) * hf,
                                  d["volume"].to_numpy(float))
        rows += len(d)
        covers.append(res["chip_grid_cover_ratio"])
        print(f"{code} rows={len(d)} main={np.nanmean(res['chip_main_share']):.1f}% "
              f"profit={np.nanmean(res['chip_profit_ratio']):.1f}% "
              f"w90={np.nanmean(res['chip_width90']):.1f}% cover={res['chip_grid_cover_ratio']:.3f}",
              flush=True)
    el = time.time() - t0
    print(f"[smoke] stocks={len(covers)} rows={rows} elapsed={el:.1f}s "
          f"per_stock={el/max(len(covers),1):.3f}s "
          f"full_market_est={el/max(len(covers),1)*5839/60:.1f}min "
          f"mean_cover={np.mean(covers) if covers else float('nan'):.3f}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
