#!/usr/bin/env python3
"""
信号有效性普查 v2（批量事件研究，只读 data/local_store）。

统一口径：全 A（剔除 ST 与上市不足 60 个交易日）、2005-01 起、T 日收盘确认、
T+1 开盘可买（开盘触涨停/停牌剔除并单独计数）、持有 3/5/10/20 个交易日后收盘平仓、
后复权收益、超额 = 信号组等权收益 − 同日全 A 等权收益。

v2 相对 v1 的修正：
  1. t 值按信号日聚类（同日多信号先聚成一个日度观测，再对日度超额序列求 t）；
  2. 扣往返成本 0.13%（佣金+印花税+过户费+滑点），超额分扣成本前后两列；
  3. BH-FDR(q=0.05) 多重检验，报未校正 p 与校正后结论；
  4. 效应量门槛：5 日净超额 ≥ +0.3% 或 20 日净超额 ≥ ±1.0% 才可交易（卖信号取反）；
  5. 样本外三段：2005-2014 / 2015-2019 / 2020-2026，三段方向一致才通过；
  6. 指标口径与修复后的指标库一致（MACD 柱 2×(DIF-DEA)、RSI SMA 平滑、EMV/MCST/ASI 原式等）。

输出（work/指标信号普查_2026-11-21/）：
  leaderboard.csv / sweep_stats.csv / sweep_yearly.csv /
  sweep_price_buckets.csv / sweep_unbuyable.csv / sweep_oos.csv

用法：
  python tools/signal_sweep.py            # 全量
  python tools/signal_sweep.py --limit 20 # 冒烟测试（前 20 只）
"""

from __future__ import annotations

import glob
import math
import os
import sys
import time
from datetime import date

import numpy as np
import pandas as pd
import polars as pl

ROOT = r"e:\code\python\quant"
STORE = os.path.join(ROOT, "data", "local_store")
OUT = os.path.join(ROOT, "work", "指标信号普查_2026-11-21")
os.makedirs(OUT, exist_ok=True)

START = pd.Timestamp("2005-01-01")
HORIZONS = (3, 5, 10, 20)
COST = 0.0013                      # 往返 0.13%
MIN_N_DAY = 3                      # 日度聚类时单日最少信号数
SEGMENTS = [("train", 2005, 2014), ("valid", 2015, 2019), ("test", 2020, 2026)]
BUCKETS = (("lt10", 0, 10), ("10_20", 10, 20), ("gt20", 20, 1e9))
BASE_ORD = date(1990, 1, 1).toordinal()
NB = 15000
CHINEXT_20PCT = date(2020, 8, 24)
EPS = 1e-9

# key, direction, group, label
VARIANTS = [
    # 均线 / EMA
    ("ma5x20_up", "买", "均线", "MA5 上穿 MA20"),
    ("ma5x20_dn", "卖", "均线", "MA5 下穿 MA20"),
    ("ema5x20_up", "买", "均线", "EMA5 上穿 EMA20"),
    ("ema5x20_dn", "卖", "均线", "EMA5 下穿 EMA20"),
    ("ema12x50_up", "买", "均线", "EMA12 上穿 EMA50"),
    ("ma_bull_first", "买", "均线", "5>10>20 多头排列首日"),
    ("ma_bear_first", "卖", "均线", "5<10<20 空头排列首日"),
    # MACD
    ("macd_gold", "买", "MACD", "MACD 金叉（DIF 上穿 DEA）"),
    ("macd_dead", "卖", "MACD", "MACD 死叉"),
    ("macd_gold_above0", "买", "MACD", "MACD 零轴上金叉"),
    # KDJ / SKDJ
    ("kdj_gold", "买", "KDJ", "KDJ 金叉"),
    ("kdj_dead", "卖", "KDJ", "KDJ 死叉"),
    ("kdj_low_gold", "买", "KDJ", "KDJ 低位金叉（K<30）"),
    ("kdj_high_dead", "卖", "KDJ", "KDJ 高位死叉（K>70）"),
    ("kdj_j_gt100", "卖", "KDJ", "KDJ J>100"),
    ("skdj_gold", "买", "SKDJ", "SKDJ 金叉"),
    ("skdj_dead", "卖", "SKDJ", "SKDJ 死叉"),
    # RSI
    ("rsi_x30_up", "买", "RSI", "RSI14 上穿 30"),
    ("rsi_x70_dn", "卖", "RSI", "RSI14 下穿 70"),
    ("rsi_lt30", "买", "RSI", "RSI14<30"),
    ("rsi_gt70", "卖", "RSI", "RSI14>70"),
    # DMI / ADX
    ("dmi_pdi_up", "买", "DMI", "+DI 上穿 -DI"),
    ("dmi_ndi_up", "卖", "DMI", "-DI 上穿 +DI"),
    ("adx25_pdi", "买", "DMI", "ADX>25 且 +DI>-DI"),
    ("adx25_ndi", "卖", "DMI", "ADX>25 且 -DI>+DI"),
    # BOLL / 新高新低
    ("boll_break_up", "买", "突破", "收盘上破 BOLL 上轨"),
    ("boll_break_dn", "卖", "突破", "收盘下破 BOLL 下轨"),
    ("boll_mid_up", "买", "突破", "收盘上穿 BOLL 中轨"),
    ("high20", "买", "突破", "收盘创 20 日新高"),
    ("low20", "卖", "突破", "收盘创 20 日新低"),
    ("high60", "买", "突破", "收盘创 60 日新高"),
    # WR / BIAS
    ("wr10_lt10", "卖", "摆动", "WR10<10（超买）"),
    ("wr10_gt90", "买", "摆动", "WR10>90（超卖）"),
    ("bias6_gt6", "卖", "摆动", "BIAS6>+6%"),
    ("bias6_ltm6", "买", "摆动", "BIAS6<-6%"),
    ("bias12_gt10", "卖", "摆动", "BIAS12>+10%"),
    ("bias12_ltm10", "买", "摆动", "BIAS12<-10%"),
    # 量价
    ("obv_high20", "买", "量价", "OBV 创 20 日新高"),
    ("obv_low20", "卖", "量价", "OBV 创 20 日新低"),
    ("obv_x_ma20_up", "买", "量价", "OBV 上穿 20 日均线"),
    ("mfi_gt80", "卖", "量价", "MFI14>80"),
    ("mfi_lt20", "买", "量价", "MFI14<20"),
    ("mfi_x20_up", "买", "量价", "MFI14 上穿 20"),
    ("cmf_x0_up", "买", "量价", "CMF20 上穿 0"),
    ("cmf_x0_dn", "卖", "量价", "CMF20 下穿 0"),
    ("vwap_dev_gt2", "卖", "量价", "收盘高于 VWAP20 超 2%"),
    ("vwap_dev_ltm2", "买", "量价", "收盘低于 VWAP20 超 2%"),
    ("lb_gt2", "买", "量价", "量比>2"),
    ("lb_gt3", "买", "量价", "量比>3"),
    ("lb_lt09", "买", "量价", "量比<0.9"),
    ("lb_lt06", "买", "量价", "量比<0.6（极度缩量）"),
    ("vol_up3", "买", "量价", "放量上涨（量比>1.5 且涨幅>3%）"),
    ("vol_dn3", "卖", "量价", "放量下跌（量比>1.5 且跌幅>3%）"),
    # ASI / EMV / MCST
    ("asi_x_sig_up", "买", "趋势量能", "ASI 上穿信号线"),
    ("asi_x_sig_dn", "卖", "趋势量能", "ASI 下穿信号线"),
    ("emv_x0_up", "买", "趋势量能", "EMV14 上穿 0"),
    ("emv_x0_dn", "卖", "趋势量能", "EMV14 下穿 0"),
    ("close_x_mcst_up", "买", "趋势量能", "收盘上穿 MCST21"),
    ("close_x_mcst_dn", "卖", "趋势量能", "收盘下穿 MCST21"),
    # 大盘同步
    ("mkt_sync_gt80", "买", "大盘", "mkt_sync60>80"),
    ("mkt_sync_lt30", "卖", "大盘", "mkt_sync60<30"),
    ("mkt_rs_gt10", "卖", "大盘", "mkt_rs60>+10pp"),
    ("mkt_rs_ltm10", "买", "大盘", "mkt_rs60<-10pp"),
    # 超跌追涨（v1 已知基线）
    ("ret5_ltm10", "买", "动量", "5 日跌幅<-10%"),
    ("ret5_gt10", "卖", "动量", "5 日涨幅>10%"),
]
KEYS = [v[0] for v in VARIANTS]
NH = len(HORIZONS)

# ---------------------------------------------------------------- 等频模式 ----
# v3 新增：等频（quantile）阈值模式与筹码集中度信号。
# 变体名约定： eq_<指标>_lo/hi = 当日横截面后/前 10% 分位
#              rl_<指标>_lo/hi = 个股过去 252 日滚动后/前 10% 分位
# 默认仍是固定阈值（--mode fixed），等价频结果与原 65 变体口径互不影响。
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from chip_signals import compute_chip_series  # noqa: E402

OUT_EQ = os.path.join(ROOT, "work", "等频与集中度_2026-11-21")
EQ_WINDOW = 252
EQ_MIN_PERIODS = 126
EQ_Q = (0.10, 0.90)
EQ_MIN_N_DAY = 30                  # 当日横截面最少股票数，不足则不给阈值

EQ_IND_KEYS = ["rsi", "bias12", "lb", "mfi", "chip_main", "chip_profit", "chip_width"]
EQ_IND_LABEL = {
    "rsi": "RSI14", "bias12": "BIAS12", "lb": "量比", "mfi": "MFI14",
    "chip_main": "筹码主峰占比", "chip_profit": "获利盘比例", "chip_width": "90%成本区间宽度",
}
EQ_IND_GROUP = {
    "rsi": "RSI", "bias12": "摆动", "lb": "量价", "mfi": "量价",
    "chip_main": "筹码", "chip_profit": "筹码", "chip_width": "筹码",
}
EQ_VARIANTS = []
for _n in EQ_IND_KEYS:
    EQ_VARIANTS.append((f"eq_{_n}_lo", "买", EQ_IND_GROUP[_n], f"{EQ_IND_LABEL[_n]} 当日横截面后10%"))
    EQ_VARIANTS.append((f"eq_{_n}_hi", "卖", EQ_IND_GROUP[_n], f"{EQ_IND_LABEL[_n]} 当日横截面前10%"))
    EQ_VARIANTS.append((f"rl_{_n}_lo", "买", EQ_IND_GROUP[_n], f"{EQ_IND_LABEL[_n]} 个股252日滚动后10%"))
    EQ_VARIANTS.append((f"rl_{_n}_hi", "卖", EQ_IND_GROUP[_n], f"{EQ_IND_LABEL[_n]} 个股252日滚动前10%"))
FIXED_EQ_VARIANTS = [
    ("rsi_lt30", "买", "RSI", "RSI14<30（固定阈值）"),
    ("rsi_gt70", "卖", "RSI", "RSI14>70（固定阈值）"),
    ("bias12_ltm10", "买", "摆动", "BIAS12<−10%（固定阈值）"),
    ("bias12_gt10", "卖", "摆动", "BIAS12>+10%（固定阈值）"),
    ("lb_lt09", "买", "量价", "量比<0.9（固定阈值）"),
    ("lb_gt2", "卖", "量价", "量比>2（固定阈值）"),
    ("mfi_lt20", "买", "量价", "MFI14<20（固定阈值）"),
    ("mfi_gt80", "卖", "量价", "MFI14>80（固定阈值）"),
    ("chipmain_lt", "买", "筹码", "筹码主峰占比<训练期p30"),
    ("chipmain_gt", "卖", "筹码", "筹码主峰占比>训练期p70"),
    ("chipwidth_lt", "买", "筹码", "90%成本区间宽度<训练期p30"),
    ("chipwidth_gt", "卖", "筹码", "90%成本区间宽度>训练期p70"),
]
EQ_ALL_VARIANTS = EQ_VARIANTS + FIXED_EQ_VARIANTS
EQ_KEYS = [v[0] for v in EQ_ALL_VARIANTS]
RL_KEYS = [f"{p}_{n}_{s}" for n in EQ_IND_KEYS for p, s in (("rl", "lo"), ("rl", "hi"))]


def load_meta():
    sb = pl.read_parquet(os.path.join(STORE, "meta", "stock_basic.parquet")).to_pandas()
    list_date = {r.ts_code: pd.Timestamp(r.list_date) for r in sb.itertuples() if pd.notna(r.list_date)}
    st = pl.read_parquet(os.path.join(STORE, "meta", "st", "st_intervals.parquet")).to_pandas()
    st_map = {}
    for code, g in st.groupby("ts_code"):
        st_map[code] = [(pd.Timestamp(s), pd.Timestamp(e) if pd.notna(e) else pd.Timestamp("2099-01-01"))
                        for s, e in zip(g["start_date"], g["end_date"])]
    return list_date, st_map


def load_bench():
    p = os.path.join(STORE, "quotes", "index", "sh", "000300.SH.parquet")
    b = pl.read_parquet(p).select(pl.col("date").cast(pl.Date), pl.col("close").cast(pl.Float64)).to_pandas()
    b = b.dropna().sort_values("date")
    s = pd.Series(b["close"].to_numpy(float), index=pd.to_datetime(b["date"]))
    return s, s.pct_change()


LIST_DATE, ST_MAP = load_meta()
BENCH_CLOSE, BENCH_RET = load_bench()


def round_half_up(x):
    return np.floor(x * 100.0 + 0.5) / 100.0


def ema_tdx(s: pd.Series, n: float) -> pd.Series:
    """通达信 EMA(X,N)：alpha = 2/(N+1)。"""
    return s.ewm(alpha=2.0 / (n + 1.0), adjust=False).mean()


def sma_tdx(s: pd.Series, n: float, m: float = 1.0) -> pd.Series:
    """通达信 SMA(X,N,M)：alpha = M/N（RSI / KDJ 用）。"""
    return s.ewm(alpha=m / n, adjust=False).mean()


def st_flags(code, dates: pd.Series) -> np.ndarray:
    out = np.zeros(len(dates), dtype=bool)
    for s, e in ST_MAP.get(code, []):
        out |= ((dates >= s) & (dates <= e)).to_numpy()
    return out


def limit_pct_arr(code, dates: pd.Series, st: np.ndarray) -> np.ndarray:
    d = dates.dt.date
    pct = np.full(len(dates), 0.10)
    if code.startswith(("688", "689")):
        pct[:] = 0.20
    elif code.startswith(("300", "301")):
        pct = np.where(d >= CHINEXT_20PCT, 0.20, 0.10)
    elif code.endswith(".BJ"):
        pct[:] = 0.30
    else:
        pct = np.where(st, 0.05, 0.10)
    return pct


def process(path, mode="fixed"):
    try:
        raw = pl.read_parquet(path)
    except Exception:
        return None
    if raw.height < 120:
        return None
    d = raw.select(
        pl.col("date").cast(pl.Date), "ts_code", "open", "high", "low", "close", "pre_close",
        "volume", "amount", "hfq_factor",
    ).to_pandas().sort_values("date").reset_index(drop=True)
    code = str(d["ts_code"].iloc[0])
    n = len(d)
    hf = d["hfq_factor"].fillna(1.0).to_numpy(float)
    o = d["open"].to_numpy(float)
    h = d["high"].to_numpy(float)
    lo = d["low"].to_numpy(float)
    c = d["close"].to_numpy(float)
    pclose = d["pre_close"].to_numpy(float)
    vol = d["volume"].to_numpy(float)
    amt = d["amount"].to_numpy(float)
    # 指标统一用后复权价，保证除权不打断指标连续性
    qc = pd.Series(c * hf)
    qo, qh, ql = pd.Series(o * hf), pd.Series(h * hf), pd.Series(lo * hf)

    # ---- 均线 / EMA ----
    ma5, ma10, ma20, ma60 = (qc.rolling(w).mean() for w in (5, 10, 20, 60))
    ema5, ema20, ema50 = ema_tdx(qc, 5), ema_tdx(qc, 20), ema_tdx(qc, 50)
    ema12 = ema_tdx(qc, 12)
    ema26 = ema_tdx(qc, 26)
    dif = ema12 - ema26
    dea = ema_tdx(dif, 9)

    # ---- KDJ / SKDJ ----
    llv9, hhv9 = ql.rolling(9).min(), qh.rolling(9).max()
    rsv = (qc - llv9) / (hhv9 - llv9).replace(0, np.nan) * 100
    k = sma_tdx(rsv, 3)
    dd = sma_tdx(k, 3)
    j = 3 * k - 2 * dd
    sk = ema_tdx(rsv, 3)
    sd = sma_tdx(sk, 3)
    sj = 3 * sk - 2 * sd

    # ---- RSI ----
    delta = qc.diff()
    up = delta.clip(lower=0)
    dn = -delta.clip(upper=0)
    au, ad = sma_tdx(up, 14), sma_tdx(dn, 14)
    rsi = 100 * au / (au + ad)

    # ---- DMI / ADX ----
    ph, pl_, pc = qh.shift(1), ql.shift(1), qc.shift(1)
    tr = pd.concat([qh - pl_, (qh - pc).abs(), (ql - pc).abs()], axis=1).max(axis=1)
    hd, ld = qh - ph, pl_ - ql
    pdm = pd.Series(np.where((hd > ld) & (hd > 0), hd, 0.0))
    ndm = pd.Series(np.where((ld > hd) & (ld > 0), ld, 0.0))
    tr_sum = tr.rolling(14, min_periods=1).sum()
    pdi = pdm.rolling(14, min_periods=1).sum() / tr_sum.replace(0, np.nan) * 100
    ndi = ndm.rolling(14, min_periods=1).sum() / tr_sum.replace(0, np.nan) * 100
    dx = (pdi - ndi).abs() / (pdi + ndi).replace(0, np.nan) * 100
    adx = dx.rolling(14).mean()

    # ---- BOLL / WR / BIAS ----
    sd20 = qc.rolling(20).std()
    boll_up, boll_mid, boll_dn = ma20 + 2 * sd20, ma20, ma20 - 2 * sd20
    hhv10, llv10 = qh.rolling(10).max(), ql.rolling(10).min()
    wr10 = (hhv10 - qc) / (hhv10 - llv10).replace(0, np.nan) * 100
    ma6, ma12 = qc.rolling(6).mean(), qc.rolling(12).mean()
    bias6 = (qc - ma6) / ma6 * 100
    bias12 = (qc - ma12) / ma12 * 100

    # ---- 量价 ----
    lb = pd.Series(vol) / pd.Series(vol).shift(1).rolling(5).mean()
    obv = (np.sign(pd.Series(c).diff().fillna(0)) * pd.Series(vol)).cumsum()
    obv_ma20 = obv.rolling(20).mean()
    tp = (qh + ql + qc) / 3
    mf = tp * pd.Series(vol)
    mfp = pd.Series(np.where(tp > tp.shift(1), mf, 0.0))
    mfn = pd.Series(np.where(tp < tp.shift(1), mf, 0.0))
    mfp_s, mfn_s = mfp.rolling(14).sum(), mfn.rolling(14).sum()
    mfi = (100 - 100 / (1 + mfp_s / mfn_s.replace(0, np.nan))).where(mfn_s != 0, 100.0)
    mfi = mfi.where(mfp_s.notna(), np.nan)
    hl = (qh - ql)
    clv = ((qc - ql) - (qh - qc)) / hl.replace(0, np.nan)
    cmf = (clv.fillna(0) * pd.Series(vol)).rolling(20).sum() / pd.Series(vol).rolling(20).sum()
    vwap20 = pd.Series(amt).rolling(20).sum() / (pd.Series(vol).rolling(20).sum() * 100)
    vwap_dev = (pd.Series(c) / vwap20 - 1) * 100

    # ---- ASI / EMV / MCST ----
    atr_ = pd.concat([(ph - qc).abs(), (pl_ - qc).abs(), (ph - pl_).abs()], axis=1).max(axis=1)
    si_num = (qo - pc) + 0.5 * (qo - qc) + 0.25 * (pc - qo.shift(1))
    asi = (si_num / atr_.replace(0, np.nan) * 16).fillna(0).cumsum()
    asi_sig = asi.rolling(6).mean()
    hl_sum = qh + ql
    mid = 100 * (hl_sum - hl_sum.shift(1)) / hl_sum
    vol_ratio = pd.Series(vol).rolling(14).mean() / pd.Series(vol).replace(0, np.nan)
    range_ma = hl.rolling(14).mean()
    emv = (mid * vol_ratio * hl / range_ma.replace(0, np.nan)).rolling(14).mean()
    pv = qc * pd.Series(vol)
    mcst = pv.rolling(21).sum() / pd.Series(vol).rolling(21).sum()

    # ---- 大盘同步 ----
    bclose = BENCH_CLOSE.reindex(d["date"]).to_numpy(float)
    bret = BENCH_RET.reindex(d["date"]).to_numpy(float)
    sret = pd.Series(c).pct_change().to_numpy(float)
    same = np.where(np.isfinite(bret) & np.isfinite(sret) & (sret != 0),
                    (sret > 0) == (bret > 0), np.nan)
    mkt_sync = pd.Series(same).rolling(60, min_periods=60).mean() * 100
    bser = pd.Series(bclose)
    mkt_rs = (qc / qc.shift(60) - bser / bser.shift(60)) * 100

    st = st_flags(code, d["date"])
    pct = limit_pct_arr(code, d["date"], st)
    prev = np.where(np.isfinite(pclose) & (pclose > 0), pclose, c)
    L = round_half_up(prev * (1 + pct))
    ret1 = qc / qc.shift(1) - 1

    def cross_up(a, b):
        return (a > b) & (a.shift(1) <= b.shift(1))

    def cross_dn(a, b):
        return (a < b) & (a.shift(1) >= b.shift(1))

    sig = {
        "ma5x20_up": cross_up(ma5, ma20),
        "ma5x20_dn": cross_dn(ma5, ma20),
        "ema5x20_up": cross_up(ema5, ema20),
        "ema5x20_dn": cross_dn(ema5, ema20),
        "ema12x50_up": cross_up(ema12, ema50),
        "ma_bull_first": ((ma5 > ma10) & (ma10 > ma20)) &
                         ~((ma5.shift(1) > ma10.shift(1)) & (ma10.shift(1) > ma20.shift(1))),
        "ma_bear_first": ((ma5 < ma10) & (ma10 < ma20)) &
                         ~((ma5.shift(1) < ma10.shift(1)) & (ma10.shift(1) < ma20.shift(1))),
        "macd_gold": cross_up(dif, dea),
        "macd_dead": cross_dn(dif, dea),
        "macd_gold_above0": cross_up(dif, dea) & (dif > 0),
        "kdj_gold": cross_up(k, dd),
        "kdj_dead": cross_dn(k, dd),
        "kdj_low_gold": cross_up(k, dd) & (k < 30),
        "kdj_high_dead": cross_dn(k, dd) & (k > 70),
        "kdj_j_gt100": j > 100,
        "skdj_gold": cross_up(sk, sd),
        "skdj_dead": cross_dn(sk, sd),
        "rsi_x30_up": cross_up(rsi, pd.Series(30.0, index=rsi.index)),
        "rsi_x70_dn": cross_dn(rsi, pd.Series(70.0, index=rsi.index)),
        "rsi_lt30": rsi < 30,
        "rsi_gt70": rsi > 70,
        "dmi_pdi_up": cross_up(pdi, ndi),
        "dmi_ndi_up": cross_up(ndi, pdi),
        "adx25_pdi": (adx > 25) & (pdi > ndi),
        "adx25_ndi": (adx > 25) & (ndi > pdi),
        "boll_break_up": cross_up(qc, boll_up),
        "boll_break_dn": cross_dn(qc, boll_dn),
        "boll_mid_up": cross_up(qc, boll_mid),
        "high20": qc >= qc.rolling(20).max(),
        "low20": qc <= qc.rolling(20).min(),
        "high60": qc >= qc.rolling(60).max(),
        "wr10_lt10": wr10 < 10,
        "wr10_gt90": wr10 > 90,
        "bias6_gt6": bias6 > 6,
        "bias6_ltm6": bias6 < -6,
        "bias12_gt10": bias12 > 10,
        "bias12_ltm10": bias12 < -10,
        "obv_high20": obv >= obv.rolling(20).max(),
        "obv_low20": obv <= obv.rolling(20).min(),
        "obv_x_ma20_up": cross_up(obv, obv_ma20),
        "mfi_gt80": mfi > 80,
        "mfi_lt20": mfi < 20,
        "mfi_x20_up": cross_up(mfi, pd.Series(20.0, index=mfi.index)),
        "cmf_x0_up": cross_up(cmf, pd.Series(0.0, index=cmf.index)),
        "cmf_x0_dn": cross_dn(cmf, pd.Series(0.0, index=cmf.index)),
        "vwap_dev_gt2": vwap_dev > 2,
        "vwap_dev_ltm2": vwap_dev < -2,
        "lb_gt2": lb > 2,
        "lb_gt3": lb > 3,
        "lb_lt09": lb < 0.9,
        "lb_lt06": lb < 0.6,
        "vol_up3": (lb > 1.5) & (ret1 > 0.03),
        "vol_dn3": (lb > 1.5) & (ret1 < -0.03),
        "asi_x_sig_up": cross_up(asi, asi_sig),
        "asi_x_sig_dn": cross_dn(asi, asi_sig),
        "emv_x0_up": cross_up(emv, pd.Series(0.0, index=emv.index)),
        "emv_x0_dn": cross_dn(emv, pd.Series(0.0, index=emv.index)),
        "close_x_mcst_up": cross_up(qc, mcst),
        "close_x_mcst_dn": cross_dn(qc, mcst),
        "mkt_sync_gt80": mkt_sync > 80,
        "mkt_sync_lt30": mkt_sync < 30,
        "mkt_rs_gt10": mkt_rs > 10,
        "mkt_rs_ltm10": mkt_rs < -10,
        "ret5_ltm10": (qc / qc.shift(5) - 1) < -0.10,
        "ret5_gt10": (qc / qc.shift(5) - 1) > 0.10,
    }

    # ---- 收益与可买 ----
    op = o * hf
    cl = c * hf
    entry = np.append(op[1:], np.nan)
    rets = {}
    for hz in HORIZONS:
        ex = np.append(cl[hz:], np.full(hz, np.nan))
        rets[hz] = ex / entry - 1.0
    nxt_open = np.append(o[1:], np.nan)
    nxt_vol = np.append(vol[1:], np.nan)
    limit_next = np.append(L[1:], np.nan)
    suspended = ~np.isfinite(nxt_open) | ~(nxt_vol > 0)
    limit_block = np.isfinite(nxt_open) & (nxt_vol > 0) & (nxt_open >= limit_next - EPS)
    buyable = ~suspended & ~limit_block

    # ---- 股票池 ----
    ld = LIST_DATE.get(code, pd.Timestamp("1990-01-01"))
    pos = np.arange(n)
    listed = (d["date"] >= ld).to_numpy() & (pos >= 60)
    base = listed & ~st & (d["date"] >= START).to_numpy()

    idx = (d["date"].to_numpy().astype("datetime64[D]").astype(np.int64) + 719163 - BASE_ORD)
    buckets = np.digitize(c, [10, 20])  # 0:<10, 1:10-20, 2:>20

    sig_masks = {}
    unb = {}
    want_ind = mode == "eqfreq"
    ind = {}
    if want_ind:
        # 筹码集中度：逐日因果版（tools/chip_signals.py），与固定阈值模式互不影响
        chip = compute_chip_series(ql.to_numpy(float), qh.to_numpy(float),
                                   qc.to_numpy(float), vol)
        ind = {
            "rsi": rsi.to_numpy(float),
            "bias12": bias12.to_numpy(float),
            "lb": lb.to_numpy(float),
            "mfi": mfi.to_numpy(float),
            "chip_main": chip["chip_main_share"],
            "chip_profit": chip["chip_profit_ratio"],
            "chip_width": chip["chip_width90"],
        }
        # 个股滚动 252 日分位（pandas rolling rank，pct=True 即历史分位）
        for name in EQ_IND_KEYS:
            rk = pd.Series(ind[name], dtype="float64").rolling(
                EQ_WINDOW, min_periods=EQ_MIN_PERIODS).rank(pct=True)
            raw_lo = (rk <= EQ_Q[0]).fillna(False).to_numpy(dtype=bool)
            raw_hi = (rk >= EQ_Q[1]).fillna(False).to_numpy(dtype=bool)
            for key, raw in ((f"rl_{name}_lo", raw_lo), (f"rl_{name}_hi", raw_hi)):
                m = raw & base
                sig_masks[key] = m & buyable
                unb[key] = (int((m & limit_block).sum()), int((m & suspended).sum()))
    else:
        for key in KEYS:
            s = sig[key].fillna(False).to_numpy(dtype=bool)
            m = s & base
            sig_masks[key] = m & buyable
            unb[key] = (int((m & limit_block).sum()), int((m & suspended).sum()))

    # 基准：全 A（不做池过滤）
    bench = {}
    for hz in HORIZONS:
        r = rets[hz]
        ok = np.isfinite(r)
        bench[hz] = (np.bincount(idx[ok], weights=r[ok], minlength=NB),
                     np.bincount(idx[ok], minlength=NB))
    out = {
        "code": code, "idx": idx, "rets": rets, "masks": sig_masks, "unb": unb,
        "buckets": buckets, "bench": bench, "n": n,
    }
    if want_ind:
        out["ind"] = {k: v.astype(np.float32) for k, v in ind.items()}
        out["base"] = base
        out["buyable"] = buyable
        out["block"] = limit_block
        out["susp"] = suspended
        out["rets"] = {hz: r.astype(np.float32) for hz, r in rets.items()}
        out["buckets"] = buckets.astype(np.int8)
        out["idx"] = idx.astype(np.int32)
    return out


def pval_t(t, df):
    if not np.isfinite(t):
        return np.nan
    try:
        from scipy import stats as _st
        return float(2 * _st.t.sf(abs(t), max(df, 1)))
    except Exception:
        return math.erfc(abs(t) / math.sqrt(2))


def bh_fdr(pvals, alpha=0.05):
    """返回每个 p 是否通过 BH-FDR。"""
    p = np.asarray(pvals, float)
    ok = np.zeros(len(p), dtype=bool)
    valid = np.isfinite(p)
    m = int(valid.sum())
    if m == 0:
        return ok
    pv = p[valid]
    order = np.argsort(pv)
    passed = pv[order] <= (np.arange(1, m + 1) * alpha / m)
    if not passed.any():
        return ok
    k = np.max(np.where(passed)[0])
    idx_valid = np.where(valid)[0]
    ok[idx_valid[order[:k + 1]]] = True
    return ok


def _init_acc(keys):
    ACC = {(k, hz): {"s": np.zeros(NB), "n": np.zeros(NB)} for k in keys for hz in HORIZONS}
    BUCK = {(k, hz, b): {"s": np.zeros(NB), "n": np.zeros(NB)}
            for k in keys for hz in (5, 20) for b in range(3)}
    BENCH = {hz: {"s": np.zeros(NB), "n": np.zeros(NB)} for hz in HORIZONS}
    return ACC, BUCK, BENCH


def _accumulate(r, keys, ACC, BUCK, BENCH, UNB, CODES):
    idx = r["idx"]
    for hz in HORIZONS:
        bs, bc = r["bench"][hz]
        BENCH[hz]["s"] += bs
        BENCH[hz]["n"] += bc
    for k in keys:
        m = r["masks"][k]
        if not m.any():
            continue
        CODES[k].add(r["code"])
        UB = r["unb"][k]
        UNB[k][0] += UB[0]
        UNB[k][1] += UB[1]
        for hz in HORIZONS:
            rr = r["rets"][hz]
            ok = m & np.isfinite(rr)
            if not ok.any():
                continue
            ii, w = idx[ok], rr[ok]
            a = ACC[(k, hz)]
            a["s"] += np.bincount(ii, weights=w, minlength=NB)
            a["n"] += np.bincount(ii, minlength=NB)
            if hz in (5, 20):
                b = r["buckets"][ok]
                for bi in range(3):
                    sel = b == bi
                    if not sel.any():
                        continue
                    bb = BUCK[(k, hz, bi)]
                    bb["s"] += np.bincount(ii[sel], weights=w[sel], minlength=NB)
                    bb["n"] += np.bincount(ii[sel], minlength=NB)


def _stock_files():
    files = []
    for mkt in ("sh", "sz", "bj"):
        files.extend(sorted(glob.glob(os.path.join(STORE, "quotes", "stock", mkt, "*.parquet"))))
    return files


def process_eq(path):
    return process(path, mode="eqfreq")


def _daily_quantiles(idx, val, qs=EQ_Q, min_n=EQ_MIN_N_DAY):
    """按日横截面分位切点，返回 (qlo[NB], qhi[NB])。"""
    qlo, qhi = np.full(NB, np.nan), np.full(NB, np.nan)
    ok = np.isfinite(val)
    d, v = idx[ok], val[ok]
    order = np.argsort(d, kind="stable")
    d, v = d[order], v[order]
    uniq, start, counts = np.unique(d, return_index=True, return_counts=True)
    for u, s, c in zip(uniq, start, counts):
        if c < min_n:
            continue
        seg = v[s:s + c]
        lo, hi = np.quantile(seg, qs)
        qlo[int(u)] = lo
        qhi[int(u)] = hi
    return qlo, qhi


def _year_of(idx):
    return ((np.datetime64("1990-01-01") + np.asarray(idx, np.int64).astype("timedelta64[D]"))
            .astype("datetime64[Y]").astype(np.int64) + 1970)


def run_fixed(files, out_dir):
    t0 = time.time()
    print(f"[start] mode=fixed files={len(files)} variants={len(KEYS)} horizons={HORIZONS}", flush=True)
    ACC, BUCK, BENCH = _init_acc(KEYS)
    UNB = {k: [0, 0] for k in KEYS}
    CODES = {k: set() for k in KEYS}
    errs = 0
    for i, f in enumerate(files, 1):
        try:
            r = process(f)
        except Exception as e:
            errs += 1
            if errs <= 5:
                print(f"[warn] {os.path.basename(f)}: {e}", flush=True)
            continue
        if r is None:
            continue
        _accumulate(r, KEYS, ACC, BUCK, BENCH, UNB, CODES)
        if i % 500 == 0:
            print(f"[{i}/{len(files)}] errs={errs} {time.time()-t0:.0f}s", flush=True)
    return finalize(VARIANTS, ACC, BUCK, BENCH, UNB, CODES, out_dir, t0, errs)


def run_eqfreq(files, out_dir, workers=8):
    """等频阈值 + 筹码集中度：两阶段（并行采集 → 单进程按日分位聚合）。"""
    from concurrent.futures import ProcessPoolExecutor
    t0 = time.time()
    print(f"[start] mode=eqfreq files={len(files)} variants={len(EQ_KEYS)} workers={workers}", flush=True)
    recs, errs = [], 0
    with ProcessPoolExecutor(max_workers=workers) as ex:
        for i, r in enumerate(ex.map(process_eq, files, chunksize=16), 1):
            if r is None:
                errs += 1
                continue
            recs.append(r)
            if i % 500 == 0:
                print(f"[collect {i}/{len(files)}] errs={errs} {time.time()-t0:.0f}s", flush=True)
    print(f"[collect done] stocks={len(recs)} errs={errs} {time.time()-t0:.0f}s", flush=True)

    idx_all = np.concatenate([r["idx"] for r in recs])
    years_all = _year_of(idx_all)
    QLO, QHI, CHIP_THR = {}, {}, {}
    for name in EQ_IND_KEYS:
        vals = np.concatenate([r["ind"][name] for r in recs])
        QLO[name], QHI[name] = _daily_quantiles(idx_all, vals)
        if name in ("chip_main", "chip_width"):
            tr = (years_all >= 2005) & (years_all <= 2014) & np.isfinite(vals)
            CHIP_THR[name] = tuple(float(x) for x in np.quantile(vals[tr], [0.30, 0.70]))
        print(f"[quantile] {name} days={int(np.isfinite(QLO[name]).sum())} "
              f"train_thr={CHIP_THR.get(name)} {time.time()-t0:.0f}s", flush=True)
        del vals
    del idx_all, years_all

    ACC, BUCK, BENCH = _init_acc(EQ_KEYS)
    UNB = {k: [0, 0] for k in EQ_KEYS}
    CODES = {k: set() for k in EQ_KEYS}
    for i, r in enumerate(recs, 1):
        idx = r["idx"]
        base, buy = r["base"], r["buyable"]
        block, susp = r["block"], r["susp"]
        masks = dict(r["masks"])
        unb = dict(r["unb"])
        for name in EQ_IND_KEYS:
            v = r["ind"][name].astype(np.float64)
            lo, hi = QLO[name][idx], QHI[name][idx]
            raw_lo = np.isfinite(v) & np.isfinite(lo) & (v <= lo)
            raw_hi = np.isfinite(v) & np.isfinite(hi) & (v >= hi)
            for key, raw in ((f"eq_{name}_lo", raw_lo), (f"eq_{name}_hi", raw_hi)):
                m = raw & base
                masks[key] = m & buy
                unb[key] = (int((m & block).sum()), int((m & susp).sum()))
        rsi_v, bias_v = r["ind"]["rsi"], r["ind"]["bias12"]
        lb_v, mfi_v = r["ind"]["lb"], r["ind"]["mfi"]
        cm_v, cw_v = r["ind"]["chip_main"], r["ind"]["chip_width"]
        fixed = {
            "rsi_lt30": rsi_v < 30, "rsi_gt70": rsi_v > 70,
            "bias12_ltm10": bias_v < -10, "bias12_gt10": bias_v > 10,
            "lb_lt09": lb_v < 0.9, "lb_gt2": lb_v > 2,
            "mfi_lt20": mfi_v < 20, "mfi_gt80": mfi_v > 80,
            "chipmain_lt": cm_v <= CHIP_THR["chip_main"][0],
            "chipmain_gt": cm_v > CHIP_THR["chip_main"][1],
            "chipwidth_lt": cw_v <= CHIP_THR["chip_width"][0],
            "chipwidth_gt": cw_v > CHIP_THR["chip_width"][1],
        }
        for key, raw in fixed.items():
            m = np.asarray(raw, dtype=bool) & base
            masks[key] = m & buy
            unb[key] = (int((m & block).sum()), int((m & susp).sum()))
        r2 = {"code": r["code"], "idx": idx.astype(np.int64), "rets": r["rets"],
              "masks": masks, "unb": unb, "buckets": r["buckets"], "bench": r["bench"]}
        _accumulate(r2, EQ_KEYS, ACC, BUCK, BENCH, UNB, CODES)
        if i % 500 == 0:
            print(f"[agg {i}/{len(recs)}] {time.time()-t0:.0f}s", flush=True)
    return finalize(EQ_ALL_VARIANTS, ACC, BUCK, BENCH, UNB, CODES, out_dir, t0, errs)


def finalize(variants, ACC, BUCK, BENCH, UNB, CODES, out_dir, t0, errs):
    os.makedirs(out_dir, exist_ok=True)
    KEYS = [v[0] for v in variants]
    VARIANTS = variants
    OUT = out_dir

    # 日期→年
    ords = np.arange(NB) + BASE_ORD
    dates = pd.to_datetime([date.fromordinal(int(x)) for x in ords])
    years = dates.year.values
    bench_mean = {hz: np.where(BENCH[hz]["n"] > 0, BENCH[hz]["s"] / np.maximum(BENCH[hz]["n"], 1), np.nan)
                  for hz in HORIZONS}

    def daily_excess(k, hz, store=None):
        """日度超额序列（同日多信号先聚成一个观测），仅用于「典型日」参考口径。"""
        a = store if store is not None else ACC[(k, hz)]
        nn = a["n"]
        bm = bench_mean[hz]
        ok = (nn >= MIN_N_DAY) & np.isfinite(bm)
        if not ok.any():
            return np.array([np.nan])
        return a["s"][ok] / nn[ok] - bm[ok]

    def stat(k, hz, store=None):
        """点估计=交易等权（同日多信号全买），标准误按信号日聚类（CR0）。

        pooled  = Σ_d(信号收益和 − n_d×基准均值) / Σ_d n_d
        var(m)  = G/(G−1) × Σ_d (日残差和 − n_d×m)² / N²
        这样既不丢同日多信号的经济含义，也不会因重叠样本把 t 放大。
        """
        a = store if store is not None else ACC[(k, hz)]
        nn = a["n"]
        bm = bench_mean[hz]
        ok = np.isfinite(bm)
        n, s, b = nn[ok], a["s"][ok], bm[ok]
        N = float(n.sum())
        G = int((n > 0).sum())
        if N == 0 or G < 20:
            return dict(n=int(N), ndays=G, gross=np.nan, net=np.nan, t=np.nan, p=np.nan, day=np.nan)
        xg = s - n * b
        m = xg.sum() / N
        resid = xg - n * m
        var = (G / (G - 1.0)) * float((resid ** 2).sum()) / (N ** 2) if G > 1 else np.nan
        t = m / math.sqrt(var) if var and var > 0 else np.nan
        d = daily_excess(k, hz, store)
        d = d[np.isfinite(d)]
        return dict(n=int(N), ndays=G, gross=m, net=m - COST, t=t, p=pval_t(t, G - 1),
                    day=float(d.mean()) if len(d) else np.nan)

    def seg(k, hz, sel):
        """子样本 pooled 超额（年内 / 三段样本外 / 股价分组共用）。"""
        a = ACC[(k, hz)]
        n, s, b = a["n"][sel], a["s"][sel], bench_mean[hz][sel]
        ok = np.isfinite(b)
        n, s, b = n[ok], s[ok], b[ok]
        tot = float(n.sum())
        if tot == 0:
            return 0, np.nan, 0
        return int(tot), float((s - n * b).sum() / tot), int((n > 0).sum())

    rows = []
    for k, direction, group, label in VARIANTS:
        row = {"variant": k, "direction": direction, "group": group, "label": label,
               "stocks": len(CODES[k]), "unbuyable": UNB[k][0] + UNB[k][1],
               "unb_limit": UNB[k][0], "unb_suspend": UNB[k][1]}
        for hz in HORIZONS:
            s = stat(k, hz)
            row[f"n{hz}"] = s["n"]
            row[f"ndays{hz}"] = s["ndays"]
            row[f"excess{hz}"] = s["gross"]
            row[f"net{hz}"] = s["net"]
            row[f"t{hz}"] = s["t"]
            row[f"p{hz}"] = s["p"]
            if hz in (5, 20):
                row[f"excess_day{hz}"] = s["day"]
        rows.append(row)
    res = pd.DataFrame(rows)

    # BH-FDR（对全部 变体×持有期 检验）
    tests = []
    for _, r in res.iterrows():
        for hz in HORIZONS:
            tests.append((r["variant"], hz, r[f"p{hz}"]))
    tp = pd.DataFrame(tests, columns=["variant", "horizon", "p"])
    tp["bh"] = bh_fdr(tp["p"].to_numpy(float))
    for hz in HORIZONS:
        mp = {r.variant: bool(r.bh) for r in tp[tp["horizon"] == hz].itertuples()}
        res[f"fdr{hz}"] = res["variant"].map(mp)

    # 分年度
    yr_rows = []
    for k in KEYS:
        for y in range(2005, 2027):
            sel = years == y
            r = {"variant": k, "year": y}
            n5, ex5, _ = seg(k, 5, sel)
            n20, ex20, _ = seg(k, 20, sel)
            r["n5"], r["excess5"] = n5, ex5
            r["n20"], r["excess20"] = n20, ex20
            if n5 or n20:
                yr_rows.append(r)
    yearly = pd.DataFrame(yr_rows)

    # 股价分组
    buck_rows = []
    for k in KEYS:
        for hz in (5, 20):
            for bi, (bname, _, _) in enumerate(BUCKETS):
                s = stat(k, hz, BUCK[(k, hz, bi)])
                if s["n"] == 0:
                    continue
                buck_rows.append({"variant": k, "horizon": hz, "bucket": bname,
                                  "n": s["n"], "ndays": s["ndays"],
                                  "excess": s["gross"], "net": s["net"], "t": s["t"]})
    buckets = pd.DataFrame(buck_rows)

    # 样本外三段
    oos_rows = []
    for k, direction, _, _ in VARIANTS:
        r = {"variant": k, "direction": direction}
        segs = {}
        for name, y0, y1 in SEGMENTS:
            sel = (years >= y0) & (years <= y1)
            _, ex, _ = seg(k, 5, sel)
            if np.isfinite(ex):
                r[f"{name}_ex5"] = ex
                r[f"{name}_net5"] = ex - COST
                segs[name] = ex - COST
            else:
                r[f"{name}_ex5"], r[f"{name}_net5"] = np.nan, np.nan
        if len(segs) == 3:
            sign = 1 if direction == "买" else -1
            r["oos_consistent"] = bool(all(np.sign(v) == sign for v in segs.values()))
        else:
            r["oos_consistent"] = False
        oos_rows.append(r)
    oos = pd.DataFrame(oos_rows)

    # 稳健性
    stable, price_flag = {}, {}
    for k, direction, _, _ in VARIANTS:
        yg = yearly[(yearly["variant"] == k) & (yearly["n5"] >= 20)].dropna(subset=["excess5"])
        sign = 1 if direction == "买" else -1
        n_y = len(yg)
        share = float((np.sign(yg["excess5"]) == sign).mean()) if n_y else 0.0
        stable[k] = bool(n_y >= 5 and share >= 0.6)
        bg = buckets[(buckets["variant"] == k) & (buckets["horizon"] == 5)].set_index("bucket")["net"]
        ex5 = float(res.loc[res["variant"] == k, "net5"].iloc[0])
        price_flag[k] = bool(
            {"lt10", "10_20", "gt20"}.issubset(bg.index)
            and (np.sign(ex5) == sign) and (np.sign(bg["lt10"]) == sign) and (np.sign(bg["gt20"]) != sign)
        )
    res["yearly_stable"] = res["variant"].map(stable)
    res["yearly_share"] = res["variant"].map(
        lambda k: float((np.sign(yearly[(yearly["variant"] == k) & (yearly["n5"] >= 20)]["excess5"]) ==
                         (1 if dict((v[0], v[1]) for v in VARIANTS)[k] == "买" else -1)).mean())
        if ((yearly["variant"] == k) & (yearly["n5"] >= 20)).any() else np.nan)
    res["price_effect"] = res["variant"].map(price_flag)
    res = res.merge(oos, on=["variant", "direction"], how="left")

    # 效应量门槛（买为正、卖为负；扣成本后）
    def gate(v, direction, hz, thr):
        sgn = 1 if direction == "买" else -1
        return bool(sgn * res.loc[res["variant"] == v, f"net{hz}"].iloc[0] >= thr)

    res["gate5"] = [gate(v, d, 5, 0.003) for v, d in zip(res["variant"], res["direction"])]
    res["gate20"] = [gate(v, d, 20, 0.010) for v, d in zip(res["variant"], res["direction"])]
    res["tradeable"] = res["gate5"] | res["gate20"]

    keep = ["variant", "direction", "group", "label", "stocks", "n3", "ndays3", "excess3", "net3",
            "n5", "ndays5", "excess5", "net5", "excess_day5", "t5", "p5", "fdr5",
            "n10", "ndays10", "excess10", "net10", "t10",
            "n20", "ndays20", "excess20", "net20", "excess_day20", "t20", "p20", "fdr20",
            "unbuyable", "unb_limit", "unb_suspend",
            "oos_consistent", "train_net5", "valid_net5", "test_net5",
            "yearly_stable", "yearly_share", "price_effect", "gate5", "gate20", "tradeable"]
    lb = res[keep].copy()
    lb["sort_key"] = np.where(lb["direction"] == "买", -lb["net5"], lb["net5"])
    lb = lb.sort_values(["direction", "sort_key"]).drop(columns=["sort_key"])
    lb.to_csv(os.path.join(OUT, "leaderboard.csv"), index=False, encoding="utf-8-sig")
    res.to_csv(os.path.join(OUT, "sweep_stats.csv"), index=False, encoding="utf-8-sig")
    yearly.to_csv(os.path.join(OUT, "sweep_yearly.csv"), index=False, encoding="utf-8-sig")
    buckets.to_csv(os.path.join(OUT, "sweep_price_buckets.csv"), index=False, encoding="utf-8-sig")
    oos.to_csv(os.path.join(OUT, "sweep_oos.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame([{"variant": k, "unb_limit": UNB[k][0], "unb_suspend": UNB[k][1],
                   "unbuyable": UNB[k][0] + UNB[k][1]} for k in KEYS]).to_csv(
        os.path.join(OUT, "sweep_unbuyable.csv"), index=False, encoding="utf-8-sig")

    pd.set_option("display.width", 260)
    m = len(tp)
    print(f"[tests] m={m} fdr_pass={int(tp['bh'].sum())}", flush=True)
    buy = lb[lb["direction"] == "买"].head(5)
    sell = lb[lb["direction"] == "卖"].head(5)
    print("[buy_top5]\n" + buy[["variant", "n5", "net5", "t5", "fdr5", "net20", "oos_consistent",
                                "yearly_stable", "price_effect"]].to_string(index=False), flush=True)
    print("[sell_bottom5]\n" + sell[["variant", "n5", "net5", "t5", "fdr5", "net20", "oos_consistent",
                                     "yearly_stable", "price_effect"]].to_string(index=False), flush=True)
    print("[tradeable]\n" + lb[lb["tradeable"]][["variant", "direction", "net5", "net20", "t5",
                                                 "fdr5", "oos_consistent"]].to_string(index=False), flush=True)
    print(f"[done] errs={errs} elapsed={time.time()-t0:.0f}s", flush=True)
    return 0


def main() -> int:
    def _arg(name, default=None):
        if name in sys.argv:
            return sys.argv[sys.argv.index(name) + 1]
        return default

    mode = _arg("--mode", "fixed")
    workers = int(_arg("--workers", 8))
    out_dir = _arg("--out", OUT)
    limit = _arg("--limit")
    files = _stock_files()
    if limit:
        files = files[:int(limit)]
    if mode == "fixed":
        return run_fixed(files, out_dir)
    if mode == "eqfreq":
        return run_eqfreq(files, out_dir, workers)
    print(f"unknown --mode {mode!r}，允许 fixed / eqfreq", flush=True)
    return 2


if __name__ == "__main__":
    sys.exit(main())
