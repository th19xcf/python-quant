#!/usr/bin/env python3
"""
超跌止跌买点验证（信号层 + 组合层），只读 data/local_store。

口径：全 A（剔 ST、上市不足 60 日）、2005 起、后复权、T 日收盘信号 → T+1 开盘买入
（开盘涨停/停牌剔除）、持有 3/5/10/20 日收盘平仓、往返成本 0.13%。
同时报绝对净收益（不是只看超额）、对全 A 等权超额、胜率、样本数、按信号日聚类 t、BH-FDR。

四类候选规则（研究假设）：
  A 超跌+止跌：RSI14<thr；5 日跌幅<thr；T 日收阳；T 日收盘>T-1 收盘；T 日未触跌停
  B 超跌后突破确认：BIAS12<-10% 或 RSI14<30；3 日最低价不再下移；收盘突破前 3 日最高收盘；
    成交量高于过去 5 日均量
  C 相对弱势修复：20 日相对全 A 跑输>10%；RSI<40；近 3 日相对市场改善；收盘>MA5
  D 恐慌日反转：全市场超跌股票数异常放大 + 等权指数当日跌幅>2% + 个股超跌且止跌

环境过滤（逐项开关）：E1 等权指数>20 日均线；E2 全市场成交额>20 日均额；E3 非「指数在 MA20 下且 5 日跌>3%」

输出：work/超跌止跌买点_2026-11-21/{leaderboard.csv, 消融.csv, 组合层.csv, market.parquet}
用法：
  python tools/oversold_rebound.py --stage signal [--limit 200]
  python tools/oversold_rebound.py --stage portfolio --rule "A:30,-0.08,e1" [--rule "B:none"]
"""

from __future__ import annotations

import argparse
import glob
import math
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import date

import numpy as np
import pandas as pd
import polars as pl

ROOT = r"e:\code\python\quant"
STORE = os.path.join(ROOT, "data", "local_store")
OUT = os.path.join(ROOT, "work", "超跌止跌买点_2026-11-21")
os.makedirs(OUT, exist_ok=True)

START = pd.Timestamp("2005-01-01")
HORIZONS = (3, 5, 10, 20)
COST = 0.0013
BASE_ORD = date(1990, 1, 1).toordinal()
NB = 15000
EPS = 1e-9
CHINEXT_20PCT = date(2020, 8, 24)
SEGMENTS = (("train", 2005, 2014), ("valid", 2015, 2019), ("test", 2020, 2026))
MAXPOS = 20

YEAR = np.array([date.fromordinal(i + BASE_ORD).year for i in range(NB)], dtype=np.int16)
DOW = np.array([date.fromordinal(i + BASE_ORD).weekday() for i in range(NB)], dtype=np.int8)


# ------------------------------------------------------------------ 基础 ----
def stock_files():
    files = []
    for mkt in ("sh", "sz", "bj"):
        files.extend(sorted(glob.glob(os.path.join(STORE, "quotes", "stock", mkt, "*.parquet"))))
    return files


def load_meta():
    sb = pl.read_parquet(os.path.join(STORE, "meta", "stock_basic.parquet")).to_pandas()
    list_date = {r.ts_code: pd.Timestamp(r.list_date) for r in sb.itertuples() if pd.notna(r.list_date)}
    st = pl.read_parquet(os.path.join(STORE, "meta", "st", "st_intervals.parquet")).to_pandas()
    st_map = {}
    for code, g in st.groupby("ts_code"):
        st_map[code] = [(pd.Timestamp(s), pd.Timestamp(e) if pd.notna(e) else pd.Timestamp("2099-01-01"))
                        for s, e in zip(g["start_date"], g["end_date"])]
    return list_date, st_map


LIST_DATE, ST_MAP = load_meta()


def ords_of(dates) -> np.ndarray:
    return pd.DatetimeIndex(dates).values.astype("datetime64[D]").astype(np.int64) + 719163 - BASE_ORD


def round_half_up(x):
    return np.floor(x * 100.0 + 0.5) / 100.0


def sma_tdx(s: pd.Series, n: float, m: float = 1.0) -> pd.Series:
    return s.ewm(alpha=m / n, adjust=False).mean()


def st_flags(code, dates: pd.Series) -> np.ndarray:
    out = np.zeros(len(dates), dtype=bool)
    for s, e in ST_MAP.get(code, []):
        out |= ((dates >= s) & (dates <= e)).to_numpy()
    return out


def limit_pct_arr(code, dser: pd.Series, st: np.ndarray) -> np.ndarray:
    d = dser.dt.date
    if code.startswith(("688", "689")):
        pct = np.full(len(dser), 0.20)
    elif code.startswith(("300", "301")):
        pct = np.where(d >= CHINEXT_20PCT, 0.20, 0.10)
    elif code.endswith(".BJ"):
        pct = np.full(len(dser), 0.30)
    else:
        pct = np.where(st, 0.05, 0.10)
    return pct


def _load(path):
    try:
        raw = pl.read_parquet(path)
    except Exception:
        return None
    need = ["date", "ts_code", "open", "high", "low", "close", "pre_close",
            "volume", "amount", "hfq_factor"]
    if raw.height < 120 or any(c not in raw.columns for c in need):
        return None
    d = raw.select([pl.col(c) for c in need]).to_pandas()
    d["date"] = pd.to_datetime(d["date"])
    return d.sort_values("date").reset_index(drop=True)


def _features(d):
    """返回指标/可交易性数组（全部因果，T 日只用 T 及以前）。"""
    code = str(d["ts_code"].iloc[0])
    f = lambda col: pd.to_numeric(d[col], errors="coerce").to_numpy(float)
    o, h, lo, c = f("open"), f("high"), f("low"), f("close")
    pclose, vol = f("pre_close"), f("volume")
    hf = np.nan_to_num(f("hfq_factor"), nan=1.0, posinf=1.0, neginf=1.0)
    hf[~np.isfinite(hf) | (hf <= 0)] = 1.0
    qc = pd.Series(c * hf)
    qo, qh, ql = pd.Series(o * hf), pd.Series(h * hf), pd.Series(lo * hf)

    ma5 = qc.rolling(5).mean()
    ma10 = qc.rolling(10).mean()
    ma12 = qc.rolling(12).mean()
    delta = qc.diff()
    up = delta.clip(lower=0)
    dn = -delta.clip(upper=0)
    au, ad = sma_tdx(up, 14), sma_tdx(dn, 14)
    rsi = 100 * au / (au + ad)
    bias12 = (qc - ma12) / ma12 * 100
    ret1 = qc / qc.shift(1) - 1
    ret5 = qc / qc.shift(5) - 1
    amt_arr = np.nan_to_num(f("amount"), nan=0.0, posinf=0.0, neginf=0.0)
    r1 = ret1.to_numpy(float)
    illiq = np.divide(np.abs(r1), amt_arr, out=np.full(len(r1), np.nan), where=amt_arr > 0)
    amihud = pd.Series(illiq).rolling(20).mean().to_numpy(float)

    # 市场（全 A 等权）对齐
    ord_idx = ords_of(d["date"])
    mret = MKT_RET.reindex(ord_idx).to_numpy(float)
    mret20 = MKT_RET20.reindex(ord_idx).to_numpy(float)
    mret3 = MKT_RET3.reindex(ord_idx).to_numpy(float)
    sret20 = (qc / qc.shift(20) - 1).to_numpy(float)
    sret3 = (qc / qc.shift(3) - 1).to_numpy(float)
    rel20 = sret20 - mret20
    rel3 = sret3 - mret3

    vol5 = pd.Series(vol).rolling(5).mean().shift(1)
    llv3 = ql.rolling(3).min()
    llv3_prev = llv3.shift(3)
    hhc3_prev = qc.rolling(3).max().shift(1)

    st = st_flags(code, d["date"])
    pct = limit_pct_arr(code, d["date"], st)
    prev = np.where(np.isfinite(pclose) & (pclose > 0), pclose, c)
    lup = round_half_up(prev * (1 + pct))
    ldn = round_half_up(prev * (1 - pct))

    entry = np.append(qo.to_numpy(float)[1:], np.nan)
    rets = np.full((len(d), len(HORIZONS)), np.nan)
    for j, hz in enumerate(HORIZONS):
        ex = np.append(qc.to_numpy(float)[hz:], np.full(hz, np.nan))
        rets[:, j] = ex / entry - 1.0
    nxt_o = np.append(o[1:], np.nan)
    nxt_v = np.append(vol[1:], np.nan)
    nxt_lup = np.append(lup[1:], np.nan)
    suspended = ~np.isfinite(nxt_o) | ~(nxt_v > 0)
    limit_block = np.isfinite(nxt_o) & (nxt_v > 0) & (nxt_o >= nxt_lup - EPS)
    buyable = ~suspended & ~limit_block

    ld = LIST_DATE.get(code, pd.Timestamp("1990-01-01"))
    pos = np.arange(len(d))
    listed = (d["date"] >= ld).to_numpy() & (pos >= 60)
    base = listed & ~st & (d["date"] >= START).to_numpy()

    idx = ord_idx
    return dict(
        code=code, idx=idx, base=base, buyable=buyable, rets=rets,
        o=o, h=h, lo=lo, c=c, pclose=pclose, vol=vol, hf=hf,
        qc=qc.to_numpy(float), qo=qo.to_numpy(float), qh=qh.to_numpy(float), ql=ql.to_numpy(float),
        ma5=ma5.to_numpy(float), ma10=ma10.to_numpy(float),
        rsi=rsi.to_numpy(float), bias12=bias12.to_numpy(float),
        ret1=ret1.to_numpy(float), ret5=ret5.to_numpy(float),
        amihud=amihud,
        rel20=rel20, rel3=rel3, vol5=vol5.to_numpy(float),
        llv3=llv3.to_numpy(float), llv3_prev=llv3_prev.to_numpy(float),
        hhc3_prev=hhc3_prev.to_numpy(float), ldn=ldn, lup=lup,
        suspended=suspended, limit_block=limit_block, mret=mret,
        limit_block_n=int((base & limit_block).sum()), susp_n=int((base & suspended).sum()),
    )


# --------------------------------------------------------------- 市场环境 ----
MKT_RET = None       # pd.Series（ordinal → 全 A 等权日收益）
MKT_RET20 = None
MKT_RET3 = None
ENV1 = ENV2 = ENV3 = None


def _init_worker(m1, m2, m3):
    global MKT_RET, MKT_RET20, MKT_RET3
    MKT_RET, MKT_RET20, MKT_RET3 = m1, m2, m3


def market_part(path):
    try:
        d = pl.read_parquet(path, columns=["date", "pre_close", "close", "amount"]).to_pandas()
    except Exception:
        return None
    if len(d) < 20:
        return None
    d = d.sort_values("date")
    pre = pd.to_numeric(d["pre_close"], errors="coerce").to_numpy(float)
    c = pd.to_numeric(d["close"], errors="coerce").to_numpy(float)
    a = pd.to_numeric(d["amount"], errors="coerce").to_numpy(float)
    ok = np.isfinite(pre) & (pre > 0) & np.isfinite(c)
    r = np.where(ok, c / np.where(pre > 0, pre, 1) - 1, np.nan)
    idx = ords_of(d["date"])
    aok = np.isfinite(a) & (a > 0)
    return (idx[ok].astype(np.int32), r[ok].astype(np.float32),
            idx[aok].astype(np.int32), a[aok].astype(np.float64))


def build_market(files, workers=8):
    global MKT_RET, MKT_RET20, MKT_RET3, ENV1, ENV2, ENV3
    t0 = time.time()
    idxs, rets, aids, amts = [], [], [], []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        for i, r in enumerate(ex.map(market_part, files, chunksize=32), 1):
            if r is None:
                continue
            idxs.append(r[0]); rets.append(r[1]); aids.append(r[2]); amts.append(r[3])
            if i % 1000 == 0:
                print(f"[market {i}/{len(files)}] {time.time()-t0:.0f}s", flush=True)
    idx = np.concatenate(idxs); ret = np.concatenate(rets).astype(float)
    aid = np.concatenate(aids); amt = np.concatenate(amts)
    sr = np.bincount(idx, weights=ret, minlength=NB)
    nr = np.bincount(idx, minlength=NB)
    sa = np.bincount(aid, weights=amt, minlength=NB)
    ew = np.where(nr > 0, sr / np.maximum(nr, 1), np.nan)
    ords = np.nonzero(nr > 0)[0]
    ewser = pd.Series(ew[ords], index=ords).sort_index()
    amtser = pd.Series(sa[ords], index=ords).sort_index()
    level = (1 + ewser).cumprod()
    ma20 = level.rolling(20).mean()
    r5 = level.pct_change(5)
    amt_ma20 = amtser.rolling(20).mean()

    def to_nb(s, dtype=np.float32, fill=np.nan):
        out = np.full(NB, fill, dtype=dtype)
        out[s.index.to_numpy()] = s.to_numpy()
        return out

    global MKT_RET, MKT_RET20, MKT_RET3, ENV1, ENV2, ENV3
    MKT_RET = ewser
    MKT_RET20 = level.pct_change(20)
    MKT_RET3 = level.pct_change(3)
    ENV1 = to_nb(level > ma20, dtype=bool, fill=False)
    ENV2 = to_nb(amtser > amt_ma20, dtype=bool, fill=False)
    ENV3 = to_nb(~((level < ma20) & (r5 < -0.03)), dtype=bool, fill=False)
    print(f"[market] days={len(ords)} {time.time()-t0:.0f}s", flush=True)
    return pd.DataFrame({
        "ordinal": ords, "ew_ret": ewser.to_numpy(), "index": level.to_numpy(),
        "amount": amtser.to_numpy(), "e1": ENV1[ords], "e2": ENV2[ords], "e3": ENV3[ords],
    })


# --------------------------------------------------------------- 事件抽取 ----
def process_signals(path, family):
    d = _load(path)
    if d is None:
        return None
    ft = _features(d)
    idx, base, buy = ft["idx"], ft["base"], ft["buyable"]
    rets = ft["rets"]
    bench = {}
    for j, hz in enumerate(HORIZONS):
        ok = np.isfinite(rets[:, j])
        bench[hz] = (idx[ok].astype(np.int32), rets[ok, j].astype(np.float32))
    ev = None
    ov = None
    bov = bvalid = None
    if family == "A":
        early = (ft["rsi"] < 32) | (ft["ret5"] < -0.06)
        cand = base & buy & early & np.isfinite(ft["rsi"]) & np.isfinite(ft["ret5"])
        ii = np.nonzero(cand)[0]
        ev = dict(
            day=idx[ii].astype(np.int32),
            rsi=ft["rsi"][ii].astype(np.float32),
            r5=ft["ret5"][ii].astype(np.float32),
            c1=(ft["c"][ii] > ft["o"][ii]),
            c2=(ft["c"][ii] > ft["pclose"][ii]),
            c3=(ft["lo"][ii] > ft["ldn"][ii] - EPS),
            amihud=ft["amihud"][ii].astype(np.float32),
            rets=rets[ii].astype(np.float32),
        )
        ovm = base & (ft["rsi"] < 30) & (ft["ret5"] < -0.08)
        ov = idx[ovm].astype(np.int32)
        bias_ok = np.isfinite(ft["bias12"])
        bov = idx[bias_ok & (ft["bias12"] < -10)].astype(np.int32)
        bvalid = idx[bias_ok].astype(np.int32)
    elif family == "B":
        early = (ft["bias12"] < -10) | (ft["rsi"] < 30)
        cand = base & buy & early
        ii = np.nonzero(cand)[0]
        ev = dict(
            day=idx[ii].astype(np.int32),
            bias=ft["bias12"][ii].astype(np.float32),
            rsi=ft["rsi"][ii].astype(np.float32),
            nolower=(ft["llv3"][ii] >= ft["llv3_prev"][ii] - EPS),
            brk=(ft["c"][ii] > ft["hhc3_prev"][ii]),
            volup=(ft["vol"][ii] > ft["vol5"][ii]),
            rets=rets[ii].astype(np.float32),
        )
    elif family == "C":
        cand = base & buy & (ft["rsi"] < 40)
        ii = np.nonzero(cand)[0]
        ev = dict(
            day=idx[ii].astype(np.int32),
            rel20=ft["rel20"][ii].astype(np.float32),
            rsi=ft["rsi"][ii].astype(np.float32),
            rel3=ft["rel3"][ii].astype(np.float32),
            ma5up=(ft["c"][ii] > ft["ma5"][ii]),
            rets=rets[ii].astype(np.float32),
        )
    elif family == "D":
        cand = (base & buy & (ft["rsi"] < 30) & (ft["ret5"] < -0.08) &
                (ft["c"] > ft["o"]) & (ft["c"] > ft["pclose"]) & (ft["lo"] > ft["ldn"] - EPS))
        ii = np.nonzero(cand)[0]
        ev = dict(
            day=idx[ii].astype(np.int32),
            mret=ft["mret"][ii].astype(np.float32),
            rets=rets[ii].astype(np.float32),
        )
    return dict(bench=bench, ev=ev, ov=ov, all_idx=idx.astype(np.int32),
                bov=bov, bvalid=bvalid,
                unb=(ft["limit_block_n"], ft["susp_n"]), code=ft["code"])


# --------------------------------------------------------------- 统计工具 ----
def pval_t(t, df):
    if not np.isfinite(t):
        return np.nan
    return math.erfc(abs(t) / math.sqrt(2.0))


def bh_fdr(pvals, alpha=0.05):
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
    k = int(np.max(np.where(passed)[0]))
    ok[np.where(valid)[0][order[:k + 1]]] = True
    return ok


def cluster_exc_t(day, exc):
    n = len(exc)
    nd = np.bincount(day, minlength=NB)
    sd = np.bincount(day, weights=exc, minlength=NB)
    G = int((nd > 0).sum())
    if n == 0 or G < 5:
        return np.nan, G, np.nan
    m = float(exc.mean())
    resid = sd - nd * m
    var = (G / (G - 1.0)) * float((resid ** 2).sum()) / (n ** 2) if G > 1 else np.nan
    t = m / math.sqrt(var) if var and var > 0 else np.nan
    return t, G, pval_t(t, G - 1)


def daily_t(day, x):
    nd = np.bincount(day, minlength=NB)
    sd = np.bincount(day, weights=x, minlength=NB)
    ok = nd > 0
    dd = sd[ok] / nd[ok]
    G = len(dd)
    if G < 5:
        return np.nan
    return float(dd.mean() / (dd.std(ddof=1) / math.sqrt(G)))


def eval_mask(ev, bench, mask, hz):
    j = HORIZONS.index(hz)
    day = ev["day"][mask]
    r = ev["rets"][mask, j].astype(float)
    ok = np.isfinite(r) & np.isfinite(bench[hz][day])
    day, r = day[ok], r[ok]
    if len(r) == 0:
        return None
    b = bench[hz][day].astype(float)
    exc = r - b
    t_exc, G, p = cluster_exc_t(day, exc)
    return dict(
        n=len(r), ndays=G,
        abs=float(r.mean()) - COST, exc=float(exc.mean()) - COST,
        win=float((r > COST).mean()),
        t_abs=daily_t(day, r - COST), t_exc=t_exc, p=p,
        day_arr=day, r=r, exc_arr=exc,
    )


def seg_stat(day, r, exc, y0, y1):
    yrs = YEAR[day]
    sel = (yrs >= y0) & (yrs <= y1)
    if sel.sum() == 0:
        return np.nan, np.nan
    return float(r[sel].mean()) - COST, float(exc[sel].mean()) - COST


# ------------------------------------------------------------------- 信号层 ----
ENV_OPTS = ("none", "e1", "e2", "e3", "e1e2")


def env_mask(day, env):
    if env == "none":
        return np.ones(len(day), dtype=bool)
    m = np.ones(len(day), dtype=bool)
    if "e1" in env:
        m &= ENV1[day]
    if "e2" in env:
        m &= ENV2[day]
    if "e3" in env:
        m &= ENV3[day]
    return m


def run_signal(files, workers=8):
    global PANIC
    t0 = time.time()
    market = build_market(files, workers)
    mkt_path = os.path.join(OUT, "market.parquet")
    market.to_parquet(mkt_path, index=False)

    evs = {}
    bench = {hz: [np.zeros(NB), np.zeros(NB)] for hz in HORIZONS}
    ov_cnt = np.zeros(NB)
    bov_cnt = np.zeros(NB)
    bvalid_cnt = np.zeros(NB)
    valid_cnt = np.zeros(NB)
    unb = {}

    for family in ("A", "B", "C", "D"):
        print(f"[pass {family}] start {time.time()-t0:.0f}s", flush=True)
        recs = []
        with ProcessPoolExecutor(max_workers=workers, initializer=_init_worker,
                                 initargs=(MKT_RET, MKT_RET20, MKT_RET3)) as ex:
            for i, r in enumerate(ex.map(process_signals, files, [family] * len(files), chunksize=16), 1):
                if r is None:
                    continue
                if family == "A":
                    bch = r.pop("bench")
                    for hz in HORIZONS:
                        ii, rr = bch[hz]
                        bench[hz][0] += np.bincount(ii, weights=rr, minlength=NB)
                        bench[hz][1] += np.bincount(ii, minlength=NB)
                    ov_cnt[:] += np.bincount(r.pop("ov"), minlength=NB)
                    bov_cnt[:] += np.bincount(r.pop("bov"), minlength=NB)
                    bvalid_cnt[:] += np.bincount(r.pop("bvalid"), minlength=NB)
                    valid_cnt[:] += np.bincount(r["all_idx"], minlength=NB)
                else:
                    r.pop("bench", None)
                    r.pop("ov", None)
                ub = r.get("unb", (0, 0))
                prev = unb.get(family, [0, 0])
                unb[family] = [prev[0] + ub[0], prev[1] + ub[1]]
                recs.append(r["ev"])
                if i % 1000 == 0:
                    print(f"[{family} {i}/{len(files)}] {time.time()-t0:.0f}s", flush=True)
        if not recs:
            continue
        keys = recs[0].keys()
        ev = {k: np.concatenate([x[k] for x in recs]) for k in keys}
        evs[family] = ev
        print(f"[pass {family}] events={len(ev['day'])} {time.time()-t0:.0f}s", flush=True)

    bench_mean = {}
    for hz in HORIZONS:
        s, n = bench[hz]
        bench_mean[hz] = np.where(n > 0, s / np.maximum(n, 1), np.nan).astype(np.float32)

    # 恐慌日（市场宽度定义）：每日 BIAS12<-10% 股票数 / 当日可算该指标股票数，
    # 阈值 = 此前三个交易年（750 交易日）该比例的 95 分位，且只用前一日及以前数据（shift 1）
    ratio = np.where(bvalid_cnt > 0, bov_cnt / np.maximum(bvalid_cnt, 1), np.nan)
    ords = np.nonzero(bvalid_cnt > 0)[0]
    rs = pd.Series(ratio[ords], index=ords)
    thr = rs.rolling(750, min_periods=250).quantile(0.95).shift(1)
    breadth_panic = np.zeros(NB, dtype=bool)
    breadth_panic[ords[(rs > thr).fillna(False).to_numpy()]] = True
    PANIC = breadth_panic & (MKT_RET.reindex(np.arange(NB)).to_numpy() < -0.02)  # 规则 D 仍叠加指数跌幅
    print(f"[panic] breadth_days={int(breadth_panic.sum())} with_index_drop={int(PANIC.sum())}", flush=True)
    market["panic"] = PANIC[market["ordinal"].to_numpy()]
    market["panic_breadth"] = breadth_panic[market["ordinal"].to_numpy()]
    market["bias_ratio"] = ratio[market["ordinal"].to_numpy()]
    market.to_parquet(mkt_path, index=False)

    rows = []
    yr_rows = []

    def add(family, label, params, env, mask, ev, extra=None):
        m = mask & env_mask(ev["day"], env)
        if m.sum() == 0:
            return
        got = {hz: eval_mask(ev, bench_mean, m, hz) for hz in HORIZONS}
        if got[5] is None:
            return
        g5, g20 = got[5], got[20]
        yrs = YEAR[g5["day_arr"]]
        row = dict(rule=label, family=family, env=env, params=params,
                   n=g5["n"], ndays=g5["ndays"],
                   year_first=int(yrs.min()), year_last=int(yrs.max()),
                   n_years=int(len(np.unique(yrs))),
                   win5=g5["win"], win20=g20["win"] if g20 else np.nan,
                   t_abs5=g5["t_abs"], t_exc5=g5["t_exc"], p5=g5["p"],
                   t_abs20=g20["t_abs"] if g20 else np.nan,
                   t_exc20=g20["t_exc"] if g20 else np.nan,
                   p20=g20["p"] if g20 else np.nan)
        for hz in HORIZONS:
            g = got[hz]
            row[f"abs{hz}"] = g["abs"] if g else np.nan
            row[f"exc{hz}"] = g["exc"] if g else np.nan
        for nm, y0, y1 in SEGMENTS:
            a5, e5 = seg_stat(g5["day_arr"], g5["r"], g5["exc_arr"], y0, y1)
            row[f"{nm}_abs5"], row[f"{nm}_exc5"] = a5, e5
        row["oos_abs5"] = bool(all(np.isfinite(row[f"{nm}_abs5"]) and row[f"{nm}_abs5"] > 0
                                   for nm, _, _ in SEGMENTS))
        row["oos_exc5"] = bool(all(np.isfinite(row[f"{nm}_exc5"]) and row[f"{nm}_exc5"] > 0
                                   for nm, _, _ in SEGMENTS))
        row["reliable"] = bool(row["n"] >= 300 and row["n_years"] >= 10)
        rows.append(row)
        yr_all = YEAR[g5["day_arr"]]
        for yy in np.unique(yr_all):
            sel = yr_all == yy
            yr_rows.append(dict(rule=label, family=family, env=env, year=int(yy),
                                n=int(sel.sum()),
                                abs5=float(g5["r"][sel].mean()) - COST,
                                exc5=float(g5["exc_arr"][sel].mean()) - COST,
                                win5=float((g5["r"][sel] > COST).mean())))

    # 规则 A：网格
    evA = evs["A"]
    for rsi_th in (28, 30, 32):
        for r5_th in (-0.06, -0.08, -0.10):
            base_m = (evA["c1"] & evA["c2"] & evA["c3"] &
                      (evA["rsi"] < rsi_th) & (evA["r5"] < r5_th))
            for env in ENV_OPTS:
                add("A", f"A_rsi{rsi_th}_r5{int(r5_th*100)}", f"rsi<{rsi_th},ret5<{r5_th:.0%}", env, base_m, evA)
    # 规则 B
    evB = evs["B"]
    bm = (evB["nolower"] & evB["brk"] & evB["volup"] &
          ((evB["bias"] < -10) | (evB["rsi"] < 30)))
    for env in ENV_OPTS:
        add("B", "B_break_confirm", "bias<-10%或rsi<30,3日低点不下移,破3日高收,量>5日均量", env, bm, evB)
    # 规则 C
    evC = evs["C"]
    cm = (evC["rel20"] < -0.10) & (evC["rel3"] > 0) & evC["ma5up"] & (evC["rsi"] < 40)
    for env in ENV_OPTS:
        add("C", "C_rel_repair", "rel20<-10%,rsi<40,rel3>0,close>ma5", env, cm, evC)
    # 规则 D
    evD = evs["D"]
    dm = (evD["mret"] < -0.02) & PANIC[evD["day"]]
    for env in ENV_OPTS:
        add("D", "D_panic_reversal", "panic,index<-2%,rsi<30,ret5<-8%,收阳,未跌停", env, dm, evD)

    lb = pd.DataFrame(rows)

    # BH-FDR（全部 规则×持有期）
    tests = []
    for i, r in lb.iterrows():
        for hz in HORIZONS:
            tests.append((i, hz, r.get(f"p{hz}", np.nan)))
    tp = pd.DataFrame(tests, columns=["i", "horizon", "p"])
    tp["bh"] = bh_fdr(tp["p"].to_numpy(float))
    for hz in HORIZONS:
        mp = {r.i: bool(r.bh) for r in tp[tp["horizon"] == hz].itertuples()}
        lb[f"fdr{hz}"] = lb.index.map(mp)

    cols = ["rule", "family", "env", "params", "n", "ndays", "n_years", "year_first", "year_last",
            "abs3", "abs5", "abs10", "abs20", "exc3", "exc5", "exc10", "exc20",
            "win5", "win20", "t_abs5", "t_exc5", "p5", "fdr5", "t_abs20", "t_exc20", "p20", "fdr20",
            "train_abs5", "valid_abs5", "test_abs5", "train_exc5", "valid_exc5", "test_exc5",
            "oos_abs5", "oos_exc5", "reliable"]
    lb = lb[cols].sort_values(["abs5", "abs20"], ascending=False)
    lb.to_csv(os.path.join(OUT, "leaderboard.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(yr_rows).to_csv(os.path.join(OUT, "年度分布.csv"), index=False, encoding="utf-8-sig")

    # ---- 逐项消融（四个家族都做，重点看最优组合）----
    abl = []

    def abl_row(fam, spec, cond, mask, env):
        m = mask & env_mask(evs[fam]["day"], env)
        got = {hz: eval_mask(evs[fam], bench_mean, m, hz) for hz in HORIZONS}
        if got[5] is None:
            return None
        return dict(family=fam, base_spec=spec, removed=cond, env=env, n=got[5]["n"],
                    abs5=got[5]["abs"], abs20=got[20]["abs"] if got[20] else np.nan,
                    exc5=got[5]["exc"], exc20=got[20]["exc"] if got[20] else np.nan,
                    t_exc5=got[5]["t_exc"], win5=got[5]["win"])

    # A：对最优组合 A_rsi30_r5-10 逐条删条件重跑，并对比不同环境开关
    for env_ab in ("e2", "none", "e1"):
        conds = {
            "全条件": (evA["c1"] & evA["c2"] & evA["c3"] & (evA["rsi"] < 30) & (evA["r5"] < -0.10)),
            "去掉RSI<30": (evA["c1"] & evA["c2"] & evA["c3"] & (evA["r5"] < -0.10) &
                          np.isfinite(evA["rsi"])),
            "去掉5日跌幅": (evA["c1"] & evA["c2"] & evA["c3"] & (evA["rsi"] < 30)),
            "去掉收阳": (evA["c2"] & evA["c3"] & (evA["rsi"] < 30) & (evA["r5"] < -0.10)),
            "去掉收盘高于昨收": (evA["c1"] & evA["c3"] & (evA["rsi"] < 30) & (evA["r5"] < -0.10)),
            "去掉未跌停": (evA["c1"] & evA["c2"] & (evA["rsi"] < 30) & (evA["r5"] < -0.10)),
            "只保留超跌(RSI+跌幅)": ((evA["rsi"] < 30) & (evA["r5"] < -0.10)),
            "只保留止跌(阳线+收盘高于昨收+未跌停)": (evA["c1"] & evA["c2"] & evA["c3"]),
            "去掉环境过滤": (evA["c1"] & evA["c2"] & evA["c3"] & (evA["rsi"] < 30) & (evA["r5"] < -0.10)),
        }
        for nm, m in conds.items():
            r = abl_row("A", "A_rsi30_r5-10", nm, m, "none" if nm == "去掉环境过滤" else env_ab)
            if r:
                abl.append(r)
    # B/C/D 消融
    for nm, m in {
        "全条件": bm,
        "去掉超跌": (evB["nolower"] & evB["brk"] & evB["volup"]),
        "去掉3日低点不下移": (((evB["bias"] < -10) | (evB["rsi"] < 30)) & evB["brk"] & evB["volup"]),
        "去掉突破3日高收": (((evB["bias"] < -10) | (evB["rsi"] < 30)) & evB["nolower"] & evB["volup"]),
        "去掉放量": (((evB["bias"] < -10) | (evB["rsi"] < 30)) & evB["nolower"] & evB["brk"]),
    }.items():
        r = abl_row("B", "B_break_confirm", nm, m, "none")
        if r:
            abl.append(r)
    for nm, m in {
        "全条件": cm,
        "去掉相对跑输": ((evC["rel3"] > 0) & evC["ma5up"] & (evC["rsi"] < 40)),
        "去掉RSI<40": ((evC["rel20"] < -0.10) & (evC["rel3"] > 0) & evC["ma5up"]),
        "去掉相对改善": ((evC["rel20"] < -0.10) & evC["ma5up"] & (evC["rsi"] < 40)),
        "去掉收盘>MA5": ((evC["rel20"] < -0.10) & (evC["rel3"] > 0) & (evC["rsi"] < 40)),
    }.items():
        r = abl_row("C", "C_rel_repair", nm, m, "none")
        if r:
            abl.append(r)
    pd.DataFrame(abl).to_csv(os.path.join(OUT, "消融.csv"), index=False, encoding="utf-8-sig")

    # ---- 补充：同日对照 / 恐慌事件聚类 / Amihud 分组 ----
    evA = evs["A"]
    spec_m = evA["c1"] & evA["c2"] & evA["c3"] & (evA["rsi"] < 30) & (evA["r5"] < -0.10)
    start_ord = int(ords_of(pd.DatetimeIndex([START]))[0])
    cal = np.nonzero((bvalid_cnt > 0) & (np.arange(NB) >= start_ord))[0]
    pdays = np.array([d for d in cal if breadth_panic[d]], dtype=int)
    ndays = np.array([d for d in cal if not breadth_panic[d]], dtype=int)
    in_p = np.isin(evA["day"], pdays)

    same = []
    for scope, dm, days in (("panic", in_p, pdays), ("normal", ~in_p, ndays)):
        for hz in HORIZONS:
            g = eval_mask(evA, bench_mean, spec_m & dm, hz)
            if g:
                same.append(dict(scope=scope, arm="oversold", hz=hz, n=g["n"], ndays=g["ndays"],
                                 abs=g["abs"], exc=g["exc"], win=g["win"], t_exc=g["t_exc"]))
            b = bench_mean[hz][days].astype(float)
            b = b[np.isfinite(b)]
            same.append(dict(scope=scope, arm="allA_ew", hz=hz, n=len(b), ndays=len(b),
                             abs=float(b.mean()) - COST, exc=0.0,
                             win=float((b > COST).mean()), t_exc=np.nan))
    df_same = pd.DataFrame(same)
    df_same.to_csv(os.path.join(OUT, "恐慌日同日对照.csv"), index=False, encoding="utf-8-sig")

    # 恐慌事件：连续交易日算同一次事件；剔除贡献最大（收益总额最大）的一次事件
    ev_rows = []
    g5 = eval_mask(evA, bench_mean, spec_m & in_p, 5)
    if g5 and len(g5["day_arr"]):
        day = g5["day_arr"]
        order = np.argsort(day, kind="stable")
        day = day[order]
        r_s = g5["r"][order]
        exc_s = g5["exc_arr"][order]
        pos = np.searchsorted(cal, day)
        eid = np.cumsum(np.r_[True, np.diff(pos) > 1]) - 1
        cnt = np.bincount(eid)
        net = r_s - COST
        exc = exc_s
        sums = np.bincount(eid, weights=net)
        means = sums / np.maximum(cnt, 1)
        worst_i = int(np.argmin(means))
        drop_i = int(np.argmax(sums))
        keep = eid != drop_i
        t_a, _, p_a = cluster_exc_t(day[keep], exc[keep])
        ev_rows.append(dict(n_events=int(len(cnt)), n_trades=int(len(day)),
                            mean_event_abs5=float(means.mean()),
                            worst_event_abs5=float(means.min()),
                            worst_event_trades=int(cnt[worst_i]),
                            drop_best_event_abs5=float(net[keep].mean()),
                            drop_best_event_exc5=float(exc[keep].mean()),
                            drop_best_event_n=int(keep.sum()),
                            drop_best_event_t_exc=t_a, drop_best_event_p=p_a))
    pd.DataFrame(ev_rows).to_csv(os.path.join(OUT, "恐慌事件.csv"), index=False, encoding="utf-8-sig")

    # Amihud 非流动性分组（|日收益|/成交额 20 日均值，信号样本三分位；零成交日已剔除）
    am = evA["amihud"]
    ok_am = np.isfinite(am) & spec_m
    q1, q2 = np.quantile(am[ok_am], [1 / 3, 2 / 3])
    amr = []
    for nm, gm in (("low", am <= q1), ("mid", (am > q1) & (am <= q2)), ("high", am > q2)):
        for hz in (5, 20):
            g = eval_mask(evA, bench_mean, spec_m & gm, hz)
            if g:
                amr.append(dict(amihud_group=nm, hz=hz, n=g["n"], abs=g["abs"], exc=g["exc"],
                                win=g["win"], t_exc=g["t_exc"],
                                median_amihud=float(np.nanmedian(am[spec_m & gm]))))
    df_am = pd.DataFrame(amr)
    df_am.to_csv(os.path.join(OUT, "Amihud分组.csv"), index=False, encoding="utf-8-sig")
    print("[same-day]\n" + df_same.to_string(index=False), flush=True)
    print("[events] " + str(ev_rows), flush=True)
    print("[amihud]\n" + df_am.to_string(index=False), flush=True)

    print(f"[leaderboard] rows={len(lb)}", flush=True)
    pd.set_option("display.width", 240)
    show = ["rule", "env", "n", "abs5", "exc5", "abs20", "exc20", "t_exc5", "fdr5", "oos_abs5", "reliable"]
    print(lb.head(15)[show].to_string(index=False), flush=True)
    print(f"[done] {time.time()-t0:.0f}s", flush=True)
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", default="signal")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args()
    fs = stock_files()
    if a.limit:
        fs = fs[:a.limit]
    if a.stage == "signal":
        sys.exit(run_signal(fs, a.workers))
    print("portfolio stage 见 oversold_rebound_portfolio.py", flush=True)
