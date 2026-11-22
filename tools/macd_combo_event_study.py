#!/usr/bin/env python3
"""
MACD 组合信号事件研究（只读 data/local_store，产物落 work/macd_组合信号_2026-11-21/）。

信号定义（四条同时成立）：
1. 周线 MACD 金叉：周线由日线重采样，只取「上一个完整周」的周线，严格口径=该周 DIF 上穿 DEA；
2. 日线 MACD 金叉：当日 DIF 上穿 DEA；
3. 0 < 日线 DIF < cap（默认 0.4，前复权价格口径）；
4. 量比 < 0.9（日线代理：当日量 / 前 5 日均量）。

口径：
- MACD 用前复权价（close × qfq_factor），与 src/tech_analysis/indicators/formulas.py 的
  ema_expr（alpha=2/(period+1), adjust=False）逐点等价；
- 收益用后复权价（close × hfq_factor），T+1 开盘买入、T+N 收盘卖出；
- 股价分布用未复权 close；
- T+1 开盘触及涨停（按板块/ST 规则四舍五入到分）或停牌则标记不可买入，主口径剔除。
"""

from __future__ import annotations

import glob
import math
import os
import sys
import time
from bisect import bisect_right
from datetime import date

import numpy as np
import pandas as pd
import polars as pl

ROOT = r"e:\code\python\quant"
STORE = os.path.join(ROOT, "data", "local_store")
OUT = os.path.join(ROOT, "work", "macd_组合信号_2026-11-21")
os.makedirs(OUT, exist_ok=True)

SIGNAL_START = pd.Timestamp("2005-01-01")
HORIZONS = (1, 3, 5, 10, 20)
CAPS = (0.2, 0.4, 0.8, 1.0)
LB_MAIN = 0.9
CHINEXT_20PCT = date(2020, 8, 24)
BASE_ORD = date(1990, 1, 1).toordinal()
ORD_BINS = 15000


# ---------------- ST 区间 ----------------
def load_st_intervals() -> dict[str, tuple[np.ndarray, np.ndarray]]:
    df = pl.read_parquet(os.path.join(STORE, "meta", "st", "st_intervals.parquet")).to_pandas()
    out = {}
    for code, g in df.groupby("ts_code"):
        starts, ends = [], []
        for _, r in g.iterrows():
            s = pd.Timestamp(r["start_date"]).date()
            e = pd.Timestamp(r["end_date"]).date() if pd.notna(r["end_date"]) else date(2099, 1, 1)
            starts.append(s.toordinal())
            ends.append(e.toordinal())
        order = np.argsort(starts)
        out[code] = (np.asarray(starts)[order], np.asarray(ends)[order])
    return out


ST_MAP = load_st_intervals()


def is_st(code: str, d: date) -> bool:
    iv = ST_MAP.get(code)
    if not iv:
        return False
    starts, ends = iv
    i = bisect_right(starts, d.toordinal()) - 1
    return i >= 0 and ends[i] >= d.toordinal()


def limit_pct(code: str, d: date) -> float:
    st = is_st(code, d)
    if code.startswith(("688", "689")):
        return 0.20
    if code.startswith(("300", "301")):
        return 0.20 if d >= CHINEXT_20PCT else 0.10
    if code.endswith(".BJ"):
        return 0.30
    return 0.05 if st else 0.10


def round_half_up(x: float, nd: int = 2) -> float:
    m = 10 ** nd
    return math.floor(x * m + 0.5) / m


def ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()


def process_stock(path: str):
    """单票：返回候选信号表 + 基准收益累加器。"""
    try:
        raw = pl.read_parquet(path)
    except Exception:
        return None, None
    if raw.height < 150:
        return None, None
    d = raw.select(
        pl.col("date").cast(pl.Date),
        "ts_code", "open", "high", "low", "close", "pre_close",
        "volume", "amount", "qfq_factor", "hfq_factor",
    ).to_pandas()
    d = d.sort_values("date").reset_index(drop=True)
    code = str(d["ts_code"].iloc[0])

    qf = d["qfq_factor"].fillna(1.0).to_numpy(dtype=float)
    hf = d["hfq_factor"].fillna(1.0).to_numpy(dtype=float)
    close = d["close"].to_numpy(dtype=float)
    qclose = pd.Series(close * qf)

    dif = ema(qclose, 12) - ema(qclose, 26)
    dea = ema(dif, 9)
    cross = (dif > dea) & (dif.shift(1) <= dea.shift(1))
    cross.iloc[:60] = False

    # 不复权 DIF（仅做口径对照）
    rclose = pd.Series(close)
    dif_raw = ema(rclose, 12) - ema(rclose, 26)
    dea_raw = ema(dif_raw, 9)
    cross_raw = (dif_raw > dea_raw) & (dif_raw.shift(1) <= dea_raw.shift(1))

    vol = pd.Series(d["volume"].to_numpy(dtype=float))
    lb = vol / vol.shift(1).rolling(5).mean()

    # 周线（ISO 周，周五为周期末；取该周最后一个交易日）
    w = pd.DataFrame({"date": d["date"], "qclose": close * qf})
    w["wk"] = w["date"].dt.to_period("W-FRI")
    wg = w.groupby("wk", sort=True).agg(w_date=("date", "last"), w_close=("qclose", "last"))
    wg["wdif"] = ema(wg["w_close"], 12) - ema(wg["w_close"], 26)
    wg["wdea"] = ema(wg["wdif"], 9)
    wg["wcross"] = (wg["wdif"] > wg["wdea"]) & (wg["wdif"].shift(1) <= wg["wdea"].shift(1))
    wg.loc[wg.index[:35], "wcross"] = False
    # 最近一次周线金叉落在哪一周（用于回答「周线是否早已金叉」）
    wg["_x_date"] = np.where(wg["wcross"], wg["w_date"], pd.NaT)
    wg["last_x_date"] = pd.Series(pd.to_datetime(wg["_x_date"])).ffill()
    wg = wg.reset_index()
    wg["wk_label"] = wg["wk"].astype(np.int64)

    dl = pd.DataFrame({"date": d["date"]})
    dl["wk_label"] = dl["date"].dt.to_period("W-FRI").astype(np.int64)
    m = pd.merge_asof(
        dl.sort_values("wk_label"),
        wg[["wk_label", "w_date", "wdif", "wdea", "wcross", "last_x_date"]].sort_values("wk_label"),
        on="wk_label", allow_exact_matches=False,
    ).sort_values("date")

    # 收益（后复权，T+1 开盘买入 → T+N 收盘卖出）
    op = d["open"].to_numpy(dtype=float) * hf
    cl = close * hf
    entry = np.append(op[1:], np.nan)
    rets = {}
    for n in HORIZONS:
        ex = np.append(cl[n:], np.full(n, np.nan)) if n < len(cl) else np.full(len(cl), np.nan)
        rets[n] = ex / entry - 1.0

    # T+1 可买性（原始价口径；收益另用后复权口径）
    raw_open = d["open"].to_numpy(dtype=float)
    nxt_open = np.append(raw_open[1:], np.nan)
    nxt_vol = np.append(vol.to_numpy()[1:], np.nan)
    nxt_pc = np.append(d["pre_close"].to_numpy(dtype=float)[1:], np.nan)
    pc = np.where(np.isfinite(nxt_pc) & (nxt_pc > 0), nxt_pc, d["close"].to_numpy(dtype=float))
    next_dates = d["date"].shift(-1)
    limit_up = np.full(len(d), np.nan)
    for i in range(len(d)):
        nd = next_dates.iloc[i]
        if pd.isna(nd):
            continue
        limit_up[i] = round_half_up(pc[i] * (1 + limit_pct(code, nd.date())), 2)
    buyable = np.isfinite(nxt_open) & (nxt_vol > 0) & (nxt_open < limit_up - 1e-9)
    t1_limit_touch = np.isfinite(nxt_open) & np.isfinite(limit_up) & (nxt_open >= limit_up - 1e-9)

    cand = pd.DataFrame({
        "date": d["date"], "ts_code": code,
        "close_raw": close, "qclose": qclose.to_numpy(dtype=float),
        "dif": dif.to_numpy(dtype=float), "dea": dea.to_numpy(dtype=float),
        "dif_rel": (dif / pd.Series(qclose)).to_numpy(dtype=float),
        "dif_raw": dif_raw.to_numpy(dtype=float), "cross_raw": cross_raw.to_numpy(),
        "lb": lb.to_numpy(dtype=float), "volume": vol.to_numpy(dtype=float),
        "w_date": m["w_date"].to_numpy(), "w_dif": m["wdif"].to_numpy(dtype=float),
        "w_dea": m["wdea"].to_numpy(dtype=float), "w_cross": m["wcross"].fillna(False).to_numpy(dtype=bool),
        "w_last_x_date": m["last_x_date"].to_numpy(),
        "buyable": buyable, "t1_limit_touch": t1_limit_touch,
    })
    for n in HORIZONS:
        cand[f"ret{n}"] = rets[n]

    mask = cross.to_numpy(dtype=bool)
    cand = cand.loc[mask]
    cand = cand[(cand["dif"] > 0) & ((cand["dif"] < 1.2) | (cand["dif_rel"] < 0.10))
                & (cand["lb"] < 1.1)]
    if cand.empty:
        cand = None

    # 基准累加（全 A 等权，任意可算行都计入）
    idx = (d["date"].dt.date.map(lambda x: x.toordinal()) - BASE_ORD).to_numpy()
    bench = {}
    for n in HORIZONS:
        r = rets[n]
        ok = np.isfinite(r)
        bench[n] = (
            np.bincount(idx[ok], weights=r[ok], minlength=ORD_BINS),
            np.bincount(idx[ok], minlength=ORD_BINS),
        )
    return cand, bench


def main() -> int:
    t0 = time.time()
    files = []
    for mkt in ("sh", "sz", "bj"):
        files.extend(sorted(glob.glob(os.path.join(STORE, "quotes", "stock", mkt, "*.parquet"))))
    print(f"[start] files={len(files)}", flush=True)

    cands, bsum, bcnt = [], {n: np.zeros(ORD_BINS) for n in HORIZONS}, {n: np.zeros(ORD_BINS) for n in HORIZONS}
    errs = 0
    for i, f in enumerate(files, 1):
        try:
            c, b = process_stock(f)
        except Exception as e:  # 单票失败不影响整体
            errs += 1
            if errs <= 5:
                print(f"[warn] {os.path.basename(f)}: {e}", flush=True)
            continue
        if c is not None:
            cands.append(c)
        if b is not None:
            for n in HORIZONS:
                bsum[n] += b[n][0]
                bcnt[n] += b[n][1]
        if i % 500 == 0:
            print(f"[{i}/{len(files)}] events={sum(len(x) for x in cands)} "
                  f"errs={errs} {time.time()-t0:.0f}s", flush=True)

    df = pd.concat(cands, ignore_index=True) if cands else pd.DataFrame()
    print(f"[load] candidates={len(df)} errs={errs} elapsed={time.time()-t0:.0f}s", flush=True)
    if df.empty:
        return 1

    df["year"] = df["date"].dt.year
    df["w_age_weeks"] = (df["date"].dt.normalize() - pd.to_datetime(df["w_date"])).dt.days / 7.0
    df["w_age_from_cross_weeks"] = (
        df["date"].dt.normalize() - pd.to_datetime(df["w_last_x_date"])).dt.days / 7.0
    df["w_loose"] = df["w_dif"] > df["w_dea"]

    # 基准
    idx = (df["date"].dt.date.map(lambda x: x.toordinal()) - BASE_ORD).to_numpy()
    for n in HORIZONS:
        with np.errstate(invalid="ignore", divide="ignore"):
            bench_mean = np.where(bcnt[n] > 0, bsum[n] / np.maximum(bcnt[n], 1), np.nan)
        df[f"bench{n}"] = bench_mean[idx]

    df = df[df["date"] >= SIGNAL_START].copy()
    print(f"[filter] since 2005: {len(df)}", flush=True)
    print(f"[weekly] rows={len(df)} strict_share={df['w_cross'].mean():.4f} "
          f"loose_share={df['w_loose'].mean():.4f}", flush=True)
    print("[weekly-age(loose)] " + df.loc[df["w_loose"], "w_age_from_cross_weeks"]
          .describe(percentiles=[0.25, 0.5, 0.75, 0.9]).to_string().replace("\n", " | "), flush=True)

    def variant(cap, lb_thr, weekly="strict", only_buyable=True, mode="abs"):
        v = df.copy()
        v = v[v["lb"] < lb_thr]
        if mode == "rel":
            v = v[(v["dif_rel"] > 0) & (v["dif_rel"] < cap)]
        else:
            v = v[(v["dif"] > 0) & (v["dif"] < cap)]
        v = v[v["w_cross"]] if weekly == "strict" else v[v["w_loose"]]
        if only_buyable:
            v = v[v["buyable"]]
        return v

    def stats(v, tag):
        rows = []
        for n in HORIZONS:
            r = v[f"ret{n}"].dropna()
            if r.empty:
                continue
            b = v.loc[r.index, f"bench{n}"]
            ex = r - b
            t = ex.mean() / (ex.std(ddof=1) / math.sqrt(len(ex))) if len(ex) > 2 and ex.std(ddof=1) > 0 else np.nan
            rows.append({
                "variant": tag, "horizon": n, "n": int(len(r)),
                "mean": r.mean(), "median": r.median(), "win": (r > 0).mean(),
                "bench": b.mean(), "excess": ex.mean(), "t_excess": t,
            })
        return pd.DataFrame(rows)

    # ---- 修正口径变体：相对 DIF × 周线严格（本周内金叉） ----
    new_main = variant(0.04, LB_MAIN, "strict", True, "rel")
    new_main_all = variant(0.04, LB_MAIN, "strict", False, "rel")
    prev_main = variant(0.4, LB_MAIN, "strict", True, "abs")
    only_dif = variant(0.04, LB_MAIN, "loose", True, "rel")
    prev_loose = variant(0.4, LB_MAIN, "loose", True, "abs")

    stats_all = [
        stats(new_main, "新主口径_相对4%_周线本周金叉"),
        stats(new_main_all, "新主口径_含T+1涨停不可买"),
        stats(prev_main, "上一版主口径_绝对0.4_周线本周金叉"),
        stats(only_dif, "隔离_只改DIF_相对4%_周线已金叉"),
        stats(prev_loose, "上一版宽松_绝对0.4_周线已金叉"),
        stats(variant(0.02, LB_MAIN, "strict", True, "rel"), "敏感性_相对2%_周线本周金叉"),
        stats(variant(0.06, LB_MAIN, "strict", True, "rel"), "敏感性_相对6%_周线本周金叉"),
    ]
    summary = pd.concat(stats_all, ignore_index=True)

    # 分年度（新主口径）
    yr = []
    for y, g in new_main.groupby("year"):
        row = {"year": int(y), "n": len(g), "stocks": g["ts_code"].nunique(),
               "win5": (g["ret5"] > 0).mean(), "mean5": g["ret5"].mean(),
               "excess5": (g["ret5"] - g["bench5"]).mean(),
               "mean10": g["ret10"].mean(), "mean20": g["ret20"].mean()}
        yr.append(row)
    yearly = pd.DataFrame(yr).sort_values("year")

    price_q = new_main["close_raw"].describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9])
    low_share = (new_main["close_raw"] < 10).mean()
    n_years = max(new_main["year"].nunique(), 1)
    print("[price] " + price_q.to_string().replace("\n", " | "), flush=True)
    print(f"[price-low] <10元占比={low_share:.4f} 年数={n_years} 年均={len(new_main)/n_years:.1f}", flush=True)
    print("[yearly]\n" + yearly.to_string(index=False), flush=True)
    print("[summary]\n" + summary.to_string(index=False), flush=True)

    # 关键计数
    print(f"[count] 新主口径信号={len(new_main)} 标的={new_main['ts_code'].nunique()} "
          f"含不可买={len(new_main_all)}; 上一版严格={len(prev_main)}; "
          f"只改DIF(宽松)={len(only_dif)}; 上一版宽松={len(prev_loose)}", flush=True)

    # 并排对照（5/10/20 日）
    cmp = summary[summary["horizon"].isin([5, 10, 20])].pivot(
        index="variant", columns="horizon", values=["n", "excess", "win", "t_excess"])
    print("[compare]\n" + cmp.to_string(), flush=True)

    # 落盘（rel_ 前缀，不覆盖上一版）
    cols = ["date", "ts_code", "close_raw", "qclose", "dif", "dif_rel", "dea", "lb",
            "w_date", "w_dif", "w_dea", "w_cross", "w_loose", "w_age_weeks",
            "w_age_from_cross_weeks", "buyable", "t1_limit_touch"]
    cols += [f"ret{n}" for n in HORIZONS] + [f"bench{n}" for n in HORIZONS]
    new_main[cols].to_csv(os.path.join(OUT, "rel_signals_primary.csv"), index=False, encoding="utf-8-sig")
    df.to_parquet(os.path.join(OUT, "rel_candidates.parquet"), index=False)
    summary.to_csv(os.path.join(OUT, "rel_stats_by_variant.csv"), index=False, encoding="utf-8-sig")
    yearly.to_csv(os.path.join(OUT, "rel_stats_by_year.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame({"metric": price_q.index, "value": price_q.values}).to_csv(
        os.path.join(OUT, "rel_price_distribution.csv"), index=False, encoding="utf-8-sig")
    cmp.to_csv(os.path.join(OUT, "rel_compare.csv"), encoding="utf-8-sig")
    print(f"[done] elapsed={time.time()-t0:.0f}s out={OUT}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
