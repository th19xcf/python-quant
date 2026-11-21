#!/usr/bin/env python3
"""
信号强度分层与按强度下注测试（规则 A：RSI30 / 5 日跌幅 −10%，无环境 37388 个、E2 15479 个）。

阶段 1（signal）：四个维度各分五组，报 5/20 日绝对净收益、对全 A 超额、胜率、按信号日聚类 t、
  单调性（Spearman、相邻组同向数、Q5 与均值差）与 BH-FDR。
  维度：depth=−5日跌幅、rsi=−RSI14、vol=当日成交量/前5日均量、panic=当日 BIAS12<−10% 股票占比。
阶段 2（portfolio）：平均加权 / 按信号层最强维度加权 / 只买最强 20%（因果扩展分位），
  周频与日频、hold20、20 只上限、单票 5%，与 11.62%/−32.55%/0.743/28.4% 对比。

产物：work/超跌止跌买点_2026-11-21/信号强度/{分层.csv, 单调性.csv, 加权组合层.csv, 结论.md}
用法：
  python tools/oversold_rebound_strength.py --stage signal
  python tools/oversold_rebound_strength.py --stage portfolio --wdim depth
"""

from __future__ import annotations

import argparse
import bisect
import math
import os
import sys

import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import oversold_rebound as ob  # noqa: E402
import oversold_rebound_portfolio as pb  # noqa: E402
import oversold_rebound_exposure as ex  # noqa: E402

OUT = os.path.join(ob.OUT, "信号强度")
os.makedirs(OUT, exist_ok=True)

RULE_SETS = [("A28_none", 28, "none"), ("A28_e2", 28, "e2"),
             ("A30_none", 30, "none"), ("A30_e2", 30, "e2")]
DIMS = ("depth", "rsi", "vol", "panic")
R5THR = -0.10
COST = ob.COST
HZ2COL = {3: "r3", 5: "r5h", 10: "r10", 20: "r20"}

SPECS = None
BRD = None


def _init(args):
    mkt, e1, e2, e3, pn, brd, specs = args
    pb._init(mkt, e1, e2, e3, pn)
    global SPECS, BRD
    SPECS, BRD = specs, brd


def _scan(path, need_close):
    d = ob._load(path)
    if d is None:
        return None
    ft = ob._features(d)
    n, idx, qo, qc = len(ft["idx"]), ft["idx"], ft["qo"], ft["qc"]
    vol, vol5 = ft["vol"], ft["vol5"]
    out, diag = [], {}
    for si, (name, rsi_thr, env) in enumerate(SPECS):
        cond = (ft["base"] & (ft["rsi"] < rsi_thr) & (ft["ret5"] < R5THR) &
                (ft["c"] > ft["o"]) & (ft["c"] > ft["pclose"]) &
                (ft["lo"] > ft["ldn"] - ob.EPS) & pb.env_ok(idx, env))
        ok = cond & ft["buyable"]
        diag[si] = dict(n_cond=int(cond.sum()), n_ok=int(ok.sum()),
                        n_lb=int((cond & ft["limit_block"]).sum()),
                        n_susp=int((cond & ft["suspended"]).sum()))
        for i in np.nonzero(ok)[0]:
            k = i + 1
            if k >= n or not np.isfinite(qo[k]) or qo[k] <= 0:
                continue
            j = k + 19
            if j >= n or not np.isfinite(qc[j]):
                continue
            rr = ft["rets"][i]
            if not np.all(np.isfinite(rr[[1, 3]])):
                continue
            vr = vol[i] / vol5[i] if np.isfinite(vol5[i]) and vol5[i] > 0 else np.nan
            out.append((si, ft["code"], int(idx[i]), int(idx[k]), int(idx[j]), float(qo[k]),
                        float(ft["ret5"][i]), float(ft["rsi"][i]), float(vr),
                        float(BRD[int(idx[i])]) if BRD is not None else np.nan,
                        float(rr[0]), float(rr[1]), float(rr[2]), float(rr[3])))
    if not out or not need_close:
        return (out, ft["code"], None, None, diag) if out else None
    d0 = min(x[3] for x in out) - 5
    d1 = max(x[4] for x in out) + 5
    sel = (idx >= d0) & (idx <= d1)
    return out, ft["code"], idx[sel].astype(np.int32), qc[sel].astype(np.float32), diag


def _scan_no_close(path):
    return _scan(path, False)


def _scan_close(path):
    return _scan(path, True)


def _market():
    mkt = pd.read_parquet(os.path.join(ob.OUT, "market.parquet"))
    ords = mkt["ordinal"].to_numpy()
    iser = pd.Series(mkt["index"].to_numpy(), index=ords)
    mkt_dict = {"ret": pd.Series(mkt["ew_ret"].to_numpy(), index=ords),
                "ret20": iser.pct_change(20), "ret3": iser.pct_change(3)}
    e = {}
    for k in ("e1", "e2", "e3"):
        a = np.zeros(ob.NB, bool)
        a[ords] = mkt[k].to_numpy(bool)
        e[k] = a
    brd = np.full(ob.NB, np.nan)
    brd[ords] = mkt["bias_ratio"].to_numpy(float)
    pn = np.zeros(ob.NB, bool)
    pn[ords] = mkt["panic"].to_numpy(bool)
    return mkt, ords, mkt_dict, e, pn, brd


def _bench(ords, ew_ret, h):
    """ordinal → 未来 h 日全 A 等权收益（T+1..T+h 收盘对收盘）。"""
    pos = {int(o): i for i, o in enumerate(ords)}
    ls = np.concatenate([[0.0], np.cumsum(np.log1p(np.nan_to_num(ew_ret, nan=0.0)))])
    out = np.full(ob.NB, np.nan)
    for o in ords:
        p = pos[int(o)]
        if p + h < len(ls):
            out[int(o)] = math.exp(ls[p + h] - ls[p]) - 1.0
    return out


def cluster_t(day, r):
    s = pd.Series(np.asarray(r, float)).groupby(pd.Series(np.asarray(day))).mean().dropna()
    if len(s) < 5 or s.std(ddof=1) == 0:
        return np.nan
    return float(s.mean() / (s.std(ddof=1) / math.sqrt(len(s))))


def pnorm(t):
    return math.erfc(abs(t) / math.sqrt(2)) if np.isfinite(t) else np.nan


# ------------------------------------------------------------------ 阶段 1 ----
def stage_signal(workers=8):
    mkt, ords, mkt_dict, e, pn, brd = _market()
    files = ob.stock_files()
    recs = []
    with ProcessPoolExecutor(max_workers=workers, initializer=_init,
                             initargs=((mkt_dict, e["e1"], e["e2"], e["e3"], pn, brd, RULE_SETS),)) as pool:
        for i, r in enumerate(pool.map(_scan_no_close, files, chunksize=16), 1):
            if r is None:
                continue
            recs.extend(r[0])
            if i % 1500 == 0:
                print(f"[scan {i}/{len(files)}]", flush=True)
    cols = ["si", "code", "day", "entry_day", "exit_day", "entry_px", "ret5", "rsi", "volr",
            "breadth", "r3", "r5h", "r10", "r20"]
    df = pd.DataFrame(recs, columns=cols)
    df["rule"] = [RULE_SETS[s][0] for s in df["si"]]
    df["depth"] = -df["ret5"]
    df["ver"] = -df["rsi"]
    df["vol"] = df["volr"]
    df["panic"] = df["breadth"]
    df["ym"] = [ob.YEAR[d] for d in df["day"]]
    df.to_parquet(os.path.join(OUT, "信号明细.parquet"), index=False)
    print(f"[signal] rows={len(df)} " + str(df["rule"].value_counts().to_dict()), flush=True)

    ew = mkt.set_index("ordinal")["ew_ret"]
    bench = {h: _bench(ords, ew.to_numpy(), h) for h in (3, 5, 10, 20)}
    rows, tests = [], []
    for rule in ("A28_none", "A28_e2", "A30_none", "A30_e2"):
        sub = df[df["rule"] == rule].copy()
        if not len(sub):
            continue
        for dim in DIMS:
            x = sub[dim]
            valid = x.notna()
            if valid.sum() < 500:
                continue
            sub2 = sub[valid].copy()
            q = pd.qcut(sub2[dim], 5, labels=False, duplicates="drop") + 1
            sub2["q"] = q.astype(int)
            for hz in (5, 20):
                rc, bc = HZ2COL[hz], bench[hz][sub2["entry_day"].to_numpy().astype(int)]
                ret = sub2[rc].to_numpy() - COST
                exc = ret - bc
                for g in (1, 2, 3, 4, 5):
                    m = (sub2["q"] == g).to_numpy()
                    if m.sum() == 0:
                        continue
                    r, xc, dd = ret[m], exc[m], sub2["day"].to_numpy()[m]
                    t_abs, t_exc = cluster_t(dd, r), cluster_t(dd, xc)
                    rows.append(dict(rule=rule, dim=dim, horizon=hz, quintile=g, n=int(m.sum()),
                                     abs_net=float(r.mean()), exc=float(xc.mean()),
                                     win=float((r > 0).mean()), t_abs=t_abs, t_exc=t_exc,
                                     p_exc=pnorm(t_exc)))
                    if hz == 5:
                        tests.append(len(rows) - 1)
    out = pd.DataFrame(rows)
    # BH-FDR（跨全部 规则×维度×分组，5 日超额）
    idx5 = [i for i in range(len(out)) if out.iloc[i]["horizon"] == 5]
    p = out.iloc[idx5]["p_exc"].to_numpy(float)
    order = np.argsort(p)
    qv = np.empty(len(p))
    for rank, j in enumerate(order, 1):
        qv[j] = min(1.0, p[j] * len(p) / rank)
    for k, i in enumerate(idx5):
        out.loc[out.index[i], "q_exc5"] = qv[k]
    out.to_csv(os.path.join(OUT, "分层.csv"), index=False, encoding="utf-8-sig")

    mono = []
    for (rule, dim), g in out[out["horizon"] == 5].groupby(["rule", "dim"]):
        g = g.sort_values("quintile")
        if len(g) < 5:
            continue
        for col in ("abs_net", "exc"):
            rho = pd.Series(g["quintile"].to_numpy()).corr(pd.Series(g[col].to_numpy()), method="spearman")
            adj = int((np.diff(g[col].to_numpy()) > 0).sum())
            mono.append(dict(rule=rule, dim=dim, metric=col, spearman=float(rho),
                             adj_up=adj, q5=float(g[col].iloc[-1]), q1=float(g[col].iloc[0]),
                             mean=float(g[col].mean()), q5_minus_mean=float(g[col].iloc[-1] - g[col].mean()),
                             q5_t=float(g["t_exc"].iloc[-1]) if col == "exc" else np.nan,
                             q5_n=int(g["n"].iloc[-1]),
                             pass_mono=bool(abs(rho) >= 0.9 and adj >= 3 and
                                            (g[col].iloc[-1] - g[col].iloc[0]) * rho > 0)))
    mdf = pd.DataFrame(mono)
    mdf.to_csv(os.path.join(OUT, "单调性.csv"), index=False, encoding="utf-8-sig")
    pd.set_option("display.width", 240)
    print(mdf[mdf["metric"] == "exc"].to_string(index=False), flush=True)
    print(out[out["horizon"] == 5][["rule", "dim", "quintile", "n", "abs_net", "exc", "t_exc", "q_exc5"]]
          .to_string(index=False), flush=True)
    return 0


# ------------------------------------------------------------------ 阶段 2 ----
def causal_rank(x):
    """扩展分位：只用此前信号（按时间顺序），至少 200 个历史才给值。"""
    v = np.asarray(x, float)
    out = np.full(len(v), np.nan)
    hist = []
    for i, a in enumerate(v):
        if not np.isfinite(a):
            continue
        if len(hist) >= 200:
            out[i] = bisect.bisect_right(hist, a) / len(hist)
        bisect.insort(hist, a)
    return out


def simulate(tr, C, dates, code_index, freq, maxpos=20, wcap=0.05, mul=None, only=None):
    di_of = {int(d): i for i, d in enumerate(dates)}
    entries = {}
    epx_a = tr["entry_px"].to_numpy(float)
    for n_i, (code, ed, xd, rg) in enumerate(zip(tr["code"].to_numpy(), tr["entry_day"].to_numpy(),
                                                 tr["exit_day"].to_numpy(), tr["ret"].to_numpy(float))):
        if only is not None and not only[n_i]:
            continue
        entries.setdefault(int(ed), []).append(
            (code, int(xd), rg, epx_a[n_i], 1.0 if mul is None else mul[n_i]))
    held, cash, equity_prev = {}, 1.0, 1.0
    eq, done = [], []
    invest = buy = 0.0
    for di, d in enumerate(dates):
        for code, xday, rg, epx, mlt in entries.get(int(d), ()):
            if (freq == "weekly" and ob.DOW[d] != 0) or code in held or len(held) >= maxpos:
                continue
            budget = min(cash, equity_prev * wcap * mlt)
            if budget <= 1e-9:
                continue
            sh = budget / epx
            cash -= budget
            buy += budget
            held[code] = (sh, epx, xday, rg, d)
        mv = 0.0
        for code, (sh, epx, xday, rg, ed) in held.items():
            ci = code_index.get(code)
            mv += (C[di, ci] * sh if ci is not None and np.isfinite(C[di, ci]) else epx * sh)
        equity = cash + mv
        invest += mv / equity if equity > 0 else 0.0
        for code in [c for c, v in held.items() if v[2] == d]:
            sh, epx, xday, rg, ed = held.pop(code)
            ci = code_index[code]
            px = C[di, ci] if np.isfinite(C[di, ci]) else epx * (1 + rg)
            cash += sh * px * (1 - COST)
            done.append({"code": code, "entry_day": ed, "exit_day": d,
                         "ret": px * (1 - COST) / epx - 1.0,
                         "pnl": sh * (px * (1 - COST) - epx),
                         "hold": di_of[d] - di_of[ed] + 1})
        eq.append((d, cash + sum(v[0] * (C[di, code_index[c]] if np.isfinite(C[di, code_index[c]]) else v[1])
                                 for c, v in held.items())))
        equity_prev = eq[-1][1]
    trd = pd.DataFrame(done)
    yrs = len(eq) / 252.0
    met = pb.metrics(pd.Series([x[1] for x in eq], index=[x[0] for x in eq]), trd,
                     invest / len(eq) if eq else np.nan, buy / yrs if yrs else np.nan)
    return met


def stage_portfolio(wdim="depth", workers=8):
    mkt, ords, mkt_dict, e, pn, brd = _market()
    one = [("A28_none", 28, "none")]
    files = ob.stock_files()
    trades, recs = [], []
    with ProcessPoolExecutor(max_workers=workers, initializer=_init,
                             initargs=((mkt_dict, e["e1"], e["e2"], e["e3"], pn, brd, one),)) as pool:
        for i, r in enumerate(pool.map(_scan_close, files, chunksize=8), 1):
            if r is None:
                continue
            trades.extend(r[0])
            recs.append((r[1], r[2], r[3]))
            if i % 1500 == 0:
                print(f"[scan {i}/{len(files)}]", flush=True)
    cols = ["si", "code", "day", "entry_day", "exit_day", "entry_px", "ret5", "rsi", "volr",
            "breadth", "r3", "r5h", "r10", "r20"]
    df = pd.DataFrame(trades, columns=cols).sort_values("day", kind="stable").reset_index(drop=True)
    dim_map = {"depth": -df["ret5"].to_numpy(float), "rsi": -df["rsi"].to_numpy(float),
               "vol": df["volr"].to_numpy(float), "panic": df["breadth"].to_numpy(float)}
    rk = causal_rank(dim_map[wdim])
    df["rank_pct"] = rk
    df["mul"] = 0.5 + rk
    print(f"[weight] dim={wdim} ranked={int(np.isfinite(rk).sum())}/{len(df)} "
          f"mean_mul={np.nanmean(df['mul']):.3f}", flush=True)

    start_ord = int(ob.ords_of(pd.DatetimeIndex([ob.START]))[0])
    dates = np.sort(ords)
    dates = dates[dates >= start_ord]
    pos_of = {int(d): i for i, d in enumerate(dates)}
    code_index = {}
    for code, _, _ in recs:
        code_index.setdefault(code, len(code_index))
    C = np.full((len(dates), max(len(code_index), 1)), np.nan, dtype=np.float32)
    for code, o, cl in recs:
        rows = np.fromiter((pos_of.get(int(x), -1) for x in o), dtype=np.int32, count=len(o))
        ok = rows >= 0
        C[rows[ok], code_index[code]] = cl[ok]

    tr = pd.DataFrame({"code": df["code"], "entry_day": df["entry_day"],
                       "exit_day": df["exit_day"], "ret": df["r20"] - COST,
                       "entry_px": df["entry_px"]})
    top = (df["rank_pct"].to_numpy() >= 0.8)
    rows = []
    for freq in ("daily", "weekly"):
        for scheme in ("equal", "strength", "top20"):
            if scheme == "top20" and not top.any():
                continue
            met = simulate(tr, C, dates, code_index, freq, 20, 0.05,
                           mul=(df["mul"].to_numpy() if scheme == "strength" else None),
                           only=(top if scheme == "top20" else None))
            if not met:
                continue
            rows.append(dict(scheme=scheme, freq=freq, dim=wdim, cagr=met["cagr"], mdd=met["mdd"],
                             vol=met["vol"], sharpe=met["sharpe"], calmar=met["calmar"],
                             exposure=met["exposure"], turnover=met["turnover"],
                             n_trades=met["n_trades"], win=met["win"], pf=met["pf"]))
            print(f"[pf] {scheme} {freq} cagr={met['cagr']:.4f} mdd={met['mdd']:.4f} "
                  f"sharpe={met['sharpe']:.3f} exp={met['exposure']:.3f}", flush=True)
    pd.DataFrame(rows).to_csv(os.path.join(OUT, "加权组合层.csv"), index=False, encoding="utf-8-sig")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", choices=("signal", "portfolio"), required=True)
    ap.add_argument("--wdim", default="depth")
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args()
    sys.exit(stage_signal(a.workers) if a.stage == "signal" else stage_portfolio(a.wdim, a.workers))
