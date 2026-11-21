#!/usr/bin/env python3
"""
超跌止跌买点 —— 组合层模拟（依赖 oversold_rebound.py --stage signal 产的 market.parquet）。

真实账户约束：每日/每周可入场、单票等权（目标仓位 = 权益/20）、最多 20 只、
同一只股票重复信号只进一次、停牌与开盘涨停不可买、记换手与资金占用。
退出规则（事先固定、全部报出）：
  hold5 / hold10 / hold20：固定持有
  tp10sl5 / tp15sl5：收盘价止盈 +10%/+15%，止损 −5%，最长 20 日
  ma5 / ma10：收盘跌破 5/10 日均线退出（最长 60 日）
成本：往返 0.13%（在卖出时一次性扣）。

用法：
  python tools/oversold_rebound_portfolio.py --rule "A:30,-0.08,e1" --rule "B:none"
输出：work/超跌止跌买点_2026-11-21/组合层.csv
"""

from __future__ import annotations

import argparse
import glob
import math
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from datetime import date

import numpy as np
import pandas as pd
import polars as pl

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import oversold_rebound as ob  # noqa: E402

OUT = ob.OUT
COST = ob.COST
MAXPOS = ob.MAXPOS
HZ = ob.HORIZONS
EXITS = ("hold5", "hold10", "hold20", "tp10sl5", "tp15sl5", "ma5", "ma10")

MKT = None
ENVS = None
PANIC = None


def _init(mkt, e1, e2, e3, panic):
    global MKT, ENVS, PANIC
    ob._init_worker(mkt["ret"], mkt["ret20"], mkt["ret3"])
    ENVS = {"e1": e1, "e2": e2, "e3": e3}
    PANIC = panic


def env_ok(idx, env):
    if env == "none":
        return np.ones(len(idx), dtype=bool)
    m = np.ones(len(idx), dtype=bool)
    for k in ("e1", "e2", "e3"):
        if k in env:
            m &= ENVS[k][idx]
    return m


def rule_mask(ft, spec):
    fam, env = spec["fam"], spec["env"]
    m = ft["base"] & ft["buyable"]
    if fam == "A":
        m &= ((ft["rsi"] < spec["rsi"]) & (ft["ret5"] < spec["r5"]) &
              (ft["c"] > ft["o"]) & (ft["c"] > ft["pclose"]) & (ft["lo"] > ft["ldn"] - ob.EPS))
    elif fam == "B":
        m &= (((ft["bias12"] < -10) | (ft["rsi"] < 30)) &
              (ft["llv3"] >= ft["llv3_prev"] - ob.EPS) &
              (ft["c"] > ft["hhc3_prev"]) & (ft["vol"] > ft["vol5"]))
    elif fam == "C":
        m &= ((ft["rel20"] < -0.10) & (ft["rel3"] > 0) &
              (ft["c"] > ft["ma5"]) & (ft["rsi"] < 40))
    elif fam == "D":
        m &= ((ft["rsi"] < 30) & (ft["ret5"] < -0.08) & (ft["c"] > ft["o"]) &
              (ft["c"] > ft["pclose"]) & (ft["lo"] > ft["ldn"] - ob.EPS) &
              PANIC[ft["idx"]] & (ft["mret"] < -0.02))
    return m & env_ok(ft["idx"], env)


def _exit_plan(k, n, qc, ma5, ma10):
    out = {}
    for hz in (5, 10, 20):
        j = k + hz - 1
        while j < n and j <= k + hz + 4 and not np.isfinite(qc[j]):
            j += 1
        if j < n and np.isfinite(qc[j]):
            out[f"hold{hz}"] = (j, qc[j])
    for tp, sl, nm in ((0.10, 0.05, "tp10sl5"), (0.15, 0.05, "tp15sl5")):
        j = k + 1
        hit = None
        while j < n and j < k + 20:
            if np.isfinite(qc[j]):
                rr = qc[j] / qc[k] - 1.0
                if rr <= -sl:
                    hit = (j, qc[j])
                    break
                if rr >= tp:
                    hit = (j, qc[j])
                    break
            j += 1
        if hit:
            out[nm] = hit
    for w, nm in ((ma5, "ma5"), (ma10, "ma10")):
        j = k + 1
        hit = None
        while j < n and j < k + 60:
            if np.isfinite(qc[j]) and np.isfinite(w[j]) and qc[j] < w[j]:
                hit = (j, qc[j])
                break
            j += 1
        if hit:
            out[nm] = hit
    return out


def process(path, specs):
    d = ob._load(path)
    if d is None:
        return None
    ft = ob._features(d)
    n = len(ft["idx"])
    qc, qo = ft["qc"], ft["qo"]
    out = []
    for si, spec in enumerate(specs):
        m = rule_mask(ft, spec)
        ii = np.nonzero(m)[0]
        if len(ii) == 0:
            continue
        for i in ii:
            k = i + 1
            if k >= n or not np.isfinite(qo[k]) or qo[k] <= 0:
                continue
            entry = qo[k]
            plan = _exit_plan(k, n, qc, ft["ma5"], ft["ma10"])
            for nm, (j, epx) in plan.items():
                out.append((si, nm, ft["code"], int(ft["idx"][k]), int(ft["idx"][j]),
                            float(epx / entry - 1.0), int(j - k + 1), float(entry)))
    if not out:
        return None
    d0 = min(x[3] for x in out) - 5
    d1 = max(x[4] for x in out) + 5
    idx = ft["idx"]
    sel = (idx >= d0) & (idx <= d1)
    return out, ft["code"], idx[sel].astype(np.int32), qc[sel].astype(np.float32)


# ------------------------------------------------------------------ 指标 ----
def metrics(equity: pd.Series, trades: pd.DataFrame, exposure: float, turnover: float):
    e = equity.dropna()
    if len(e) < 30:
        return {}
    ret = e.pct_change().dropna()
    nd = len(e)
    cagr = float((e.iloc[-1] / e.iloc[0]) ** (252.0 / max(nd - 1, 1)) - 1)
    vol = float(ret.std() * math.sqrt(252))
    sharpe = float(ret.mean() / ret.std() * math.sqrt(252)) if ret.std() > 0 else np.nan
    mdd = float((e / e.cummax() - 1).min())
    calmar = cagr / abs(mdd) if mdd < 0 else np.nan
    if len(trades):
        w = trades["ret"] > 0
        win = float(w.mean())
        gains = trades.loc[w, "pnl"].sum()
        losses = -trades.loc[~w, "pnl"].sum()
        pf = float(gains / losses) if losses > 0 else np.nan
        srt = trades.sort_values("exit_day")
        mx, cur = 0, 0
        for x in (srt["ret"] > 0).to_numpy():
            cur = 0 if x else cur + 1
            mx = max(mx, cur)
        avg_hold = float(srt["hold"].mean())
    else:
        win = pf = np.nan
        mx = 0
        avg_hold = np.nan
    years = ob.YEAR[e.index.to_numpy()]
    ye = e.groupby(years).last()
    yr = ye.pct_change()
    if len(ye):
        yr.iloc[0] = ye.iloc[0] / 1.0 - 1
    return dict(cagr=cagr, mdd=mdd, vol=vol, sharpe=sharpe, calmar=calmar,
                win=win, pf=pf, max_consec_loss=mx, n_trades=len(trades),
                avg_hold=avg_hold, exposure=exposure, turnover=turnover,
                yearly={int(y): float(v) for y, v in yr.items() if np.isfinite(v)})


def simulate(trades: pd.DataFrame, C, dates, code_index, freq: str, maxpos=MAXPOS):
    """trades: code, entry_day, exit_day, ret, hold, entry_px。C: 日期×股票 收盘矩阵。"""
    di_of = {int(d): i for i, d in enumerate(dates)}
    entries = {}
    for r in trades.itertuples():
        entries.setdefault(int(r.entry_day), []).append((r.code, int(r.exit_day), r.ret, r.entry_px))
    held = {}
    cash, equity_prev = 1.0, 1.0
    eq = []
    done = []
    buy_notional = 0.0
    invest_sum = 0.0
    for di, d in enumerate(dates):
        if d in entries:
            for code, xday, ret_gross, entry_px in entries[d]:
                if code in held or len(held) >= maxpos:
                    continue
                if freq == "weekly" and ob.DOW[d] != 0:
                    continue
                budget = min(cash, equity_prev / maxpos)
                if budget <= 1e-9:
                    continue
                sh = budget / entry_px
                cash -= budget
                buy_notional += budget
                held[code] = (sh, entry_px, xday, ret_gross, d)
                if len(held) >= maxpos:
                    pass
        mv = 0.0
        for code, (sh, epx, xday, rg, ed) in held.items():
            ci = code_index.get(code)
            v = C[di, ci] * sh if ci is not None and np.isfinite(C[di, ci]) else epx * sh
            mv += v
        equity = cash + mv
        invest_sum += mv / equity if equity > 0 else 0.0
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
    tr = pd.DataFrame(done)
    shift_years = len(eq) / 252.0
    turnover = buy_notional / shift_years if shift_years > 0 else np.nan
    exposure = invest_sum / len(eq) if eq else np.nan
    eqs = pd.Series([x[1] for x in eq], index=[x[0] for x in eq])
    return metrics(eqs, tr, exposure, turnover), eqs


def run(specs, workers=8):
    mkt = pd.read_parquet(os.path.join(OUT, "market.parquet"))
    ords = mkt["ordinal"].to_numpy()
    def to_nb(col, dtype=np.float32):
        out = np.full(ob.NB, np.nan, dtype=dtype)
        out[ords] = mkt[col].to_numpy()
        return out
    idx_ser = pd.Series(mkt["index"].to_numpy(), index=ords)
    mkt_dict = {"ret": pd.Series(mkt["ew_ret"].to_numpy(), index=ords),
                "ret20": idx_ser.pct_change(20),
                "ret3": idx_ser.pct_change(3)}
    e1 = np.zeros(ob.NB, bool); e2 = np.zeros(ob.NB, bool); e3 = np.zeros(ob.NB, bool)
    pn = np.zeros(ob.NB, bool)
    e1[ords] = mkt["e1"].to_numpy(bool)
    e2[ords] = mkt["e2"].to_numpy(bool)
    e3[ords] = mkt["e3"].to_numpy(bool)
    pn[ords] = mkt["panic"].to_numpy(bool)

    files = ob.stock_files()
    trades, recs = [], []
    with ProcessPoolExecutor(max_workers=workers, initializer=_init,
                             initargs=(mkt_dict, e1, e2, e3, pn)) as ex:
        for i, r in enumerate(ex.map(process, files, [specs] * len(files), chunksize=8), 1):
            if r is None:
                continue
            trades.extend(r[0])
            recs.append((r[1], r[2], r[3]))
            if i % 1000 == 0:
                print(f"[portfolio {i}/{len(files)}]", flush=True)
    tr = pd.DataFrame(trades, columns=["si", "exit", "code", "entry_day", "exit_day",
                                      "ret", "hold", "entry_px"])
    start_ord = int(ob.ords_of(pd.DatetimeIndex([ob.START]))[0])
    dates = np.sort(ords)
    dates = dates[dates >= start_ord]
    pos_of = {int(d): i for i, d in enumerate(dates)}
    code_index = {}
    for code, _, _ in recs:
        if code not in code_index:
            code_index[code] = len(code_index)
    C = np.full((len(dates), max(len(code_index), 1)), np.nan, dtype=np.float32)
    for code, o, cl in recs:
        ci = code_index[code]
        rows = np.fromiter((pos_of.get(int(x), -1) for x in o), dtype=np.int32, count=len(o))
        ok = rows >= 0
        C[rows[ok], ci] = cl[ok]
    print(f"[data] trades={len(tr)} stocks={len(code_index)} days={len(dates)}", flush=True)

    rows = []
    bench_eq = (1 + mkt.set_index("ordinal").loc[lambda x: x.index >= start_ord, "ew_ret"]
                .fillna(0)).cumprod()
    bm = metrics(bench_eq, pd.DataFrame(), np.nan, np.nan)
    bench_row = dict(rule="BENCH_EW_ALL_A", exit="", freq="", cagr=bm.get("cagr"),
                     mdd=bm.get("mdd"), vol=bm.get("vol"), sharpe=bm.get("sharpe"),
                     calmar=bm.get("calmar"), win=np.nan, pf=np.nan, max_consec_loss=np.nan,
                     n_trades=0, avg_hold=np.nan, exposure=np.nan, turnover=np.nan)
    bench_row.update({f"y{y}": v for y, v in bm.get("yearly", {}).items()})
    rows.append(bench_row)

    for si, spec in enumerate(specs):
        sub = tr[tr["si"] == si]
        if not len(sub):
            continue
        for nm in EXITS:
            s = sub[sub["exit"] == nm]
            if not len(s):
                continue
            for freq in ("daily", "weekly"):
                met, eqs = simulate(s, C, dates, code_index, freq)
                if not met:
                    continue
                row = dict(rule=spec["name"], exit=nm, freq=freq,
                           cagr=met["cagr"], mdd=met["mdd"], vol=met["vol"],
                           sharpe=met["sharpe"], calmar=met["calmar"], win=met["win"],
                           pf=met["pf"], max_consec_loss=met["max_consec_loss"],
                           n_trades=met["n_trades"], avg_hold=met["avg_hold"],
                           exposure=met["exposure"], turnover=met["turnover"])
                row.update({f"y{y}": v for y, v in met["yearly"].items()})
                rows.append(row)
    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(OUT, "组合层.csv"), index=False, encoding="utf-8-sig")
    pd.set_option("display.width", 240)
    print(df[["rule", "exit", "freq", "cagr", "mdd", "sharpe", "win", "pf",
              "n_trades", "exposure"]].to_string(index=False), flush=True)
    return 0


def parse_rule(s):
    fam, rest = s.split(":", 1)
    fam = fam.strip().upper()
    if fam == "A":
        p = [x.strip() for x in rest.split(",")]
        rsi, r5, env = int(p[0]), float(p[1]), p[2]
        return dict(fam="A", rsi=rsi, r5=r5, env=env, name=f"A_rsi{rsi}_r5{int(r5*100)}_{env}")
    return dict(fam=fam, env=rest.strip(), name=f"{fam}_{rest.strip()}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rule", action="append", required=True)
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args()
    specs = [parse_rule(x) for x in a.rule]
    sys.exit(run(specs, a.workers))
