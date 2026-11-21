#!/usr/bin/env python3
"""
状态机分批建仓 vs 一次性买入（规则 A：RSI28 / 5 日跌幅 −10% / 无环境过滤，周频入场）。

状态机：首次触发买 1/3；此后第 5、10 个交易日，若仍超跌（RSI<30 或 5 日跌幅<−10%）
且当日收盘未创持仓期新低，则各加 1/3；单票满仓 = 权益 5%（每批 1.67%）。
退出四组：hold20 / hold20+止盈10% / hold20+止损−8% / 收盘涨回 MA20 上方（最长 60 日）。
成本：每批各计 0.13% 往返（买入 0.065%、卖出 0.065%）。

对照：一次性买入（同信号集）、同平均仓位的恒定仓位对照（按仓位比例缩放单票预算）、
三段样本外（2015–19 / 2020–23 / 2024–26）。

产物：work/超跌止跌买点_2026-11-21/分批建仓/{状态机.csv, 分段.csv, 结论.md}
用法：python tools/oversold_rebound_staged.py --workers 8
"""

from __future__ import annotations

import argparse
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

OUT = os.path.join(ob.OUT, "分批建仓")
os.makedirs(OUT, exist_ok=True)

SPECS = [("A28_none", 28, -0.10, "none")]
EXITS = ("hold20", "tp10", "sl8", "ma20")
COST = ob.COST
HALF = COST / 2.0
SEGMENTS = (("2015-19", "2015-01-01", "2019-12-31"),
            ("2020-23", "2020-01-01", "2023-12-31"),
            ("2024-26", "2024-01-01", "2026-12-31"))


# --------------------------------------------------------------- 单票计划 ----
def _exit_hold20(k, n, qc):
    e = k + 19
    while e < n and e <= k + 23 and not np.isfinite(qc[e]):
        e += 1
    return e if e < n and np.isfinite(qc[e]) else None


def _plan_exit(rule, k, n, qc, ma20, buys):
    """buys: [(row, px)]；返回 (exit_row, exit_px) 或 None。"""
    cost_px = lambda upto: (len([b for b in buys if b[0] <= upto]) /
                            sum(1.0 / b[1] for b in buys if b[0] <= upto))
    if rule in ("hold20", "tp10", "sl8"):
        e = _exit_hold20(k, n, qc)
        if e is None:
            return None
        if rule != "hold20":
            lim = 0.10 if rule == "tp10" else -0.08
            for j in range(k + 1, e + 1):
                if not np.isfinite(qc[j]):
                    continue
                cpx = cost_px(j)
                if (qc[j] / cpx - 1.0) >= lim if rule == "tp10" else (qc[j] / cpx - 1.0) <= lim:
                    return j, float(qc[j])
        return e, float(qc[e])
    last = min(k + 59, n - 1)
    for j in range(k + 1, last + 1):
        if np.isfinite(qc[j]) and np.isfinite(ma20[j]) and qc[j] > ma20[j]:
            return j, float(qc[j])
    j = last
    while j > k and not np.isfinite(qc[j]):
        j -= 1
    return (j, float(qc[j])) if j > k and np.isfinite(qc[j]) else None


def stock_plans(path, specs):
    d = ob._load(path)
    if d is None:
        return None
    ft = ob._features(d)
    n, idx, qo, qc = len(ft["idx"]), ft["idx"], ft["qo"], ft["qc"]
    ma20 = pd.Series(qc).rolling(20).mean().to_numpy(float)
    vol, lup, rsi, ret5 = ft["vol"], ft["lup"], ft["rsi"], ft["ret5"]
    out = []
    for si, (name, rsi_thr, r5_thr, env) in enumerate(specs):
        cond = (ft["base"] & (rsi < rsi_thr) & (ret5 < r5_thr) &
                (ft["c"] > ft["o"]) & (ft["c"] > ft["pclose"]) &
                (ft["lo"] > ft["ldn"] - ob.EPS) & pb.env_ok(idx, env))
        sig = cond & ft["buyable"]
        for t in np.nonzero(sig)[0]:
            k = t + 1
            if k >= n or not np.isfinite(qo[k]) or qo[k] <= 0:
                continue
            buys = [(k, float(qo[k]))]
            for j in (1, 2):
                m = k + 5 * j
                if m >= n:
                    break
                ok_zone = (rsi[m - 1] < 30) or (ret5[m - 1] < -0.10)
                low_prev = np.nanmin(qc[k - 1:m - 1]) if m - 1 > k - 1 else qc[k - 1]
                if not np.isfinite(low_prev):
                    low_prev = qc[k - 1]
                no_new_low = np.isfinite(qc[m - 1]) and qc[m - 1] > low_prev
                can_buy = (np.isfinite(qo[m]) and vol[m] > 0 and
                           qo[m] < lup[m] - ob.EPS)
                if not (ok_zone and no_new_low and can_buy):
                    break
                buys.append((m, float(qo[m])))
            for rule in EXITS:
                ex_ = _plan_exit(rule, k, n, qc, ma20, buys)
                if ex_ is None:
                    continue
                e, epx = ex_
                out.append((si, "staged", rule, ft["code"], int(idx[k]), int(idx[e]),
                            [int(idx[b[0]]) for b in buys], [b[1] for b in buys], epx))
                out.append((si, "once", rule, ft["code"], int(idx[k]), int(idx[e]),
                            [int(idx[k])], [float(qo[k])], epx))
    if not out:
        return None
    d0 = min(x[4] for x in out) - 5
    d1 = max(x[5] for x in out) + 5
    sel = (idx >= d0) & (idx <= d1)
    return out, ft["code"], idx[sel].astype(np.int32), qc[sel].astype(np.float32)


# ------------------------------------------------------------------ 组合 ----
def simulate(plans, C, dates, code_index, freq, frac, maxpos=20):
    """plans: list of (code, entry_ord, exit_ord, buy_ords, buy_pxs, exit_px)。frac=单批预算/权益。"""
    by_entry, by_add = {}, {}
    for code, e0, e1, bords, bpxs, epx in plans:
        by_entry.setdefault(int(e0), []).append((code, int(e1), bords, bpxs, epx))
        for b in range(1, len(bords)):
            by_add.setdefault(int(bords[b]), []).append((code, int(e0), b, bpxs[b]))
    di_of = {int(d): i for i, d in enumerate(dates)}
    held, cash, equity_prev = {}, 1.0, 1.0
    eq, done = [], []
    invest = buys = 0.0
    n_first = n_third = n_second = 0
    for di, d in enumerate(dates):
        for code, xday, bords, bpxs, epx in by_entry.get(int(d), ()):
            if (freq == "weekly" and ob.DOW[d] != 0) or code in held or len(held) >= maxpos:
                continue
            budget = min(cash, equity_prev * frac)
            if budget <= 1e-9:
                continue
            cash -= budget * (1 + HALF)
            buys += budget
            held[code] = dict(sh=[budget / bpxs[0]], px=bpxs, exit=xday, ed=d, entry_px=bpxs[0])
            n_first += 1
        for code, e0, bi, apx in by_add.get(int(d), ()):
            h = held.get(code)
            if h is None or h["ed"] != e0 or bi >= len(h["px"]) or h["exit"] <= d:
                continue
            budget = min(cash, equity_prev * frac)
            if budget <= 1e-9:
                continue
            cash -= budget * (1 + HALF)
            buys += budget
            h["sh"].append(budget / apx)
            if bi == 1:
                n_second += 1
            if bi == 2:
                n_third += 1
        mv = 0.0
        for code, h in held.items():
            ci = code_index.get(code)
            px = C[di, ci] if ci is not None and np.isfinite(C[di, ci]) else h["entry_px"]
            mv += sum(h["sh"]) * px
        equity = cash + mv
        invest += mv / equity if equity > 0 else 0.0
        for code in [c for c, h in held.items() if h["exit"] == d]:
            h = held.pop(code)
            ci = code_index[code]
            epx = C[di, ci] if np.isfinite(C[di, ci]) else h["px"][-1]
            proceeds = sum(h["sh"]) * epx * (1 - HALF)
            cost = sum(s * p for s, p in zip(h["sh"], h["px"])) * (1 + HALF)
            cash += proceeds
            done.append({"code": code, "entry_day": h["ed"], "exit_day": d,
                         "ret": proceeds / cost - 1.0, "pnl": proceeds - cost,
                         "hold": di_of[d] - di_of[h["ed"]] + 1,
                         "ntr": len(h["sh"])})
        eq.append((d, cash + sum(sum(h["sh"]) * (C[di, code_index[c]] if np.isfinite(C[di, code_index[c]])
                                                 else h["entry_px"]) for c, h in held.items())))
        equity_prev = eq[-1][1]
    trd = pd.DataFrame(done)
    yrs = len(eq) / 252.0
    es = pd.Series([x[1] for x in eq], index=[x[0] for x in eq])
    met = pb.metrics(es, trd, invest / len(eq) if eq else np.nan, buys / yrs if yrs else np.nan)
    if met:
        met.update(n_first=n_first, n_second=n_second, n_third=n_third,
                   third_rate=(n_third / n_first) if n_first else np.nan,
                   avg_tr=float(trd["ntr"].mean()) if len(trd) else np.nan,
                   n_pos=len(trd))
    return met, es


def seg_ann(es, a, b):
    s = es[(es.index >= ob.ords_of(pd.DatetimeIndex([pd.Timestamp(a)]))[0]) &
           (es.index <= ob.ords_of(pd.DatetimeIndex([pd.Timestamp(b)]))[0])]
    if len(s) < 60:
        return np.nan
    return float((s.iloc[-1] / s.iloc[0]) ** (252.0 / (len(s) - 1)) - 1)


def main(workers=8):
    mkt = pd.read_parquet(os.path.join(ob.OUT, "market.parquet"))
    ords = mkt["ordinal"].to_numpy()
    iser = pd.Series(mkt["index"].to_numpy(), index=ords)
    mkt_dict = {"ret": pd.Series(mkt["ew_ret"].to_numpy(), index=ords),
                "ret20": iser.pct_change(20), "ret3": iser.pct_change(3)}
    e1 = np.zeros(ob.NB, bool); e2 = np.zeros(ob.NB, bool); e3 = np.zeros(ob.NB, bool)
    pn = np.zeros(ob.NB, bool)
    e1[ords] = mkt["e1"].to_numpy(bool); e2[ords] = mkt["e2"].to_numpy(bool)
    e3[ords] = mkt["e3"].to_numpy(bool); pn[ords] = mkt["panic"].to_numpy(bool)

    files = ob.stock_files()
    plans, recs = [], []
    with ProcessPoolExecutor(max_workers=workers, initializer=pb._init,
                             initargs=(mkt_dict, e1, e2, e3, pn)) as pool:
        for i, r in enumerate(pool.map(stock_plans, files, [SPECS] * len(files), chunksize=8), 1):
            if r is None:
                continue
            plans.extend(r[0])
            recs.append((r[1], r[2], r[3]))
            if i % 1500 == 0:
                print(f"[scan {i}/{len(files)}]", flush=True)
    cols = ["si", "mode", "exit", "code", "entry", "exitd", "bords", "bpxs", "epx"]
    pl = pd.DataFrame(plans, columns=cols)
    print(f"[plan] rows={len(pl)} " + str(pl.groupby(["mode", "exit"]).size().to_dict()), flush=True)

    start = int(ob.ords_of(pd.DatetimeIndex([ob.START]))[0])
    dates = np.sort(ords); dates = dates[dates >= start]
    pos_of = {int(d): i for i, d in enumerate(dates)}
    code_index = {}
    for code, _, _ in recs:
        code_index.setdefault(code, len(code_index))
    C = np.full((len(dates), max(len(code_index), 1)), np.nan, dtype=np.float32)
    for code, o, cl in recs:
        rows = np.fromiter((pos_of.get(int(x), -1) for x in o), dtype=np.int32, count=len(o))
        ok = rows >= 0
        C[rows[ok], code_index[code]] = cl[ok]

    rows, seg_rows = [], []
    for si, (name, *_r) in enumerate(SPECS):
        for rule in EXITS:
            for mode in ("staged", "once"):
                sel = pl[(pl["si"] == si) & (pl["mode"] == mode) & (pl["exit"] == rule)]
                if not len(sel):
                    continue
                plans_list = [(r.code, r.entry, r.exitd, r.bords, r.bpxs, r.epx) for r in sel.itertuples()]
                base_frac = 0.05 / 3.0 if mode == "staged" else 0.05
                met, es = simulate(plans_list, C, dates, code_index, "weekly", base_frac)
                if not met:
                    continue
                rows.append(dict(spec=name, mode=mode, exit=rule, scale=1.0, **{k: met[k] for k in
                            ("cagr", "mdd", "vol", "sharpe", "calmar", "exposure", "turnover",
                             "win", "pf", "n_first", "n_second", "n_third", "third_rate",
                             "avg_tr", "n_pos")}))
                print(f"[sim] {name} {rule} {mode} cagr={met['cagr']:.4f} mdd={met['mdd']:.4f} "
                      f"sharpe={met['sharpe']:.3f} exp={met['exposure']:.3f} "
                      f"3rd={met['third_rate']:.3f}", flush=True)
                if mode == "staged":
                    for lab, a, b in SEGMENTS:
                        seg_rows.append(dict(spec=name, exit=rule, mode=mode, segment=lab,
                                             ann=seg_ann(es, a, b), exposure=met["exposure"]))
                else:
                    rows[-1]["matched_exp"] = np.nan
                    # 同仓位对照：线性插值缩放单票预算
                    m6, es6 = simulate(plans_list, C, dates, code_index, "weekly", base_frac * 0.6)
                    if m6 and m6["exposure"] > 0:
                        e_target = [r["exposure"] for r in rows if r["spec"] == name and
                                    r["exit"] == rule and r["mode"] == "staged"]
                        if e_target:
                            tgt = e_target[0]
                            scale = (tgt - 0.0) / (m6["exposure"] / 0.6)
                            scale = max(0.25, min(3.0, scale))
                            mm, em = simulate(plans_list, C, dates, code_index, "weekly",
                                              base_frac * scale)
                            if mm:
                                rows.append(dict(spec=name, mode="once_matched", exit=rule,
                                                 scale=scale, **{k: mm[k] for k in
                                                 ("cagr", "mdd", "vol", "sharpe", "calmar", "exposure",
                                                  "turnover", "win", "pf", "n_first", "n_second",
                                                  "n_third", "third_rate", "avg_tr", "n_pos")}))
                                for lab, a, b in SEGMENTS:
                                    seg_rows.append(dict(spec=name, exit=rule, mode="once_matched",
                                                         segment=lab, ann=seg_ann(em, a, b),
                                                         exposure=mm["exposure"]))
                                print(f"[sim] {name} {rule} matched scale={scale:.2f} "
                                      f"cagr={mm['cagr']:.4f} exp={mm['exposure']:.3f}", flush=True)
                    for lab, a, b in SEGMENTS:
                        seg_rows.append(dict(spec=name, exit=rule, mode="once", segment=lab,
                                             ann=seg_ann(es, a, b), exposure=met["exposure"]))
    rdf = pd.DataFrame(rows)
    rdf.to_csv(os.path.join(OUT, "状态机.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(seg_rows).to_csv(os.path.join(OUT, "分段.csv"), index=False, encoding="utf-8-sig")
    pd.set_option("display.width", 260)
    print(rdf[["exit", "mode", "cagr", "mdd", "sharpe", "exposure", "turnover", "n_pos",
               "third_rate", "avg_tr"]].to_string(index=False), flush=True)
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=8)
    ma = ap.parse_args()
    sys.exit(main(ma.workers))
