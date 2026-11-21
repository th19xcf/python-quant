#!/usr/bin/env python3
"""
组合层仓位归因：拆解 A_rsi28_r5-10 在 hold20 下的平均仓位（约 28.4%）。

产出（work/超跌止跌买点_2026-11-21/仓位归因/）：
  仓位日序列.csv  每日仓位比例与持仓只数（无环境过滤 / E2 环境过滤 × 日频/周频）
  仓位区间.csv    长期低于 10%、高于 60% 的连续时间段
  月度信号开仓.csv 每月信号数、实际新开仓、各类未开仓数
  拒绝原因.csv    各约束占比（上游涨停/停牌 + 组合层上限/资金 + 退出端跌停）
  敏感性.csv      持仓上限与单票权重上限各放宽一档后的仓位/年化/回撤

用法：python tools/oversold_rebound_exposure.py [--workers 8]
"""

from __future__ import annotations

import os
import sys
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import oversold_rebound as ob  # noqa: E402
import oversold_rebound_portfolio as pb  # noqa: E402

OUT = os.path.join(ob.OUT, "仓位归因")
os.makedirs(OUT, exist_ok=True)

SPECS = [
    dict(fam="A", rsi=28, r5=-0.10, env="none", name="A_rsi28_r5-10_none"),
    dict(fam="A", rsi=28, r5=-0.10, env="e2", name="A_rsi28_r5-10_e2"),
]
EXIT = "hold20"
HZ = 20


# ------------------------------------------------------------------ 扫描 ----
def process_diag(path, specs):
    d = ob._load(path)
    if d is None:
        return None
    ft = ob._features(d)
    n = len(ft["idx"])
    qc, qo, idx = ft["qc"], ft["qo"], ft["idx"]
    q_ldn = ft["ldn"] * ft["hf"]
    out, diag = [], {}
    for si, spec in enumerate(specs):
        cond = (ft["base"] & (ft["rsi"] < spec["rsi"]) & (ft["ret5"] < spec["r5"]) &
                (ft["c"] > ft["o"]) & (ft["c"] > ft["pclose"]) &
                (ft["lo"] > ft["ldn"] - ob.EPS) &
                pb.env_ok(idx, spec["env"]))
        diag[si] = dict(n_cond=int(cond.sum()),
                        n_susp=int((cond & ft["suspended"]).sum()),
                        n_lb=int((cond & ft["limit_block"]).sum()),
                        n_ok=int((cond & ft["buyable"]).sum()))
        for i in np.nonzero(cond & ft["buyable"])[0]:
            k = i + 1
            if k >= n or not np.isfinite(qo[k]) or qo[k] <= 0:
                continue
            j = k + HZ - 1
            while j < n and j <= k + HZ + 4 and not np.isfinite(qc[j]):
                j += 1
            if j >= n or not np.isfinite(qc[j]):
                continue
            ldq = bool(np.isfinite(q_ldn[j]) and qc[j] <= q_ldn[j] + 1e-6)
            out.append((si, ft["code"], int(idx[k]), int(idx[j]),
                        float(qc[j] / qo[k] - 1.0), float(qo[k]), int(j - k + 1), int(ldq)))
    if not out:
        return None
    d0 = min(x[2] for x in out) - 5
    d1 = max(x[3] for x in out) + 5
    sel = (idx >= d0) & (idx <= d1)
    return out, ft["code"], idx[sel].astype(np.int32), qc[sel].astype(np.float32), diag


# -------------------------------------------------------------- 组合模拟 ----
def simulate(tr, C, dates, code_index, freq, maxpos, wcap):
    """与 pb.simulate 同口径；额外记录每日信号/开仓/拒绝与仓位序列。"""
    di_of = {int(d): i for i, d in enumerate(dates)}
    entries = {}
    for code, ed, xd, rg, epx in zip(tr["code"].to_numpy(), tr["entry_day"].to_numpy(),
                                     tr["exit_day"].to_numpy(), tr["ret"].to_numpy(float),
                                     tr["entry_px"].to_numpy(float)):
        entries.setdefault(int(ed), []).append((code, int(xd), rg, epx))
    held, cash, equity_prev = {}, 1.0, 1.0
    eq, done, ts = [], [], []
    buy_notional = invest_sum = 0.0
    reasons = dict(sig=0, ent=0, weekly=0, held=0, maxpos=0, cash=0, wcap=0, cashcap=0)
    for di, d in enumerate(dates):
        st = dict(sig=0, ent=0, weekly=0, held=0, maxpos=0, cash=0)
        lst = entries.get(int(d))
        if lst:
            st["sig"] = len(lst)
            for code, xday, rg, epx in lst:
                if freq == "weekly" and ob.DOW[d] != 0:
                    st["weekly"] += 1
                    continue
                if code in held:
                    st["held"] += 1
                    continue
                if len(held) >= maxpos:
                    st["maxpos"] += 1
                    continue
                budget = min(cash, equity_prev * wcap)
                if budget <= 1e-9:
                    st["cash"] += 1
                    continue
                if budget < equity_prev * wcap * (1 - 1e-9):
                    reasons["cashcap"] += 1
                else:
                    reasons["wcap"] += 1
                sh = budget / epx
                cash -= budget
                buy_notional += budget
                held[code] = (sh, epx, xday, rg, d)
                st["ent"] += 1
        for k in ("sig", "ent", "weekly", "held", "maxpos", "cash"):
            reasons[k] += st[k]
        mv = 0.0
        for code, (sh, epx, xday, rg, ed) in held.items():
            ci = code_index.get(code)
            mv += (C[di, ci] * sh if ci is not None and np.isfinite(C[di, ci]) else epx * sh)
        equity = cash + mv
        invest_sum += mv / equity if equity > 0 else 0.0
        ts.append((int(d), mv / equity if equity > 0 else 0.0, len(held), st["sig"], st["ent"]))
        for code in [c for c, v in held.items() if v[2] == d]:
            sh, epx, xday, rg, ed = held.pop(code)
            ci = code_index[code]
            px = C[di, ci] if np.isfinite(C[di, ci]) else epx * (1 + rg)
            cash += sh * px * (1 - ob.COST)
            done.append({"code": code, "entry_day": ed, "exit_day": d,
                         "ret": px * (1 - ob.COST) / epx - 1.0,
                         "pnl": sh * (px * (1 - ob.COST) - epx),
                         "hold": di_of[d] - di_of[ed] + 1})
        eq.append((d, cash + sum(v[0] * (C[di, code_index[c]] if np.isfinite(C[di, code_index[c]]) else v[1])
                                 for c, v in held.items())))
        equity_prev = eq[-1][1]
    trd = pd.DataFrame(done)
    years = len(eq) / 252.0
    exposure = invest_sum / len(eq) if eq else np.nan
    met = pb.metrics(pd.Series([x[1] for x in eq], index=[x[0] for x in eq]), trd,
                     exposure, buy_notional / years if years > 0 else np.nan)
    tsdf = pd.DataFrame(ts, columns=["ordinal", "exposure", "npos", "sig", "ent"])
    tsdf["date"] = pd.to_datetime(tsdf["ordinal"].map(
        lambda o: (pd.Timestamp("1990-01-01") + pd.Timedelta(days=int(o)))))
    return met, tsdf, reasons


def segments(tsdf, band, thr, min_len=20):
    flag = (tsdf["exposure"] < thr).to_numpy() if band == "low" else (tsdf["exposure"] > thr).to_numpy()
    out, start, run = [], None, 0
    for i, f in enumerate(flag):
        if f:
            if run == 0:
                start = i
            run += 1
        else:
            if run >= min_len:
                out.append((tsdf["date"].iloc[start], tsdf["date"].iloc[i - 1], run))
            run = 0
    if run >= min_len:
        out.append((tsdf["date"].iloc[start], tsdf["date"].iloc[len(flag) - 1], run))
    return out


def main(workers=8):
    mkt = pd.read_parquet(os.path.join(ob.OUT, "market.parquet"))
    ords = mkt["ordinal"].to_numpy()
    idx_ser = pd.Series(mkt["index"].to_numpy(), index=ords)
    mkt_dict = {"ret": pd.Series(mkt["ew_ret"].to_numpy(), index=ords),
                "ret20": idx_ser.pct_change(20), "ret3": idx_ser.pct_change(3)}
    e1 = np.zeros(ob.NB, bool); e2 = np.zeros(ob.NB, bool); e3 = np.zeros(ob.NB, bool)
    pn = np.zeros(ob.NB, bool)
    e1[ords] = mkt["e1"].to_numpy(bool)
    e2[ords] = mkt["e2"].to_numpy(bool)
    e3[ords] = mkt["e3"].to_numpy(bool)
    pn[ords] = mkt["panic"].to_numpy(bool)

    files = ob.stock_files()
    trades, recs = [], []
    agg = {si: dict(n_cond=0, n_susp=0, n_lb=0, n_ok=0) for si in range(len(SPECS))}
    with ProcessPoolExecutor(max_workers=workers, initializer=pb._init,
                             initargs=(mkt_dict, e1, e2, e3, pn)) as ex:
        for i, r in enumerate(ex.map(process_diag, files, [SPECS] * len(files), chunksize=8), 1):
            if r is None:
                continue
            trades.extend(r[0])
            recs.append((r[1], r[2], r[3]))
            for si, v in r[4].items():
                for k in agg[si]:
                    agg[si][k] += v[k]
            if i % 1000 == 0:
                print(f"[scan {i}/{len(files)}]", flush=True)
    tr = pd.DataFrame(trades, columns=["si", "code", "entry_day", "exit_day",
                                      "ret", "entry_px", "hold", "exit_ld"])
    print(f"[data] trades={len(tr)} stocks={len(recs)}", flush=True)

    start_ord = int(ob.ords_of(pd.DatetimeIndex([ob.START]))[0])
    dates = np.sort(ords)
    dates = dates[dates >= start_ord]
    pos_of = {int(d): i for i, d in enumerate(dates)}
    code_index = {}
    for code, _, _ in recs:
        code_index.setdefault(code, len(code_index))
    C = np.full((len(dates), max(len(code_index), 1)), np.nan, dtype=np.float32)
    for code, o, cl in recs:
        ci = code_index[code]
        rows = np.fromiter((pos_of.get(int(x), -1) for x in o), dtype=np.int32, count=len(o))
        ok = rows >= 0
        C[rows[ok], ci] = cl[ok]

    ts_all, mon_all, sen_rows, reason_rows, seg_rows = [], [], [], [], []
    variants = [("base_20_5%", 20, 0.05), ("pos30_3.3%", 30, 1 / 30),
                ("pos20_10%", 20, 0.10), ("pos30_10%", 30, 0.10)]
    for si, spec in enumerate(SPECS):
        sub = tr[tr["si"] == si]
        if not len(sub):
            continue
        for freq in ("daily", "weekly"):
            for vname, mp, wc in variants:
                met, tsdf, reasons = simulate(sub, C, dates, code_index, freq, mp, wc)
                if not met:
                    continue
                sen_rows.append(dict(rule=spec["name"], freq=freq, variant=vname,
                                     maxpos=mp, wcap=wc, cagr=met["cagr"], mdd=met["mdd"],
                                     vol=met["vol"], sharpe=met["sharpe"], calmar=met["calmar"],
                                     exposure=met["exposure"], turnover=met["turnover"],
                                     n_trades=met["n_trades"], win=met["win"], pf=met["pf"]))
                if vname != "base_20_5%":
                    continue
                t = tsdf.copy()
                t["rule"], t["freq"] = spec["name"], freq
                ts_all.append(t)
                for band, thr in (("low", 0.10), ("high", 0.60)):
                    for a, b, ln in segments(tsdf, band, thr, 20):
                        seg_rows.append(dict(rule=spec["name"], freq=freq, band=band,
                                             start=a.date(), end=b.date(), days=ln))
                m = tsdf.set_index("date")[["sig", "ent"]]
                mth = m.groupby([m.index.year, m.index.month]).sum()
                mth.index.names = ["year", "month"]
                mth = mth.reset_index()
                mth["rejected"] = mth["sig"] - mth["ent"]
                mth["rule"], mth["freq"] = spec["name"], freq
                mon_all.append(mth)
                n_cond, n_ok = agg[si]["n_cond"], agg[si]["n_ok"]
                tot = reasons["sig"]
                base = dict(rule=spec["name"], freq=freq, n_signal_cond=n_cond)
                for k, lab in (("weekly", "非入场日(周频)"), ("held", "已持仓(重复信号)"),
                               ("maxpos", "持仓只数上限"), ("cash", "资金不足")):
                    reason_rows.append(dict(**base, reason=lab, count=reasons[k],
                                            share_of_presented=reasons[k] / tot if tot else np.nan))
                reason_rows.append(dict(**base, reason="涨停/开盘涨停买不进(信号层剔除)",
                                        count=agg[si]["n_lb"],
                                        share_of_presented=agg[si]["n_lb"] / n_cond if n_cond else np.nan))
                reason_rows.append(dict(**base, reason="停牌不可买(信号层剔除)",
                                        count=agg[si]["n_susp"],
                                        share_of_presented=agg[si]["n_susp"] / n_cond if n_cond else np.nan))
                reason_rows.append(dict(**base, reason="退出日收盘跌停(可能卖不出)",
                                        count=int(sub["exit_ld"].sum()),
                                        share_of_presented=float(sub["exit_ld"].mean())))
                print(f"[base] {spec['name']} {freq} exp={met['exposure']:.4f} "
                      f"cagr={met['cagr']:.4f} mdd={met['mdd']:.4f} n={met['n_trades']}", flush=True)

    if ts_all:
        ts = pd.concat(ts_all)
        ts[["rule", "freq", "date", "exposure", "npos", "sig", "ent"]].to_csv(
            os.path.join(OUT, "仓位日序列.csv"), index=False, encoding="utf-8-sig")
    if mon_all:
        pd.concat(mon_all, ignore_index=True).to_csv(
            os.path.join(OUT, "月度信号开仓.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(reason_rows).to_csv(os.path.join(OUT, "拒绝原因.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(seg_rows).to_csv(os.path.join(OUT, "仓位区间.csv"), index=False, encoding="utf-8-sig")
    sdf = pd.DataFrame(sen_rows)
    sdf.to_csv(os.path.join(OUT, "敏感性.csv"), index=False, encoding="utf-8-sig")
    pd.set_option("display.width", 220)
    print(sdf[["rule", "freq", "variant", "cagr", "mdd", "sharpe", "exposure", "n_trades"]]
          .to_string(index=False), flush=True)
    return 0


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 8)
