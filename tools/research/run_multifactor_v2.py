#!/usr/bin/env python3
"""个股多因子 v2 驱动：7 因子（并入 accruals/sue）+ 波动率目标仓位。

归档自 reports/stock_multi_factor_v2_20261105/run_v2.py，回测逻辑与默认参数不变；
只把输入/输出路径改为 CLI 参数（默认值指回仓库内既有产物目录）。

改造 A：FACTORS = 5 个基础因子 + accruals(-) + sue(+)，信号由
build_multifactor_signals.py 生成。
改造 B：exposure = min(1, target_vol / 已实现 60 日波动)，下限 0，不加杠杆；
target_vol 只用 IS（2015-01~2022-12）择定，规则为"IS 可行集(DD<=20% 且 水下<=18 月)
内取 Calmar 最大；可行集为空则回退 IS Calmar 最大"。

输出（默认 out-dir = reports/stock_multi_factor_v2_20261105）：
- tables/summary.json、tables/excess_A.csv
- runs/A_net/{daily_nav.csv,trades.csv}、runs/A_gross/...

数据依赖见 multifactor_backtest.py 文件头与 build_multifactor_signals.py 文件头。
"""
from __future__ import annotations

import argparse
import json
from dataclasses import replace
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from multifactor_backtest import (
    BT,
    DEFAULT_BENCH_DIR,
    DEFAULT_CALENDAR,
    DEFAULT_PANEL,
    END,
    INIT_EQUITY,
    IS_END,
    IS_START,
    OOS_START,
    P,
    excess_table,
    load_benchmarks,
    load_calendar,
    load_prices,
    metrics,
    month_end_nav,
    monthly_returns,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BASE = REPO_ROOT / "reports" / "stock_multi_factor_v2_20261105"
DEFAULT_SIGNALS = DEFAULT_BASE / "signals" / "signals.parquet"

SEGS = {"IS": (IS_START, IS_END), "OOS": (OOS_START, END), "ALL": (IS_START, END)}
ERAS = {
    "2015-2016": (date(2015, 1, 1), date(2016, 12, 31)),
    "2017": (date(2017, 1, 1), date(2017, 12, 31)),
    "2018-2022": (date(2018, 1, 1), IS_END),
    "OOS 2023-2026": (OOS_START, END),
}
GRID = [0.04, 0.05, 0.06, 0.08, 0.10, 0.12, 0.15, 0.18]


def daily_rets(daily):
    d = daily.sort("date")
    e = d["equity"].to_numpy().astype(float)
    r = np.diff(e) / e[:-1]
    return dict(zip(d["date"].to_list()[1:], r, strict=False))


def stats(daily, start, end):
    """区间年化/回撤/水下（日历折算，与 PerformanceAnalyzer 口径一致）。"""
    d = daily.filter((pl.col("date") >= start) & (pl.col("date") <= end)).sort("date")
    if d.height < 2:
        return {"ann": None, "mdd": None, "uw": None}
    e = d["equity"].to_numpy().astype(float)
    span = (d["date"][-1] - d["date"][0]).days
    ann = (e[-1] / e[0]) ** (365 / span) - 1 if e[0] > 0 and span > 0 else None
    peak = np.maximum.accumulate(e)
    dd = e / peak - 1
    mdd = float(-dd.min())  # 与 PerformanceAnalyzer 一致：正数表示回撤幅度（%）
    under = e < peak
    uw, cur = 0, 0
    for u in under:
        cur = cur + 1 if u else 0
        uw = max(uw, cur)
    return {"ann": ann, "mdd": mdd, "uw": uw * 30.44 / span}


def main(argv=None) -> int:  # noqa: C901
    ap = argparse.ArgumentParser(description="个股多因子 v2 回测（7 因子 + 波动率目标）")
    ap.add_argument("--signals", type=Path, default=DEFAULT_SIGNALS)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_BASE)
    ap.add_argument("--panel", type=Path, default=DEFAULT_PANEL)
    ap.add_argument("--index", type=Path, default=DEFAULT_CALENDAR,
                    help="交易日历 parquet（默认 000001.SH）")
    ap.add_argument("--bench-dir", type=Path, default=DEFAULT_BENCH_DIR)
    a = ap.parse_args(argv)

    base_dir = Path(a.out_dir)
    (base_dir / "tables").mkdir(parents=True, exist_ok=True)
    (base_dir / "runs").mkdir(parents=True, exist_ok=True)

    sig = pl.read_parquet(a.signals)
    cal = load_calendar(a.index)
    me = sig["date"].unique().sort().to_list()
    price = load_prices(a.panel)
    bt = BT(sig, price, cal, {200: np.ones(len(cal))})
    out = {"grid": [], "era_factors": {}}

    # 各年 + 各段可用因子数（披露用，禁止填补未来值）
    yr = (sig.with_columns(pl.col("date").dt.year().alias("y"))
          .group_by("y").agg(pl.col("n_factor").mean().round(2).alias("mean_n")).sort("y"))
    out["era_factors"]["by_year"] = {int(r["y"]): r["mean_n"] for r in yr.to_dicts()}
    for name, (a0, b0) in ERAS.items():
        d = sig.filter((pl.col("date") >= a0) & (pl.col("date") <= b0))
        out["era_factors"][name] = round(float(d["n_factor"].mean()), 2)

    base = P(score_col="score_a", vol_target=0.12, exposure_floor=0.0, gate_low=1.0)
    # 改造 B：target_vol 网格（只用 IS 判定）
    runs = {}
    for tv in GRID:
        p = replace(base, vol_target=tv)
        dn, tn = bt.run(p, True)
        dg, tg = bt.run(p, False)
        runs[tv] = {"net": (dn, tn), "gross": (dg, tg), "net_daily": daily_rets(dn),
                        "blocked": dict(bt.blocked)}
        is_n = metrics(dn, tn, IS_START, IS_END, f"tv{tv}_net")
        is_g = metrics(dg, tg, IS_START, IS_END, f"tv{tv}_gross")
        out["grid"].append({"target_vol": tv, "is_net": is_n.get("annual_return"),
                                "is_gross": is_g.get("annual_return"),
                                "is_dd": is_n.get("max_drawdown"),
                                "is_uw": is_n.get("longest_underwater_months"),
                                "is_calmar": is_n.get("calmar"),
                                "is_to": is_n.get("turnover_one_way"),
                                "is_cost": is_n.get("annual_cost_pp")})
        print(f"tv={tv:.2f} IS net={is_n['annual_return']:.2f}% gross={is_g['annual_return']:.2f}% "
              f"DD={is_n['max_drawdown']:.2f}% UW={is_n['longest_underwater_months']} "
              f"Calmar={is_n['calmar']:.3f} to1={is_n['turnover_one_way']:.3f}x", flush=True)

    feas = [g for g in out["grid"]
            if g["is_dd"] is not None and g["is_dd"] <= 20.0
            and g["is_uw"] is not None and g["is_uw"] <= 18]
    chosen = max(feas, key=lambda g: g["is_calmar"]) if feas else \
        max(out["grid"], key=lambda g: g["is_calmar"])
    tv = chosen["target_vol"]
    out["chosen_target_vol"] = tv
    out["chosen_feasible"] = bool(feas)
    print(f"chosen target_vol={tv} feasible={bool(feas)}", flush=True)

    p_final = replace(base, vol_target=tv)
    dn, tn = runs[tv]["net"]
    dg, tg = runs[tv]["gross"]
    out["blocked"] = runs[tv]["blocked"]
    rd = base_dir / "runs"
    for tag, d, t in (("A_net", dn, tn), ("A_gross", dg, tg)):
        (rd / tag).mkdir(parents=True, exist_ok=True)
        d.write_csv(rd / tag / "daily_nav.csv")
        if t.height:
            t.write_csv(rd / tag / "trades.csv")
    # 主结果
    seg = {}
    for kind, (d, t) in (("net", (dn, tn)), ("gross", (dg, tg))):
        seg[kind] = {s: metrics(d, t, a0, b0, f"A_{kind}_{s}") for s, (a0, b0) in SEGS.items()}
        seg[kind]["ERAS"] = {e: stats(d, a0, b0) for e, (a0, b0) in ERAS.items()}
    # 逐年收益（净），首个年度以上一期初权益为基
    yr_rows = []
    dd = dn.with_columns(pl.col("date").dt.year().alias("y"))
    for (y,), part in dd.group_by(["y"], maintain_order=True):
        yr_rows.append({"year": int(y), "end_equity": float(part["equity"][-1])})
    base_eq = INIT_EQUITY
    for r in yr_rows:
        r["ret"] = r["end_equity"] / base_eq - 1.0
        base_eq = r["end_equity"]
    seg["net"]["yearly"] = yr_rows
    out["A"] = seg

    # A2（ICIR 加权，参考）
    p2 = replace(p_final, score_col="score_a2")
    d2n, t2n = bt.run(p2, True)
    d2g, t2g = bt.run(p2, False)
    out["A2"] = {kind: {s: metrics(d, t, a0, b0, f"A2_{kind}_{s}") for s, (a0, b0) in SEGS.items()}
                 for kind, (d, t) in (("net", (d2n, t2n)), ("gross", (d2g, t2g)))}

    # 基准超额（净、月度）
    bench = load_benchmarks(me, a.bench_dir)
    sr = monthly_returns(month_end_nav(dn, me, IS_START, END))
    ext = excess_table(sr, bench, {"IS": (IS_START, IS_END), "OOS": (OOS_START, END)})
    ext.write_csv(base_dir / "tables" / "excess_A.csv")
    out["excess_A"] = ext.to_dicts()

    # 扰动 ±20%（仅作稳健性诊断，不参与参数选择）
    pert = []
    for mult in (0.8, 1.2):
        pv = replace(p_final, vol_target=tv * mult)
        dp, tp = bt.run(pv, True)
        rec = {"mult": mult, "target_vol": tv * mult}
        for s, (a0, b0) in SEGS.items():
            m = metrics(dp, tp, a0, b0, f"pert{mult}_{s}")
            rec[s] = {"ann": m.get("annual_return"), "dd": m.get("max_drawdown"),
                          "uw": m.get("longest_underwater_months"),
                          "calmar": m.get("calmar"),
                          "to": m.get("turnover_one_way")}
        pert.append(rec)
        print(f"pert {mult}: IS {rec['IS']['ann']:.2f}% OOS {rec['OOS']['ann']:.2f}% "
              f"ALL {rec['ALL']['ann']:.2f}%", flush=True)
    out["perturbation"] = pert

    # walk-forward：每年只用上一年的已实现数据，按与静态同一规则选 target_vol，下一年整年执行
    def select_tv(tr_end):
        rows = [(v, stats(runs[v]["net"][0], IS_START, tr_end)) for v in GRID]
        feas0 = [(v, s) for v, s in rows
                 if s["mdd"] is not None and s["mdd"] <= 20.0
                 and s["uw"] is not None and s["uw"] <= 18]
        pool = feas0 if feas0 else rows

        def calm(x):
            s = x[1]
            return (s["ann"] * 100.0 / s["mdd"]) if s["mdd"] else -9.0

        return max(pool, key=calm)[0]

    rows = []
    for y in range(2016, 2027):
        tr_end = date(y - 1, 12, 31)
        sel = select_tv(tr_end)
        rets = [r for k, r in runs[sel]["net_daily"].items()
                if date(y, 1, 1) <= k <= date(y, 12, 31)]
        if not len(rets):
            continue
        yr_ret = float(np.prod([1 + r for r in rets]) - 1)
        rows.append({"year": y, "target_vol": sel, "net": yr_ret})
    if rows:
        def ann_of(rs):
            eq = float(np.prod([1 + r["net"] for r in rs]))
            return eq ** (252 / sum(
                len([k for k in runs[r["target_vol"]]["net_daily"]
                     if date(r["year"], 1, 1) <= k <= date(r["year"], 12, 31)])
                for r in rs)) - 1

        all_rows = rows
        oos_rows = [r for r in rows if r["year"] >= 2023]
        out["walkforward"] = {
            "annualized": ann_of(all_rows), "years": all_rows,
            "oos_annualized": (ann_of(oos_rows) if oos_rows else None),
            "oos_years": oos_rows}
        print(f"WF net={out['walkforward']['annualized']:.4f} "
              f"OOS-WF={out['walkforward']['oos_annualized']}", flush=True)

    out["params"] = {"target_vol": tv, "n_hold": base.n_hold, "exit_mult": base.exit_mult,
                         "turnover_cap": base.turnover_cap, "vol_target_rule": "min(1, tv/60d realized)"}
    (base_dir / "tables" / "summary.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print("done", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
