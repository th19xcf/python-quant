#!/usr/bin/env python3
"""个股多因子月频回测库（归档自 reports/stock_multi_factor_20261105/backtest_mf.py）。

本文件是可复用实现，不含批次产物逻辑；驱动脚本是 run_multifactor_v2.py，
信号构建脚本是 build_multifactor_signals.py。归档时只改了输入路径的取法
（改为函数参数 + 仓库内默认值），交易/成本/风险层逻辑与默认参数保持不变。

依赖数据集（路径均可用函数参数或 CLI 覆盖）：
- 行情面板 panel.parquet（默认 reports/stock_momentum_20261105/data/panel.parquet，
  约 1.25GB，未入库；字段 ts_code/date/adj_open/adj_close/raw_open/pre_close/volume）
- 交易日历 data/local_store/quotes/index/sh/000001.SH.parquet
- 基准日收益 reports/stock_momentum_20261105/data/{bench_ew_daily,bench_mw_daily,
  bench_000300_daily}.csv
- 成本模型 src/backtest/costs/cost_model.py（佣金 5bp 双边、印花历史表、过户费、
  按市值/ADV 分档滑点）
- 绩效口径 src/backtest/performance/{performance_analyzer,benchmark_eval}.py
  （max_drawdown 为正数幅度；annual_return 为百分数；calmar=annual_return/|mdd|）

成交口径：月末 T 收盘出信号，T+1 开盘成交；后复权价；成本走 CostModel 个股分支。
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.backtest.costs.cost_model import CostModel  # noqa: E402
from src.backtest.performance.benchmark_eval import alpha_beta_hac  # noqa: E402
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer  # noqa: E402

DEFAULT_PANEL = REPO_ROOT / "reports" / "stock_momentum_20261105" / "data" / "panel.parquet"
DEFAULT_BENCH_DIR = REPO_ROOT / "reports" / "stock_momentum_20261105" / "data"
DEFAULT_CALENDAR = REPO_ROOT / "data" / "local_store" / "quotes" / "index" / "sh" / "000001.SH.parquet"

IS_START = date(2015, 1, 1)
IS_END = date(2022, 12, 31)
OOS_START = date(2023, 1, 1)
END = date(2026, 11, 4)
INIT_EQUITY = 1.0e8
COMP_KEYS = ("commission", "stamp", "transfer", "slippage")


@dataclass
class P:
    score_col: str = "score_a"
    n_hold: int = 40
    exit_mult: float = 2.0
    min_adv: float = 5.0e7
    min_rows: int = 272
    min_valid: int = 250
    max_weight: float = 0.0375
    turnover_cap: float = 0.20
    gate_low: float = 1.0
    gate_ma: int = 200
    vol_target: float = 0.0      # 0 = 不做波动率目标
    exposure_floor: float = 0.0
    drift_tol: float = 0.0       # 保留持仓允许的权重漂移（0=每月拉回等权）


def load_calendar(path: Path = DEFAULT_CALENDAR, start: date = date(2013, 1, 1),
                  end: date = END) -> list[date]:
    idx = (pl.read_parquet(path).select(pl.col("date").cast(pl.Date)).sort("date"))
    return [d for d in idx["date"].to_list() if start <= d <= end]


def load_ew_gate(cal: list[date], window: int, bench_dir: Path = DEFAULT_BENCH_DIR) -> np.ndarray:
    """EW 全市场指数的均线状态（收盘 > MA 记 1，否则 0）。"""
    ew = pl.read_csv(bench_dir / "bench_ew_daily.csv")
    ew = ew.with_columns(pl.col("date").str.to_date()).drop_nulls("ew_ret").sort("date")
    ew = ew.with_columns((1.0 + pl.col("ew_ret")).cum_prod().alias("level"))
    ew = ew.with_columns(
        pl.col("level").rolling_mean(window, min_samples=int(window * 0.75)).alias("ma"))
    st = dict(zip(ew["date"].to_list(),
                  (ew["level"] > ew["ma"]).to_list(), strict=False))
    out = np.ones(len(cal), dtype=float)
    for i, d in enumerate(cal):
        v = st.get(d)
        if v is not None:
            out[i] = 1.0 if v else 0.0
    return out


def load_prices(path: Path = DEFAULT_PANEL):
    panel = (pl.read_parquet(path)
             .select("ts_code", "date", "adj_open", "adj_close", "raw_open",
                     "pre_close", "volume")
             .sort(["ts_code", "date"]))
    out = {}
    for (code,), part in panel.group_by(["ts_code"], maintain_order=True):
        out[code] = {
            "epoch": part["date"].dt.epoch("d").to_numpy().astype(np.int64),
            "adj_open": part["adj_open"].to_numpy().astype(float),
            "adj_close": part["adj_close"].to_numpy().astype(float),
            "raw_open": part["raw_open"].to_numpy().astype(float),
            "pre_close": part["pre_close"].to_numpy().astype(float),
            "volume": part["volume"].to_numpy().astype(float),
        }
    return out


def limit_pct(code: str) -> float:
    c = code.split(".")[0]
    if code.endswith(".BJ"):
        return 0.30
    if c.startswith("688") or c.startswith(("300", "301")):
        return 0.20
    return 0.10


class BT:
    def __init__(self, sig: pl.DataFrame, price, cal: list[date], gates: dict[int, np.ndarray]):
        self.by_date = {d: part for (d,), part in
                        sig.group_by(["date"], maintain_order=True)}
        self.me = sig["date"].unique().sort().to_list()
        self.price = price
        self.cal = cal
        self.ce = np.array([(d - date(1970, 1, 1)).days for d in cal], dtype=np.int64)
        self.idx = {d: i for i, d in enumerate(cal)}
        self.exec_map = {d: cal[i + 1] for i, d in enumerate(cal[:-1])}
        self.gates = gates
        self.cost = CostModel()
        self.blocked = {}
        self.diag = []

    def px(self, code, epoch, col):
        a = self.price.get(code)
        if a is None:
            return None
        i = int(np.searchsorted(a["epoch"], epoch))
        if i < len(a["epoch"]) and a["epoch"][i] == epoch:
            return float(a[col][i])
        return None

    def last_px(self, code, epoch):
        a = self.price.get(code)
        if a is None or len(a["epoch"]) == 0:
            return None
        i = int(np.searchsorted(a["epoch"], epoch, side="right")) - 1
        return float(a["adj_close"][i]) if i >= 0 else None

    def tradable(self, code, epoch, side):
        a = self.price.get(code)
        if a is None:
            return False
        i = int(np.searchsorted(a["epoch"], epoch))
        if i >= len(a["epoch"]) or a["epoch"][i] != epoch:
            return False
        if not np.isfinite(a["volume"][i]) or a["volume"][i] <= 0:
            return False
        po, pc = a["raw_open"][i], a["pre_close"][i]
        if not np.isfinite(po) or not np.isfinite(pc) or pc <= 0:
            return False
        chg, lim = po / pc - 1.0, limit_pct(code)
        return not (side == "buy" and chg >= lim - 5e-4) and not (
            side == "sell" and chg <= -lim + 5e-4)

    def cost_of(self, notional, code, adv20, mv_yi, side, on, d):
        if not on or notional <= 0:
            return 0.0, dict.fromkeys(COMP_KEYS, 0.0)
        cp = self.cost.params
        comm = max(notional * cp["commission_rate"], cp["min_commission"])
        transfer = notional * self.cost._transfer_fee_rate(d, code)
        stamp = (notional * self.cost._stamp_tax_rate(d, "sell", code)
                 if side == "sell" else 0.0)
        slip = notional * self.cost.slippage_rate(code, adv20, mv_yi, None)
        return comm + stamp + transfer + slip, {
            "commission": comm, "stamp": stamp, "transfer": transfer, "slippage": slip}

    def _mark(self, shares, cash, i):
        eq = cash
        for c, n in shares.items():
            if n <= 0:
                continue
            px = self.px(c, self.ce[i], "adj_close")
            if px is None:
                px = self.last_px(c, self.ce[i])
            if px is not None:
                eq += n * px
        return eq

    def port_vol(self, codes, weights, i_sig):
        """持仓 60 日已实现波动（年化），用于波动率目标仓位。"""
        lo = max(0, i_sig - 60)
        win = self.ce[lo:i_sig + 1]
        if len(win) < 21:
            return 0.25
        rets = np.full((len(win) - 1, len(codes)), np.nan)
        for j, c in enumerate(codes):
            a = self.price.get(c)
            if a is None or len(a["epoch"]) == 0:
                continue
            pos = np.clip(np.searchsorted(a["epoch"], win), 0, len(a["epoch"]) - 1)
            ok = a["epoch"][pos] == win
            vals = np.where(ok, a["adj_close"][pos], np.nan)
            rets[:, j] = vals[1:] / vals[:-1] - 1.0
        w = np.asarray(weights, float)
        pr = []
        for t in range(rets.shape[0]):
            m = np.isfinite(rets[t])
            if m.sum() < 3 or w[m].sum() <= 0:
                continue
            pr.append(float(np.sum(rets[t][m] * w[m]) / w[m].sum()))
        return float(np.std(pr, ddof=1) * 252 ** 0.5) if len(pr) >= 10 else 0.25

    def run(self, p: P, cost_on: bool = True, start=IS_START, end=END):  # noqa: C901
        cash = INIT_EQUITY
        shares: dict[str, float] = {}
        nav, trades = [], []
        self.blocked = {"buy_missing": 0, "buy_untradable": 0,
                            "sell_missing": 0, "sell_untradable": 0}
        for si, sig_d in enumerate(self.me):
            if sig_d < start or sig_d > end:
                continue
            exec_d = self.exec_map.get(sig_d)
            if exec_d is None or exec_d > end:
                continue
            i_sig, i_exec = self.idx[sig_d], self.idx[exec_d]
            val_end = (self.idx[self.me[si + 1]] if si + 1 < len(self.me)
                       else self.idx[self.cal[-1]])

            cross = self.by_date[sig_d]
            cdf = cross.filter(
                pl.col(p.score_col).is_not_null()
                & (pl.col("adv20") >= p.min_adv)
                & (pl.col("row_no") >= p.min_rows)
                & (pl.col("valid_252") >= p.min_valid)
                & (pl.col("vol60") > 0)
                & (pl.col("adj_close").is_not_null()))
            rows = {r["ts_code"]: r for r in cdf.to_dicts()}
            ranked = [c for c, _ in sorted(rows.items(), key=lambda kv: -kv[1][p.score_col])]
            rank = {c: i for i, c in enumerate(ranked)}

            hold = [c for c in shares if shares[c] > 1e-9]
            retained = sorted([c for c in hold
                               if rank.get(c, 10 ** 9) < p.exit_mult * p.n_hold],
                              key=lambda c: rank.get(c, 10 ** 9))[:p.n_hold]
            probe = retained[:]
            for c in ranked:
                if len(probe) >= p.n_hold:
                    break
                if c not in probe and rank[c] < p.n_hold:
                    probe.append(c)

            eq_pre = self._mark(shares, cash, i_exec)
            gate = self.gates[p.gate_ma][i_sig] if p.gate_low < 1.0 else 1.0
            lev = 1.0
            if p.vol_target > 0 and probe:
                pv = self.port_vol(probe, [1.0] * len(probe), i_sig)
                lev = min(1.0, p.vol_target / pv) if pv > 0 else 1.0
            expo = min(1.0, max(p.exposure_floor, gate * lev))
            w_cur = {}
            for c, n in shares.items():
                if n <= 0:
                    continue
                px = self.px(c, self.ce[i_exec], "adj_open") or self.last_px(c, self.ce[i_exec])
                if px is not None and eq_pre > 0:
                    w_cur[c] = n * px / eq_pre
            gross_cur = sum(w_cur.values())

            # 选股层：整只退出优先，预算不足则延后整只退出，不做部分卖出
            sel, used, exited = {}, 0.0, []
            for c in sorted([x for x in hold if x not in retained],
                            key=lambda c: -rank.get(c, 10 ** 9)):
                cw = w_cur.get(c, 0.0)
                if used + 0.5 * cw > p.turnover_cap:
                    break
                sel[c] = sel.get(c, 0.0) - cw
                used += 0.5 * cw
                exited.append(c)
            slots = max(0, p.n_hold - (len(hold) - len(exited)))
            entry_w = expo / p.n_hold
            entries = []
            for c in ranked:
                if slots <= 0:
                    break
                if c in retained or c in exited or rank[c] >= p.n_hold:
                    continue
                if used + 0.5 * entry_w > p.turnover_cap:
                    break
                sel[c] = sel.get(c, 0.0) + entry_w
                used += 0.5 * entry_w
                slots -= 1
                entries.append(c)

            # 风险层：只作用于保留持仓，退出/新进按整只处理（避免互相抵消）
            exit_w = sum(w_cur.get(c, 0.0) for c in exited)
            gross_after = gross_cur - exit_w + entry_w * len(entries)
            k = expo / gross_after if gross_after > 1e-9 else 1.0
            for c, w in w_cur.items():
                if c in exited:
                    continue
                d = w * (k - 1.0)
                if c in retained and w > p.max_weight:
                    d -= (w - p.max_weight)
                if abs(d) > 1e-9:
                    sel[c] = sel.get(c, 0.0) + d
            delta = sel
            self.diag.append({"date": str(sig_d), "hold": len(hold), "retained": len(retained),
                                  "exited": len(exited), "slots": slots,
                                  "entries": sum(1 for v in sel.values() if v > 0.0)})

            # 卖出
            for c, dv in sorted(delta.items(), key=lambda kv: kv[1]):
                if dv >= -1e-6:
                    continue
                px = self.px(c, self.ce[i_exec], "adj_open")
                if px is None:
                    self.blocked["sell_missing"] += 1
                    continue
                if not self.tradable(c, self.ce[i_exec], "sell"):
                    self.blocked["sell_untradable"] += 1
                    continue
                n = min(shares.get(c, 0.0), -dv * eq_pre / px)
                if n <= 1e-12:
                    continue
                notional = n * px
                r = rows.get(c, {})
                cost, comp = self.cost_of(notional, c, r.get("adv20"),
                                          r.get("total_mv_yi"), "sell", cost_on, exec_d)
                cash += notional - cost
                shares[c] = shares.get(c, 0.0) - n
                trades.append(dict(date=exec_d, ts_code=c, side="sell", shares=n,
                                   price=px, notional=notional, cost=cost, **comp))
            # 买入
            buys = [(c, dv) for c, dv in delta.items() if dv > 1e-6]
            want = sum(dv * eq_pre for _, dv in buys)
            scale = min(1.0, cash / want) if want > 0 else 1.0
            for c, dv in sorted(buys, key=lambda kv: -kv[1]):
                px = self.px(c, self.ce[i_exec], "adj_open")
                if px is None:
                    self.blocked["buy_missing"] += 1
                    continue
                if not self.tradable(c, self.ce[i_exec], "buy"):
                    self.blocked["buy_untradable"] += 1
                    continue
                r = rows.get(c, {})
                notional = dv * eq_pre * scale
                rate = (self.cost.get_buy_cost_rate(exec_d, c, r.get("adv20"),
                                                    r.get("total_mv_yi"), None)
                        if cost_on else 0.0)
                if notional * (1 + rate) > cash:
                    notional = max(0.0, cash / (1 + rate))
                if notional <= 0:
                    continue
                n = notional / px
                cost, comp = self.cost_of(notional, c, r.get("adv20"),
                                          r.get("total_mv_yi"), "buy", cost_on, exec_d)
                cash -= notional + cost
                shares[c] = shares.get(c, 0.0) + n
                trades.append(dict(date=exec_d, ts_code=c, side="buy", shares=n,
                                   price=px, notional=notional, cost=cost, **comp))
            shares = {c: n for c, n in shares.items() if n > 1e-9}

            for i in range(i_exec, val_end + 1):
                nav.append({"date": self.cal[i], "equity": self._mark(shares, cash, i),
                                "n_pos": len(shares), "cash": cash})

        daily = pl.DataFrame(nav).unique(subset=["date"], keep="last").sort("date")
        tdf = pl.DataFrame(trades) if trades else pl.DataFrame(
            schema={**dict.fromkeys(("shares", "price", "notional", "cost"), pl.Float64),
                    "date": pl.Date, "ts_code": pl.Utf8, "side": pl.Utf8,
                    **dict.fromkeys(COMP_KEYS, pl.Float64)})
        return daily, tdf


def metrics(daily, trades, start, end, label):
    d = daily.filter((pl.col("date") >= start) & (pl.col("date") <= end))
    if d.height < 2:
        return {}
    curve = [{"date": r["date"], "equity": r["equity"]} for r in d.to_dicts()]
    pa = PerformanceAnalyzer()
    m = pa.analyze(curve, [], frequency="daily")
    years = (curve[-1]["date"] - curve[0]["date"]).days / 365.25
    avg_eq = float(np.mean([c["equity"] for c in curve]))
    tb = trades.filter((pl.col("date") >= start) & (pl.col("date") <= end))
    buy_n = float(tb.filter(pl.col("side") == "buy")["notional"].sum()) if tb.height else 0.0
    sell_n = float(tb.filter(pl.col("side") == "sell")["notional"].sum()) if tb.height else 0.0
    total_cost = float(tb["cost"].sum()) if tb.height else 0.0
    to = PerformanceAnalyzer.annual_turnover(buy_n, avg_eq, years)
    comps = {k: float(tb[k].sum()) if tb.height else 0.0 for k in COMP_KEYS}
    return {"label": label, "start": str(curve[0]["date"]), "end": str(curve[-1]["date"]),
                "years": round(years, 3), "annual_return": m["annual_return"],
                "max_drawdown": m["max_drawdown"],
                "longest_underwater_months": m["longest_underwater_months"],
                "calmar": m["calmar"], "volatility": m["volatility"], "sharpe": m["sharpe_ratio"],
                "turnover_one_way": to["one_way"], "turnover_two_way": to["two_way"],
                "annual_cost_pp": PerformanceAnalyzer.annual_cost_ratio(total_cost, avg_eq, years),
                "cost_pp": {k: PerformanceAnalyzer.annual_cost_ratio(v, avg_eq, years)
                         for k, v in comps.items()},
                "sell_buy_ratio": (sell_n / buy_n if buy_n > 0 else None),
                "avg_equity": avg_eq, "buy_notional": buy_n,
                "final_equity": float(curve[-1]["equity"]),
                "initial_equity": float(curve[0]["equity"])}


def month_end_nav(daily, me, start, end):
    d = daily.filter((pl.col("date") >= start) & (pl.col("date") <= end)).sort("date")
    dates, eqs = d["date"].to_list(), d["equity"].to_numpy()
    out, j = {}, 0
    for m in me:
        if m < start or m > end:
            continue
        while j < len(dates) and dates[j] <= m:
            j += 1
        if j > 0:
            out[m] = float(eqs[j - 1])
    return out


def monthly_returns(navmap):
    keys = sorted(navmap)
    out = {}
    for i in range(1, len(keys)):
        prev = navmap[keys[i - 1]]
        cur = navmap[keys[i]]
        if prev is None or cur is None:
            continue
        prev, cur = float(prev), float(cur)
        if not np.isfinite(prev) or not np.isfinite(cur) or prev <= 0:
            continue
        out[keys[i]] = cur / prev - 1.0
    return out


def load_benchmarks(me, bench_dir: Path = DEFAULT_BENCH_DIR):
    def to_month(path, col):
        daily = pl.read_csv(path).with_columns(pl.col("date").str.to_date())
        daily = daily.select("date", col).drop_nulls().sort("date")
        lvl = (1 + daily[col]).cum_prod().to_numpy()
        dates = daily["date"].to_list()
        vals, j = {}, 0
        for m in me:
            while j < len(dates) and dates[j] <= m:
                j += 1
            if j > 0:
                vals[m] = float(lvl[j - 1])
        return monthly_returns(vals)

    return {"EW全市场": to_month(bench_dir / "bench_ew_daily.csv", "ew_ret"),
            "市值加权": to_month(bench_dir / "bench_mw_daily.csv", "mw_ret"),
            "000300.SH": to_month(bench_dir / "bench_000300_daily.csv", "idx300_ret")}


def excess_table(sr, bench, segs):
    out = []
    for s, (a, b) in segs.items():
        seg = {k: v for k, v in sr.items() if a <= k <= b}
        for bn, bm in bench.items():
            common = sorted(k for k in seg if k in bm)
            if len(common) < 12:
                out.append({"segment": s, "benchmark": bn, "n_months": len(common),
                                "excess_ann": None, "ir": None, "alpha_ann": None,
                                "alpha_t": None, "beta": None, "note": "样本不足12月"})
                continue
            y = [seg[k] for k in common]
            x = [bm[k] for k in common]
            ab = alpha_beta_hac(y, x, periods_per_year=12, maxlags=3)
            out.append({"segment": s, "benchmark": bn, "n_months": len(common),
                            "excess_ann": ab.get("excess_ann"), "ir": ab.get("ir"),
                            "alpha_ann": ab.get("alpha_ann"), "alpha_t": ab.get("alpha_t"),
                            "beta": ab.get("beta")})
    return pl.DataFrame(out)


def params_dict(p: P):
    return dict(p.__dict__)
