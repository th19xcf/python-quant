"""A 股风格轮动复盘（2010-2026 定量风格/行业 + 1990-2009 指数层面）

用途：描述性复盘，不产出交易信号、不做参数优化、不给买入建议。
数据：本地 data/local_store 只读消费。

口径（可复现）：
- 全 A 含退市：stock_basic 中 list_date <= 月末 且 (delist_date 为空 或 月末 <= delist_date)，
  行情来自 quotes/stock/{sh,sz,bj}/*.parquet，退市股在其存续期内参与。
- 月度收益：后复权价 close*hfq_factor 的月末对月末环比（与 pct_chg 一致）。
- 市值：原始 close × total_share（as-of 最近一次生效股本），用于规模分组与换手率。
- 估值/股息：factors/factor_panel.parquet 月末行的 ep / bp / dy（PIT 行业随行）。
- 行业：industry_pit_sw.parquet 每只股票最近一次非空申万一级名称（静态现代归类，回溯应用，
  仅在分类层面存在前视，用于描述性行业对比，已在报告注明）。
- 基准：全 A 等权（当月全体成员等权平均收益，含退市）为主基准；沪深300(000300.SH) 作对照。

风格分组（每月截面独立排序）：
- 规模：市值 pct 排位，小盘=后 30%，大盘=前 30%。
- 价值/成长：bp>0 者按 bp 排位，价值=前 20%，成长=后 20%。
- 高股息：dy>=0 者按 dy 排位，高股息=前 20%，低股息=后 20%。
- 高换手：月度 amount/市值 排名前 20%。
- 小盘高换手：小盘 ∩ 高换手。

阶段四项指标（对每个风格组/行业）：
1) 相对强弱：区间累计相对全 A 等权的超额（相对净值连乘 -1）与年化超额；
2) 成交额占比：区间内该组成交额 / 全市场成交额；
3) 上涨扩散度：组内“个股月度收益 > 全 A 当月等权收益”的观测占比；
4) 区间累计超额与回撤：组自身复利累计收益、区间最大回撤（组净值）。

输出（同目录）：
- stage_style_metrics.json  分阶段风格组四项指标
- stage_industry_metrics.json 分阶段行业四项指标（31 申万一级）
- index_overview.json  1990-2009 指数层面概览
- panel_coverage.json  面板覆盖与样本量
- details/ 明细（月度风格组净值、行业净值等，仅供复核，不进报告上下文）

运行：
  .venv\\Scripts\\python.exe work/风格轮动复盘_2026-11-28/style_rotation_review.py
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
STORE = ROOT / "data" / "local_store"
STOCK_DIRS = [STORE / "quotes" / "stock" / ex for ex in ("sh", "sz", "bj")]
INDEX_SH = STORE / "quotes" / "index" / "sh"
STOCK_BASIC = STORE / "meta" / "stock_basic.parquet"
SHARES = STORE / "meta" / "factors" / "stock_shares.parquet"
FACTOR = STORE / "factors" / "factor_panel.parquet"
SW_PIT = STORE / "meta" / "industry_pit" / "industry_pit_sw.parquet"

START_PERIOD = pd.Period("2010-01", "M")
END_PERIOD = pd.Period("2026-10", "M")

# 分阶段（2010+ 定量）
QUANT_STAGES = [
    ("2010-2012", "2010-01", "2012-12", "消费医药成长"),
    ("2013-2015", "2013-01", "2015-12", "互联网创业板传媒软件"),
    ("2016-2017", "2016-01", "2017-12", "白马消费金融家电"),
    ("2018", "2018-01", "2018-12", "防御低估值金融地产"),
    ("2019-2021H1", "2019-01", "2021-06", "科技新能源医药消费"),
    ("2021H2-2022", "2021-07", "2022-12", "周期能源资源高股息"),
    ("2023-2024", "2023-01", "2024-12", "高股息中特估资源兼题材反复"),
    ("2025-2026", "2025-01", "2026-10", "科技成长与AI主线、与红利资源反复轮动"),
]

# 1990-2009 指数层面阶段
INDEX_STAGES = [
    ("1990-1995", "1990-12", "1995-12", "题材重组小盘"),
    ("1996-2001", "1996-01", "2001-12", "绩优股与网络科技成长"),
    ("2002-2005", "2002-01", "2005-12", "熊市防御低估值"),
    ("2006-2007", "2006-01", "2007-12", "大盘蓝筹金融地产周期"),
    ("2008-2009", "2008-01", "2009-12", "基建地产周期"),
]


def month_end_trading_dates() -> pd.Series:
    """每个自然月的最后一个真实交易日（来自上证综指日线）。"""
    d = pd.read_parquet(INDEX_SH / "000001.SH.parquet", columns=["date"])
    d["date"] = pd.to_datetime(d["date"])
    d["period"] = d["date"].dt.to_period("M")
    return d.groupby("period")["date"].max()


def _read_index(code: str) -> pd.DataFrame:
    """读取指数日线并返回月末复利净值序列。"""
    for ex in ("sh", "sz"):
        p = INDEX_SH.parent / ex / f"{code}.parquet"
        if p.exists():
            d = pd.read_parquet(p, columns=["date", "close"]).sort_values("date")
            d["date"] = pd.to_datetime(d["date"])
            d["period"] = d["date"].dt.to_period("M")
            last = d.groupby("period").tail(1).set_index("period")["close"]
            return last
    return pd.Series(dtype=float)


def _max_drawdown(nav: pd.Series) -> float:
    if len(nav) == 0:
        return float("nan")
    peak = nav.cummax()
    dd = nav / peak - 1.0
    return float(dd.min())


def _stage_nav(returns: pd.Series, start: pd.Period, end: pd.Period) -> pd.Series:
    r = returns[(returns.index >= start) & (returns.index <= end)]
    return (1.0 + r).cumprod()


def load_universe() -> pd.DataFrame:
    sb = pd.read_parquet(
        STOCK_BASIC, columns=["ts_code", "list_date", "delist_date", "status"]
    )
    for c in ("list_date", "delist_date"):
        sb[c] = pd.to_datetime(sb[c], errors="coerce")
    return sb


def load_stock_monthly() -> pd.DataFrame:
    """逐只读取个股日线，聚合成 月末原始价/后复权价/月度成交额。"""
    frames = []
    files = sorted(f for d in STOCK_DIRS for f in d.glob("*.parquet"))
    for f in files:
        d = pd.read_parquet(f, columns=["date", "close", "amount", "hfq_factor"])
        if d.empty:
            continue
        d["date"] = pd.to_datetime(d["date"])
        d = d[d["date"] >= pd.Timestamp("2009-12-01")]
        if d.empty:
            continue
        d["period"] = d["date"].dt.to_period("M")
        d["adj"] = d["close"] * d["hfq_factor"]
        d = d.sort_values("date")
        g = d.groupby("period")
        last = g.tail(1).set_index("period")
        amt = g["amount"].sum()
        out = pd.DataFrame(
            {
                "close_raw": last["close"],
                "adj": last["adj"],
                "date_m": last["date"],
                "amount_m": amt.reindex(last.index),
            }
        )
        out["ts_code"] = f.stem.split(".")[0] + "." + f.stem.split(".")[1]
        frames.append(out.reset_index())
    monthly = pd.concat(frames, ignore_index=True)
    return monthly


def load_shares_asof(monthly: pd.DataFrame) -> np.ndarray:
    """按 (股票, 月份) 取最近一次生效股本（total_share 优先，缺失用 float_share）。

    用「月末月份网格 + 前向填充」实现 as-of，避免 merge_asof 的分组排序限制。
    同一月内多次变更取 effective_date 最晚的一条，等于月末口径。
    """
    sh = pd.read_parquet(
        SHARES, columns=["ts_code", "effective_date", "total_share", "float_share"]
    )
    sh["effective_date"] = pd.to_datetime(sh["effective_date"])
    sh["ts_code"] = sh["ts_code"].astype(str)
    sh["period"] = sh["effective_date"].dt.to_period("M")
    sh = sh.sort_values("effective_date").groupby(
        ["ts_code", "period"], as_index=False
    ).tail(1)
    periods = pd.period_range("2009-12", "2026-10", freq="M")
    piv_t = (
        sh.pivot(index="ts_code", columns="period", values="total_share")
        .reindex(columns=periods)
        .ffill(axis=1)
    )
    piv_f = (
        sh.pivot(index="ts_code", columns="period", values="float_share")
        .reindex(columns=periods)
        .ffill(axis=1)
    )
    shares = piv_t.where(piv_t.notna(), piv_f)
    long = shares.stack().rename("shares").reset_index()
    long.columns = ["ts_code", "period", "shares"]
    merged = monthly[["ts_code", "period"]].merge(
        long, on=["ts_code", "period"], how="left"
    )
    return merged["shares"].to_numpy()


def load_factors_monthend(month_ends: list) -> pd.DataFrame:
    """读取 factor_panel 在给定月末交易日的截面。"""
    dates = [d.date() if hasattr(d, "date") else d for d in month_ends]
    df = pd.read_parquet(
        FACTOR,
        columns=["ts_code", "signal_date", "ep", "bp", "dy", "industry_l1"],
        filters=[("signal_date", "in", dates)],
    )
    df["period"] = pd.to_datetime(df["signal_date"]).dt.to_period("M")
    df = df.drop(columns=["signal_date"])
    df["ts_code"] = df["ts_code"].astype(str)
    return df


def load_static_industry() -> pd.DataFrame:
    """每只股票最近一次非空申万一级名称（静态现代归类）。"""
    sw = pd.read_parquet(
        SW_PIT, columns=["ts_code", "industry_l1_name", "in_date", "record_updated_at"]
    )
    sw = sw.dropna(subset=["industry_l1_name"]).copy()
    sw["ord"] = pd.to_datetime(sw["record_updated_at"], errors="coerce")
    sw = sw.sort_values(["ts_code", "ord", "in_date"])
    latest = sw.groupby("ts_code").tail(1)[["ts_code", "industry_l1_name"]]
    latest = latest.rename(columns={"industry_l1_name": "industry"})
    latest["ts_code"] = latest["ts_code"].astype(str)
    return latest


def build_panel() -> tuple[pd.DataFrame, dict]:
    universe = load_universe()
    monthly = load_stock_monthly()
    monthly["ts_code"] = monthly["ts_code"].astype(str)
    monthly["period"] = monthly["period"].astype("period[M]")
    # 保留 2009-12 用于计算 2010-01 的环比，之后再按阶段裁掉
    monthly = monthly[monthly["period"] >= pd.Period("2009-12", "M")].reset_index(drop=True)

    # 月度收益：仅当上一期是“紧邻的上一个月”才计算，避免停牌/复牌/上市跨越造成的伪收益
    monthly = monthly.sort_values(["ts_code", "period"]).reset_index(drop=True)
    monthly["ym"] = monthly["period"].dt.year * 12 + monthly["period"].dt.month
    prev_ym = monthly.groupby("ts_code")["ym"].shift(1)
    contiguous = (monthly["ym"] - prev_ym) == 1
    raw_ret = monthly.groupby("ts_code")["adj"].pct_change()
    monthly["ret"] = raw_ret.where(contiguous)

    # 市值
    monthly["shares"] = load_shares_asof(monthly)
    monthly["mktcap"] = monthly["close_raw"] * monthly["shares"]

    # 月末真实交易日列表（用于取 factor_panel 截面）
    med = month_end_trading_dates()
    month_ends = [d.to_pydatetime() for d in med[med.index >= START_PERIOD]]
    factors = load_factors_monthend(month_ends)
    industry = load_static_industry()

    panel = monthly.merge(factors, on=["ts_code", "period"], how="left")
    panel = panel.merge(industry, on="ts_code", how="left")

    # factor_panel 的 dy 为百分数，统一成小数；bp 与之同尺度（book/price）
    panel["dy"] = panel["dy"] / 100.0
    # 2015-2017 factor_panel 的 bp/dy 全缺，改用 analysis_monthly 的 ff_value_bp / ff_div_yield 补齐
    am = pd.read_parquet(
        STORE / "meta" / "research_inputs" / "analysis_monthly.parquet",
        columns=["ts_code", "date", "ff_value_bp", "ff_div_yield"],
    )
    am["ts_code"] = am["ts_code"].astype(str)
    am["period"] = pd.to_datetime(am["date"]).dt.to_period("M")
    am = am[["ts_code", "period", "ff_value_bp", "ff_div_yield"]].rename(
        columns={"ff_value_bp": "bp_am", "ff_div_yield": "dy_am"}
    )
    panel = panel.merge(am, on=["ts_code", "period"], how="left")
    panel["bp"] = panel["bp"].where(panel["bp"].notna(), panel["bp_am"])
    panel["dy"] = panel["dy"].where(panel["dy"].notna(), panel["dy_am"])
    panel = panel.drop(columns=["bp_am", "dy_am"])

    # 月度截面收益截尾 1%/99%，抑制源端异常跳变对等权组合的污染
    g = panel.groupby("period")["ret"]
    lo = g.transform(lambda s: s.quantile(0.01))
    hi = g.transform(lambda s: s.quantile(0.99))
    panel["ret_tr"] = panel["ret"].clip(lo, hi)

    # 上市/退市过滤（含退市）
    panel = panel.merge(
        universe[["ts_code", "list_date", "delist_date"]], on="ts_code", how="left"
    )
    panel["date_m"] = pd.to_datetime(panel["date_m"])
    listed = panel["date_m"] >= panel["list_date"]
    alive = panel["delist_date"].isna() | (panel["date_m"] <= panel["delist_date"])
    panel = panel[listed & alive].copy()
    panel = panel[(panel["period"] >= START_PERIOD) & (panel["period"] <= END_PERIOD)].copy()

    meta = {
        "panel_rows": int(len(panel)),
        "stocks": int(panel["ts_code"].nunique()),
        "months": int(panel["period"].nunique()),
        "first_period": str(panel["period"].min()),
        "last_period": str(panel["period"].max()),
        "ret_nonnull": int(panel["ret"].notna().sum()),
        "mktcap_nonnull": int(panel["mktcap"].notna().sum()),
        "bp_nonnull": int(panel["bp"].notna().sum()),
        "dy_nonnull": int(panel["dy"].notna().sum()),
        "industry_nonnull": int(panel["industry"].notna().sum()),
        "industry_names": int(panel["industry"].nunique()),
    }
    return panel, meta


def monthly_benchmarks(panel: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    alla = panel.groupby("period")["ret_tr"].mean()
    idx = _read_index("000300.SH")
    csi = idx.pct_change().dropna()
    return alla, csi


def assign_style_groups(panel: pd.DataFrame) -> pd.DataFrame:
    """每月截面打分并打标签（布尔列）。"""
    p = panel.copy()
    for col, out in [
        ("mktcap", "rank_size"),
        ("bp", "rank_bp"),
        ("dy", "rank_dy"),
        ("amount_m", "rank_amt"),
    ]:
        p[f"{out}"] = np.nan
    grp = p.groupby("period", sort=False)
    p["rank_size"] = grp["mktcap"].rank(pct=True)
    # 价值/成长只用 bp>0
    bp_valid = p["bp"].where(p["bp"] > 0)
    p["rank_bp"] = bp_valid.groupby(p["period"]).rank(pct=True)
    dy_valid = p["dy"].where(p["dy"] >= 0)
    p["rank_dy"] = dy_valid.groupby(p["period"]).rank(pct=True)
    p["turnover"] = p["amount_m"] / p["mktcap"]
    p["rank_turn"] = p.groupby("period")["turnover"].rank(pct=True)

    p["g_size_small"] = p["rank_size"] <= 0.30
    p["g_size_large"] = p["rank_size"] >= 0.70
    p["g_value"] = p["rank_bp"] >= 0.80
    p["g_growth"] = p["rank_bp"] <= 0.20
    p["g_highdiv"] = p["rank_dy"] >= 0.80
    p["g_lowdiv"] = p["rank_dy"] <= 0.20
    p["g_highturn"] = p["rank_turn"] >= 0.80
    p["g_small_highturn"] = p["g_size_small"] & p["g_highturn"]
    return p


STYLE_GROUPS = {
    "小盘": "g_size_small",
    "大盘": "g_size_large",
    "价值(高BP)": "g_value",
    "成长(低BP)": "g_growth",
    "高股息": "g_highdiv",
    "低股息": "g_lowdiv",
    "高换手": "g_highturn",
    "小盘高换手": "g_small_highturn",
}


def _group_stage_metrics(
    p: pd.DataFrame,
    mask: pd.Series,
    alla: pd.Series,
    start: pd.Period,
    end: pd.Period,
    total_amt_by_period: pd.Series,
) -> dict:
    sub = p[mask & p["ret_tr"].notna()]
    if sub.empty:
        return {}
    g = sub.groupby("period")
    gret = g["ret_tr"].mean()
    gret = gret[(gret.index >= start) & (gret.index <= end)]
    b = alla[(alla.index >= start) & (alla.index <= end)]
    if len(gret) == 0:
        return {}
    idx = gret.index
    b = b.reindex(idx)
    group_nav = (1 + gret).cumprod()
    bench_nav = (1 + b).cumprod()
    rel_nav = (1 + gret) / (1 + b)
    cum_excess = float(rel_nav.cumprod().iloc[-1] - 1.0)
    months = len(idx)
    ann_excess = float((1 + cum_excess) ** (12.0 / months) - 1) if months else float("nan")

    # 成交额占比
    amt = sub[(sub["period"] >= start) & (sub["period"] <= end)].groupby("period")["amount_m"].sum()
    tot = total_amt_by_period.reindex(amt.index)
    amt_share = float(amt.sum() / tot.sum()) if tot.sum() else float("nan")

    # 上涨扩散度：组内“原始月度收益 > 全A当月等权收益”的观测占比
    sub2 = sub[(sub["period"] >= start) & (sub["period"] <= end)].copy()
    sub2["bench"] = sub2["period"].map(alla)
    valid = sub2["bench"].notna() & sub2["ret"].notna()
    breadth = float((sub2.loc[valid, "ret"] > sub2.loc[valid, "bench"]).mean()) if valid.any() else float("nan")

    return {
        "months": int(months),
        "cum_return": float(group_nav.iloc[-1] - 1.0),
        "bench_return": float(bench_nav.iloc[-1] - 1.0),
        "cum_excess": cum_excess,
        "ann_excess": ann_excess,
        "maxdd": _max_drawdown(group_nav),
        "breadth": breadth,
        "amount_share": amt_share,
        "members_avg": float(sub2.groupby("period").size().mean()),
    }


def compute_style_stages(p: pd.DataFrame, alla: pd.Series) -> dict:
    total_amt = p.groupby("period")["amount_m"].sum()
    result = {}
    for name, s, e, desc in QUANT_STAGES:
        sp, ep = pd.Period(s, "M"), pd.Period(e, "M")
        stage = {"label": name, "desc": desc, "groups": {}}
        for gname, col in STYLE_GROUPS.items():
            stage["groups"][gname] = _group_stage_metrics(
                p, p[col].fillna(False), alla, sp, ep, total_amt
            )
        # 全 A 等权基准自身
        b = alla[(alla.index >= sp) & (alla.index <= ep)]
        stage["bench"] = {
            "cum_return": float((1 + b).prod() - 1.0),
            "months": int(len(b)),
        }
        result[name] = stage
    return result


def compute_industry_stages(p: pd.DataFrame, alla: pd.Series) -> dict:
    total_amt = p.groupby("period")["amount_m"].sum()
    inds = p["industry"].dropna().unique()
    result = {}
    for name, s, e, desc in QUANT_STAGES:
        sp, ep = pd.Period(s, "M"), pd.Period(e, "M")
        rows = {}
        for ind in inds:
            m = p["industry"].eq(ind)
            if m.sum() < 200:  # 成员观测太少跳过
                continue
            met = _group_stage_metrics(p, m, alla, sp, ep, total_amt)
            if met:
                rows[ind] = met
        result[name] = {"label": name, "desc": desc, "industries": rows}
    return result


def compute_index_overview() -> dict:
    idx_map = {
        "上证综指(000001.SH)": _read_index("000001.SH"),
        "深证成指(399001.SZ)": _read_index("399001.SZ"),
        "沪深300(000300.SH)": _read_index("000300.SH"),
    }
    out = {}
    for name, s, e, desc in INDEX_STAGES:
        sp, ep = pd.Period(s, "M"), pd.Period(e, "M")
        stage = {"label": name, "desc": desc, "indices": {}}
        for iname, series in idx_map.items():
            r = series.pct_change().dropna()
            seg = r[(r.index >= sp) & (r.index <= ep)]
            if len(seg) < 6:
                stage["indices"][iname] = {"available": False}
                continue
            nav = (1 + seg).cumprod()
            ann_vol = float(seg.std() * np.sqrt(12))
            ann_ret = float(nav.iloc[-1] ** (12.0 / len(seg)) - 1)
            stage["indices"][iname] = {
                "available": True,
                "months": int(len(seg)),
                "cum_return": float(nav.iloc[-1] - 1.0),
                "ann_return": ann_ret,
                "ann_vol": ann_vol,
                "maxdd": _max_drawdown(nav),
            }
        out[name] = stage
    return out


def main() -> None:
    panel, meta = build_panel()
    alla, csi = monthly_benchmarks(panel)
    p = assign_style_groups(panel)

    style = compute_style_stages(p, alla)
    industry = compute_industry_stages(p, alla)
    index = compute_index_overview()

    (HERE / "stage_style_metrics.json").write_text(
        json.dumps(style, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    (HERE / "stage_industry_metrics.json").write_text(
        json.dumps(industry, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    (HERE / "index_overview.json").write_text(
        json.dumps(index, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    (HERE / "panel_coverage.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8"
    )

    # 明细（复核用，不进报告上下文）
    det = HERE / "details"
    det.mkdir(exist_ok=True)
    # 保存月度面板供复核（period 转字符串以便落盘）
    save_cols = ["ts_code", "period", "ret", "ret_tr", "mktcap", "bp", "dy", "industry", "amount_m"]
    pnl = p[save_cols].copy()
    pnl["period"] = pnl["period"].astype(str)
    pnl.to_parquet(det / "panel_monthly.parquet", index=False)
    # 月度风格组净值
    nav_rows = []
    for gname, col in STYLE_GROUPS.items():
        m = p[col].fillna(False) & p["ret_tr"].notna()
        gret = p[m].groupby("period")["ret_tr"].mean()
        nav = (1 + gret).cumprod()
        for per, v in nav.items():
            nav_rows.append({"period": str(per), "group": gname, "nav": float(v)})
    pd.DataFrame(nav_rows).to_csv(det / "style_nav_monthly.csv", index=False, encoding="utf-8-sig")
    # 行业全量指标
    ind_rows = []
    for stg, d in industry.items():
        for ind, met in d["industries"].items():
            row = {"stage": stg, "industry": ind}
            row.update({k: met[k] for k in ("cum_return", "cum_excess", "breadth", "amount_share", "maxdd", "members_avg")})
            ind_rows.append(row)
    pd.DataFrame(ind_rows).to_csv(det / "industry_stage_metrics.csv", index=False, encoding="utf-8-sig")

    print("OK panel_rows=%d stocks=%d months=%d" % (meta["panel_rows"], meta["stocks"], meta["months"]))
    print("last_period=%s bp_nn=%d industry_nn=%d" % (meta["last_period"], meta["bp_nonnull"], meta["industry_nonnull"]))


if __name__ == "__main__":
    main()
