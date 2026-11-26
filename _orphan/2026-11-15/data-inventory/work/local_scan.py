"""只读数据盘点：单遍聚合 local_store 正式表，输出 work/local_stats.json。

不写 data/、不改任何正式文件；只读 parquet / sidecar JSON。
"""
import json
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import pyarrow.parquet as pq

ROOT = Path(r"d:\code\python\quant")
LS = ROOT / "data" / "local_store"
WORK = ROOT / "2026-11-15" / "data-inventory" / "work"
WORK.mkdir(parents=True, exist_ok=True)


def pct(vals):
    a = np.asarray(vals, dtype=np.int64)
    if a.size == 0:
        return {"p50": None, "p95": None, "min": None, "max": None, "n": 0}
    return {"p50": int(np.percentile(a, 50)), "p95": int(np.percentile(a, 95)),
            "min": int(a.min()), "max": int(a.max()), "n": int(a.size)}


def series_stats(path, code_col="ts_code", date_col="trade_date", cols=None):
    lf = pl.scan_parquet(path)
    if cols:
        lf = lf.select(cols)
    g = (lf.group_by(code_col)
         .agg(pl.len().alias("rows"),
              pl.col(date_col).min().alias("start"),
              pl.col(date_col).max().alias("end"),
              pl.col(date_col).n_unique().alias("uniq"))
         .collect())
    df = g.to_pandas()
    rows = int(df["rows"].sum())
    uniq = int(df["uniq"].sum())
    return {
        "rows": rows, "codes": int(len(df)),
        "start": str(df["start"].min()), "end": str(df["end"].max()),
        "dup_key": rows - uniq, "per_code": pct(df["rows"].values),
    }


out = {}

# ---------------- 交易日历 ----------------
LEDGER = LS / "meta" / "trading_calendar" / "session_ledger.parquet"
led = pd.read_parquet(LEDGER)
bench = ["000001.SH", "399001.SZ", "399002.SZ", "399003.SZ", "000300.SH"]
dates = set()
for code in bench:
    mkt = "sh" if code.endswith(".SH") else "sz"
    p = LS / "quotes" / "index" / mkt / f"{code}.parquet"
    dates |= set(pd.read_parquet(p, columns=["date"])["date"].astype(str))
dates |= set(led.loc[led["status"] == "session", "date"].astype(str))
dates -= set(led.loc[led["status"] == "closed", "date"].astype(str))
sessions = np.array(sorted(dates))
out["calendar"] = {"sessions": int(len(sessions)), "first": str(sessions[0]),
                   "latest": str(sessions[-1]), "ledger_rows": int(len(led))}
LATEST = str(sessions[-1])


def gap_in_window(start, end, rows):
    """[start,end] 内交易日数 - 实际行数（含停牌，属理论上界口径）。"""
    expect = int(np.searchsorted(sessions, end, "right") - np.searchsorted(sessions, start, "left"))
    return max(expect - rows, 0)


# ---------------- 行情（sidecar 汇总 + 基金价格扫描） ----------------
quotes = {}
for market in ("stock", "fund", "index"):
    base = LS / "quotes" / market
    files = sorted(base.glob("*/*.parquet"))
    recs = []
    miss_sidecar = 0
    for p in files:
        code = p.stem
        sc = p.with_name(p.name.replace(".parquet", ".manifest.json"))
        if sc.exists():
            j = json.loads(sc.read_text(encoding="utf-8"))
            st = j.get("stats", {})
            recs.append({"ts_code": code, "rows": int(st.get("rows", 0)),
                         "start": st.get("start"), "end": st.get("end"),
                         "dup": int(st.get("dup_dates", 0))})
        else:
            miss_sidecar += 1
            t = pq.read_table(p, columns=["date"]).column("date").to_pylist()
            ds = pd.Series(t).astype(str)
            recs.append({"ts_code": code, "rows": len(ds), "start": ds.min(),
                         "end": ds.max(), "dup": int(len(ds) - ds.nunique())})
    q = pd.DataFrame(recs)
    q["gap"] = [gap_in_window(a, b, r) for a, b, r in zip(q["start"], q["end"], q["rows"])]
    quotes[market] = {
        "files": len(files), "rows": int(q["rows"].sum()), "codes": int(q["ts_code"].nunique()),
        "start": str(q["start"].min()), "end": str(q["end"].max()),
        "dup_dates": int(q["dup"].sum()), "per_code": pct(q["rows"].values),
        "gap_rows": int(q["gap"].sum()), "gap_codes": int((q["gap"] > 0).sum()),
        "gap_p50": pct(q.loc[q["gap"] > 0, "gap"].values),
        "miss_sidecar": miss_sidecar,
        "code_set": set(q["ts_code"]),
    }

# 基金恒 1.0 占位检测（只读 close/volume）
const1 = []
for p in sorted((LS / "quotes" / "fund").glob("*/*.parquet")):
    df = pl.read_parquet(p, columns=["close", "volume"])
    if df.height and df["close"].max() == 1.0 and df["close"].min() == 1.0:
        const1.append((p.stem, df.height, int(df["volume"].max())))
quotes["fund"]["const1_codes"] = len(const1)
quotes["fund"]["const1_zero_vol"] = sum(1 for _, _, v in const1 if v == 0)
quotes["fund"]["const1_rows"] = sum(r for _, r, _ in const1)

# ---------------- 基础名录 ----------------
def basic_stats(path, date_col, extra=None):
    df = pd.read_parquet(path)
    r = {"rows": int(len(df)), "codes": int(df["ts_code"].nunique()),
         "dup_key": int(len(df) - df["ts_code"].nunique()),
         "start": str(df[date_col].min()), "end": str(df[date_col].max())}
    if extra:
        r.update(extra(df))
    return r


sb = pd.read_parquet(LS / "meta" / "stock_basic.parquet")
fb = pd.read_parquet(LS / "meta" / "fund_basic.parquet")
ib = pd.read_parquet(LS / "meta" / "index_basic.parquet")
basic = {
    "stock_basic": basic_stats(LS / "meta" / "stock_basic.parquet", "list_date",
                               lambda d: {"status": d["status"].value_counts().to_dict(),
                                          "delist_filled": int(d["delist_date"].notna().sum())}),
    "fund_basic": basic_stats(LS / "meta" / "fund_basic.parquet", "list_date",
                              lambda d: {"status": d["status"].value_counts().to_dict()}),
    "index_basic": basic_stats(LS / "meta" / "index_basic.parquet", "list_date"),
}
out["basic"] = basic

# ---------------- 复权因子 / 分红 / 换手 / 股本 ----------------
F = LS / "factors"
factors = {
    "stock_adj_factor": series_stats(F / "stock_adj_factor.parquet"),
    "fund_adj_factor": series_stats(F / "fund_adj_factor.parquet"),
    "stock_turnover": series_stats(F / "stock_turnover.parquet"),
    "stock_shares": series_stats(LS / "meta" / "factors" / "stock_shares.parquet",
                                 date_col="effective_date", cols=["ts_code", "effective_date"]),
}
div = pd.read_parquet(F / "stock_dividend.parquet",
                      columns=["ts_code", "ex_date", "total_div", "cash_div", "rights_issue_ratio"])
factors["stock_dividend"] = {
    "rows": int(len(div)), "codes": int(div["ts_code"].nunique()),
    "start": str(div["ex_date"].min()), "end": str(div["ex_date"].max()),
    "dup_key": int(len(div) - len(div.drop_duplicates(subset=["ts_code", "ex_date"]))),
    "per_code": pct(div.groupby("ts_code").size().values),
    "total_div_nonzero": int((div["total_div"].fillna(0) != 0).sum()),
    "total_div_null": int(div["total_div"].isna().sum()),
    "total_div_sum": float(div["total_div"].fillna(0).sum()),
    "cash_div_nonzero": int((div["cash_div"].fillna(0) != 0).sum()),
}
fd = pd.read_parquet(F / "fund_dividend.parquet")
factors["fund_dividend"] = {
    "rows": int(len(fd)), "codes": int(fd["ts_code"].nunique()),
    "start": str(fd["ex_date"].min()), "end": str(fd["ex_date"].max()),
    "dup_key": int(len(fd) - len(fd.drop_duplicates(["ts_code", "ex_date"]))),
    "per_code": pct(fd.groupby("ts_code").size().values),
}
out["factors"] = factors

# ---------------- 财务 / 估值 ----------------
FIN = LS / "meta" / "financials"
financials = {
    "balance": series_stats(FIN / "balance.parquet", date_col="end_date"),
    "income": series_stats(FIN / "income.parquet", date_col="end_date"),
    "cashflow": series_stats(FIN / "cashflow.parquet", date_col="end_date"),
    "valuation_daily": series_stats(FIN / "valuation_daily.parquet"),
    "ann_date_map": series_stats(FIN / "ann_date_map.parquet", code_col="symbol", date_col="ann_date"),
}
out["financials"] = financials

# ---------------- 事件 ----------------
ev = pd.read_parquet(LS / "meta" / "events" / "events.parquet")
ev["_y"] = pd.to_datetime(ev["publish_time"], errors="coerce").dt.year
out["events"] = {
    "rows": int(len(ev)), "codes": int(ev["ts_code"].nunique()),
    "start": str(ev["publish_time"].min()), "end": str(ev["publish_time"].max()),
    "dup_key": int(len(ev) - ev["event_id"].nunique()),
    "per_code": pct(ev.groupby("ts_code").size().values),
    "type_counts": ev["event_type"].value_counts().head(8).to_dict(),
    "year_counts": {str(int(k)): int(v) for k, v in ev["_y"].value_counts().sort_index().items()},
}

# ---------------- ST ----------------
st = pd.read_parquet(LS / "meta" / "st" / "st_intervals.parquet")
stc = pd.read_parquet(LS / "meta" / "st" / "st_coverage.parquet")
sth = pd.read_parquet(LS / "meta" / "st_status" / "st_status_history.parquet")
out["st"] = {
    "intervals_rows": int(len(st)), "intervals_codes": int(st["ts_code"].nunique()),
    "start": str(st["start_date"].min()), "end": str(st["end_date"].max()),
    "flag_counts": st["flag"].value_counts().to_dict(),
    "coverage_rows": int(len(stc)), "coverage_codes": int(stc["ts_code"].nunique()),
    "status_rows": int(len(sth)), "status_codes": int(sth["code"].nunique()),
}

# ---------------- 广度 ----------------
br = pd.read_parquet(LS / "meta" / "breadth" / "breadth_daily.parquet")
out["breadth"] = {
    "rows": int(len(br)), "start": str(br["trade_date"].min()), "end": str(br["trade_date"].max()),
    "dup_key": int(len(br) - br["trade_date"].nunique()),
}

# ---------------- 已知结论复核 ----------------
SB = set(sb["ts_code"])
stock_codes = quotes["stock"]["code_set"]
fund_codes = quotes["fund"]["code_set"]
index_codes = quotes["index"]["code_set"]
f_adj_codes = set(pl.scan_parquet(F / "stock_adj_factor.parquet").select("ts_code").unique().collect()["ts_code"].to_list())
# 因子末端缺口：因子末日 vs 行情末日（stock）
fac_end = (pl.scan_parquet(F / "stock_adj_factor.parquet").group_by("ts_code")
           .agg(pl.col("trade_date").max().alias("fac_end")).collect().to_pandas())
# 用 daily_index 作为逐码末日来源（与 sidecar 同源，已交叉核对）
di = pd.read_parquet(LS / "meta" / "daily_index.parquet")
di["end"] = di["end"].astype(str)
qs = di[di["data_type"] == "stock"][["ts_code", "end", "rows"]].rename(columns={"end": "q_end"})
m = qs.merge(fac_end, on="ts_code", how="left")
m["fac_end"] = m["fac_end"].astype(str)
gapped = m[(m["fac_end"] == "None") | (m["fac_end"] < m["q_end"])]
deficit = 0
for fe, qe in zip(gapped["fac_end"], gapped["q_end"]):
    if fe == "None":
        continue
    deficit += int(np.searchsorted(sessions, qe, "right") - np.searchsorted(sessions, fe, "right"))
out["known"] = {
    "placeholder_const1_funds": quotes["fund"]["const1_codes"],
    "placeholder_const1_zero_vol": quotes["fund"]["const1_zero_vol"],
    "placeholder_const1_rows": quotes["fund"]["const1_rows"],
    "stale_codes": int(pd.read_parquet(LS / "meta" / "universe" / "stale_status.parquet")["is_stale"].sum()),
    "no_quote_stock_codes": len(SB - stock_codes),
    "quote_not_in_basic_stock": len(stock_codes - SB),
    "factor_tail_codes": int(len(gapped)),
    "factor_tail_rows": deficit,
    "total_div_nonzero": factors["stock_dividend"]["total_div_nonzero"],
    "event_years": out["events"]["year_counts"],
    "st_no_record_stocks": len(SB - set(st["ts_code"])),
}
# 无法直接复核的已知结论另附原始清单规模
for name in ("placeholder_quote_codes", "source_stale_codes", "factor_tail_lag_codes", "no_local_quote_codes"):
    p = LS / "meta" / "anomalies" / f"{name}.csv"
    out["known"][name + "_rows"] = int(len(pd.read_csv(p)))

# ---------------- 跨表一致性 ----------------
out["cross"] = {
    "stock_quote_not_in_basic": len(stock_codes - SB),
    "basic_no_quote": len(SB - stock_codes),
    "fund_quote_not_in_basic": len(fund_codes - set(fb["ts_code"])),
    "fund_basic_no_quote": len(set(fb["ts_code"]) - fund_codes),
    "index_quote_not_in_basic": len(index_codes - set(ib["ts_code"])),
    "index_basic_no_quote": len(set(ib["ts_code"]) - index_codes),
    "stock_quote_no_factor": len(stock_codes - f_adj_codes),
    "factor_no_quote": len(f_adj_codes - stock_codes),
    "dividend_no_basic": len(set(div["ts_code"]) - SB),
    "valuation_no_basic": None,  # 由下方填充
    "event_no_basic": len(set(ev["ts_code"]) - SB),
    "st_no_basic": len(set(st["ts_code"]) - SB),
}
val_codes = set(pl.scan_parquet(FIN / "valuation_daily.parquet").select("ts_code").unique().collect()["ts_code"].to_list())
out["cross"]["valuation_no_basic"] = len(val_codes - SB)
out["cross"]["valuation_codes"] = len(val_codes)

# ---------------- 新鲜度 ----------------
def lag_days(d):
    if d is None or str(d) == "None":
        return None
    return int(np.searchsorted(sessions, LATEST, "right") - np.searchsorted(sessions, str(d), "right"))


fresh = {}
for k in ("stock", "fund", "index"):
    fresh[k + "_daily"] = {"latest": quotes[k]["end"], "lag_sessions": lag_days(quotes[k]["end"])}
fresh["stock_adj_factor"] = {"latest": factors["stock_adj_factor"]["end"],
                             "lag_sessions": lag_days(factors["stock_adj_factor"]["end"])}
fresh["fund_adj_factor"] = {"latest": factors["fund_adj_factor"]["end"],
                            "lag_sessions": lag_days(factors["fund_adj_factor"]["end"])}
fresh["stock_dividend"] = {"latest": factors["stock_dividend"]["end"],
                           "lag_sessions": lag_days(factors["stock_dividend"]["end"])}
fresh["valuation_daily"] = {"latest": financials["valuation_daily"]["end"],
                            "lag_sessions": lag_days(financials["valuation_daily"]["end"])}
fresh["balance"] = {"latest": financials["balance"]["end"],
                    "lag_sessions": lag_days(financials["balance"]["end"])}
fresh["income"] = {"latest": financials["income"]["end"],
                   "lag_sessions": lag_days(financials["income"]["end"])}
fresh["cashflow"] = {"latest": financials["cashflow"]["end"],
                     "lag_sessions": lag_days(financials["cashflow"]["end"])}
fresh["events"] = {"latest": out["events"]["end"], "lag_sessions": lag_days(str(out["events"]["end"])[:10])}
fresh["st_intervals"] = {"latest": out["st"]["end"], "lag_sessions": lag_days(out["st"]["end"])}
fresh["breadth"] = {"latest": out["breadth"]["end"], "lag_sessions": lag_days(out["breadth"]["end"])}
out["freshness"] = fresh
out["latest_trading_day"] = LATEST

# set 不可 JSON 序列化，剔除
for k in ("stock", "fund", "index"):
    quotes[k].pop("code_set", None)
out["quotes"] = quotes
path = WORK / "local_stats.json"
path.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
print("wrote", path)
print(json.dumps({k: out[k] for k in ("calendar", "known", "cross")}, ensure_ascii=False, indent=1))
