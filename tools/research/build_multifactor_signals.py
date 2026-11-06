#!/usr/bin/env python3
"""个股多因子月末信号构建（归档自 reports/stock_multi_factor_v2_20261105/build_signals_v2.py）。

本脚本只做信号，不跑回测；驱动脚本是 run_multifactor_v2.py。

因子集（7 因子，方向已按上游 IC 实测统一）
- amount_ma20   流动性/成交额，方向 -1（成交额越高越差）
- volatility_20 波动率，方向 -1
- ret_20        20 日反转，方向 -1
- ff_div_yield  股息率，方向 +1（PIT 事件法覆写）
- ff_value_bp   账面市值比，方向 +1（可用起点 2018-01）
- accruals      应计，方向 -1（可用起点 2017-01）
- sue           标准化盈余意外，方向 +1（可用起点 2018-07）
- turnover_20 未加入：正交化后仅 +0.066，已被现有簇吸收。

处理链：行业 + 市值中性化（FWL）→ 1%/99% winsorize → z-score → 截断 ±3
→ 方向统一 → 等权/ICIR 加权（按当月实际可用因子归一）→ 总分。
PIT 股息覆写必须排在标准化/中性化之前；缺失因子不回填、不使用未来值；
MIN_FACTORS=3，当月可用因子不足 3 个的股票不打分。

依赖数据集（均可用 CLI 覆盖）：
- analysis_monthly.parquet（默认 data/local_store/meta/research_inputs/，
  归档副本；含 ts_code/date/fwd_ret/ln_mv/ind_code 与 5 个基础因子）
- new_factors.parquet（默认同一目录；含 accruals/sue）
- panel.parquet（默认 reports/stock_momentum_20261105/data/panel.parquet，约 1.25GB，
  未入库；仅取月末 adj_close/raw_close/vol60/adv20/row_no/valid_252）
- strict_whitelist.parquet（默认 reports/stock_momentum_20261105/data/，未入库）
- data/local_store/factors/stock_dividend.parquet（PIT 股息事件表）
- data/local_store/meta/financials/valuation_daily.parquet（total_mv）

2015-2016 只有 3 个价量因子 + 股息（ff_value_bp/accruals/sue 全缺），
2017 年均 4.70 个，2018-2022 年均 6.63 个，OOS 2023-2026 年均 6.90 个。
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data" / "local_store"
RESEARCH_INPUTS = DATA_DIR / "meta" / "research_inputs"
DEFAULT_ANALYSIS = RESEARCH_INPUTS / "analysis_monthly.parquet"
DEFAULT_NEW_FACTORS = RESEARCH_INPUTS / "new_factors.parquet"
DEFAULT_PANEL = REPO_ROOT / "reports" / "stock_momentum_20261105" / "data" / "panel.parquet"
DEFAULT_WHITELIST = (REPO_ROOT / "reports" / "stock_momentum_20261105" / "data"
                     / "strict_whitelist.parquet")
DEFAULT_DIVIDEND = DATA_DIR / "factors" / "stock_dividend.parquet"
DEFAULT_VALUATION = DATA_DIR / "meta" / "financials" / "valuation_daily.parquet"
DEFAULT_OUT_DIR = REPO_ROOT / "reports" / "stock_multi_factor_v2_20261105" / "signals"

FACTORS = {
    "amount_ma20": -1.0,
    "volatility_20": -1.0,
    "ret_20": -1.0,
    "ff_div_yield": 1.0,
    "ff_value_bp": 1.0,
    "accruals": -1.0,
    "sue": 1.0,
}
MIN_FACTORS = 3
ICIR_WINDOW = 36
ICIR_MIN = 12
W_LO, W_HI = 0.1, 0.4


def _neutralize(df: pl.DataFrame, col: str) -> pl.Series:
    """行业 + ln_mv 中性化残差（FWL）。"""
    f = pl.col(col).cast(pl.Float64)
    x = pl.col("ln_mv").cast(pl.Float64)
    f_dm = f - f.mean().over("ind_code")
    x_dm = x - x.mean().over("ind_code")
    beta = (f_dm * x_dm).sum().over("date") / (x_dm * x_dm).sum().over("date")
    return (f_dm - beta * x_dm).alias(col)


def _zscore(df: pl.DataFrame, col: str) -> pl.Series:
    x = pl.col(col).cast(pl.Float64)
    lo = x.quantile(0.01).over("date")
    hi = x.quantile(0.99).over("date")
    xw = x.clip(lo, hi)
    mu = xw.mean().over("date")
    sd = xw.std().over("date")
    z = pl.when(sd > 0).then((xw - mu) / sd).otherwise(None)
    return z.clip(-3.0, 3.0).alias(col)


def _spearman_ic(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """逐月 rank IC（因子 vs fwd_ret），仅用于 ICIR 权重，不含未来数据。"""
    d = df.filter(pl.col(col).is_not_null() & pl.col("fwd_ret").is_not_null())
    return d.group_by("date").agg(
        pl.corr(pl.col(col).rank(), pl.col("fwd_ret").rank()).alias(f"ic_{col}")
    ).sort("date")


def build_signals(analysis: Path = DEFAULT_ANALYSIS,
                  new_factors: Path = DEFAULT_NEW_FACTORS,
                  panel: Path = DEFAULT_PANEL,
                  whitelist: Path = DEFAULT_WHITELIST,
                  dividend: Path = DEFAULT_DIVIDEND,
                  valuation: Path = DEFAULT_VALUATION,
                  out_dir: Path = DEFAULT_OUT_DIR) -> int:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cols = ["ts_code", "date", "fwd_ret", "ln_mv", "ind_code",
            "amount_ma20", "volatility_20", "ret_20", "ff_div_yield", "ff_value_bp"]
    am = pl.read_parquet(analysis, columns=cols)
    # 并入 accruals / sue（可用起点分别 2017-01、2018-07，缺失不回填）
    nf = pl.read_parquet(new_factors, columns=["ts_code", "date", "accruals", "sue"])
    am = am.join(nf, on=["ts_code", "date"], how="left")
    wl = pl.read_parquet(whitelist)

    # PIT：只保留当月 strict 白名单
    df = am.join(wl, on=["ts_code", "date"], how="inner").sort(["date", "ts_code"])
    print(f"analysis={am.height} whitelist-joined={df.height} months={df['date'].n_unique()}",
          flush=True)

    # 并回面板月末价格/流动性字段（须在因子标准化之前，股息 PIT 依赖 raw_close）
    me = df["date"].unique().sort().to_list()
    panel_df = pl.read_parquet(panel).filter(pl.col("date").is_in(me)).select(
        "ts_code", "date", "adj_close", "raw_close", "vol60", "adv20", "row_no", "valid_252")
    df = df.join(panel_df, on=["ts_code", "date"], how="left")

    # 股息率 PIT 重算：stock_dividend 事件法，滚动 365 天（ex_date 落在窗口内才计入）
    # 必须在标准化/中性化之前覆写，否则 2015-2017 的合成分数用不到事件法股息。
    div = (pl.read_parquet(dividend, columns=["ts_code", "ex_date", "cash_div"])
           .drop_nulls().filter(pl.col("cash_div") > 0)
           .with_columns(pl.col("ex_date").cast(pl.Date))
           .sort(["ts_code", "ex_date"])
           .with_columns(pl.col("cash_div").cum_sum().over("ts_code").alias("cum")))
    grid = df.select("ts_code", "date", "raw_close").drop_nulls("raw_close").unique().sort(
        ["ts_code", "date"])
    ev = div.select("ts_code", pl.col("ex_date").alias("date"), "cum")
    now = grid.join_asof(ev, on="date", by="ts_code", strategy="backward")
    prev = (grid.with_columns((pl.col("date") - pl.duration(days=365)).alias("date"))
            .join_asof(ev, on="date", by="ts_code", strategy="backward")
            .select("ts_code", "date", pl.col("cum").alias("cum_prev"))
            .sort(["ts_code", "date"]))
    grid = now.join(prev, on=["ts_code", "date"], how="left", coalesce=True)
    grid = grid.with_columns(
        ((pl.col("cum").fill_null(0.0) - pl.col("cum_prev").fill_null(0.0))
         / pl.col("raw_close") * 100.0).alias("div_yield_pit"))
    df = df.join(grid.select("ts_code", "date", "div_yield_pit"),
                 on=["ts_code", "date"], how="left")
    df = df.with_columns(
        pl.when(pl.col("div_yield_pit").is_not_null())
        .then(pl.col("div_yield_pit") / 100.0)
        .otherwise(pl.col("ff_div_yield")).alias("ff_div_yield"))

    # 中性化 + 标准化（保留未中性化副本用于覆盖率披露）
    for c in FACTORS:
        df = df.with_columns(_neutralize(df, c))
    for c in FACTORS:
        df = df.with_columns(_zscore(df, c).alias(f"z_{c}"))
    df = df.with_columns([(pl.col(f"z_{c}") * s).alias(f"d_{c}") for c, s in FACTORS.items()])

    # IC 序列（中性化因子 vs 未来 1 月收益）
    ics = []
    for c in FACTORS:
        ics.append(_spearman_ic(df, c))
    ic = ics[0]
    for x in ics[1:]:
        ic = ic.join(x, on="date", how="full", coalesce=True)
    ic = ic.sort("date")
    ic.write_csv(out_dir / "monthly_ic.csv")

    # 逐月 ICIR 权重：t 月只用 t-1 及之前（fwd_ret 已实现）
    rows = []
    dates = ic["date"].to_list()
    for i, d in enumerate(dates):
        hist = ic.slice(max(0, i - ICIR_WINDOW), min(i, ICIR_WINDOW))
        w = {}
        for c in FACTORS:
            s = hist[f"ic_{c}"].drop_nulls()
            if s.len() >= ICIR_MIN and s.std() > 0:
                w[c] = float(s.mean() / s.std())
            else:
                w[c] = None
        valid = {k: abs(v) for k, v in w.items() if v is not None}
        if not valid:
            rows.append(dict(date=d, **{f"w_{c}": 1.0 / len(FACTORS) for c in FACTORS},
                             icir_ready=False))
            continue
        tot = sum(valid.values())
        raw = {k: v / tot for k, v in valid.items()}
        # 截断到 [0.1, 0.4]，缺失因子记 0，再归一
        clipped = {k: min(W_HI, max(W_LO, raw.get(k, 0.0))) for k in FACTORS}
        s = sum(clipped.values())
        rows.append(dict(date=d, **{f"w_{c}": clipped[c] / s for c in FACTORS},
                         icir_ready=True))
    wdf = pl.DataFrame(rows)
    wdf.write_csv(out_dir / "icir_weights.csv")

    # 等权与 ICIR 加权总分：只对当月可用因子归一
    cnt = sum([pl.col(f"z_{c}").is_not_null().cast(pl.Int32) for c in FACTORS])
    eq_num = sum([pl.col(f"d_{c}").fill_null(0.0) for c in FACTORS])
    eq_den = sum([pl.when(pl.col(f"z_{c}").is_not_null()).then(1.0).otherwise(0.0)
                  for c in FACTORS])
    df = df.with_columns(cnt.alias("n_factor"))
    df = df.with_columns(
        pl.when(pl.col("n_factor") >= MIN_FACTORS).then(eq_num / eq_den).otherwise(None)
        .alias("score_a"))
    df = df.join(wdf, on="date", how="left")
    a2_num = sum([(pl.col(f"d_{c}") * pl.col(f"w_{c}")).fill_null(0.0) for c in FACTORS])
    a2_den = sum([pl.when(pl.col(f"z_{c}").is_not_null()).then(pl.col(f"w_{c}"))
                  .otherwise(0.0) for c in FACTORS])
    df = df.with_columns(
        pl.when(pl.col("n_factor") >= MIN_FACTORS).then(a2_num / a2_den).otherwise(None)
        .alias("score_a2"))

    vals = (
        pl.read_parquet(valuation)
        .select("ts_code", "trade_date", "total_mv").drop_nulls()
        .unique(subset=["ts_code", "trade_date"], keep="last").sort(["ts_code", "trade_date"])
    )
    s = df.select("ts_code", "date").sort(["ts_code", "date"])
    j = s.join_asof(vals, left_on="date", right_on="trade_date", by="ts_code",
                    strategy="backward")
    df = df.join(j, on=["ts_code", "date"], how="left").with_columns(
        (pl.col("total_mv") / 1e8).alias("total_mv_yi"))

    keep = ["ts_code", "date", "score_a", "score_a2", "n_factor",
            "adv20", "vol60", "total_mv_yi", "adj_close", "row_no", "valid_252",
            "ln_mv", "ind_code", *[f"z_{c}" for c in FACTORS]]
    out = df.select(keep).sort(["date", "ts_code"])
    out.write_parquet(out_dir / "signals.parquet")

    # 覆盖率（strict 白名单内，按年；ff_div_yield 为事件法重算后的值）
    cov = []
    d2 = df.with_columns(pl.col("date").dt.year().alias("year"))
    for y, part in d2.group_by("year", maintain_order=True):
        rec = {"year": int(y[0]), "rows": part.height}
        for c in FACTORS:
            rec[c] = round(1 - part[c].null_count() / part.height, 4)
        cov.append(rec)
    pl.DataFrame(cov).write_csv(out_dir / "coverage_by_year.csv")

    # 行业分组（含 unknown 处理披露）
    ind = df.group_by("ind_code").agg(pl.len().alias("rows")).sort("rows", descending=True)
    ind.write_csv(out_dir / "industry_groups.csv")

    meta = {
        "rows": out.height, "months": out["date"].n_unique(),
        "start": str(out["date"].min()), "end": str(out["date"].max()),
        "factors": FACTORS, "min_factors": MIN_FACTORS,
        "score_a_nonnull": int(out["score_a"].is_not_null().sum()),
        "score_a2_nonnull": int(out["score_a2"].is_not_null().sum()),
        "factors_available": out.group_by("date").agg(
            pl.col("n_factor").mean().alias("mean_n")).sort("date").to_dicts(),
    }
    (out_dir / "signal_manifest.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps({k: v for k, v in meta.items() if k != "factors_available"},
                     ensure_ascii=False, indent=2), flush=True)
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="构建个股多因子月末信号（7 因子）")
    ap.add_argument("--analysis", type=Path, default=DEFAULT_ANALYSIS)
    ap.add_argument("--new-factors", type=Path, default=DEFAULT_NEW_FACTORS)
    ap.add_argument("--panel", type=Path, default=DEFAULT_PANEL)
    ap.add_argument("--whitelist", type=Path, default=DEFAULT_WHITELIST)
    ap.add_argument("--dividend", type=Path, default=DEFAULT_DIVIDEND)
    ap.add_argument("--valuation", type=Path, default=DEFAULT_VALUATION)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    a = ap.parse_args(argv)
    return build_signals(a.analysis, a.new_factors, a.panel, a.whitelist,
                         a.dividend, a.valuation, a.out_dir)


if __name__ == "__main__":
    raise SystemExit(main())
