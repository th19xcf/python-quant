"""按表名 stemming 就近搜索写调用，补齐 helpers 间接写导致的漏检。"""
import json
import os
import re
from pathlib import Path

ROOT = Path(r"e:\code\python\quant")
WORK = ROOT / "2026-11-12" / "data-lineage" / "work"

WRITE = re.compile(
    r"\b(write_parquet_atomic|write_parquet|to_parquet|sink_parquet|_write_frame|write_frame|atomic_write)\b"
)

STEMS = {
    "meta/st/st_intervals.parquet": "st_intervals",
    "meta/st/st_coverage.parquet": "st_coverage",
    "meta/st/st_price_inferred.parquet": "st_price_inferred",
    "meta/st/supplement/supplement_intervals.parquet": "supplement_intervals",
    "meta/suspension/suspension_daily.parquet": "suspension_daily",
    "meta/suspension/implied_suspension.parquet": "implied_suspension",
    "meta/industry_pit/industry_pit_sw.parquet": "industry_pit_sw",
    "meta/industry_pit/industry_pit.parquet": "industry_pit",
    "meta/industry_pit/industry_change_pit.parquet": "industry_change_pit",
    "meta/events/events.parquet": "events.parquet",
    "meta/events/proxy/equity_issue_proxy.parquet": "equity_issue_proxy",
    "meta/events/proxy/unlock_proxy.parquet": "unlock_proxy",
    "meta/universe/stale_status.parquet": "stale_status",
    "meta/universe/listing_intervals.parquet": "listing_intervals",
    "meta/universe/layers/l0_all_history.parquet": "l0_all_history",
    "meta/universe/universe_exclusions.parquet": "universe_exclusions",
    "meta/universe/universe_price_jumps.parquet": "universe_price_jumps",
    "meta/breadth/breadth_daily.parquet": "breadth_daily",
    "meta/breadth/breadth_ma_daily.parquet": "breadth_ma_daily",
    "meta/stock_basic.parquet": "stock_basic",
    "meta/name_history.parquet": "name_history",
    "meta/names.json": "names.json",
    "meta/code_migration.parquet": "code_migration",
    "meta/index_members/membership_intervals.parquet": "membership_intervals",
    "meta/index_members/index_weight_monthly.parquet": "index_weight_monthly",
    "meta/financials/valuation_daily.parquet": "valuation_daily",
    "meta/financials/ann_date_map.parquet": "ann_date_map",
    "meta/financials/income.parquet": "income.parquet",
    "meta/factors/stock_shares.parquet": "stock_shares",
    "factors/stock_adj_factor.parquet": "stock_adj_factor",
    "factors/stock_dividend.parquet": "stock_dividend",
    "factors/stock_turnover.parquet": "stock_turnover",
    "factors/fund_adj_factor.parquet": "fund_adj_factor",
    "factors/fund_dividend.parquet": "fund_dividend",
    "factors/fund_share_change.parquet": "fund_share_change",
    "meta/macro/cn_treasury_yield_10y.parquet": "cn_treasury_yield_10y",
    "meta/macro/cn_market_margin_daily.parquet": "cn_market_margin_daily",
    "meta/state/state_vector_daily.parquet": "state_vector_daily",
    "meta/multi_asset/futures_continuous.parquet": "futures_continuous",
    "meta/multi_asset/etf_pool.parquet": "etf_pool",
    "meta/pools/etf_equity_pool.csv": "etf_equity_pool",
    "meta/research_inputs/analysis_monthly.parquet": "analysis_monthly",
    "meta/research_inputs/new_factors.parquet": "new_factors",
    "meta/daily_index.parquet": "daily_index",
    "meta/fund_corporate_actions_2019plus.parquet": "fund_corporate_actions",
}


def files():
    for base in ("src", "tools"):
        for dp, dn, fn in os.walk(ROOT / base):
            dn[:] = [d for d in dn if d != "__pycache__"]
            for f in fn:
                if f.endswith(".py"):
                    yield Path(dp) / f


ALL = list(files())
TEXT = {}
for p in ALL:
    try:
        TEXT[p] = p.read_text(encoding="utf-8", errors="ignore").split("\n")
    # errors="ignore" 之后只剩 OSError；单个文件读不到即跳过，不中断候选扫描。
    except OSError:  # noqa: S110
        pass

result = {}
for table, stem in STEMS.items():
    hits = []
    for p, lines in TEXT.items():
        for i, line in enumerate(lines):
            if stem not in line:
                continue
            window = lines[max(0, i - 14) : i + 14]
            wr = [
                (j + max(0, i - 14) + 1, w)
                for j, w in enumerate(window)
                for w in [m.group(0) for m in WRITE.finditer(w)]
            ]
            if wr:
                rel = str(p.relative_to(ROOT)).replace("\\", "/")
                hits.append({"file": rel, "line": i + 1, "src": line.strip()[:110], "write_near": wr[:3]})
                break
    result[table] = hits

(WORK / "writer_candidates.json").write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
for t, hits in result.items():
    print("##", t)
    for h in hits[:6]:
        print("   ", h["file"], h["line"], "|", h["src"][:80], "|", h["write_near"])
