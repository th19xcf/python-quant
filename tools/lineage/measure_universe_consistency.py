"""实测：L5 池 vs src/data/universe_filter.filter_universe 的一致性（纯只读）。

抽样 3 个信号日，输入集合统一取 L2 可交易，比较两边保留集合。
输出 outputs/universe_consistency.json
"""
from __future__ import annotations

import json
import sys
import time
from datetime import date
from pathlib import Path

import polars as pl

ROOT = Path(r"e:\code\python\quant")
sys.path.insert(0, str(ROOT))
OUT = ROOT / "2026-11-12" / "data-lineage" / "outputs"
OUT.mkdir(parents=True, exist_ok=True)

from src.data.universe_filter import filter_universe  # noqa: E402

LAYERS = ROOT / "data/local_store/meta/universe/layers"
DATES = [date(2026, 11, 11), date(2024, 6, 28), date(2021, 6, 30)]


def layer_set(layer: str, day: date) -> set[str]:
    f = LAYERS / f"{layer}.parquet"
    df = pl.read_parquet(f).filter((pl.col("date") == day) & pl.col("in_layer"))
    return set(df["ts_code"].to_list())


records = []
for day in DATES:
    t0 = time.time()
    l2 = layer_set("l2_tradable", day)
    l5 = layer_set("l5_pool", day)
    l4 = layer_set("l4_liquid", day)
    res = filter_universe(day, sorted(l2))
    kept = set(res.kept)
    both = kept & l5
    only_filter = kept - l5
    only_l5 = l5 - kept
    rec = {
        "date": day.isoformat(),
        "l2_tradable": len(l2),
        "l4_liquid": len(l4),
        "l5_pool": len(l5),
        "filter_input": res.input_count,
        "filter_kept": len(kept),
        "intersection": len(both),
        "only_in_filter": len(only_filter),
        "only_in_l5": len(only_l5),
        "jaccard": round(len(both) / max(1, len(kept | l5)), 4),
        "filter_rule_counts": res.rule_counts,
        "sample_only_in_filter": sorted(only_filter)[:20],
        "sample_only_in_l5": sorted(only_l5)[:20],
        "elapsed_s": round(time.time() - t0, 1),
    }
    records.append(rec)
    print(json.dumps({k: v for k, v in rec.items() if not k.startswith("sample")}, ensure_ascii=False))

# 被 filter 剔除的 L5 成员，按规则归类（用于解释差异方向）
res_last = filter_universe(DATES[0], sorted(layer_set("l2_tradable", DATES[0])))
l5_last = layer_set("l5_pool", DATES[0])
reason_map = {}
for rule, codes in res_last.dropped.items():
    hit = sorted(set(codes) & l5_last)
    if hit:
        reason_map[rule] = {"count": len(hit), "sample": hit[:15]}
print("L5 成员被 filter 剔除的规则分布：", json.dumps(reason_map, ensure_ascii=False)[:1200])

payload = {"dates": [d.isoformat() for d in DATES], "records": records,
           "l5_members_dropped_by_filter": reason_map}
(OUT / "universe_consistency.json").write_text(
    json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8"
)
print("saved", OUT / "universe_consistency.json")
