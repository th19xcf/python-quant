"""最终表清单：local_store 下所有非单票 parquet（含 CSV 清单表）+ 行情族聚合。

输出：
  work/inventory_full.json
  work/inventory_full.csv
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pyarrow.parquet as pq

ROOT = Path(r"e:\code\python\quant")
LS = ROOT / "data" / "local_store"
WORK = ROOT / "2026-11-12" / "data-lineage" / "work"
OUT = ROOT / "2026-11-12" / "data-lineage" / "outputs"

per_code_dirs = [
    "quotes/stock/sh",
    "quotes/stock/sz",
    "quotes/stock/bj",
    "quotes/index/sh",
    "quotes/index/sz",
    "quotes/index/bj",
    "quotes/fund/sh",
    "quotes/fund/sz",
    "quotes/futures/cffex",
    "quotes/futures/shfe",
    "quotes/futures/ine",
]

manifests = {}
for p in LS.rglob("*manifest*.json"):
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        continue
    if isinstance(d, dict) and d.get("path"):
        key = str(d["path"]).replace("data/local_store/", "").replace("\\", "/")
        manifests.setdefault(key, d)

rows = []
for p in sorted(LS.rglob("*.parquet")):
    rel = str(p.relative_to(LS)).replace("\\", "/")
    if any(rel.startswith(d + "/") for d in per_code_dirs):
        continue
    try:
        meta = pq.ParquetFile(p).metadata
        nrows = meta.num_rows
        cols = meta.schema.names
    except Exception:
        nrows, cols = None, []
    m = manifests.get(rel) or manifests.get(rel.replace(".parquet", "")) or {}
    stem = p.name.replace(".parquet", "")
    sidecar = (p.parent / f"{stem}.manifest.json").is_file()
    dir_manifest = p.parent / "manifest.json"
    dir_hit = False
    dir_manifest_text = ""
    if dir_manifest.is_file():
        dir_manifest_text = dir_manifest.read_text(encoding="utf-8", errors="ignore")
        dir_hit = p.name in dir_manifest_text
    if m:
        mkind = "declared"
    elif sidecar:
        mkind = "sidecar"
    elif dir_hit:
        mkind = "dir_sibling"
    else:
        mkind = "none"
    rows.append(
        {
            "semantic_dir": str(Path(rel).parent).replace("\\", "/"),
            "rel": rel,
            "rows": nrows,
            "bytes": p.stat().st_size,
            "columns": len(cols),
            "manifest": mkind != "none",
            "manifest_kind": mkind,
            "manifest_rows": m.get("rows"),
            "manifest_rows_match": (m.get("rows") == nrows) if m.get("rows") is not None else None,
            "produced_by": m.get("produced_by"),
            "semantics": m.get("semantics"),
            "generated_at": m.get("generated_at"),
        }
    )

# 行情族聚合（用 daily_index 的逐票 rows/start/end，期货用文件元数据）
import polars as pl

di = pl.read_parquet(LS / "meta/daily_index.parquet")
fam = (
    di.group_by("data_type")
    .agg(
        pl.len().alias("codes"),
        pl.col("rows").sum().alias("rows"),
        pl.col("start").min().alias("start"),
        pl.col("end").max().alias("end"),
        pl.col("file_size").sum().alias("bytes"),
    )
    .sort("data_type")
)
families = fam.to_dicts()
for f in families:
    f["start"] = str(f["start"])
    f["end"] = str(f["end"])
    f["in_daily_index"] = True

fut_files = [p for d in per_code_dirs[8:] for p in (LS / d).glob("*.parquet")]
fut_rows = 0
for p in fut_files:
    try:
        fut_rows += pq.ParquetFile(p).metadata.num_rows
    except Exception:
        pass
families.append(
    {
        "data_type": "futures",
        "codes": len(fut_files),
        "rows": fut_rows,
        "bytes": sum(p.stat().st_size for p in fut_files),
        "start": None,
        "end": None,
        "in_daily_index": False,
    }
)

OUT.mkdir(parents=True, exist_ok=True)
(WORK / "inventory_full.json").write_text(
    json.dumps({"tables": rows, "quote_families": families}, ensure_ascii=False, indent=1),
    encoding="utf-8",
)
with (WORK / "inventory_full.csv").open("w", encoding="utf-8", newline="") as f:
    f.write("rel,rows,bytes,columns,manifest,manifest_rows_match,produced_by\n")
    for r in rows:
        f.write(
            f"{r['rel']},{r['rows']},{r['bytes']},{r['columns']},{r['manifest']},"
            f"{r['manifest_rows_match']},{r['produced_by']}\n"
        )

print("non-quote parquet tables:", len(rows))
print("with sidecar/dir manifest:", sum(1 for r in rows if r["manifest"]))
print("manifest rows mismatch:", [r["rel"] for r in rows if r["manifest_rows_match"] is False])
print("no manifest:", [r["rel"] for r in rows if not r["manifest"]])
print("\nquote families:")
for f in families:
    print("  ", f)
