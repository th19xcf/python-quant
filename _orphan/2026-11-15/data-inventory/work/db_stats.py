"""只读：数据库正式表单遍聚合（每表一次 GROUP BY 扫描，不写库）。"""
import json
from pathlib import Path

import numpy as np
import pymysql
from dotenv import dotenv_values


def _pct(vals):
    a = np.asarray(vals, dtype=np.int64)
    return {
        "p50": int(np.percentile(a, 50)), "p95": int(np.percentile(a, 95)),
        "min": int(a.min()), "max": int(a.max()),
    }


ROOT = Path(r"d:\code\python\quant")
e = dotenv_values(ROOT / ".env")
META = {
    "stock_basic": ("list_date", "ts_code", "basic"),
    "stock_daily": ("trade_date", "ts_code", "series"),
    "stock_adj_factor": ("trade_date", "ts_code", "series"),
    "stock_dividend": ("ex_date", "ts_code", "series"),
    "index_basic": ("list_date", "ts_code", "basic"),
    "index_daily": ("trade_date", "ts_code", "series"),
    "market_breadth": ("trade_date", None, "daily_no_code"),
    "fund_basic": ("list_date", "ts_code", "basic"),
    "fund_daily": ("trade_date", "ts_code", "series"),
    "stock_status_history": ("start_date", "ts_code", "series"),
    "stock_minute": ("trade_time", "ts_code", "series"),
}
conn = pymysql.connect(
    host=e["DATABASE__HOST"], port=int(e["DATABASE__PORT"]), user=e["DATABASE__USERNAME"],
    password=e["DATABASE__PASSWORD"], database=e["DATABASE__DATABASE"],
    connect_timeout=5, read_timeout=900, charset="utf8mb4",
)
cur = conn.cursor()
out = {}
for t, (dc, cc, kind) in META.items():
    rec = {}
    if kind == "series":
        cur.execute(
            f"select {cc}, count(*) n, min(`{dc}`) mn, max(`{dc}`) mx, "
            f"count(*) - count(distinct `{dc}`) dup from `{t}` group by {cc}"
        )
        rows = cur.fetchall()
        rec["rows"] = sum(int(r[1]) for r in rows)
        rec["codes"] = len(rows)
        starts = [str(r[2]) for r in rows if r[2] is not None]
        ends = [str(r[3]) for r in rows if r[3] is not None]
        rec["start"] = min(starts) if starts else None
        rec["end"] = max(ends) if ends else None
        rec["dup_key"] = sum(int(r[4]) for r in rows)
        per = sorted(int(r[1]) for r in rows)
        rec["per_code"] = _pct(per) if per else {}
    elif kind == "basic":
        cur.execute(f"select count(*), count(distinct {cc}), min(`{dc}`), max(`{dc}`) from `{t}`")
        r = cur.fetchone()
        rec.update(rows=int(r[0]), codes=int(r[1]), start=str(r[2]), end=str(r[3]), dup_key=int(r[0]) - int(r[1]))
    else:
        cur.execute(f"select count(*), min(`{dc}`), max(`{dc}`), count(*) - count(distinct `{dc}`) from `{t}`")
        r = cur.fetchone()
        rec.update(rows=int(r[0]), start=str(r[1]), end=str(r[2]), dup_key=int(r[3]))
    out[t] = rec
    print(t, rec.get("rows"), rec.get("codes"), rec.get("start"), rec.get("end"), rec.get("dup_key"), flush=True)
cur.close()
conn.close()
p = ROOT / "2026-11-15" / "data-inventory" / "work" / "db_stats.json"
p.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
print("wrote", p)
