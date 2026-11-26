"""只读：数据库正式表结构、规模与日期边界探测（不做任何写入）。"""
import json
from pathlib import Path

import pymysql
from dotenv import dotenv_values

ROOT = Path(r"d:\code\python\quant")
e = dotenv_values(ROOT / ".env")
TABLES = [
    "stock_basic", "stock_daily", "stock_adj_factor", "stock_dividend",
    "index_basic", "index_daily", "market_breadth", "fund_basic",
    "fund_daily", "stock_status_history", "stock_minute",
]
DATE_COL = {
    "stock_basic": ("list_date", "ts_code"),
    "stock_daily": ("trade_date", "ts_code"),
    "stock_adj_factor": ("trade_date", "ts_code"),
    "stock_dividend": ("ex_date", "ts_code"),
    "index_basic": ("list_date", "ts_code"),
    "index_daily": ("trade_date", "ts_code"),
    "market_breadth": ("trade_date", None),
    "fund_basic": ("list_date", "ts_code"),
    "fund_daily": ("trade_date", "ts_code"),
    "stock_status_history": ("trade_date", "ts_code"),
    "stock_minute": ("trade_time", "ts_code"),
}
conn = pymysql.connect(
    host=e["DATABASE__HOST"], port=int(e["DATABASE__PORT"]), user=e["DATABASE__USERNAME"],
    password=e["DATABASE__PASSWORD"], database=e["DATABASE__DATABASE"],
    connect_timeout=5, read_timeout=600, charset="utf8mb4",
)
cur = conn.cursor()
cur.execute("select version()")
out = {"mysql_version": cur.fetchone()[0], "tables": {}}
cur.execute(
    "select table_name, table_rows, data_length, index_length from information_schema.tables "
    "where table_schema=%s", (e["DATABASE__DATABASE"],)
)
est = {r[0]: {"est_rows": int(r[1] or 0), "mb": round(((r[2] or 0) + (r[3] or 0)) / 1048576, 1)} for r in cur.fetchall()}
for t in TABLES:
    info = dict(est.get(t, {}))
    cur.execute(f"show columns from `{t}`")
    cols = [(r[0], r[1]) for r in cur.fetchall()]
    info["n_cols"] = len(cols)
    info["columns"] = [c[0] for c in cols]
    cur.execute(f"show index from `{t}`")
    idx = {}
    for r in cur.fetchall():
        idx.setdefault(r[2], []).append((r[4], r[3]))
    info["indexes"] = {k: v for k, v in idx.items()}
    out["tables"][t] = info
cur.close()
conn.close()
p = ROOT / "2026-11-15" / "data-inventory" / "work" / "db_schema.json"
p.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
print("wrote", p, "tables", len(out["tables"]), "version", out["mysql_version"])
