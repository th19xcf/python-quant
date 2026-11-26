"""只读补充核对：主键定义、跨表覆盖差、数据库/本地单位对拍。"""
import json
from pathlib import Path

import pandas as pd
import pymysql
import pyarrow.parquet as pq
from dotenv import dotenv_values

ROOT = Path(r"d:\code\python\quant")
LS = ROOT / "data" / "local_store"
WORK = ROOT / "2026-11-15" / "data-inventory" / "work"
out = {}

# ann_date_map 主键应为 (symbol, period)
am = pd.read_parquet(LS / "meta" / "financials" / "ann_date_map.parquet",
                     columns=["symbol", "period", "ann_date"])
out["ann_date_map_dup_symbol_period"] = int(len(am) - len(am.drop_duplicates(["symbol", "period"])))
out["ann_date_map_codes"] = int(am["symbol"].nunique())

sb = set(pd.read_parquet(LS / "meta" / "stock_basic.parquet", columns=["ts_code"])["ts_code"])
di = pd.read_parquet(LS / "meta" / "daily_index.parquet")
stock_codes = set(di.loc[di["data_type"] == "stock", "ts_code"])
div_codes = set(pd.read_parquet(LS / "factors" / "stock_dividend.parquet",
                                columns=["ts_code"])["ts_code"])
ev = pd.read_parquet(LS / "meta" / "events" / "events.parquet", columns=["ts_code"])
val = set(pd.read_parquet(LS / "meta" / "financials" / "valuation_daily.parquet",
                          columns=["ts_code"])["ts_code"].unique())
st = set(pd.read_parquet(LS / "meta" / "st" / "st_intervals.parquet", columns=["ts_code"])["ts_code"])
out["stock_codes_no_dividend"] = len(stock_codes - div_codes)
out["stock_codes_no_event"] = len(stock_codes - set(ev["ts_code"]))
out["stock_codes_no_valuation"] = len(stock_codes - val)
out["stock_codes_no_st_interval"] = len(stock_codes - st)

# 陈旧分布
ss = pd.read_parquet(LS / "meta" / "universe" / "stale_status.parquet")
out["stale_by_class"] = ss.loc[ss["is_stale"], "asset_class"].value_counts().to_dict()
out["stale_last_date_max"] = str(ss["last_trade_date"].max())

# DB vs 本地：单位与同一行对拍
e = dotenv_values(ROOT / ".env")
conn = pymysql.connect(host=e["DATABASE__HOST"], port=int(e["DATABASE__PORT"]),
                       user=e["DATABASE__USERNAME"], password=e["DATABASE__PASSWORD"],
                       database=e["DATABASE__DATABASE"], connect_timeout=5,
                       read_timeout=60, charset="utf8mb4")
cur = conn.cursor()
cur.execute("select ts_code,trade_date,close,vol,amount from stock_daily "
            "where ts_code=%s and trade_date=%s", ("600000.SH", "2026-11-09"))
db_row = cur.fetchone()
loc = pd.read_parquet(LS / "quotes" / "stock" / "sh" / "600000.SH.parquet")
loc_row = loc[loc["date"].astype(str) == "2026-11-09"].iloc[0]
out["db_local_same_row"] = {
    "db": [str(x) for x in db_row], "db_vol": float(db_row[3]), "db_amount": float(db_row[4]),
    "local_volume": float(loc_row["volume"]), "local_amount": float(loc_row["amount"]),
    "vol_ratio_db_to_local": float(db_row[3]) / float(loc_row["volume"]),
}
# 数据库 index_daily 单位是否与本地一致（股/元）
idx = pd.read_parquet(LS / "quotes" / "index" / "sh" / "000001.SH.parquet")
irow = idx[idx["date"].astype(str) == "2026-11-03"].iloc[0]
cur.execute("select vol,amount from index_daily where ts_code=%s and trade_date=%s",
            ("000001.SH", "2026-11-03"))
ir = cur.fetchone()
out["db_index_unit"] = {"db_vol": float(ir[0]), "db_amount": float(ir[1]),
                        "local_volume": float(irow["volume"]), "local_amount": float(irow["amount"])}
cur.close()
conn.close()

p = WORK / "extra_checks.json"
p.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
print(json.dumps(out, ensure_ascii=False, indent=1))
