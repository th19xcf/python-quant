# ruff: noqa: T201, BLE001, S110, SIM115  # 归档的一次性验证脚本，依赖已卸载
"""分钟数据语义/单位/指数/回溯年限实测"""
import statistics
from datetime import date

import polars as pl
from tdxpy.hq import TdxHq_API

from src.data.tdx_day_parser import parse_day_file

NODE = ('202.108.253.139', 80)

api = TdxHq_API(raise_exception=False)
api.connect(*NODE, time_out=3)

# --- 1) 与本地日线对拍：分钟成交量求和的量级 ---
daily = parse_day_file(__import__('pathlib').Path(r'H:\zxzq\vipdoc\sh\lday\sh600000.day'))
for d in [20261019, 20261016, 20261015]:
    r = api.get_history_minute_time_data(1, '600000', d)
    row = daily.filter(pl.col('date') == pl.lit(date(d // 10000, (d // 100) % 100, d % 100)))
    if not r or row.is_empty():
        print(d, 'no data', None if r is None else len(r))
        continue
    s = sum(x['vol'] for x in r)
    dv = row['volume'][0]
    dc = row['close'][0]
    print(f'{d}: minute_rows={len(r)} sum_vol={s:.0f} daily_vol={dv:.0f} ratio={s / dv:.3f} '
          f'last_min_price={r[-1]["price"]} daily_close={dc} first={r[0]} last={r[-1]}')

# --- 2) 字段清单 ---
r = api.get_history_minute_time_data(1, '600000', 20261019)
print('minute keys:', list(r[0].keys()) if r else None)
print('realtime minute keys:', list((api.get_minute_time_data(1, '600000') or [{}])[0].keys()))

# --- 3) 指数分钟 ---
for mkt, code in [(1, '000001'), (0, '399001'), (1, '000300')]:
    r = api.get_history_minute_time_data(mkt, code, 20261019)
    print(f'index {code} (market={mkt}) rows={None if r is None else len(r)}',
          r[0] if r else '')

# --- 4) 回溯年限探测（2 分法）---
print('\n回溯探测 600000:')
for d in [20260102, 20250102, 20240102, 20230103, 20220104, 20210104, 20200102,
          20190102, 20180102, 20170103, 20160104, 20150105, 20140102, 20130104,
          20120104, 20110104, 20100104, 20090105, 20080102, 20070104]:
    try:
        r = api.get_history_minute_time_data(1, '600000', d)
        n = None if r is None else len(r)
        avg = round(statistics.mean(x['price'] for x in r), 3) if r else None
        print(f'  {d} rows={n} avg_price={avg}')
    except Exception as e:
        print(f'  {d} ERR {type(e).__name__} {str(e)[:40]}')

api.disconnect()
