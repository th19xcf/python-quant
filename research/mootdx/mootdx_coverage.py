# ruff: noqa: T201, BLE001, S110, SIM115  # 归档的一次性验证脚本，依赖已卸载
"""mootdx API 层行为确认 + 回溯下限 + 品种覆盖"""
import time

from mootdx.quotes import Quotes
from tdxpy.hq import TdxHq_API

NODE = ('202.108.253.139', 80)

print('=== 1) mootdx StdQuotes 接口层行为 ===')
t = time.perf_counter()
q = Quotes.factory(market='std', server=NODE, timeout=5, auto_retry=False)
print(f'factory+connect {time.perf_counter()-t:.3f}s, bestip={q.server}')

for name, fn in [
    ('bars(日线, 800)', lambda: q.bars(symbol='600000', frequency=9, offset=800)),
    ('bars(1分钟)', lambda: q.bars(symbol='600000', frequency=8, offset=240)),
    ('minute(今日分时)', lambda: q.minute(symbol='600000')),
    ('minutes(历史分时 20261019)', lambda: q.minutes(symbol='600000', date='20261019')),
    ('index(指数日线)', lambda: q.index(symbol='000001', frequency=9, offset=10)),
    ('xdxr', lambda: q.xdxr(symbol='600000')),
    ('finance', lambda: q.finance(symbol='600000')),
]:
    t = time.perf_counter()
    try:
        r = fn()
        rows = 0 if r is None else (r.shape[0] if hasattr(r, 'shape') else len(r))
        print(f'  {name:28} rows={rows:<5} {time.perf_counter()-t:6.3f}s cols={list(r.columns) if hasattr(r, "columns") and rows else "-"}')
    except Exception as e:
        print(f'  {name:28} ERR {type(e).__name__}: {str(e)[:50]}')
q.close()

print('\n=== 2) 分时回溯下限 ===')
api = TdxHq_API(raise_exception=False)
api.connect(*NODE, time_out=3)
for d in [20060104, 20050104, 20040102, 20030102, 20020104, 20010102, 20000104, 19991110]:
    r = api.get_history_minute_time_data(1, '600000', d)
    print(f'  {d} rows={None if r is None else len(r)}')

print('\n=== 3) 品种覆盖（历史分时）===')
for mkt, code, label in [(1, '510300', 'SH ETF'), (0, '159915', 'SZ ETF'),
                          (1, '688981', '科创板'), (0, '300750', '创业板'),
                          (1, '000001', '上证指数'), (0, '399006', '创业板指')]:
    try:
        r = api.get_history_minute_time_data(mkt, code, 20261019)
        print(f'  {label:8} {code} rows={None if r is None else len(r)}')
    except Exception as e:
        print(f'  {label:8} {code} ERR {type(e).__name__}: {str(e)[:40]}')

print('\n=== 4) 北交所 ===')
for mkt in [0, 1, 2]:
    try:
        r = api.get_history_minute_time_data(mkt, '899050', 20261019)
        print(f'  market={mkt} 899050 rows={None if r is None else len(r)}')
    except Exception as e:
        print(f'  market={mkt} ERR {type(e).__name__}: {str(e)[:40]}')
api.disconnect()
