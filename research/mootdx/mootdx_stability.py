# ruff: noqa: T201, BLE001, S110, SIM115  # 归档的一次性验证脚本，依赖已卸载
"""稳定性与吞吐实测：约 200 次请求，统计成功/失败/超时/延迟"""
import random
import statistics
import time

import polars as pl
from tdxpy.hq import TdxHq_API

NODE = ('202.108.253.139', 80)

# 取本地真实存在的一批标的（sh/sz 各 10 个）
from pathlib import Path

codes = []
for mkt, d in [('sh', 1), ('sz', 0)]:
    files = sorted(Path(rf'H:\zxzq\vipdoc\{mkt}\lday').glob('*.day'))
    # 过滤指数，只取 6 位 A 股
    for f in files:
        c = f.stem[2:]
        if mkt == 'sh' and c.startswith(('60', '68')):
            codes.append((d, c))
        elif mkt == 'sz' and c.startswith(('00', '30')):
            codes.append((d, c))
codes = codes[::max(1, len(codes) // 20)][:20]
print(f'样本标的 {len(codes)} 个: {[c[1] for c in codes[:5]]}...')

DATES = [20261019, 20261016, 20261015, 20261014, 20261013]

api = TdxHq_API(raise_exception=False, auto_retry=False)
t_conn = time.perf_counter()
api.connect(*NODE, time_out=3)
print(f'connect {(time.perf_counter()-t_conn)*1000:.1f}ms')

# ---------- 1) 稳定性：200 次历史分时请求 ----------
stats = {'ok': 0, 'empty': 0, 'fail': 0, 'timeout': 0}
lat = []
t_start = time.perf_counter()
for i in range(200):
    mkt, code = codes[i % len(codes)]
    d = DATES[(i // len(codes)) % len(DATES)]
    t = time.perf_counter()
    try:
        r = api.get_history_minute_time_data(mkt, code, d)
        dt_ms = (time.perf_counter() - t) * 1000
        lat.append(dt_ms)
        if r:
            stats['ok'] += 1
        else:
            stats['empty'] += 1
    except TimeoutError:
        stats['timeout'] += 1
    except Exception:
        stats['fail'] += 1
        api.connect(*NODE, time_out=3)
    if (i + 1) % 50 == 0:
        print(f'  进度 {i+1}/200 累计 {stats} 累计耗时 {time.perf_counter()-t_start:.2f}s')
total = time.perf_counter() - t_start
print(f'\n200 次请求: {stats}  总耗时 {total:.2f}s  QPS={200/total:.1f}')
if lat:
    lat.sort()
    print(f'延迟 p50={statistics.median(lat):.1f}ms p90={lat[int(len(lat)*0.9)-1]:.1f}ms '
          f'p99={lat[int(len(lat)*0.99)-1]:.1f}ms min={lat[0]:.1f} max={lat[-1]:.1f} mean={statistics.mean(lat):.1f}')

# ---------- 2) 吞吐：单票多日 / 20 票批量 ----------
print('\n吞吐测试（历史分时，每票每日一次请求）:')
t = time.perf_counter()
n = 0
for d in DATES:
    r = api.get_history_minute_time_data(1, '600000', d)
    n += len(r or [])
one_sym_ms = (time.perf_counter() - t) * 1000
print(f'  单票 {len(DATES)} 个交易日: {one_sym_ms:.1f}ms, {n} 行 -> 单日单票 {one_sym_ms/len(DATES):.1f}ms')

t = time.perf_counter()
for mkt, code in codes:
    for d in DATES:
        api.get_history_minute_time_data(mkt, code, d)
batch = time.perf_counter() - t
print(f'  20 票 × {len(DATES)} 日 = {len(codes)*len(DATES)} 次请求: {batch:.2f}s '
      f'-> 单票 5 日 {batch/len(codes)*1000:.1f}ms, 单票单日 {batch/(len(codes)*len(DATES))*1000:.1f}ms')

# ---------- 3) 近 2 周空缺日是否存在分时 ----------
print('\n近期交易日分时可用性（本地日线止于 2026-10-19）:')
for d in [20261030, 20261102, 20261103, 20261020, 20261009]:
    r = api.get_history_minute_time_data(1, '600000', d)
    print(f'  {d} rows={None if r is None else len(r)}')

api.disconnect()
