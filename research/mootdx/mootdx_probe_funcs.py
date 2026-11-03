# ruff: noqa: T201, BLE001, S110, SIM115  # 归档的一次性验证脚本，依赖已卸载
"""mootdx/tdxpy 协议层功能矩阵探测"""
import time

from tdxpy.hq import TdxHq_API

NODES = [
    ('202.108.253.139', 80),
    ('180.153.18.170', 7709),
    ('180.153.18.172', 80),
    ('60.191.117.167', 7709),
    ('115.238.56.198', 7709),
    ('60.12.136.250', 7709),
    ('59.36.5.11', 7709),
    ('119.29.19.242', 7709),
]


def probe(ip, port):
    out = {'node': f'{ip}:{port}'}
    api = TdxHq_API(raise_exception=False, auto_retry=False)
    t0 = time.perf_counter()
    api.connect(ip, port, time_out=2)
    out['connect_ms'] = round((time.perf_counter() - t0) * 1000, 1)
    tests = {
        'security_count': lambda: api.get_security_count(market=0),
        'quotes': lambda: api.get_security_quotes([(1, '600000')]),
        'day_bars': lambda: api.get_security_bars(9, 1, '600000', 0, 10),
        'day_bars_f4': lambda: api.get_security_bars(4, 1, '600000', 0, 10),
        'min_bars_8': lambda: api.get_security_bars(8, 1, '600000', 0, 10),
        '5min_bars_0': lambda: api.get_security_bars(0, 1, '600000', 0, 10),
        'index_bars': lambda: api.get_index_bars(9, 1, '000001', 0, 10),
        'hist_minute': lambda: api.get_history_minute_time_data(0, 1, '600000', 20261030),
        'xdxr': lambda: api.get_xdxr_info(1, '600000'),
        'finance': lambda: api.get_finance_info(1, '600000'),
    }
    for name, fn in tests.items():
        t = time.perf_counter()
        try:
            r = fn()
            n = len(r) if r is not None else None
            out[name] = f'ok rows={n} {round((time.perf_counter()-t)*1000,1)}ms'
        except Exception as e:
            out[name] = f'{type(e).__name__}: {str(e)[:60]}'
    api.disconnect()
    return out


for ip, port in NODES:
    r = probe(ip, port)
    print(r['node'], 'connect', r['connect_ms'], 'ms')
    for k, v in r.items():
        if k not in ('node', 'connect_ms'):
            print(f'   {k:18} {v}')
