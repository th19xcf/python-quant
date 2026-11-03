# ruff: noqa: T201, BLE001, S110, SIM115  # 归档的一次性验证脚本，依赖已卸载
"""mootdx 行情服务器可用性探测（只读，验证用）"""
import json
import socket
import time
from pathlib import Path

from mootdx.consts import HQ_HOSTS
from tdxpy.constants import hq_hosts
from tdxpy.hq import TdxHq_API

hosts = [{'site': h[0], 'addr': h[1], 'port': int(h[2])} for h in list(hq_hosts) + list(HQ_HOSTS)]
print(f'候选节点数: {len(hosts)}')

# 1) TCP 连通性 + 握手延迟
tcp_ok = []
for h in hosts:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(0.8)
    t0 = time.perf_counter()
    try:
        s.connect((h['addr'], h['port']))
        h['tcp_ms'] = round((time.perf_counter() - t0) * 1000, 2)
        tcp_ok.append(h)
    except Exception as e:
        h['err'] = type(e).__name__
    finally:
        try:
            s.close()
        except Exception:
            pass

print(f'TCP 可达节点: {len(tcp_ok)}/{len(hosts)}')

# 2) 协议层实测：get_security_count 能否拿到数据
func_ok = []
for h in tcp_ok:
    api = TdxHq_API(raise_exception=False, auto_retry=False)
    t0 = time.perf_counter()
    try:
        with api.connect(h['addr'], h['port'], time_out=1.0):
            cnt = api.get_security_count(market=0)
            bars = api.get_security_bars(9, 1, '600000', 0, 5)
        h['api_ms'] = round((time.perf_counter() - t0) * 1000, 2)
        h['sh_count'] = cnt
        h['bars_rows'] = len(bars) if bars else 0
        if cnt and h['bars_rows']:
            func_ok.append(h)
    except Exception as e:
        h['api_err'] = f'{type(e).__name__}: {e}'
    finally:
        try:
            api.disconnect()
        except Exception:
            pass

func_ok.sort(key=lambda x: x['api_ms'])
print(f'协议层可用节点: {len(func_ok)}/{len(tcp_ok)} (tcp)/{len(hosts)} (total)')
print('\n可用节点（按协议延迟排序，前 15 个）:')
for h in func_ok[:15]:
    print(f"  {h['site']:<10} {h['addr']:<16}:{h['port']:<5} tcp={h['tcp_ms']:>7.2f}ms api={h['api_ms']:>7.2f}ms sh_cnt={h['sh_count']}")

print('\n全部节点明细:')
for h in hosts:
    print(json.dumps(h, ensure_ascii=False))

with open(Path(__file__).resolve().parent / 'mootdx_probe_servers.json', 'w',
          encoding='utf-8') as f:
    json.dump({'hosts': hosts, 'func_ok': [h['addr'] for h in func_ok]}, f, ensure_ascii=False, indent=2)
