# ruff: noqa: T201, BLE001, S110, SIM115  # 归档的一次性验证脚本，依赖已卸载
"""全量节点功能扫描：并发探测 142 节点，判定是否真能返回 K 线"""
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from mootdx.consts import HQ_HOSTS
from tdxpy.constants import hq_hosts
from tdxpy.hq import TdxHq_API

hosts = [{'site': h[0], 'addr': h[1], 'port': int(h[2])} for h in list(hq_hosts) + list(HQ_HOSTS)]


def probe(h):
    api = TdxHq_API(raise_exception=False, auto_retry=False)
    r = dict(h)
    try:
        api.connect(h['addr'], h['port'], time_out=2.5)
        cnt = api.get_security_count(0)
        day = api.get_security_bars(9, 1, '600000', 0, 5)
        r['count'] = cnt
        r['day_rows'] = 0 if day is None else len(day)
        if r['day_rows']:
            r['day_last'] = day[-1].get('datetime') if isinstance(day[-1], dict) else None
    except Exception as e:
        r['err'] = f'{type(e).__name__}: {str(e)[:50]}'
    finally:
        try:
            api.disconnect()
        except Exception:
            pass
    return r


res = []
with ThreadPoolExecutor(max_workers=16) as ex:
    futs = [ex.submit(probe, h) for h in hosts]
    for f in as_completed(futs):
        res.append(f.result())

ok = [r for r in res if r.get('day_rows')]
cnt_ok = [r for r in res if r.get('count')]
print(f'节点总数 {len(res)}  |  count 可取 {len(cnt_ok)}  |  日K 可取 {len(ok)}')
for r in sorted(ok, key=lambda x: x['day_rows'], reverse=True):
    print(f"  {r['site']:<14} {r['addr']}:{r['port']}  rows={r['day_rows']} count={r['count']}")

out_path = Path(__file__).resolve().parent / 'mootdx_scan_all.json'
json.dump(res, open(out_path, 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
