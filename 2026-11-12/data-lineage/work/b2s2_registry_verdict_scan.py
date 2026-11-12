# ruff: noqa: T201, E741, B905
#!/usr/bin/env python3
"""扫描缺口登记表 §3 的「当前判定」列，列出不在 §0.1 七值内的行。

只读，不改文件。
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

DOC = Path(r'e:\code\python\quant\docs\数据缺口登记_正式表_2026-11-12.md')
OUT = Path(__file__).with_name('b2s2_registry_verdict_scan.json')

SEVEN = ('已修', '可补', '不可补', '待采', '接受偏差', '待核实', '已退役')


def main() -> None:
    text = DOC.read_text(encoding='utf-8')
    lines = text.splitlines()
    start = next(i for i, ln in enumerate(lines) if ln.startswith('## 3. 正式登记表'))
    end = next(i for i, ln in enumerate(lines) if ln.startswith('## 4. 退役与让号'))

    rows = []
    for i in range(start, end):
        ln = lines[i]
        if not ln.startswith('| G-'):
            continue
        cells = [c.strip() for c in ln.strip().strip('|').split('|')]
        if len(cells) < 7:
            rows.append({'line': i + 1, 'bad_columns': len(cells), 'raw': ln[:160]})
            continue
        rows.append({
            'line': i + 1,
            'gid': cells[0],
            'cls': cells[1],
            'verdict': cells[4],
            'owner': cells[6] if len(cells) > 6 else '',
            'verify': cells[7] if len(cells) > 7 else '',
        })

    bad = [r for r in rows if 'verdict' in r and r['verdict'] not in SEVEN]
    from collections import Counter
    cnt = Counter(r['verdict'] for r in bad)
    payload = {
        'doc': str(DOC),
        'bytes': DOC.stat().st_size,
        'sha256': hashlib.sha256(DOC.read_bytes()).hexdigest().upper(),
        'total_lines': len(lines),
        'sec3_rows': len(rows),
        'malformed': [r for r in rows if 'bad_columns' in r],
        'seven_value_rows': len(rows) - len(bad),
        'bad_rows': len(bad),
        'bad_distinct': len(cnt),
        'bad_counter': dict(cnt.most_common()),
        'bad_detail': bad,
    }
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding='utf-8')
    print('bytes', payload['bytes'], 'lines', payload['total_lines'], 'sha', payload['sha256'])
    print('sec3_rows', payload['sec3_rows'], 'seven', payload['seven_value_rows'], 'bad', payload['bad_rows'], 'distinct', payload['bad_distinct'])
    for k, v in payload['bad_counter'].items():
        print(f'  {v:3d}  {k}')
    for r in bad:
        print(f"L{r['line']:4d} {r['gid']:7s} {r['cls']:2s} | {r['verdict']} | {r['owner']}")


if __name__ == '__main__':
    main()
