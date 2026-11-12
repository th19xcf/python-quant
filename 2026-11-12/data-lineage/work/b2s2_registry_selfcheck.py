# ruff: noqa: T201, E741, B905
#!/usr/bin/env python3
"""缺口登记表自检：判定列七值归类、编号连续、§2 分类计数与正文一致。"""
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

DOC = Path(r'e:\code\python\quant\docs\数据缺口登记_正式表_2026-11-12.md')
SEVEN = ('已修', '可补', '不可补', '待采', '接受偏差', '待核实', '已退役')


def classify(cell: str) -> str | None:
    s = cell.lstrip('*').strip()
    hits = [v for v in SEVEN if s.startswith(v)]
    if not hits:
        return None
    return max(hits, key=len)


def main() -> None:
    text = DOC.read_text(encoding='utf-8')
    lines = text.splitlines()
    s3 = next(i for i, ln in enumerate(lines) if ln.startswith('## 3. 正式登记表'))
    s4 = next(i for i, ln in enumerate(lines) if ln.startswith('## 4. 退役与让号'))
    s2 = next(i for i, ln in enumerate(lines) if ln.startswith('## 2. 分类索引'))

    rows, unmapped = [], []
    for i in range(s3, s4):
        ln = lines[i]
        if not ln.startswith('| G-'):
            continue
        cells = [c.strip() for c in ln.strip().strip('|').split('|')]
        gid = cells[0]
        v = classify(cells[4]) if len(cells) > 4 else None
        rows.append({'gid': gid, 'cls': cells[1], 'verdict': cells[4], 'seven': v})
        if v is None:
            unmapped.append({'gid': gid, 'verdict': cells[4][:80]})

    nums = []
    for r in rows:
        m = re.fullmatch(r'G-(\d{3})', r['gid'])
        nums.append(int(m.group(1)) if m else -1)
    expected = [n for n in range(1, max(nums) + 1) if n not in (40, 41, 42, 43)]
    missing = sorted(set(expected) - set(nums))
    dup = [n for n, c in Counter(nums).items() if c > 1]

    dist = Counter(r['seven'] for r in rows)
    cls_actual = Counter(r['cls'] for r in rows)

    # §2 声明
    declared = {}
    for i in range(s2, s3):
        ln = lines[i]
        m = re.match(r'\|\s*([A-K])\s*\|[^|]*\|[^|]*\|\s*(\d+)\s*\|', ln)
        if m:
            declared[m.group(1)] = int(m.group(2))
    total_line = [ln for ln in lines[s2:s3] if '**合计**' in ln]
    declared_total = int(re.search(r'\*\*(\d+)\*\*', total_line[0]).group(1)) if total_line else None

    payload = {
        'sha256': hashlib.sha256(DOC.read_bytes()).hexdigest().upper(),
        'bytes': DOC.stat().st_size,
        'lines': len(lines),
        'sec3_rows': len(rows),
        'distinct_ids': len(set(nums)),
        'missing_numbers': missing,
        'duplicate_numbers': dup,
        'verdict_distribution': dict(dist.most_common()),
        'unmapped': unmapped,
        'class_actual': dict(sorted(cls_actual.items())),
        'class_declared': dict(sorted(declared.items())),
        'class_mismatch': {k: (declared.get(k), cls_actual.get(k))
                           for k in set(declared) | set(cls_actual)
                           if declared.get(k) != cls_actual.get(k)},
        'declared_total': declared_total,
        'actual_total_excluding_reserved': len(rows) - sum(1 for r in rows if r['cls'] == '—'),
    }
    out = Path(__file__).with_name('b2s2_registry_selfcheck.json')
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding='utf-8')
    for k, v in payload.items():
        if k == 'unmapped':
            print('unmapped', len(v), v)
        else:
            print(k, v)


if __name__ == '__main__':
    main()
