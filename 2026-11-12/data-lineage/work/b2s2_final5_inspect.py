# ruff: noqa: T201, E741, B905
"""列出 5 条待落定行的判定列原文与列数，供人工核对（只读）。"""
from __future__ import annotations

import json
from pathlib import Path

DOC = Path(__file__).resolve().parents[3] / 'docs' / '数据缺口登记_正式表_2026-11-12.md'
TARGETS = ('G-067', 'G-089', 'G-090', 'G-098', 'G-117')

lines = DOC.read_text(encoding='utf-8').split('\n')
out = []
for idx, line in enumerate(lines, 1):
    s = line.strip()
    if not s.startswith('| G-'):
        continue
    gid = s.split('|')[1].strip()
    if gid not in TARGETS:
        continue
    # 朴素按 | 切分，用于暴露列错位
    naive = s.split('|')
    out.append({
        'line_no': idx,
        'gid': gid,
        'naive_cells': len(naive) - 2,
        'verdict_raw': s.split('|')[5].strip() if len(naive) > 5 else '',
        'preview': s[:400],
    })

header = [l for l in lines if l.startswith('| 编号 |')]
print('HEADER:', header[0] if header else 'NOT FOUND')
for row in out:
    print(json.dumps(row, ensure_ascii=False, indent=2))
Path(Path(__file__).parent / 'b2s2_final5_inspect.json').write_text(
    json.dumps(out, ensure_ascii=False, indent=2), encoding='utf-8')
