# ruff: noqa: T201, E741, B905
#!/usr/bin/env python3
"""列出 d4 文档指定行区间，便于人工核对重复/错位。"""
import sys
from pathlib import Path

P = Path(r'e:\code\python\quant\2026-11-12\data-lineage\outputs\d4_graded_fund_evidence.md')
lines = P.read_text(encoding='utf-8').splitlines()
for seg in ((413, 419), (423, 429), (453, 459)):
    print(f'=== {seg[0]}-{seg[1]}')
    for i in range(seg[0] - 1, min(seg[1], len(lines))):
        print(i + 1, '|', lines[i][:200])
