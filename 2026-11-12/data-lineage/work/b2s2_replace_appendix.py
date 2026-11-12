# ruff: noqa: T201, E741, B905
#!/usr/bin/env python3
"""用无转义污染的版本替换 d4 文档的 §13~§15 附录。"""
from pathlib import Path

DOC = Path(r'e:\code\python\quant\2026-11-12\data-lineage\outputs\d4_graded_fund_evidence.md')
APP = Path(__file__).with_name('d4_appendix_s13_15.md')

lines = DOC.read_text(encoding='utf-8').splitlines()
cut = next(i for i, ln in enumerate(lines) if ln.startswith('## 13. 复核纠正'))
head = lines[:cut]
while head and head[-1].strip() in ('', '---'):
    head.pop()
body = APP.read_text(encoding='utf-8').rstrip('\n')
DOC.write_text('\n'.join(head) + '\n\n---\n\n' + body + '\n', encoding='utf-8')

text = DOC.read_text(encoding='utf-8')
bad = [(i + 1, repr(ln[:60])) for i, ln in enumerate(text.splitlines())
       if any(ord(c) < 9 or 11 <= ord(c) <= 12 or 14 <= ord(c) <= 31 for c in ln)]
print('lines', len(text.splitlines()), 'bytes', DOC.stat().st_size)
print('control_char_lines', bad)
print('sections', [ln for ln in text.splitlines() if ln.startswith('## ')])
