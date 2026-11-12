import re
from pathlib import Path

p = Path(r"e:\code\python\quant\docs\数据缺口登记_正式表_2026-11-12.md")
txt = p.read_text(encoding="utf-8")
lines = txt.split("\n")
out = []
for l in lines:
    m = re.match(r"^\|\s*(G-\d+)\s*\|\s*([A-Z])\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|", l)
    if m:
        out.append(f"{m.group(1)}\t{m.group(2)}\t{m.group(3)}\t{m.group(4)}")
Path(r"e:\code\python\quant\2026-11-12\data-lineage\work\gaps.tsv").write_text(
    "\n".join(out), encoding="utf-8"
)
print(len(out))
for o in out:
    print(o)
