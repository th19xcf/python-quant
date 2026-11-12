"""对比 writers_detected 旧口径（build_lineage 原实现）与新口径（lineage_writers）。

只读：不写 outputs/、不碰 data/。表清单用 ast 从 build_lineage.py 的 add(...) 抽取，
因此本脚本可在不改动构建脚本的前提下独立复现两套判定。

    .venv\\Scripts\\python.exe 2026-11-12\\data-lineage\\work\\report_writer_diff.py
"""
from __future__ import annotations

import ast
import json
import sys
from pathlib import Path

ROOT = Path(r'e:\code\python\quant')
WORK = ROOT / '2026-11-12' / 'data-lineage' / 'work'
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from tools import lineage_writers as lw  # noqa: E402  (tools/lineage_writers.py)

DATA_CALLS = {"write_parquet_atomic", "write_parquet", "to_parquet", "sink_parquet",
              "save", "write_csv", "to_csv"}


def table_defs() -> list[dict]:
    """从 build_lineage.py 抽取 add(id=..., path=...) 的表定义（不执行脚本）。"""
    tree = ast.parse((HERE / 'build_lineage.py').read_text(encoding='utf-8'))
    out = []
    for node in tree.body:
        if not (isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)):
            continue
        call = node.value
        if not (isinstance(call.func, ast.Name) and call.func.id == 'add'):
            continue
        kw = {k.arg: k.value for k in call.keywords if k.arg}
        try:
            out.append({'id': ast.literal_eval(kw['id']), 'path': ast.literal_eval(kw['path'])})
        except (KeyError, ValueError):
            continue
    return out


def old_writers_for(path: str, edges: dict, wc: dict) -> list[dict]:
    """build_lineage.writers_for 的原样复刻（前缀子串 + 就近候选 + 插入序截断）。"""
    rel = path.replace("data/local_store/", "").split("{")[0].rstrip("/")
    cand = []
    for k in edges["writers"]:
        if k == f"data/local_store/{rel}" or k.startswith(f"data/local_store/{rel}"):
            seen = {}
            for r in edges["writers"].get(k, []):
                if "/ci_cov_sim_" in r["file"]:
                    continue
                if r["file"] not in seen or r["confidence"] == "high":
                    seen[r["file"]] = r
            cand += [{"file": r["file"], "line": r["line"], "call": r["call"]} for r in seen.values()]
    for k, hits in wc.items():
        if k.startswith(rel.replace(".parquet", "")):
            for h in hits:
                cand.append({"file": h["file"], "line": h["line"], "call": "near-write"})
    out, seen = [], set()
    for c in cand:
        key = (c["file"], c["line"])
        if key in seen:
            continue
        seen.add(key)
        if c["call"] not in DATA_CALLS and c["call"] != "near-write":
            continue
        mode = "oneoff" if c["file"].startswith("_staging/") else "routine"
        out.append({"file": c["file"], "line": c["line"], "mode": mode})
    return out[:8]


def main() -> int:
    edges = json.loads((WORK / 'edges_raw2.json').read_text(encoding='utf-8'))
    wc = json.loads((WORK / 'writer_candidates.json').read_text(encoding='utf-8'))
    tables = table_defs()
    inventory = [t['path'] for t in tables]

    sites = lw.scan(ROOT)
    records = [r for rows in edges['writers'].values() for r in rows]
    print(f'表 {len(tables)}，AST 写站点 {len(sites)}，上游记录 {len(records)}')

    result = {}
    for t in tables:
        old = old_writers_for(t['path'], edges, wc)
        new = lw.detect_writers(t['path'], sites, records, inventory)
        result[t['id']] = {'path': t['path'], 'old': old, 'new': new}

    empty_new = [k for k, v in result.items() if not v['new']]
    empty_old = [k for k, v in result.items() if not v['old']]
    print(f'新口径空表 {len(empty_new)}: {empty_new}')
    print(f'旧口径空表 {len(empty_old)}: {empty_old}')
    print()
    for tid, v in result.items():
        o = {(x['file'], x['line']) for x in v['old']}
        n = {(x['file'], x['line']) for x in v['new']}
        if o == n:
            continue
        print(f'== {tid}  旧 {len(o)} → 新 {len(n)}')
        for f, ln in sorted(o - n):
            print(f'   - 减 {f}:{ln}')
        for f, ln in sorted(n - o):
            print(f'   + 增 {f}:{ln}')
    (WORK / 'writer_diff.json').write_text(
        json.dumps(result, ensure_ascii=False, indent=1), encoding='utf-8')
    print('\nsaved', WORK / 'writer_diff.json')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
