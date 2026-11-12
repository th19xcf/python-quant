"""只读解析 v2：带作用域常量求值，解析写点/读点 -> 表边。

覆盖 src/、tools/、_staging/（后者用于识别「只能靠一次性脚本更新」的表）。
"""
from __future__ import annotations

import ast
import json
import os
import re
from pathlib import Path

ROOT = Path(r"e:\code\python\quant")
WORK = ROOT / "2026-11-12" / "data-lineage" / "work"
scan = json.loads((WORK / "code_scan.json").read_text(encoding="utf-8"))
tables: dict[str, dict] = scan["tables"]

WRITE_FUNCS = {
    "write_parquet_atomic",
    "write_text_atomic",
    "write_bytes_atomic",
    "write_parquet",
    "write_csv",
    "write_json",
    "to_parquet",
    "to_csv",
    "save",
    "sink_parquet",
    "write_excel",
}
READ_FUNCS = {
    "read_parquet",
    "scan_parquet",
    "read_csv",
    "scan_csv",
    "read_json",
    "read_ipc",
    "read_table",
    "load",
}

VARIANTS: dict[str, list[tuple[str, str]]] = {}
for rel, meta in tables.items():
    if "{code}" in rel:
        d = rel.rsplit("/", 1)[0]
        VARIANTS[meta["path"]] = [(d + "/", "high")]
        continue
    vs = [(rel, "high")]
    base = rel.rsplit("/", 1)[-1]
    stem = base.rsplit(".", 1)[0]
    if not re.match(r"^(manifest|coverage|meta|index|summary)$", stem):
        vs.append((base, "mid"))
        vs.append((stem, "low"))
    VARIANTS[meta["path"]] = vs

ORDER = {"high": 3, "mid": 2, "low": 1}


def match_text(text: str) -> list[tuple[str, str]]:
    hits: dict[str, str] = {}
    if not text:
        return []
    t = text.replace("\\", "/")
    for tpath, vs in VARIANTS.items():
        for pat, conf in vs:
            if pat in t:
                if tpath not in hits or ORDER[conf] > ORDER[hits[tpath]]:
                    hits[tpath] = conf
                break
    return list(hits.items())


class Env:
    def __init__(self, parent: "Env | None" = None) -> None:
        self.map: dict[str, ast.AST] = {}
        self.parent = parent

    def get(self, name: str) -> ast.AST | None:
        e: Env | None = self
        while e is not None:
            if name in e.map:
                return e.map[name]
            e = e.parent
        return None


def collect_env(node: ast.AST, env: Env) -> None:
    for stmt in ast.walk(node):
        if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1:
            t = stmt.targets[0]
            if isinstance(t, ast.Name):
                env.map[t.id] = stmt.value
        elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name) and stmt.value:
            env.map[stmt.target.id] = stmt.value
        elif isinstance(stmt, ast.withitem):
            pass


def evaluate(node: ast.AST, env: Env, depth: int = 0) -> tuple[str, bool]:
    """返回 (路径文本, 是否完整解析)。未解析部分用 <unk> 表示但保留已知字面量。"""
    if depth > 6:
        return "<deep>", False
    if isinstance(node, ast.Constant):
        return (node.value, True) if isinstance(node.value, str) else ("<const>", False)
    if isinstance(node, ast.Name):
        bound = env.get(node.id)
        if bound is None:
            return f"<{node.id}>", False
        return evaluate(bound, env, depth + 1)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
        left, lok = evaluate(node.left, env, depth + 1)
        right, rok = evaluate(node.right, env, depth + 1)
        return left.rstrip("/") + "/" + right.lstrip("/"), lok and rok
    if isinstance(node, ast.JoinedStr):
        parts = []
        ok = True
        for v in node.values:
            if isinstance(v, ast.Constant):
                parts.append(str(v.value))
            elif isinstance(v, ast.FormattedValue):
                s, k = evaluate(v.value, env, depth + 1)
                parts.append(s if k else "<f>")
                ok = ok and k
        return "".join(parts), ok
    if isinstance(node, ast.Call):
        f = node.func
        name = f.attr if isinstance(f, ast.Attribute) else (f.id if isinstance(f, ast.Name) else "")
        if name in {"joinpath", "join"}:
            parts = [evaluate(a, env, depth + 1) for a in node.args]
            return "/".join(p[0].strip("/") for p in parts), all(p[1] for p in parts)
        if name in {"Path", "str", "resolve", "absolute", "expanduser", "with_suffix"}:
            if node.args:
                return evaluate(node.args[0], env, depth + 1)
        return f"<call:{name}>", False
    if isinstance(node, ast.Call):
        return "<call>", False
    if isinstance(node, ast.Subscript):
        return evaluate(node.value, env, depth + 1)
    return "<expr>", False


def scan_file(path: Path, origin: str) -> dict:
    src = path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return {"error": "syntax"}
    mod = Env()
    collect_env(tree, mod)
    writes: list[dict] = []
    reads: list[dict] = []

    def visit_func(fn: ast.AST, stack: list[str]) -> None:
        fenv = Env(mod)
        collect_env(fn, fenv)
        for node in ast.walk(fn):
            if isinstance(node, ast.Call):
                f = node.func
                name = f.attr if isinstance(f, ast.Attribute) else (f.id if isinstance(f, ast.Name) else "")
                if name in WRITE_FUNCS or name in READ_FUNCS:
                    args = [evaluate(a, fenv) for a in node.args[:4]]
                    kw = [evaluate(k.value, fenv) for k in node.keywords if k.arg]
                    rec = {
                        "line": node.lineno,
                        "func": stack[-1] if stack else "<module>",
                        "call": name,
                        "paths": [a[0] for a in args + kw],
                        "complete": all(a[1] for a in args + kw),
                        "origin": origin,
                    }
                    (writes if name in WRITE_FUNCS else reads).append(rec)

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            visit_func(node, [node.name])
        elif isinstance(node, ast.ClassDef):
            for sub in node.body:
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    visit_func(sub, [node.name, sub.name])
        elif isinstance(node, ast.If):
            for sub in node.body:
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    visit_func(sub, [sub.name])
        elif isinstance(node, ast.Try):
            for sub in node.body:
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    visit_func(sub, [sub.name])
    # 模块级调用
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            for sub in ast.walk(node):
                if isinstance(sub, ast.Call):
                    f = sub.func
                    name = f.attr if isinstance(f, ast.Attribute) else (f.id if isinstance(f, ast.Name) else "")
                    if name in WRITE_FUNCS or name in READ_FUNCS:
                        args = [evaluate(a, mod) for a in sub.args[:4]]
                        rec = {
                            "line": sub.lineno,
                            "func": "<module>",
                            "call": name,
                            "paths": [a[0] for a in args],
                            "complete": all(a[1] for a in args),
                            "origin": origin,
                        }
                        (writes if name in WRITE_FUNCS else reads).append(rec)
    return {"writes": writes, "reads": reads}


def iter_py(base: Path):
    for dp, dn, fn in os.walk(base):
        dn[:] = [d for d in dn if d not in ("__pycache__", ".git", ".venv", "node_modules")]
        for f in fn:
            if f.endswith(".py"):
                yield Path(dp) / f


writers: dict[str, list[dict]] = {}
readers: dict[str, list[dict]] = {}
files_scanned = 0
for base, origin in ((ROOT / "src", "src"), (ROOT / "tools", "tools"), (ROOT / "_staging", "staging")):
    if not base.exists():
        continue
    for p in iter_py(base):
        rel = str(p.relative_to(ROOT)).replace("\\", "/")
        res = scan_file(p, origin)
        files_scanned += 1
        if "error" in res:
            continue
        for w in res["writes"]:
            seen = set()
            for txt in w["paths"]:
                for tpath, conf in match_text(txt):
                    if tpath in seen:
                        continue
                    seen.add(tpath)
                    writers.setdefault(tpath, []).append(
                        {
                            "file": rel,
                            "line": w["line"],
                            "func": w["func"],
                            "call": w["call"],
                            "path_expr": txt[:140],
                            "complete": w["complete"],
                            "confidence": conf,
                            "origin": w["origin"],
                        }
                    )
        for r in res["reads"]:
            seen = set()
            for txt in r["paths"]:
                for tpath, conf in match_text(txt):
                    if tpath in seen:
                        continue
                    seen.add(tpath)
                    readers.setdefault(tpath, []).append(
                        {
                            "file": rel,
                            "line": r["line"],
                            "func": r["func"],
                            "call": r["call"],
                            "path_expr": txt[:140],
                            "complete": r["complete"],
                            "confidence": conf,
                            "origin": r["origin"],
                        }
                    )

out = {"writers": writers, "readers": readers, "files_scanned": files_scanned}
(WORK / "edges_raw2.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")

print("scanned", files_scanned)
print("tables with writer:", len(writers), "/", len(tables))
no_w = [t for t in tables if tables[t]["path"] not in writers]
print("no writer:", len(no_w))
for t in sorted(no_w):
    print("  ", t)
