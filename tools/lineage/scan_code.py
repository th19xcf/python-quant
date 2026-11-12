"""只读扫描：从 src/ 与 tools/ 提取数据表路径常量、读调用、写调用。

输出 work/code_scan.json：
  - tables: 目录内表清单（由 enumerate.json + 目录扫描得到）
  - files: 每个 py 文件 -> 常量映射 / 读点 / 写点
"""
from __future__ import annotations

import ast
import json
import os
import re
from pathlib import Path

ROOT = Path(r"e:\code\python\quant")
WORK = ROOT / "2026-11-12" / "data-lineage" / "work"
ENUM = ROOT / "2026-11-12" / "authority-reverse-audit" / "work" / "enumerate.json"

WRITE_FUNCS = {
    "write_parquet_atomic",
    "write_text_atomic",
    "write_parquet",
    "write_csv",
    "write_json",
    "to_parquet",
    "write_excel",
    "save",
}
READ_FUNCS = {
    "read_parquet",
    "scan_parquet",
    "read_csv",
    "read_json",
    "read_ipc",
    "load",
    "read_table",
}


def table_catalog() -> dict[str, dict]:
    d = json.loads(ENUM.read_text(encoding="utf-8"))
    files = d["files"]
    tables: dict[str, dict] = {}
    for f in files:
        rel = f["rel"]
        if f["role"] == "table":
            tables[rel] = {
                "path": "data/local_store/" + rel,
                "kind": "file",
                "bytes": f["bytes"],
                "suffix": f["suffix"],
                "rows": f.get("rows"),
                "registered": f.get("registered"),
                "reg_semantics": f.get("reg_semantics") or [],
                "manifest_status": f.get("manifest_status"),
                "consumer_hits": f.get("consumer_hits") or [],
                "consumer_level": f.get("consumer_level"),
            }
        elif f["role"] == "quote_table":
            # 单票一文件，聚合到目录表
            m = re.match(r"^quotes/(stock|index|fund|futures)/(?:[a-z]+/)?", rel)
            key = m.group(0).rstrip("/") if m else "quotes"
            if key.startswith("quotes/futures"):
                key = "quotes/futures/" + rel.split("/")[2]
            ent = tables.setdefault(
                key,
                {
                    "path": "data/local_store/" + key + "/{code}.parquet",
                    "kind": "dir_of_files",
                    "bytes": 0,
                    "suffix": ".parquet",
                    "rows": None,
                    "registered": None,
                    "reg_semantics": [],
                    "manifest_status": None,
                    "consumer_hits": [],
                    "consumer_level": None,
                    "file_count": 0,
                },
            )
            ent["bytes"] += f["bytes"]
            ent["file_count"] = ent.get("file_count", 0) + 1
            ent.setdefault("codes", set())
    for k, v in tables.items():
        if "codes" in v:
            v["codes"] = len(v["codes"])
    return tables


def _collect_consts(tree: ast.Module) -> dict[str, str]:
    """模块顶层「字符串常量赋值」，用于把路径变量还原成字面量。"""
    consts: dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            val = node.value
            if isinstance(val, ast.Constant) and isinstance(val.value, str):
                targets = (
                    [node.target] if isinstance(node, ast.AnnAssign) else node.targets
                )
                for t in targets:
                    if isinstance(t, ast.Name):
                        consts[t.id] = val.value
    return consts


class _CallVisitor(ast.NodeVisitor):
    """收集读写调用点；``stack`` 只用来回填调用点所在的函数名。"""

    def __init__(self, reads: list[dict], writes: list[dict]) -> None:
        self.reads = reads
        self.writes = writes
        self.stack: list[str] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()

    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore[assignment]

    def _call_name(self, node: ast.Call) -> str:
        f = node.func
        if isinstance(f, ast.Attribute):
            return f.attr
        if isinstance(f, ast.Name):
            return f.id
        return ""

    def _arg_repr(self, node: ast.AST) -> str:
        try:
            return ast.unparse(node)
        # ast.unparse 对极深表达式可能抛 RecursionError（RuntimeError 子类），
        # 也可能抛 TypeError；这里只是给参数生成文本，兜底成占位串即可。
        except Exception:  # noqa: BLE001
            return "<unparse-fail>"

    def visit_Call(self, node: ast.Call) -> None:
        name = self._call_name(node)
        if name in WRITE_FUNCS or name in READ_FUNCS:
            rec = {
                "line": node.lineno,
                "func": self.stack[-1] if self.stack else "<module>",
                "call": name,
                "args": [self._arg_repr(a) for a in node.args[:3]],
                "kwargs": {k.arg: self._arg_repr(k.value) for k in node.keywords if k.arg},
            }
            (self.writes if name in WRITE_FUNCS else self.reads).append(rec)
        self.generic_visit(node)


def scan_py(path: Path) -> dict:
    src = path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return {"error": "syntax"}
    consts = _collect_consts(tree)

    reads: list[dict] = []
    writes: list[dict] = []
    _CallVisitor(reads, writes).visit(tree)
    # 所有字符串字面量（用于路径命中）
    lits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            v = node.value
            if "/" in v or v.endswith(".parquet") or v.endswith(".json") or v.endswith(".csv"):
                lits.append({"line": node.lineno, "value": v})
    return {"consts": consts, "reads": reads, "writes": writes, "string_literals": lits}


def main() -> None:
    tables = table_catalog()
    files: dict[str, dict] = {}
    for base in ("src", "tools"):
        for dp, dn, fn in os.walk(ROOT / base):
            dn[:] = [d for d in dn if d != "__pycache__"]
            for f in fn:
                if not f.endswith(".py"):
                    continue
                p = Path(dp) / f
                rel = str(p.relative_to(ROOT)).replace("\\", "/")
                files[rel] = scan_py(p)
    out = {"tables": tables, "files": files}
    WORK.mkdir(parents=True, exist_ok=True)
    (WORK / "code_scan.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    print("tables", len(tables), "files scanned", len(files))


if __name__ == "__main__":
    main()
