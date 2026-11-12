"""权威路径清单覆盖面校验：按「章节 + 文件级片段」严格求差，不依赖行号。

用途：校验 `docs/数据权威路径清单.md` 是否把 `data/local_store/**` 下的每一张正式 parquet 表
都登记到位。原现场脚本是 `2026-11-12/doc-registry/work/strict_diff.py`（按行窗 `11-85` + `100-101`
定位），本脚本把定位方式改成**按章节标题 + 只取表格行**，清单增删行、段落挪位都不会让窗口失效。

口径（与清单「表数与覆盖口径」注记一致）：
- 一个登记行可覆盖多张表：`{a,b}` 展开后每个文件各算一张表；
- 只有以 `.parquet` 结尾（或 `{a,b}` 展开后以 `.parquet` 结尾）的片段算登记；
- 目录级片段（`meta/st/`）、`.json` / `.csv`、说明段与引用段里的路径一律不计覆盖；
- 分组总数与登记片段数都从磁盘 / 清单实测得出，**不写死期望值**（原先的 84 是当时快照）。

退出码：0 = 全覆盖；1 = 存在未覆盖分组（清单需补登记）；2 = 清单章节定位失败。

用法：
    .venv\\Scripts\\python.exe tools\\check_authority_coverage.py
    .venv\\Scripts\\python.exe tools\\check_authority_coverage.py --doc docs/数据权威路径清单.md
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
STORE = REPO / "data" / "local_store"
DEFAULT_DOC = REPO / "docs" / "数据权威路径清单.md"
# 计入覆盖的章节：主表 + 非数据资产节（后者即便登记了 parquet 也按同一口径计入）
SECTIONS = ("权威路径", "非数据资产（操作产物）")


def collect_groups(store: Path) -> dict[str, dict]:
    """把正式层 parquet 文件归并成「表分组」：quotes 分片按标的名、按日分片按日期掩码。"""
    groups: dict[str, dict] = {}
    for p in store.rglob("*.parquet"):
        rel = p.relative_to(store).as_posix()
        if rel.startswith(("_staging/", "_backup/")):
            continue
        parts = rel.split("/")
        if parts[0] == "quotes" and len(parts) == 4:
            key = "/".join(parts[:3]) + "/<ts_code>.parquet"
        elif re.search(r"_\d{8}\.parquet$", rel):
            key = re.sub(r"_\d{8}\.parquet$", "_<YYYYMMDD>.parquet", rel)
        else:
            key = rel
        d = groups.setdefault(key, {"files": 0, "bytes": 0, "sample": rel})
        d["files"] += 1
        d["bytes"] += p.stat().st_size
    return groups


def section_lines(lines: list[str], title: str) -> list[str]:
    """取 `## <title>` 到下一个 `## ` 之间的正文行（标题按去空白后的前缀匹配）。"""
    head = "## " + title
    out: list[str] = []
    inside = False
    for ln in lines:
        if ln.startswith("## ") and not ln.startswith("### "):
            inside = ln.strip().startswith(head)
            continue
        if inside:
            out.append(ln)
    return out


def table_spans(lines: list[str], title: str) -> list[str]:
    """只取章节内表格行（以 `|` 开头）的反引号片段，跳过说明段与引用段。"""
    spans: list[str] = []
    for ln in section_lines(lines, title):
        if not ln.lstrip().startswith("|"):
            continue
        spans.extend(re.findall(r"`([^`\n]{4,200})`", ln))
    return spans


def to_regex(s: str) -> str | None:
    """文件级片段转正则；目录级 / 非 parquet / 带空格或 URL 的一律返回 None（不计覆盖）。"""
    s = s.strip()
    if " " in s or "http" in s or not s:
        return None
    s = s.removeprefix("data/local_store/").lstrip("/")
    if not s.startswith(("quotes/", "factors/", "meta/")):
        return None
    out = []
    i = 0
    while i < len(s):
        c = s[i]
        if c == "{":
            j = s.find("}", i)
            if j < 0:
                out.append(re.escape(c))
                i += 1
                continue
            inner = s[i + 1 : j]
            out.append("(?:" + "|".join(re.escape(x) for x in inner.split(",")) + ")")
            i = j + 1
        elif c == "<":
            j = s.find(">", i)
            if j < 0:
                out.append(re.escape(c))
                i += 1
                continue
            out.append(r"[^/]+")
            i = j + 1
        else:
            out.append(re.escape(c))
            i += 1
    pat = "".join(out)
    if not pat.endswith(".parquet"):
        return None
    return "^" + pat + "$"


def main() -> int:
    ap = argparse.ArgumentParser(description="权威路径清单覆盖面校验（只读）")
    ap.add_argument("--doc", default=str(DEFAULT_DOC), help="清单路径，默认 docs/数据权威路径清单.md")
    args = ap.parse_args()

    doc = Path(args.doc)
    if not doc.is_absolute():
        doc = REPO / doc
    lines = doc.read_text(encoding="utf-8", errors="replace").splitlines()

    if not any(ln.strip().startswith("## " + t) for t in SECTIONS for ln in lines):
        print(f"[FAIL] 清单里找不到章节：{SECTIONS}，无法定位登记表")
        return 2

    groups = collect_groups(STORE)
    spans = [sp for t in SECTIONS for sp in table_spans(lines, t)]
    regs = [re.compile(r) for sp in spans if (r := to_regex(sp)) is not None]

    matched, unmatched = [], []
    for key, d in sorted(groups.items()):
        hit = any(rx.match(key) for rx in regs)
        (matched if hit else unmatched).append({"key": key, **d})

    print("=== 权威路径覆盖面校验 ===")
    print(f"清单: {doc.relative_to(REPO).as_posix() if doc.is_relative_to(REPO) else doc}")
    print(f"章节: {' / '.join(SECTIONS)}")
    print(f"parquet 分组: {len(groups)}  文件级登记片段: {len(regs)}")
    print(f"已覆盖分组: {len(matched)}  未覆盖分组: {len(unmatched)}")
    print("\n--- 未覆盖 ---")
    for r in unmatched:
        print(f"{r['key']}  files={r['files']} bytes={r['bytes']}  sample={r['sample']}")
    if unmatched:
        print(f"\n[FAIL] {len(unmatched)} 个分组未登记，请在清单主表补文件级登记行")
        return 1
    print(f"\n[PASS] {len(matched)}/{len(groups)} 全覆盖")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
