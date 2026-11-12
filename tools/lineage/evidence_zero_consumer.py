"""「零消费者」四通道取证（纯只读）。

通道：
  1. 路径常量：路径解析入口（store.py / 任何 *_paths.py / registry 类）是否注册
  2. 字符串字面量：src/ tools/ research/ tests/ docs/ config/ 2026-11-12/ 全扫，按代码/文档分列
  3. manifest sources/consumers：全部 manifest json 里是否有表把它登记为上游/下游
  4. 间接消费：把 tools/ 校验脚本、tests/、research/ 与 src/ 生产链路分开统计

输出 outputs/zero_consumer_evidence.json + .md
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

ROOT = Path(r"e:\code\python\quant")
OUT = ROOT / "2026-11-12" / "data-lineage" / "outputs"
OUT.mkdir(parents=True, exist_ok=True)

TOKENS = {
    "layers_L0": "l0_all_history",
    "layers_L1": "l1_listed_120d",
    "layers_L2": "l2_tradable",
    "layers_L3": "l3_risk_excluded",
    "layers_L4": "l4_liquid",
    "layers_L5": "l5_pool",
    "layers_dir": "universe/layers",
    "state_vector": "state_vector_daily",
}

SEARCH_ROOTS = ["src", "tools", "research", "tests", "docs", "config", "2026-11-12", "_staging"]
TEXT_EXT = {".py", ".md", ".json", ".csv", ".tsv", ".yaml", ".yml", ".toml", ".txt", ".ini"}
SKIP_PARTS = {".git", ".venv", "__pycache__", "node_modules", "quotes", "local_store", "outputs", "work"}


def iter_files():
    for r in SEARCH_ROOTS:
        base = ROOT / r
        if not base.exists():
            continue
        for dp, dn, fn in os.walk(base):
            dn[:] = [d for d in dn if d not in SKIP_PARTS and not d.startswith("ci_cov_sim")]
            for f in fn:
                p = Path(dp) / f
                if p.suffix.lower() in TEXT_EXT:
                    yield p


def classify(p: Path) -> str:
    rel = str(p.relative_to(ROOT)).replace("\\", "/")
    if rel.startswith("docs/") or rel.endswith(".md"):
        return "doc"
    if rel.startswith("tests/"):
        return "test"
    if rel.startswith("2026-11-12/"):
        return "task_script"
    if rel.startswith("_staging/"):
        return "oneoff_script"
    if rel.startswith("research/"):
        return "research"
    if rel.startswith("src/"):
        return "src"
    if rel.startswith("tools/"):
        return "tool"
    return "other"


hits: dict[str, list[dict]] = {k: [] for k in TOKENS}
for p in iter_files():
    try:
        txt = p.read_text(encoding="utf-8", errors="ignore")
    # errors="ignore" 已消掉解码异常，剩下的唯一来源是文件系统 OSError（权限/占用/消失）。
    # continue 是本通道的设计意图：单个文件读不到不应中断整体取证；本脚本不走 logger，
    # 免得把告警混进 stdout 的固定报告格式。
    except OSError:  # noqa: S112
        continue
    if not any(t in txt for t in TOKENS.values()):
        continue
    rel = str(p.relative_to(ROOT)).replace("\\", "/")
    kind = classify(p)
    for i, line in enumerate(txt.split("\n"), 1):
        for key, tok in TOKENS.items():
            if tok in line:
                hits[key].append({"file": rel, "line": i, "kind": kind, "text": line.strip()[:180]})

# 通道 1：路径常量注册
registry_files = [
    "src/data/local_store/store.py",
    "src/data/local_store/paths.py",
    "src/data/local_store/__init__.py",
    "src/data/quality/freshness_config.json",
    "tools/manifest_tool.py",
]
registry = {}
for rf in registry_files:
    p = ROOT / rf
    if not p.exists():
        registry[rf] = "MISSING"
        continue
    txt = p.read_text(encoding="utf-8", errors="ignore")
    registry[rf] = {
        tok: [i for i, line in enumerate(txt.split("\n"), 1) if tok in line]
        for tok in TOKENS.values()
    }

# 通道 3：manifest 里的 sources / consumers 提及
manifest_mentions = {k: [] for k in TOKENS}
man_count = 0
for p in (ROOT / "data" / "local_store").rglob("*manifest*.json"):
    man_count += 1
    try:
        txt = p.read_text(encoding="utf-8", errors="ignore")
    # 同通道 2：errors="ignore" 之后只剩 OSError；单份 manifest 读不到即跳过。
    except OSError:  # noqa: S112
        continue
    rel = str(p.relative_to(ROOT)).replace("\\", "/")
    for key, tok in TOKENS.items():
        if tok in txt:
            manifest_mentions[key].append(rel)

summary = []
for key, tok in TOKENS.items():
    by_kind: dict[str, set[str]] = {}
    for h in hits[key]:
        by_kind.setdefault(h["kind"], set()).add(h["file"])
    code_kinds = ("src", "tool", "research", "test")
    code_files = sorted({h["file"] for h in hits[key] if h["kind"] in code_kinds})
    summary.append(
        {
            "token": tok,
            "total_lines": len(hits[key]),
            "files_by_kind": {k: sorted(v) for k, v in sorted(by_kind.items())},
            "code_files": code_files,
            "code_file_count": len(code_files),
            "doc_file_count": len(by_kind.get("doc", ())),
            "manifest_mentions": sorted(set(manifest_mentions[key])),
        }
    )

payload = {
    "search_roots": SEARCH_ROOTS,
    "text_hits": hits,
    "registry_channel": registry,
    "manifest_scanned": man_count,
    "summary": summary,
}
(OUT / "zero_consumer_evidence.json").write_text(
    json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8"
)

lines = ["# 零消费者四通道取证（自动生成）", "", f"扫描根：{', '.join(SEARCH_ROOTS)}；manifest 文件数：{man_count}", ""]
for s in summary:
    lines.append(f"## `{s['token']}` —— 代码引用 {s['code_file_count']} 个文件 / 文档 {s['doc_file_count']} 个文件 / 命中行 {s['total_lines']}")
    for kind, files in s["files_by_kind"].items():
        lines.append(f"- **{kind}**（{len(files)}）：" + ("、".join(f"`{f}`" for f in files) if files else "无"))
    lines.append(f"- manifest 提及：{('、'.join('`'+m+'`' for m in s['manifest_mentions'])) if s['manifest_mentions'] else '无'}")
    lines.append("")
lines.append("## 通道 1：路径常量注册检查")
for rf, info in registry.items():
    lines.append(f"- `{rf}`：{info}")
(OUT / "zero_consumer_evidence.md").write_text("\n".join(lines), encoding="utf-8")
print("\n".join(lines)[:6000])
