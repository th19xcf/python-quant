"""静态守卫：扫描 ``src/`` 与 ``tools/`` 里绕过 ``write_dataset`` 的裸 parquet 写入。

口径与 ``docs/裸写替换清单_2026-11-22.md`` 对齐（``.write_parquet(`` / ``.to_parquet(`` /
``write_parquet_atomic(``），另报 ``pq.write_table`` / ``pl.write_parquet`` 扩展口径。

只有「真正面向数据集」的裸写才计入清单与基线；研究输出、staging/缓存/血缘/一次性
脚本按 ``config/raw_write_exemptions.json`` 显式豁免（逐目录 / 逐文件 / 逐行，均带理由）。
只告警、不阻断日更；清单处数只允许下降，超出基线即视为新增裸写异常。
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOC_PATH = PROJECT_ROOT / 'docs' / '裸写替换清单_2026-11-22.md'
EXEMPTION_PATH = PROJECT_ROOT / 'config' / 'raw_write_exemptions.json'

#: 与替换清单一致的三类裸写调用。
DOC_PATTERNS = ('.write_parquet(', '.to_parquet(', 'write_parquet_atomic(')
#: 清单未单列、但同属裸写的扩展调用。
EXTRA_PATTERNS = ('pq.write_table(', 'pl.write_parquet(')

#: 排除：write_dataset/原子写自身实现、测试、work、备份与缓存。
EXCLUDED_FILES = {'src/data/dataset_contracts.py', 'src/data/atomic_write.py',
                  'src/data/raw_write_scan.py'}
EXCLUDED_DIRS = {'tests', 'work', '_backup', '__pycache__'}

#: 清单缺失时的兜底基线（tools 侧剩余处数）。
FALLBACK_BASELINE = 1


def doc_baseline() -> int:
    """从替换清单里读取 tools 侧剩余处数。"""
    try:
        text = DOC_PATH.read_text(encoding='utf-8')
    except OSError:
        return FALLBACK_BASELINE
    match = re.search(r'tools 侧剩余\s*(\d+)\s*处', text)
    return int(match.group(1)) if match else FALLBACK_BASELINE


def load_exemptions() -> dict[str, Any]:
    """读取豁免白名单；文件缺失时返回空表（即全部裸写都算数据集裸写）。"""
    try:
        payload = json.loads(EXEMPTION_PATH.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {'directories': [], 'files': [], 'sites': []}
    payload.setdefault('directories', [])
    payload.setdefault('files', [])
    payload.setdefault('sites', [])
    return payload


def _excluded(path: Path, base: Path) -> bool:
    rel = path.relative_to(base)
    if rel.as_posix() in EXCLUDED_FILES or rel.as_posix().startswith('tools/_oneshot/'):
        return True
    if any(part in EXCLUDED_DIRS for part in rel.parts[:-1]):
        return True
    return path.name.startswith('test_') or path.name.endswith('_test.py')


def _dir_exempt(rel: str, payload: dict[str, Any]) -> str | None:
    for item in payload['directories']:
        if rel.startswith(str(item.get('path') or '')):
            return str(item.get('reason') or '')
    return None


def _file_exempt(rel: str, payload: dict[str, Any]) -> str | None:
    for item in payload['files']:
        if rel == str(item.get('path') or ''):
            return str(item.get('reason') or '')
    return None


def scan_raw_writes(root: Path | str | None = None) -> dict[str, Any]:
    """返回数据集裸写的文件数/处数、豁免处数、清单基线与新增判定。"""
    base = Path(root) if root is not None else PROJECT_ROOT
    payload = load_exemptions()
    site_index: dict[tuple[str, str], dict[str, Any]] = {}
    for item in payload['sites']:
        key = (str(item.get('file') or ''), str(item.get('text') or '').strip())
        site_index[key] = {'left': int(item.get('count') or 1)}
    counts = {'tools': [0, 0], 'src': [0, 0]}
    raw_counts = {'tools': [0, 0], 'src': [0, 0]}
    exempt_calls = 0
    hits: list[dict[str, Any]] = []
    remaining: list[dict[str, Any]] = []
    for scope in ('tools', 'src'):
        for path in sorted((base / scope).rglob('*.py')):
            if _excluded(path, base):
                continue
            rel = path.relative_to(base).as_posix()
            text = path.read_text(encoding='utf-8', errors='replace')
            lines = text.splitlines()
            found = [(i + 1, line.strip()) for i, line in enumerate(lines)
                     if any(token in line for token in DOC_PATTERNS + EXTRA_PATTERNS)]
            if not found:
                continue
            raw_counts[scope][0] += 1
            raw_counts[scope][1] += len(found)
            directory_reason = _dir_exempt(rel, payload)
            file_reason = _file_exempt(rel, payload)
            if directory_reason or file_reason:
                exempt_calls += len(found)
                continue
            kept: list[tuple[int, str]] = []
            for lineno, stripped in found:
                entry = site_index.get((rel, stripped))
                if entry and entry['left'] > 0:
                    entry['left'] -= 1
                    exempt_calls += 1
                    continue
                kept.append((lineno, stripped))
            if not kept:
                continue
            doc_calls = sum(sum(text_line.count(item) for item in DOC_PATTERNS)
                            for _, text_line in kept)
            counts[scope][0] += 1
            counts[scope][1] += len(kept)
            hit = {'file': rel, 'doc_scope': doc_calls,
                   'extra': len(kept) - doc_calls,
                   'lines': [lineno for lineno, _ in kept]}
            hits.append(hit)
            remaining.extend({'file': rel, 'line': lineno, 'text': stripped}
                             for lineno, stripped in kept)
    baseline = doc_baseline()
    tools_doc_calls = sum(hit['doc_scope'] for hit in hits if hit['file'].startswith('tools/'))
    return {
        'tools_files': counts['tools'][0],
        'tools_calls': counts['tools'][1],
        'src_files': counts['src'][0],
        'src_calls': counts['src'][1],
        'raw_tools_calls': raw_counts['tools'][1],
        'raw_src_calls': raw_counts['src'][1],
        'exempt_calls': exempt_calls,
        'doc_scope_calls': tools_doc_calls,
        'baseline': baseline,
        'delta': tools_doc_calls - baseline,
        'new_writes': tools_doc_calls > baseline,
        'hits': hits,
        'remaining': remaining,
    }


if __name__ == '__main__':
    result = scan_raw_writes()
    print(f'数据集裸写 tools {result["tools_files"]} 文件 / {result["tools_calls"]} 处；'
          f'src {result["src_files"]} 文件 / {result["src_calls"]} 处；'
          f'已豁免 {result["exempt_calls"]} 处')
    print(f'清单口径 tools {result["doc_scope_calls"]} 处，基线 {result["baseline"]}，'
          f'差 {result["delta"]:+d}，'
          f'{"新增裸写异常" if result["new_writes"] else "未超基线"}')
    for item in result['remaining']:
        print(f'  剩余 {item["file"]}:{item["line"]} {item["text"]}')
