"""扫「写盘路径不刷新 manifest」的产者，输出结构化清单。

判定链（全部可复核）：

1. 在 ``src/`` 与 ``tools/`` 的 .py 里找写盘调用点：
   ``write_parquet_atomic`` / ``write_text_atomic`` / ``write_csv_atomic`` /
   ``.write_parquet(`` / ``.write_csv(`` / ``.write_text(``；
2. 取调用点所在语句（括号配对补齐）里出现的、带数据扩展名的字符串字面量，
   逐个在 ``data/local_store`` 下找同名文件：唯一命中即得目标目录；
3. 目标目录有没有 ``manifest.json``；该文件是否登记在里面（比对 files 段 path）；
4. 该模块有没有写这个 manifest（出现 ``manifest`` 且出现 ``write_text_atomic``
   或 ``_refresh_manifest`` / ``refresh_manifest`` / ``_write_manifest`` 之一）。

严重度：
  A 高：目标目录有 manifest、产物已登记、该文件从不刷新 → 重跑必漂移
  B 中：目标目录有 manifest、产物未登记 → 不漂移但登记缺项
  C 低：目标目录无 manifest → 整层无登记
"""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path('.')
STORE = ROOT / 'data/local_store'
EXTS = ('.parquet', '.csv', '.json', '.txt')
CALL_RE = re.compile(
    r'(write_parquet_atomic|write_text_atomic|write_csv_atomic'
    r'|\.write_parquet\s*\(|\.write_csv\s*\(|\.write_text\s*\()')
STR_RE = re.compile(r"""['"]([^'"\n]{1,120}?\.(?:parquet|csv|json|txt))['"]""")
MANIFEST_HINT = re.compile(r'manifest', re.IGNORECASE)
MANIFEST_WRITE = re.compile(
    r'(refresh_manifest|_write_manifest|write_manifest|manifest\.json)')


def call_text(lines: list[str], start: int, limit: int = 8) -> str:
    """从调用点起拼到括号平衡（最多 limit 行）。"""
    chunk = []
    depth = 0
    for line in lines[start:start + limit]:
        chunk.append(line)
        depth += line.count('(') - line.count(')')
        if len(chunk) > 1 and depth <= 0:
            break
    return '\n'.join(chunk)


def store_files() -> dict[str, list[Path]]:
    index: dict[str, list[Path]] = {}
    for path in STORE.rglob('*'):
        if path.is_file() and path.suffix.lower() in EXTS:
            index.setdefault(path.name, []).append(path)
    return index


def manifest_registry(manifest: Path, store: Path) -> set[str]:
    try:
        payload = json.loads(manifest.read_text(encoding='utf-8'))
    except Exception:  # noqa: BLE001
        return set()
    out: set[str] = set()

    def walk(node: object) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key in ('path', 'source_parquet') and isinstance(value, str):
                    out.add(value.replace('\\', '/'))
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return out


def classify_level(manifest: Path, registered: bool, writes_manifest: bool) -> str:
    """按模块 docstring 的四段判定链给严重度：C / B / ok / A。

    与原先内联的 if-elif 链逐分支等价，只是把判定从 ``main`` 里提出来。
    """
    if not manifest.is_file():
        return 'C'
    if not registered:
        return 'B'
    return 'ok' if writes_manifest else 'A'


def main() -> None:
    index = store_files()
    rows: list[dict[str, object]] = []
    for base in ('src', 'tools'):
        for path in sorted((ROOT / base).rglob('*.py')):
            text = path.read_text(encoding='utf-8', errors='ignore')
            if 'local_store' not in text and 'store_root' not in text:
                continue
            lines = text.splitlines()
            has_manifest = bool(MANIFEST_HINT.search(text))
            has_manifest_write = bool(MANIFEST_WRITE.search(text))
            for idx, line in enumerate(lines):
                if not CALL_RE.search(line):
                    continue
                block = call_text(lines, idx)
                names = STR_RE.findall(block)
                for name in names:
                    hits = index.get(Path(name).name, [])
                    if len(hits) != 1:
                        continue
                    target = hits[0]
                    manifest = target.parent / 'manifest.json'
                    registered = False
                    if manifest.is_file():
                        registered = any(
                            Path(r).name == target.name
                            for r in manifest_registry(manifest, STORE))
                    level = classify_level(manifest, registered, has_manifest_write)
                    rows.append({
                        'file': path.relative_to(ROOT).as_posix(),
                        'line': idx + 1,
                        'product': target.relative_to(STORE).as_posix(),
                        'dir_manifest': manifest.is_file(),
                        'registered': registered,
                        'module_touches_manifest': has_manifest,
                        'module_writes_manifest': has_manifest_write,
                        'level': level,
                    })

    out = ROOT / '2026-11-12/data-lineage/work/producer_scan.json'
    out.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding='utf-8')

    by_level: dict[str, int] = {}
    for row in rows:
        by_level[str(row['level'])] = by_level.get(str(row['level']), 0) + 1
    print('调用点条目', len(rows), by_level)

    for level in ('A', 'B', 'C'):
        subset = [r for r in rows if r['level'] == level]
        print(f'\n=== {level} 级：{len(subset)} 条 ===')
        seen: dict[tuple[str, str], int] = {}
        for row in subset:
            key = (str(row['file']), str(row['product']).rsplit('/', 1)[0])
            seen[key] = seen.get(key, 0) + 1
        for (file, prod_dir), n in sorted(seen.items(),
                                          key=lambda kv: (-kv[1], kv[0])):
            targets = sorted({str(r['product']).rsplit('/', 1)[-1]
                              for r in subset
                              if r['file'] == file
                              and str(r['product']).rsplit('/', 1)[0] == prod_dir})
            lines_hit = [r['line'] for r in subset
                         if r['file'] == file
                         and str(r['product']).rsplit('/', 1)[0] == prod_dir]
            print(f'  {file}  行 {lines_hit}  → {prod_dir}/  '
                  f'{len(targets)} 个产物: {targets[:4]}')


if __name__ == '__main__':
    main()
