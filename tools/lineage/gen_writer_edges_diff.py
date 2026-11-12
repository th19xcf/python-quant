"""生成 writers_detected 口径修复前后的边集合差（只读，不改任何既定产物）。

输入：
  work/lineage_before_fix.json       修复前（旧口径）血缘图，从 outputs/lineage.json 冻结而来
  work/rerun_20261113c/lineage.json  修复后（新口径）重跑
输出：
  work/writer_edges_diff.json     机读差异
  outputs/writers_detected_diff_20261113.md  人读差异清单（含两张空表的机制/证据）
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(r'e:\code\python\quant')
WORK = ROOT / '2026-11-12' / 'data-lineage' / 'work'
OUT = ROOT / '2026-11-12' / 'data-lineage' / 'outputs'

#: 修复前后的基线都冻结在 work 下：outputs/lineage.json 之后会被修复后的版本覆盖，
#: 拿它当「修复前」会让本清单在下次重生成时变成自己跟自己比。
OLD = WORK / 'lineage_before_fix.json'
NEW = WORK / 'rerun_20261113c' / 'lineage.json'

old = json.loads(OLD.read_text(encoding='utf-8'))
new = json.loads(NEW.read_text(encoding='utf-8'))


def keyed(doc: dict) -> dict[str, set[tuple[str, int, str]]]:
    return {t['id']: {(w['file'], w['line'], w['mode']) for w in t.get('writers_detected', [])}
            for t in doc['tables']}


def line_text(rel: str, line: int) -> str:
    path = ROOT / rel
    if not path.is_file():
        return '(文件不存在)'
    lines = path.read_text(encoding='utf-8', errors='ignore').splitlines()
    return lines[line - 1].strip() if 0 < line <= len(lines) else '(行号越界)'


a, b = keyed(old), keyed(new)
report: dict[str, dict] = {}
for tid in a:
    added = sorted(b[tid] - a[tid])
    removed = sorted(a[tid] - b[tid])
    if not added and not removed:
        continue
    report[tid] = {
        'added': [{'file': f, 'line': ln, 'mode': m, 'source': line_text(f, ln)}
                  for f, ln, m in added],
        'removed': [{'file': f, 'line': ln, 'mode': m, 'source': line_text(f, ln)}
                    for f, ln, m in removed],
    }

(WORK / 'writer_edges_diff.json').write_text(
    json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')

tot_add = sum(len(v['added']) for v in report.values())
tot_del = sum(len(v['removed']) for v in report.values())
lines = [
    '# writers_detected 口径修复：边集合差（2026-11-13）',
    '',
    f'- 修复前：`work/lineage_before_fix.json`（旧口径 writers_for，'
    f'{sum(len(v) for v in a.values())} 条边；由修复前的 outputs/lineage.json 冻结）',
    f'- 修复后：`work/rerun_20261113c/lineage.json`（tools/lineage_writers.py，'
    f'{sum(len(v) for v in b.values())} 条边）',
    f'- 增加 **{tot_add}** 条，减少 **{tot_del}** 条，涉及 {len(report)} 张表',
    '- 本文件只是差异清单；`governance_order.md` 与 `base_layer_calibration.csv` 未改动，'
    '是否重出施工单由主控决定。',
    '',
]
for tid in sorted(report):
    item = report[tid]
    lines += [f'## {tid}', '']
    if item['added']:
        lines += ['| 类型 | 边 | 该行源码 |', '|---|---|---|']
        for w in item['added']:
            lines.append(f"| 增 | `{w['file']}:{w['line']}` | `{w['source'][:110]}` |")
        lines.append('')
    if item['removed']:
        lines += ['| 类型 | 边 | 该行源码 |', '|---|---|---|']
        for w in item['removed']:
            lines.append(f"| 减 | `{w['file']}:{w['line']}` | `{w['source'][:110]}` |")
        lines.append('')

# 修复后仍为 0 条的 2 张表：写清机制与证据，不留空字段（不造边）
lines += [
    '## 修复后仍为 0 条写者的 2 张表',
    '',
    '### membership_intervals（写者存在，路径由注册表驱动，静态解析到此为止）',
    '',
    '- 真实写调用：`src/data/quality/extended_builders.py:629`'
    ' `_write_frame(_path(store, \'index_membership\'), frame)`（函数 `build_index_membership`，:519）',
    '- 机制：`_path` 返回 `root / _SPEC[key].path`（extended_builders.py:106-107），'
    '`_SPEC = {spec.key: spec for spec in CONTRACTS}`（:63），CONTRACTS 来自'
    ' `extended_contracts.py`，该表 path 登记在 extended_contracts.py:107。'
    '表名由注册表键解析，静态 AST 停在 `_SPEC[key].path`，故 `writers_detected` 为 0 而非无写者。',
    '- 旧口径那条边是 `extended_builders.py:25` 的模块 docstring 提及，不是写调用。',
    '- 「写入口」列已改写为上面这条真实写调用（tools/lineage/build_lineage.py 表定义），不再指向'
    ' `tools/rebuild_index_constituents.py`——该脚本 :15 明确声明不写权威路径。',
    '',
    '### implied_suspension（仓库内确无常规定义入口）',
    '',
    '- 全仓引用 `implied_suspension.parquet` 的 12 处全是读或文档：'
    '`src/data/quality/suspension_union.py:27`（并表源）、`:130`（读取校验）、'
    '`src/data/quality/clean_set.py:974`（`_load_implied_suspension`）、'
    '`tools/build_universe_layers.py:87`、`tools/check_entity_dedupe.py:77`、'
    '`tests/test_suspension_union.py:25`（只往临时目录写夹具）。',
    '- 没有任何 `write_parquet*` / `to_parquet` 指向该路径，写入方为仓库外一次性脚本。',
    '- 「写入口」列已写明「无常规定义入口 + 上述证据」，不留空字段；该表仍按不可复现资产登记缺陷。',
    '',
]
(OUT / 'writers_detected_diff_20261113.md').write_text('\n'.join(lines), encoding='utf-8')
print(f'tables {len(report)} added {tot_add} removed {tot_del}')
