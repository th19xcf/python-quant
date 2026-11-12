# ruff: noqa: T201, E741, B905
#!/usr/bin/env python3
"""把 §3「当前判定」列 18 行可映射的自由文本收进 §0.1 七值（仅替换首标签）。

5 行语义无法映射（G-067 / G-089 / G-090 / G-098 / G-117）保持原样，另报。
"""
from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

from src.data.atomic_write import write_text_atomic

DOC = Path(r'e:\code\python\quant\docs\数据缺口登记_正式表_2026-11-12.md')
SNAP = Path(r'e:\code\python\quant\_backup\docs_gap_registry_20261113b') / DOC.name
SCAN = Path(__file__).with_name('b2s2_registry_verdict_scan.json')

NEW = {
    'G-080': '已修（B `14ce50c`；A 逐字节 0 差异，正式表一行未动）',
    'G-088': '接受偏差（历史不追溯回填；新任务硬条件已生效）',
    'G-091': '已修（迁出正式层 + 撤销探针）',
    'G-092': '接受偏差（保留历史文件名，不回改为现名）',
    'G-093': ('接受偏差（保持现状；MIXED 已随 11-12 14:59 重写消失；`files` 覆盖风险随本轮配套'
              '代码修复收口，改为 `pull_index_weight._merge_manifest` 合并语义）'),
    'G-094': '已修（定性关闭：无断链、无需搬回；权威清单相应行按「已迁出、新址见废弃路径表」处理）',
    'G-099': '接受偏差（四处常量保持指向 `_staging/`）',
    'G-100': '已修（4 个直接对位删除 + 4 个先纠正快照落点再删除）',
    'G-101': '接受偏差（不改名、不改口径；重开条件见依据列）',
    'G-102': ('已修（改为 `@pytest.mark.xfail(strict=True)`，断言、阈值、`TOP_N=15`、软筛条件'
              '**一律不动**，`reason` 挂 `G-102`）'),
    'G-111': '可补（正式表保留旧值，待定向写入）',
    'G-113': '可补（待补正式入口：把 staging 脚本固化为 `tools/` 下可反复跑的入口并配 `manifest.json`）',
    'G-114': '可补（待补来源与日期口径说明：采集入口、抓取时刻、是否为当日盘后截面；未收口）',
    'G-115': '可补（待补三项说明：全量重建入口、去重键、覆盖登记职责；未收口）',
    'G-116': '可补（待与官方成分表逐月对拍：只数、权重、增删名单；未收口）',
    'G-118': '可补（派生层冻结；需与因子重建同批修）',
    'G-119': '待核实（**unresolved**：现有本地证据只能给出区间，不定值；派生层冻结，需与因子重建同批修）',
}
# 需要整段前缀改写的两行（判定列里另有正文，只换开头标签）
PREFIX_ONLY = {
    'G-096': ('**已判定不改（列块拆分，非 DG-1 违规）**',
              '**已修（定性关闭：列块拆分，非 DG-1 违规）**'),
}


def main() -> None:
    scan = json.loads(SCAN.read_text(encoding='utf-8'))
    old_detail = {r['gid']: r for r in scan['bad_detail']}
    text = DOC.read_text(encoding='utf-8')
    lines = text.splitlines(keepends=True)
    log = []

    for gid, new in NEW.items():
        r = old_detail[gid]
        idx = r['line'] - 1
        old = r['verdict']
        raw = lines[idx]
        assert raw.rstrip('\n').strip().startswith(f'| {gid} |'), (gid, raw[:60])
        assert raw.count(f'| {old} |') == 1, (gid, 'verdict cell not unique')
        assert new not in raw, (gid, 'already applied')
        lines[idx] = raw.replace(f'| {old} |', f'| {new} |')
        log.append({'gid': gid, 'old': old, 'new': new})

    for gid, (old, new) in PREFIX_ONLY.items():
        r = old_detail[gid]
        idx = r['line'] - 1
        raw = lines[idx]
        assert old in raw and new not in raw, gid
        lines[idx] = raw.replace(old, new, 1)
        log.append({'gid': gid, 'old': old, 'new': new})

    out = ''.join(lines)
    write_text_atomic(out, DOC)

    backup_sha = hashlib.sha256(SNAP.read_bytes()).hexdigest().upper()
    now_sha = hashlib.sha256(DOC.read_bytes()).hexdigest().upper()
    print('snapshot_sha', backup_sha)
    print('after_sha   ', now_sha)
    print('rows_applied', len(log))
    print('bytes', DOC.stat().st_size, 'lines', len(DOC.read_text(encoding='utf-8').splitlines()))
    (Path(__file__).with_name('b2s2_verdict_map.json')).write_text(
        json.dumps({'snapshot': str(SNAP), 'snapshot_sha256': backup_sha,
                    'after_sha256': now_sha, 'applied': log}, ensure_ascii=False, indent=1),
        encoding='utf-8')
    for e in log:
        print(f"{e['gid']}  {e['old'][:34]}  ->  {e['new'][:34]}")


if __name__ == '__main__':
    main()
