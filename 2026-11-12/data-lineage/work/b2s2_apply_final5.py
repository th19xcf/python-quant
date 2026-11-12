# ruff: noqa: T201, E741, B905
"""落定 5 条判定 + 修 G-117/G-127 行内转义竖线造成的列错位（只动竖线）。

判定来源：父会话 2026-11-13 明确落定（G-067 可补 / G-098 待核实 /
G-089 接受偏差 / G-090 接受偏差 / G-117 待核实）。

写盘口径：DG-11 快照 → 原子替换（write_text_atomic，参数顺序 text, path）。
断言：每条旧判定串在全文中必须恰好命中 1 次；竖线替换命中数必须与预期相等。
"""
from __future__ import annotations

import hashlib
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from src.data.atomic_write import write_text_atomic  # noqa: E402

DOC = ROOT / 'docs' / '数据缺口登记_正式表_2026-11-12.md'
SNAP = ROOT / '_backup' / 'docs_gap_registry_20261113d' / DOC.name

# 编号 -> (旧判定原文, 新判定原文)
VERDICT = {
    'G-067': (
        '待处置（第二段 A 已解锁：`quotes` 线于 2026-11-12 收口）',
        '可补（第二段 A 已解锁：`quotes` 线于 2026-11-12 收口；动作是执行重跑，不再核实）',
    ),
    'G-089': (
        '不修（不可反向重建；分片按重复残留退役即可）',
        '接受偏差（不可反向重建；分片按重复残留退役属残留清理，清理完成前不记已退役）',
    ),
    'G-090': (
        '不修（登记缺陷 + 硬约束；成因未取证）',
        '接受偏差（登记缺陷 + 硬约束；成因未取证，决策为不修）',
    ),
    'G-098': (
        '待处置（需定「固定无日期正式路径 + 版本写进 manifest」的迁移口径，'
        '属改变既定口径，需上层拍板）；本轮只登记，不动数据、不改代码',
        '待核实（需定「固定无日期正式路径 + 版本写进 manifest」的迁移口径，'
        '属改变既定口径，需上层拍板；决定落地前不冒充进度）；本轮只登记，不动数据、不改代码',
    ),
}

# 行内转义竖线 -> 全角竖线；只这两行被授权/本路新增
PIPE_FIX = {'G-117': 4, 'G-127': 2}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def main() -> int:
    before_sha = sha256(DOC)
    raw = DOC.read_text(encoding='utf-8')
    before_lines = len(raw.split('\n'))
    before_bytes = len(DOC.read_bytes())
    print(f'[before] sha256={before_sha} bytes={before_bytes} lines={before_lines}')

    SNAP.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(DOC, SNAP)
    assert sha256(SNAP) == before_sha, '快照 SHA256 与原文不一致'
    print(f'[snapshot] {SNAP}')

    out = raw
    for gid, (old, new) in VERDICT.items():
        needle = f'| {old} |'
        cnt = out.count(needle)
        assert cnt == 1, f'{gid} 旧判定串命中 {cnt} 次（应为 1）'
        out = out.replace(needle, f'| {new} |', 1)
        print(f'[verdict] {gid}: {old}\n        -> {new}')

    lines = out.split('\n')
    for gid, expected in PIPE_FIX.items():
        idx = next(i for i, l in enumerate(lines)
                   if l.strip().startswith(f'| {gid} |'))
        occ = lines[idx].count('\\|')
        assert occ == expected, f'{gid} 转义竖线 {occ} 处（预期 {expected}）'
        lines[idx] = lines[idx].replace('\\|', '｜')
        cells = len(lines[idx].strip().split('|')) - 2
        assert cells == 8, f'{gid} 修后列数 {cells}（应为 8）'
        print(f'[pipe] {gid} line={idx + 1}: {occ} 处 -> 全角，列数 8')
    out = '\n'.join(lines)

    assert out != raw, '内容未发生任何变化'
    write_text_atomic(out, DOC)
    after_sha = sha256(DOC)
    after_bytes = len(DOC.read_bytes())
    after_lines = len(out.split('\n'))
    print(f'[after] sha256={after_sha} bytes={after_bytes} lines={after_lines} '
          f'(delta bytes {after_bytes - before_bytes:+d}, lines {after_lines - before_lines:+d})')

    # 全表列数复核
    sec3 = [i for i, l in enumerate(out.split('\n')) if l.startswith('## 3.')][0]
    sec4 = [i for i, l in enumerate(out.split('\n')) if l.startswith('## 4.')][0]
    bad = [(i + 1, l.strip().split('|')[1].strip(), len(l.strip().split('|')) - 2)
           for i, l in enumerate(out.split('\n'))
           if sec3 < i < sec4 and l.strip().startswith('| G-')
           and len(l.strip().split('|')) - 2 != 8]
    print(f'[colcheck] 仍非 8 列的行: {bad}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
