"""修前 vs 修后 eval_results.json 逐字段对照（只读），结果写 compare_results.txt。"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / 'reports' / 'l1_baseline'
before = json.loads((OUT / 'eval_results_before.json').read_text(encoding='utf-8'))
after = json.loads((OUT / 'eval_results.json').read_text(encoding='utf-8'))

buf: list[str] = []


def say(msg: str = '') -> None:
    buf.append(msg)
    print(msg, flush=True)


def leaves(obj, prefix=''):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from leaves(v, f'{prefix}.{k}' if prefix else k)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from leaves(v, f'{prefix}[{i}]')
    else:
        yield prefix, obj


def diff(a, b):
    la, lb = dict(leaves(a)), dict(leaves(b))
    out = []
    for k, va in la.items():
        if k not in lb:
            out.append((k, va, '<新增后缺失>'))
        elif isinstance(va, (int, float)) and isinstance(lb[k], (int, float)) \
                and not isinstance(va, bool):
            if va != lb[k]:
                out.append((k, va, lb[k]))
        elif va != lb[k]:
            out.append((k, va, lb[k]))
    return out


say('== 合成侧（orient=1，应逐位不变）==')
bad = 0
for h in ('h20', 'h5', 'h60'):
    for side in ('composite_oo', 'composite_c2c'):
        d = diff(before[h][side], after[h][side])
        if d:
            bad += len(d)
            say(f'  [{h}/{side}] {len(d)} 处差异:')
            for k, va, vb in d[:10]:
                say(f'    {k}: {va} -> {vb}')
        else:
            say(f'  [{h}/{side}] 无差异（新增键 '
                f'{len(dict(leaves(after[h][side]))) - len(dict(leaves(before[h][side])))} 个）')

say('\n== 单因子侧 ==')
for h in ('h20', 'h5', 'h60'):
    for tag, s in after[h]['singles_oo'].items():
        old = before[h]['singles_oo'][tag]
        d = diff(old, s)
        fac, orient = s['factor'], s['orient']
        say(f'  [{h}/{tag}] {fac} orient={orient:+.0f} 改动 {len(d)} 处')
        for k, va, vb in d:
            if isinstance(va, float):
                say(f'    {k}: {va:.6f} -> {vb:.6f}')
            else:
                say(f'    {k}: {va} -> {vb}')
        if orient > 0 and d:
            say('    !! 正向因子不应有改动')
            bad += 1

say('\n== vs_single_best_h20 ==')
d = diff(before['vs_single_best_h20'], after['vs_single_best_h20'])
for k, va, vb in d:
    say(f'  {k}: {va} -> {vb}')
say(f'  改动 {len(d)} 处（预期仅 q5_excess_bp_best / d_q5_excess_bp / turnover_best）')

say('\n== 新增断言块 ==')
say('  orient_assertions: '
    f'{after.get("orient_assertions", {}).get("n_pass")} / '
    f'{after.get("orient_assertions", {}).get("n_checks")}')

say(f'\n结论: 合成侧差异 {bad} 处（应为 0）')
(OUT / 'compare_results.txt').write_text('\n'.join(buf) + '\n', encoding='utf-8')
sys.exit(0 if bad == 0 else 1)
