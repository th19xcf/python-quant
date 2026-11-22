#!/usr/bin/env python3
"""派单成本 / 产出比周报（只读，标准库）。

把 ``logs/task_ledger.csv`` 的调用记录与 ``work/<任务名>_<日期>/`` 的产物规模对齐，
列出「高消耗低产出」任务，供每周复核（阈值与处置见 ``.trae/rules/token-efficiency.md``）。

可得维度
--------
- 台账：``session_id`` / ``title`` / ``phase`` / 轮次 / 调用数 / 预算 ``budget_calls`` / 耗时 / 提交短哈希；
- 超支：``tool_calls`` 与可选的 ``budget_calls`` 相比，得出超支次数、超支幅度与命中率；
- work 目录：产物文件数（排除 ``__pycache__`` 与 ``.pyc``）、报告字节数（``.md``）。

不可得（脚本在输出的「不可得」段如实列出，不推测、不造数）
----------------------------------------------------------
- 真实 token 消耗：本地无任何逐会话 token 记录；
- 会话标题：本地没有 ``session_id -> 标题`` 映射，只能用 work 目录名与台账 ``title`` 归一化匹配；
- 未记调用数的台账行：只计数，不参与「调用 / 产物」比值。

用法::

    python tools/dispatch_cost_report.py --days 7
    python tools/dispatch_cost_report.py --days 7 --min-calls 30 --json
    python tools/dispatch_cost_report.py --days 30 --out work/派单成本周报_2026-11-22.md

默认只打印到 stdout；``--out`` 才写文件。``--json`` 输出机器可读结构。
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEDGER = ROOT / 'logs' / 'task_ledger.csv'
DEFAULT_WORK = ROOT / 'work'

#: 日期后缀：``个股两融买点_2026-11-22`` -> ``个股两融买点``。
_DATE_SUFFIX = re.compile(r'[_\- ]?\d{4}-\d{2}-\d{2}\s*$')
#: 台账里表示「取不到」的占位值。
_UNKNOWN_VALUES = {'', 'unknown', 'none', 'null', '-'}


def norm_name(text: Any) -> str:
    """归一化任务名：去日期后缀、去空白、转小写，用于台账 title 与 work 目录名对齐。"""
    value = _DATE_SUFFIX.sub('', str(text or '').strip())
    return re.sub(r'\s+', '', value).lower()


def _num(value: Any) -> float | None:
    text = str(value or '').strip()
    if text.lower() in _UNKNOWN_VALUES:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_day(value: Any) -> date | None:
    text = str(value or '').strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def read_ledger(path: Path) -> list[dict[str, str]]:
    """读台账；文件不存在返回空表。列缺失时按空串处理，不因旧表头报错。"""
    if not path.is_file():
        return []
    with path.open('r', encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        rows: list[dict[str, str]] = []
        for raw in reader:
            raw.pop(None, None)
            rows.append({str(k): str(v or '') for k, v in raw.items() if k is not None})
        return rows


def fold_tasks(rows: list[dict[str, str]], since: date | None) -> list[dict[str, Any]]:
    """按 ``session_id + title`` 折叠成一任务一行；数值取最后一条非空值。

    日期过滤用 ``dispatched_at``（done 行沿用派单时间）；取不到日期的行保留，
    避免把「账没记全」误判成「没派过」。
    """
    folded: dict[tuple[str, str], dict[str, Any]] = {}
    order: list[tuple[str, str]] = []
    for row in rows:
        day = _parse_day(row.get('dispatched_at'))
        if since is not None and day is not None and day < since:
            continue
        key = (str(row.get('session_id') or '').strip(), str(row.get('title') or '').strip())
        if not key[0] and not key[1]:
            continue
        if key not in folded:
            order.append(key)
            folded[key] = {
                'session_id': key[0],
                'title': key[1],
                'phase': str(row.get('phase') or ''),
                'when': str(row.get('dispatched_at') or ''),
                'turns': None,
                'rounds': None,
                'tool_calls': None,
                'budget_calls': None,
                'duration_min': None,
                'commit': '-',
            }
        task = folded[key]
        if row.get('phase'):
            task['phase'] = str(row['phase'])
        if row.get('dispatched_at'):
            task['when'] = str(row['dispatched_at'])
        for field in ('turns', 'rounds', 'tool_calls', 'budget_calls', 'duration_min'):
            value = _num(row.get(field))
            if value is not None:
                task[field] = value
        match = re.search(r'commit\s+([0-9a-fA-F]{7,40})', str(row.get('note') or ''))
        if match:
            task['commit'] = match.group(1)[:7]
    return [folded[key] for key in order]


def scan_dir(path: Path) -> dict[str, int]:
    """统计一个 work 目录：产物文件数（排除缓存）、报告个数与字节数。"""
    files = 0
    report_files = 0
    report_bytes = 0
    for item in path.rglob('*'):
        try:
            if not item.is_file():
                continue
        except OSError:
            continue
        if '__pycache__' in item.parts or item.suffix.lower() == '.pyc':
            continue
        files += 1
        if item.suffix.lower() == '.md':
            report_files += 1
            try:
                report_bytes += item.stat().st_size
            except OSError:
                pass
    return {'files': files, 'report_files': report_files, 'report_bytes': report_bytes}


def scan_work(work_root: Path) -> dict[str, dict[str, Any]]:
    """扫描 work 一级子目录，按归一化目录名建索引。"""
    index: dict[str, dict[str, Any]] = {}
    if not work_root.is_dir():
        return index
    for child in sorted(work_root.iterdir()):
        try:
            if not child.is_dir():
                continue
        except OSError:
            continue
        if child.name.startswith('_') or child.name.startswith('.'):
            continue
        key = norm_name(child.name)
        if not key:
            continue
        stats = scan_dir(child)
        stats['name'] = child.name
        stats['path'] = str(child)
        # 同归一化名冲突时保留文件数更多的那个（更可能是主产物目录）。
        if key not in index or stats['files'] > index[key]['files']:
            index[key] = stats
    return index


def match_work(title: str, index: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    """任务名与 work 目录名：先精确，再取最长前缀/被前缀关系。"""
    key = norm_name(title)
    if not key:
        return None
    if key in index:
        return index[key]
    candidates = [k for k in index if k and (k.startswith(key) or key.startswith(k))]
    if not candidates:
        return None
    return index[max(candidates, key=len)]


def build_report(
    tasks: list[dict[str, Any]],
    index: dict[str, dict[str, Any]],
    args: argparse.Namespace,
) -> dict[str, Any]:
    """计算每任务指标与命中原因，返回结构化结果。"""
    entries: list[dict[str, Any]] = []
    for task in tasks:
        work = match_work(task['title'], index)
        calls = task['tool_calls']
        budget = task['budget_calls']
        overrun = calls - budget if calls is not None and budget else None
        overrun_pct = round(overrun / budget * 100, 1) if overrun is not None else None
        artifacts = work['files'] if work else None
        report_bytes = work['report_bytes'] if work else None
        calls_per_art = round(calls / artifacts, 2) if calls and artifacts else None
        bytes_per_call = round(report_bytes / calls, 1) if report_bytes is not None and calls else None
        reasons: list[str] = []
        # 低于最低调用数的任务不算「高消耗」，只进台账对照表。
        if calls is not None and calls >= args.min_calls:
            if calls >= args.hard_stop:
                reasons.append(f'R5 调用≥{args.hard_stop}')
            if calls_per_art is not None and calls_per_art >= args.max_calls_per_artifact:
                reasons.append(f'R2/R4 调用/产物≥{args.max_calls_per_artifact}')
            if (bytes_per_call is not None
                    and artifacts
                    and bytes_per_call < args.min_report_bytes_per_call):
                reasons.append('R3 报告产出偏薄')
        if overrun_pct is not None and overrun_pct >= args.overrun_pct:
            reasons.append(f'R8 超预算≥{args.overrun_pct:g}%')
        entries.append({
            'title': task['title'] or '-',
            'session_id': task['session_id'] or '-',
            'phase': task['phase'] or '-',
            'when': task['when'][:19],
            'tool_calls': calls,
            'budget_calls': budget,
            'overrun': overrun,
            'overrun_pct': overrun_pct,
            'duration_min': task['duration_min'],
            'rounds': task['rounds'],
            'commit': task['commit'],
            'work_dir': work['name'] if work else None,
            'artifacts': artifacts,
            'report_files': work['report_files'] if work else None,
            'report_bytes': report_bytes,
            'calls_per_artifact': calls_per_art,
            'report_bytes_per_call': bytes_per_call,
            'reasons': reasons,
        })

    flagged = [e for e in entries if e['reasons']]
    flagged.sort(key=lambda e: (e['tool_calls'] or 0), reverse=True)

    listed = [e for e in entries
              if e['tool_calls'] is not None and e['tool_calls'] >= args.min_calls]
    listed.sort(key=lambda e: e['tool_calls'], reverse=True)

    known = [e for e in entries if e['tool_calls'] is not None]
    unknown = [e for e in entries if e['tool_calls'] is None]
    unmatched = [e for e in known if e['work_dir'] is None]
    no_duration = [e for e in entries if e['duration_min'] is None]

    total_calls = sum(e['tool_calls'] for e in known)
    total_artifacts = sum(e['artifacts'] or 0 for e in known)
    total_report_bytes = sum(e['report_bytes'] or 0 for e in known)

    # 预算维度：只统计「调用数与预算都有值」的任务，缺预算的任务单列，不按 0 算。
    with_budget = [e for e in known if e['budget_calls']]
    over = [e for e in with_budget if (e['overrun'] or 0) > 0]
    hit_rate = round((len(with_budget) - len(over)) / len(with_budget), 3) if with_budget else None
    max_overrun_pct = max((e['overrun_pct'] or 0.0 for e in with_budget), default=None)

    return {
        'window_days': args.days,
        'thresholds': {
            'min_calls': args.min_calls,
            'hard_stop': args.hard_stop,
            'max_calls_per_artifact': args.max_calls_per_artifact,
            'min_report_bytes_per_call': args.min_report_bytes_per_call,
            'overrun_pct': args.overrun_pct,
        },
        'tasks_total': len(entries),
        'tasks_with_calls': len(known),
        'tasks_unknown_calls': len(unknown),
        'unknown_ratio': round(len(unknown) / len(entries), 3) if entries else 0.0,
        'tasks_without_duration': len(no_duration),
        'tasks_without_work_dir': len(unmatched),
        'tasks_with_budget': len(with_budget),
        'tasks_over_budget': len(over),
        'budget_hit_rate': hit_rate,
        'max_overrun_pct': max_overrun_pct,
        'total_calls': int(total_calls),
        'total_artifacts': total_artifacts,
        'total_report_bytes': total_report_bytes,
        'flagged': flagged[:args.limit],
        'flagged_total': len(flagged),
        'ledger_rows': listed[:args.limit],
        'unavailable': [
            '真实 token 消耗：本地无逐会话 token 记录，只能以调用数近似',
            '会话标题：无 session_id -> 标题映射，仅用 work 目录名与台账 title 归一化匹配',
            f'调用数缺失的任务：{len(unknown)} 个（占比 {round(len(unknown) / len(entries), 3) if entries else 0.0}），不参与比值',
            '无 work 目录的任务：多为代码类改动，产物不在 work/，本表不判低产出',
            (f'预算对照：台账 budget_calls 有值的任务 {len(with_budget)} / {len(known)}；'
             '历史行未记预算，超支率只能覆盖已记预算的任务'
             if len(with_budget) < len(known) else '预算对照：全部有调用数的任务均已有 budget_calls'),
        ],
    }


def _fmt(value: Any, digits: int = 1) -> str:
    if value is None:
        return '?'
    if isinstance(value, float):
        return f'{value:.{digits}f}'
    return str(value)


def render_markdown(report: dict[str, Any]) -> str:
    """输出 Markdown：汇总 → 高消耗低产出 → 台账前 N → 不可得。"""
    lines = [
        f'# 派单成本 / 产出比（近 {report["window_days"]} 天）',
        '',
        f'- 任务数 {report["tasks_total"]}；有调用数 {report["tasks_with_calls"]}，'
        f'缺失 {report["tasks_unknown_calls"]}；合计调用 {report["total_calls"]} 次',
        f'- 产物文件 {report["total_artifacts"]} 个，报告 {report["total_report_bytes"]} 字节'
        f'（合计 {report["total_report_bytes"] / 1024:.1f} KB）',
        f'- 无 work 目录任务 {report["tasks_without_work_dir"]} 个；无耗时任务 '
        f'{report["tasks_without_duration"]} 个',
        f'- 阈值：调用 ≥{report["thresholds"]["hard_stop"]}、调用/产物 ≥'
        f'{report["thresholds"]["max_calls_per_artifact"]}、报告字节/调用 <'
        f'{report["thresholds"]["min_report_bytes_per_call"]}、超预算 >'
        f'{report["thresholds"]["overrun_pct"]:g}%',
        '',
        (f'- 预算：有预算的任务 {report["tasks_with_budget"]}，超支 {report["tasks_over_budget"]}，'
         f'命中率 {_fmt(report["budget_hit_rate"] * 100 if report["budget_hit_rate"] is not None else None)}%，'
         f'最大超支 {_fmt(report["max_overrun_pct"])}%'
         if report['tasks_with_budget'] else
         '- 预算：台账无 budget_calls（历史行未记预算），超支率不可得'),
        '',
        f'## 高消耗低产出（命中 {report["flagged_total"]} 个，列出前 {len(report["flagged"])}）',
        '',
        '| 任务 | 会话 | 调用 | 预算 | 超支% | 耗时min | 产物 | 报告KB | 调用/产物 | 报告B/调用 | 命中 |',
        '|---|---|---|---|---|---|---|---|---|---|---|',
    ]
    if not report['flagged']:
        lines.append('| 无 | - | - | - | - | - | - | - | - | - | 未命中阈值 |')
    for entry in report['flagged']:
        report_kb = entry['report_bytes'] / 1024 if entry['report_bytes'] is not None else None
        lines.append(
            '| {title} | {session} | {calls} | {budget} | {over} | {duration} | {artifacts} | {kb} | {cpa} | {bpc} | {why} |'.format(
                title=entry['title'][:28],
                session=entry['session_id'][:12],
                calls=_fmt(entry['tool_calls'], 0),
                budget=_fmt(entry['budget_calls'], 0),
                over=_fmt(entry['overrun_pct']),
                duration=_fmt(entry['duration_min']),
                artifacts=_fmt(entry['artifacts'], 0),
                kb=_fmt(report_kb),
                cpa=_fmt(entry['calls_per_artifact'], 2),
                bpc=_fmt(entry['report_bytes_per_call'], 0),
                why='；'.join(entry['reasons']),
            ))

    lines += [
        '',
        f'## 台账调用数前 {len(report["ledger_rows"])}（含未命中项，供对照）',
        '',
        '| 任务 | 会话 | 调用 | 预算 | 超支% | 耗时min | 产物 | 报告KB | commit |',
        '|---|---|---|---|---|---|---|---|---|',
    ]
    if not report['ledger_rows']:
        lines.append('| 无 | - | - | - | - | - | - | - | - |')
    for entry in report['ledger_rows']:
        report_kb = entry['report_bytes'] / 1024 if entry['report_bytes'] is not None else None
        lines.append(
            '| {title} | {session} | {calls} | {budget} | {over} | {duration} | {artifacts} | {kb} | {commit} |'.format(
                title=entry['title'][:28],
                session=entry['session_id'][:12],
                calls=_fmt(entry['tool_calls'], 0),
                budget=_fmt(entry['budget_calls'], 0),
                over=_fmt(entry['overrun_pct']),
                duration=_fmt(entry['duration_min']),
                artifacts=_fmt(entry['artifacts'], 0),
                kb=_fmt(report_kb),
                commit=entry['commit'],
            ))

    lines += ['', '## 不可得（不推测、不造数）', '']
    lines += [f'- {item}' for item in report['unavailable']]
    lines += ['', '复核口径与阈值处置见 `.trae/rules/token-efficiency.md` 第二节。']
    return '\n'.join(lines) + '\n'


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='派单成本 / 产出比周报（只读）')
    parser.add_argument('--ledger', default=str(DEFAULT_LEDGER))
    parser.add_argument('--work-root', default=str(DEFAULT_WORK))
    parser.add_argument('--days', type=int, default=7, help='按 dispatched_at 回看天数，缺省 7')
    parser.add_argument('--min-calls', type=int, default=30, help='进入台账对照表的最低调用数')
    parser.add_argument('--hard-stop', type=int, default=60, help='硬停线，命中即标 R5')
    parser.add_argument('--max-calls-per-artifact', type=float, default=20.0)
    parser.add_argument('--min-report-bytes-per-call', type=float, default=100.0)
    parser.add_argument('--overrun-pct', type=float, default=20.0,
                        help='超预算百分比硬停线（R8），缺省 20')
    parser.add_argument('--limit', type=int, default=15)
    parser.add_argument('--json', action='store_true', help='输出 JSON 而不是 Markdown')
    parser.add_argument('--out', default='', help='写文件路径；缺省只打印到 stdout')
    args = parser.parse_args(argv)

    ledger_path = Path(args.ledger)
    if not ledger_path.is_file():
        print(f'台账不存在：{ledger_path}', file=sys.stderr)
        return 2
    rows = read_ledger(ledger_path)
    since = date.today() - timedelta(days=args.days)
    tasks = fold_tasks(rows, since)
    index = scan_work(Path(args.work_root))
    report = build_report(tasks, index, args)

    if args.json:
        text = json.dumps(report, ensure_ascii=False, indent=2) + '\n'
    else:
        text = render_markdown(report)

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding='utf-8')
        print(f'已写入：{out_path}')
    else:
        print(text, end='')
    return 0


if __name__ == '__main__':
    sys.exit(main())
