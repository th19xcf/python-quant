#!/usr/bin/env python3
"""最小任务账本：给「长任务必审」留可核查的派单与收口记录。

账本落在 ``logs/task_ledger.csv``（``logs/*`` 已被 .gitignore 忽略，不入库），
append-only——只追加，不重写、不删除历史行。列固定为::

    dispatched_at, session_id, title, phase, turns, tool_calls, duration_min, note

``phase=dispatch`` 派单时记一行；``phase=done`` 收口时按 ``session_id + title``
追加完成行，同一任务在 :func:`cmd_top` 里折叠成一条：优先取最新的 done 行，
缺的数值回退到 dispatch 行。done 行未显式给 ``--dispatched-at`` 时沿用该任务
的派单时间，完成时刻可写进 ``--note``；已有 done 行时仍追加新行，历史不覆盖。

默认 dry-run：``add`` 只打印将要追加的 CSV 行，``--apply`` 才落盘；``top``
是只读排名，不需要 ``--apply``。用法::

    python tools/task_ledger.py add --session-id 6ae82 --title "数据版本标记" \\
        --phase dispatch
    python tools/task_ledger.py add --session-id 6ae82 --title "数据版本标记" \\
        --phase dispatch --apply
    python tools/task_ledger.py add --session-id 6ae82 --title "数据版本标记" \\
        --phase done --turns 18 --tool-calls 63 --duration-min 24.5 --apply
    python tools/task_ledger.py top --days 7 --by calls --limit 10
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEDGER = ROOT / 'logs' / 'task_ledger.csv'
FIELDS = [
    'dispatched_at', 'session_id', 'title', 'phase',
    'turns', 'tool_calls', 'duration_min', 'note',
]
PHASES = ('dispatch', 'done')
#: 阈值口径：单任务超过任一值即发起 token 审计（判定仍按 00-core.md「长任务必审」）。
AUDIT_TOOL_CALLS = 60
AUDIT_MINUTES = 20.0


def _now() -> str:
    return datetime.now().isoformat(timespec='seconds')


def _read_rows(path: Path) -> list[dict[str, Any]]:
    """读账本；文件不存在返回空表，表头与约定不一致时直接报错。"""
    if not path.is_file():
        return []
    with path.open('r', encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != FIELDS:
            raise SystemExit(
                f'账本表头不符：{path}\n'
                f'期望 {",".join(FIELDS)}\n实际 {",".join(reader.fieldnames or [])}')
        return [dict(row) for row in reader]


def _same_task(row: dict[str, Any], session_id: str, title: str) -> bool:
    return row.get('session_id') == session_id and row.get('title') == title


def _latest(rows: list[dict[str, Any]], phase: str) -> dict[str, Any] | None:
    """按文件顺序取最后一条指定 phase 的行（append-only，最后即最新）。"""
    for row in reversed(rows):
        if row.get('phase') == phase:
            return row
    return None


def _inherit_dispatch_time(rows: list[dict[str, Any]], session_id: str,
                           title: str) -> str | None:
    dispatch = _latest([r for r in rows if _same_task(r, session_id, title)], 'dispatch')
    return dispatch.get('dispatched_at') if dispatch else None


def _num(value: Any) -> float:
    try:
        return float(str(value).strip()) if str(value).strip() else 0.0
    except ValueError:
        return 0.0


def _parse_day(value: Any) -> date | None:
    text = str(value or '').strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def cmd_add(args: argparse.Namespace) -> int:
    """追加一行 dispatch / done；默认只打印，``--apply`` 才写。"""
    ledger = Path(args.ledger)
    rows = _read_rows(ledger)
    same = [r for r in rows if _same_task(r, args.session_id, args.title)]

    dispatched_at = args.dispatched_at
    if not dispatched_at:
        inherited = _inherit_dispatch_time(rows, args.session_id, args.title)
        dispatched_at = inherited or _now()

    note = args.note or ''
    if args.phase == 'done':
        if not same:
            print(f'提示：未找到 session_id={args.session_id} title={args.title} 的 dispatch 行，'
                  '仍按收口追加；建议先补派单行。', file=sys.stderr)
        elif _latest(same, 'done') is not None:
            print('提示：同一任务已有 done 行，本次追加新行，历史保留。', file=sys.stderr)
        if not args.note:
            note = f'完成于 {_now()}'

    row = {
        'dispatched_at': dispatched_at,
        'session_id': args.session_id,
        'title': args.title,
        'phase': args.phase,
        'turns': '' if args.turns is None else str(args.turns),
        'tool_calls': '' if args.tool_calls is None else str(args.tool_calls),
        'duration_min': '' if args.duration_min is None else str(args.duration_min),
        'note': note,
    }
    body = ','.join(row[field] for field in FIELDS)

    if not args.apply:
        new_file = not ledger.is_file()
        print(f'账本：{ledger}')
        if new_file:
            print(f'表头：{",".join(FIELDS)}')
            print(f'待写：{body}')
        else:
            print(f'追加：{body}')
        print('[dry-run] 未写入；加 --apply 落盘')
        return 0

    ledger.parent.mkdir(parents=True, exist_ok=True)
    new_file = not ledger.is_file()
    with ledger.open('a', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        if new_file:
            writer.writeheader()
        writer.writerow(row)
    print(f'已追加 {args.phase}：{args.title}（{args.session_id}）')
    return 0


def _fold(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """把同一 ``session_id + title`` 的 dispatch / done 行折叠成一条任务。

    done 行优先（取最近一条有数值的 done），dispatch 行只作缺省回退；
    append-only 保留全部历史，但排名视图里同一任务只出现一次。
    """
    dispatch: dict[tuple[str, str], list[dict[str, Any]]] = {}
    done: dict[tuple[str, str], list[dict[str, Any]]] = {}
    order: list[tuple[str, str]] = []
    for row in rows:
        key = (str(row.get('session_id') or ''), str(row.get('title') or ''))
        if key not in order:
            order.append(key)
        bucket = done if row.get('phase') == 'done' else dispatch
        bucket.setdefault(key, []).append(row)

    tasks: list[dict[str, Any]] = []
    for key in order:
        dispatches = dispatch.get(key, [])
        dones = done.get(key, [])
        task: dict[str, Any] = {
            'session_id': key[0],
            'title': key[1],
            'phase': 'done' if dones else 'dispatch',
            'dispatched_at': (dispatches[0] if dispatches else dones[-1]).get('dispatched_at'),
        }
        for field in ('turns', 'tool_calls', 'duration_min'):
            value = 0.0
            for row in reversed(dones):
                value = _num(row.get(field))
                if value:
                    break
            if not value:
                for row in reversed(dispatches):
                    value = _num(row.get(field))
                    if value:
                        break
            task[field] = value
        tasks.append(task)
    return tasks


def cmd_top(args: argparse.Namespace) -> int:
    """按调用数或耗时排近 N 天前几名；只读，输出 ≤limit 行。"""
    rows = _read_rows(Path(args.ledger))
    cutoff = date.today() - timedelta(days=args.days)
    recent = []
    for row in rows:
        day = _parse_day(row.get('dispatched_at'))
        if day is None or day >= cutoff:
            recent.append(row)
    tasks = _fold(recent)
    key = 'tool_calls' if args.by == 'calls' else 'duration_min'
    tasks.sort(key=lambda task: task[key], reverse=True)
    for rank, task in enumerate(tasks[:args.limit], start=1):
        print(f'{rank}. {task["title"]} | {task["session_id"]} | '
              f'phase={task["phase"]} calls={task["tool_calls"]:.0f} '
              f'turns={task["turns"]:.0f} min={task["duration_min"]:.1f}')
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='最小任务账本（默认 dry-run）')
    parser.add_argument('--ledger', default=str(DEFAULT_LEDGER),
                        help=f'账本路径，缺省 {DEFAULT_LEDGER}')
    sub = parser.add_subparsers(dest='cmd', required=True)

    add = sub.add_parser('add', help='追加 dispatch / done 一行')
    add.add_argument('--session-id', required=True)
    add.add_argument('--title', required=True)
    add.add_argument('--phase', choices=PHASES, default='dispatch')
    add.add_argument('--turns', type=int)
    add.add_argument('--tool-calls', type=int)
    add.add_argument('--duration-min', type=float)
    add.add_argument('--note', default='')
    add.add_argument('--dispatched-at', default='',
                     help='缺省：done 沿用该任务 dispatch 时间，无 dispatch 时取当前时间')
    add.add_argument('--apply', action='store_true', help='真正落盘；缺省只打印')

    top = sub.add_parser('top', help='按调用数/耗时排名（只读）')
    top.add_argument('--days', type=int, default=7)
    top.add_argument('--by', choices=('calls', 'duration'), default='calls')
    top.add_argument('--limit', type=int, default=10)

    args = parser.parse_args(argv)
    if args.cmd == 'add':
        return cmd_add(args)
    return cmd_top(args)


if __name__ == '__main__':
    sys.exit(main())
