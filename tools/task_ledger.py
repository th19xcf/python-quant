#!/usr/bin/env python3
"""最小任务账本：给「长任务必审」留可核查的派单与收口记录。

账本落在 ``logs/task_ledger.csv``（``logs/*`` 已被 .gitignore 忽略，不入库），
append-only——只追加，不重写、不删除历史行。列固定为::

    dispatched_at, session_id, title, phase, turns, rounds, tool_calls, duration_min, note

``rounds`` 是同一任务（``session_id + title``）按会话累积的派发 / 回报轮数：
``add`` 显式给 ``--rounds`` 时照写，否则取该任务既有行数 + 1；收口自动记账
找不到同标识的派单行时写 ``unknown`` 并在 stderr 告警，不阻断收口。旧账本
（没有 ``rounds`` 列）可直接读取，首次追加时把表头就地升级为新 9 列、旧行
该列留空，历史行内容不改。

``phase=dispatch`` 派单时记一行；``phase=done`` 收口时按 ``session_id + title``
追加完成行，同一任务在 :func:`cmd_top` 里折叠成一条：优先取最新的 done 行，
缺的数值回退到 dispatch 行。done 行未显式给 ``--dispatched-at`` 时沿用该任务
的派单时间，完成时刻可写进 ``--note``；已有 done 行时仍追加新行，历史不覆盖。

默认 dry-run：``add`` 只打印将要追加的 CSV 行，``--apply`` 才落盘；``top``
与 ``status`` 是只读，不需要 ``--apply``。``status`` 输出每个任务最近一行的
任务标识 / 最近轮次 / 累计调用数 / 耗时 / 提交短哈希，按最近时间倒序，供派单前
判定是否换会话（轮次 ≥15 换会话，见 :data:`TURN_SWITCH_ROUNDS`）。用法::

    python tools/task_ledger.py add --session-id 6ae82 --title "数据版本标记" \\
        --phase dispatch
    python tools/task_ledger.py add --session-id 6ae82 --title "数据版本标记" \\
        --phase dispatch --rounds 3 --apply
    python tools/task_ledger.py add --session-id 6ae82 --title "数据版本标记" \\
        --phase done --turns 18 --rounds 18 --tool-calls 63 --duration-min 24.5 --apply
    python tools/task_ledger.py top --days 7 --by calls --limit 10
    python tools/task_ledger.py status
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEDGER = ROOT / 'logs' / 'task_ledger.csv'
FIELDS = [
    'dispatched_at', 'session_id', 'title', 'phase',
    'turns', 'rounds', 'tool_calls', 'duration_min', 'note', 'budget_calls',
]
#: 读取时仍接受的旧表头：9 列（无 budget_calls）与 8 列（再缺 rounds）。
LEGACY_FIELDS = [field for field in FIELDS if field != 'budget_calls']
HEADER_VARIANTS = (
    FIELDS,
    LEGACY_FIELDS,
    [field for field in LEGACY_FIELDS if field != 'rounds'],
)
PHASES = ('dispatch', 'done')
#: 轮次闸门：同一任务累计达到 15 轮即换会话（与 60 次硬停分工不同）。
TURN_SWITCH_ROUNDS = 15
#: 单会话闸门阈值：达到 60 次写交接停手，100 次为绝对上限（阈值改动须同轮改单测）。
HARD_STOP_TOOL_CALLS = 60
ABSOLUTE_MAX_TOOL_CALLS = 100
GATE_CONTINUE = 'continue'
GATE_HANDOFF = 'handoff'
GATE_STOP = 'stop'
UNKNOWN = 'unknown'
_COMMIT_RE = re.compile(r'commit\s+([0-9a-fA-F]{7,40})')


def session_gate(tool_calls: int) -> str:
    """单会话闸门：``<60`` 继续、``60~99`` 停手写交接、``>=100`` 绝对停止。"""
    if tool_calls >= ABSOLUTE_MAX_TOOL_CALLS:
        return GATE_STOP
    if tool_calls >= HARD_STOP_TOOL_CALLS:
        return GATE_HANDOFF
    return GATE_CONTINUE


def _now() -> str:
    return datetime.now().isoformat(timespec='seconds')


def _read_header(path: Path) -> list[str]:
    """只读表头；文件不存在或为空返回空表。"""
    if not path.is_file():
        return []
    with path.open('r', encoding='utf-8-sig', newline='') as handle:
        return next(csv.reader(handle), [])


def _read_rows(path: Path) -> list[dict[str, Any]]:
    """读账本；文件不存在返回空表，表头不认识时直接报错。

    新旧表头都接受：旧账本没有 ``rounds`` 列，读回时补空串，调用方无需分支。
    """
    if not path.is_file():
        return []
    with path.open('r', encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        if fieldnames not in HEADER_VARIANTS:
            raise SystemExit(
                f'账本表头不符：{path}\n'
                f'期望 {" 或 ".join(",".join(v) for v in HEADER_VARIANTS)}\n'
                f'实际 {",".join(fieldnames)}')
        rows: list[dict[str, Any]] = []
        for raw in reader:
            raw.pop(None, None)
            rows.append({field: str(raw.get(field) or '') for field in FIELDS})
        return rows


def _write_all(path: Path, rows: list[dict[str, Any]]) -> None:
    """整表写回（仅用于新文件与旧表头升级；行内容原样保留）。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, '') for field in FIELDS})


def _append_rows(path: Path, new_rows: list[dict[str, Any]]) -> None:
    """追加行；遇到旧表头先把表头升级为 9 列再整体写回，避免列错位。"""
    if not path.is_file():
        _write_all(path, new_rows)
        return
    if _read_header(path) == FIELDS:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('a', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDS)
            for row in new_rows:
                writer.writerow({field: row.get(field, '') for field in FIELDS})
        return
    _write_all(path, _read_rows(path) + new_rows)



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

    if args.rounds is None:
        rounds = str(len(same) + 1)
    else:
        rounds = str(args.rounds)

    row = {
        'dispatched_at': dispatched_at,
        'session_id': args.session_id,
        'title': args.title,
        'phase': args.phase,
        'turns': '' if args.turns is None else str(args.turns),
        'rounds': rounds,
        'tool_calls': '' if args.tool_calls is None else str(args.tool_calls),
        'duration_min': '' if args.duration_min is None else str(args.duration_min),
        'note': note,
        'budget_calls': '' if args.budget_calls is None else str(args.budget_calls),
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

    _append_rows(ledger, [row])
    print(f'已追加 {args.phase}：{args.title}（{args.session_id}）')
    return 0


def append_finalize_row(
    ledger: Path,
    *,
    commit: str,
    task_id: str | None = None,
    tool_calls: int | None = None,
    duration_min: float | None = None,
    rounds: int | None = None,
    when: str | None = None,
) -> str:
    """收口自动记账：追加一行 done；同一提交已记过则返回 ``duplicate`` 不重复写。

    映射到当前列：``dispatched_at`` 记时间、``session_id`` / ``title`` 记任务标识
    （缺省用提交短哈希）、``rounds`` 记轮次（显式入参优先，否则取同标识既有行数
    +1；找不到派单行写 ``unknown`` 并在 stderr 告警、不阻断）、``tool_calls``
    记调用数（未知写 ``unknown``）、``duration_min`` 记耗时（未知留空）、
    ``note`` 记 ``commit <短哈希>`` 供去重、``budget_calls`` 沿用同标识派单行的预算。
    """
    short = commit[:7]
    marker = f'commit {short}'
    try:
        rows = _read_rows(ledger)
    except SystemExit as exc:
        raise RuntimeError(str(exc)) from None
    if any(marker in str(row.get('note') or '') for row in rows):
        return 'duplicate'
    ident = (task_id or '').strip() or short
    if rounds is not None:
        rounds_text = str(rounds)
    else:
        matched = [row for row in rows if _same_task(row, ident, ident)]
        if matched:
            rounds_text = str(len(matched) + 1)
        else:
            rounds_text = UNKNOWN
            print(f'警告：账本里没有任务标识 {ident} 的派单行，轮次写 {UNKNOWN}', file=sys.stderr)
    budget_text = ''
    for prior in reversed([row for row in rows if _same_task(row, ident, ident)]):
        if str(prior.get('budget_calls') or '').strip():
            budget_text = str(prior['budget_calls'])
            break
    row = {
        'dispatched_at': when or _now(),
        'session_id': ident,
        'title': ident,
        'phase': 'done',
        'turns': '',
        'rounds': rounds_text,
        'tool_calls': UNKNOWN if tool_calls is None else str(tool_calls),
        'duration_min': '' if duration_min is None else str(duration_min),
        'note': marker,
        'budget_calls': budget_text,
    }
    _append_rows(ledger, [row])
    return 'written'


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


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or '').strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _latest_text(rows: list[dict[str, Any]], field: str) -> str:
    """取最近一条非空值（append-only，文件靠后即更新）。"""
    for row in reversed(rows):
        value = str(row.get(field) or '').strip()
        if value:
            return value
    return ''


def _last_commit(rows: list[dict[str, Any]]) -> str:
    for row in reversed(rows):
        match = _COMMIT_RE.search(str(row.get('note') or ''))
        if match:
            return match.group(1)[:7]
    return '-'


def cmd_status(args: argparse.Namespace) -> int:
    """每任务一行：任务标识 / 最近轮次 / 累计调用数 / 耗时 / 提交短哈希；只读。

    按最近时间倒序，供派单前判定；轮次为数值且 ≥ :data:`TURN_SWITCH_ROUNDS`
    时行尾标 ``switch=yes``。缺值分别显示 ``unknown`` / ``-``，不报错。
    """
    rows = _read_rows(Path(args.ledger))
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    order: list[tuple[str, str]] = []
    for row in rows:
        key = (str(row.get('session_id') or ''), str(row.get('title') or ''))
        if key not in grouped:
            grouped[key] = []
            order.append(key)
        grouped[key].append(row)

    tasks: list[dict[str, str]] = []
    for key in order:
        task_rows = grouped[key]
        rounds = _latest_text(task_rows, 'rounds') or UNKNOWN
        tasks.append({
            'session_id': key[0],
            'title': key[1],
            'when': _latest_text(task_rows, 'dispatched_at'),
            'rounds': rounds,
            'tool_calls': _latest_text(task_rows, 'tool_calls') or UNKNOWN,
            'duration_min': _latest_text(task_rows, 'duration_min') or '-',
            'commit': _last_commit(task_rows),
        })

    def sort_key(task: dict[str, str]) -> tuple[int, float]:
        stamp = _parse_dt(task['when'])
        return (0, -stamp.timestamp()) if stamp else (1, 0.0)

    tasks.sort(key=sort_key)
    for task in tasks:
        line = (f'{task["title"]} | {task["session_id"]} | '
                f'rounds={task["rounds"]} | calls={task["tool_calls"]} | '
                f'min={task["duration_min"]} | commit={task["commit"]}')
        if task['rounds'].isdigit() and int(task['rounds']) >= TURN_SWITCH_ROUNDS:
            line += ' | switch=yes'
        print(line)
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
    add.add_argument('--rounds', type=int,
                     help='同一任务累计轮次；缺省取该任务既有行数 +1')
    add.add_argument('--tool-calls', type=int)
    add.add_argument('--budget-calls', type=int, help='派单预算调用数，用于超支率统计')
    add.add_argument('--duration-min', type=float)
    add.add_argument('--note', default='')
    add.add_argument('--dispatched-at', default='',
                     help='缺省：done 沿用该任务 dispatch 时间，无 dispatch 时取当前时间')
    add.add_argument('--apply', action='store_true', help='真正落盘；缺省只打印')

    top = sub.add_parser('top', help='按调用数/耗时排名（只读）')
    top.add_argument('--days', type=int, default=7)
    top.add_argument('--by', choices=('calls', 'duration'), default='calls')
    top.add_argument('--limit', type=int, default=10)

    sub.add_parser('status', help='每任务一行：轮次/调用/耗时/短哈希（只读）')

    args = parser.parse_args(argv)
    if args.cmd == 'add':
        return cmd_add(args)
    if args.cmd == 'status':
        return cmd_status(args)
    return cmd_top(args)


if __name__ == '__main__':
    sys.exit(main())
