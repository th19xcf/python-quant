#!/usr/bin/env python3
"""日粒度台账快照一致性校验（只读）。

比对三处口径，按天输出缺口：

1. 当日 git commit 数：``git log --since="<日期> 00:00" --until="<日期> 23:59:59" --oneline``；
2. 当日台账 done 行数：``logs/task_ledger.csv`` 里 ``phase=done`` 的行（按 ``dispatched_at`` 取日期）；
3. 当日快照 done 行数：``docs/token/ledger_daily.csv`` 的 ``done_rows``。

用法::

    python tools/ledger_daily_check.py            # 近 7 天
    python tools/ledger_daily_check.py --days 30
    python tools/ledger_daily_check.py --date 2026-11-23
    python tools/ledger_daily_check.py --json

退出码：0 = 全部对齐；1 = 存在缺口或快照缺行。可直接挂进体检。
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEDGER = ROOT / 'logs' / 'task_ledger.csv'
DEFAULT_DAILY = ROOT / 'docs' / 'token' / 'ledger_daily.csv'


def _day_range(args: argparse.Namespace) -> list[date]:
    if args.date:
        return [date.fromisoformat(args.date)]
    today = date.today()
    return [today - timedelta(days=i) for i in range(args.days - 1, -1, -1)]


def git_commits(day: date, root: Path) -> int | None:
    """当日 commit 数；git 不可用时返回 None（不当作 0）。"""
    cmd = ['git', '-C', str(root), 'log',
           '--since', f'{day.isoformat()} 00:00',
           '--until', f'{day.isoformat()} 23:59:59',
           '--oneline']
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if out.returncode != 0:
        return None
    return len([line for line in out.stdout.splitlines() if line.strip()])


def ledger_done(day: date, path: Path) -> int:
    if not path.is_file():
        return 0
    with path.open(encoding='utf-8-sig', newline='') as handle:
        return sum(1 for row in csv.DictReader(handle)
                   if str(row.get('dispatched_at') or '').startswith(day.isoformat())
                   and str(row.get('phase') or '').strip() == 'done')


def snapshots(path: Path) -> dict[str, int | None]:
    result: dict[str, int | None] = {}
    if not path.is_file():
        return result
    with path.open(encoding='utf-8-sig', newline='') as handle:
        for row in csv.DictReader(handle):
            key = str(row.get('date') or '').strip()
            if not key:
                continue
            raw = str(row.get('done_rows') or '').strip().lower()
            try:
                result[key] = int(raw)
            except ValueError:
                result[key] = None
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='日粒度台账快照一致性校验（只读）')
    parser.add_argument('--days', type=int, default=7)
    parser.add_argument('--date', default='', help='只校验指定日期 YYYY-MM-DD')
    parser.add_argument('--root', default=str(ROOT), help='仓库根目录（供 git log 使用）')
    parser.add_argument('--ledger', default=str(DEFAULT_LEDGER))
    parser.add_argument('--daily', default=str(DEFAULT_DAILY))
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    ledger_path, daily_path = Path(args.ledger), Path(args.daily)
    snap = snapshots(daily_path)
    rows = []
    for day in _day_range(args):
        key = day.isoformat()
        commits = git_commits(day, root)
        done = ledger_done(day, ledger_path)
        snap_done = snap.get(key)
        gap = None if commits is None else commits - done
        snap_gap = None if snap_done is None else done - snap_done
        rows.append({
            'date': key,
            'git_commits': commits,
            'ledger_done': done,
            'commit_gap': gap,
            'snapshot_done': snap_done,
            'snapshot_gap': snap_gap,
        })

    bad = [r for r in rows
           if r['commit_gap'] not in (None, 0)
           or r['snapshot_gap'] not in (None, 0)]

    if args.json:
        print(json.dumps({'rows': rows, 'mismatched_days': len(bad)}, ensure_ascii=False, indent=2))
    else:
        print('日期        git_commits  ledger_done  commit缺口  snapshot_done  snapshot缺口')
        for r in rows:
            def fmt(value: object) -> str:
                return '-' if value is None else str(value)
            print(f"{r['date']}  {fmt(r['git_commits']):>11}  {r['ledger_done']:>11}  "
                  f"{fmt(r['commit_gap']):>10}  {fmt(r['snapshot_done']):>13}  "
                  f"{fmt(r['snapshot_gap']):>12}")
        if bad:
            print(f'\n不一致 {len(bad)} 天：' + '、'.join(r['date'] for r in bad) +
                  '（commit缺口 ≠ 0 或快照与台账不等）')
        else:
            print('\n全部对齐。')

    return 1 if bad else 0


if __name__ == '__main__':
    sys.exit(main())
