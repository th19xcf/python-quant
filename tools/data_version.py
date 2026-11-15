#!/usr/bin/env python3

"""数据版本台账命令行：``record`` / ``show`` / ``stamp`` / ``audit`` / ``bind-run``。

只读写标记文件，不改数据、不改引擎、不触发重建或重跑。用法见
``src/data/data_version.py`` 与 ``src/backtest/runner/records.py``。
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.backtest.runner.records import bind_run_data_basis
from src.data.data_version import (
    VERSION_FILE,
    audit_derived,
    format_audit_line,
    load_current,
    record_version,
    stamp_derived,
)
from src.data.local_store.paths import backtest_runs_dir, default_store_root


def _history(root: Path) -> list[dict]:
    path = root / VERSION_FILE
    if not path.is_file():
        return []
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return []
    return [item for item in payload.get('versions') or [] if isinstance(item, dict)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='数据版本台账（基础表 → 派生表）')
    parser.add_argument('--store-root', default='', help='缺省用 default_store_root()')
    sub = parser.add_subparsers(dest='cmd', required=True)
    record = sub.add_parser('record', help='记录当前数据版本')
    record.add_argument('--note', default='')
    sub.add_parser('show', help='当前版本与历史')
    stamp = sub.add_parser('stamp', help='给派生表打「基于哪版数据」标记')
    stamp.add_argument('--table', required=True, help='派生表相对路径')
    stamp.add_argument('--note', default='')
    sub.add_parser('audit', help='判派生表是否落后于基础表')
    bind = sub.add_parser('bind-run', help='凭证据给历史 run 绑定数据版本（禁止推断）')
    bind.add_argument('--run', required=True, help='run_id 或 run 目录路径')
    bind.add_argument('--runs-dir', default='', help='缺省用 backtest_runs_dir()')
    bind.add_argument('--version', required=True, help='被绑定的数据版本号')
    bind.add_argument('--evidence', required=True, help='证据：manifest 或输入集合的具体依据')
    bind.add_argument('--note', default='')
    args = parser.parse_args(argv)

    root = Path(args.store_root) if args.store_root else default_store_root()
    if args.cmd == 'record':
        record = record_version(root, note=args.note)
        state = '新版本' if record['changed'] else '同版本刷新'
        print(f"[版本] 已记录 {record['version']}（{state}，历史 {record['history']} 条）")
        return 0
    if args.cmd == 'show':
        current = load_current(root)
        if not current:
            print('[版本] 尚未记录（先跑 record）')
            return 1
        print(format_audit_line(audit_derived(root)))
        for item in _history(root)[:10]:
            print(f"  {item.get('version')}  as_of={item.get('as_of')}  "
                  f"first={item.get('first_seen')}  last={item.get('last_seen')}  "
                  f"seen={item.get('seen_count')}")
        return 0
    if args.cmd == 'bind-run':
        run_arg = args.run
        if Path(run_arg).is_absolute() or '/' in run_arg or '\\' in run_arg:
            run_dir = Path(run_arg)
        else:
            base = Path(args.runs_dir) if args.runs_dir else backtest_runs_dir()
            run_dir = base / run_arg
        if not run_dir.is_dir():
            print(f"[版本] run 目录不存在：{run_dir}")
            return 1
        try:
            entry = bind_run_data_basis(run_dir, version=args.version,
                                        evidence=args.evidence, note=args.note,
                                        store_root=root)
        except ValueError as exc:
            print(f"[版本] 拒绝绑定：{exc}")
            return 2
        digest = entry.get('digest') or '台账未找到对应 digest'
        print(f"[版本] 已绑定 {run_dir.name} ← {entry['version']}"
              f"（ledger={entry['ledger_check']}，{digest}）")
        return 0
    if args.cmd == 'stamp':
        entry = stamp_derived(root, args.table, note=args.note)
        print(f"[版本] 已标记 {args.table} ← {entry['based_on_version']}")
        return 0
    result = audit_derived(root)
    print(format_audit_line(result))
    for row in result['rows']:
        if row['status'] != 'ok':
            print(f"  {row['status']:<9} {row['table']}（{row['label']}，{row['producer']}）")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
