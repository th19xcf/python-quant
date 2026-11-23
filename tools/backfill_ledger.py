#!/usr/bin/env python3

r"""按磁盘实测补登 ledger：把已有数据集登记进 ``meta/registry/auto_register.json``。

背景：契约与 ``write_dataset`` 受控写入入口早已就位，但历史数据在接入该入口
之前就已落盘，ledger 因此对所有数据集都报「无写入登记」。本工具不重写任何
parquet、不改 manifest，只按契约扫磁盘实测 ``rows / min_date / max_date``
（单文件表另记 ``sha256 / bytes``）补一条 ``write_mode=backfill`` 的登记，
把「历史未登记」标注为「自补登日期的实测状态」。

默认只补没有登记的数据集；``--force`` 才覆盖已有登记。

用法::

    .venv\\Scripts\\python.exe tools/backfill_ledger.py --dry-run
    .venv\\Scripts\\python.exe tools/backfill_ledger.py
    .venv\\Scripts\\python.exe tools/backfill_ledger.py --only macro_market_margin
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.dataset_contracts import (  # noqa: E402
    ContractViolation,
    contract_keys,
    iter_contracts,
    register_dataset,
    registration_summary,
)

DEFAULT_ROOT = ROOT / 'data' / 'local_store'


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog='python tools/backfill_ledger.py',
        description='按磁盘实测补登数据集 ledger（不写 data/**）')
    parser.add_argument('--root', type=Path, default=DEFAULT_ROOT)
    parser.add_argument('--only', default=None, help='只补指定数据集，逗号分隔')
    parser.add_argument('--dry-run', action='store_true', help='只列计划，不写 ledger')
    parser.add_argument('--force', action='store_true', help='覆盖已有登记')
    args = parser.parse_args(argv)

    keys = [k.strip() for k in args.only.split(',') if k.strip()] if args.only else None
    if keys:
        unknown = [k for k in keys if k not in contract_keys()]
        if unknown:
            print(f'未注册的数据集：{unknown}', file=sys.stderr)
            return 2
    registered = (registration_summary(args.root).get('datasets') or {})

    done: list[str] = []
    skipped: list[str] = []
    missing: list[str] = []
    for contract in iter_contracts(keys):
        entry = registered.get(contract.key) or {}
        if entry.get('last_write') and not args.force:
            skipped.append(contract.key)
            continue
        if args.dry_run:
            print(f'[计划] {contract.key} <- {contract.pattern}')
            continue
        try:
            registration = register_dataset(
                contract.key, root=args.root,
                writer='tools/backfill_ledger.py',
                write_mode='backfill', evidence='disk_scan')
        # 兜底理由：缺文件的数据集要单独点名，不能让整轮回填中断
        except ContractViolation as exc:
            missing.append(f'{contract.key}: {exc}')
            continue
        done.append(f'{contract.key} rows={registration["rows"]:,} '
                    f'max_date={registration["max_date"]}')
        print(f'[补登] {done[-1]}')

    print(f'[ledger] 补登 {len(done)} / 已有登记跳过 {len(skipped)} / 缺文件 {len(missing)}')
    for item in missing:
        print(f'  [缺] {item}')
    return 1 if missing else 0


if __name__ == '__main__':
    raise SystemExit(main())
