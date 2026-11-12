#!/usr/bin/env python3

"""行情 schema 唯一性巡检：quotes/{stock,fund,index,futures} 各目录 schema 唯一。

只读：不写任何文件、不改数据。供收口前手动跑，退出码 1 表示存在漂移。

用法::

    .venv\\Scripts\\python.exe tools\\check_quote_schema.py
    .venv\\Scripts\\python.exe tools\\check_quote_schema.py --root data\\local_store
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

import polars as pl

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.data.quote_schema import QUOTE_COLUMNS  # noqa: E402

SKIP_DIRS = {'__pycache__'}


def _schema_signature(path: Path) -> tuple[tuple[str, str], ...]:
    return tuple((name, str(dtype)) for name, dtype in pl.read_parquet_schema(path).items())


def check(root: Path) -> int:
    quotes_root = root / 'quotes'
    problems = 0
    for data_type, expected in QUOTE_COLUMNS.items():
        directory = quotes_root / data_type
        if not directory.is_dir():
            print(f'[SKIP] {data_type}: 目录不存在 {directory}')
            continue
        files = sorted(
            p for p in directory.rglob('*.parquet')
            if not (set(p.parts) & SKIP_DIRS))
        if not files:
            print(f'[SKIP] {data_type}: 目录为空 {directory}')
            continue
        groups: dict[tuple[tuple[str, str], ...], list[Path]] = defaultdict(list)
        for path in files:
            groups[_schema_signature(path)].append(path)
        ok = len(groups) == 1
        names = tuple(name for name, _ in next(iter(groups)))
        if ok and names != expected:
            ok = False
        status = 'OK  ' if ok else 'FAIL'
        print(f'[{status}] {data_type}: {len(files)} 个分片 / {len(groups)} 种 schema'
              f' / 契约 {len(expected)} 列')
        if not ok:
            problems += 1
            for signature, members in sorted(groups.items(), key=lambda kv: -len(kv[1])):
                extra = [name for name, _ in signature if name not in expected]
                missing = [name for name in expected if name not in dict(signature)]
                print(f'        {len(members)} 个文件 schema 与契约不符'
                      f'：多列 {extra} / 缺列 {missing}')
                print(f'        样例：{[str(p.relative_to(root)) for p in members[:3]]}')
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description='quotes 各目录 schema 唯一性巡检（只读）')
    parser.add_argument(
        '--root', default=str(_REPO_ROOT / 'data' / 'local_store'),
        help='本地列存根目录，默认 data/local_store')
    args = parser.parse_args()
    problems = check(Path(args.root))
    if problems:
        print(f'\n发现 {problems} 类行情目录 schema 漂移')
        return 1
    print('\n全部行情目录 schema 唯一且与契约一致')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
