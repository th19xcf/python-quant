#!/usr/bin/env python3

"""数据新鲜度只读检查 CLI。

读取 ``src/data/quality/freshness_config.json`` 中登记的数据集，输出每类数据的
最新日期、距今天数、超期倍数、级别与覆盖占比，并写 JSON/CSV 结果到
``artifacts/data_freshness/``。

只读：不写 ``data/**``、不触发采集/构建，只允许写 artifacts 下的结果文件。

用法::

    .venv\\Scripts\\python.exe tools/data_freshness_check.py
    .venv\\Scripts\\python.exe tools/data_freshness_check.py --today 2026-11-05
    .venv\\Scripts\\python.exe tools/data_freshness_check.py --fail-on critical
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.quality.freshness import (  # noqa: E402
    _SEVERITY_ORDER,
    check_all,
    format_table,
    load_config,
    write_outputs,
)

DEFAULT_STORE_ROOT = ROOT / 'data' / 'local_store'
DEFAULT_CONFIG = ROOT / 'src' / 'data' / 'quality' / 'freshness_config.json'
DEFAULT_OUT_DIR = ROOT / 'artifacts' / 'data_freshness'


def _parse_date(value: str) -> date:
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(value[:10] if '-' in value else value, fmt).date()
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f'无法解析日期: {value}')


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='python tools/data_freshness_check.py',
        description='数据新鲜度只读检查（不写 data/**，不触发采集）',
    )
    parser.add_argument('--store-root', type=Path, default=DEFAULT_STORE_ROOT,
                        help='本地列存根目录，默认 data/local_store')
    parser.add_argument('--config', type=Path, default=DEFAULT_CONFIG,
                        help='新鲜度与阈值配置 JSON')
    parser.add_argument('--out-dir', type=Path, default=DEFAULT_OUT_DIR,
                        help='结果输出目录，默认 artifacts/data_freshness')
    parser.add_argument('--today', type=_parse_date, default=None,
                        help='指定检查基准日（默认系统当天）')
    parser.add_argument('--fail-on', choices=['none', 'warn', 'critical'],
                        default='none', help='命中该级别及以上时以非 0 退出')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    today = args.today or date.today()
    config = load_config(args.config)
    results = check_all(args.store_root, config, today)

    print(format_table(results, today))
    alerts = [item for item in results if item.severity != 'ok']
    if alerts:
        print('\n告警明细:')
        for item in alerts:
            detail = '；'.join(item.reasons) if item.reasons else item.note
            print(f'- [{item.severity}] {item.label}: {detail}')

    json_path, csv_path = write_outputs(args.out_dir, results, today)
    print(f'\n结果已写出:\n  JSON: {json_path}\n  CSV : {csv_path}')

    if args.fail_on != 'none':
        threshold = _SEVERITY_ORDER[args.fail_on]
        if any(_SEVERITY_ORDER[item.severity] >= threshold for item in results):
            return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
