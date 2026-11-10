#!/usr/bin/env python3

"""名称类事实三方对账巡检（只读，不写任何数据文件）。

三张表分别对账：当前简称（``names.json`` vs ``stock_basic``）、历史简称能否
当当前名（``name_history`` 当前段 vs ``stock_basic``）、上市日能否由历史表
反推（``min(start_date)`` vs ``list_date``）。每行都带 ``verdict``，直接看出
是哪一方陈旧。

产物::

    reports/name_consistency_<YYYYMMDD>/names_json_vs_stock_basic.csv
    reports/name_consistency_<YYYYMMDD>/name_history_vs_stock_basic.csv
    reports/name_consistency_<YYYYMMDD>/list_date_vs_history_min_start.csv
    reports/name_consistency_<YYYYMMDD>/summary.json

用法::

    .venv\\Scripts\\python.exe tools\\check_name_consistency.py
    .venv\\Scripts\\python.exe tools\\check_name_consistency.py --out reports/xxx
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.name_consistency import write_reports  # noqa: E402

STORE = ROOT / 'data' / 'local_store'
REPORT_ROOT = ROOT / 'reports'


def _parse_args(argv: Iterable[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='名称类事实三方对账巡检（只读）')
    parser.add_argument('--store', default=str(STORE))
    parser.add_argument('--out', default=None,
                        help='输出目录，默认 reports/name_consistency_<YYYYMMDD>')
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    out_dir = Path(args.out) if args.out else (
        REPORT_ROOT / f'name_consistency_{date.today():%Y%m%d}')
    summary = write_reports(Path(args.store), out_dir)
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == '__main__':
    sys.exit(main())
