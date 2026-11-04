#!/usr/bin/env python3

"""手工触发异常台账清单刷新（日常补数流程已自动调用）。

用法::

    python tools/refresh_anomaly_lists.py
    python tools/refresh_anomaly_lists.py --store-root data/local_store --lag-days 30
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from loguru import logger

from src.data.quality.anomaly_lists import refresh_anomaly_lists


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='刷新异常台账清单（幂等覆盖）')
    parser.add_argument('--store-root', default='',
                        help='本地列存根目录（默认 data/local_store）')
    parser.add_argument('--lag-days', type=int, default=30,
                        help='因子末端落后的近期窗口天数（默认 30）')
    parser.add_argument('--json', action='store_true', help='以 JSON 输出结果')
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    root = (Path(args.store_root) if args.store_root
            else Path(__file__).resolve().parents[1] / 'data' / 'local_store')
    result = refresh_anomaly_lists(root, lag_days=args.lag_days)
    if args.json:
        logger.info(json.dumps(result, ensure_ascii=False, indent=2))
    if result['status'] != 'ok':
        logger.error(f'清单刷新失败: {result["error"]}')
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
