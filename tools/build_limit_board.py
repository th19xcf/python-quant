#!/usr/bin/env python3

"""重建逐股逐日涨跌停标记（调用 src.data.limit_board）。

规则唯一真源为 ``src.core.market_rules.ExecutionRules``，本脚本只做环境
装配与参数透传。落盘 ``meta/limit/daily/limit_daily_YYYYMM.parquet`` 及
manifest / freshness / comparison，详见模块 docstring。

用法::

    python tools/build_limit_board.py               # 全量（2005 起）
    python tools/build_limit_board.py --since 2015-01-01
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.limit_board import main  # noqa: E402

if __name__ == '__main__':
    raise SystemExit(main())
