#!/usr/bin/env python3
"""补全 A 股 ST 历史覆盖的长期入口。

用法（在项目根目录）::

    .venv\\Scripts\\python.exe tools\\sync_st_name_history.py sync-em
    .venv\\Scripts\\python.exe tools\\sync_st_name_history.py sync-candidates
    .venv\\Scripts\\python.exe tools\\sync_st_name_history.py apply
    .venv\\Scripts\\python.exe tools\\sync_st_name_history.py verify

等价于 ``python -m src.data.st_name_history <子命令>``，所有幂等 / 断点续跑逻辑
都在 :mod:`src.data.st_name_history` 内。
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.data.st_name_history import main  # noqa: E402

if __name__ == '__main__':
    raise SystemExit(main())
