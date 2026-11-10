#!/usr/bin/env python3

"""staging（候选数据工作区）根目录的唯一权威定义。

候选数据必须写在 ``<仓库根>/_staging/<任务>_<日期>/``，**不在** ``data/`` 之下：

- ``data/`` 一律不得删除（红线），把临时工作区放进去就得为它单开删除例外；
  挪到仓库根后 ``data/`` 回到真正的一律不删；
- ``build/`` 是打包产物目录（``.gitignore`` 归在 Distribution / packaging，
  ``pyproject.toml`` 用 poetry 打包），候选数据放进去有被打包流程清理的风险，
  故不选 ``build/_staging``；
- 本目录已在 ``.gitignore`` 中，且不进入任何取数/回测/展示链路。

验收通过后按 DG-8 删除本次任务的子目录；``_staging`` 根本身保留。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

#: 仓库根目录（``src/data/staging.py`` 上溯三级）
PROJECT_ROOT = Path(__file__).resolve().parents[2]

#: 候选数据工作区根目录：``<仓库根>/_staging``
STAGING_ROOT = PROJECT_ROOT / '_staging'


def staging_dir(task: str, *, day: date | None = None, with_date: bool = False) -> Path:
    """返回 ``_staging/<task>``（或 ``_staging/<task>_<YYYYMMDD>``）。

    只拼路径，不创建目录；是否已存在由调用方用 ``exist_ok=False`` 自己把关。
    """
    name = f'{task}_{(day or date.today()):%Y%m%d}' if with_date else task
    return STAGING_ROOT / name
