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


def staging_dir_for_store(store_root: Path | str, task: str) -> Path:
    """按列存根目录推导 staging 落点：``<store_root>``（``<仓库>/data/local_store``）上溯两级。

    用途：生产者写出的「无运行期消费者的留档 / 缓存」不再落正式层（DG-32），
    统一改落仓库根 ``_staging/<task>``。按 ``store_root`` 推导而不是取模块级
    ``STAGING_ROOT``，测试用临时根时落点随临时根走，不污染仓库 ``_staging``。
    先例：``src/data/industry_pit.py::raw_dir``。

    **调用约定（本函数不校验，传错不报错）**：``store_root`` 必须是
    ``<仓库根>/data/local_store`` 这种「两级深」的层级——函数固定取
    ``parents[1]`` 当仓库根，只对这张形态成立。传浅了（例如直接把
    ``<临时目录>/某个测试目录`` 当 store）不会抛错，而是**静默把 staging 写到
    意料之外的目录**：``parents[1]`` 可能落在跨运行常驻的上级目录里，
    续跑记录（``progress.json`` / ``fetch_progress.json`` 之类）就会跨用例、
    跨运行互相污染；``test_extended_builders.py`` 的
    ``test_remote_fetch_break_records_skipped_and_stops`` 正是这样被残留的
    ``fetch_progress.json`` 让续跑跳过首只指数而失败。

    正确::

        repo / 'data' / 'local_store'
        # -> repo / '_staging' / '<task>'          （parents[1] 就是 repo）

    错误（少一层；落点跑到常驻的 base，而不是 run0 或 test_x0 之下）::

        base / 'run0' / 'test_x0'
        # -> base / '_staging' / '<task>'          （parents[1] 是 base）
    """
    return Path(store_root).resolve().parents[1] / '_staging' / task
