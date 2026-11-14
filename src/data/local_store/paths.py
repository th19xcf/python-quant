#!/usr/bin/env python3

r"""本地数据路径的唯一定义处。

历史上 ``store.py`` / ``share_capital.py`` / ``turnover.py`` /
``finance_service.py`` 各自用 ``parents[3] / 'data' / 'local_store'`` 或裸
字符串 ``Path('data/local_store')``，导致两个问题：

- 相对路径依赖进程 cwd，从其他目录启动 CLI 时会读错目录；
- 自定义 ``DATA__LOCAL_STORE_PATH`` 时，部分模块仍写回默认目录。

本模块收口三件事：项目根、默认/配置解析后的列存根、以及巡检报告等
artifacts 目录。**不导入** ``store.py`` 或任何业务模块，避免循环依赖，
因此可被任意层（含 ``src.data.local_store`` 包初始化阶段）安全引用。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

#: 配置里的默认列存路径（相对项目根）。
DEFAULT_STORE_PATH = 'data/local_store'
#: 新鲜度巡检报告输出目录（相对项目根，已被 .gitignore 排除）。
DEFAULT_FRESHNESS_ARTIFACTS = 'artifacts/data_freshness'
#: 不可重建资产备份目录（相对项目根，已被 .gitignore 排除）。
DEFAULT_BACKUP_DIR = 'backups/critical_assets'
#: 回测运行记录根（结果资产，随 ``data/local_store`` 一并镜像备份且被 .gitignore 覆盖）。
#: 写侧 runner、读侧工作台与清单页都从这里取，不再各写一份字符串。
DEFAULT_BACKTEST_RUNS = 'data/local_store/backtest_runs'


def project_root() -> Path:
    """返回项目根目录（本文件位于 ``src/data/local_store/`` 下三层）。"""
    return Path(__file__).resolve().parents[3]


def default_store_root() -> Path:
    """默认列存根目录（项目根/data/local_store），与 cwd 无关。"""
    return project_root() / DEFAULT_STORE_PATH


def backtest_runs_dir() -> Path:
    """回测运行记录根目录（项目根/data/local_store/backtest_runs）。

    该目录放 run 归档资产（spec 快照、metrics、净值、日志、索引），不是数据表，
    不参与日更与 manifest 盘点；读写两侧唯一来源见本函数。
    """
    return project_root() / DEFAULT_BACKTEST_RUNS


def artifacts_dir(*parts: str) -> Path:
    """项目内 artifacts 下子目录（绝对路径）。"""
    return project_root() / 'artifacts' / Path(*parts) if parts else project_root() / 'artifacts'


def freshness_artifacts_dir() -> Path:
    """每日补数收尾 freshness 报告目录。"""
    return artifacts_dir('data_freshness')


def backup_dir() -> Path:
    """不可重建资产备份目录。"""
    return project_root() / DEFAULT_BACKUP_DIR


def resolve_store_root(config: Any) -> Path:
    """按配置解析列存根目录；相对路径一律以项目根为基准。

    ``config`` 可以是 ``Config``、``DataSettings`` 或任意 duck-typed 对象；
    读取不到 ``data.local_store_path`` 时退回 :func:`default_store_root`。
    """
    settings = getattr(config, 'data', config)
    raw = getattr(settings, 'local_store_path', '') or DEFAULT_STORE_PATH
    path = Path(str(raw))
    if not path.is_absolute():
        path = project_root() / path
    return path
