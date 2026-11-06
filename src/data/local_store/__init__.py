#!/usr/bin/env python3

"""本地 Parquet 列存。

行情（日线）、复权因子、分红以 Parquet 形式落在项目内 ``data/local_store/``：
- 画图/回测的主读取路径不再经过公网 MySQL；
- 通达信 ``.day`` 只用于首次构建与增量更新；
- 远程库退化为归档 / 跨机共享 / 首次构建时的数据来源。

对外入口：
- :class:`LocalQuoteStore` —— 日线行情读写与覆盖度元数据；
- :class:`LocalFactorStore` —— 复权因子与分红快照；
- :func:`get_local_quote_store` / :func:`get_local_factor_store` —— 进程内单例；
- ``python -m src.data.local_store build`` —— 构建/增量 CLI。
"""

from src.data.local_store.paths import (
    default_store_root,
    freshness_artifacts_dir,
    project_root,
)
from src.data.local_store.store import (
    LocalFactorStore,
    LocalQuoteStore,
    get_local_factor_store,
    get_local_quote_store,
    local_store_enabled,
    remote_database_enabled,
    reset_local_stores,
    resolve_store_root,
)

__all__ = [
    'LocalFactorStore',
    'LocalQuoteStore',
    'resolve_store_root',
    'default_store_root',
    'freshness_artifacts_dir',
    'project_root',
    'get_local_quote_store',
    'get_local_factor_store',
    'local_store_enabled',
    'remote_database_enabled',
    'reset_local_stores',
]
