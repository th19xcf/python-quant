"""Alembic 运行环境：复用项目 Config 与 ORM Base.metadata。

数据库 URL 统一由 src/utils/config.py（读取 .env）构造，
不在 alembic.ini 中重复维护，避免两处配置漂移。
"""

import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import URL, create_engine

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.database.db_manager import Base  # noqa: E402
from src.database.models import fund, index, stock  # noqa: E402,F401  确保全部表注册到 metadata
from src.utils.config import get_config  # noqa: E402

config = context.config
target_metadata = Base.metadata

if config.config_file_name is not None:
    fileConfig(config.config_file_name)


def _database_url() -> URL:
    """按 .env 配置构造数据库 URL（密码等特殊字符由 URL.create 转义）。"""
    db = get_config().database
    return URL.create(
        drivername="mysql+pymysql",
        username=db.username,
        password=db.password,
        host=db.host,
        port=db.port,
        database=db.database,
        query={"charset": db.charset},
    )


def run_migrations_offline() -> None:
    """离线模式：只生成 SQL，不建立数据库连接。"""
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """在线模式：连接数据库并执行迁移。"""
    connectable = create_engine(_database_url())

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
