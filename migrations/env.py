"""Alembic 运行环境配置。

数据库 URL 从 src/utils/config.py（.env）运行时构建，避免凭据写入 alembic.ini。
迁移脚本统一使用 src.database.db_manager.Base 作为 target_metadata。
"""

from logging.config import fileConfig

from alembic import context
from sqlalchemy import URL, create_engine, pool

from src.database.db_manager import Base
from src.database.models import fund, index, stock  # noqa: F401  导入以注册全部表
from src.utils.config import get_config

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

# 远程库为共享库，存在大量与本项目无关的表；autogenerate 只应比较项目模型表，
# 否则会把其他业务的表误判为“待删除”并生成 drop_table。
_PROJECT_TABLES = frozenset(target_metadata.tables)


def _include_name(
    name: str | None,
    type_: str,
    parent_names: dict[str, str | None],
) -> bool:
    """限制 autogenerate 的反射范围，仅保留项目模型定义的表。"""
    if type_ == 'table':
        return name in _PROJECT_TABLES
    return True


def _database_url() -> URL:
    """按 DatabaseSettings 构造 SQLAlchemy URL，与运行时连接口径保持一致。"""
    db = get_config().database
    return URL.create(
        drivername='mysql+pymysql',
        username=db.username,
        password=db.password,
        host=db.host,
        port=db.port,
        database=db.database,
        query={'charset': db.charset},
    )


def run_migrations_offline() -> None:
    """离线模式：仅生成 SQL，不实际连接数据库。"""
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={'paramstyle': 'named'},
        compare_type=True,
        include_name=_include_name,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """在线模式：使用 NullPool 建立一次性连接执行迁移。"""
    connectable = create_engine(_database_url(), poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            include_name=_include_name,
        )
        with context.begin_transaction():
            context.run_migrations()
    connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
