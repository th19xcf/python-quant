"""基线：创建全部表（幂等）

Revision ID: 0001_baseline
Revises:
Create Date: 2026-11-02

使用 Base.metadata.create_all(checkfirst=True)，对旧版 create_all 已建表的
数据库可重复执行，不会重复建表或清空数据。
"""

from collections.abc import Sequence

from alembic import op

from src.database.db_manager import Base
from src.database.models import fund, index, stock  # noqa: F401  导入以注册全部表

revision: str = '0001_baseline'
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    Base.metadata.create_all(bind=op.get_bind(), checkfirst=True)


def downgrade() -> None:
    Base.metadata.drop_all(bind=op.get_bind())
