"""baseline：以当前 ORM 元数据建立初始 schema

Revision ID: 0001
Revises:
Create Date: 2026-11-01

说明：
- 该基线为“当前模型快照”，upgrade 依据 Base.metadata 建表；
  HEAD 落地后会写入 alembic_version 表，后续变更必须以
  `alembic revision --autogenerate -m "..."` 生成增量脚本，禁止再手写 DDL。
- downgrade 会删除全部由 ORM 管理的表，仅用于开发/测试库回滚。
"""

from alembic import op

from src.database.db_manager import Base
from src.database.models import fund, index, stock  # noqa: F401  确保全部表注册到 metadata

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    Base.metadata.create_all(op.get_bind())


def downgrade() -> None:
    Base.metadata.drop_all(op.get_bind())
