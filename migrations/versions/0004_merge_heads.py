"""合并迁移分叉：0001 与 0003_double_precision

Revision ID: 0004_merge_heads
Revises: 0001, 0003_double_precision
Create Date: 2026-11-02

背景：仓库存在两条独立根迁移（0001 与 0001_baseline），
0002/0003 挂在 0001_baseline 链上，导致 `alembic heads` 有两个 head，
`upgrade head` 因歧义无法执行。本迁移把所有 head 合并为一个，
不改变任何 schema；后续增量迁移统一从本 revision 派生。

注意：当前库 alembic_version 为 0003_double_precision 时，
升级到本 revision 会先补执行 0001（create_all(checkfirst=True)，幂等、不删表），
再落到合并点。downgrade 只退回到分叉前的两个 head，不做 schema 变更。
"""

from collections.abc import Sequence

from alembic import op

revision: str = '0004_merge_heads'
down_revision: tuple[str, str] | None = ('0001', '0003_double_precision')
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """合并点无 schema 变更。"""
    pass


def downgrade() -> None:
    """拆分回两个 head；schema 不变。"""
    pass
