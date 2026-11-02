"""分钟线/基金/指数表成交单位对齐，并修正 index_daily.vol

Revision ID: 0007_units_minute_index
Revises: 0006_daily_units
Create Date: 2026-11-02

取证（2026-11-02，实测）：
- stock_minute：0 行，无数据可取证，仅注释错误；
- fund_daily：0 行，无数据可取证，仅注释错误；
- closed_fund_daily：0 行，无数据可取证，仅注释错误；
- index_daily：2401 行（3 个指数，2020-01-02 ~ 2026-04-14）。与本地 TDX
  指数 .day 全量逐行比对：amount 与 TDX 元值比值恒为 1.00000（存量即「元」，
  注释误写「千元」）；vol 与 TDX「手」的比值恒为 100.0000（存量是「股」），
  2401/2401 行无一例外，无口径混用，可安全整除折手。

本迁移做两件事：
1. index_daily.vol 由「股」改为「手」（vol = vol / 100），amount 不动；
2. 四张表的 vol/amount 列注释对齐契约。

幂等性：用 index_daily.vol 列注释中的契约标记「1手=100股」判断是否已换算，
带标记则跳过数据除法；空表的注释变更同样先比较再执行。MySQL DDL 隐式提交，
无法与 DML 原子化，标记仅用于中断恢复；正常路径由 alembic_version 保证单次执行。
写库前已导出四表 CSV 备份并记录 SHA256。

并发：会话级 lock_wait_timeout / innodb_lock_wait_timeout = 60 秒，超时报错退出，
不 KILL 连接；遇 1205/1213 由调用方重试一次即停。
版本号 ≤32 字符（alembic_version.version_num 默认 VARCHAR(32)）。
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0007_units_minute_index'
down_revision: str | None = '0006_daily_units'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLES = ('stock_minute', 'fund_daily', 'closed_fund_daily', 'index_daily')
_VOL_COMMENT = '成交量(手，1手=100股)'
_AMOUNT_COMMENT = '成交额(元)'
# 契约标记：出现在 vol 列注释中即表示「股→手」已完成
_VOL_MARKER = '1手=100股'
_VOL_COMMENT_LEGACY = '成交量(手)'
_AMOUNT_COMMENT_LEGACY = '成交额(千元)'


def _column_comments(table: str) -> dict[str, str]:
    """读取指定表 vol/amount 列的实际注释（小写列名 -> 注释）。"""
    bind = op.get_bind()
    rows = bind.execute(sa.text(
        "SELECT COLUMN_NAME, COLUMN_COMMENT FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table "
        "AND COLUMN_NAME IN ('vol', 'amount')"
    ), {'table': table}).all()
    return {str(name).lower(): (comment or '') for name, comment in rows}


def _set_lock_timeouts() -> None:
    """限制元数据锁与行锁等待，避免在共享库上无限排队。"""
    op.execute('SET SESSION lock_wait_timeout = 60')
    op.execute('SET SESSION innodb_lock_wait_timeout = 60')


def _alter_comment(table: str, column: str, comment: str) -> None:
    """只改列注释，列类型/可空性/默认值保持不变（vol、amount 均为 FLOAT NULL）。"""
    op.execute(
        f"ALTER TABLE `{table}` MODIFY COLUMN `{column}` "
        f"FLOAT DEFAULT NULL COMMENT '{comment}'"  # noqa: S608
    )


def upgrade() -> None:
    _set_lock_timeouts()

    # index_daily 存量 vol 为「股」：仅在注释还没有契约标记时执行一次
    comments = _column_comments('index_daily')
    if _VOL_MARKER not in comments.get('vol', ''):
        op.execute(
            "UPDATE `index_daily` SET `vol` = `vol` / 100 WHERE `vol` IS NOT NULL"
        )

    for table in _TABLES:
        comments = _column_comments(table)
        if comments.get('vol') != _VOL_COMMENT:
            _alter_comment(table, 'vol', _VOL_COMMENT)
        if comments.get('amount') != _AMOUNT_COMMENT:
            _alter_comment(table, 'amount', _AMOUNT_COMMENT)


def downgrade() -> None:
    _set_lock_timeouts()

    # 回退数据：注释仍带契约标记说明数据是「手」，乘回 100 恢复旧口径
    comments = _column_comments('index_daily')
    if _VOL_MARKER in comments.get('vol', ''):
        op.execute(
            "UPDATE `index_daily` SET `vol` = `vol` * 100 WHERE `vol` IS NOT NULL"
        )

    for table in _TABLES:
        comments = _column_comments(table)
        if comments.get('vol') != _VOL_COMMENT_LEGACY:
            _alter_comment(table, 'vol', _VOL_COMMENT_LEGACY)
        if comments.get('amount') != _AMOUNT_COMMENT_LEGACY:
            _alter_comment(table, 'amount', _AMOUNT_COMMENT_LEGACY)
