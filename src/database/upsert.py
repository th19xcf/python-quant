#!/usr/bin/env python3

"""
ORM 批量 upsert 工具（收口点）

原子写入实现（替代原「批量 SELECT -> 逐行 setattr / 批量 INSERT」）：

- MySQL：``INSERT ... ON DUPLICATE KEY UPDATE``，冲突时更新 value_fields；
- SQLite：``INSERT ... ON CONFLICT (key_fields) DO UPDATE``（测试环境使用）；
- 其他方言：退化为普通 ``INSERT``（由调用方保证无冲突）。

语义：
- 冲突时只更新 value_fields；``create_defaults`` 仅在新插入时生效；
- 新增行写入 ``created_at`` / ``updated_at`` 默认值；
- 冲突更新保留原 ``created_at``，并把 ``updated_at`` 刷新为数据库当前时间；
- 调用方签名与返回值 ``(inserted, updated)`` 保持不变：返回值由写入前的
  主键预加载统计（仅用于日志，不改变写入的原子性）。

分批：写入按 ``_WRITE_CHUNK`` 分批 executemany，避免单条超长语句/长事务；
同一批内重复键由 ON DUPLICATE / ON CONFLICT 处理，跨批重复键在事务内同样
命中唯一索引，不会抛 1062。

单位契约：本函数原样写库、不做任何换算。K 线字段必须由数据源归一化层
先统一为「vol=手（1 手=100 股）、amount=元」，再传入这里。
"""

from collections.abc import Iterable, Sequence
from typing import Dict, Tuple

from sqlalchemy import and_, func, insert, or_
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

# 单条 SQL 里 or_ 条件的分块大小，避免语句过长
_PRELOAD_CHUNK = 400
# 单次 executemany 的行数上限，避免单条语句过长
_WRITE_CHUNK = 400


def _existing_keys(session, model, rows, key_fields,
                   preload_chunk: int | None = None) -> set[tuple]:
    """一次性预加载所有主键已存在的键集合（仅用于统计 inserted/updated）。"""
    keys: set[tuple] = set()
    conditions = []
    seen = set()
    chunk_size = preload_chunk or _PRELOAD_CHUNK

    for row in rows:
        key = tuple(row[field] for field in key_fields)
        if key in seen:
            continue
        seen.add(key)
        conditions.append(and_(*[getattr(model, field) == row[field]
                                 for field in key_fields]))

    for start in range(0, len(conditions), chunk_size):
        chunk = conditions[start:start + chunk_size]
        for obj in session.query(model).filter(or_(*chunk)).all():
            keys.add(tuple(getattr(obj, field) for field in key_fields))
    return keys


def _build_upsert_statement(model, key_fields, value_fields, dialect_name: str):
    """按方言构造原子 upsert 语句（供测试直接校验生成的 SQL）。"""
    if dialect_name == 'mysql':
        mysql_stmt = mysql_insert(model)
        updates: Dict[str, object] = {
            field: mysql_stmt.inserted[field] for field in value_fields
        }
        # 冲突更新保留 created_at，刷新 updated_at（数据库时间）
        if 'updated_at' in model.__table__.c:
            updates['updated_at'] = func.now()
        return mysql_stmt.on_duplicate_key_update(**updates)

    if dialect_name == 'sqlite':
        sqlite_stmt = sqlite_insert(model)
        sqlite_updates = {
            field: sqlite_stmt.excluded[field] for field in value_fields
        }
        if 'updated_at' in model.__table__.c:
            sqlite_updates['updated_at'] = func.now()
        return sqlite_stmt.on_conflict_do_update(
            index_elements=list(key_fields), set_=sqlite_updates)

    return insert(model)


def _apply_column_defaults(model, payload: Dict) -> None:
    """补齐模型上的 Python 侧默认值（如 created_at / updated_at）。

    MySQL 的 executemany 不会为缺失列求值 Python 默认值（实测 created_at
    落库为 NULL），因此插入前显式补齐；conflict 分支只更新 value_fields
    与 updated_at，created_at 仍保留原值。
    """
    for column in model.__table__.columns:
        name = column.key
        if name in payload or column.default is None:
            continue
        default = column.default
        if default.is_scalar:
            payload[name] = default.arg
        elif default.is_callable:
            payload[name] = default.arg({})


def upsert_rows(
    session,
    model,
    rows: Iterable[Dict],
    key_fields: Sequence[str],
    value_fields: Sequence[str] = (),
    create_defaults: Dict | None = None,
    write_chunk: int | None = None,
    preload_chunk: int | None = None,
) -> Tuple[int, int]:
    """
    按主键原子写入（存在则更新 value_fields，不存在则插入）。

    Args:
        session: SQLAlchemy 会话
        model: ORM 模型类
        rows: 待写入行字典列表，每个字典含 key_fields 与 value_fields
        key_fields: 唯一键字段名，如 ('ts_code', 'trade_date')
        value_fields: 冲突时需要更新的字段名
        create_defaults: 仅在插入时赋值的字段（如 frequency），默认不赋值
        write_chunk: 单次 executemany 的行数上限；None 用模块默认 400
        preload_chunk: 主键预加载单条 SQL 的条件数；None 用模块默认 400。
            仅影响 inserted/updated 统计查询，不影响写入原子性；大批量
            全量回补可调大以减少往返次数。

    Returns:
        Tuple[int, int]: (新增条数, 更新条数)
    """
    rows = list(rows)
    if not rows:
        return 0, 0

    existing = _existing_keys(session, model, rows, key_fields, preload_chunk)
    defaults = dict(create_defaults or {})
    payloads: Dict[tuple, Dict] = {}
    seen: set[tuple] = set()
    inserted = 0
    updated = 0

    for row in rows:
        key = tuple(row[field] for field in key_fields)
        if key in existing or key in seen:
            updated += 1
        else:
            inserted += 1
        seen.add(key)

        payload = {field: row[field] for field in key_fields}
        payload.update({field: row[field] for field in value_fields})
        payload.update(defaults)
        _apply_column_defaults(model, payload)
        # 同批重复键：后者覆盖前者（与原 ORM 路径的 setattr 语义一致）
        payloads[key] = payload

    if not payloads:
        return inserted, updated

    dialect_name = session.get_bind().dialect.name
    values = list(payloads.values())
    chunk_size = write_chunk or _WRITE_CHUNK
    for start in range(0, len(values), chunk_size):
        chunk = values[start:start + chunk_size]
        # 显式 values（而非 ``execute(stmt, chunk)`` 的 executemany）：
        # MySQL 8 的 insertmanyvalues 在参数含 NULL 时会把对应列从 INSERT
        # 列表中剔除，导致 ON DUPLICATE KEY UPDATE 里 ``new.<col>`` 引用
        # 不存在的列（1054 Unknown column）。显式 values 保留 NULL 列，
        # 且同样支持 ON DUPLICATE / ON CONFLICT。
        stmt = _build_upsert_statement(
            model, key_fields, value_fields, dialect_name).values(chunk)
        session.execute(stmt)

    return inserted, updated
