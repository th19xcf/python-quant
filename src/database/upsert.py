#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ORM 批量 upsert 工具（收口点）

各数据源 Handler 的日线/分钟线写库逻辑原本各自重复实现
「逐行 query 查重 -> 更新或插入」，共 6 处。此模块提供统一实现：

- 按主键批量预加载已存在的行（or_ 条件分块），逐行 SELECT 降为每 400 行一次
- 已存在：更新 value_fields；不存在：用 key_fields + value_fields + create_defaults 插入
- create_defaults 只在新插入时赋值，不会覆盖已有行的存量字段

调用方需保证每个 dict 同时包含 key_fields 与 value_fields 的键；
无法解析的行由调用方在构造 dict 前跳过，保持原有的容错语义。
"""

from typing import Dict, Iterable, Sequence, Tuple

from sqlalchemy import and_, or_

# 单条 SQL 里 or_ 条件的分块大小，避免语句过长
_PRELOAD_CHUNK = 400


def _preload_existing(session, model, rows, key_fields):
    """一次性预加载所有主键已存在的 ORM 行，返回 {key_tuple: obj}。"""
    existing = {}
    conditions = []
    seen = set()

    for row in rows:
        key = tuple(row[field] for field in key_fields)
        if key in seen:
            continue
        seen.add(key)
        conditions.append(and_(*[getattr(model, field) == row[field]
                                 for field in key_fields]))

    for start in range(0, len(conditions), _PRELOAD_CHUNK):
        chunk = conditions[start:start + _PRELOAD_CHUNK]
        for obj in session.query(model).filter(or_(*chunk)).all():
            key = tuple(getattr(obj, field) for field in key_fields)
            # 与 query(...).first() 语义一致：同键多行时保留先取到的一条
            existing.setdefault(key, obj)

    return existing


def upsert_rows(
    session,
    model,
    rows: Iterable[Dict],
    key_fields: Sequence[str],
    value_fields: Sequence[str] = (),
    create_defaults: Dict = None,
) -> Tuple[int, int]:
    """
    按主键批量写入（存在则更新 value_fields，不存在则插入）。

    Args:
        session: SQLAlchemy 会话
        model: ORM 模型类
        rows: 待写入行字典列表，每个字典含 key_fields 与 value_fields
        key_fields: 主键字段名，如 ('ts_code', 'trade_date')
        value_fields: 存在时需要更新的字段名
        create_defaults: 仅在插入时赋值的字段（如 frequency），默认不赋值

    Returns:
        Tuple[int, int]: (新增条数, 更新条数)
    """
    rows = list(rows)
    if not rows:
        return 0, 0

    existing = _preload_existing(session, model, rows, key_fields)
    defaults = dict(create_defaults or {})
    inserted = 0
    updated = 0

    for row in rows:
        key = tuple(row[field] for field in key_fields)
        obj = existing.get(key)

        if obj is None:
            payload = {field: row[field] for field in key_fields}
            payload.update({field: row[field] for field in value_fields})
            payload.update(defaults)
            obj = model(**payload)
            session.add(obj)
            existing[key] = obj
            inserted += 1
        else:
            for field in value_fields:
                setattr(obj, field, row[field])
            updated += 1

    return inserted, updated
