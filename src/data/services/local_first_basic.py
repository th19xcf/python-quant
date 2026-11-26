#!/usr/bin/env python3

"""基础表「本地优先」合并式导出（``fund_basic`` / ``index_basic``）。

背景：这类基础表在 MySQL 里也有一份，但本地 parquet 才是正式输入，且本地可能
带有逐只取证后的修正值。因此导出**不得整表覆盖**，只能以本地文件为基，按单元格
做两类写入：

1. 本地为空、库里有值 → 用库值补齐；
2. 两边都有值但不一致 → 只有给出判别依据才允许改写（当前实现一律保留本地，
   并把差异登记到 ``_staging/local_first_<日期>/``）。

未命中任何分支时写回原值；同时校验「改写前后各列空值数不增加」，避免
``coalesce`` 一类无兜底表达式把本地值写成 null（DG-31）。

每日检查、有变才落盘
====================
名录改为每日检查后，避免「远端没变也天天整表重写」：

1. 先取**轻量快照**（库侧行数 + 最大 ``updated_at``）与 manifest 上次记录比对，
   一致即视为无变更，不拉全量、不写正式表，只更新 ``last_checked_at``；
2. 快照不一致才做全量合并，并用**内容指纹**（按 ``ts_code`` 排序后逐行哈希）
   与本地比对——行数相同但成分变化同样能识别；
3. 无变更时不写 parquet（mtime 不动），检查日期单独记在 manifest 的
   ``last_checked_at``，保鲜体检按它判新鲜度。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

from src.data.atomic_write import write_parquet_atomic, write_text_atomic
from src.data.local_store.paths import resolve_store_root
from src.data.staging import PROJECT_ROOT, staging_dir

FUND_PATH = Path('meta') / 'fund_basic.parquet'
FUND_MANIFEST = Path('meta') / 'fund_basic.manifest.json'
INDEX_PATH = Path('meta') / 'index_basic.parquet'
INDEX_MANIFEST = Path('meta') / 'index_basic.manifest.json'

#: 需要逐单元格对拍的语义字段（id/created_at/updated_at 属库侧元数据，单独处理）
FUND_CORE_FIELDS: tuple[str, ...] = (
    'id', 'symbol', 'name', 'fund_type', 'market',
    'list_date', 'delist_date', 'status',
)
FUND_META_FIELDS: tuple[str, ...] = ('created_at', 'updated_at')

INDEX_CORE_FIELDS: tuple[str, ...] = (
    'id', 'ts_code', 'name', 'market', 'publisher', 'index_type', 'category',
    'base_date', 'base_point', 'list_date', 'weight_rule', 'desc',
)
INDEX_META_FIELDS: tuple[str, ...] = ('created_at', 'updated_at')

#: 冲突时保留本地值的判别依据（按字段给出，便于差异清单直接引用）
CONFLICT_REASONS: dict[str, str] = {
    'status': '本地为 L/D/P 状态口径，MySQL 为 1/0 上架码，口径不同；保留本地',
    'fund_type': '本地 fund_type 按 TDX 名称/代码推断并人工核对（ccd96ea），库值未取证',
    'list_date': '本地 list_date 已逐只取证（8f5f1a5），库值未取证',
    'delist_date': '本地 delist_date 来自逐只退市取证，库中为空',
}
DEFAULT_CONFLICT_REASON = '无第三源可判正误，保留本地既有值（DG-18）'

INDEX_CONFLICT_REASONS: dict[str, str] = {
    'name': '本地名称已按东财现货表与 baostock query_stock_basic 逐只取证（ef14481），库值未取证',
    'publisher': '本地 publisher 由代码后缀推断（SH→上交所 / SZ→深交所 / BJ→北交所），口径固定',
    'index_type': '本地 index_type 由名称关键词推断并人工核对，库值同源但可能被后续覆盖',
    'category': '本地 category 由名称关键词推断并人工核对，库值同源但可能被后续覆盖',
    'weight_rule': '本地 weight_rule 为「市值加权」统一口径，库值未取证',
    'list_date': '本地 list_date 已按外部源逐只回填，库值未取证',
    'base_date': '本地 base_date 已按外部源逐只回填，库值未取证',
}


@dataclass(frozen=True)
class BasicSpec:
    """一张基础表的导出参数。"""

    kind: str
    relative_path: Path
    manifest_path: Path
    core_fields: tuple[str, ...]
    meta_fields: tuple[str, ...]
    conflict_reasons: dict[str, str]


FUND_SPEC = BasicSpec(
    kind='fund_basic', relative_path=FUND_PATH, manifest_path=FUND_MANIFEST,
    core_fields=FUND_CORE_FIELDS, meta_fields=FUND_META_FIELDS,
    conflict_reasons=CONFLICT_REASONS,
)
INDEX_SPEC = BasicSpec(
    kind='index_basic', relative_path=INDEX_PATH, manifest_path=INDEX_MANIFEST,
    core_fields=INDEX_CORE_FIELDS, meta_fields=INDEX_META_FIELDS,
    conflict_reasons=INDEX_CONFLICT_REASONS,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _read_manifest(path: Path) -> dict[str, Any]:
    """读现有 manifest；缺失或不可解析时返回空字典（后续按新建处理）。"""
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        logger.warning(f'现有 manifest 无法解析，将被覆盖: {path}')
        return {}


def _remote_snapshot(db_manager: Any, model: Any) -> dict[str, Any]:
    """轻量快照：库侧行数与最大 ``updated_at``，用于判断是否需要全量拉取。"""
    from sqlalchemy import func  # noqa: PLC0415

    session = db_manager.get_session()
    try:
        rows, max_updated = session.query(
            func.count(), func.max(model.updated_at)).one()
    finally:
        session.close()
    return {
        'rows': int(rows or 0),
        'max_updated_at': None if max_updated is None else str(max_updated),
    }


def content_digest(frame: pl.DataFrame) -> str:
    """整表内容指纹：按 ``ts_code`` 排序后逐行哈希（行数相同、成分变化也能识别）。"""
    digest = hashlib.sha256()
    if frame.is_empty():
        return digest.hexdigest()
    for row in frame.sort('ts_code').iter_rows(named=False):
        digest.update(repr(row).encode('utf-8'))
        digest.update(b'\n')
    return digest.hexdigest()


def null_counts(frame: pl.DataFrame) -> dict[str, int]:
    return {name: int(frame[name].null_count()) for name in frame.columns}


def assert_no_null_increase(
    before: dict[str, int], after: dict[str, int], *,
    fields: tuple[str, ...] | None = None,
) -> None:
    """断言所有受检字段的空值数不增加（改前 -> 改后）。"""
    for name in (fields or tuple(before)):
        old = before.get(name, 0)
        new = after.get(name, 0)
        if new > old:
            raise AssertionError(
                f'字段 {name} 空值增加: {old} -> {new}，'
                '合并式导出禁止把本地已有非空值写成 null')


def _load_from_model(db_manager: Any, model: Any) -> pl.DataFrame:
    session = db_manager.get_session()
    try:
        rows = [
            {column.name: getattr(row, column.name) for column in model.__table__.columns}
            for row in session.query(model).all()
        ]
    finally:
        session.close()
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def _merge_row_cells(
    row: dict[str, Any], db_row: dict[str, Any], *, spec: BasicSpec,
    null_typed: set[str], diffs: list[dict[str, str]], counters: dict[str, int],
) -> dict[str, Any]:
    """按单元格合并一行：只补本地空值，冲突保留本地并登记依据。"""
    code = str(row['ts_code'])
    merged = dict(row)

    for field in spec.core_fields:
        local_value = row.get(field)
        db_value = db_row.get(field)
        if local_value is None and db_value is not None:
            if field in null_typed:
                raise ValueError(
                    f'{spec.kind}.{field} 在本地为全空列（dtype=Null），'
                    f'库里存在值 {db_value!r}；是否补该列需显式决策并说明列类型，'
                    '合并式导出不自动改写列类型')
            merged[field] = db_value
            counters['fill_from_db'] += 1
            diffs.append({
                'ts_code': code, 'field': field, 'local_value': '',
                'db_value': str(db_value), 'action': 'fill_from_db',
                'reason': '本地为空、库里有值',
            })
        elif local_value is not None and db_value is not None and local_value != db_value:
            counters['conflict_keep_local'] += 1
            diffs.append({
                'ts_code': code, 'field': field,
                'local_value': str(local_value), 'db_value': str(db_value),
                'action': 'conflict_keep_local',
                'reason': spec.conflict_reasons.get(field, DEFAULT_CONFLICT_REASON),
            })
        elif local_value is not None and db_value is None:
            counters['local_only_keep'] += 1
            diffs.append({
                'ts_code': code, 'field': field, 'local_value': str(local_value),
                'db_value': '', 'action': 'local_only_keep',
                'reason': '库中为空、本地有值，保留本地',
            })

    for field in spec.meta_fields:
        if row.get(field) is None and db_row.get(field) is not None:
            merged[field] = db_row[field]
            counters['fill_from_db'] += 1
            diffs.append({
                'ts_code': code, 'field': field, 'local_value': '',
                'db_value': str(db_row[field]), 'action': 'fill_from_db',
                'reason': '库侧元数据为空值补齐',
            })
    return merged


def merge_basic(
    local: pl.DataFrame, db: pl.DataFrame, *, spec: BasicSpec,
) -> tuple[pl.DataFrame, list[dict[str, str]], dict[str, int]]:
    """以本地为基做单元格级合并，返回 (合并结果, 差异清单, 计数)。

    写入规则只有两类：``fill_from_db``（本地空、库有值）与
    ``conflict_keep_local``（两边都有值但冲突，保留本地并登记依据）。
    库里没有的代码整行保留本地；库里多出的代码整行追加（本地为空）。
    """
    schema = local.schema
    db_map: dict[str, dict[str, Any]] = {}
    if not db.is_empty():
        if 'ts_code' not in db.columns:
            raise ValueError(f'MySQL {spec.kind} 结果缺少 ts_code 列')
        db_map = {str(row['ts_code']): row for row in db.to_dicts()}

    # 本地某列是空类型（如 index_basic.base_point 全空）时，不允许从库里补数：
    # 那会改掉列类型并把「全空是已知事实」变成「部分有值」，需要显式决策而非自动填。
    null_typed = {
        name for name in schema if schema[name] == pl.Null and name in spec.core_fields
    }

    diffs: list[dict[str, str]] = []
    counters = {
        'fill_from_db': 0, 'conflict_keep_local': 0,
        'local_only_keep': 0, 'append_row': 0, 'missing_in_db': 0,
    }
    merged_rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    for row in local.to_dicts():
        code = str(row['ts_code'])
        seen.add(code)
        db_row = db_map.get(code)
        if db_row is None:
            counters['missing_in_db'] += 1
            diffs.append({
                'ts_code': code, 'field': '*', 'local_value': code, 'db_value': '',
                'action': 'missing_in_db', 'reason': '库中无该代码，整行保留本地',
            })
            merged_rows.append(dict(row))
            continue
        merged_rows.append(_merge_row_cells(
            row, db_row, spec=spec, null_typed=null_typed,
            diffs=diffs, counters=counters))

    for code, db_row in db_map.items():
        if code in seen:
            continue
        row = dict.fromkeys(schema)
        for field, value in db_row.items():
            if field in row:
                row[field] = value
        merged_rows.append(row)
        counters['append_row'] += 1
        diffs.append({
            'ts_code': code, 'field': '*', 'local_value': '', 'db_value': code,
            'action': 'append_row', 'reason': '本地无该代码，整行追加',
        })

    merged = pl.DataFrame(merged_rows, schema=schema)
    return merged, diffs, counters


def merge_fund_basic(
    local: pl.DataFrame, db: pl.DataFrame,
) -> tuple[pl.DataFrame, list[dict[str, str]], dict[str, int]]:
    """``fund_basic`` 的合并式导出规则（见 :func:`merge_basic`）。"""
    return merge_basic(local, db, spec=FUND_SPEC)


def merge_index_basic(
    local: pl.DataFrame, db: pl.DataFrame,
) -> tuple[pl.DataFrame, list[dict[str, str]], dict[str, int]]:
    """``index_basic`` 的合并式导出规则（见 :func:`merge_basic`）。"""
    return merge_basic(local, db, spec=INDEX_SPEC)


def _write_diff(
    diffs: list[dict[str, str]], stage: Path, payload: dict[str, Any], *, kind: str,
) -> Path:
    schema = {
        'ts_code': pl.String, 'field': pl.String, 'local_value': pl.String,
        'db_value': pl.String, 'action': pl.String, 'reason': pl.String,
    }
    frame = pl.DataFrame(diffs, schema=schema) if diffs else pl.DataFrame(schema=schema)
    csv_path = stage / f'{kind}_diff.csv'
    write_text_atomic(frame.write_csv(), csv_path)
    write_text_atomic(
        json.dumps(payload, ensure_ascii=False, indent=2) + '\n',
        stage / f'{kind}_summary.json',
    )
    return csv_path


def _export_basic(
    spec: BasicSpec, db_model: Any, config: Any, db_manager: Any, *,
    store_root: Path | str | None = None, write: bool = True,
) -> dict[str, Any]:
    root = Path(store_root) if store_root else resolve_store_root(config)
    local_path = root / spec.relative_path
    if not local_path.is_file():
        raise FileNotFoundError(f'缺少本地正式文件，拒绝无基导出: {local_path}')

    manifest_path = root / spec.manifest_path
    existing = _read_manifest(manifest_path)
    now = datetime.now().isoformat(timespec='seconds')
    local = pl.read_parquet(local_path)
    snapshot = _remote_snapshot(db_manager, db_model)

    # 轻量门：库侧行数与最大 updated_at 都没变时，不拉全量、不写正式表，
    # 只把「最后检查日期」推进到本次（mtime 保持不动）。
    if write and existing.get('remote_snapshot') == snapshot:
        manifest = {
            **existing,
            'last_checked_at': now,
            'last_check': {
                'checked_at': now, 'changed': False,
                'mode': 'remote_snapshot_unchanged', **snapshot,
            },
        }
        write_text_atomic(
            json.dumps(manifest, ensure_ascii=False, indent=2) + '\n', manifest_path)
        return {
            'kind': spec.kind, 'changed': False, 'skipped_write': True,
            'mode': 'remote_snapshot_unchanged',
            'local_rows': local.height, 'merged_rows': local.height,
            'mysql_rows': snapshot['rows'], 'diff_rows': 0,
        }

    db = _load_from_model(db_manager, db_model)
    merged, diffs, counters = merge_basic(local, db, spec=spec)

    before = null_counts(local)
    # 空值断言只覆盖本地既有 ts_code：追加行天然带空字段（在市基金的 delist_date、
    # 名称无本地来源的 name 等），不参与“本地非空值不得被写成 null”的判定。
    after = null_counts(merged.filter(pl.col('ts_code').is_in(local['ts_code'].to_list())))
    assert_no_null_increase(before, after)
    if merged.height != local.height + counters['append_row']:
        raise AssertionError(
            f"行数不守恒: 本地 {local.height} + 追加 {counters['append_row']} "
            f"!= 合并后 {merged.height}")
    if not set(local['ts_code'].to_list()) <= set(merged['ts_code'].to_list()):
        raise AssertionError('合并后丢失了本地既有的 ts_code')

    # 内容指纹比对：不只看行数，行数相同但成分变化同样判为变更。
    changed = content_digest(merged) != content_digest(local)

    result: dict[str, Any] = {
        'kind': spec.kind, 'changed': changed, 'skipped_write': not changed,
        'mode': 'full_merge',
        'local_rows': local.height, 'mysql_rows': db.height,
        'merged_rows': merged.height, 'counters': counters,
        'diff_rows': len(diffs),
        'null_counts_before': before, 'null_counts_after': after,
    }
    if not write:
        return result

    if not changed:
        # 远端快照变了但合并结果与本地一致：不写正式表，只记检查日期与快照。
        manifest = {
            **existing,
            'path': f'data/local_store/{spec.relative_path.as_posix()}',
            'last_checked_at': now,
            'remote_snapshot': snapshot,
            'last_check': {
                'checked_at': now, 'changed': False,
                'mode': 'full_merge_no_diff', 'mysql_rows': db.height,
                'diff_rows': len(diffs), **counters,
            },
        }
        write_text_atomic(
            json.dumps(manifest, ensure_ascii=False, indent=2) + '\n', manifest_path)
        result['sha256'] = existing.get('sha256') or _sha256(local_path)
        logger.info(f'{spec.kind} 检查完成：无变更，未落盘（检查日期 {now}）')
        return result

    stage = staging_dir('local_first', with_date=True)
    csv_path = _write_diff(
        diffs, stage,
        {
            'generated_at': now,
            'kind': spec.kind,
            'local_path': str(local_path),
            'local_rows': local.height, 'mysql_rows': db.height,
            'merged_rows': merged.height, 'counters': counters,
        },
        kind=spec.kind,
    )
    result['diff_csv'] = str(csv_path)

    write_parquet_atomic(merged, local_path)
    sha = _sha256(local_path)

    try:
        staging_rel = str(stage.relative_to(PROJECT_ROOT))
    except ValueError:
        staging_rel = str(stage)
    manifest = {
        **existing,
        'path': f'data/local_store/{spec.relative_path.as_posix()}',
        'rows': merged.height,
        'codes': int(merged['ts_code'].n_unique()),
        'sha256': sha,
        'built_at': now,
        'produced_by': f'src.data.services.local_first_basic.export_{spec.kind}',
        'merge_policy': (
            '以本地 parquet 为基：仅当本地为空时用库值补齐；两边冲突保留本地并登记差异；'
            '禁止整表覆盖与无兜底 coalesce'),
        'last_checked_at': now,
        'remote_snapshot': snapshot,
        'last_check': {
            'checked_at': now, 'changed': True, 'mode': 'full_merge_written',
            'mysql_rows': db.height, 'diff_rows': len(diffs), **counters,
        },
        'last_export': {
            'mysql_rows': db.height,
            **counters,
            'staging': staging_rel,
        },
    }
    from src.data.manifest_semantics import apply_semantics  # noqa: PLC0415
    apply_semantics(manifest, spec.manifest_path.as_posix())
    write_text_atomic(
        json.dumps(manifest, ensure_ascii=False, indent=2) + '\n', manifest_path)
    result['sha256'] = sha
    logger.info(
        f'{spec.kind} 合并导出完成: {counters}，差异 {len(diffs)} 条 -> {stage}')
    return result


def export_fund_basic(
    config: Any, db_manager: Any, *,
    store_root: Path | str | None = None, write: bool = True,
) -> dict[str, Any]:
    """把 MySQL ``fund_basic`` 合并式导出到本地正式 parquet。

    ``write=False`` 时只算差异与断言，不落盘（供测试/演练使用）。
    """
    from src.database.models.fund import FundBasic

    return _export_basic(
        FUND_SPEC, FundBasic, config, db_manager,
        store_root=store_root, write=write)


def export_index_basic(
    config: Any, db_manager: Any, *,
    store_root: Path | str | None = None, write: bool = True,
) -> dict[str, Any]:
    """把 MySQL ``index_basic`` 合并式导出到本地正式 parquet。"""
    from src.database.models.index import IndexBasic

    return _export_basic(
        INDEX_SPEC, IndexBasic, config, db_manager,
        store_root=store_root, write=write)


def check_fund_basic_local(
    config: Any = None, *,
    store_root: Path | str | None = None, write: bool = True,
) -> dict[str, Any]:
    """日常基金名录检查：只读本地 stock_basic，不回写 MySQL。

    本地 ``stock_basic`` 若混入基金代码（正常为 0 行），按 ``_is_fund_ts_code``
    与 ``_classify_local_fund_symbol`` 判定后整行并入本地 ``fund_basic``；
    无新增时只推进 manifest 的 ``last_checked_at``，不写正式表。
    回灌 MySQL 属一次性独立任务，不在此处发生。
    """
    import shutil

    from src.data.services.sync_common import _is_fund_ts_code
    from src.data.services.sync_fund import _classify_local_fund_symbol

    root = Path(store_root) if store_root else resolve_store_root(config)
    local_path = root / FUND_PATH
    stock_path = root / 'meta' / 'stock_basic.parquet'
    if not local_path.is_file():
        raise SystemExit(f'本地基金名录缺失，拒绝检查：{local_path}')
    if not stock_path.is_file():
        raise SystemExit(f'本地股票名录缺失，拒绝检查：{stock_path}')

    local = pl.read_parquet(local_path)
    stock = pl.read_parquet(
        stock_path,
        columns=['ts_code', 'name', 'market', 'list_date', 'delist_date', 'status'])
    existing = set(local['ts_code'].to_list())
    seen = set(existing)
    now = datetime.now().isoformat(timespec='seconds')
    stamped = datetime.now().strftime('%Y%m%d_%H%M%S')

    appended: list[dict[str, Any]] = []
    max_id = local['id'].max() if local.height else 0
    next_id = int(max_id or 0) + 1  # type: ignore[arg-type]  # polars max 返回宽联合类型
    for ts_code, name, market, list_date, delist_date, status in stock.iter_rows():
        if not ts_code or ts_code in seen:
            continue
        if not _is_fund_ts_code(ts_code, name, market):
            continue
        classified = _classify_local_fund_symbol(str(ts_code)[:6], name)
        if classified is None:
            continue
        fund_type, _target = classified
        appended.append({
            'id': next_id,
            'ts_code': ts_code,
            'symbol': str(ts_code)[:6],
            'name': name,
            'fund_type': fund_type,
            'market': market,
            'list_date': list_date,
            'delist_date': delist_date,
            'status': status,
            'created_at': datetime.now(),
            'updated_at': datetime.now(),
            'is_tradable': status == 'L',
            'tradable_reason': 'migrated_from_local_stock_basic',
            'source': f'migrated_from_stock_basic@{stamped}',
        })
        seen.add(ts_code)
        next_id += 1

    # 行情先行、源端名录尚未收录的新基金（2026-11-24 实测 20 只新上市主动 ETF，
    # 如 158900.SZ）：按 meta/daily_index 的基金覆盖 + 本地名称快照补进本地名录。
    # 不补的话保鲜体检会把「行情有、名录无」判成新增未登记，直接把 basic 步打成失败。
    index_path = root / 'meta' / 'daily_index.parquet'
    names_path = root / 'meta' / 'names.json'
    if index_path.is_file() and names_path.is_file():
        covered = pl.read_parquet(index_path, columns=['ts_code', 'data_type', 'start', 'end'])
        names = json.loads(names_path.read_text(encoding='utf-8'))
        for ts_code, _data_type, start, end in covered.filter(
                pl.col('data_type') == 'fund').iter_rows():
            if not ts_code or ts_code in seen:
                continue
            name = names.get(ts_code)
            classified = _classify_local_fund_symbol(str(ts_code)[:6], name)
            if classified is None:
                continue
            fund_type, _target = classified
            appended.append({
                'id': next_id,
                'ts_code': ts_code,
                'symbol': str(ts_code)[:6],
                'name': name,
                'fund_type': fund_type,
                'market': str(ts_code)[-2:].upper(),
                'list_date': start,
                'delist_date': None,
                'status': 'L',
                'created_at': datetime.now(),
                'updated_at': datetime.now(),
                'is_tradable': True,
                'tradable_reason': None,
                'source': f'status=local_tdx_quote|last_bar={end}',
            })
            seen.add(ts_code)
            next_id += 1

    manifest_path = root / FUND_MANIFEST
    manifest = _read_manifest(manifest_path)
    result: dict[str, Any] = {
        'kind': 'fund_basic', 'changed': bool(appended),
        'appended': len(appended), 'local_rows': local.height,
        'checked_at': now,
    }
    if not appended:
        if write:
            write_text_atomic(
                json.dumps({
                    **manifest,
                    'last_checked_at': now,
                    'last_check': {
                        'checked_at': now, 'changed': False,
                        'mode': 'local_only_no_diff', 'rows': local.height,
                    },
                }, ensure_ascii=False, indent=2) + '\n',
                manifest_path)
        logger.info(f'fund_basic 本地检查：无新增基金行（{local.height} 行）')
        return result

    backup = root / '_backup' / f'fund_basic_local_migrate_{stamped}'
    backup.mkdir(parents=True, exist_ok=True)
    shutil.copy2(local_path, backup / local_path.name)
    merged = pl.concat([local, pl.DataFrame(appended, schema=local.schema)])
    if write:
        write_parquet_atomic(merged, local_path)
        manifest = {
            **manifest,
            'path': f'data/local_store/{FUND_PATH.as_posix()}',
            'rows': merged.height,
            'codes': int(merged['ts_code'].n_unique()),
            'sha256': _sha256(local_path),
            'built_at': now,
            'produced_by': 'src.data.services.local_first_basic.check_fund_basic_local',
            'last_checked_at': now,
            'last_check': {
                'checked_at': now, 'changed': True,
                'mode': 'local_only_appended', 'rows': merged.height,
                'appended': len(appended),
            },
            'last_local_migration': {
                'backup_path': str(backup.relative_to(root.parent.parent))
                if backup.is_relative_to(root.parent.parent) else str(backup),
                'appended': len(appended),
            },
        }
        write_text_atomic(
            json.dumps(manifest, ensure_ascii=False, indent=2) + '\n', manifest_path)
        result['rows_after'] = merged.height
        result['backup'] = str(backup)
    logger.info(f'fund_basic 本地检查：并入 {len(appended)} 只基金代码')
    return result
