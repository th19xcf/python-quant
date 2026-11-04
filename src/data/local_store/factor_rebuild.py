#!/usr/bin/env python3

"""用本地列存行情 + 本地分红快照批量重算全市场复权因子。

设计要点：
- 只读本地 Parquet（行情、分红），不访问远程库、不写库表；
- 按 ``chunk_size`` 个标的为一组计算，每组写成独立 part 文件并原子替换，
  同时把完成进度写入 ``manifest.json`` —— 中断后重跑自动跳过已完成分块；
- 全部分块完成后，用流式 ``sink_parquet`` 合并成单个
  ``factors/stock_adj_factor.parquet``（原子替换），避免千万行级内存峰值；
- 默认只补「快照里还没有的标的」：已存在的因子（例如远程已校准的 104 只）
  保持不变，避免与行情文件里内嵌的因子列出现口径漂移。
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from loguru import logger

from src.data.adj_factor_calculator import AdjFactorCalculator
from src.data.local_store.store import (
    STORE_VERSION,
    LocalFactorStore,
    LocalQuoteStore,
    refresh_quote_factor_columns,
)

_FACTOR_SCHEMA = {
    'ts_code': pl.Utf8,
    'trade_date': pl.Date,
    'qfq_factor': pl.Float64,
    'hfq_factor': pl.Float64,
}
_DIVIDEND_NUMERIC = (
    'cash_div', 'share_div', 'rights_issue_ratio', 'rights_issue_price')
_PARTS_GLOB = 'part_*.parquet'


def _sidecar_path(factor_store: LocalFactorStore) -> Path:
    return factor_store.root / 'factors' / 'covered_codes.txt'


def existing_factor_codes(factor_store: LocalFactorStore) -> set[str]:
    """已覆盖因子快照的代码集合。

    优先读 sidecar（每次重建后维护）；没有 sidecar 的历史快照直接扫
    ``ts_code`` 列求唯一值（只读一列，成本可控）。
    """
    sidecar = _sidecar_path(factor_store)
    if sidecar.exists():
        try:
            return {
                line.strip() for line in sidecar.read_text(encoding='utf-8').splitlines()
                if line.strip()
            }
        except OSError:
            logger.warning(f'[factor-rebuild] 读取 {sidecar} 失败，回退扫描因子文件')
    path = factor_store.factor_path
    if not path.exists():
        return set()
    frame = pl.read_parquet(path, columns=['ts_code'])
    return set(frame['ts_code'].unique().to_list())


def _write_sidecar(factor_store: LocalFactorStore, codes: Iterable[str]) -> None:
    path = _sidecar_path(factor_store)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = '\n'.join(sorted(codes)) + ('\n' if codes else '')
    tmp = path.with_suffix('.txt.tmp')
    tmp.write_text(payload, encoding='utf-8', newline='\n')
    os.replace(tmp, path)


def _compute_code_factors(code: str, quote_store: LocalQuoteStore,
                          factor_store: LocalFactorStore) -> pl.DataFrame | None:
    """单标的因子，计算口径与 ``TdxHandler`` 现算路径完全一致。"""
    prices = quote_store.read_file('stock', code)
    if prices is None or prices.is_empty() or 'close' not in prices.columns:
        return None
    prices = prices.select([
        pl.lit(code).alias('ts_code'),
        pl.col('date').alias('trade_date'),
        pl.col('close').cast(pl.Float64),
    ]).sort('trade_date')

    dividends = factor_store.dividends(code)
    if dividends is not None and not dividends.is_empty() and 'ex_date' in dividends.columns:
        dividends = dividends.filter(pl.col('ex_date').is_not_null())
        if not dividends.is_empty():
            dividends = dividends.with_columns([
                pl.col(col).cast(pl.Float64).fill_null(0.0)
                for col in _DIVIDEND_NUMERIC if col in dividends.columns
            ])
            result = AdjFactorCalculator.calculate_factors(prices, dividends)
        else:
            result = None
    else:
        result = None

    if result is None or result.is_empty():
        result = prices.with_columns([
            pl.lit(1.0).alias('qfq_factor'),
            pl.lit(1.0).alias('hfq_factor'),
        ])
    return result.select(list(_FACTOR_SCHEMA)).sort('trade_date')


def _load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {}


def _write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(payload)
    payload['updated_at'] = datetime.now().isoformat(timespec='seconds')
    tmp = path.with_suffix('.json.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(tmp, path)


def _clear_parts(rebuild_dir: Path) -> None:
    """清空重建临时目录里的 part 文件（保留目录，避免误删上层数据）。"""
    if not rebuild_dir.exists():
        return
    for part in rebuild_dir.glob(_PARTS_GLOB):
        try:
            part.unlink()
        except OSError as exc:
            logger.warning(f'[factor-rebuild] 清理临时分块失败 {part}: {exc}')


def _write_part(part_path: Path, frame: pl.DataFrame) -> None:
    tmp = part_path.with_suffix('.parquet.tmp')
    frame.write_parquet(
        tmp, compression='zstd', compression_level=3,
        statistics=True, row_group_size=100_000)
    os.replace(tmp, part_path)


def _assemble(parts: list[Path], target: Path, replaced: set[str]) -> int | None:
    """把分块合并进目标快照（流式，原子替换）；返回合并后的总行数。"""
    new_lazy = pl.concat([pl.scan_parquet(str(p)) for p in parts], how='vertical_relaxed')
    keep_lazy = None
    if target.exists() and replaced:
        keep_lazy = pl.scan_parquet(str(target)).filter(
            ~pl.col('ts_code').is_in(sorted(replaced)))
    source = (
        pl.concat([keep_lazy, new_lazy], how='vertical_relaxed')
        if keep_lazy is not None else new_lazy
    )
    source = source.sort(['ts_code', 'trade_date'])

    tmp = target.with_suffix('.parquet.tmp')
    source.sink_parquet(
        str(tmp), compression='zstd', compression_level=3,
        statistics=True, row_group_size=100_000)
    os.replace(tmp, target)
    try:
        return int(pl.scan_parquet(str(target)).select(pl.len()).collect().item())
    except (OSError, ValueError, TypeError) as exc:
        logger.warning(f'[factor-rebuild] 统计最终行数失败: {exc}')
        return None


def _process_chunk(
    index: int,
    chunk: list[str],
    quote_store: LocalQuoteStore,
    factor_store: LocalFactorStore,
    rebuild_dir: Path,
) -> tuple[list[str], list[str], int]:
    """计算单个分块并落盘，返回（已写入代码, 失败代码, 新增行数）。"""
    frames: list[pl.DataFrame] = []
    failed: list[str] = []
    for code in chunk:
        try:
            frame = _compute_code_factors(code, quote_store, factor_store)
        except (OSError, ValueError, RuntimeError, TypeError) as exc:
            failed.append(code)
            logger.warning(f'[factor-rebuild] {code} 计算失败: {exc}')
            continue
        if frame is not None and not frame.is_empty():
            frames.append(frame)
    if not frames:
        return [], failed, 0
    part_frame = pl.concat(frames, how='vertical_relaxed')
    _write_part(rebuild_dir / f'part_{index:05d}.parquet', part_frame)
    return list(chunk), failed, part_frame.height


def rebuild_factors_from_local(
    quote_store: LocalQuoteStore,
    factor_store: LocalFactorStore,
    *,
    codes: Iterable[str] | None = None,
    chunk_size: int = 200,
    include_existing: bool = False,
    resume: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    """批量重算并落盘因子快照，返回统计信息。

    Args:
        codes: 限定标的；None 表示行情列存里的全部个股。
        chunk_size: 每个分块处理的标的数（分块越小断点粒度越细）。
        include_existing: 是否重算快照中已有的标的（默认 False，只补缺）。
        resume: 是否复用同一批标的的已完成分块。
        dry_run: 只统计候选与行数，不落盘。
    """
    chunk_size = max(1, int(chunk_size))
    candidates = quote_store.codes('stock')
    if codes is not None:
        wanted = {c.strip().upper() for c in codes if c.strip()}
        candidates = [c for c in candidates if c in wanted]
    existing = existing_factor_codes(factor_store)
    if not include_existing:
        candidates = [c for c in candidates if c not in existing]

    planned_rows = 0
    for candidate in candidates:
        cov = quote_store.coverage('stock', candidate)
        if cov is not None:
            planned_rows += cov.rows
    if dry_run:
        logger.info(
            f'[dry-run] 候选 {len(candidates)} 只，预计 {planned_rows:,} 行；'
            f'已覆盖 {len(existing)} 只')
        return {
            'codes_added': len(candidates), 'rows_added': planned_rows,
            'codes_skipped': len(existing), 'elapsed_s': 0.0, 'dry_run': True,
        }

    if not candidates:
        logger.info(f'[factor-rebuild] 无缺失标的，快照已覆盖 {len(existing)} 只')
        return {'codes_added': 0, 'rows_added': 0,
                'codes_skipped': len(existing), 'elapsed_s': 0.0}

    rebuild_dir = factor_store.root / 'factors' / 'rebuild'
    rebuild_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = rebuild_dir / 'manifest.json'
    codes_hash = hashlib.sha1('\n'.join(candidates).encode('utf-8')).hexdigest()
    chunks = [candidates[i:i + chunk_size] for i in range(0, len(candidates), chunk_size)]

    state = _load_manifest(manifest_path)
    completed: set[int] = set()
    resuming = (
        resume
        and state.get('codes_hash') == codes_hash
        and state.get('status') in {'running', 'failed'}
    )
    if resuming:
        completed = {
            int(idx) for idx in state.get('completed_parts', [])
            if (rebuild_dir / f'part_{int(idx):05d}.parquet').exists()
        }
        logger.info(
            f'[factor-rebuild] 断点续跑：已完成 {len(completed)}/{len(chunks)} 个分块')
    else:
        _clear_parts(rebuild_dir)

    _write_manifest(manifest_path, {
        'version': STORE_VERSION,
        'status': 'running',
        'codes_hash': codes_hash,
        'chunk_size': chunk_size,
        'total_codes': len(candidates),
        'total_parts': len(chunks),
        'completed_parts': sorted(completed),
        'started_at': state.get('started_at') if resuming else
        datetime.now().isoformat(timespec='seconds'),
    })

    started = time.perf_counter()
    written_codes: list[str] = []
    failed_codes: list[str] = []
    rows_added = 0

    for index, chunk in enumerate(chunks):
        if index in completed:
            written_codes.extend(chunk)
            continue
        written, failed, chunk_rows = _process_chunk(
            index, chunk, quote_store, factor_store, rebuild_dir)
        written_codes.extend(written)
        failed_codes.extend(failed)
        rows_added += chunk_rows
        completed.add(index)
        _write_manifest(manifest_path, {
            'version': STORE_VERSION,
            'status': 'running',
            'codes_hash': codes_hash,
            'chunk_size': chunk_size,
            'total_codes': len(candidates),
            'total_parts': len(chunks),
            'completed_parts': sorted(completed),
            'started_at': state.get('started_at') if resuming else
            datetime.now().isoformat(timespec='seconds'),
        })
        if (index + 1) % 5 == 0 or index == len(chunks) - 1:
            elapsed = time.perf_counter() - started
            logger.info(
                f'[factor-rebuild] 进度 {index + 1}/{len(chunks)} 分块，'
                f'已写 {len(written_codes)} 只 / {rows_added:,} 行，已用 {elapsed:.1f}s')

    if failed_codes:
        logger.warning(
            f'[factor-rebuild] {len(failed_codes)} 只计算失败，首批: {failed_codes[:5]}')

    parts = [
        rebuild_dir / f'part_{idx:05d}.parquet'
        for idx in sorted(completed)
        if (rebuild_dir / f'part_{idx:05d}.parquet').exists()
    ]
    if not parts:
        _write_manifest(manifest_path, {'version': STORE_VERSION, 'status': 'failed',
                                        'codes_hash': codes_hash, 'error': '无可用分块'})
        raise RuntimeError('因子重建失败：没有生成任何分块')

    replaced = set(written_codes)
    total_rows = _assemble(parts, factor_store.factor_path, replaced)
    final_codes = (existing - replaced) | replaced
    _write_sidecar(factor_store, final_codes)

    # 因子快照是唯一可信来源：重建完成后立刻把行情 parquet 里内嵌的
    # qfq/hfq 副本对齐，避免「副本还是旧口径、因子快照已是新口径」。
    quote_refresh: dict[str, Any] | None = None
    try:
        quote_refresh = refresh_quote_factor_columns(
            quote_store, factor_store, codes=sorted(replaced))
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning(f'[factor-rebuild] 行情因子副本同步失败，需手工重跑: {exc}')

    elapsed = time.perf_counter() - started
    file_bytes = factor_store.factor_path.stat().st_size if factor_store.factor_path.exists() else 0
    previous_meta = factor_store.meta()
    meta = {
        'source': 'local_dividend',
        'factor_rows': total_rows,
        'factor_codes': len(final_codes),
        'codes_added': len(written_codes),
        'rows_added': rows_added,
        'failed_codes': len(failed_codes),
        'factor_file_bytes': file_bytes,
        'dividend_rows': previous_meta.get('dividend_rows'),
        'chunk_size': chunk_size,
        'include_existing': include_existing,
        'elapsed_s': round(elapsed, 3),
        'updated_at': datetime.now().isoformat(timespec='seconds'),
        'version': STORE_VERSION,
        'quote_factor_refresh': quote_refresh,
    }
    meta_path = factor_store.root / 'factors' / 'meta.json'
    tmp_meta = meta_path.with_suffix('.json.tmp')
    tmp_meta.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(tmp_meta, meta_path)

    _clear_parts(rebuild_dir)
    _write_manifest(manifest_path, {
        'version': STORE_VERSION,
        'status': 'completed',
        'codes_hash': codes_hash,
        'completed_parts': sorted(completed),
        'total_parts': len(chunks),
    })
    logger.info(
        f'[factor-rebuild] 完成：新增 {len(written_codes)} 只 / {rows_added:,} 行，'
        f'快照共 {len(final_codes)} 只，耗时 {elapsed:.1f}s，'
        f'体积 {file_bytes / 1024 / 1024:.1f} MB')
    return {
        'codes_added': len(written_codes),
        'rows_added': rows_added,
        'codes_skipped': len(existing),
        'failed_codes': len(failed_codes),
        'factor_codes': len(final_codes),
        'factor_rows': total_rows,
        'file_bytes': file_bytes,
        'elapsed_s': round(elapsed, 3),
    }
