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

**落盘顺序（勿调换）**：``分块落盘 → manifest 记 running → 合并出快照 →
manifest 记 completed → 删分块``，与 ``src/data/local_store/turnover.py``
的收尾顺序一致。

跨文件一致性靠**顺序**保证，不靠 ``os.replace``——按 ``atomic_write`` 的约定，
单文件替换原子，不等于多文件事务。这里维持的不变式是「manifest 记录为已完成
的分块，在磁盘上一定真实存在」，因此任意中断点都能被
:func:`_inspect_resume_state` 在启动时判定，不会被静默当成「没有断点」：

- 清分块之前中断：manifest 与全部分块都完好 → 续跑；
- 清分块之后中断：manifest 仍指向已删分块 → 判「分块缺失」后重采；
- 合并前 / 合并中中断：manifest=running + 部分分块 → 只补缺失分块；
- 合并完成但 manifest 未收尾：全部分块仍在 → 直接合并，**不重采**；
- manifest 记 completed 之后、删分块未完成：分块只是残留 → 清理后按本次参数重建。

已知边界：上述判定假设进程级中断（异常、Ctrl-C、OOM），``os.replace`` 之后
目标文件即可见；断电这类「rename 未落盘」的场景不在覆盖范围内，最坏结果是
一次多余的全量重采，不会产出错误数据。
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from loguru import logger

from src.data.adj_factor_calculator import AdjFactorCalculator
from src.data.atomic_write import write_parquet_atomic, write_text_atomic
from src.data.factors_manifest import write_factors_manifest
from src.data.local_store.store import (
    STORE_VERSION,
    LocalFactorStore,
    LocalQuoteStore,
    refresh_quote_factor_columns,
)
from src.data.trading_calendar import filter_nontrading_bars

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
    payload = '\n'.join(sorted(codes)) + ('\n' if codes else '')
    write_text_atomic(payload, path)


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
    # G-112：派生层落盘前剔除非交易日 bar（原始 quotes 不动；裁定表 keep 行保留）
    prices = filter_nontrading_bars(prices, factor_store.root)

    dividends = factor_store.dividends(code)
    if dividends is not None and not dividends.is_empty() and 'ex_date' in dividends.columns:
        dividends = dividends.filter(pl.col('ex_date').is_not_null())
        if not dividends.is_empty():
            dividends = dividends.with_columns([
                pl.col(col).cast(pl.Float64).fill_null(0.0)
                for col in _DIVIDEND_NUMERIC if col in dividends.columns
            ])
            # 事件截断由 calculate_factors 按 prices 末行自动完成，不额外传 as_of：
            # 本函数只重建到行情末行为止的快照，prices 末行即作用域边界，
            # 显式再传一个日期等于重复同一事实，反而可能在增量重建时漂移。
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
    payload = dict(payload)
    payload['updated_at'] = datetime.now().isoformat(timespec='seconds')
    write_text_atomic(json.dumps(payload, ensure_ascii=False, indent=2), path)


def _clear_parts(rebuild_dir: Path) -> None:
    """清空重建临时目录里的 part 文件（保留目录，避免误删上层数据）。"""
    if not rebuild_dir.exists():
        return
    for part in rebuild_dir.glob(_PARTS_GLOB):
        try:
            part.unlink()
        except OSError as exc:
            logger.warning(f'[factor-rebuild] 清理临时分块失败 {part}: {exc}')


def _parts_on_disk(rebuild_dir: Path) -> dict[int, Path]:
    """扫描磁盘上的分块，返回 ``{分块下标: 路径}``；文件名不合规的忽略。"""
    parts: dict[int, Path] = {}
    if not rebuild_dir.exists():
        return parts
    for path in rebuild_dir.glob(_PARTS_GLOB):
        try:
            parts[int(path.stem.rsplit('_', 1)[1])] = path
        except (IndexError, ValueError):
            continue
    return parts


def _inspect_resume_state(
    rebuild_dir: Path,
    state: dict[str, Any],
    *,
    codes_hash: str,
    chunk_size: int,
    total_parts: int,
    resume: bool,
) -> tuple[set[int], str]:
    """启动时核对 manifest 记录与磁盘分块，返回（可复用分块, 处置动作）。

    处置动作：

    - ``resume``：manifest 与磁盘分块同属本批，续跑缺失分块；
    - ``restart``：没有可用断点，旧分块作废后重采；
    - ``assembled``：上一轮已把分块合并进快照（manifest=completed），
      剩下的分块只是「删除动作没跑完」的残留，作废后按本次参数重来。

    判定只看 manifest 记录的 ``completed_parts`` 与分块文件是否存在，不看
    文件时间，因此「分块已写、manifest 未更新」这类错位一律按缺失处理，
    不会被误当成已完成而漏算标的。
    """
    parts = _parts_on_disk(rebuild_dir)
    if not state:
        if parts:
            logger.warning(
                f'[factor-rebuild] manifest 缺失但发现 {len(parts)} 个残留分块，'
                f'无法判定完整性，作废后重采')
        return set(), 'restart'
    status = state.get('status')
    if status == 'completed':
        if parts:
            logger.info(
                f'[factor-rebuild] 上一轮已合并落盘（manifest=completed），'
                f'清理 {len(parts)} 个未删净的残留分块，按本次参数重建')
        return set(), 'assembled'
    if not resume:
        return set(), 'restart'
    if (state.get('codes_hash') != codes_hash
            or int(state.get('chunk_size') or chunk_size) != chunk_size):
        # 分块下标只在「同一批标的 + 同一分块粒度」下才有意义：换了粒度仍按
        # 旧下标跳过，会把新布局里落在别的块上的标的漏掉，等于静默丢行。
        logger.info('[factor-rebuild] 标的集合或分块粒度与上次不同，旧分块作废重采')
        return set(), 'restart'
    if status not in {'running', 'failed'}:
        logger.warning(f'[factor-rebuild] manifest 状态未知（{status!r}），重采')
        return set(), 'restart'

    recorded = {int(idx) for idx in state.get('completed_parts') or []}
    completed = recorded & set(parts)
    lost = recorded - completed
    if lost:
        logger.warning(
            f'[factor-rebuild] {len(lost)} 个已登记分块在磁盘上缺失，按缺失重采')
    orphans = set(parts) - recorded
    if orphans:
        logger.info(
            f'[factor-rebuild] {len(orphans)} 个分块未登记（写入后 manifest 未更新），'
            f'将被重新计算覆盖')
    if completed and len(completed) >= total_parts:
        logger.info(
            f'[factor-rebuild] 检测到分块完整但 manifest 未收尾'
            f'（{len(completed)}/{total_parts}），直接合并，不重采')
    else:
        logger.info(
            f'[factor-rebuild] 断点续跑：已完成 {len(completed)}/{total_parts} 个分块')
    return completed, 'resume'


def _write_part(part_path: Path, frame: pl.DataFrame) -> None:
    write_parquet_atomic(
        frame, part_path, compression='zstd', compression_level=3,
        statistics=True, row_group_size=100_000)


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

    write_parquet_atomic(
        source, target, compression='zstd', compression_level=3,
        statistics=True, row_group_size=100_000)
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
    completed, action = _inspect_resume_state(
        rebuild_dir, state, codes_hash=codes_hash, chunk_size=chunk_size,
        total_parts=len(chunks), resume=resume)
    if action != 'resume':
        # 旧分块不属于本次这批量（换批 / 换分块粒度 / 上一轮已合并），先作废
        _clear_parts(rebuild_dir)

    _write_manifest(manifest_path, {
        'version': STORE_VERSION,
        'status': 'running',
        'codes_hash': codes_hash,
        'chunk_size': chunk_size,
        'total_codes': len(candidates),
        'total_parts': len(chunks),
        'completed_parts': sorted(completed),
        'started_at': state.get('started_at') if action == 'resume' else
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
            'started_at': state.get('started_at') if action == 'resume' else
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
    # 快照已原子替换：立即丢弃同进程内的旧 LRU / LazyFrame / mtime 结论，
    # 否则后续对齐核对会在 TTL 窗口内读到重建前的因子。
    factor_store.invalidate()
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
    write_text_atomic(json.dumps(meta, ensure_ascii=False, indent=2), meta_path)

    # 收尾顺序不可调换（对照 turnover.py 的收尾）：先记 completed，再删分块。
    # 反过来的话，两步之间中断会留下「分块已删、manifest 仍 running」，
    # 重启时无从判定分块是否完整，只能全量重采；按现在这个顺序，中断最多
    # 留下几个没删掉的分块，下次启动按 manifest 的 completed 识别为残留并清理。
    _write_manifest(manifest_path, {
        'version': STORE_VERSION,
        'status': 'completed',
        'codes_hash': codes_hash,
        'chunk_size': chunk_size,
        'completed_parts': sorted(completed),
        'total_parts': len(chunks),
    })
    _clear_parts(rebuild_dir)
    # 目录级 manifest 收尾：factors/ 的逐文件 SHA256 / 行数 / 区间记账，
    # 必须在快照与 sidecar 都已落盘之后刷新，否则记的是上一版文件的哈希。
    factors_manifest_path = write_factors_manifest(factor_store.root)
    logger.info(f'[factor-rebuild] 已刷新 {factors_manifest_path}')
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
