#!/usr/bin/env python3

"""异常台账清单刷新（日常补数流程末步）。

从本地列存快照复算五份标的清单，覆盖写入
``data/local_store/meta/anomalies/``，供 ``src.data.quality.anomalies`` 按
mtime 懒加载：

- ``factor_tail_lag_codes.csv``：因子快照末端落后行情末端（近 N 天）；
- ``no_local_quote_codes.csv``：有除权事件但无本地行情的标的；
- ``source_stale_codes.csv``：本地序列未到最新交易日的标的（源端停更）；
- ``rights_issue_codes.csv``：配股事件日（仅同源复核，low_confidence）；
- ``placeholder_quote_codes.csv``：价格恒为 1.0 的占位行情（整段或尾部）。

设计约束：
- **只读数据文件**，不修改任何行情/因子/分红值；
- **幂等**：内容确定（排序稳定），重复执行覆盖旧文件，SHA 不变；
- **非致命**：任何异常只记 warning 并返回 ``status='failed'``，不抛给调用方；
- 返回耗时与各文件行数，供日常补数耗时表引用。
"""

from __future__ import annotations

import hashlib
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

from src.data.quality.anomalies import ANOMALY_LIST_DIR

#: 因子末端落后的“近期”窗口（天）：窗口内视为当次增量未覆盖，窗口外归入
#: 源端停更/历史缺口，避免把历史性落后重复登记。
DEFAULT_LAG_DAYS = 30


def _write_atomic(path: Path, frame: pl.DataFrame) -> None:
    """先写临时文件再替换，避免台账读取到半个 CSV。"""
    tmp = path.with_name(f'{path.name}.tmp')
    frame.write_csv(tmp, include_header=True)
    os.replace(tmp, path)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _build_factor_tail_lag(di: pl.DataFrame, adj_path: Path,
                           cutoff: date) -> pl.DataFrame:
    stock = di.filter(pl.col('data_type') == 'stock').select([
        'ts_code', pl.col('end').alias('quote_end')])
    factor = (
        pl.scan_parquet(adj_path)
        .group_by('ts_code')
        .agg(pl.col('trade_date').max().alias('factor_end'))
        .collect()
    )
    return (stock.join(factor, on='ts_code', how='left')
            .filter(pl.col('factor_end').is_not_null() &
                    (pl.col('factor_end') < pl.col('quote_end')) &
                    (pl.col('factor_end') >= cutoff))
            .with_columns((pl.col('factor_end') + timedelta(days=1)).alias('start'))
            .select(['ts_code', 'start', pl.col('quote_end').alias('end')])
            .sort('ts_code'))


def _build_no_local_quote(di: pl.DataFrame, div: pl.DataFrame) -> pl.DataFrame:
    stock_codes = set(di.filter(pl.col('data_type') == 'stock')['ts_code'].to_list())
    codes = sorted(set(div['ts_code'].unique().to_list()) - stock_codes)
    # 空列表时 Polars 会把 ts_code 推断成 Null，与右侧 str 连接键冲突；
    # 显式声明 schema，让「清单变干净 → 0 行」也能正常落盘。
    left = pl.DataFrame({'ts_code': codes}, schema={'ts_code': pl.Utf8})
    events = (div.filter(pl.col('ts_code').is_in(codes) &
                         pl.col('ex_date').is_not_null())
              .group_by('ts_code')
              .agg(pl.len().alias('events'),
                   pl.col('ex_date').max().alias('last_event')))
    if events.is_empty():
        events = pl.DataFrame(schema={
            'ts_code': pl.Utf8,
            'events': pl.UInt32,
            'last_event': div.schema['ex_date'],
        })
    empty_date = pl.lit(None, dtype=pl.Date)
    return (left
            .join(events, on='ts_code', how='left')
            .select(['ts_code', empty_date.alias('start'),
                     empty_date.alias('end'), 'events', 'last_event'])
            .sort('ts_code'))


def _build_source_stale(di: pl.DataFrame, latest: date) -> pl.DataFrame:
    empty_date = pl.lit(None, dtype=pl.Date)
    return (di.filter(pl.col('end') < latest)
            .select(['ts_code', 'data_type', pl.col('end').alias('local_end')])
            .with_columns(
                (pl.col('local_end') + timedelta(days=1)).alias('start'),
                empty_date.alias('end'))
            .select(['ts_code', 'start', 'end', 'data_type', 'local_end'])
            .sort(['data_type', 'ts_code']))


def _build_placeholder_quote(root: Path) -> pl.DataFrame | None:
    """扫描基金行情，识别源端「无行情」占位桩。

    判据是**三重条件** ``close == 1.0 且 volume == 0 且 amount == 0``。
    只看 ``close == 1.0`` 会把新发基金面值 1 元误判成占位桩；而真实成交换手
    不可能 ``volume == 0``，三个条件同时成立才确定是占位。

    通达信对已退市/转型/长期无成交的 LOF，用 ``close=1.0, volume=0`` 表示
    「该日无行情」（实测 ``163803.SZ`` 源端 1,456 行全是如此），本地只是
    忠实复制，因此这些行**不可修、只能隔离**。

    区间按**连续段**切分，一只标的多段就多行：早先只识别「全序列」与
    「尾部连续」两种形态，会漏掉头部连续段与中间的分散段（实测漏 147 只）。

    基金行情目录不存在或无任何 parquet 时返回 ``None``，调用方跳过该清单并
    保留旧文件，避免把故障态写成空清单。
    """
    fund_dir = root / 'quotes' / 'fund'
    if not fund_dir.is_dir():
        return None
    paths = sorted(fund_dir.rglob('*.parquet'))
    if not paths:
        return None
    rows: list[tuple[str, date, date, int, int, bool]] = []
    for path in paths:
        # 严格判据需要 volume/amount；早期落盘的基金文件可能只有 date/close，
        # 此时退回「只看 close==1.0」的宽松判据，并标记为不可池级剔除，
        # 避免因单只文件缺列把整份台账刷成 failed。
        schema = pl.read_parquet_schema(path)
        strict = {'volume', 'amount'} <= set(schema)
        columns = (['date', 'close', 'volume', 'amount'] if strict
                   else ['date', 'close'])
        frame = pl.read_parquet(path, columns=columns).sort('date')
        if frame.is_empty():
            continue
        if strict:
            mask = ((pl.col('close') == 1.0)
                    & (pl.col('volume') == 0)
                    & (pl.col('amount') == 0))
        else:
            mask = pl.col('close') == 1.0
        flags = frame.select(mask.alias('placeholder'))['placeholder'].to_list()
        dates = frame['date'].to_list()
        total = len(flags)
        # 连续段切分：遇到 False 就结算前一段
        start_idx: int | None = None
        for index, flag in enumerate(flags):
            if flag and start_idx is None:
                start_idx = index
            elif not flag and start_idx is not None:
                seg = index - start_idx
                rows.append((path.stem, dates[start_idx], dates[index - 1],
                             seg, total, seg == total and strict))
                start_idx = None
        if start_idx is not None:
            seg = total - start_idx
            rows.append((path.stem, dates[start_idx], dates[-1],
                         seg, total, seg == total and strict))
    if not rows:
        return pl.DataFrame(
            schema={'ts_code': pl.Utf8, 'start': pl.Date, 'end': pl.Date,
                    'rows': pl.Int64, 'total': pl.Int64, 'full': pl.Boolean})
    return pl.DataFrame(
        rows,
        schema=['ts_code', 'start', 'end', 'rows', 'total', 'full'],
        orient='row',
    ).sort(['ts_code', 'start'])


def _build_rights_issue(div: pl.DataFrame) -> pl.DataFrame:
    return (div.filter((pl.col('rights_issue_ratio') > 0) |
                       (pl.col('rights_issue_price') > 0))
            .filter(pl.col('ex_date').is_not_null())
            .select(['ts_code', pl.col('ex_date').alias('start'),
                     pl.col('ex_date').alias('end'), 'source'])
            .unique(subset=['ts_code', 'start'])
            .sort(['ts_code', 'start']))


def refresh_anomaly_lists(
    store_root: str | Path,
    *,
    list_dir: str | Path | None = None,
    lag_days: int = DEFAULT_LAG_DAYS,
) -> dict[str, Any]:
    """复算并覆盖五份异常台账清单（非致命、幂等）。

    Args:
        store_root: 本地列存根目录（含 ``meta/daily_index.parquet`` 与
            ``factors/*.parquet``）
        list_dir: 清单输出目录；None 用 ``ANOMALY_LIST_DIR``
        lag_days: 因子末端落后的近期窗口天数

    Returns:
        ``status``（ok/failed）、``elapsed_sec``、各文件行数/标的数/SHA；
        失败时额外带 ``error``，调用方据此记录 warning，不中断主流程。
    """
    started = datetime.now()
    root = Path(store_root)
    out_dir = Path(list_dir) if list_dir else ANOMALY_LIST_DIR
    result: dict[str, Any] = {
        'status': 'failed',
        'elapsed_sec': 0.0,
        'list_dir': str(out_dir),
        'files': {},
        'error': None,
    }
    try:
        di = pl.read_parquet(root / 'meta' / 'daily_index.parquet')
        div = pl.read_parquet(root / 'factors' / 'stock_dividend.parquet')
        latest_raw = di.filter(pl.col('data_type') == 'stock')['end'].max()
        if latest_raw is None:
            raise ValueError('行情快照为空，无法确定最新交易日')
        latest = (latest_raw if isinstance(latest_raw, date)
                  else date.fromisoformat(str(latest_raw)[:10]))
        cutoff = latest - timedelta(days=lag_days)
        frames = {
            'factor_tail_lag_codes.csv': _build_factor_tail_lag(
                di, root / 'factors' / 'stock_adj_factor.parquet', cutoff),
            'no_local_quote_codes.csv': _build_no_local_quote(di, div),
            'source_stale_codes.csv': _build_source_stale(di, latest),
            'rights_issue_codes.csv': _build_rights_issue(div),
        }
        placeholder = _build_placeholder_quote(root)
        if placeholder is not None:
            frames['placeholder_quote_codes.csv'] = placeholder
        out_dir.mkdir(parents=True, exist_ok=True)
        for name, frame in frames.items():
            path = out_dir / name
            _write_atomic(path, frame)
            result['files'][name] = {
                'rows': frame.height,
                'codes': frame['ts_code'].n_unique(),
                'sha256': _sha256(path),
                'path': str(path),
            }
        result['status'] = 'ok'
        result['latest_trade_date'] = str(latest)
        result['lag_cutoff'] = str(cutoff)
    except Exception as exc:  # noqa: BLE001 - 清单刷新失败不得拖垮补数主流程
        result['error'] = f'{type(exc).__name__}: {exc}'
    finally:
        result['elapsed_sec'] = round(
            (datetime.now() - started).total_seconds(), 2)
    if result['status'] == 'ok':
        logger.info(
            f'异常台账清单刷新完成：{result["files"]}，'
            f'耗时 {result["elapsed_sec"]}s')
    return result


__all__ = ('DEFAULT_LAG_DAYS', 'refresh_anomaly_lists')
