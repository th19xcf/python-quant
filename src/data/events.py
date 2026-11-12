#!/usr/bin/env python3

"""公告事件本地表：离线导入、增量追加与冷却窗口查询（运行时不联网）。

本模块把 ``research/events/`` 里的公告解析逻辑提升为生产可用的本地表：

- 落盘：``{store_root}/meta/events/events.parquet``（单表，按事件去重）；
- 覆盖声明：``{store_root}/meta/events/coverage.json``（导入区间与完整性）；
- 导入：支持证券公告原始表（``keyword/stock_code/display_time``）与已规范化表
  （``ts_code/event_type/publish_time``），CSV / Parquet 均可；
- 逐行来源标记：``source`` 列的合法取值见 ``EVENT_SOURCES``，新增来源须先在那里登记，
  禁止采集脚本自由拼写（DG-19：混源必须逐行可追溯）；
- 运行时只读本地 parquet，**禁止联网**（项目硬约束）。

事件冷却口径：公告日 T，冷却区间 ``[T+1, T+N]``，N 为该事件类型的交易日数。
覆盖区间之外一律返回 ``unknown``——**不得当成「没有事件」**。
"""

from __future__ import annotations

import argparse
import json
import re
from bisect import bisect_left, bisect_right
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Sequence

import polars as pl
from loguru import logger

from src.data.atomic_write import write_parquet_atomic

RISK_EVENT_COOLDOWNS: dict[str, int] = {
    '并购重组': 20,
    '重大合同/中标': 5,
    '监管处罚': 5,
}

#: ``events.parquet`` 里 ``source`` 列的全部合法取值（逐行来源标记，DG-19）。
#: 新增来源必须先在此登记，禁止采集脚本自由拼写：
#:   cninfo  —— 巨潮 hisAnnouncement，默认口径；未传 source 时按它兜底
#:   em_yjyg —— 东方财富业绩预告独立通道（业绩预增/预减），非公告检索
#:   em_ann  —— 东方财富公告口径补数（2021-10 股权激励；title 检索宽于巨潮 searchkey）
EVENT_SOURCES: tuple[str, ...] = ('cninfo', 'em_yjyg', 'em_ann')

KEYWORD_TO_TYPE = {
    '减持': '减持',
    '增持': '增持',
    '股权激励': '股权激励',
    '中标': '重大合同/中标',
    '重大合同': '重大合同/中标',
    '处罚': '监管处罚',
    '立案': '监管处罚',
    '重组': '并购重组',
    '并购': '并购重组',
    '分红': '分红送转',
}

TITLE_FILTERS: dict[str, tuple[str | None, str | None]] = {
    '减持': (r'减持', r'增持'),
    '增持': (r'增持', r'减持'),
    '股权激励': (r'股权激励|股票期权|限制性股票', None),
    '重大合同/中标': (r'中标|重大合同|框架协议|战略合作', None),
    '监管处罚': (r'处罚|监管措施|立案|警示函|责令改正|问询', None),
    '并购重组': (r'重组|并购|收购|吸收合并', None),
    '分红送转': (r'分红|利润分配|权益分派|派息', None),
}

STATUS_COOLING = 'cooling'
STATUS_CLEAR = 'clear'
STATUS_UNKNOWN = 'unknown'

EVENT_COLUMNS = ('event_id', 'ts_code', 'event_type', 'publish_time',
                 'title', 'source')
DEDUP_DAYS = 30
_EMPTY_SCHEMA = {
    'event_id': pl.String,
    'ts_code': pl.String,
    'event_type': pl.String,
    'publish_time': pl.Datetime,
    'title': pl.String,
    'source': pl.String,
}

# 代理事件表（G-070 定增 / G-071 限售解禁）：落点 meta/events/proxy/，
# 由 _staging/event_proxy_events_20261112/build_tables.py 从 stock_shares 派生。
EQUITY_ISSUE_PROXY_FILENAME = 'equity_issue_proxy.parquet'
UNLOCK_PROXY_FILENAME = 'unlock_proxy.parquet'

EQUITY_ISSUE_PROXY_SCHEMA: dict[str, type[pl.DataType]] = {
    'ts_code': pl.String,
    'event_date': pl.Date,
    'change_reason': pl.String,
    'signal': pl.String,
    'has_concurrent_reason': pl.Boolean,
    'total_share': pl.Int64,
    'total_share_prev': pl.Int64,
    'issued_shares': pl.Int64,
    'issued_ratio_prev': pl.Float64,
    'delta_branch': pl.String,
    'event_kind': pl.String,
    'basis': pl.String,
    'source_data': pl.String,
}

UNLOCK_PROXY_SCHEMA: dict[str, type[pl.DataType]] = {
    'ts_code': pl.String,
    'event_date': pl.Date,
    'change_reason': pl.String,
    'primary_reason': pl.String,
    'float_share': pl.Int64,
    'float_share_prev': pl.Int64,
    'total_share': pl.Int64,
    'total_share_prev': pl.Int64,
    'unlock_shares': pl.Int64,
    'unlock_ratio_total': pl.Float64,
    'float_delta_branch': pl.String,
    'is_equity_incentive': pl.Boolean,
    'event_kind': pl.String,
    'basis': pl.String,
    'source_data': pl.String,
}


def events_path(store_root: Path | str) -> Path:
    return Path(store_root) / 'meta' / 'events' / 'events.parquet'


def coverage_path(store_root: Path | str) -> Path:
    return Path(store_root) / 'meta' / 'events' / 'coverage.json'


def _as_date(value: Any) -> date | None:
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    try:
        return datetime.fromisoformat(text.replace('Z', '')).date()
    except ValueError:
        return None


def _to_ts_code(stock_code: Any) -> str | None:
    from src.utils.code_converter import parse_code  # noqa: PLC0415

    try:
        symbol, market = parse_code(str(stock_code).zfill(6))
    except (ValueError, TypeError):
        return None
    return f'{symbol}.{market}'


def empty_events() -> pl.DataFrame:
    return pl.DataFrame(schema=_EMPTY_SCHEMA)


def normalize_announcements(
    frame: pl.DataFrame, start: Any = None, end: Any = None,
    source: str | None = None,
) -> pl.DataFrame:
    """把原始公告表规范化为事件表（同一标的同类型 30 自然日内只留最早一条）。

    ``source`` 是调用方声明的行级来源标记（DG-19）。取值优先级：记录自带的
    ``source`` 键 > 调用方传入的 ``source``；**两者都没有直接抛错**，不兜底成
    ``cninfo``——否则用东财通道抓来的公告会被错标成巨潮口径。
    """
    if frame.is_empty():
        return empty_events()
    need = {'keyword', 'stock_code', 'display_time'}
    missing = need - set(frame.columns)
    if missing:
        raise ValueError(f'原始公告表缺少列: {sorted(missing)}')
    start_day = _as_date(start)
    end_day = _as_date(end)
    default_source = str(source).strip() if source else ''

    rows: list[dict[str, Any]] = []
    for position, item in enumerate(frame.to_dicts(), 1):
        event_type = KEYWORD_TO_TYPE.get(str(item.get('keyword') or ''))
        if event_type is None:
            continue
        title = str(item.get('title') or '')
        include, exclude = TITLE_FILTERS.get(event_type, (None, None))
        if include and not re.search(include, title):
            continue
        if exclude and re.search(exclude, title):
            continue
        published = item.get('display_time')
        if isinstance(published, str):
            published = _parse_time(published)
        if not isinstance(published, datetime):
            continue
        if start_day is not None and published.date() < start_day:
            continue
        if end_day is not None and published.date() > end_day:
            continue
        ts_code = _to_ts_code(item.get('stock_code'))
        if ts_code is None:
            continue
        row_source = str(item.get('source') or '').strip() or default_source
        if not row_source:
            raise ValueError(
                f'公告记录缺少来源标记且调用方未传 source（第 {position} 条，'
                f"art_code={item.get('art_code')}，stock_code={item.get('stock_code')}）；"
                '禁止兜底成 cninfo，请显式传入 cninfo / em_ann / em_yjyg')
        rows.append({
            'event_id': f"{item.get('art_code') or ''}-{item.get('stock_code')}",
            'ts_code': ts_code,
            'event_type': event_type,
            'publish_time': published,
            'title': title,
            'source': row_source,
        })
    return dedup_events(_coerce_events(pl.DataFrame(rows, infer_schema_length=None)
                                       if rows else empty_events()))


def _parse_time(value: Any) -> datetime | None:
    text = str(value).strip()
    if not text:
        return None
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
        try:
            return datetime.strptime(text[:len(fmt)], fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace('Z', '').replace('T', ' '))
    except ValueError:
        return None


def _coerce_events(frame: pl.DataFrame) -> pl.DataFrame:
    """统一列顺序与类型；缺列补空，日期列统一为 Datetime。"""
    if frame.is_empty():
        return empty_events()
    for column in EVENT_COLUMNS:
        if column not in frame.columns:
            frame = frame.with_columns(pl.lit(None).alias(column))
    frame = frame.select(list(EVENT_COLUMNS))
    dtype = frame.schema['publish_time']
    if dtype == pl.Date:
        frame = frame.with_columns(pl.col('publish_time').cast(pl.Datetime))
    elif dtype == pl.String:
        frame = frame.with_columns(
            pl.col('publish_time').str.slice(0, 19)
            .str.to_datetime(strict=False))
    return frame.filter(pl.col('publish_time').is_not_null())


def dedup_events(frame: pl.DataFrame) -> pl.DataFrame:
    """同标的同类型 30 自然日内只保留最早一条（滤掉进展/结果类重复公告）。

    排序键必须带上 ``event_id`` 兜底：同一天常有多份同类型公告（如激励计划
    草案与律所意见书），只按 ``publish_time`` 排序时「保留哪一条」取决于输入
    行序，会让同一批次在不同进程选出不同的代表行，破坏重复导入的幂等性。
    """
    if frame.is_empty():
        return frame
    records = sorted(frame.to_dicts(),
                     key=lambda item: (item['ts_code'], item['event_type'],
                                       item['publish_time'], item['event_id']))
    kept: list[dict[str, Any]] = []
    markers: dict[tuple[str, str], datetime] = {}
    for item in records:
        key = (item['ts_code'], item['event_type'])
        previous = markers.get(key)
        if previous is not None \
                and item['publish_time'] - previous <= timedelta(days=DEDUP_DAYS):
            continue
        markers[key] = item['publish_time']
        kept.append(item)
    return _coerce_events(pl.DataFrame(kept, infer_schema_length=None))


def _drop_near_existing(batch: pl.DataFrame,
                        existing: pl.DataFrame) -> pl.DataFrame:
    """剔除与既有行同标的同类型且相距 ≤30 天的批次行。

    既有行本身一条不动，这里只决定「新行要不要写进来」。没有这一步，
    同一天的多份同类型公告（不同 ``art_code``）会在换一个代表行时被重复
    追加，重复导入同一批次就不再幂等。
    """
    if batch.is_empty() or existing.is_empty():
        return batch
    window = timedelta(days=DEDUP_DAYS)
    known: dict[tuple[str, str], list[datetime]] = {}
    for ts_code, event_type, publish_time in existing.select(
            'ts_code', 'event_type', 'publish_time').iter_rows():
        known.setdefault((ts_code, event_type), []).append(publish_time)
    keep: list[bool] = []
    for ts_code, event_type, publish_time in batch.select(
            'ts_code', 'event_type', 'publish_time').iter_rows():
        times = known.get((ts_code, event_type)) or []
        near = any(abs(publish_time - t) <= window for t in times)
        keep.append(not near)
    return batch.filter(pl.Series('_keep', keep))


def _row_key(item: dict[str, Any]) -> tuple:
    return tuple(item[column] for column in EVENT_COLUMNS)


def _assert_replace_conservation(
    existing: pl.DataFrame, incoming: pl.DataFrame, merged: pl.DataFrame,
) -> dict[str, int]:
    """``mode='replace'`` 落盘前的三项守恒断言（DG-31），不通过直接抛错。

    三项断言都不是「看起来合理」的自证，而是能对具体错误失败：

    1. **行数守恒**：写盘行数必须等于待写行数；存量行必须被完整归入
       「保留未改 / 保留但改值 / 被替换掉」三类，三类之和必须等于存量行数。
       漏算任何一类（例如只统计 ``event_id`` 交集就当年守恒）即报错。
    2. **改动行数**：同 ``event_id`` 的既有行若内容被改写，逐行计入
       ``existing_modified``；该计数直接回填 ``stats['existing_affected']``，
       不再写死 0。
    3. **null 计数**：写盘后各列空值数必须等于待写数据空值数（写路径不得
       凭空造 null），且不得多于存量空值数——**空值从 0 变成 N 在这里必然
       失败**，正是 DG-31 反例中 ``pl.coalesce`` 无兜底那类缺陷的拦截点。

    返回三类存量行的计数，供调用方写进返回统计。
    """
    if merged.height != incoming.height:
        raise RuntimeError(
            f'replace 写盘行数不守恒：待写 {incoming.height} 行，实际 {merged.height} 行')

    merged_keys: dict[str, list[tuple]] = {}
    for item in merged.to_dicts():
        merged_keys.setdefault(item['event_id'], []).append(_row_key(item))
    retained = modified = replaced_out = 0
    for item in existing.to_dicts():
        matches = merged_keys.get(item['event_id']) or []
        if not matches:
            replaced_out += 1
        elif _row_key(item) in matches:
            retained += 1
        else:
            modified += 1
    if retained + modified + replaced_out != existing.height:
        raise RuntimeError(
            f'replace 存量行不守恒：存量 {existing.height} 行，'
            f'保留 {retained} + 改值 {modified} + 替换掉 {replaced_out} '
            f'= {retained + modified + replaced_out} 行')

    null_incoming = incoming.null_count()
    null_merged = merged.null_count()
    null_existing = existing.null_count()
    for column in EVENT_COLUMNS:
        incoming_nulls = int(null_incoming[column][0])
        merged_nulls = int(null_merged[column][0])
        if merged_nulls != incoming_nulls:
            raise RuntimeError(
                f'replace 写路径为 {column} 造出了空值：待写 {incoming_nulls} 行空，'
                f'写盘 {merged_nulls} 行空')
        existing_nulls = int(null_existing[column][0])
        if merged_nulls > existing_nulls:
            raise RuntimeError(
                f'replace 使 {column} 的空值从 {existing_nulls} 行增加到 '
                f'{merged_nulls} 行（空值从 0 变成 N 即判定为写回缺陷）')
    return {'existing_retained': retained, 'existing_modified': modified,
            'existing_replaced_out': replaced_out}


def load_events(store_root: Path | str) -> pl.DataFrame:
    path = events_path(store_root)
    if not path.is_file():
        return empty_events()
    try:
        return _coerce_events(pl.read_parquet(path))
    except (OSError, ValueError) as exc:
        logger.warning(f'[events] 事件表读取失败，按无数据处理: {path}: {exc}')
        return empty_events()


def load_equity_issue_events(store_root: Path | str) -> pl.DataFrame:
    """读取定增代理事件表 ``{store_root}/meta/events/proxy/equity_issue_proxy.parquet``。

    **代理口径，不是发行公告事件**：事件日取 ``stock_shares.effective_date``
    （增发股份上市日，通常比发行结果公告滞后约 1~4 周），新增股本由
    ``total_share`` 相邻行差分得到；**不区分定向增发与公开增发**（含新三板），
    不含发行规模、发行价与预案日。每行的 ``event_kind`` / ``basis`` 已写死口径，
    下游不得把本表当「定增公告」使用。

    只读：不加工、不过滤、不排序。文件缺失或 schema 不符时直接抛错，
    不返回空表——空表会被下游误判为「期间内没有定增事件」。
    """
    path = Path(store_root) / 'meta' / 'events' / 'proxy' / EQUITY_ISSUE_PROXY_FILENAME
    if not path.is_file():
        raise FileNotFoundError(
            f'定增代理事件表缺失: {path}；该表由 '
            '_staging/event_proxy_events_20261112/build_tables.py 生成，'
            '未生成前不得按「无事件」处理')
    frame = pl.read_parquet(path)
    missing = [name for name in EQUITY_ISSUE_PROXY_SCHEMA
               if name not in frame.columns]
    if missing:
        raise ValueError(f'定增代理事件表缺列 {missing}，文件: {path}')
    mismatched = [f'{name}={frame.schema[name]}' for name, dtype
                  in EQUITY_ISSUE_PROXY_SCHEMA.items()
                  if type(frame.schema[name]) is not dtype]
    if mismatched:
        raise ValueError(f'定增代理事件表列类型不符 {mismatched}，文件: {path}')
    blank = frame.select(
        (pl.col('event_kind').is_null()
         | (pl.col('event_kind').str.len_chars() == 0)
         | pl.col('basis').is_null()
         | (pl.col('basis').str.len_chars() == 0)).sum()).item()
    if blank:
        raise ValueError(
            f'定增代理事件表有 {blank} 行 event_kind/basis 为空（口径未写死），'
            f'文件: {path}')
    return frame


def load_unlock_events(store_root: Path | str) -> pl.DataFrame:
    """读取限售解禁代理事件表 ``{store_root}/meta/events/proxy/unlock_proxy.parquet``。

    **代理口径，不是解禁公告事件**：事件日取 ``stock_shares.effective_date``
    （限售股上市流通日），解禁股数由 ``float_share`` 相邻行差分得到；
    **不区分解禁与新增限售**，差分未命中时按分支留空（``prev_value_null``）
    或为负（``negative``，股份性质变更/回购注销等），不得自行截断成正值。

    双计风险：``is_equity_incentive`` 为真的行同时属于股权激励限售股上市，
    与 ``ev_equity_incentive`` 类因子叠加会重复计入，组合使用前须去重。

    只读：不加工、不过滤、不排序。文件缺失或 schema 不符时直接抛错，
    不返回空表——空表会被下游误判为「期间内没有解禁事件」。
    """
    path = Path(store_root) / 'meta' / 'events' / 'proxy' / UNLOCK_PROXY_FILENAME
    if not path.is_file():
        raise FileNotFoundError(
            f'限售解禁代理事件表缺失: {path}；该表由 '
            '_staging/event_proxy_events_20261112/build_tables.py 生成，'
            '未生成前不得按「无事件」处理')
    frame = pl.read_parquet(path)
    missing = [name for name in UNLOCK_PROXY_SCHEMA if name not in frame.columns]
    if missing:
        raise ValueError(f'限售解禁代理事件表缺列 {missing}，文件: {path}')
    mismatched = [f'{name}={frame.schema[name]}' for name, dtype
                  in UNLOCK_PROXY_SCHEMA.items()
                  if type(frame.schema[name]) is not dtype]
    if mismatched:
        raise ValueError(f'限售解禁代理事件表列类型不符 {mismatched}，文件: {path}')
    blank = frame.select(
        (pl.col('event_kind').is_null()
         | (pl.col('event_kind').str.len_chars() == 0)
         | pl.col('basis').is_null()
         | (pl.col('basis').str.len_chars() == 0)).sum()).item()
    if blank:
        raise ValueError(
            f'限售解禁代理事件表有 {blank} 行 event_kind/basis 为空（口径未写死），'
            f'文件: {path}')
    return frame


def load_coverage(store_root: Path | str) -> dict[str, Any]:
    path = coverage_path(store_root)
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        logger.warning(f'[events] 覆盖声明读取失败: {path}: {exc}')
        return {}


def _recompute_source(frame: pl.DataFrame) -> str:
    """按表内实测重算字段级来源串（DG-19）：``cninfo=181110|em_yjyg=4543|em_ann=97``。

    顺序固定：先按 :data:`EVENT_SOURCES` 的登记顺序，未登记的取值按字典序追加在后；
    缺的类不出现。空表返回空串；存在空 ``source`` 时直接报错，不静默丢行。
    """
    if frame.is_empty():
        return ''
    if 'source' not in frame.columns:
        raise ValueError('事件表缺少 source 列，无法重算字段级来源标记（DG-19）')
    counts = dict(frame.group_by('source').len().iter_rows())
    if None in counts:
        raise ValueError(
            f'事件表存在 {counts[None]} 行 source 为空，无法写成字段级来源标记（DG-19）')
    ordered = [key for key in EVENT_SOURCES if key in counts]
    ordered += sorted(key for key in counts if key not in EVENT_SOURCES)
    return '|'.join(f'{key}={counts[key]}' for key in ordered)


def _write_coverage(store_root: Path | str, frame: pl.DataFrame,
                    complete: bool,
                    coverage_start: Any = None,
                    coverage_end: Any = None) -> dict[str, Any]:
    start_day = _as_date(coverage_start)
    end_day = _as_date(coverage_end)
    # 保留既有声明里的附加字段（如分片审计口径），只覆盖本次能计算出的键，
    # 避免一次入库把人工补充的可回溯性说明整体冲掉。
    path = coverage_path(store_root)
    payload: dict[str, Any] = {}
    if path.is_file():
        try:
            loaded = json.loads(path.read_text(encoding='utf-8'))
            if isinstance(loaded, dict):
                payload = loaded
        except (OSError, ValueError) as exc:
            logger.warning(f'[events] 覆盖声明读取失败，将整体重写: {exc}')
    # ``source`` 一律按表内实测重算，不用调用方参数整值覆盖：多来源表必须写成
    # 字段级串，否则一次 import 就会把 cninfo=…|em_ann=… 冲成单值（DG-19）。
    previous_source = str(payload.get('source') or '')
    recomputed = _recompute_source(frame) or previous_source
    if frame.is_empty():
        logger.warning('[events] 覆盖声明写入时事件表为空，source 沿用既有声明')
    elif previous_source and previous_source != recomputed:
        logger.warning(
            f'[events] coverage.source 与表内实测不一致，按实测重写: '
            f'旧={previous_source} 新={recomputed}')
    if frame.is_empty():
        info: dict[str, Any] = {
            'source': recomputed,
            'start': start_day.isoformat() if start_day else None,
            'end': end_day.isoformat() if end_day else None,
            'rows': 0,
            'complete': complete, 'updated_at': datetime.now().isoformat(timespec='seconds'),
        }
    else:
        data_start = frame['publish_time'].min()
        data_end = frame['publish_time'].max()
        start_value = start_day or _as_date(data_start)
        end_value = end_day or _as_date(data_end)
        info = {
            'source': recomputed,
            'start': start_value.isoformat() if start_value else None,
            'end': end_value.isoformat() if end_value else None,
            'rows': frame.height,
            'complete': complete,
            'declared': bool(start_day and end_day),
            'updated_at': datetime.now().isoformat(timespec='seconds'),
        }
    payload.update(info)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding='utf-8')
    # 差异只回传给调用方，不写进 coverage.json 污染声明
    return {**info, 'source_previous': previous_source}


def _read_source(path: Path) -> pl.DataFrame:
    if path.suffix.lower() == '.csv':
        return pl.read_csv(path, try_parse_dates=True, infer_schema_length=10000)
    if path.suffix.lower() in ('.parquet', '.pq'):
        return pl.read_parquet(path)
    raise ValueError(f'不支持的事件文件类型: {path.suffix}')


def _detect_format(columns: Iterable[str]) -> str:
    names = set(columns)
    if {'keyword', 'stock_code', 'display_time'} <= names:
        return 'raw'
    if {'ts_code', 'event_type'} & names and 'publish_time' in names:
        return 'normalized'
    raise ValueError(
        '无法识别事件文件格式：需要 (keyword, stock_code, display_time) '
        '或 (ts_code, event_type, publish_time)')


def import_file(path: Path | str, store_root: Path | str | None = None,
                mode: str = 'replace', source: str | None = None,
                complete: bool = True, start: Any = None,
                end: Any = None, coverage_start: Any = None,
                coverage_end: Any = None,
                dry_run: bool = False) -> dict[str, Any]:
    """导入离线事件文件；``mode='replace'`` 全量替换，``'append'`` 增量合并。

    ``source`` 是行级来源标记（DG-19），**必须由调用方显式声明**（``cninfo`` /
    ``em_ann`` / ``em_yjyg``）：原始公告表没有 ``source`` 列、已规范化表缺该列时
    都用它填充，两者都没有直接抛错，不兜底成 ``cninfo``。已规范化表自带
    ``source`` 列时以表内值为准，``source`` 参数不覆盖既有标记。

    ``append`` 的去重语义（与早期版本不同，务必留意）：

    - **既有行一律保留**：按 ``event_id`` 判定，既有记录不参与任何去重，
      一条不删、一个字段不改；
    - **30 日去重只在本次新批次内部生效**：新批次先剔除与既有行重复的
      ``event_id``，再在批次内做同标的同类型 30 日去重，不与既有行比较——
      因此补历史数据不会因为新出现的更早公告把既有行挤掉；
    - ``dry_run=True`` 只计算不落盘，返回将新增、将丢弃与既有行受影响数。

    ``coverage_start/end`` 是**覆盖声明**（该区间内已完整抓取），与
    ``start/end``（记录过滤）分开：声明区间之外的日期一律按 ``unknown`` 处理。
    """
    if mode not in ('replace', 'append'):
        raise ValueError(f"未知导入模式: {mode}")
    root = Path(store_root) if store_root else _default_store_root()
    file_path = Path(path)
    raw = _read_source(file_path)
    kind = _detect_format(raw.columns)
    if kind == 'raw':
        incoming = normalize_announcements(raw, start, end, source=source)
    else:
        if 'source' not in raw.columns:
            if not source:
                raise ValueError(
                    '已规范化事件表缺少 source 列且调用方未传 source；'
                    '禁止兜底成 cninfo，请显式传入 cninfo / em_ann / em_yjyg')
            raw = raw.with_columns(pl.lit(source).alias('source'))
        incoming = dedup_events(_coerce_events(raw))
    if incoming.is_empty():
        raise ValueError(f'事件文件没有可用记录: {file_path}')

    stats: dict[str, Any] = {}
    source_rows = raw.height
    if mode == 'append':
        existing = load_events(root)
        if not existing.is_empty():
            duplicate_id_rows = existing.height - existing['event_id'].n_unique()
            if duplicate_id_rows > 0:
                logger.warning(
                    f'[events] 现有事件表存在 {duplicate_id_rows} 行重复 event_id；'
                    'append 会按 event_id 保留首条，重复行将被丢弃——'
                    '若这些行本应是不同事件，说明上游 ID 生成缺少区分维度'
                )
            existing_ids = set(existing['event_id'].to_list())
            batch = incoming.filter(~pl.col('event_id').is_in(list(existing_ids)))
        else:
            batch = incoming
        dropped_existing_conflict = incoming.height - batch.height
        batch_after_conflict = batch.height
        batch = _drop_near_existing(batch, existing)
        dropped_near_existing = batch_after_conflict - batch.height
        batch = batch.unique(subset=['event_id'], keep='first',
                             maintain_order=True)
        batch = dedup_events(batch)
        stats = {
            'existing_rows': existing.height,
            'source_rows': source_rows,
            'batch_rows': incoming.height,
            'added': batch.height,
            # 与既有行重复而跳过（既有行保留，新行不覆盖）
            'dropped_existing_conflict': dropped_existing_conflict,
            # 与既有行同标的同类型且相距 ≤30 天而跳过
            'dropped_near_existing': dropped_near_existing,
            # 批次内丢弃：标题/关键词过滤 + event_id 重复 + 30 日去重
            'dropped_in_batch': source_rows - dropped_existing_conflict
            - dropped_near_existing - batch.height,
            'existing_affected': 0,
        }
        if batch.height:
            merged = pl.concat([existing, batch], how='vertical_relaxed')
        else:
            merged = existing
        if not existing.is_empty():
            lost = set(existing['event_id'].to_list()) - set(merged['event_id'].to_list())
            if lost:
                raise RuntimeError(
                    f'append 破坏了既有行（{len(lost)} 条 event_id 丢失），已中止写入')
    else:
        # replace 是整表替换：存量行不会被保留，但必须逐类记账——
        # 「保留未改 + 保留改值 + 被替换掉 = 存量行数」由守恒断言强制（DG-31）。
        existing = load_events(root)
        merged = incoming
        conservation = _assert_replace_conservation(existing, incoming, merged)
        if conservation['existing_replaced_out']:
            logger.warning(
                f'[events] replace 将丢弃 {conservation["existing_replaced_out"]} 条'
                f'存量行（存量 {existing.height} 行 → 写盘 {merged.height} 行）')
        if conservation['existing_modified']:
            logger.warning(
                f'[events] replace 改写了 {conservation["existing_modified"]} 条'
                '既有行的内容')
        stats = {'existing_rows': existing.height, 'source_rows': source_rows,
                 'batch_rows': incoming.height, 'added': incoming.height,
                 'dropped_existing_conflict': 0,
                 'dropped_near_existing': 0,
                 'dropped_in_batch': source_rows - incoming.height,
                 'existing_affected': conservation['existing_modified'],
                 'existing_retained': conservation['existing_retained'],
                 'existing_replaced_out': conservation['existing_replaced_out']}

    path_out = events_path(root)
    if dry_run:
        logger.info(f'[events] dry-run：{mode} 将写入 {merged.height} 条（未落盘）→ {path_out}')
        return {'rows': merged.height, 'mode': mode, 'coverage': None,
                'source_format': kind, 'dry_run': True, **stats}

    path_out.parent.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(merged, path_out)
    coverage = _write_coverage(root, merged, complete,
                               coverage_start, coverage_end)
    logger.info(f'[events] {mode} 导入 {merged.height} 条 → {path_out}')
    return {'rows': merged.height, 'mode': mode, 'coverage': coverage,
            'source_format': kind, 'dry_run': False, **stats}


def _default_store_root() -> Path:
    from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
    from src.utils.config import get_config  # noqa: PLC0415

    return resolve_store_root(get_config())


def cooldown_range(publish_time: Any, trading_dates: Sequence[date],
                   days: int) -> tuple[date, date] | None:
    """返回冷却区间 ``[T+1, T+N]``（按交易日计）；日历不足时返回 ``None``。"""
    published = publish_time.date() if isinstance(publish_time, datetime) \
        else _as_date(publish_time)
    if published is None or days <= 0:
        return None
    index = bisect_right(list(trading_dates), published)
    if index >= len(trading_dates):
        return None
    last = min(len(trading_dates) - 1, index + days - 1)
    return trading_dates[index], trading_dates[last]


class EventStore:
    """本地事件表 + 冷却窗口查询（初始化后只读，运行时不联网）。"""

    def __init__(self, store_root: Path | str | None = None,
                 cooldowns: dict[str, int] | None = None) -> None:
        self.store_root = Path(store_root) if store_root else _default_store_root()
        self.cooldowns = dict(cooldowns or RISK_EVENT_COOLDOWNS)
        self.events = load_events(self.store_root)
        self.coverage = load_coverage(self.store_root) or self._derive_coverage()
        self.available = not self.events.is_empty()
        self.risk_events = self.events.filter(
            pl.col('event_type').is_in(list(self.cooldowns)))

    def _derive_coverage(self) -> dict[str, Any]:
        if self.events.is_empty():
            return {}
        return {
            'source': 'derived',
            'start': str(self.events['publish_time'].min())[:10],
            'end': str(self.events['publish_time'].max())[:10],
            'rows': self.events.height,
            'complete': False,
        }

    def trading_dates(self, start: Any = None, end: Any = None) -> tuple[date, ...]:
        """从统一交易日历取 ``[start, end]`` 的交易日，供 ``blocked_map`` 等使用。

        事件模块不自己拼日历：口径与 freshness / clean_set / tradable_universe
        一致（优先本地基准指数真实交易日）。``start`` 缺省取日历首日，
        ``end`` 缺省取今天。
        """
        from src.data.trading_calendar import get_calendar  # noqa: PLC0415

        calendar = get_calendar(self.store_root)
        lo = _as_date(start) or (calendar.dates[0] if calendar.dates else date.today())
        hi = _as_date(end) or date.today()
        return calendar.between(lo, hi)

    @property
    def covered_range(self) -> tuple[date | None, date | None]:
        start = _as_date(self.coverage.get('start'))
        end = _as_date(self.coverage.get('end'))
        return start, end

    def covers(self, as_of: Any) -> bool:
        day = _as_date(as_of)
        start, end = self.covered_range
        if not self.available or day is None or start is None or end is None:
            return False
        return start <= day <= end

    def blocked_map(self, trading_dates: Sequence[date]) -> dict[date, dict[str, dict]]:
        """预计算 ``{交易日: {ts_code: 冷却事件详情}}``，供逐日 O(1) 查询。"""
        blocked: dict[date, dict[str, dict]] = {}
        if not self.available or not trading_dates:
            return blocked
        dates = list(trading_dates)
        for item in self.risk_events.select(
                'ts_code', 'event_type', 'publish_time', 'event_id').to_dicts():
            days = self.cooldowns.get(item['event_type'])
            if not days:
                continue
            span = cooldown_range(item['publish_time'], dates, days)
            if span is None:
                continue
            first, last = span
            for day in dates[bisect_left(dates, first):bisect_right(dates, last)]:
                blocked.setdefault(day, {})[item['ts_code']] = {
                    'event_type': item['event_type'],
                    'publish_date': str(item['publish_time'])[:10],
                    'cooldown_until': last.isoformat(),
                    'event_id': item['event_id'],
                }
        return blocked

    def status(self, code: str, as_of: Any,
               trading_dates: Sequence[date]) -> dict[str, Any]:
        """单标的冷却状态：``cooling`` / ``clear`` / ``unknown``。"""
        day = _as_date(as_of)
        base: dict[str, Any] = {
            'status': STATUS_UNKNOWN, 'event_type': '', 'publish_date': '',
            'cooldown_until': '', 'event_id': '', 'reason': '',
        }
        if day is None:
            base['reason'] = 'as_of_missing'
            return base
        if not self.covers(day):
            base['reason'] = 'no_coverage'
            return base
        target = str(code).upper()
        rows = self.risk_events.filter(pl.col('ts_code') == target)
        hits: list[dict[str, Any]] = []
        for item in rows.select('ts_code', 'event_type', 'publish_time',
                                'event_id').to_dicts():
            days = self.cooldowns.get(item['event_type'])
            span = cooldown_range(item['publish_time'], list(trading_dates), days or 0)
            if span is None:
                continue
            first, last = span
            if first <= day <= last:
                hits.append({**item, 'first': first, 'last': last})
        if not hits:
            base['status'] = STATUS_CLEAR
            return base
        hit = max(hits, key=lambda item: item['last'])
        base.update({
            'status': STATUS_COOLING,
            'event_type': hit['event_type'],
            'publish_date': str(hit['publish_time'])[:10],
            'cooldown_until': hit['last'].isoformat(),
            'event_id': hit['event_id'],
        })
        return base

    def summary(self) -> dict[str, Any]:
        counts = self.events['event_type'].value_counts().to_dicts() \
            if self.available else []
        return {
            'available': self.available,
            'rows': self.events.height,
            'coverage': self.coverage,
            'cooldowns': self.cooldowns,
            'by_type': {item['event_type']: item['count'] for item in counts},
        }


def get_event_status(code: str, as_of: Any, *, trading_dates: Sequence[date],
                     store_root: Path | str | None = None,
                     cooldowns: dict[str, int] | None = None) -> dict[str, Any]:
    """单标的冷却状态查询（``as_of`` 与交易日历必须显式传入）。

    交易日历可用 ``EventStore(store_root).trading_dates(start, end)`` 从统一
    日历模块取得；这里保持强制显式传入，避免静默使用低置信度近似日历。
    """
    if not trading_dates:
        raise ValueError('缺少交易日历，无法按交易日计算冷却窗口')
    store = EventStore(store_root, cooldowns)
    result = store.status(code, as_of, trading_dates)
    result['covered_range'] = list(store.covered_range)
    return result


def _main(args: argparse.Namespace) -> int:
    root = Path(args.store_root) if args.store_root else _default_store_root()
    if args.command in ('import', 'append'):
        result = import_file(
            args.path, root, mode='replace' if args.command == 'import' else 'append',
            source=args.source, complete=not args.partial,
            start=args.start, end=args.end,
            coverage_start=args.coverage_start,
            coverage_end=args.coverage_end)
        logger.info(f'[events] 导入完成: {result["rows"]} 条，'
                    f'覆盖 {result["coverage"].get("start")}~'
                    f'{result["coverage"].get("end")}')
        return 0
    info = EventStore(root).summary()
    logger.info(f'[events] 事件表: {json.dumps(info, ensure_ascii=False, indent=2)}')
    return 0 if info['available'] else 1


def main() -> int:
    parser = argparse.ArgumentParser(description='公告事件本地表（离线导入）')
    parser.add_argument('command', choices=['import', 'append', 'stats'])
    parser.add_argument('--path', default=None, help='CSV / Parquet 事件文件')
    parser.add_argument('--source', default='cninfo', help='数据来源标识')
    parser.add_argument('--start', default=None, help='只导入该日期之后的事件')
    parser.add_argument('--end', default=None, help='只导入该日期之前的事件')
    parser.add_argument('--coverage-start', default=None,
                        help='覆盖声明起始日（区间内已完整抓取）')
    parser.add_argument('--coverage-end', default=None,
                        help='覆盖声明结束日（区间内已完整抓取）')
    parser.add_argument('--partial', action='store_true',
                        help='标记覆盖不完整（区间内仍可能有漏抓）')
    parser.add_argument('--store-root', default=None, help='本地列存根目录')
    parsed = parser.parse_args()
    if parsed.command in ('import', 'append') and not parsed.path:
        parser.error(f'{parsed.command} 需要 --path')
    return _main(parsed)


if __name__ == '__main__':
    raise SystemExit(main())
