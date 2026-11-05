#!/usr/bin/env python3

"""股本区间事实表（总股本 / 流通股本 / 自由流通股本）。

结论先行
========
* 唯一产物：``data/local_store/meta/factors/stock_shares.parquet``，
  **只描述股本数量**；PE/PB/总市值/流通市值由财务线写 ``valuation_daily``，
  本模块既不写该表，也不改 MySQL 库表。
* 单位统一 **股**（整数，不是万股/亿股）。东财源端已是股；巨潮源端是
  **万股**（含 4 位小数），采集时 ×10000 后四舍五入到整股。
* 语义是**按 ``effective_date`` 生效的区间事实**：同一票多行 = 多次股本
  变动，不逐日展开。下游按 ``effective_date`` forward-fill 得到「某日适用
  股本」。查询入口 :func:`shares_as_of` 已按此语义实现。
* ``free_float_share``（自由流通股本）取东财 F10 股本结构同一张表的
  ``FREELIQCI_SHARES``（同生效日、单位股）：2019 年后事件行覆盖较全，
  上市前/源端未披露的行保持 NULL。填充时强制 ``free <= float``，
  东财自相矛盾（自由流通大于流通）的行跳过不填。

生效日选择（点时正确）
=====================
``effective_date`` 取**股本实际变化并生效的日期**，不用公告日：

* 送股 / 转增：取除权日（股本实际增加的那天）。已实测对齐：000001 1993
  年度 10 送 5（``share_div=0.5``）在 ``stock_dividend`` 的 ``ex_date`` 是
  1994-07-11，巨潮股本变动表的 ``变动日期`` 同为 1994-07-11。
* 增发 / 配股 / 可转债转股：取新股（转股股份）上市日。
* 回购注销：取注销日。
* 公告日只影响「提前知道」，不影响「当日已生效股本」。用生效日可保证下游
  不会看到未来股本（宁可晚一天，不可早一天）。
* 硬保护：``effective_date > 采集日`` 的行一律丢弃（东财偶有未来生效行）。

数据源实测（2026-11-04，akshare 1.19.3）
========================================
===========  ================================  ==========  ============  =======
源            接口                             单位        历史深度       耗时
===========  ================================  ==========  ============  =======
东财股本结构  ``stock_zh_a_gbjg_em``            股          最早 1991     0.30~0.85s
巨潮股本变动  ``stock_share_change_cninfo``     万股        最早 1991     0.48~0.86s
东财个股信息  ``stock_individual_info_em``      股          仅当前        约 0.4s
东财全市场    ``stock_zh_a_spot_em``            元(市值)    仅当前        约 84s
===========  ================================  ==========  ============  =======

* 东财 ``stock_zh_a_gbjg_em``：列 ``变更日期 / 总股本 / 已上市流通A股 /
  流通受限股份 / 变动原因``，无分页、单票一次请求、覆盖沪深京（920 段亦可用）。
  注意其 ``已流通股份`` 列恒等于总股本（口径损坏），**流通股本只能取
  ``已上市流通A股``**——该列对 A+H 公司只含 A 股，正好是换手率分母。
* 巨潮 ``stock_share_change_cninfo``：列含 ``变动日期 / 公告日期 / 总股本 /
  已流通股份 / 流通受限股份 / 变动原因``。缺点：``已流通股份`` 对 A+H 公司
  **含 H 股**（实测 688981 为 80.1 亿 vs 实际 A 股流通 20.0 亿），故只作备用源。
  另有大量 ``定期报告`` 快照行（值未变），入库前按「相邻行是否变化」折叠。
* baostock 日线无股本字段；只能用 ``turn`` 反推流通股本（见交叉校验），
  不作入库源。
* 雪球 ``stock_individual_basic_info_xq`` 需 token，匿名 400，不采用。

选源规则（默认单源，老票在**校验过的锚点**上分段拼接）
=====================================================
1. 东财序列非空且末行与全市场快照（``总市值/最新价``）偏差 ≤2% → 用东财；
2. 东财末行偏差 >2% 且巨潮更接近快照 → 用巨潮（``source`` 标注，A+H 的
   流通股本会含 H，交叉校验可检出）；
3. 1996 年前上市的老票额外拉巨潮：找到「两源同日适用值（总股本+流通股本）
   一致」的最早日期作锚点，锚点前用巨潮、锚点后用东财（详见
   :func:`stitch_legacy_series`）。找不到锚点的（如 A+B/A+H 流通口径不同）
   保持东财整条序列，不做猜测。实测依据：1995 年前东财/巨潮同日样本 19%
   偏差 >5%（最大 90%），1996-2000 年 1.8%，2001 年后 0。
4. 两源都空 → 用当前值 + ``stock_dividend`` 送转比例倒推（``source`` 为
   ``derived:share_div``；流通股本按当前流通比例等比缩放，精度边界见
   :func:`derive_share_series`）。

存储布局
========
单文件、按 ``(ts_code, effective_date)`` 升序、zstd(level=3)、
``row_group_size=100_000``；``ts_code`` 排序让读单票时 parquet 行组统计可直接
跳过无关数据块（与 ``stock_turnover`` / ``stock_adj_factor`` 同一套读法）。
采集中间件（分块 part、进度 JSON、失败清单）统一放项目 ``tmp/stock_shares/``，
不污染 ``meta/factors/``——该目录只落最终 parquet 与 ``.meta.json`` 列说明。

用法::

    python -m tools.fetch_stock_shares --limit 20      # 小批试跑
    python -m tools.fetch_stock_shares                # 全量 + 报告
"""

from __future__ import annotations

import json
import math
import os
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, NamedTuple, Sequence

import polars as pl
from loguru import logger

from src.utils.code_converter import to_symbol, to_ts_code
from src.utils.exception_handler import handle_exception_with_retry
from src.utils.exceptions import DataSourceConnectionError

SHARES_FILENAME = 'stock_shares.parquet'
SHARES_META_FILENAME = 'stock_shares.meta.json'

SHARE_COLUMNS: tuple[str, ...] = (
    'ts_code',
    'effective_date',
    'total_share',
    'float_share',
    'free_float_share',
    'change_reason',
    'source',
    'fetched_at',
)

SHARE_SCHEMA: dict[str, pl.DataType] = {
    'ts_code': pl.String(),
    'effective_date': pl.Date(),
    'total_share': pl.Int64(),
    'float_share': pl.Int64(),
    'free_float_share': pl.Int64(),
    'change_reason': pl.String(),
    'source': pl.String(),
    'fetched_at': pl.Datetime('us'),
}

SOURCE_EM = 'eastmoney:stock_zh_a_gbjg_em'
SOURCE_CNINFO = 'cninfo:stock_share_change_cninfo'
SOURCE_DERIVED = 'derived:share_div'

#: 末行股本与全市场快照的允许相对偏差；超过即认为该源序列未跟上最新股本。
SNAPSHOT_TOLERANCE = 0.02
#: 该日期前上市的老票额外接巨潮（官方申报口径）历史段；详见 ``stitch_legacy_series``。
LEGACY_LIST_DATE_CUTOFF = date(1996, 1, 1)
#: 巨潮接口内部用 py_mini_racer（V8），多线程并发实例化会让 V8 初始化
#: partition address space 竞争并直接 abort（实测 FATAL:
#: partition_address_space.cc(243) Check failed），故串行化该接口。
_CNINFO_LOCK = threading.Lock()
#: 巨潮源端单位是万股（1 万股 = 10000 股）。
_CNINFO_SCALE = 10000.0
_LRU_MAX = 128

#: 缓存键为 ``<path>|<ts_code>``，同时支持多根目录（测试会写到 tmp_path）。
_SHARES_CACHE: OrderedDict[str, pl.DataFrame | None] = OrderedDict()
_SHARES_LAZY: dict[str, tuple[int, pl.LazyFrame]] = {}


class ShareSnapshot(NamedTuple):
    """某只票在某个日期适用的股本（单位：股）。"""

    ts_code: str
    effective_date: date
    total_share: int
    float_share: int | None
    free_float_share: int | None


# ----------------------------------------------------------------------
# 路径与读取
# ----------------------------------------------------------------------
def default_store_root() -> Path:
    """默认列存根目录（项目根/data/local_store）。"""
    return Path(__file__).resolve().parents[3] / 'data' / 'local_store'


def default_work_dir() -> Path:
    """采集中间件目录（项目根/tmp/stock_shares）。"""
    return Path(__file__).resolve().parents[3] / 'tmp' / 'stock_shares'


def shares_path(root: Path | str | None = None) -> Path:
    """股本事实表路径：``<root>/meta/factors/stock_shares.parquet``。"""
    return _root(root) / 'meta' / 'factors' / SHARES_FILENAME


def read_shares(ts_code: str | None = None, root: Path | str | None = None,
                columns: Iterable[str] | None = None) -> pl.DataFrame | None:
    """读取股本事实表；给定 ``ts_code`` 时只返回该票的区间序列并升序排序。

    Returns:
        pl.DataFrame | None: 文件不存在或无该票记录时返回 None。
    """
    path = shares_path(root)
    if not path.exists():
        return None
    try:
        mtime = path.stat().st_mtime_ns
    except OSError:
        return None

    selected = list(columns) if columns else list(SHARE_COLUMNS)

    if ts_code is None:
        return pl.read_parquet(path, columns=selected)

    path_key = str(path)
    state = _SHARES_LAZY.get(path_key)
    if state is None or state[0] != mtime:
        prefix = f'{path_key}|'
        for key in [item for item in _SHARES_CACHE if item.startswith(prefix)]:
            _SHARES_CACHE.pop(key, None)
        state = (mtime, pl.scan_parquet(path_key))
        _SHARES_LAZY[path_key] = state
    lazy = state[1]

    cache_key = f'{path_key}|{ts_code}'
    cached = _SHARES_CACHE.get(cache_key)
    if cached is not None or cache_key in _SHARES_CACHE:
        _SHARES_CACHE.move_to_end(cache_key)
        return cached

    frame = (lazy.filter(pl.col('ts_code') == ts_code)
             .select(selected).collect().sort('effective_date'))
    result: pl.DataFrame | None = None if frame.is_empty() else frame
    _SHARES_CACHE[cache_key] = result
    while len(_SHARES_CACHE) > _LRU_MAX:
        _SHARES_CACHE.popitem(last=False)
    return result


def shares_frame(ts_code: str, root: Path | str | None = None) -> pl.DataFrame | None:
    """单票股本区间序列（``effective_date`` 升序）；无记录返回 None。"""
    code = _normalize_code(ts_code)
    return read_shares(code, root)


def shares_as_of(ts_code: str, as_of: str | date | datetime,
                 root: Path | str | None = None) -> ShareSnapshot | None:
    """返回 ``as_of`` 当日**已生效**的股本（单位：股）。

    语义：取 ``effective_date <= as_of`` 的最后一行，即生效日当天即可见新值，
    前一天仍返回旧值。无记录或早于首个生效日返回 None（不向前臆造）。
    """
    day = _as_date(as_of)
    if day is None:
        return None
    code = _normalize_code(ts_code)
    frame = read_shares(code, root)
    if frame is None or frame.is_empty():
        return None
    visible = frame.filter(pl.col('effective_date') <= day)
    if visible.is_empty():
        return None
    row = visible.tail(1).to_dicts()[0]
    return ShareSnapshot(
        ts_code=code,
        effective_date=row['effective_date'],
        total_share=int(row['total_share']),
        float_share=_opt_int(row.get('float_share')),
        free_float_share=_opt_int(row.get('free_float_share')),
    )


def write_shares(frame: pl.DataFrame, root: Path | str | None = None, *,
                 write_meta: bool = True) -> Path:
    """原子写入事实表（排序 + zstd + 行组统计），可选同步写列说明。"""
    path = shares_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    prepared = _normalize_frame(frame).sort(['ts_code', 'effective_date'])
    tmp = path.with_suffix('.parquet.tmp')
    prepared.write_parquet(
        tmp, compression='zstd', compression_level=3,
        statistics=True, row_group_size=100_000)
    os.replace(tmp, path)
    if write_meta:
        _write_meta(path, prepared)
    return path


# ----------------------------------------------------------------------
# 单位、日期与序列清洗
# ----------------------------------------------------------------------
def _root(root: Path | str | None) -> Path:
    return Path(root) if root else default_store_root()


def _normalize_code(ts_code: str) -> str:
    """任意代码格式 → 标准 ts_code；无法识别时原样返回（查询走空结果）。"""
    try:
        return to_ts_code(str(ts_code))
    except (ValueError, TypeError):
        return str(ts_code)


def _opt_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return int(number)


def _as_date(value: Any) -> date | None:
    """宽松解析日期：date / datetime / pandas Timestamp / 'YYYY-MM-DD'。"""
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()[:10]
    if not text or text.lower() in {'nan', 'nat', 'none', '-'}:
        return None
    try:
        return datetime.strptime(text.replace('/', '-'), '%Y-%m-%d').date()
    except ValueError:
        return None


def _to_shares(value: Any, scale: float = 1.0) -> int | None:
    """源端数值 → 整股；``scale`` 用于万股→股。非正值/NaN 归为 None。"""
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    return int(round(number * scale))


def _frame_from_rows(rows: Sequence[dict[str, Any]]) -> pl.DataFrame:
    """按固定 schema 构造股本帧（空列表也返回带 schema 的空帧）。"""
    if not rows:
        return pl.DataFrame(schema=SHARE_SCHEMA)
    return pl.DataFrame(rows, schema=SHARE_SCHEMA)


def _normalize_frame(frame: pl.DataFrame) -> pl.DataFrame:
    """补齐缺失列、统一 dtype，保证写出文件列顺序稳定。"""
    out = frame
    for name, dtype in SHARE_SCHEMA.items():
        if name not in out.columns:
            out = out.with_columns(pl.lit(None, dtype=dtype).alias(name))
        elif out.schema[name] != dtype:
            out = out.with_columns(pl.col(name).cast(dtype, strict=False))
    return out.select(list(SHARE_COLUMNS))


def _collapse_steps(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """相邻行股本完全相同时只保留最早一行（折叠定期报告快照）。"""
    collapsed: list[dict[str, Any]] = []
    for row in rows:
        if collapsed:
            last = collapsed[-1]
            if (last['total_share'] == row['total_share']
                    and last['float_share'] == row['float_share']):
                continue
        collapsed.append(row)
    return collapsed


def normalize_rows(raw_rows: Sequence[dict[str, Any]], *, ts_code: str,
                   date_key: str, total_key: str, float_key: str,
                   scale: float, source: str, fetched_at: datetime,
                   today: date, reason_key: str | None = None) -> pl.DataFrame:
    """把单源原始行标准化为事实表行（整股、去未来行、同值折叠）。

    Args:
        raw_rows: 源端逐行字典（pandas ``to_dict('records')`` 或 polars ``to_dicts``）。
        date_key/total_key/float_key: 源端中文字段名。
        scale: 源端→股 的比例（万股→股为 10000）。
        today: 采集日；``effective_date > today`` 的行被丢弃（防止未来股本）。
    """
    by_date: dict[date, dict[str, Any]] = {}
    for raw in raw_rows:
        effective = _as_date(raw.get(date_key))
        total = _to_shares(raw.get(total_key), scale)
        if effective is None or total is None or effective > today:
            continue
        float_share = _to_shares(raw.get(float_key), scale)
        previous = by_date.get(effective)
        if previous is not None and previous['total_share'] >= total:
            continue
        reason = None
        if reason_key is not None:
            reason = _text(raw.get(reason_key))
        by_date[effective] = {
            'ts_code': ts_code,
            'effective_date': effective,
            'total_share': total,
            'float_share': float_share,
            'free_float_share': None,
            'change_reason': reason,
            'source': source,
            'fetched_at': fetched_at,
        }

    rows = _collapse_steps([by_date[key] for key in sorted(by_date)])
    return _frame_from_rows(rows)


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return None if not text or text.lower() in {'nan', 'none', '-'} else text


# ----------------------------------------------------------------------
# 选源（纯函数，可单测）
# ----------------------------------------------------------------------
def snapshot_error(series: pl.DataFrame,
                   snapshot: tuple[int | None, int | None] | None) -> float | None:
    """序列末行与当前快照（总股本, 流通股本）的最大相对偏差；无法比较返回 None。"""
    if snapshot is None or series.is_empty():
        return None
    last = series.tail(1).to_dicts()[0]
    snap_total, snap_float = snapshot
    errors: list[float] = []
    if snap_total and last['total_share']:
        errors.append(abs(last['total_share'] - snap_total) / snap_total)
    if snap_float and last['float_share']:
        errors.append(abs(last['float_share'] - snap_float) / snap_float)
    return max(errors) if errors else None


def _close(left: int | None, right: int | None, tolerance: float) -> bool:
    """两个股本值是否在容差内一致；任一缺失视为不可比（不拼接）。"""
    if not left or not right:
        return False
    return abs(left - right) / right <= tolerance


def stitch_legacy_series(em: pl.DataFrame, cninfo: pl.DataFrame,
                         *, tolerance: float = SNAPSHOT_TOLERANCE
                         ) -> tuple[pl.DataFrame, bool]:
    """用巨潮（官方申报口径）替换东财 ``anchor`` 之前的历史段。

    实测东财早期（1995 年前）股本量与巨潮官方口径分歧明显：抽 40 只老票的 399
    个同日样本里，1995 年前 19% 的样本偏差 >5%（最大 90%，如 600618 1992 年
    差 10 倍），1996-2000 年降到 1.8%，2001 年后为 0。故对 1996 年前上市的老票
    接巨潮前半段。

    拼接条件：``anchor`` = 巨潮中最早的、与东财**同日适用值**（总股本与流通股本
    同时）一致的行日期。两源在 ``anchor`` 数值相同，拼接后不会跳变；找不到
    ``anchor``（如 A+B/A+H 标的流通口径不同）则原样返回东财，不做猜测。

    Returns:
        ``(序列, 是否拼接)``；拼接后前缀行 ``source`` 为巨潮，后缀为东财。
    """
    if em.is_empty() or cninfo.is_empty():
        return em, False
    em_rows = em.sort('effective_date').to_dicts()
    cn_rows = cninfo.sort('effective_date').to_dicts()

    anchor: date | None = None
    em_index = 0
    latest_em: dict[str, Any] | None = None
    for row_c in cn_rows:
        while (em_index < len(em_rows)
               and em_rows[em_index]['effective_date'] <= row_c['effective_date']):
            latest_em = em_rows[em_index]
            em_index += 1
        if latest_em is None:
            continue
        if (_close(latest_em['total_share'], row_c['total_share'], tolerance)
                and _close(latest_em['float_share'], row_c['float_share'], tolerance)):
            anchor = row_c['effective_date']
            break
    if anchor is None:
        return em, False

    prefix = [row for row in cn_rows if row['effective_date'] < anchor]
    suffix = [row for row in em_rows if row['effective_date'] >= anchor]
    return _frame_from_rows(_collapse_steps(prefix + suffix)), True


def choose_share_series(em: pl.DataFrame, cninfo: pl.DataFrame,
                        snapshot: tuple[int | None, int | None] | None = None,
                        *, tolerance: float = SNAPSHOT_TOLERANCE) -> pl.DataFrame:
    """按「整条序列单源」原则选源。

    * 东财为空 → 用巨潮；巨潮为空 → 用东财；
    * 两源都有：东财末行与快照偏差 ≤ ``tolerance`` 或无法比较 → 用东财；
    * 东财偏差过大且巨潮更接近快照 → 用巨潮（典型场景：东财缺最近一次送转）。
    """
    if em.is_empty():
        return cninfo
    if cninfo.is_empty():
        return em
    em_error = snapshot_error(em, snapshot)
    cn_error = snapshot_error(cninfo, snapshot)
    if em_error is None or cn_error is None:
        return em
    if em_error > tolerance and cn_error < em_error:
        return cninfo
    return em


# ----------------------------------------------------------------------
# 倒推（bootstrapping）
# ----------------------------------------------------------------------
def derive_share_series(ts_code: str, *, list_date: date,
                        current_total: int, current_float: int | None,
                        dividend_events: Sequence[tuple[date, float]],
                        fetched_at: datetime, today: date,
                        source: str = SOURCE_DERIVED) -> pl.DataFrame:
    """由「当前股本 + 送转比例」倒推历史区间。

    ``dividend_events`` 为 ``(ex_date, share_div)``，``share_div`` 是每股送转股数
    （10 送 3 → 0.3），比例 ``1+share_div`` 在 ``ex_date`` 生效；用生效日而非
    公告日，与主源一致。

    精度边界：
    * 只覆盖送股/转增。增发、配股、回购注销不在 ``stock_dividend`` 里，
      越往前误差越大（缺失一次增发会让更早的股本被低估）；
    * 流通股本按「当前流通比例」等比缩放，无法反映限售解禁节奏；
    * 仅在两源都拿不到数据时使用，``source`` 标注可让下游识别。
    """
    events = sorted(
        (day, ratio) for day, ratio in dividend_events
        if day <= today and ratio > 0 and day >= list_date
    )
    if current_total <= 0:
        return _frame_from_rows([])

    suffix = [1.0] * (len(events) + 1)
    for index in range(len(events) - 1, -1, -1):
        suffix[index] = suffix[index + 1] * (1.0 + events[index][1])

    ratio_now = (current_float / current_total) if current_float else None

    def _row(effective: date, total: int) -> dict[str, Any]:
        float_share = int(round(total * ratio_now)) if ratio_now else None
        return {
            'ts_code': ts_code,
            'effective_date': effective,
            'total_share': total,
            'float_share': float_share,
            'free_float_share': None,
            'change_reason': '倒推（送转链）',
            'source': source,
            'fetched_at': fetched_at,
        }

    rows: list[dict[str, Any]] = []
    earliest = int(round(current_total / suffix[0]))
    if earliest > 0:
        rows.append(_row(list_date, earliest))
    for index, (ex_date, _ratio) in enumerate(events):
        post = int(round(current_total / suffix[index + 1])) if suffix[index + 1] > 0 \
            else current_total
        if rows and rows[-1]['total_share'] == post:
            continue
        rows.append(_row(ex_date, post))
    if not rows or rows[-1]['total_share'] != current_total:
        rows.append(_row(today, current_total))
    return _frame_from_rows(_collapse_steps(rows))


# ----------------------------------------------------------------------
# 远程抓取（单票；带项目统一重试装饰器）
# ----------------------------------------------------------------------
def _sleep(throttle: float) -> None:
    if throttle > 0:
        time.sleep(throttle)


@handle_exception_with_retry(max_retries=3, retry_delay=1.0,
                             backoff_factor=2.0, max_delay=8.0)
def fetch_em_history(ts_code: str, *, fetched_at: datetime, today: date,
                     throttle: float = 0.15) -> pl.DataFrame:
    """东财股本结构（股）：单票一次请求，失败抛可重试异常。"""
    import akshare as ak  # noqa: PLC0415 - 延迟导入，离线/测试不加载远程库

    try:
        raw = ak.stock_zh_a_gbjg_em(symbol=ts_code)
    except Exception as exc:  # noqa: BLE001 - akshare 抛错类型不稳定
        raise DataSourceConnectionError(
            source='eastmoney', message=f'{ts_code} 股本结构: {exc}', cause=exc
        ) from exc
    finally:
        _sleep(throttle)

    if raw is None or len(raw) == 0:
        return _frame_from_rows([])
    frame = pl.from_pandas(raw, include_index=False)
    if '变更日期' not in frame.columns or '总股本' not in frame.columns:
        logger.warning(f'[shares] {ts_code} 东财股本结构列结构异常: {frame.columns}')
        return _frame_from_rows([])
    return normalize_rows(
        frame.to_dicts(), ts_code=ts_code, date_key='变更日期',
        total_key='总股本', float_key='已上市流通A股', reason_key='变动原因',
        scale=1.0, source=SOURCE_EM, fetched_at=fetched_at, today=today)


@handle_exception_with_retry(max_retries=3, retry_delay=1.0,
                             backoff_factor=2.0, max_delay=8.0)
def fetch_cninfo_history(ts_code: str, *, fetched_at: datetime, today: date,
                         throttle: float = 0.15, start_date: str = '19900101') -> pl.DataFrame:
    """巨潮股本变动（万股→股）：单票一次请求，失败抛可重试异常。"""
    import akshare as ak  # noqa: PLC0415

    symbol = to_symbol(ts_code)
    with _CNINFO_LOCK:
        try:
            raw = ak.stock_share_change_cninfo(
                symbol=symbol, start_date=start_date, end_date=today.strftime('%Y%m%d'))
        except KeyError as exc:
            # 极早期记录缺列（实测 '公告日期'），重试无意义，按无数据处理。
            logger.debug(f'[shares] {ts_code} 巨潮返回缺列 {exc}，按无数据处理')
            return _frame_from_rows([])
        except Exception as exc:  # noqa: BLE001 - 部分北交所/退市票返回缺列报 KeyError
            raise DataSourceConnectionError(
                source='cninfo', message=f'{ts_code} 股本变动: {exc}', cause=exc
            ) from exc
        finally:
            _sleep(throttle)

    if raw is None or len(raw) == 0:
        return _frame_from_rows([])
    frame = pl.from_pandas(raw, include_index=False)
    if '变动日期' not in frame.columns or '总股本' not in frame.columns:
        return _frame_from_rows([])
    return normalize_rows(
        frame.to_dicts(), ts_code=ts_code, date_key='变动日期',
        total_key='总股本', float_key='已流通股份', reason_key='变动原因',
        scale=_CNINFO_SCALE, source=SOURCE_CNINFO, fetched_at=fetched_at,
        today=today)


@handle_exception_with_retry(max_retries=3, retry_delay=1.0,
                             backoff_factor=2.0, max_delay=8.0)
def fetch_current_info(ts_code: str, *, throttle: float = 0.15) -> tuple[int, int | None] | None:
    """东财个股信息快照（股）：仅当前值，用于缺口兜底与抽查比对。"""
    import akshare as ak  # noqa: PLC0415

    try:
        raw = ak.stock_individual_info_em(symbol=to_symbol(ts_code))
    except Exception as exc:  # noqa: BLE001
        raise DataSourceConnectionError(
            source='eastmoney', message=f'{ts_code} 个股信息: {exc}', cause=exc
        ) from exc
    finally:
        _sleep(throttle)

    if raw is None or len(raw) == 0:
        return None
    mapping = dict(zip(raw['item'].tolist(), raw['value'].tolist(), strict=False))
    total = _to_shares(mapping.get('总股本'))
    float_share = _to_shares(mapping.get('流通股'))
    return (total, float_share) if total else None


def fetch_market_snapshot(*, cache_dir: Path | str | None = None,
                          refresh: bool = False, throttle: float = 0.0
                          ) -> dict[str, tuple[int, int | None]]:
    """全市场当前股本快照（``总市值/最新价``、``流通市值/最新价``）。

    一次请求覆盖全市场（实测 59 页、约 84s），仅用于选源校验与缺口兜底，
    不作为入库事实；同日结果缓存到 ``<cache_dir>/spot_YYYYMMDD.parquet``。
    """
    import akshare as ak  # noqa: PLC0415

    cache_path = Path(cache_dir or default_work_dir()) / f'spot_{date.today():%Y%m%d}.parquet'
    if cache_path.exists() and not refresh:
        frame = pl.read_parquet(cache_path)
    else:
        raw = ak.stock_zh_a_spot_em()
        _sleep(throttle)
        if raw is None or len(raw) == 0:
            return {}
        frame = pl.from_pandas(raw, include_index=False).select(
            ['代码', '最新价', '总市值', '流通市值'])
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(cache_path)

    result: dict[str, tuple[int, int | None]] = {}
    for row in frame.to_dicts():
        price = _float(row.get('最新价'))
        total_mv = _float(row.get('总市值'))
        float_mv = _float(row.get('流通市值'))
        if not price or price <= 0 or not total_mv:
            continue
        total = int(round(total_mv / price))
        float_share = int(round(float_mv / price)) if float_mv else None
        if total <= 0:
            continue
        try:
            code = to_ts_code(str(row.get('代码')).zfill(6))
        except (ValueError, TypeError):
            continue
        result[code] = (total, float_share)
    return result


def _float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


# ----------------------------------------------------------------------
# 单票采集（选源 + 缺口倒推）
# ----------------------------------------------------------------------
def collect_one(ts_code: str, *, fetched_at: datetime, today: date,
                snapshot: tuple[int | None, int | None] | None = None,
                dividend_events: Sequence[tuple[date, float]] = (),
                list_date: date | None = None, throttle: float = 0.15,
                allow_derive: bool = True) -> tuple[pl.DataFrame, str]:
    """采集单票股本区间；返回 ``(帧, 状态)``，状态 ∈ ok/derived/empty。

    主源东财；末行与快照偏差过大时改试巨潮；1996 年前上市的老票再接巨潮历史段
    （早期东财与官方口径分歧大）；两源皆空则当前值 + 送转倒推。
    """
    em = fetch_em_history(ts_code, fetched_at=fetched_at, today=today,
                          throttle=throttle)
    need_cninfo = em.is_empty()
    if not em.is_empty() and snapshot is not None:
        error = snapshot_error(em, snapshot)
        need_cninfo = error is not None and error > SNAPSHOT_TOLERANCE
    legacy = list_date is not None and list_date < LEGACY_LIST_DATE_CUTOFF
    need_cninfo = need_cninfo or legacy

    cninfo = _frame_from_rows([])
    if need_cninfo:
        try:
            cninfo = fetch_cninfo_history(ts_code, fetched_at=fetched_at, today=today,
                                          throttle=throttle)
        except Exception as exc:  # noqa: BLE001 - 备用源缺数据不应拖垮主源
            logger.warning(f'[shares] {ts_code} 巨潮不可用，退回东财整条序列: {exc}')
    if legacy and not em.is_empty() and not cninfo.is_empty():
        stitched, used_cninfo = stitch_legacy_series(em, cninfo)
        if used_cninfo:
            return stitched, 'ok'
    series = choose_share_series(em, cninfo, snapshot)
    if not series.is_empty():
        return series, 'ok'

    if not allow_derive:
        return _frame_from_rows([]), 'empty'
    current = snapshot or fetch_current_info(ts_code, throttle=throttle)
    if not current or not current[0]:
        return _frame_from_rows([]), 'empty'
    start = list_date or today
    derived = derive_share_series(
        ts_code, list_date=start, current_total=current[0],
        current_float=current[1], dividend_events=dividend_events,
        fetched_at=fetched_at, today=today)
    return derived, 'derived' if not derived.is_empty() else 'empty'


# ----------------------------------------------------------------------
# 批量采集（分块 + 断点续跑）
# ----------------------------------------------------------------------
def load_dividend_events(root: Path | str | None = None,
                         ) -> dict[str, list[tuple[date, float]]]:
    """读 ``factors/stock_dividend.parquet`` 的送转比例（只读，不改该表）。"""
    path = _root(root) / 'factors' / 'stock_dividend.parquet'
    if not path.exists():
        return {}
    frame = pl.read_parquet(path, columns=['ts_code', 'ex_date', 'share_div'])
    frame = frame.filter(pl.col('share_div').is_not_null() & (pl.col('share_div') > 0))
    events: dict[str, list[tuple[date, float]]] = {}
    for row in frame.to_dicts():
        day = _as_date(row['ex_date'])
        if day is None:
            continue
        events.setdefault(row['ts_code'], []).append((day, float(row['share_div'])))
    for items in events.values():
        items.sort()
    return events


def _write_part(path: Path, frame: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.parquet.tmp')
    _normalize_frame(frame).write_parquet(tmp, compression='zstd')
    os.replace(tmp, path)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.json.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding='utf-8')
    os.replace(tmp, path)


def _write_failed_csv(path: Path, failed: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(
        failed,
        schema={'ts_code': pl.Utf8, 'error': pl.Utf8, 'stage': pl.Utf8},
        orient='row',
    ) if failed else pl.DataFrame(schema={'ts_code': pl.Utf8, 'error': pl.Utf8,
                                          'stage': pl.Utf8})
    frame.write_csv(path)


def collect_shares(universe: Sequence[str], *, root: Path | str | None = None,
                   work_dir: Path | str | None = None,
                   snapshot: dict[str, tuple[int, int | None]] | None = None,
                   dividend_events: dict[str, list[tuple[date, float]]] | None = None,
                   list_dates: dict[str, date] | None = None,
                   workers: int = 4, chunk_size: int = 200, throttle: float = 0.15,
                   limit: int | None = None, refresh: bool = False,
                   allow_derive: bool = True) -> dict[str, Any]:
    """分块采集全量股本并落 part 文件；中断后重跑只补未完成块。

    Returns:
        统计字典：``target/ok/derived/empty/failed/rows/elapsed_s/failed_list``。
    """
    work = Path(work_dir) if work_dir else default_work_dir()
    parts_dir = work / 'parts'
    progress_path = work / 'progress.json'
    failed_path = work / 'failed.csv'
    parts_dir.mkdir(parents=True, exist_ok=True)

    codes = [_normalize_code(code) for code in universe]
    if limit is not None:
        codes = codes[:limit]
    if snapshot is None:
        snapshot = fetch_market_snapshot(cache_dir=work)
    if dividend_events is None:
        dividend_events = load_dividend_events(root)

    signature = {'chunk_size': chunk_size, 'limit': limit, 'universe': len(codes)}
    progress = {} if refresh else _load_json(progress_path)
    if progress and progress.get('signature') != signature:
        logger.warning('[shares] 进度参数与本次不一致（块大小/标的数变了），忽略旧进度')
        progress = {}
    chunk_status: dict[str, str] = dict(progress.get('chunk_status', {}))
    failed: dict[str, dict[str, Any]] = {
        item['ts_code']: item for item in progress.get('failed', [])
        if isinstance(item, dict) and item.get('ts_code')
    }

    started = time.perf_counter()
    total_chunks = (len(codes) + chunk_size - 1) // chunk_size if codes else 0
    stats = {'ok': 0, 'derived': 0, 'empty': 0, 'rows': 0}

    if refresh:
        chunk_status.clear()
        failed.clear()

    for index in range(total_chunks):
        key = f'{index:05d}'
        part_path = parts_dir / f'part_{key}.parquet'
        if not refresh and chunk_status.get(key) == 'ok' and part_path.exists():
            continue
        chunk = codes[index * chunk_size:(index + 1) * chunk_size]
        frames: list[pl.DataFrame] = []
        chunk_failed: list[dict[str, Any]] = []
        today = date.today()
        fetched_at = datetime.now()
        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            futures = {
                pool.submit(_collect_task, code, today, fetched_at, snapshot,
                            list_date=(list_dates or {}).get(code),
                            throttle=throttle, allow_derive=allow_derive,
                            dividend_events=dividend_events): code
                for code in chunk
            }
            for future in as_completed(futures):
                code = futures[future]
                try:
                    frame, status_name = future.result()
                except Exception as exc:  # noqa: BLE001 - 单票失败不拖垮整批
                    chunk_failed.append({'ts_code': code,
                                         'error': f'{type(exc).__name__}: {exc}'[:200],
                                         'stage': 'fetch'})
                    failed[code] = chunk_failed[-1]
                    continue
                failed.pop(code, None)
                stats[status_name] = stats.get(status_name, 0) + 1
                if not frame.is_empty():
                    frames.append(frame)

        merged = pl.concat(frames, how='vertical') if frames else _frame_from_rows([])
        merged = merged.unique(subset=['ts_code', 'effective_date'], keep='last',
                               maintain_order=True)
        _write_part(part_path, merged)
        stats['rows'] += merged.height
        chunk_status[key] = 'partial' if chunk_failed else 'ok'
        _write_json(progress_path, {
            'updated_at': datetime.now().isoformat(timespec='seconds'),
            'signature': signature,
            'chunk_status': chunk_status,
            'failed': sorted(failed.values(), key=lambda item: item['ts_code']),
            'parts_dir': str(parts_dir),
        })
        _write_failed_csv(failed_path, sorted(
            failed.values(), key=lambda item: item['ts_code']))
        logger.info(
            f'[shares] 分块 {index + 1}/{total_chunks} 完成：'
            f'本块 {len(chunk)} 只，失败 {len(chunk_failed)} 只，累计行 '
            f'{stats["rows"]}')

    elapsed = time.perf_counter() - started
    return {
        'target': len(codes),
        **stats,
        'failed': len(failed),
        'failed_list': sorted(failed.values(), key=lambda item: item['ts_code']),
        'elapsed_s': round(elapsed, 2),
        'parts_dir': str(parts_dir),
        'progress_path': str(progress_path),
        'failed_path': str(failed_path),
    }


def _collect_task(ts_code: str, today: date, fetched_at: datetime,
                  snapshot: dict[str, tuple[int, int | None]] | None,
                  list_date: date | None, throttle: float, allow_derive: bool,
                  dividend_events: dict[str, list[tuple[date, float]]] | None
                  ) -> tuple[pl.DataFrame, str]:
    """线程池任务包装：把可选快照与送转事件按代码取出后调用 :func:`collect_one`。"""
    snap = snapshot.get(ts_code) if snapshot else None
    events = (dividend_events or {}).get(ts_code, ())
    return collect_one(
        ts_code, fetched_at=fetched_at, today=today, snapshot=snap,
        dividend_events=events, list_date=list_date, throttle=throttle,
        allow_derive=allow_derive)


def assemble_shares(root: Path | str | None = None,
                    work_dir: Path | str | None = None) -> dict[str, Any]:
    """把全部 part 合并为单文件事实表，并写列说明与覆盖统计。"""
    work = Path(work_dir) if work_dir else default_work_dir()
    parts_dir = work / 'parts'
    parts = sorted(parts_dir.glob('part_*.parquet'))
    if not parts:
        raise FileNotFoundError(f'没有可合并的分块文件: {parts_dir}')

    frames = [pl.read_parquet(path) for path in parts]
    frame = _normalize_frame(pl.concat(frames, how='vertical'))
    frame = (frame.unique(subset=['ts_code', 'effective_date'], keep='last')
             .sort(['ts_code', 'effective_date']))
    path = write_shares(frame, root)
    return {
        'path': str(path),
        'rows': frame.height,
        'codes': frame['ts_code'].n_unique(),
        'parts': len(parts),
    }


def _write_meta(path: Path, frame: pl.DataFrame) -> None:
    """写列说明（单位=股、点时语义、来源占比），供下游与人工核对。"""
    by_source = (frame.group_by('source').len().sort('len', descending=True)
                 .to_dicts()) if not frame.is_empty() else []
    payload = {
        'path': str(path),
        'unit': '股（整数，1 万股 = 10000 股；不是万/亿股）',
        'built_at': datetime.now().isoformat(timespec='seconds'),
        'rows': frame.height,
        'codes': frame['ts_code'].n_unique() if not frame.is_empty() else 0,
        'columns': {
            'ts_code': '标准代码，如 600000.SH',
            'effective_date': '股本实际生效日（送转=除权日，增发=新股上市日，回购=注销日）',
            'total_share': '总股本（股）',
            'float_share': '流通股本（股，A股口径；无数据为 NULL）',
            'free_float_share': '自由流通股本（股）；东财 F10 FREELIQCI_SHARES 口径，'
                                '未披露/上市前为 NULL，已强制 ≤ 流通股本',
            'change_reason': '源端变动原因原文（审计用）',
            'source': '数据源标识',
            'fetched_at': '采集时间',
        },
        'point_in_time': (
            '按 effective_date 生效的区间事实，不逐日展开；下游 forward-fill。'
            '查询请用 shares_as_of(ts_code, date)，生效日当天即可见新值，前一天只见旧值。'),
        'sources': by_source,
    }
    meta_path = path.with_name(SHARES_META_FILENAME)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = meta_path.with_suffix('.json.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding='utf-8')
    os.replace(tmp, meta_path)


def coverage_report(universe: Sequence[str], root: Path | str | None = None) -> dict[str, Any]:
    """覆盖率：覆盖多少只、缺哪些、各源占比。"""
    frame = read_shares(root=root)
    covered = set(frame['ts_code'].unique().to_list()) if frame is not None else set()
    missing = [code for code in universe if code not in covered]
    return {
        'universe': len(universe),
        'covered': len(universe) - len(missing),
        'missing': missing,
        'rows': frame.height if frame is not None else 0,
    }
