#!/usr/bin/env python3

"""换手率（turn）本地事实表：上游回补、幂等断点续跑与读取。

**单位结论（已实测，勿再换算）**：baostock 日线 ``turn`` 为**百分数**，
``0.1979`` 表示 0.1979%（不是 19.79%）。实测 2026-09-20~2026-11-03：

- 600000.SH：``volume / (turn / 100)`` 连续 26 个交易日稳定在 3.3304e10
  （波动 0.03%），即流通股本 333.0 亿股；
- 300750.SZ：同式稳定在 4.2606e9（波动 0.01%），即流通股本 42.6 亿股，
  与公开流通股本一致。

若把 ``turn`` 当小数解释，反推流通股本会小两个数量级，与公开数据不符。
故本表 ``turn`` 原样存百分数，HSL 直接使用、不再 ×100。

**存储布局**：``data/local_store/factors/stock_turnover.parquet``

- 单文件、按 ``(ts_code, trade_date)`` 升序、zstd(level=3)、
  ``row_group_size=100_000``；``ts_code`` 排序保证读单票时 parquet 行组
  统计可直接跳过无关数据块（与 ``stock_adj_factor`` 同一套读法）。
- 全市场约 2.8 千万行、约 100~200 MB，与因子快照同级；不按年/按市场分区，
  因为读侧是按票查询、写侧是一次性回补+增量，分区只会增加路径复杂度。
- 回补期间先写分块 ``factors/turnover_parts/part_XXXXX.parquet``（每 200 只
  一个文件），全部完成后流式合并成单文件；中断后重跑只补未完成分块，
  不会反复重写千万行单文件。

**不写 stock_daily / 不改 MySQL 表结构**：换手率是独立事实表，避免再走一次
Alembic 迁移；行情表保持 13 列契约不变。

**并发（实测结论：默认串行）**：baostock SDK 的 ``context.default_socket`` /
``user_id`` 是模块级全局变量（见 ``baostock/common/context.py``），同进程多线程
共享同一 socket 会互相踩报文，因此只能多进程。但实测同机 8 进程并发登录时
上游直接返回「网络接收错误」，继续加压后账号被拉黑
（``10001011 黑名单用户``），4 进程同样触发。故默认 ``workers=1``：单连接
复用 + 每票 0.15s 节流，牺牲速度换取不被封禁。``workers>1`` 仍保留，但需
自行承担被拉黑的风险。
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from loguru import logger

from src.data.sources import baostock_schema as schema
from src.utils.code_converter import to_baostock_code
from src.utils.exception_handler import handle_exception_with_retry
from src.utils.exceptions import DataSourceConnectionError

TURN_FIELDS = 'date,code,turn,tradestatus'
TURN_SCHEMA = {
    'ts_code': pl.Utf8,
    'trade_date': pl.Date,
    'turn': pl.Float64,
    'source': pl.Utf8,
    'fetched_at': pl.Datetime,
}
_PARTS_GLOB = 'part_*.parquet'
_LRU_MAX = 128
# baostock 日线最早可取日期（早于该日查询会报错）
_BAOSTOCK_MIN_DATE = date(1990, 12, 19)

_TURN_CACHE: OrderedDict[str, pl.DataFrame | None] = OrderedDict()
_TURN_LAZY: pl.LazyFrame | None = None
_TURN_MTIME: int | None = None
_TURN_PATH: str | None = None


# ----------------------------------------------------------------------
# 路径与读取
# ----------------------------------------------------------------------
def default_store_root() -> Path:
    """默认列存根目录（项目根/data/local_store）。"""
    return Path(__file__).resolve().parents[3] / 'data' / 'local_store'


def turnover_path(root: Path | str | None = None) -> Path:
    return Path(root or default_store_root()) / 'factors' / 'stock_turnover.parquet'


def read_turnover(ts_code: str, root: Path | str | None = None,
                  columns: Iterable[str] = ('trade_date', 'turn')) -> pl.DataFrame | None:
    """按代码读取换手率片段（LRU 缓存，文件重建后自动失效）。

    Returns:
        pl.DataFrame | None: 列 ``trade_date/turn``（升序）；无记录返回 None。
    """
    path = turnover_path(root)
    if not path.exists():
        return None
    try:
        mtime = path.stat().st_mtime_ns
    except OSError:
        return None

    global _TURN_LAZY, _TURN_MTIME, _TURN_PATH
    path_key = str(path)
    if _TURN_MTIME != mtime or _TURN_PATH != path_key:
        # 测试/多根目录场景下同一进程会读不同 path，缓存必须按 path 失效
        _TURN_CACHE.clear()
        _TURN_LAZY = pl.scan_parquet(path_key)
        _TURN_MTIME = mtime
        _TURN_PATH = path_key

    cached = _TURN_CACHE.get(ts_code)
    if cached is not None or ts_code in _TURN_CACHE:
        _TURN_CACHE.move_to_end(ts_code)
        return cached

    lazy = _TURN_LAZY
    if lazy is None:
        return None
    frame = lazy.filter(pl.col('ts_code') == ts_code).select(list(columns)).collect()
    result: pl.DataFrame | None = None if frame.is_empty() else frame.sort('trade_date')
    _TURN_CACHE[ts_code] = result
    while len(_TURN_CACHE) > _LRU_MAX:
        _TURN_CACHE.popitem(last=False)
    return result


def read_float_share_series(ts_code: str, root: Path | str | None = None,
                            ) -> pl.DataFrame | None:
    """读取逐日流通股本区间事实（只读 ``share_capital``，本模块不写该表）。

    Returns:
        pl.DataFrame | None: 列 ``effective_date/float_share``（升序、单位股）；
        模块缺失、文件不存在或无该票记录时返回 None。

    TODO(share_capital): 该模块接口尚未定稿；若 ``shares_frame`` 签名变化，
    只需改这一处适配，不要反向修改 ``share_capital.py``。
    """
    names = _share_module_names()
    if names is None:
        return None
    try:
        frame = names['shares_frame'](ts_code, root)
    except (OSError, ValueError, TypeError, pl.exceptions.PolarsError) as exc:
        logger.debug(f'[turnover] 读取流通股本失败({ts_code}): {exc}')
        return None
    if frame is None or frame.is_empty() or 'float_share' not in frame.columns:
        return None
    out = frame.select([
        pl.col('effective_date').cast(pl.Date),
        pl.col('float_share').cast(pl.Float64),
    ]).filter(
        pl.col('effective_date').is_not_null()
        & pl.col('float_share').is_not_null()
        & (pl.col('float_share') > 0)
    ).sort('effective_date')
    return None if out.is_empty() else out


def _share_module_names() -> dict[str, Any] | None:
    """探测股本模块接口；不存在/签名不符时返回 None（不 import 尚不存在的东西）。"""
    try:
        from src.data.local_store import share_capital
    except ImportError:
        return None
    if not hasattr(share_capital, 'shares_frame'):
        return None
    return {'shares_frame': share_capital.shares_frame}


def _normalize_date_col(lazy: pl.LazyFrame, date_col: str) -> pl.LazyFrame:
    """把日期列统一为 Date，供 join_asof 使用（Datetime/String 均兼容）。"""
    dtype = lazy.collect_schema()[date_col]
    if dtype == pl.Date:
        return lazy
    if isinstance(dtype, pl.Datetime):
        return lazy.with_columns(pl.col(date_col).dt.date().alias(date_col))
    return lazy.with_columns(
        pl.col(date_col).cast(pl.String).str.to_date('%Y-%m-%d', strict=False)
        .alias(date_col))


def attach_turn(
    frame: pl.DataFrame | pl.LazyFrame,
    ts_code: str,
    root: Path | str | None = None,
    *,
    date_col: str = 'date',
    volume_col: str = 'volume',
    keep_float_share: bool = False,
) -> pl.LazyFrame:
    """给行情帧追加换手率列 ``turn``（百分数）与来源列 ``turn_source``。

    数据来源分级（优先级从高到低，逐行决定，缺一档就落到下一档）：

    1. ``float_share`` 逐日：股本事实表有多条区间记录时，按 ``effective_date``
       向后填充到每根 K 线，``turn = volume(手) × 100 / float_share × 100``；
    2. ``float_share_static``：股本事实表只有一条记录时，用当前流通股本近似
       全部历史（精度边界：历史送转/增发期间会有偏差，但比无数据强）；
    3. ``baostock``：已落库的 ``stock_turnover`` 原始 turn（仅覆盖已回补的票）。

    Args:
        frame: 含 ``date`` 与 ``volume``（单位：手）的行情帧
        ts_code: 标准 ts_code
        root: 列存根目录；None 用项目默认目录
        keep_float_share: 为 True 时保留日级 ``float_share`` 列（周/月聚合要用）

    Returns:
        pl.LazyFrame: 追加 ``turn`` / ``turn_source``（无任何来源时两列为 null）
    """
    lazy = frame.lazy() if isinstance(frame, pl.DataFrame) else frame
    names = lazy.collect_schema().names()
    if date_col not in names or volume_col not in names:
        return lazy
    if 'turn' in names:
        return lazy  # 已带真实 turn（例如周/月聚合后的帧），不重复计算

    lazy = _normalize_date_col(lazy, date_col).sort(date_col)
    shares = read_float_share_series(ts_code, root)

    share_turn: pl.Expr | None = None
    share_source: pl.Expr | None = None
    if shares is not None:
        share_count = shares.height
        if share_count >= 2:
            # 逐日：asof 向后填充，早于首个生效日的行保持 null，后续回落到 baostock
            lazy = lazy.join_asof(
                shares.lazy(), left_on=date_col, right_on='effective_date',
                strategy='backward')
            tier = 'float_share'
        else:
            # 仅当前股本：直接取唯一值近似全部历史
            lazy = lazy.with_columns(
                pl.lit(float(shares['float_share'][0]), dtype=pl.Float64)
                .alias('float_share'))
            tier = 'float_share_static'
        valid = (pl.col('float_share').is_not_null() & (pl.col('float_share') > 0))
        # volume 单位是「手」：1 手 = 100 股，故 ×100 换股，再 ×100 得百分数
        share_turn = pl.when(valid).then(
            pl.col(volume_col) * 100.0 * 100.0 / pl.col('float_share')
        ).otherwise(None)
        share_source = pl.when(valid).then(pl.lit(tier)).otherwise(None)

    stored = read_turnover(
        _canonical_code(ts_code), root,
        columns=('trade_date', 'turn', 'source'))
    if stored is not None and not stored.is_empty():
        lazy = lazy.join(
            stored.rename({'trade_date': date_col, 'source': 'stored_source'}).lazy(),
            on=date_col, how='left')
    else:
        lazy = lazy.with_columns([
            pl.lit(None, dtype=pl.Float64).alias('turn'),
            pl.lit(None, dtype=pl.Utf8).alias('stored_source'),
        ])

    if share_turn is not None:
        lazy = lazy.with_columns([
            pl.coalesce(share_turn, pl.col('turn')).alias('turn'),
            pl.coalesce(share_source, pl.col('stored_source')).alias('turn_source'),
        ])
    else:
        lazy = lazy.with_columns(
            pl.col('stored_source').alias('turn_source'))

    drop_cols = ['stored_source', 'effective_date']
    if not keep_float_share:
        drop_cols.append('float_share')
    # join 后行序无保证，指标计算（shift/rolling）依赖日期升序，必须显式排序
    return lazy.drop(
        [c for c in drop_cols if c in lazy.collect_schema().names()]
    ).sort(date_col)


def _canonical_code(ts_code: str) -> str:
    """统一代码格式；无法识别时原样返回。"""
    try:
        from src.utils.code_converter import to_ts_code
        return to_ts_code(str(ts_code))
    except (ImportError, ValueError, TypeError):
        return str(ts_code)


def attach_turnover(frame: pl.DataFrame, ts_code: str,
                    root: Path | str | None = None) -> pl.DataFrame:
    """把真实换手率按日期左连接到行情帧（列名 ``turn``）。

    兼容旧调用：帧内有 ``volume`` 时走分级来源（股本反算优先），
    否则退化为只读已落库 baostock turn。
    """
    if frame.is_empty() or 'date' not in frame.columns:
        return frame
    if 'volume' in frame.columns:
        return attach_turn(frame, ts_code, root).collect()
    turn = read_turnover(ts_code, root)
    if turn is None or turn.is_empty():
        return frame
    return frame.join(
        turn.rename({'trade_date': 'date'}), on='date', how='left')


# ----------------------------------------------------------------------
# 上游拉取（进程内）
# ----------------------------------------------------------------------
_SESSION: dict[str, Any] = {'login': False}


def _worker_init() -> None:
    """进程池初始化：每个进程独立登录 baostock（socket 不可跨线程共享）。"""
    import socket  # noqa: PLC0415

    import baostock as bs  # noqa: PLC0415 - 子进程内延迟导入，避免主进程强依赖

    # 上游偶发无响应；不设全局超时会让 login/next() 永久挂起，
    # 进程池整批卡死。20s 后抛 socket.timeout，交给重试装饰器处理。
    socket.setdefaulttimeout(20)
    # 错峰登录：上游对并发登录敏感，同时建立多条连接会返回「网络接收错误」
    time.sleep(0.4 + (os.getpid() % 10) * 0.15)
    lg = bs.login()
    _SESSION['login'] = lg is not None and lg.error_code == '0'
    if not _SESSION['login']:
        logger.warning(f'[turnover] 子进程 baostock 登录失败: {getattr(lg, "error_msg", "")}')


def _ensure_login() -> None:
    import socket  # noqa: PLC0415

    import baostock as bs  # noqa: PLC0415

    if _SESSION.get('login'):
        return
    if _SESSION.get('blocked'):
        raise RuntimeError('baostock 登录连续失败，已熔断；稍后重跑可断点续跑')
    socket.setdefaulttimeout(20)
    lg = bs.login()
    if lg is None or lg.error_code != '0':
        fails = int(_SESSION.get('login_fails', 0)) + 1
        _SESSION['login_fails'] = fails
        if fails >= 3:
            # 账号被拉黑/上游拒绝时，逐票重试只会放大失败量，直接熔断
            _SESSION['blocked'] = True
            logger.error(
                f'[turnover] baostock 连续 {fails} 次登录失败，熔断本次回补: '
                f'{getattr(lg, "error_msg", "")}')
        raise DataSourceConnectionError(
            source='baostock', message=f'登录失败: {getattr(lg, "error_msg", "")}')
    _SESSION['login'] = True
    _SESSION['login_fails'] = 0


@handle_exception_with_retry(max_retries=3, retry_delay=2)
def _query_turn_range(ts_code: str, start: date, end: date) -> pl.DataFrame:
    """查询单票单区间的 turn（百分数）；上游错误转成可重试异常。"""
    import baostock as bs  # noqa: PLC0415

    _ensure_login()
    rs = bs.query_history_k_data_plus(
        to_baostock_code(ts_code), TURN_FIELDS,
        start_date=start.isoformat(), end_date=end.isoformat(),
        frequency='d', adjustflag='3')
    if rs is None:
        raise DataSourceConnectionError(source='baostock', message=f'{ts_code} 返回 None')
    if rs.error_code != '0':
        raise DataSourceConnectionError(
            source='baostock',
            message=f'{ts_code} {start}~{end}: [{rs.error_code}] {rs.error_msg}')
    frame = schema.result_to_frame(rs)
    if rs.error_code != '0':  # 分页中途失败会留下半截数据，必须重试整段
        raise DataSourceConnectionError(
            source='baostock',
            message=f'{ts_code} 分页中断: [{rs.error_code}] {rs.error_msg}')
    return frame


def _frame_to_records(frame: pl.DataFrame, ts_code: str,
                      fetched_at: datetime) -> pl.DataFrame:
    """上游 frame -> 落库 frame；空 turn（停牌日）与坏值丢弃。"""
    if frame is None:
        return pl.DataFrame(schema=TURN_SCHEMA)
    df = pl.from_pandas(frame) if not isinstance(frame, pl.DataFrame) else frame
    if df.is_empty():
        return pl.DataFrame(schema=TURN_SCHEMA)
    # 停牌日 turn 为空串，转换后为 null，直接剔除
    out = df.select([
        pl.lit(ts_code).alias('ts_code'),
        pl.col('date').str.to_date('%Y-%m-%d', strict=False).alias('trade_date'),
        pl.col('turn').cast(pl.Float64, strict=False).alias('turn'),
    ]).filter(
        pl.col('trade_date').is_not_null()
        & pl.col('turn').is_not_null()
        & (pl.col('turn') >= 0)
    )
    if out.is_empty():
        return pl.DataFrame(schema=TURN_SCHEMA)
    return out.with_columns([
        pl.lit('baostock').alias('source'),
        pl.lit(fetched_at, dtype=pl.Datetime).alias('fetched_at'),
    ]).select(list(TURN_SCHEMA))


def _fetch_code(ts_code: str, start: date, end: date) -> pl.DataFrame:
    """单票全区间抓取；整段失败/为空时按自然年分段重试合并。"""
    fetched_at = datetime.now()
    try:
        frame = _query_turn_range(ts_code, start, end)
        records = _frame_to_records(frame, ts_code, fetched_at)
        if not records.is_empty():
            return records
    except DataSourceConnectionError as exc:
        # 登录/网络层失败：重试已在装饰器内耗尽，按年分段只会放大失败次数
        logger.warning(f'[turnover] {ts_code} 上游连接失败，本票放弃: {exc}')
        return pl.DataFrame(schema=TURN_SCHEMA)
    except (ConnectionError, TimeoutError, OSError) as exc:
        logger.warning(f'[turnover] {ts_code} 整段抓取失败，改按年分段: {exc}')

    # 单次查询过长（或跨页中断）时按自然年分段，逐段累积
    parts: list[pl.DataFrame] = []
    cursor = start
    while cursor <= end:
        seg_end = min(date(cursor.year, 12, 31), end)
        try:
            frame = _query_turn_range(ts_code, cursor, seg_end)
            seg = _frame_to_records(frame, ts_code, fetched_at)
            if not seg.is_empty():
                parts.append(seg)
        except DataSourceConnectionError as exc:
            # 连接层错误不再逐段重试，避免 20+ 年 × 3 次重试的雪崩
            logger.warning(f'[turnover] {ts_code} {cursor}~{seg_end} 连接失败，终止本票: {exc}')
            return pl.DataFrame(schema=TURN_SCHEMA)
        except (ConnectionError, TimeoutError, OSError) as exc:
            logger.warning(f'[turnover] {ts_code} {cursor}~{seg_end} 抓取失败: {exc}')
        cursor = seg_end + timedelta(days=1)
    if not parts:
        return pl.DataFrame(schema=TURN_SCHEMA)
    return pl.concat(parts, how='vertical_relaxed').unique(
        subset=['ts_code', 'trade_date'], keep='last').sort('trade_date')


def _worker_task(payload: dict[str, Any]) -> dict[str, Any]:
    """子进程任务：返回 (code, rows, last_date, error)。"""
    code = payload['code']
    start = date.fromisoformat(payload['start'])
    end = date.fromisoformat(payload['end'])
    try:
        records = _fetch_code(code, start, end)
    except Exception as exc:  # noqa: BLE001 - 子进程必须把异常回传主进程，不能中断整批
        return {'code': code, 'records': None, 'error': f'{type(exc).__name__}: {exc}'}
    if records.is_empty():
        return {'code': code, 'records': None, 'error': 'empty'}
    last = records['trade_date'].max()
    return {
        'code': code, 'records': records, 'error': None,
        'rows': records.height,
        'last_date': last.isoformat() if isinstance(last, date) else None,
    }


# ----------------------------------------------------------------------
# 回补编排
# ----------------------------------------------------------------------
def _parts_dir(root: Path) -> Path:
    return root / 'factors' / 'turnover_parts'


def _write_part(path: Path, frame: pl.DataFrame) -> None:
    tmp = path.with_suffix('.parquet.tmp')
    frame.write_parquet(
        tmp, compression='zstd', compression_level=3,
        statistics=True, row_group_size=100_000)
    os.replace(tmp, path)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.json.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(tmp, path)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {}


def _write_progress_csv(path: Path, rows: dict[str, str], failed: list[dict[str, Any]]) -> None:
    """每批刷新进度：已完成票的最新日期 + 失败清单。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ['code,last_date']
    lines += [f'{code},{last}' for code, last in sorted(rows.items())]
    tmp = path.with_suffix('.csv.tmp')
    tmp.write_text('\n'.join(lines) + '\n', encoding='utf-8', newline='\n')
    os.replace(tmp, path)
    _write_failed_csv(path.with_name('failed.csv'), failed)


def _write_failed_csv(path: Path, failed: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ['code,start,end,error,retries_used']
    lines += [
        f'{item["code"]},{item.get("start", "")},{item.get("end", "")},'
        f'"{str(item.get("error", "")).replace(chr(34), chr(39))}",{item.get("retries_used", 0)}'
        for item in failed
    ]
    tmp = path.with_suffix('.csv.tmp')
    tmp.write_text('\n'.join(lines) + '\n', encoding='utf-8', newline='\n')
    os.replace(tmp, path)


def _assemble(parts: list[Path], target: Path, replaced: set[str]) -> int | None:
    """把分块合并进目标表（流式、原子替换），返回合并后总行数。"""
    new_lazy = pl.concat([pl.scan_parquet(str(p)) for p in parts],
                         how='vertical_relaxed')
    keep_lazy = None
    if target.exists() and replaced:
        keep_lazy = pl.scan_parquet(str(target)).filter(
            ~pl.col('ts_code').is_in(sorted(replaced)))
    source = (pl.concat([keep_lazy, new_lazy], how='vertical_relaxed')
              if keep_lazy is not None else new_lazy)
    source = source.sort(['ts_code', 'trade_date'])

    tmp = target.with_suffix('.parquet.tmp')
    source.sink_parquet(
        str(tmp), compression='zstd', compression_level=3,
        statistics=True, row_group_size=100_000)
    os.replace(tmp, target)
    try:
        return int(pl.scan_parquet(str(target)).select(pl.len()).collect().item())
    except (OSError, ValueError, TypeError) as exc:
        logger.warning(f'[turnover] 统计最终行数失败: {exc}')
        return None


def _existing_last_dates(path: Path) -> dict[str, str]:
    """读取现表每个代码的末日期（用于增量起点）。"""
    if not path.exists():
        return {}
    try:
        frame = pl.scan_parquet(str(path)).group_by('ts_code').agg(
            pl.col('trade_date').max().alias('last')).collect()
        out: dict[str, str] = {}
        for row in frame.to_dicts():
            last = row['last']
            if isinstance(last, date):
                out[row['ts_code']] = last.isoformat()
        return out
    except (OSError, ValueError, pl.exceptions.PolarsError) as exc:
        logger.warning(f'[turnover] 读取现表末日期失败，按全历史回补: {exc}')
        return {}


def _build_tasks(quote_store: Any, universe: list[str],
                 last_dates: dict[str, str]) -> tuple[list[dict[str, Any]], int]:
    """按本地行情覆盖区间生成待抓任务；本地已覆盖到最新则跳过。"""
    tasks: list[dict[str, Any]] = []
    skipped = 0
    for code in universe:
        cov = quote_store.coverage('stock', code)
        if cov is None or cov.start is None or cov.end is None:
            skipped += 1
            continue
        begin = cov.start
        if begin < _BAOSTOCK_MIN_DATE:
            begin = _BAOSTOCK_MIN_DATE  # 元数据缺起始日时会写成 date.min
        if code in last_dates:
            begin = max(begin, date.fromisoformat(last_dates[code]) + timedelta(days=1))
        if begin > cov.end:
            continue  # 已是最新
        tasks.append({'code': code, 'start': begin.isoformat(),
                      'end': cov.end.isoformat()})
    return tasks, skipped


def _run_chunk(chunk: list[dict[str, Any]], workers: int
               ) -> tuple[list[pl.DataFrame], dict[str, str], list[dict[str, Any]]]:
    """跑一批标的；返回 (数据帧, 已完成 code->末日期, 失败项)。"""
    frames: list[pl.DataFrame] = []
    done: dict[str, str] = {}
    failures: list[dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=workers, initializer=_worker_init) as pool:
        futures = {pool.submit(_worker_task, task): task for task in chunk}
        for future in as_completed(futures):
            task = futures[future]
            try:
                result = future.result()
            except Exception as exc:  # noqa: BLE001 - 子进程崩溃也要记账
                result = {'code': task['code'], 'records': None,
                          'error': f'{type(exc).__name__}: {exc}'}
            if result.get('records') is None:
                failures.append({
                    'code': result['code'], 'start': task['start'],
                    'end': task['end'], 'error': result.get('error'),
                    # 装饰器内部重试耗尽后才会落到这里，故记最大重试次数
                    'retries_used': 3 if result.get('error') != 'empty' else 0,
                })
                continue
            frames.append(result['records'])
            done[result['code']] = result['last_date']
    return frames, done, failures


def _run_chunk_inline(chunk: list[dict[str, Any]]
                      ) -> tuple[list[pl.DataFrame], dict[str, str], list[dict[str, Any]]]:
    """串行跑一批（单连接复用）：上游对并发登录会拉黑账号，默认走这条路径。"""
    if not _SESSION.get('login'):
        _worker_init()
    frames: list[pl.DataFrame] = []
    done: dict[str, str] = {}
    failures: list[dict[str, Any]] = []
    for task in chunk:
        if _SESSION.get('blocked'):
            break  # 熔断：剩余票不记为失败，留给重跑
        result = _worker_task(task)
        if result.get('records') is None:
            failures.append({
                'code': result['code'], 'start': task['start'], 'end': task['end'],
                'error': result.get('error'),
                'retries_used': 3 if result.get('error') != 'empty' else 0,
            })
            continue
        frames.append(result['records'])
        done[result['code']] = result['last_date']
        time.sleep(0.15)  # 轻微节流，避免触发上游限流
    return frames, done, failures


def backfill_turnover(quote_store: Any, root: Path | str | None = None, *,
                      codes: Iterable[str] | None = None,
                      chunk_size: int = 200,
                      workers: int = 1,
                      resume: bool = True,
                      dry_run: bool = False,
                      max_codes: int | None = None) -> dict[str, Any]:
    """全市场换手率回补（幂等 + 断点续跑 + 失败清单）。

    Args:
        quote_store: ``LocalQuoteStore``，用于取每票本地行情起止日，保证
            turn 只覆盖有行情的区间（避免无效抓取）。
        codes: 限定标的；None 表示列存里的全部个股。
        chunk_size: 每批写盘的标的数（= 断点粒度）。
        workers: 并发进程数。默认 1（串行、单连接复用）：实测 >1 会触发
            baostock 账号拉黑（见模块文档），仅在确认上游放开时调大。
        resume: 复用同一批标的的已完成分块。
        max_codes: 只跑前 N 只（调试用）。
    """
    resolved_root = Path(root) if root else getattr(quote_store, 'root', default_store_root())
    target = turnover_path(resolved_root)
    parts_dir = _parts_dir(resolved_root)
    parts_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = parts_dir / 'manifest.json'
    progress_path = parts_dir / 'progress.csv'

    universe = list(quote_store.codes('stock'))
    if codes is not None:
        wanted = {c.strip().upper() for c in codes if c.strip()}
        universe = [c for c in universe if c in wanted]
    if max_codes is not None:
        universe = universe[:max_codes]

    last_dates = _existing_last_dates(target)
    tasks, skipped_no_quotes = _build_tasks(quote_store, universe, last_dates)

    if dry_run:
        total_rows = sum(quote_store.coverage('stock', t['code']).rows for t in tasks)
        logger.info(f'[turnover][dry-run] 待回补 {len(tasks)} 只，'
                    f'约 {total_rows:,} 行，跳过无行情 {skipped_no_quotes} 只')
        return {'codes': len(tasks), 'planned_rows': total_rows,
                'skipped_no_quotes': skipped_no_quotes, 'dry_run': True}

    if not tasks:
        logger.info('[turnover] 无待回补标的，表已是最新')
        return {'codes': 0, 'rows': 0, 'failed': 0, 'elapsed_s': 0.0}

    codes_hash = hashlib.sha1(
        '\n'.join(sorted(t['code'] for t in tasks)).encode('utf-8')).hexdigest()
    chunks = [tasks[i:i + chunk_size] for i in range(0, len(tasks), chunk_size)]

    state = _load_json(manifest_path)
    completed_parts: set[int] = set()
    resuming = (resume and state.get('codes_hash') == codes_hash
                and state.get('status') in {'running', 'failed'})
    completed_codes: dict[str, str] = dict(state.get('completed') or {}) if resuming else {}
    failed_items: list[dict[str, Any]] = list(state.get('failed') or []) if resuming else []
    if resuming:
        completed_parts = {
            int(idx) for idx in state.get('completed_parts', [])
            if (parts_dir / f'part_{int(idx):05d}.parquet').exists()}
        logger.info(f'[turnover] 断点续跑：已完成 {len(completed_parts)}/{len(chunks)} 分块，'
                    f'已有 {len(completed_codes)} 只')
    else:
        for stale in parts_dir.glob(_PARTS_GLOB):
            try:
                stale.unlink()
            except OSError:
                pass

    started = time.perf_counter()
    rows_added = 0
    written_codes = len(completed_codes)

    for index, chunk in enumerate(chunks):
        if index in completed_parts:
            continue
        frames, done, failures = (_run_chunk_inline(chunk) if workers <= 1
                                  else _run_chunk(chunk, workers))
        completed_codes.update(done)
        failed_items.extend(failures)
        written_codes += len(done)

        if frames:
            part_frame = pl.concat(frames, how='vertical_relaxed').sort(
                ['ts_code', 'trade_date'])
            _write_part(parts_dir / f'part_{index:05d}.parquet', part_frame)
            rows_added += part_frame.height
        blocked = bool(_SESSION.get('blocked'))
        # 熔断时本批未跑完，不能标记 complete，否则重跑会跳过未抓的票
        if not blocked:
            completed_parts.add(index)

        _write_json(manifest_path, {
            'status': 'running',
            'codes_hash': codes_hash,
            'chunk_size': chunk_size,
            'total_codes': len(tasks),
            'total_parts': len(chunks),
            'completed_parts': sorted(completed_parts),
            # 每批落盘一次进度（票 -> 最新日期），中断后可精确续跑
            'completed': completed_codes,
            'failed': failed_items,
            'started_at': state.get('started_at') if resuming else
            datetime.now().isoformat(timespec='seconds'),
        })
        _write_progress_csv(progress_path, completed_codes, failed_items)
        elapsed = time.perf_counter() - started
        logger.info(
            f'[turnover] 进度 {index + 1}/{len(chunks)} 分块，'
            f'已完成 {written_codes}/{len(tasks)} 只 / {rows_added:,} 行，'
            f'失败 {len(failed_items)} 只，已用 {elapsed:.1f}s')
        if blocked:
            logger.error(f'[turnover] 上游熔断，停在分块 {index}/{len(chunks)}，'
                         f'已落盘 {written_codes} 只，重跑将断点续跑')
            break

    parts = [parts_dir / f'part_{idx:05d}.parquet' for idx in sorted(completed_parts)
             if (parts_dir / f'part_{idx:05d}.parquet').exists()]
    total_rows = _assemble(parts, target, set(completed_codes)) if parts else None

    elapsed = time.perf_counter() - started
    _write_json(manifest_path, {
        'status': 'completed' if not failed_items else 'completed_with_failures',
        'codes_hash': codes_hash,
        'completed_parts': sorted(completed_parts),
        'total_parts': len(chunks),
        'completed_codes': len(completed_codes),
        'failed_codes': len(failed_items),
        'elapsed_s': round(elapsed, 3),
        'updated_at': datetime.now().isoformat(timespec='seconds'),
    })
    # 合并完成后分块可删（进度已由 manifest + progress.csv 保留）
    for part in parts:
        try:
            part.unlink()
        except OSError:
            pass

    logger.info(
        f'[turnover] 完成：{len(completed_codes)} 只 / {rows_added:,} 行新增，'
        f'合并后 {total_rows if total_rows is not None else 0:,} 行，'
        f'失败 {len(failed_items)} 只，耗时 {elapsed:.1f}s')
    return {
        'codes': len(tasks),
        'codes_written': len(completed_codes),
        'rows_added': rows_added,
        'total_rows': total_rows,
        'failed': len(failed_items),
        'failed_codes': [item['code'] for item in failed_items[:20]],
        'skipped_no_quotes': skipped_no_quotes,
        'elapsed_s': round(elapsed, 3),
        'progress_path': str(progress_path),
        'failed_path': str(progress_path.with_name('failed.csv')),
    }
