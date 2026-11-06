#!/usr/bin/env python3

"""本地 Parquet 列存的核心读写实现。

目录结构（根目录默认 ``data/local_store``）::

    quotes/stock/{sh,sz,bj}/{ts_code}.parquet
    quotes/index/{sh,sz,bj}/{ts_code}.parquet
    meta/daily_index.parquet        # 每标的一行的覆盖度元数据
    meta/state.json                 # 存储版本与最近一次构建信息
    factors/stock_adj_factor.parquet
    factors/stock_dividend.parquet
    factors/meta.json

读取约定：
- 行情文件按 ``date`` 升序，已含 ``pre_close/change/pct_chg`` 与
  ``qfq_factor/hfq_factor``，列名与 ``DataSourceBase._normalize_kline`` 一致；
- 行情文件里的 ``qfq_factor/hfq_factor`` **只是构建时点的副本**，因子快照
  ``factors/stock_adj_factor.parquet`` 才是唯一可信来源。读出口统一用快照
  覆盖副本（asof 前向填充 + 1.0 兜底），避免因子重建早于/晚于行情构建时
  出现「存的是一套、用的是另一套」；
- ``LocalQuoteStore.read_daily`` 只做区间切片，不做复权计算（复权由
  ``DataFetcher._apply_adjustment`` 在出口统一完成），保证与旧路径逐值一致；
- 覆盖度元数据单独存放，避免每次读单票都要打开 5,800 个文件。
"""

from __future__ import annotations

import contextlib
import json
import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable

import polars as pl
from loguru import logger

from src.data.local_store.paths import resolve_store_root as _resolve_store_root

STORE_VERSION = 1
# 因子快照单文件可能达到千万行，不能像分红快照那样一次性 partition 进内存。
# 采用「按 ts_code 谓词扫描 + 小容量 LRU」把常驻内存控制在几十 MB，
# 同时借助 parquet 行组统计（文件按 ts_code 排序）跳过无关数据块。
_FACTOR_LRU_MAX = 128

_META_SCHEMA = {
    'ts_code': pl.Utf8,
    'data_type': pl.Utf8,
    'market': pl.Utf8,
    'rows': pl.Int64,
    'start': pl.Date,
    'end': pl.Date,
    'built_at': pl.Datetime,
    'file_size': pl.Int64,
    'version': pl.Int64,
    # 是否由整文件构建（True 表示覆盖该标的在通达信里的全部历史）
    'full': pl.Boolean,
    # 构建时源 .day 文件的 mtime，用于判断源端是否已更新
    'source_mtime_ns': pl.Int64,
}

_LOCK = threading.RLock()
_QUOTE_STORE_CACHE: Dict[str, LocalQuoteStore] = {}
_FACTOR_STORE_CACHE: Dict[str, LocalFactorStore] = {}


def _data_settings(config: Any) -> Any:
    return getattr(config, 'data', None)


def local_store_enabled(config: Any) -> bool:
    """配置是否启用本地列存。

    仅接受真正的布尔 True：轻量测试配置（SimpleNamespace/MagicMock）缺省关闭，
    避免测试误读仓库里真实构建的列存。
    """
    return getattr(_data_settings(config), 'local_store_enabled', False) is True


def remote_database_enabled(config: Any) -> bool:
    """是否允许读侧回退远程 MySQL。

    ``DataSettings`` 显式默认 False；缺少该字段的历史轻量配置按 True 处理，
    保持既有调用方（测试/脚本）行为不变。
    """
    settings = _data_settings(config)
    sentinel = object()
    value = getattr(settings, 'remote_database_fallback', sentinel)
    if value is sentinel:
        return True
    return bool(value)


def resolve_store_root(config: Any) -> Path:
    """解析本地列存根目录（相对路径以项目根为基准）。

    实现已收口到 ``src.data.local_store.paths``；此处保留同名入口以兼容
    ``from src.data.local_store.store import resolve_store_root`` 的既有调用方。
    """
    return _resolve_store_root(config)


def _as_date(value: Any) -> date | None:
    """把 str/date/datetime 统一成 date。"""
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        with contextlib.suppress(ValueError):
            return datetime.strptime(text[:10] if '-' in text else text, fmt).date()
    return None


def market_of(ts_code: str) -> str:
    """从标准 ts_code 取市场目录（sh/sz/bj）。"""
    suffix = ts_code.rsplit('.', 1)[-1].lower() if '.' in ts_code else ''
    return suffix if suffix in {'sh', 'sz', 'bj'} else 'sz'


@dataclass(frozen=True)
class Coverage:
    """单标的的本地覆盖度。"""

    ts_code: str
    data_type: str
    rows: int
    start: date
    end: date
    built_at: datetime | None
    full: bool = True
    source_mtime_ns: int | None = None


class LocalQuoteStore:
    """日线行情本地列存（读写）。"""

    def __init__(self, root: Path | str, enabled: bool = True,
                 factor_store: LocalFactorStore | None = None) -> None:
        self.root = Path(root)
        self.enabled = bool(enabled)
        # 因子快照才是复权口径的唯一可信来源；行情文件里的 qfq/hfq 只是
        # 构建时点的副本。未显式注入时按同一根目录惰性创建。
        self.factor_store = factor_store
        self._meta: Dict[tuple[str, str], dict[str, Any]] = {}
        self._meta_mtime: int | None = None
        self._lock = threading.RLock()

    def factor_snapshot_store(self) -> LocalFactorStore:
        """返回因子快照读取器（未注入时按同一根目录惰性创建）。"""
        if self.factor_store is None:
            self.factor_store = LocalFactorStore(self.root, enabled=self.enabled)
        return self.factor_store

    # ------------------------------------------------------------------
    # 路径
    # ------------------------------------------------------------------
    @property
    def meta_path(self) -> Path:
        return self.root / 'meta' / 'daily_index.parquet'

    @property
    def state_path(self) -> Path:
        return self.root / 'meta' / 'state.json'

    def quote_path(self, data_type: str, ts_code: str) -> Path:
        return self.root / 'quotes' / data_type / market_of(ts_code) / f'{ts_code}.parquet'

    def exists(self) -> bool:
        return self.root.is_dir()

    # ------------------------------------------------------------------
    # 元数据
    # ------------------------------------------------------------------
    def _load_meta(self) -> Dict[tuple[str, str], dict[str, Any]]:
        """加载覆盖度元数据（按 mtime 失效）。"""
        if not self.meta_path.exists():
            # 不能清空内存：write_daily 逐标的调用本函数，元数据文件尚未落盘时
            # 清空会把前几个标的的条目丢掉（首次构建即触发）。
            self._meta_mtime = None
            return self._meta
        mtime = self.meta_path.stat().st_mtime_ns
        with self._lock:
            if self._meta_mtime == mtime:
                return self._meta
            frame = pl.read_parquet(self.meta_path)
            self._meta = {
                (row['data_type'], row['ts_code']): row
                for row in frame.to_dicts()
            }
            self._meta_mtime = mtime
            return self._meta

    def coverage(self, data_type: str, ts_code: str) -> Coverage | None:
        """返回单标的覆盖度；未构建返回 None。"""
        if not self.enabled:
            return None
        row = self._load_meta().get((data_type, ts_code))
        if not row:
            return None
        return Coverage(
            ts_code=ts_code,
            data_type=data_type,
            rows=int(row['rows']),
            start=_as_date(row['start']) or date.min,
            end=_as_date(row['end']) or date.min,
            built_at=row.get('built_at'),
            full=bool(row.get('full', True)),
            source_mtime_ns=row.get('source_mtime_ns'),
        )

    def covers(self, data_type: str, ts_code: str, start: Any, end: Any) -> bool:
        """判断本地存量是否覆盖请求区间。

        前缀必须完整；尾部允许 ``_GAP_TOLERANCE_DAYS`` 的节假日容差。
        """
        cov = self.coverage(data_type, ts_code)
        if cov is None:
            return False
        start_dt = _as_date(start)
        end_dt = _as_date(end)
        # 请求区间必须与存量有交集
        if start_dt is not None and cov.end < start_dt:
            return False
        if end_dt is not None and cov.start > end_dt:
            return False
        # 只有"整文件构建"过的标的才允许请求早于存量起点；
        # 否则说明该标的由 --since 增量首次写入，前缀缺失，需要回退 TDX。
        return not (not cov.full and start_dt is not None and cov.start > start_dt)

    def is_stale(self, data_type: str, ts_code: str, source_path: Path | None) -> bool:
        """源 ``.day`` 文件内容是否比本地存量更新。

        以**内容级末行日期**为准：只读源文件首尾各 32 字节，末行日期晚于
        本地 ``end`` 即判落后；mtime 仅在源文件无法解析时作兜底。这样即使
        同步工具保留了旧 mtime，也不会静默停在旧日期。
        """
        if source_path is None or not source_path.exists():
            return False
        cov = self.coverage(data_type, ts_code)
        if cov is None:
            return True
        from src.data.tdx_day_parser import read_first_last_trade_date  # noqa: PLC0415

        _, source_last = read_first_last_trade_date(source_path)
        if source_last is not None:
            return source_last > cov.end
        if cov.source_mtime_ns is None:
            return False
        try:
            return source_path.stat().st_mtime_ns > int(cov.source_mtime_ns)
        except OSError:
            return False

    def rescan_meta(self, tdx_path: Path | None = None) -> dict[str, Any]:
        """从 ``quotes/`` 现有 Parquet 重建覆盖度元数据。

        元数据文件被误覆盖/丢失时使用：逐文件读 ``date`` 列的 min/max 与
        行数，不重算行情；给了 ``tdx_path`` 时顺带对比源文件首日，判断
        ``full`` 并记录源 mtime。返回按类型统计的数量。
        """
        from src.data.tdx_day_parser import read_first_last_trade_date  # noqa: PLC0415

        quote_dir = self.root / 'quotes'
        rebuilt: Dict[tuple[str, str], dict[str, Any]] = {}
        by_type: Dict[str, int] = {}
        if not quote_dir.is_dir():
            logger.warning(f'[local-store] 列存目录不存在，无法重建元数据: {quote_dir}')
            return {'total': 0, 'by_type': {}}

        for kind_dir in sorted(p for p in quote_dir.iterdir() if p.is_dir()):
            data_type = kind_dir.name
            for market_dir in sorted(p for p in kind_dir.iterdir() if p.is_dir()):
                market = market_dir.name
                for path in sorted(market_dir.glob('*.parquet')):
                    ts_code = path.stem
                    try:
                        agg = pl.scan_parquet(str(path)).select([
                            pl.len().alias('rows'),
                            pl.col('date').min().alias('start'),
                            pl.col('date').max().alias('end'),
                        ]).collect()
                    except (OSError, pl.exceptions.PolarsError) as exc:
                        logger.warning(f'[local-store] 元数据重建跳过 {path}: {exc}')
                        continue
                    if agg.is_empty() or int(agg['rows'][0]) == 0:
                        continue
                    start = _as_date(agg['start'][0])
                    end = _as_date(agg['end'][0])
                    if start is None or end is None:
                        continue
                    full = True
                    source_mtime_ns = None
                    if tdx_path is not None:
                        source = tdx_path / market / 'lday' / f'{market}{ts_code.split(".")[0]}.day'
                        if source.exists():
                            source_mtime_ns = source.stat().st_mtime_ns
                            source_first, _ = read_first_last_trade_date(source)
                            if source_first is not None:
                                full = start <= source_first
                    rebuilt[(data_type, ts_code)] = {
                        'ts_code': ts_code,
                        'data_type': data_type,
                        'market': market_of(ts_code),
                        'rows': int(agg['rows'][0]),
                        'start': start,
                        'end': end,
                        'built_at': datetime.now(),
                        'file_size': path.stat().st_size,
                        'version': STORE_VERSION,
                        'full': full,
                        'source_mtime_ns': source_mtime_ns,
                    }
                    by_type[data_type] = by_type.get(data_type, 0) + 1

        with self._lock:
            self._meta = rebuilt
            # 让 flush_meta 内的惰性加载认为内存已是磁盘最新版本，避免把重建结果覆盖回旧文件。
            self._meta_mtime = (
                self.meta_path.stat().st_mtime_ns if self.meta_path.exists() else None)
        self.flush_meta()
        logger.info(f'[local-store] 覆盖度元数据重建完成: {len(rebuilt)} 个文件 / {by_type}')
        return {'total': len(rebuilt), 'by_type': by_type}

    def codes(self, data_type: str = 'stock') -> list[str]:
        """返回该资产类型下已构建的全部 ts_code（升序）。"""
        if not self.enabled:
            return []
        return sorted(code for (kind, code) in self._load_meta() if kind == data_type)

    def stats(self) -> dict[str, Any]:
        """汇总列存规模（供 CLI 与可观测性使用）。"""
        meta = self._load_meta()
        quote_dir = self.root / 'quotes'
        files = list(quote_dir.rglob('*.parquet')) if quote_dir.exists() else []
        by_type: Dict[str, int] = {}
        for (data_type, _code) in meta:
            by_type[data_type] = by_type.get(data_type, 0) + 1
        return {
            'root': str(self.root),
            'symbols': len(meta),
            'by_type': by_type,
            'rows': sum(int(r['rows']) for r in meta.values()),
            'files': len(files),
            'disk_bytes': sum(f.stat().st_size for f in files) if files else 0,
            'meta_path': str(self.meta_path),
        }

    # ------------------------------------------------------------------
    # 读取
    # ------------------------------------------------------------------
    def read_daily(self, data_type: str, ts_code: str, start: Any, end: Any,
                   frequency: str = '1d') -> pl.DataFrame | None:
        """按区间读取日线；未构建/未覆盖返回 None（调用方回退 TDX）。"""
        if not self.enabled or frequency != '1d':
            return None
        if not self.covers(data_type, ts_code, start, end):
            return None
        path = self.quote_path(data_type, ts_code)
        if not path.exists():
            logger.warning(f"[local-store] 元数据存在但文件缺失: {path}")
            return None

        start_dt = _as_date(start)
        end_dt = _as_date(end)
        frame = pl.read_parquet(path)
        if start_dt is not None:
            frame = frame.filter(pl.col('date') >= start_dt)
        if end_dt is not None:
            frame = frame.filter(pl.col('date') <= end_dt)
        return _overlay_factor_snapshot(frame, ts_code, self.factor_snapshot_store())

    def read_file(self, data_type: str, ts_code: str,
                  with_factors: bool = False) -> pl.DataFrame | None:
        """直接读整份存量文件（不做覆盖度判断，供增量合并使用）。

        ``with_factors=True`` 时用因子快照覆盖内嵌副本；纯扫描场景（流动性
        筛选、日期序列、增量合并比对）保持默认 False，避免为每只标的都做
        一次因子查询。
        """
        path = self.quote_path(data_type, ts_code)
        if not path.exists():
            return None
        frame = pl.read_parquet(path)
        if not with_factors:
            return frame
        return _overlay_factor_snapshot(frame, ts_code, self.factor_snapshot_store())

    def read_daily_batch(self, data_type: str, codes: list[str], start: Any, end: Any,
                         frequency: str = '1d') -> dict[str, pl.DataFrame]:
        """批量读取（一次调用内完成多票切片，供组合回测/横截面使用）。"""
        out: dict[str, pl.DataFrame] = {}
        for code in codes:
            frame = self.read_daily(data_type, code, start, end, frequency)
            if frame is not None and not frame.is_empty():
                out[code] = frame
        return out

    # ------------------------------------------------------------------
    # 写入（构建器调用，原子替换）
    # ------------------------------------------------------------------
    def write_daily(self, data_type: str, ts_code: str, frame: pl.DataFrame,
                    full: bool = True, source_mtime_ns: int | None = None) -> int:
        """写入单标的行情并刷新元数据，返回写入行数。

        ``full=False`` 表示这是增量片段（前缀不完整），此时 ``covers`` 会对
        早于存量起点的请求返回 False，强制回退通达信，避免残缺前缀被当完整数据。
        """
        if frame.is_empty():
            return 0
        path = self.quote_path(data_type, ts_code)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix('.parquet.tmp')
        frame.sort('date').write_parquet(
            tmp, compression='zstd', compression_level=3, statistics=True)
        os.replace(tmp, path)

        rows = frame.height
        first = frame['date'].min()
        last = frame['date'].max()
        # 必须先加载既有元数据再更新，否则只写本次涉及的标的会把其他条目清空。
        self._load_meta()
        with self._lock:
            self._meta[(data_type, ts_code)] = {
                'ts_code': ts_code,
                'data_type': data_type,
                'market': market_of(ts_code),
                'rows': rows,
                'start': first,
                'end': last,
                'built_at': datetime.now(),
                'file_size': path.stat().st_size,
                'version': STORE_VERSION,
                'full': full,
                'source_mtime_ns': source_mtime_ns,
            }
        return rows

    def flush_meta(self) -> None:
        """把内存中的覆盖度元数据落盘。"""
        # 只在磁盘已有元数据时合并：文件不存在时 _load_meta 会把内存清空，
        # 导致首次构建（write_daily → flush_meta）写入的条目丢失。
        if self.meta_path.exists():
            self._load_meta()
        with self._lock:
            rows = list(self._meta.values())
        self.meta_path.parent.mkdir(parents=True, exist_ok=True)
        if not rows:
            if self.meta_path.exists():
                self.meta_path.unlink()
            return
        frame = pl.DataFrame(rows, schema_overrides=_META_SCHEMA).sort(
            ['data_type', 'ts_code'])
        tmp = self.meta_path.with_suffix('.parquet.tmp')
        frame.write_parquet(tmp, compression='zstd', statistics=True)
        os.replace(tmp, self.meta_path)

    def write_state(self, payload: dict[str, Any]) -> None:
        """写构建状态（版本/时间/数据源），供排障与幂等判断。"""
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(payload)
        payload.setdefault('version', STORE_VERSION)
        payload.setdefault('updated_at', datetime.now().isoformat(timespec='seconds'))
        tmp = self.state_path.with_suffix('.json.tmp')
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        os.replace(tmp, self.state_path)

    def read_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {}
        try:
            return json.loads(self.state_path.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            return {}


class LocalFactorStore:
    """复权因子与分红本地快照（只读）。

    因子快照按 ``(ts_code, trade_date)`` 升序落盘：读单票时用谓词扫描
    （parquet 行组统计可跳过无关块）+ 小容量 LRU，避免把千万行整表
    ``partition_by`` 进内存；分红快照只有几万行，仍整表分组常驻。
    """

    _FACTOR_COLUMNS = ('trade_date', 'qfq_factor', 'hfq_factor')

    def __init__(self, root: Path | str, enabled: bool = True) -> None:
        self.root = Path(root)
        self.enabled = bool(enabled)
        self._factor_path = self.root / 'factors' / 'stock_adj_factor.parquet'
        self._dividend_path = self.root / 'factors' / 'stock_dividend.parquet'
        # 基金事件与因子单独成表（语义与个股不同，维护链路也不同）；
        # 读侧按代码回退到这两个文件，避免把基金混入个股 covered_codes 语义。
        self._fund_factor_path = self.root / 'factors' / 'fund_adj_factor.parquet'
        self._fund_dividend_path = self.root / 'factors' / 'fund_dividend.parquet'
        self._factor_cache: OrderedDict[str, pl.DataFrame | None] = OrderedDict()
        self._factor_lazy: pl.LazyFrame | None = None
        self._factor_file_mtime: int | None = None
        self._fund_factor_cache: OrderedDict[str, pl.DataFrame | None] = OrderedDict()
        self._fund_factor_lazy: pl.LazyFrame | None = None
        self._fund_factor_file_mtime: int | None = None
        self._dividend_rows: dict[str, pl.DataFrame] | None = None
        self._fund_dividend_rows: dict[str, pl.DataFrame] | None = None
        self._lock = threading.RLock()

    @property
    def factor_path(self) -> Path:
        """因子快照文件路径（构建器用于原子替换）。"""
        return self._factor_path

    @property
    def dividend_path(self) -> Path:
        """分红快照文件路径。"""
        return self._dividend_path

    @property
    def fund_factor_path(self) -> Path:
        """基金因子快照文件路径（与个股因子分表存放）。"""
        return self._fund_factor_path

    def available(self) -> bool:
        return self.enabled and (
            self._factor_path.exists() or self._dividend_path.exists()
            or self._fund_factor_path.exists() or self._fund_dividend_path.exists())

    def _partition(self, path: Path, key: str) -> dict[str, pl.DataFrame]:
        if not path.exists():
            return {}
        frame = pl.read_parquet(path)
        if frame.is_empty() or key not in frame.columns:
            return {}
        return {str(group[key][0]): group for group in frame.partition_by(key, maintain_order=True)}

    def _load_dividends(self) -> dict[str, pl.DataFrame]:
        with self._lock:
            if self._dividend_rows is None:
                self._dividend_rows = self._partition(self._dividend_path, 'ts_code')
            return self._dividend_rows

    def _load_fund_dividends(self) -> dict[str, pl.DataFrame]:
        with self._lock:
            if self._fund_dividend_rows is None:
                self._fund_dividend_rows = self._partition(
                    self._fund_dividend_path, 'ts_code')
            return self._fund_dividend_rows

    def _factor_frame(self, ts_code: str) -> pl.DataFrame | None:
        """按代码读取因子片段（LRU 缓存；快照文件被重建后自动失效）。"""
        if not self._factor_path.exists():
            return None
        try:
            file_mtime = self._factor_path.stat().st_mtime_ns
        except OSError:
            return None
        with self._lock:
            if self._factor_file_mtime != file_mtime:
                self._factor_cache.clear()
                # 复用同一个 LazyFrame 可省掉每次重复读取 parquet footer 的开销。
                self._factor_lazy = pl.scan_parquet(str(self._factor_path))
                self._factor_file_mtime = file_mtime
            if ts_code in self._factor_cache:
                self._factor_cache.move_to_end(ts_code)
                return self._factor_cache[ts_code]
            lazy = self._factor_lazy

        if lazy is None:
            return None
        frame = lazy.filter(pl.col('ts_code') == ts_code).collect()
        result: pl.DataFrame | None
        if frame.is_empty():
            result = None
        else:
            cols = [c for c in self._FACTOR_COLUMNS if c in frame.columns]
            result = frame.select(cols).sort('trade_date')

        with self._lock:
            self._factor_cache[ts_code] = result
            while len(self._factor_cache) > _FACTOR_LRU_MAX:
                self._factor_cache.popitem(last=False)
        return result

    def factors(self, ts_code: str) -> pl.DataFrame | None:
        """返回单标的复权因子（trade_date/qfq_factor/hfq_factor）。

        个股/指数走 ``stock_adj_factor.parquet``；该文件中没有的代码再回退到
        ``fund_adj_factor.parquet``（基金与个股共用同一口径的因子列）。
        """
        if not self.enabled:
            return None
        result = self._factor_frame(ts_code)
        if result is None:
            result = self._fund_factor_frame(ts_code)
        return result

    def _fund_factor_frame(self, ts_code: str) -> pl.DataFrame | None:
        """按代码读取基金因子片段（LRU 缓存；文件重建后自动失效）。"""
        path = self._fund_factor_path
        if not path.exists():
            return None
        try:
            file_mtime = path.stat().st_mtime_ns
        except OSError:
            return None
        with self._lock:
            if self._fund_factor_file_mtime != file_mtime:
                self._fund_factor_cache.clear()
                self._fund_factor_lazy = pl.scan_parquet(str(path))
                self._fund_factor_file_mtime = file_mtime
            if ts_code in self._fund_factor_cache:
                self._fund_factor_cache.move_to_end(ts_code)
                return self._fund_factor_cache[ts_code]
            lazy = self._fund_factor_lazy

        if lazy is None:
            return None
        frame = lazy.filter(pl.col('ts_code') == ts_code).collect()
        result: pl.DataFrame | None
        if frame.is_empty():
            result = None
        else:
            cols = [c for c in self._FACTOR_COLUMNS if c in frame.columns]
            result = frame.select(cols).sort('trade_date')

        with self._lock:
            self._fund_factor_cache[ts_code] = result
            while len(self._fund_factor_cache) > _FACTOR_LRU_MAX:
                self._fund_factor_cache.popitem(last=False)
        return result

    def dividends(self, ts_code: str) -> pl.DataFrame | None:
        """返回单标的分红记录（列与 ``stock_dividend`` 表一致）。

        基金代码在个股分红快照中没有记录时，回退到 ``fund_dividend.parquet``。
        """
        if not self.enabled:
            return None
        frame = self._load_dividends().get(ts_code)
        if frame is None or frame.is_empty():
            frame = self._load_fund_dividends().get(ts_code)
        if frame is None or frame.is_empty():
            return None
        return frame

    def meta(self) -> dict[str, Any]:
        path = self.root / 'factors' / 'meta.json'
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            return {}


def _mtime_ns(path: Path) -> int | None:
    try:
        return path.stat().st_mtime_ns
    except OSError:
        return None


_FACTOR_STORE_DISABLED_WARNED = False


def _warn_factor_store_disabled() -> None:
    """因子存储整体未启用时只告警一次，避免逐标的刷屏。"""
    global _FACTOR_STORE_DISABLED_WARNED
    if _FACTOR_STORE_DISABLED_WARNED:
        return
    _FACTOR_STORE_DISABLED_WARNED = True
    logger.warning(
        '[local-store] 因子快照存储未启用，行情内嵌因子副本未经覆盖'
        '（副本可能过期或恒为 1.0，复权结果不可靠）')


def _overlay_factor_snapshot(
    frame: pl.DataFrame, ts_code: str, factor_store: LocalFactorStore | None,
) -> pl.DataFrame:
    """用因子快照覆盖行情内嵌的 ``qfq_factor/hfq_factor`` 副本。

    口径与 ``build_frame`` 完全一致：按日期左连接快照 → 前向填充 → 1.0 兜底。
    行情副本只是构建时点的快照；因子重建一旦晚于行情构建，副本就会过期，
    直接把副本当复权因子会把未复权价当复权价返回。
    """
    if frame.is_empty() or 'date' not in frame.columns:
        return frame
    if 'qfq_factor' not in frame.columns and 'hfq_factor' not in frame.columns:
        return frame
    if factor_store is None or not factor_store.enabled:
        _warn_factor_store_disabled()
        return frame
    snapshot = factor_store.factors(ts_code)
    if snapshot is None or snapshot.is_empty():
        # 快照缺票时不能静默沿用行情内嵌副本：副本可能过期甚至恒为 1.0，
        # 复权结果会退化成未复权价，必须让调用方/日志可见。
        logger.warning(
            f'[local-store] {ts_code} 因子快照缺失，沿用行情内嵌因子副本'
            '（副本可能过期或恒为 1.0，复权结果不可靠）')
        return frame

    snap = snapshot.rename({
        'trade_date': 'date',
        'qfq_factor': '_qfq_snapshot',
        'hfq_factor': '_hfq_snapshot',
    }).select(['date', '_qfq_snapshot', '_hfq_snapshot']).sort('date')
    merged = frame.sort('date').join_asof(snap, on='date', strategy='backward')
    exprs: list[pl.Expr] = []
    if 'qfq_factor' in frame.columns:
        exprs.append(pl.col('_qfq_snapshot').fill_null(1.0)
                     .cast(pl.Float64).alias('qfq_factor'))
    if 'hfq_factor' in frame.columns:
        exprs.append(pl.col('_hfq_snapshot').fill_null(1.0)
                     .cast(pl.Float64).alias('hfq_factor'))
    return merged.with_columns(exprs).select(frame.columns)


def overlay_factor_snapshot(
    frame: pl.DataFrame, ts_code: str, factor_store: LocalFactorStore | None,
) -> pl.DataFrame:
    """公开别名：用本地因子快照覆盖行情/库内内嵌的因子副本。"""
    return _overlay_factor_snapshot(frame, ts_code, factor_store)


def _recompute_derived_columns(frame: pl.DataFrame) -> pl.DataFrame:
    """按新 qfq_factor 重算 ``pre_close/change/pct_chg``（与 ``build_frame`` 同式）。

    调用方必须保证 ``frame`` 已按 ``date`` 升序；仅用于整文件重建，
    读取侧切片后的首行没有前一日，重算会把 pre_close 置空，故不在读路径使用。
    """
    if not {'close', 'qfq_factor', 'pre_close', 'change', 'pct_chg'} <= set(frame.columns):
        return frame
    qfq = pl.col('qfq_factor').cast(pl.Float64)
    pre_close = pl.col('close').shift(1) * (qfq.shift(1) / qfq)
    return frame.with_columns([
        pre_close.alias('pre_close'),
        (pl.col('close') - pre_close).alias('change'),
        pl.when(pre_close.is_not_null() & (pre_close != 0))
        .then((pl.col('close') / pre_close - 1) * 100)
        .otherwise(None).alias('pct_chg'),
    ])


def refresh_quote_factor_columns(
    quote_store: LocalQuoteStore,
    factor_store: LocalFactorStore,
    *,
    codes: Iterable[str] | None = None,
    kind: str = 'stock',
    dry_run: bool = False,
) -> dict[str, Any]:
    """把行情 parquet 内嵌的因子副本对齐到因子快照（原子重写单文件）。

    因子快照重建后必须调用本函数，否则行情副本会一直停在旧口径。
    返回统计信息；``dry_run=True`` 只统计需要重写多少只/多少行。
    """
    started = time.perf_counter()
    targets = list(codes) if codes is not None else quote_store.codes(kind)
    watched_paths = (factor_store.factor_path, factor_store.fund_factor_path)
    factor_mtime = tuple(_mtime_ns(path) for path in watched_paths)
    changed_codes: list[str] = []
    changed_rows = 0
    missing = 0
    unchanged = 0
    no_factor_cols = 0

    for code in targets:
        if not code:
            continue
        frame = quote_store.read_file(kind, code)
        if frame is None or frame.is_empty():
            missing += 1
            continue
        if 'qfq_factor' not in frame.columns and 'hfq_factor' not in frame.columns:
            no_factor_cols += 1
            continue
        fixed = _overlay_factor_snapshot(frame, code, factor_store)
        diff_rows = 0
        for col in ('qfq_factor', 'hfq_factor'):
            if col in frame.columns and col in fixed.columns:
                diff_rows += int((fixed[col] != frame[col]).sum())
        if diff_rows == 0:
            unchanged += 1
            continue
        changed_codes.append(code)
        changed_rows += diff_rows
        if dry_run:
            continue
        fixed = _recompute_derived_columns(fixed.sort('date'))
        cov = quote_store.coverage(kind, code)
        quote_store.write_daily(
            kind, code, fixed,
            full=cov.full if cov is not None else True,
            source_mtime_ns=cov.source_mtime_ns if cov is not None else None)

    if not dry_run and changed_codes:
        quote_store.flush_meta()
    current_mtime = tuple(_mtime_ns(path) for path in watched_paths)
    if factor_mtime != current_mtime:
        logger.warning(
            '[local-store] 因子快照在同步过程中被重建，副本可能仍落后，请重跑本步骤')

    stats = {
        'kind': kind,
        'targets': len(targets),
        'changed_codes': len(changed_codes),
        'changed_rows': changed_rows,
        'unchanged_codes': unchanged,
        'missing_codes': missing,
        'no_factor_columns': no_factor_cols,
        'dry_run': dry_run,
        'elapsed_s': round(time.perf_counter() - started, 3),
    }
    logger.info(
        f'[local-store] 行情因子副本同步{"（dry-run）" if dry_run else ""}：'
        f'重写 {stats["changed_codes"]} 只 / {changed_rows:,} 行，'
        f'已一致 {unchanged} 只，耗时 {stats["elapsed_s"]}s')
    return stats


def verify_quote_factor_consistency(
    quote_store: LocalQuoteStore,
    factor_store: LocalFactorStore,
    *,
    kinds: tuple[str, ...] = ('stock', 'index', 'fund'),
    tolerance: float = 1e-12,
) -> dict[str, Any]:
    """全量核对行情副本 vs 因子快照，返回按类型的差异统计。

    以快照为基准做 asof 前向填充 + 1.0 兜底，逐行比对 ``qfq_factor`` 与
    ``hfq_factor``；个股/指数以 ``stock_adj_factor.parquet`` 为基准，基金以
    ``fund_adj_factor.parquet`` 为基准（两表代码不重叠，合并后统一查询）。
    任何一只出现偏差都计入 ``mismatch_codes``。
    """
    factor_path = factor_store.factor_path
    fund_factor_path = factor_store.fund_factor_path
    parts: list[pl.DataFrame] = []
    for path in (factor_path, fund_factor_path):
        if path.exists():
            parts.append(pl.read_parquet(
                path, columns=['ts_code', 'trade_date', 'qfq_factor', 'hfq_factor']))
    if not parts:
        return {'error': f'因子快照不存在: {factor_path} / {fund_factor_path}'}
    snapshot = pl.concat(parts, how='vertical_relaxed').unique(
        subset=['ts_code', 'trade_date'], keep='last').sort(['ts_code', 'trade_date'])

    result: dict[str, Any] = {}
    for kind in kinds:
        files = sorted((quote_store.root / 'quotes' / kind).rglob('*.parquet'))
        if not files:
            result[kind] = {'files': 0, 'rows': 0, 'mismatch_codes': 0,
                            'mismatch_rows': 0}
            continue
        quotes = pl.scan_parquet([str(p) for p in files]).select(
            ['ts_code', 'date', 'qfq_factor', 'hfq_factor']).collect().sort(
            ['ts_code', 'date'])
        joined = quotes.join_asof(
            snapshot, left_on='date', right_on='trade_date',
            by='ts_code', strategy='backward')
        bad_q = (pl.col('qfq_factor') - pl.col('qfq_factor_right').fill_null(1.0)).abs() > (
            tolerance * pl.col('qfq_factor_right').fill_null(1.0).abs().clip(1e-300))
        bad_h = (pl.col('hfq_factor') - pl.col('hfq_factor_right').fill_null(1.0)).abs() > (
            tolerance * pl.col('hfq_factor_right').fill_null(1.0).abs().clip(1e-300))
        agg = joined.group_by('ts_code').agg([
            (bad_q | bad_h).sum().alias('bad_rows'),
            pl.len().alias('rows'),
        ])
        bad = agg.filter(pl.col('bad_rows') > 0)
        result[kind] = {
            'files': len(files),
            'rows': int(quotes.height),
            'codes': int(agg.height),
            'mismatch_codes': int(bad.height),
            'mismatch_rows': int(agg['bad_rows'].sum()),
            'sample_codes': bad.sort('bad_rows', descending=True)['ts_code'].head(20).to_list(),
        }
    return result


def get_local_quote_store(config: Any) -> LocalQuoteStore:
    """进程内单例：按根目录与开关缓存。"""
    root = resolve_store_root(config)
    key = f'{root}|{local_store_enabled(config)}'
    with _LOCK:
        store = _QUOTE_STORE_CACHE.get(key)
        if store is None:
            store = LocalQuoteStore(
                root, enabled=local_store_enabled(config),
                factor_store=get_local_factor_store(config))
            _QUOTE_STORE_CACHE[key] = store
        return store


def get_local_factor_store(config: Any) -> LocalFactorStore:
    """进程内单例：按根目录与开关缓存。"""
    root = resolve_store_root(config)
    key = f'{root}|{local_store_enabled(config)}'
    with _LOCK:
        store = _FACTOR_STORE_CACHE.get(key)
        if store is None:
            store = LocalFactorStore(root, enabled=local_store_enabled(config))
            _FACTOR_STORE_CACHE[key] = store
        return store


def reset_local_stores() -> None:
    """清空单例缓存（测试用）。"""
    with _LOCK:
        _QUOTE_STORE_CACHE.clear()
        _FACTOR_STORE_CACHE.clear()
