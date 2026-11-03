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
- ``LocalQuoteStore.read_daily`` 只做区间切片，不做复权计算（复权由
  ``DataFetcher._apply_adjustment`` 在出口统一完成），保证与旧路径逐值一致；
- 覆盖度元数据单独存放，避免每次读单票都要打开 5,800 个文件。
"""

from __future__ import annotations

import contextlib
import json
import os
import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict

import polars as pl
from loguru import logger

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
    """解析本地列存根目录（相对路径以项目根为基准）。"""
    raw = getattr(_data_settings(config), 'local_store_path', '') or 'data/local_store'
    path = Path(str(raw))
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[3] / path
    return path


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

    def __init__(self, root: Path | str, enabled: bool = True) -> None:
        self.root = Path(root)
        self.enabled = bool(enabled)
        self._meta: Dict[tuple[str, str], dict[str, Any]] = {}
        self._meta_mtime: int | None = None
        self._lock = threading.RLock()

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
            self._meta = {}
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
        """源 ``.day`` 文件在构建后是否被更新过（用于决定是否回退 TDX）。

        只比较 mtime，不解析文件；源文件不存在时视为不落后。
        """
        if source_path is None or not source_path.exists():
            return False
        cov = self.coverage(data_type, ts_code)
        if cov is None or cov.source_mtime_ns is None:
            return False
        try:
            return source_path.stat().st_mtime_ns > int(cov.source_mtime_ns)
        except OSError:
            return False

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
        return frame

    def read_file(self, data_type: str, ts_code: str) -> pl.DataFrame | None:
        """直接读整份存量文件（不做覆盖度判断，供增量合并使用）。"""
        path = self.quote_path(data_type, ts_code)
        if not path.exists():
            return None
        return pl.read_parquet(path)

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
        self._factor_cache: OrderedDict[str, pl.DataFrame | None] = OrderedDict()
        self._factor_lazy: pl.LazyFrame | None = None
        self._factor_file_mtime: int | None = None
        self._dividend_rows: dict[str, pl.DataFrame] | None = None
        self._lock = threading.RLock()

    @property
    def factor_path(self) -> Path:
        """因子快照文件路径（构建器用于原子替换）。"""
        return self._factor_path

    @property
    def dividend_path(self) -> Path:
        """分红快照文件路径。"""
        return self._dividend_path

    def available(self) -> bool:
        return self.enabled and (
            self._factor_path.exists() or self._dividend_path.exists())

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
        """返回单标的复权因子（trade_date/qfq_factor/hfq_factor）。"""
        if not self.enabled:
            return None
        return self._factor_frame(ts_code)

    def dividends(self, ts_code: str) -> pl.DataFrame | None:
        """返回单标的分红记录（列与 ``stock_dividend`` 表一致）。"""
        if not self.enabled:
            return None
        frame = self._load_dividends().get(ts_code)
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


def get_local_quote_store(config: Any) -> LocalQuoteStore:
    """进程内单例：按根目录与开关缓存。"""
    root = resolve_store_root(config)
    key = f'{root}|{local_store_enabled(config)}'
    with _LOCK:
        store = _QUOTE_STORE_CACHE.get(key)
        if store is None:
            store = LocalQuoteStore(root, enabled=local_store_enabled(config))
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
