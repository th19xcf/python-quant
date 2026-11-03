#!/usr/bin/env python3

"""跨源抽样对拍（本地列存 vs BaoStock / AkShare）。

设计约束：

- 远程源只在本次 CLI 调用内通过内存配置开启，不修改 ``.env`` 默认值；
- 单源串行（并发=1），每次请求前按最小间隔限速，失败指数退避，不高频重试；
- 连续空结果达到阈值后提前停止该源，避免对已限流端点继续施压；
- 只读对拍，输出差异报告，绝不回写任何数据。
"""

from __future__ import annotations

import csv
import json
import random
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Protocol, Sequence

import polars as pl
from loguru import logger

from src.data.quality.checker import prepare_frame
from src.utils.code_converter import is_index_code

#: 对拍字段；派生列（pre_close/change/pct_chg）由各源自行计算，口径差异大，不纳入
COMPARE_FIELDS = ('open', 'high', 'low', 'close', 'volume', 'amount')

#: 单源并发上限，当前实现为串行循环（显式常量便于审计）
MAX_CONCURRENCY = 1

#: (最小间隔秒, 最大重试次数, 退避基数秒, 退避上限秒)
RATE_LIMITS: dict[str, tuple[float, int, float, float]] = {
    'baostock': (0.2, 2, 2.0, 8.0),
    # 东财对高频请求会临时封 IP；间隔放大到 3 秒，退避上限 30 秒
    'akshare': (3.0, 2, 5.0, 30.0),
}

#: 单字段容差：(相对容差, 绝对容差)
FIELD_TOLERANCES: dict[str, tuple[float, float]] = {
    'open': (1e-6, 0.011),
    'high': (1e-6, 0.011),
    'low': (1e-6, 0.011),
    'close': (1e-6, 0.011),
    'volume': (1e-4, 1.0),
    'amount': (1e-4, 1.0),
}


class RemoteAdapter(Protocol):
    """远程行情适配器契约（单标的、日期区间、返回标准化 DataFrame）。"""

    name: str

    def fetch(self, ts_code: str, start: date, end: date) -> pl.DataFrame:
        ...


class RateLimiter:
    """串行限速 + 指数退避。

    空结果不重试（"无数据"不是瞬时错误），异常才按退避策略重试。
    """

    def __init__(self, min_interval: float, max_retries: int,
                 backoff_base: float, max_delay: float) -> None:
        if min_interval < 0:
            raise ValueError('min_interval 不能为负')
        self.min_interval = float(min_interval)
        self.max_retries = int(max_retries)
        self.backoff_base = float(backoff_base)
        self.max_delay = float(max_delay)
        self._last_call = 0.0
        self._lock = Lock()

    def _wait_turn(self) -> None:
        with self._lock:
            elapsed = time.monotonic() - self._last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self._last_call = time.monotonic()

    def call(self, func: Callable[..., pl.DataFrame], *args: Any, **kwargs: Any) -> pl.DataFrame:
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            self._wait_turn()
            try:
                return func(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001 - 第三方 SDK 异常类型不可预知
                last_error = exc
                if attempt >= self.max_retries:
                    break
                delay = min(self.max_delay, self.backoff_base * (2 ** attempt))
                logger.warning(
                    f'远程源请求失败（第 {attempt + 1} 次），{delay:.1f}s 后重试: {exc}')
                time.sleep(delay)
        assert last_error is not None
        raise last_error


class BaostockAdapter:
    """BaoStock 只读适配器（db_manager=None，确保不落库）。"""

    name = 'baostock'

    def __init__(self, config: Any) -> None:
        from src.data.baostock_handler import BaostockHandler  # noqa: PLC0415

        self._handler = BaostockHandler(config, None)

    def fetch(self, ts_code: str, start: date, end: date) -> pl.DataFrame:
        return self._handler.get_stock_data(ts_code, str(start), str(end))


class AkShareAdapter:
    """AkShare 只读适配器（直连东财日线接口）。

    项目内 ``AkShareHandler`` 依赖“昨收”等固定列契约，而上游
    ``stock_zh_a_hist`` 已不再返回该列；对拍只关心原始 OHLCV，故直接
    调用 AkShare 并映射为标准列，避免被无关列契约阻断。
    """

    name = 'akshare'

    def __init__(self, config: Any) -> None:
        self._config = config

    def fetch(self, ts_code: str, start: date, end: date) -> pl.DataFrame:
        import akshare as ak  # noqa: PLC0415

        symbol = ts_code.split('.')[0]
        frame = ak.stock_zh_a_hist(
            symbol=symbol,
            period='daily',
            start_date=start.strftime('%Y%m%d'),
            end_date=end.strftime('%Y%m%d'),
            adjust='',
        )
        if frame is None or len(frame) == 0:
            return pl.DataFrame()
        renamed = frame.rename(columns={
            '日期': 'date', '开盘': 'open', '最高': 'high', '最低': 'low',
            '收盘': 'close', '成交量': 'volume', '成交额': 'amount',
        })
        columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']
        missing = [name for name in columns if name not in renamed.columns]
        if missing:
            raise ValueError(f'AkShare 返回缺少列: {missing}')
        return pl.from_pandas(renamed[columns])


@dataclass(frozen=True)
class FieldDiff:
    """单标的、单字段的比对统计。"""

    field: str
    status: str  # ok / diff / missing_field
    compared_rows: int
    diff_rows: int
    max_abs_diff: float | None
    max_rel_diff: float | None

    def as_dict(self) -> dict[str, Any]:
        return {
            'field': self.field,
            'field_status': self.status,
            'compared_rows': self.compared_rows,
            'diff_rows': self.diff_rows,
            'max_abs_diff': self.max_abs_diff,
            'max_rel_diff': self.max_rel_diff,
        }


@dataclass
class CodeComparison:
    """单标的、单远程源的比对结果。"""

    ts_code: str
    source: str
    status: str  # matched / diff / unmatched / local_missing / source_error / skipped
    note: str = ''
    local_rows: int = 0
    remote_rows: int = 0
    overlap_rows: int = 0
    missing_local_rows: int = 0
    missing_remote_rows: int = 0
    fields: list[FieldDiff] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            'ts_code': self.ts_code,
            'source': self.source,
            'status': self.status,
            'note': self.note,
            'local_rows': self.local_rows,
            'remote_rows': self.remote_rows,
            'overlap_rows': self.overlap_rows,
            'missing_local_rows': self.missing_local_rows,
            'missing_remote_rows': self.missing_remote_rows,
            'fields': [item.as_dict() for item in self.fields],
        }


@dataclass
class CrossCheckReport:
    """跨源对拍汇总报告。"""

    scope: dict[str, Any]
    elapsed_s: float
    results: list[CodeComparison] = field(default_factory=list)
    report_files: dict[str, str] = field(default_factory=dict)

    def summary(self) -> dict[str, Any]:
        by_source: dict[str, dict[str, int]] = {}
        for result in self.results:
            bucket = by_source.setdefault(
                result.source, {'matched': 0, 'diff': 0, 'unmatched': 0,
                                'local_missing': 0, 'source_error': 0, 'skipped': 0})
            bucket[result.status] = bucket.get(result.status, 0) + 1
        field_diffs = sum(
            item.diff_rows for result in self.results for item in result.fields
        )
        return {
            'elapsed_s': round(self.elapsed_s, 3),
            'codes': len({result.ts_code for result in self.results}),
            'checks': len(self.results),
            'by_source': by_source,
            'field_diff_rows': field_diffs,
        }

    def write(self, output_dir: Path | str = 'logs') -> dict[str, str]:
        target = Path(output_dir)
        target.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        sources = '+'.join(sorted({result.source for result in self.results})) or 'none'
        base = f'cross_source_{sources}_{stamp}'
        json_path = target / f'{base}.json'
        fields_path = target / f'{base}_fields.csv'
        codes_path = target / f'{base}_codes.csv'

        payload = {
            'generated_at': datetime.now().isoformat(timespec='seconds'),
            'scope': self.scope,
            'summary': self.summary(),
            'results': [result.as_dict() for result in self.results],
        }
        json_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )

        fieldnames = [
            'source', 'ts_code', 'code_status', 'note', 'local_rows', 'remote_rows',
            'overlap_rows', 'missing_local_rows', 'missing_remote_rows',
            'field', 'field_status', 'compared_rows', 'diff_rows',
            'max_abs_diff', 'max_rel_diff',
        ]
        with fields_path.open('w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for result in self.results:
                if not result.fields:
                    writer.writerow({
                        'source': result.source,
                        'ts_code': result.ts_code,
                        'code_status': result.status,
                        'note': result.note,
                        'local_rows': result.local_rows,
                        'remote_rows': result.remote_rows,
                        'overlap_rows': result.overlap_rows,
                        'missing_local_rows': result.missing_local_rows,
                        'missing_remote_rows': result.missing_remote_rows,
                        'field': '*',
                        'field_status': result.status,
                        'compared_rows': 0,
                        'diff_rows': 0,
                        'max_abs_diff': None,
                        'max_rel_diff': None,
                    })
                    continue
                for item in result.fields:
                    writer.writerow({
                        'source': result.source,
                        'ts_code': result.ts_code,
                        'code_status': result.status,
                        'note': result.note,
                        'local_rows': result.local_rows,
                        'remote_rows': result.remote_rows,
                        'overlap_rows': result.overlap_rows,
                        'missing_local_rows': result.missing_local_rows,
                        'missing_remote_rows': result.missing_remote_rows,
                        **item.as_dict(),
                    })

        with codes_path.open('w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=['source', 'ts_code', 'status', 'note', 'local_rows',
                            'remote_rows', 'overlap_rows', 'missing_local_rows',
                            'missing_remote_rows'],
            )
            writer.writeheader()
            for result in self.results:
                writer.writerow({
                    'source': result.source,
                    'ts_code': result.ts_code,
                    'status': result.status,
                    'note': result.note,
                    'local_rows': result.local_rows,
                    'remote_rows': result.remote_rows,
                    'overlap_rows': result.overlap_rows,
                    'missing_local_rows': result.missing_local_rows,
                    'missing_remote_rows': result.missing_remote_rows,
                })

        return {
            'json': str(json_path),
            'fields_csv': str(fields_path),
            'codes_csv': str(codes_path),
        }


def compare_frames(local: pl.DataFrame, remote: pl.DataFrame,
                   fields: Sequence[str] = COMPARE_FIELDS,
                   tolerances: dict[str, tuple[float, float]] | None = None
                   ) -> tuple[list[FieldDiff], int, int, int, int]:
    """逐字段比对两个已标准化行情表。

    Returns:
        (fields, overlap_rows, missing_local_rows, missing_remote_rows,
         local_rows)
    """
    tolerances = tolerances or FIELD_TOLERANCES
    local = prepare_frame(local)
    remote = prepare_frame(remote)
    local_rows = local.height
    if 'date' not in local.columns or 'date' not in remote.columns:
        return [], 0, 0, 0, local_rows
    local_dates = set(local['date'].to_list())
    remote_dates = set(remote['date'].to_list())
    missing_local = len(remote_dates - local_dates)
    missing_remote = len(local_dates - remote_dates)

    local_columns = [c for c in ['date', *fields] if c in local.columns]
    remote_columns = [c for c in ['date', *fields] if c in remote.columns]
    joined = (
        local.select(local_columns)
        .join(remote.select(remote_columns), on='date', how='inner', suffix='_remote')
    )
    overlap = joined.height
    if overlap == 0:
        return [], overlap, missing_local, missing_remote, local_rows

    diffs: list[FieldDiff] = []
    for field_name in fields:
        if field_name not in local.columns or field_name not in remote.columns:
            diffs.append(FieldDiff(field_name, 'missing_field', overlap, 0, None, None))
            continue
        rel_tol, abs_tol = tolerances.get(field_name, (1e-6, 0.0))
        left = pl.col(field_name)
        right = pl.col(f'{field_name}_remote')
        both = (
            left.is_not_null() & right.is_not_null()
            & left.is_finite() & right.is_finite()
        )
        difference = (left - right).abs()
        threshold = abs_tol + rel_tol * pl.max_horizontal(left.abs(), right.abs())
        bad = both & (difference > threshold)
        diff_rows = int(joined.filter(bad).height)
        max_abs = joined.filter(both).select(difference.max()).item()
        relative = difference / pl.max_horizontal(
            left.abs(), right.abs(), pl.lit(1e-12))
        max_rel = joined.filter(both).select(relative.max()).item()
        diffs.append(FieldDiff(
            field=field_name,
            status='diff' if diff_rows else 'ok',
            compared_rows=overlap,
            diff_rows=diff_rows,
            max_abs_diff=float(max_abs) if max_abs is not None else None,
            max_rel_diff=float(max_rel) if max_rel is not None else None,
        ))
    return diffs, overlap, missing_local, missing_remote, local_rows


def _stock_codes(store: Any) -> list[str]:
    """股票代码列表；覆盖度索引残缺时按 quotes 目录补齐。"""
    codes = set(store.codes('stock'))
    root = getattr(store, 'root', None)
    if root is not None:
        quotes = Path(root) / 'quotes' / 'stock'
        if quotes.is_dir():
            codes.update(path.stem for path in quotes.glob('*/*.parquet'))
    return sorted(codes)


def _symbol_covers(store: Any, ts_code: str,
                   start: date, end: date) -> bool:
    """判断本地是否有该标的区间数据；覆盖度索引缺失时直接查文件。"""
    coverage = store.coverage('stock', ts_code)
    if coverage is not None:
        return bool(store.covers('stock', ts_code, start, end))
    path = store.quote_path('stock', ts_code)
    if not path.exists():
        return False
    span = pl.scan_parquet(str(path)).select(
        pl.col('date').min().alias('start'),
        pl.col('date').max().alias('end'),
    ).collect()
    if span.is_empty():
        return False
    return span['start'][0] <= end and span['end'][0] >= start


def sample_codes(store: Any, count: int = 40, seed: int = 42,
                 start: date | None = None, end: date | None = None,
                 core_codes: Sequence[str] = (
                     '600000.SH', '600519.SH', '000001.SZ', '000858.SZ',
                     '601318.SH', '600036.SH', '300750.SZ', '002594.SZ',
                 )) -> list[str]:
    """确定性抽样：核心大盘股 + 分层随机补充，仅取沪深股票。"""
    if count < 1:
        raise ValueError('抽样数量必须 >= 1')
    candidates = [
        code for code in _stock_codes(store)
        if not is_index_code(code) and code.endswith(('.SH', '.SZ'))
    ]
    if start is not None or end is not None:
        range_start = start or date(1990, 1, 1)
        range_end = end or date.today()
        candidates = [
            code for code in candidates
            if _symbol_covers(store, code, range_start, range_end)
        ]

    selected: list[str] = []
    for code in core_codes:
        if code in candidates and code not in selected and len(selected) < count:
            selected.append(code)
    remaining = [code for code in candidates if code not in selected]
    if len(selected) < count and remaining:
        rng = random.Random(seed)  # noqa: S311 —— 抽样确定性，非加密用途
        take = min(count - len(selected), len(remaining))
        selected.extend(rng.sample(remaining, take))
    return sorted(selected)


def build_remote_config() -> Any:
    """构造本次进程内临时配置：显式开启远程源，但不改 ``.env`` 默认值。"""
    from src.utils.config import Config  # noqa: PLC0415

    config = Config()
    config.data.allow_remote_sources = True
    return config


def build_adapters(sources: Sequence[str], config: Any) -> dict[str, RemoteAdapter]:
    """按名称构造只读适配器。"""
    adapters: dict[str, RemoteAdapter] = {}
    for source in sources:
        if source == 'baostock':
            adapters[source] = BaostockAdapter(config)
        elif source == 'akshare':
            adapters[source] = AkShareAdapter(config)
        else:
            raise ValueError(f'未知远程源: {source}')
    return adapters


def _compare_one_source(source: str, codes: Sequence[str],
                        local_frames: dict[str, pl.DataFrame | None],
                        adapter: RemoteAdapter, limiter: RateLimiter,
                        start: date, end: date,
                        max_consecutive_empty: int) -> list[CodeComparison]:
    """对单个远程源串行跑完抽样列表（限速 + 熔断）。"""
    results: list[CodeComparison] = []
    consecutive_empty = 0
    skipped = False
    for code in codes:
        local = local_frames.get(code)
        if local is None or local.is_empty():
            results.append(CodeComparison(
                ts_code=code, source=source, status='local_missing',
                note='本地列存无该标的',
            ))
            continue
        if skipped:
            results.append(CodeComparison(
                ts_code=code, source=source, status='skipped',
                note=f'连续空结果达到 {max_consecutive_empty}，提前停止该源',
            ))
            continue

        local_slice = prepare_frame(local).filter(
            (pl.col('date') >= start) & (pl.col('date') <= end))
        if local_slice.is_empty():
            results.append(CodeComparison(
                ts_code=code, source=source, status='local_missing',
                note='本地列存在该区间无数据',
            ))
            continue

        try:
            remote = limiter.call(adapter.fetch, code, start, end)
        except Exception as exc:  # noqa: BLE001
            logger.error(f'{source} {code} 请求失败: {exc}')
            consecutive_empty += 1
            results.append(CodeComparison(
                ts_code=code, source=source, status='source_error',
                note=str(exc)[:200],
                local_rows=local_slice.height,
            ))
            if consecutive_empty >= max_consecutive_empty:
                skipped = True
            continue

        if remote is None or remote.is_empty():
            consecutive_empty += 1
            results.append(CodeComparison(
                ts_code=code, source=source, status='unmatched',
                note='远程源无数据或请求失败（无法对拍）',
                local_rows=local_slice.height,
            ))
            if consecutive_empty >= max_consecutive_empty:
                skipped = True
            continue

        consecutive_empty = 0
        remote_prepared = prepare_frame(remote).filter(
            (pl.col('date') >= start) & (pl.col('date') <= end))
        if remote_prepared.is_empty():
            results.append(CodeComparison(
                ts_code=code, source=source, status='unmatched',
                note='远程源在该区间无数据（无法对拍）',
                local_rows=local_slice.height,
            ))
            continue

        fields, overlap, missing_local, missing_remote, local_rows = compare_frames(
            local_slice, remote_prepared)
        field_diff = any(
            item.diff_rows or item.status == 'missing_field' for item in fields)
        if overlap == 0:
            status, note = 'unmatched', '两源日期无交集（无法对拍）'
        elif field_diff:
            status, note = 'diff', ''
        elif missing_local or missing_remote:
            status = 'diff'
            note = (f'重叠 {overlap} 行数值全部一致，仅日期条目差异：'
                    f'远程多 {missing_local} 行 / 本地多 {missing_remote} 行')
        else:
            status, note = 'matched', ''
        results.append(CodeComparison(
            ts_code=code,
            source=source,
            status=status,
            note=note,
            local_rows=local_rows,
            remote_rows=remote_prepared.height,
            overlap_rows=overlap,
            missing_local_rows=missing_local,
            missing_remote_rows=missing_remote,
            fields=fields,
        ))
    return results


def run_cross_check(store: Any, codes: Sequence[str], start: date, end: date,
                    sources: Sequence[str], adapters: dict[str, RemoteAdapter],
                    limiters: dict[str, RateLimiter] | None = None,
                    max_consecutive_empty: int = 3,
                    dry_run: bool = False,
                    output_dir: Path | str = 'logs') -> CrossCheckReport:
    """执行跨源抽样对拍；只读、只出报告。"""
    if start > end:
        raise ValueError(f'起始日期 {start} 晚于结束日期 {end}')
    limiters = limiters or {
        name: RateLimiter(*RATE_LIMITS[name]) for name in sources
    }
    started = time.perf_counter()
    results: list[CodeComparison] = []

    local_frames: dict[str, pl.DataFrame | None] = {}
    for code in codes:
        local_frames[code] = store.read_file('stock', code)

    for source in sources:
        results.extend(_compare_one_source(
            source, codes, local_frames, adapters[source], limiters[source],
            start, end, max_consecutive_empty))

    elapsed = time.perf_counter() - started
    report = CrossCheckReport(
        scope={
            'sample_size': len(codes),
            'start': str(start),
            'end': str(end),
            'sources': list(sources),
            'fields': list(COMPARE_FIELDS),
            'tolerances': {
                name: {'rel': rel, 'abs': absolute}
                for name, (rel, absolute) in FIELD_TOLERANCES.items()
            },
            'max_concurrency_per_source': MAX_CONCURRENCY,
            'max_consecutive_empty': max_consecutive_empty,
        },
        elapsed_s=elapsed,
        results=results,
    )
    if not dry_run:
        report.report_files = report.write(output_dir)
    return report


def default_date_range(years: int = 3, end: date | None = None) -> tuple[date, date]:
    """默认对拍区间：以最新交易日为终点回溯 N 年。"""
    end_date = end or date.today()
    start_date = end_date - timedelta(days=365 * years)
    return start_date, end_date
