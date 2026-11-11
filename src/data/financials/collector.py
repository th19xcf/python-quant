#!/usr/bin/env python3

"""全 A 股财务三表 + 日频估值的本地采集器（akshare / 东方财富）。

设计要点：
- 三表主体走「按报告期」的全市场批量接口，一页 5,000+ 行、单期 6~8 秒；
- 批量接口的「公告日期」实测是**数据更新日**而非首次公告日，不能用于点时。
  另用东方财富原始 ``RPT_LICO_FN_CPD`` 接口按报告期取 ``NOTICE_DATE``（真公告日），
  缺失时退化为**法定披露截止日**（季报 4/30、半年报 8/31、三季报 10/31、
  年报次年 4/30），保证不会提前可见；
- 北交所无批量利润表/现金流量表，按单票 ``stock_profit_sheet_by_report_em`` /
  ``stock_cash_flow_sheet_by_report_em`` 拉取；资产负债表有 ``stock_zcfz_bj_em``；
- 估值走 ``stock_value_em`` 单票日频（约 2018 年起），线程池 + 全局限速；
- 幂等 + 断点续跑：每期/每票先落 ``parts`` 分片，``progress.json`` 记进度，
  失败写 ``failed.csv``；重跑只补缺口。
"""

from __future__ import annotations

import csv
import json
import math
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import akshare as ak
import polars as pl
import requests
from loguru import logger

from src.data.atomic_write import write_parquet_atomic, write_text_atomic
from src.data.financials import store

EM_DATACENTER_URL = 'https://datacenter-web.eastmoney.com/api/data/v1/get'
REPORT_NAME_YJBB = 'RPT_LICO_FN_CPD'

# 批量接口中文列 → 事实表列
_BULK_MAPPINGS: dict[str, dict[str, str]] = {
    'income': {
        '营业总收入': 'revenue',
        '营业总收入同比': 'revenue_yoy',
        '营业总支出-营业支出': 'operating_cost',
        '营业总支出-销售费用': 'selling_expense',
        '营业总支出-管理费用': 'admin_expense',
        '营业总支出-财务费用': 'finance_expense',
        '营业利润': 'operate_profit',
        '利润总额': 'total_profit',
        '净利润': 'net_profit',
        '净利润同比': 'net_profit_yoy',
    },
    'balance': {
        '资产-货币资金': 'monetary_funds',
        '资产-应收账款': 'accounts_receivable',
        '资产-存货': 'inventory',
        '资产-总资产': 'total_assets',
        '资产-总资产同比': 'total_assets_yoy',
        '负债-应付账款': 'accounts_payable',
        '负债-预收账款': 'advance_receipts',
        '负债-总负债': 'total_liab',
        '负债-总负债同比': 'total_liab_yoy',
        '资产负债率': 'debt_ratio',
        '股东权益合计': 'total_equity',
    },
    'cashflow': {
        '净现金流-净现金流': 'net_cash_flow',
        '净现金流-同比增长': 'net_cash_flow_yoy',
        '经营性现金流-现金流量净额': 'operating_cf',
        '投资性现金流-现金流量净额': 'investing_cf',
        '融资性现金流-现金流量净额': 'financing_cf',
    },
}

_BULK_FUNCS: dict[str, Callable[..., Any]] = {
    'income': ak.stock_lrb_em,
    'balance': ak.stock_zcfz_em,
    'cashflow': ak.stock_xjll_em,
}

_BULK_SOURCES = {
    'income': 'akshare:stock_lrb_em',
    'balance': 'akshare:stock_zcfz_em',
    'cashflow': 'akshare:stock_xjll_em',
}

_VALUATION_MAPPING: dict[str, str] = {
    '数据日期': 'trade_date',
    '当日收盘价': 'close',
    'PE(TTM)': 'pe',
    'PE(静)': 'pe_static',
    '市净率': 'pb',
    '市销率': 'ps',
    '总市值': 'total_mv',
    '流通市值': 'circ_mv',
}


@dataclass(frozen=True)
class RetryPolicy:
    """重试策略（默认值与 ``config.data`` 的出厂设置一致）。"""

    max_retries: int = 3
    delay: float = 1.0
    backoff_factor: float = 2.0
    max_delay: float = 10.0


@dataclass
class CollectResult:
    """单次采集的汇总结果（供 CLI 输出）。"""

    section: str
    rows: int = 0
    units: int = 0
    failed: int = 0
    elapsed_s: float = 0.0
    details: dict[str, Any] = field(default_factory=dict)


class _RateLimiter:
    """线程安全的最小请求间隔控制。"""

    def __init__(self, min_interval: float) -> None:
        self._min_interval = max(0.0, min_interval)
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self) -> None:
        if self._min_interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            sleep_for = self._min_interval - (now - self._last)
            if sleep_for > 0:
                time.sleep(sleep_for)
                now += sleep_for
            self._last = now


class FinancialsCollector:
    """财务事实表采集器（幂等、可断点续跑）。"""

    def __init__(self, store_root: Path | str | None = None, *,
                 workers: int = 6, retry: RetryPolicy | None = None,
                 valuation_interval: float = 0.15,
                 request_interval: float = 0.25) -> None:
        self.store_root = Path(store_root) if store_root is not None else store.DEFAULT_STORE_ROOT
        self.workers = max(1, int(workers))
        self.retry = retry or RetryPolicy()
        self._valuation_limiter = _RateLimiter(valuation_interval)
        self._request_limiter = _RateLimiter(request_interval)
        self._lock = threading.Lock()
        self._progress_lock = threading.Lock()
        self._progress = self._load_progress()

    # ------------------------------------------------------------------
    # 路径与进度
    # ------------------------------------------------------------------
    @property
    def base_dir(self) -> Path:
        return store.financials_dir(self.store_root)

    @property
    def progress_path(self) -> Path:
        return self.base_dir / 'progress.json'

    @property
    def failed_path(self) -> Path:
        return self.base_dir / 'failed.csv'

    def _part_path(self, table: str, name: str) -> Path:
        return self.base_dir / 'parts' / table / f'{name}.parquet'

    def _ann_path(self, period: str) -> Path:
        return self.base_dir / 'ann_date' / f'{period}.parquet'

    def _load_progress(self) -> dict[str, Any]:
        path = self.base_dir / 'progress.json'
        if not path.exists():
            return {'statements': {}, 'ann_dates': [], 'bj_balance': [],
                    'bj_statements': {}, 'valuation': [], 'valuation_ranges': {},
                    'updated_at': None}
        try:
            data = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, ValueError) as exc:
            logger.warning(f'[financials] progress.json 读取失败，按空进度重启: {exc}')
            return {'statements': {}, 'ann_dates': [], 'bj_balance': [],
                    'bj_statements': {}, 'valuation': [], 'valuation_ranges': {},
                    'updated_at': None}
        data.setdefault('statements', {})
        data.setdefault('ann_dates', [])
        data.setdefault('bj_balance', [])
        data.setdefault('bj_statements', {})
        data.setdefault('valuation', [])
        data.setdefault('valuation_ranges', {})
        return data

    def _save_progress(self) -> None:
        with self._progress_lock:
            payload = dict(self._progress)
            payload['updated_at'] = datetime.now().isoformat(timespec='seconds')
            write_text_atomic(
                json.dumps(payload, ensure_ascii=False, indent=2),
                self.progress_path)

    def _mark_done(self, section: str, key: str, sub: str | None = None) -> None:
        """只更新内存进度；落盘由调用方按批触发，避免每票写 JSON。"""
        with self._progress_lock:
            if sub is not None:
                bucket = self._progress.setdefault(section, {}).setdefault(sub, [])
            else:
                bucket = self._progress.setdefault(section, [])
            if key not in bucket:
                bucket.append(key)

    def _record_failure(self, task: str, *, ts_code: str | None = None,
                        period: str | None = None, error: str = '',
                        retries: int = 0, error_type: str = 'Error') -> None:
        row = {
            'time': datetime.now().isoformat(timespec='seconds'),
            'task': task,
            'ts_code': ts_code or '',
            'period': period or '',
            'error_type': error_type,
            'retries': str(retries),
            'message': str(error).replace('\n', ' ')[:500],
        }
        with self._lock:
            self.base_dir.mkdir(parents=True, exist_ok=True)
            exists = self.failed_path.exists()
            with self.failed_path.open('a', encoding='utf-8', newline='') as fh:
                writer = csv.DictWriter(fh, fieldnames=list(row))
                if not exists:
                    writer.writeheader()
                writer.writerow(row)

    # ------------------------------------------------------------------
    # 重试
    # ------------------------------------------------------------------
    @staticmethod
    def _is_retryable(exc: BaseException) -> bool:
        """程序性错误（参数/字段名）不重试，网络与 IO 错误重试。"""
        return not isinstance(exc, (ValueError, KeyError, TypeError, AttributeError))

    def _safe_call(self, task: str, func: Callable[..., Any], *args: Any,
                   ts_code: str | None = None, period: str | None = None,
                   **kwargs: Any) -> tuple[Any | None, str | None, int]:
        """带指数退避的重试；返回 ``(结果, 错误, 重试次数)``。"""
        attempts = 0
        delay = self.retry.delay
        while True:
            try:
                return func(*args, **kwargs), None, attempts
            except Exception as exc:  # noqa: BLE001 - 采集需兜底并落失败清单
                attempts += 1
                if attempts >= self.retry.max_retries or not self._is_retryable(exc):
                    logger.error(f'[financials] {task} 失败（重试 {attempts} 次）: {exc}')
                    self._record_failure(task, ts_code=ts_code, period=period,
                                         error=str(exc), retries=attempts,
                                         error_type=type(exc).__name__)
                    return None, str(exc), attempts
                sleep_for = min(delay, self.retry.max_delay)
                logger.warning(f'[financials] {task} 失败，{sleep_for:.1f}s 后重试 '
                               f'({attempts}/{self.retry.max_retries}): {exc}')
                time.sleep(sleep_for)
                delay *= self.retry.backoff_factor

    # ------------------------------------------------------------------
    # 公告日（真实首次公告日）
    # ------------------------------------------------------------------
    def fetch_ann_date_map(self, period_end: date, *, force: bool = False,
                           ) -> dict[str, tuple[date, date | None, float | None]]:
        """按报告期拉取 ``SECURITY_CODE -> (NOTICE_DATE, UPDATE_DATE, 归母净利润)``。

        结果落 ``ann_date/{period}.parquet``，重跑直接复用；``force=True`` 时
        重新拉取（用于补齐旧缓存缺失的归母净利润列）。
        """
        period = period_end.strftime('%Y-%m-%d')
        path = self._ann_path(period_end.strftime('%Y%m%d'))
        if path.exists():
            frame = pl.read_parquet(path)
            has_parent = 'parent_net_profit' in frame.columns
            # 旧缓存可能缺归母净利润列，此时即使未 force 也要重拉补齐
            if not force and has_parent:
                return {
                    str(row['symbol']): (
                        row['ann_date'], row['update_date'],
                        row.get('parent_net_profit'))
                    for row in frame.to_dicts()
                }

        rows: list[dict[str, Any]] = []
        page = 1
        pages = 1
        while page <= pages:
            def _fetch(p: int = page) -> dict[str, Any]:
                self._request_limiter.wait()
                params = {
                    'reportName': REPORT_NAME_YJBB,
                    'columns': 'SECURITY_CODE,NOTICE_DATE,UPDATE_DATE,PARENT_NETPROFIT',
                    'pageSize': '500',
                    'pageNumber': str(p),
                    'filter': f"(REPORTDATE='{period}')",
                }
                resp = requests.get(EM_DATACENTER_URL, params=params, timeout=20)
                resp.raise_for_status()
                payload = resp.json()
                if not payload.get('success', True):
                    raise RuntimeError(f'东方财富接口返回失败: {payload.get("message")}')
                return payload

            payload, error, _ = self._safe_call(
                'ann_date', _fetch, period=period_end.strftime('%Y%m%d'))
            if payload is None:
                logger.error(f'[financials] {period} 公告日映射拉取失败: {error}')
                return {}
            result = payload.get('result') or {}
            pages = int(result.get('pages') or 1)
            rows.extend(result.get('data') or [])
            page += 1

        if not rows:
            logger.warning(f'[financials] {period} 公告日映射为空')
            return {}

        frame = pl.DataFrame({
            'symbol': [str(r.get('SECURITY_CODE') or '').zfill(6) for r in rows],
            'ann_date': [store.as_date(r.get('NOTICE_DATE')) for r in rows],
            'update_date': [store.as_date(r.get('UPDATE_DATE')) for r in rows],
            'parent_net_profit': [to_float(r.get('PARENT_NETPROFIT')) for r in rows],
        })
        frame = frame.filter(pl.col('symbol') != '').unique(
            subset=['symbol'], keep='last').sort('symbol')
        write_parquet_atomic(frame, path, compression='zstd', statistics=True)
        self._mark_done('ann_dates', period_end.strftime('%Y%m%d'))
        self._save_progress()
        logger.info(f'[financials] 公告日映射 {period}: {frame.height:,} 只')
        return {
            str(row['symbol']): (row['ann_date'], row['update_date'],
                                 row['parent_net_profit'])
            for row in frame.to_dicts()
        }

    # ------------------------------------------------------------------
    # 三表（批量）
    # ------------------------------------------------------------------
    def collect_statements(self, periods: Sequence[date],
                           symbols: dict[str, str], *, force: bool = False) -> CollectResult:
        """采集全市场利润表 / 资产负债表 / 现金流量表（不含北交所两张表）。"""
        started = time.perf_counter()
        result = CollectResult(section='statements')
        for period_end in periods:
            period = period_end.strftime('%Y%m%d')
            ann_map = self.fetch_ann_date_map(period_end)
            for table in store.STATEMENTS:
                part = self._part_path(table, period)
                if part.exists() and not force:
                    result.rows += pl.scan_parquet(str(part)).select(pl.len()).collect().item()
                    continue
                func = _BULK_FUNCS[table]
                df, error, retries = self._safe_call(
                    f'{table}:{period}', func, date=period, period=period)
                if df is None:
                    result.failed += 1
                    continue
                try:
                    frame = normalize_bulk_statement(
                        table, df, period_end, ann_map, symbols,
                        source=_BULK_SOURCES[table])
                    self._write_part(table, period, frame)
                    result.rows += frame.height
                    self._mark_done('statements', period, sub=table)
                # 兜底理由：单期结构变化不得中断整轮采集，记失败后继续
                except Exception as exc:  # noqa: BLE001
                    logger.error(f'[financials] {table}:{period} 规范化失败: {exc}')
                    self._record_failure(f'{table}:{period}', period=period,
                                         error=str(exc), retries=retries)
                    result.failed += 1
                result.units += 1
                self._save_progress()
            logger.info(f'[financials] 报告期 {period_end} 三表完成')
        result.elapsed_s = round(time.perf_counter() - started, 2)
        return result

    def collect_bj_balance(self, periods: Sequence[date],
                           symbols: dict[str, str], *, force: bool = False) -> CollectResult:
        """北交所资产负债表（``stock_zcfz_bj_em``，按报告期批量）。"""
        started = time.perf_counter()
        result = CollectResult(section='bj_balance')
        for period_end in periods:
            period = period_end.strftime('%Y%m%d')
            part = self._part_path('balance_bj', period)
            if part.exists() and not force:
                result.rows += pl.scan_parquet(str(part)).select(pl.len()).collect().item()
                continue
            ann_map = self.fetch_ann_date_map(period_end)
            df, error, retries = self._safe_call(
                f'balance_bj:{period}', ak.stock_zcfz_bj_em,
                date=period, period=period)
            if df is None:
                result.failed += 1
                continue
            try:
                frame = normalize_bulk_statement(
                    'balance', df, period_end, ann_map, symbols,
                    source='akshare:stock_zcfz_bj_em')
                self._write_part('balance_bj', period, frame)
                result.rows += frame.height
            # 兜底理由：单期失败不阻塞其余报告期
            except Exception as exc:  # noqa: BLE001
                logger.error(f'[financials] 北交所资产负债表 {period} 失败: {exc}')
                self._record_failure(f'balance_bj:{period}', period=period,
                                     error=str(exc), retries=retries)
                result.failed += 1
            result.units += 1
            self._mark_done('bj_balance', period)
            self._save_progress()
        result.elapsed_s = round(time.perf_counter() - started, 2)
        return result

    def collect_bj_statements(self, codes: Sequence[str], *,
                              start_period: date) -> CollectResult:
        """北交所利润表 / 现金流量表（逐票全历史）。"""
        started = time.perf_counter()
        result = CollectResult(section='bj_statements')
        tasks: list[tuple[str, str]] = []
        done_map: dict[str, list[str]] = self._progress.get('bj_statements', {})
        for table in ('income', 'cashflow'):
            done = set(done_map.get(table, []))
            for code in codes:
                if code not in done:
                    tasks.append((table, code))

        funcs = {
            'income': ak.stock_profit_sheet_by_report_em,
            'cashflow': ak.stock_cash_flow_sheet_by_report_em,
        }

        def _one(table: str, code: str) -> tuple[str, str, pl.DataFrame | None, str | None]:
            symbol = f'BJ{code.split(".")[0]}'
            self._request_limiter.wait()
            df, error, retries = self._safe_call(
                f'{table}_bj', funcs[table], ts_code=code, period='ALL',
                symbol=symbol)
            if df is None:
                return table, code, None, error
            try:
                frame = normalize_bj_statement(table, df, code, start_period)
                return table, code, frame, None
            # 兜底理由：单票字段缺失记失败，不中断整池
            except Exception as exc:  # noqa: BLE001
                self._record_failure(f'{table}_bj', ts_code=code, period='ALL',
                                     error=str(exc), retries=retries)
                return table, code, None, str(exc)

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(_one, table, code) for table, code in tasks]
            for future in as_completed(futures):
                table, code, frame, error = future.result()
                if frame is None:
                    result.failed += 1
                    continue
                self._write_part(f'{table}_bj', code, frame)
                result.rows += frame.height
                result.units += 1
                self._mark_done('bj_statements', code, sub=table)
                if result.units % 20 == 0:
                    self._save_progress()
        self._save_progress()
        result.elapsed_s = round(time.perf_counter() - started, 2)
        return result

    # ------------------------------------------------------------------
    # 归母净利润回补（批量接口本身不含该列）
    # ------------------------------------------------------------------
    def backfill_parent_net_profit(self, periods: Sequence[date], *,
                                   force: bool = False) -> CollectResult:
        """用东方财富业绩报表接口回补利润表的 ``parent_net_profit``。

        只重写已有分片，不触发整表重采；默认复用 ``ann_date`` 缓存，
        ``force=True`` 时重新拉取。
        """
        started = time.perf_counter()
        result = CollectResult(section='parent_net_profit')
        for period_end in periods:
            period = period_end.strftime('%Y%m%d')
            part = self._part_path('income', period)
            if not part.exists():
                continue
            mapping = self.fetch_ann_date_map(period_end, force=force)
            if not mapping:
                result.failed += 1
                continue
            frame = pl.read_parquet(part)
            # 分片落盘后是 ts_code（如 600519.SH），接口映射键是 6 位 symbol
            if 'ts_code' not in frame.columns:
                continue
            frame = frame.with_columns(
                pl.col('ts_code').str.split('.').list.first()
                .map_elements(_parent_lookup(mapping), return_dtype=pl.Float64)
                .alias('parent_net_profit'))
            self._write_part('income', period, frame)
            result.rows += frame.height
            result.units += 1
            filled = frame['parent_net_profit'].is_not_null().sum()
            logger.info(f'[financials] 回补归母净利润 {period}: '
                        f'{filled:,}/{frame.height:,}')
        self._save_progress()
        result.elapsed_s = round(time.perf_counter() - started, 2)
        return result

    # ------------------------------------------------------------------
    # 日频估值
    # ------------------------------------------------------------------
    def _valuation_resume_state(self, since: date | None, until: date | None,
                                ) -> tuple[str | None, str, set[str]]:
        """返回 ``(区间键, 分片前缀, 已完成代码)``；区间模式下只认同区间的分片。"""
        batch_dir = store.parts_dir('valuation', self.store_root)
        if since is None:
            range_key, prefix = None, 'batch_'
            done = set(self._progress.get('valuation', []))
            source_files = sorted(batch_dir.glob('*.parquet'))
        elif until is None:
            raise ValueError('until 需要与 since 一起使用')
        else:
            range_key = f'{since.isoformat()}_{until.isoformat()}'
            prefix = f'incr_{since:%Y%m%d}_'
            ranges = self._progress.get('valuation_ranges', {})
            if range_key not in ranges:
                # 只保留当前区间的断点桶，避免 progress.json 随时间无限增长；
                # 历史区间的分片本身（incr_YYYYMMDD_*.parquet）仍是可恢复依据
                ranges = {range_key: []}
                self._progress['valuation_ranges'] = ranges
            done = set(ranges.get(range_key, []))
            source_files = sorted(batch_dir.glob(f'{prefix}*.parquet'))
        for path in source_files:
            try:
                done.update(pl.read_parquet(path, columns=['ts_code'])
                            ['ts_code'].unique().to_list())
            except (OSError, pl.exceptions.PolarsError) as exc:
                logger.warning(f'[financials] 估值分片读取失败 {path}: {exc}')
        return range_key, prefix, done

    def _mark_valuation_done(self, code: str, range_key: str | None) -> None:
        if range_key is None:
            self._mark_done('valuation', code)
        else:
            self._mark_done('valuation_ranges', code, sub=range_key)

    @staticmethod
    def _clip_valuation(frame: pl.DataFrame, since: date | None,
                        until: date | None) -> pl.DataFrame:
        """区间模式下只保留 ``since <= trade_date <= until``。"""
        if since is None:
            return frame
        clipped = frame.filter(pl.col('trade_date') >= since)
        return clipped if until is None else clipped.filter(pl.col('trade_date') <= until)

    def collect_valuation(self, codes: Sequence[str], *,
                          force: bool = False, batch_size: int = 100,
                          since: date | None = None,
                          until: date | None = None) -> CollectResult:
        """采集单票日频估值（``stock_value_em``），按批写分片。

        两种模式互斥：

        - 默认 / ``force=True``：每票落全历史；``force`` 额外忽略断点重采全部代码；
        - ``since``（可配 ``until``）：区间增量，只落 ``since <= trade_date <= until``
          的行，断点记在 ``progress.json`` 的 ``valuation_ranges`` 下（按区间分桶），
          已补完该区间的代码直接跳过，中断后重跑不会重写全历史。
        """
        if force and since is not None:
            raise ValueError('force（全历史重写）与 since（区间增量）互斥')
        if until is not None and since is None:
            raise ValueError('until 需要与 since 一起使用')
        if since is not None:
            until = until or date.today()
            if since > until:
                raise ValueError(f'since({since}) 不能晚于 until({until})')

        started = time.perf_counter()
        result = CollectResult(section='valuation')
        batch_dir = store.parts_dir('valuation', self.store_root)
        batch_dir.mkdir(parents=True, exist_ok=True)
        range_key, prefix, done = self._valuation_resume_state(since, until)
        existing_batches = len(list(batch_dir.glob(f'{prefix}*.parquet')))
        pending = [c for c in codes if force or c not in done]

        dividends = self._load_dividends()
        buffer: list[pl.DataFrame] = []
        batch_no = existing_batches

        def _one(code: str) -> tuple[str, pl.DataFrame | None, str | None]:
            symbol = code.split('.')[0]
            self._valuation_limiter.wait()
            df, error, retries = self._safe_call(
                'valuation', ak.stock_value_em, ts_code=code, symbol=symbol)
            if df is None:
                return code, None, error
            try:
                frame = normalize_valuation(df, code, dividends)
                return code, self._clip_valuation(frame, since, until), None
            # 兜底理由：单票字段异常记失败，不中断线程池
            except Exception as exc:  # noqa: BLE001
                self._record_failure('valuation', ts_code=code, period='ALL',
                                     error=str(exc), retries=retries)
                return code, None, str(exc)

        def _flush() -> None:
            nonlocal buffer, batch_no
            if not buffer:
                return
            frame = pl.concat(buffer, how='vertical_relaxed')
            batch_no += 1
            path = batch_dir / f'{prefix}{batch_no:04d}.parquet'
            write_parquet_atomic(frame, path, compression='zstd', statistics=True)
            logger.info(f'[financials] 估值分片 {path.name}: {frame.height:,} 行')
            buffer = []
            self._save_progress()

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(_one, code) for code in pending]
            for future in as_completed(futures):
                code, frame, error = future.result()
                result.units += 1
                if frame is None:
                    result.failed += 1
                    continue
                result.rows += frame.height
                if frame.height:
                    buffer.append(frame)
                self._mark_valuation_done(code, range_key)
                if len(buffer) >= batch_size:
                    _flush()
        _flush()
        self._save_progress()
        result.elapsed_s = round(time.perf_counter() - started, 2)
        result.details['codes'] = len(pending)
        if range_key is not None:
            result.details['range'] = range_key
        return result

    def _write_part(self, table: str, name: str, frame: pl.DataFrame) -> None:
        """原子写分片（自动对齐 schema；空帧也落盘以标记完成）。"""
        aligned = store.coerce_schema(frame, table.split('_')[0])
        path = self._part_path(table, name)
        write_parquet_atomic(aligned, path, compression='zstd', statistics=True)

    def _load_dividends(self) -> dict[str, pl.DataFrame] | None:
        """读取本地分红快照，用于计算股息率（只读，不修改该文件）。

        返回 ``None`` 表示快照整体不可用（此时 ``dv_ratio`` 保持为空，避免把
        「缺数据」误写成 0）；快照可用但某标的无分红记录时由调用方按空表处理，
        股息率记为 0。
        """
        path = Path(self.store_root) / 'factors' / 'stock_dividend.parquet'
        if not path.exists():
            logger.warning(f'[financials] 分红快照不存在，dv_ratio 将为空: {path}')
            return None
        try:
            frame = pl.read_parquet(
                path, columns=['ts_code', 'ex_date', 'cash_div']).filter(
                pl.col('ex_date').is_not_null() & (pl.col('cash_div') > 0))
            return {str(group['ts_code'][0]): group
                    for group in frame.partition_by('ts_code', maintain_order=True)}
        except (OSError, pl.exceptions.PolarsError) as exc:
            logger.warning(f'[financials] 分红快照读取失败，dv_ratio 将为空: {exc}')
            return None

    # ------------------------------------------------------------------
    # 合并
    # ------------------------------------------------------------------
    def merge_all(self) -> dict[str, int]:
        """把全部分片合并为四张最终事实表。"""
        stats: dict[str, int] = {}
        for table in store.STATEMENTS:
            extra = [store.parts_dir(f'{table}_bj', self.store_root)]
            stats[table] = store.merge_parts(table, self.store_root, extra_dirs=extra)
        stats['valuation'] = store.merge_parts('valuation', self.store_root)
        return stats


# ----------------------------------------------------------------------
# 规范化（纯函数，便于单测）
# ----------------------------------------------------------------------
def statutory_deadline(period_end: date) -> date:
    """法定披露截止日（保守可见日）：季报 4/30、半年报 8/31、三季报 10/31。"""
    if period_end.month == 3:
        return date(period_end.year, 4, 30)
    if period_end.month == 6:
        return date(period_end.year, 8, 31)
    if period_end.month == 9:
        return date(period_end.year, 10, 31)
    return date(period_end.year + 1, 4, 30)


def infer_ts_code(symbol: str) -> str:
    """把 6 位代码推断成标准 ``ts_code``（兜底用）。"""
    code = str(symbol).zfill(6)
    if code.startswith(('4', '8', '9')):
        return f'{code}.BJ'
    if code.startswith('6'):
        return f'{code}.SH'
    return f'{code}.SZ'


def periods_between(start: date, end: date) -> list[date]:
    """返回 ``[start, end]`` 区间内的所有报告期末（按季）。"""
    period = date(start.year, ((start.month - 1) // 3) * 3 + 3, 1)
    if period.month == 3:
        period = date(period.year, 3, 31)
    elif period.month == 6:
        period = date(period.year, 6, 30)
    elif period.month == 9:
        period = date(period.year, 9, 30)
    else:
        period = date(period.year, 12, 31)
    periods: list[date] = []
    while period <= end:
        periods.append(period)
        if period.month == 12:
            period = date(period.year + 1, 3, 31)
        elif period.month == 3:
            period = date(period.year, 6, 30)
        elif period.month == 6:
            period = date(period.year, 9, 30)
        else:
            period = date(period.year, 12, 31)
    return periods


def to_float(value: Any) -> float | None:
    """把接口返回值转 float（空值/NaN 返回 None）。"""
    if value is None or value == '' or value == '-':
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(number) else number


def normalize_bulk_statement(table: str, df: Any, period_end: date,
                             ann_map: dict[str, tuple[date, date | None, float | None]],
                             symbols: dict[str, str], *, source: str,
                             fetched_at: datetime | None = None) -> pl.DataFrame:
    """把 ``stock_lrb_em`` 等批量返回规范化为事实表 schema。"""
    if df is None or len(df) == 0:
        return pl.DataFrame(schema=store.SCHEMAS[table])
    frame = pl.from_pandas(df)
    if '股票代码' not in frame.columns:
        raise ValueError(f'{table} 批量返回缺少「股票代码」列: {frame.columns}')
    symbol_col = (pl.col('股票代码').cast(pl.Utf8, strict=False)
                  .str.zfill(6))
    exprs: list[pl.Expr] = [
        symbol_col.alias('symbol'),
        pl.col('公告日期').cast(pl.Utf8, strict=False).str.slice(0, 10)
        .str.to_date('%Y-%m-%d', strict=False).alias('_source_ann_date')
        if '公告日期' in frame.columns else pl.lit(None, dtype=pl.Date).alias('_source_ann_date'),
    ]
    for src, dst in _BULK_MAPPINGS[table].items():
        if src in frame.columns:
            exprs.append(pl.col(src).cast(pl.Float64, strict=False).alias(dst))
        else:
            exprs.append(pl.lit(None, dtype=pl.Float64).alias(dst))
    frame = frame.with_columns(exprs)
    frame = frame.with_columns([
        pl.col('symbol').replace_strict(symbols, default=None, return_dtype=pl.Utf8)
        .fill_null(pl.col('symbol').map_elements(infer_ts_code, return_dtype=pl.Utf8))
        .alias('ts_code'),
        pl.lit(period_end).alias('end_date'),
        pl.lit(source).alias('source'),
        pl.lit(fetched_at or datetime.now()).alias('fetched_at'),
    ])
    if table == 'income':
        frame = frame.with_columns(
            (pl.col('revenue') - pl.col('operating_cost')).alias('gross_profit'))
    frame = frame.with_columns(_ann_date_expr(ann_map)).unnest('_ann_info')
    if table == 'income':
        frame = frame.with_columns(
            pl.col('symbol').map_elements(
                _parent_lookup(ann_map), return_dtype=pl.Float64)
            .alias('parent_net_profit'))
    return store.coerce_schema(frame, table)


def _parent_lookup(
        ann_map: dict[str, tuple[date, date | None, float | None]]) -> Callable[[Any], float | None]:
    def _lookup(symbol: Any) -> float | None:
        mapped = ann_map.get(str(symbol)) if symbol is not None else None
        return mapped[2] if mapped else None
    return _lookup


def _ann_date_expr(
        ann_map: dict[str, tuple[date, date | None, float | None]]) -> pl.Expr:
    """按 symbol 取真实公告日，缺失/早于报告期时退化为法定截止日。"""
    return pl.struct(['symbol', 'end_date']).map_elements(
        _ann_lookup(ann_map), return_dtype=pl.Struct({
            'ann_date': pl.Date,
            'ann_date_source': pl.Utf8,
        })).alias('_ann_info')


def _ann_lookup(
        ann_map: dict[str, tuple[date, date | None, float | None]]
        ) -> Callable[[Any], dict[str, Any]]:
    def _lookup(payload: dict[str, Any]) -> dict[str, Any]:
        symbol = payload.get('symbol')
        period_end = payload.get('end_date')
        mapped = ann_map.get(str(symbol)) if symbol is not None else None
        ann = mapped[0] if mapped else None
        if period_end is None:
            return {'ann_date': None, 'ann_date_source': 'statutory_deadline'}
        if ann is not None and ann >= period_end:
            return {'ann_date': ann, 'ann_date_source': 'em_notice'}
        return {'ann_date': statutory_deadline(period_end),
                'ann_date_source': 'statutory_deadline'}
    return _lookup


def normalize_bj_statement(table: str, df: Any, ts_code: str,
                           start_period: date,
                           fetched_at: datetime | None = None) -> pl.DataFrame:
    """把北交所单票三表规范化为事实表 schema。"""
    if df is None or len(df) == 0:
        return pl.DataFrame(schema=store.SCHEMAS[table])
    frame = pl.from_pandas(df)
    end_col = 'REPORT_DATE' if 'REPORT_DATE' in frame.columns else None
    if end_col is None:
        raise ValueError(f'{table} 单票返回缺少 REPORT_DATE: {frame.columns}')
    exprs: list[pl.Expr] = [
        pl.col('REPORT_DATE').cast(pl.Utf8, strict=False).str.slice(0, 10)
        .str.to_date('%Y-%m-%d', strict=False).alias('end_date'),
    ]
    if table == 'income':
        mapping = {
            'TOTAL_OPERATE_INCOME': 'revenue',
            'TOTAL_OPERATE_INCOME_YOY': 'revenue_yoy',
            'OPERATE_COST': 'operating_cost',
            'SALE_EXPENSE': 'selling_expense',
            'MANAGE_EXPENSE': 'admin_expense',
            'FINANCE_EXPENSE': 'finance_expense',
            'OPERATE_PROFIT': 'operate_profit',
            'TOTAL_PROFIT': 'total_profit',
            'NETPROFIT': 'net_profit',
            'NETPROFIT_YOY': 'net_profit_yoy',
            'PARENT_NETPROFIT': 'parent_net_profit',
        }
    else:
        mapping = {
            'CCE_ADD': 'net_cash_flow',
            'CCE_ADD_YOY': 'net_cash_flow_yoy',
            'NETCASH_OPERATE': 'operating_cf',
            'NETCASH_INVEST': 'investing_cf',
            'NETCASH_FINANCE': 'financing_cf',
        }
    for src, dst in mapping.items():
        exprs.append(
            pl.col(src).cast(pl.Float64, strict=False).alias(dst)
            if src in frame.columns else pl.lit(None, dtype=pl.Float64).alias(dst))
    frame = frame.with_columns(exprs)
    frame = frame.filter(pl.col('end_date') >= start_period)
    ann_col = (pl.col('NOTICE_DATE').cast(pl.Utf8, strict=False).str.slice(0, 10)
               .str.to_date('%Y-%m-%d', strict=False)
               if 'NOTICE_DATE' in frame.columns else pl.lit(None, dtype=pl.Date))
    frame = frame.with_columns([
        ann_col.alias('_notice'),
        pl.lit(ts_code).alias('ts_code'),
        pl.lit(f'akshare:stock_{"profit" if table == "income" else "cash_flow"}_sheet_by_report_em')
        .alias('source'),
        pl.lit(fetched_at or datetime.now()).alias('fetched_at'),
    ])
    valid = pl.col('_notice').is_not_null() & (pl.col('_notice') >= pl.col('end_date'))
    frame = frame.with_columns(
        pl.when(valid).then(pl.col('_notice'))
        .otherwise(pl.col('end_date').map_elements(statutory_deadline, return_dtype=pl.Date))
        .alias('ann_date'),
        pl.when(valid).then(pl.lit('em_notice')).otherwise(pl.lit('statutory_deadline'))
        .alias('ann_date_source'),
    )
    if table == 'income':
        frame = frame.with_columns(
            (pl.col('revenue') - pl.col('operating_cost')).alias('gross_profit'))
    return store.coerce_schema(frame, table)


def normalize_valuation(df: Any, ts_code: str,
                        dividends: dict[str, pl.DataFrame] | None = None,
                        fetched_at: datetime | None = None) -> pl.DataFrame:
    """把 ``stock_value_em`` 返回规范化为日频估值表（含股息率）。"""
    if df is None or len(df) == 0:
        return pl.DataFrame(schema=store.SCHEMAS['valuation'])
    frame = pl.from_pandas(df)
    if '数据日期' not in frame.columns:
        raise ValueError(f'估值返回缺少「数据日期」: {frame.columns}')
    exprs: list[pl.Expr] = [
        pl.col('数据日期').cast(pl.Utf8, strict=False).str.slice(0, 10)
        .str.to_date('%Y-%m-%d', strict=False).alias('trade_date'),
    ]
    for src, dst in _VALUATION_MAPPING.items():
        if src == '数据日期':
            continue
        exprs.append(
            pl.col(src).cast(pl.Float64, strict=False).alias(dst)
            if src in frame.columns else pl.lit(None, dtype=pl.Float64).alias(dst))
    frame = frame.with_columns(exprs).filter(pl.col('trade_date').is_not_null())
    frame = frame.with_columns([
        pl.lit(ts_code).alias('ts_code'),
        pl.lit('akshare:stock_value_em').alias('source'),
        pl.lit(fetched_at or datetime.now()).alias('fetched_at'),
    ])
    if dividends is None:
        divs = None
    else:
        divs = dividends.get(ts_code)
        if divs is None:
            # 快照可用但该标的无任何分红记录：视为零股息，而不是缺数据
            divs = pl.DataFrame(schema={'ex_date': pl.Date, 'cash_div': pl.Float64})
    frame = _attach_dividend_yield(frame, divs)
    return store.coerce_schema(frame, 'valuation')


def _attach_dividend_yield(frame: pl.DataFrame,
                           divs: pl.DataFrame | None) -> pl.DataFrame:
    """用本地分红快照计算滚动 12 个月股息率（百分数，按除息日计）。

    ``divs=None`` 表示分红快照整体不可用，结果保持为空；``divs`` 为空表表示
    该标的确认无分红记录，股息率记 0；有记录但近 365 天无分红时同样记 0。
    """
    if divs is None:
        return frame.with_columns(pl.lit(None, dtype=pl.Float64).alias('dv_ratio'))
    if divs.is_empty():
        return frame.with_columns(pl.lit(0.0, dtype=pl.Float64).alias('dv_ratio'))
    divs = divs.sort('ex_date').with_columns(
        pl.col('cash_div').cum_sum().alias('_cum_div'))
    left = frame.sort('trade_date')
    left = left.join_asof(
        divs.select([pl.col('ex_date'), pl.col('_cum_div')]).sort('ex_date'),
        left_on='trade_date', right_on='ex_date', strategy='backward')
    left = left.with_columns(
        (pl.col('trade_date') - pl.duration(days=365)).alias('_window_start'))
    left = left.join_asof(
        divs.select([pl.col('ex_date').alias('_window_start'),
                     pl.col('_cum_div').alias('_cum_before')])
        .sort('_window_start'),
        on='_window_start', strategy='backward')
    ratio = ((pl.col('_cum_div') - pl.col('_cum_before').fill_null(0.0))
             / pl.col('close').clip(1e-9) * 100)
    result = left.with_columns(
        pl.when(pl.col('_cum_div').is_not_null()).then(ratio)
        .otherwise(0.0).alias('dv_ratio'))
    return result.select(frame.columns + ['dv_ratio'])
