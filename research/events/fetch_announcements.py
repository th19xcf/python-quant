#!/usr/bin/env python3

"""抓取公告与业绩预告样本（仅公开接口、无凭证、限速、可复现）。

数据源（二选一，``--source``）：
- ``cninfo``（默认）：巨潮资讯网 ``hisAnnouncement/query``，交易所披露口径，
  ``announcementTime`` 为**毫秒 epoch**，可还原到秒级的真实发布时间；
- ``eastmoney``：东方财富公告 API，原始响应含 ``display_time``（毫秒）或
  ``eiTime``（秒），但历史数据两者可能为空/偏移，且高频访问会被 567 拦截。

注意：东方财富的 ``notice_date`` 常比真实发布时间晚 1 个自然日
（例如真实发布 2025-12-30 18:24，``notice_date=2025-12-31``），
因此事件研究必须使用 ``display_time``，不能用公告日期。

产出：
- ``research/events/data/announcements_raw.parquet``
- ``research/events/data/yjyg_raw.parquet``
- ``research/events/data/fetch_meta.json``

本脚本只用于可行性试点，不接入生产推荐流程。
"""

from __future__ import annotations

import argparse
import calendar
import html
import json
import math
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import requests
from loguru import logger

DATA_DIR = Path(__file__).resolve().parent / 'data'
EM_API = 'https://np-anotice-stock.eastmoney.com/api/security/ann'
CNINFO_API = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
DEFAULT_KEYWORDS = ('减持', '增持', '股权激励', '中标', '重大合同', '处罚', '立案', '重组', '并购', '分红')
PAGE_SIZE = 30
MAX_PAGES_PER_CHUNK = 400
WORKERS = 2
REQUEST_SLEEP = 0.25
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Referer': 'http://www.cninfo.com.cn/new/commonUrl?url=disclosure/list/notice',
    'X-Requested-With': 'XMLHttpRequest',
}
_THREAD_LOCAL = threading.local()


def _session() -> requests.Session:
    session = getattr(_THREAD_LOCAL, 'session', None)
    if session is None:
        session = requests.Session()
        session.headers.update(HEADERS)
        _THREAD_LOCAL.session = session
    return session


def _clean_title(text: str) -> str:
    return html.unescape(re.sub(r'<[^>]+>', '', text or '')).strip()


def _quarter_chunks(start: str, end: str) -> list[tuple[str, str]]:
    """按自然季度切分日期区间，控制单块页数并降低被限流概率。"""
    first = date.fromisoformat(start)
    last = date.fromisoformat(end)
    chunks: list[tuple[str, str]] = []
    cursor = date(first.year, ((first.month - 1) // 3) * 3 + 1, 1)
    while cursor <= last:
        month_end = cursor.month + 2
        chunk_end = date(cursor.year, month_end, calendar.monthrange(cursor.year, month_end)[1])
        if chunk_end > last:
            chunk_end = last
        chunks.append((max(cursor, first).isoformat(), chunk_end.isoformat()))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def _cninfo_get(params: dict[str, Any], retries: int = 6) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = _session().post(CNINFO_API, data=params, timeout=30)
            if response.status_code == 403:
                time.sleep(20 * (attempt + 1))
                last_error = requests.HTTPError('403 Forbidden')
                continue
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # noqa: BLE001 - 网络层重试必须兜底
            last_error = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f'巨潮公告接口请求失败: {last_error}')


def _cninfo_records(payload: dict[str, Any], keyword: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for item in payload.get('announcements') or []:
        stock_code = str(item.get('secCode', ''))
        if not stock_code.isdigit() or len(stock_code) != 6:
            continue
        stamp = item.get('announcementTime')
        published = datetime.fromtimestamp(stamp / 1000) if isinstance(stamp, (int, float)) else None
        records.append({
            'keyword': keyword,
            'art_code': str(item.get('announcementId', '')),
            'stock_code': stock_code,
            'short_name': item.get('secName', ''),
            'title': _clean_title(item.get('announcementTitle', '')),
            'column_name': '',
            'display_time': published,
            'notice_date': published.date().isoformat() if published else '',
            'url': f'http://static.cninfo.com.cn/{item.get("adjunctUrl", "")}',
        })
    return records


def fetch_cninfo_chunk(keyword: str, start: str, end: str) -> tuple[list[dict[str, Any]], int]:
    params = {
        'pageNum': '1', 'pageSize': str(PAGE_SIZE), 'column': 'szse', 'tabName': 'fulltext',
        'plate': '', 'stock': '', 'searchkey': keyword, 'secid': '', 'category': '', 'trade': '',
        'seDate': f'{start}~{end}', 'sortName': '', 'sortType': '', 'isHLtitle': 'true',
    }
    first = _cninfo_get(params)
    total = int(first.get('totalRecordNum') or 0)
    pages = min(MAX_PAGES_PER_CHUNK, math.ceil(total / PAGE_SIZE))
    records = _cninfo_records(first, keyword)
    if pages > 1:
        def _page(page: int) -> list[dict[str, Any]]:
            page_params = dict(params)
            page_params['pageNum'] = str(page)
            time.sleep(REQUEST_SLEEP)
            return _cninfo_records(_cninfo_get(page_params), keyword)

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = [pool.submit(_page, page) for page in range(2, pages + 1)]
            for future in as_completed(futures):
                records.extend(future.result())
    return records, total


def _em_get(params: dict[str, Any], retries: int = 5) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = _session().get(EM_API, params=params, timeout=25)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # noqa: BLE001 - 网络层重试必须兜底
            last_error = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f'东财公告接口请求失败: {last_error}')


def _parse_display_time(text: str) -> datetime | None:
    if not text:
        return None
    normalized = text.strip()
    if normalized.count(':') >= 3:
        head, _, millis = normalized.rpartition(':')
        normalized = f'{head}.{millis}'
    for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _em_records(payload: dict[str, Any], keyword: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for item in payload.get('data', {}).get('list', []):
        codes = [c for c in item.get('codes', []) if str(c.get('ann_type', '')).startswith('A')]
        columns = item.get('columns') or [{}]
        published = _parse_display_time(item.get('display_time', '') or item.get('eiTime', ''))
        for code in codes:
            stock_code = str(code.get('stock_code', ''))
            if not stock_code.isdigit() or len(stock_code) != 6:
                continue
            records.append({
                'keyword': keyword,
                'art_code': item.get('art_code', ''),
                'stock_code': stock_code,
                'short_name': code.get('short_name', ''),
                'title': _clean_title(item.get('title', '')),
                'column_name': columns[0].get('column_name', '') if columns else '',
                'display_time': published,
                'notice_date': item.get('notice_date', '')[:10],
                'url': f'https://data.eastmoney.com/notices/detail/{stock_code}/{item.get("art_code", "")}.html',
            })
    return records


def fetch_em_chunk(keyword: str, start: str, end: str) -> tuple[list[dict[str, Any]], int]:
    params = {
        'sr': '-1', 'page_size': '100', 'page_index': '1', 'ann_type': 'A',
        'client_source': 'web', 'f_node': '0', 's_node': '0',
        'begin_time': start, 'end_time': end, 'title': keyword,
    }
    first = _em_get(params)
    total = int(first.get('data', {}).get('total_hits', 0))
    pages = min(100, math.ceil(total / 100))
    records = _em_records(first, keyword)
    if pages > 1:
        def _page(page: int) -> list[dict[str, Any]]:
            page_params = dict(params)
            page_params['page_index'] = str(page)
            time.sleep(0.15)
            return _em_records(_em_get(page_params), keyword)

        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(_page, page) for page in range(2, pages + 1)]
            for future in as_completed(futures):
                records.extend(future.result())
    return records, total


def fetch_keyword(keyword: str, start: str, end: str, source: str) -> tuple[list[dict[str, Any]], int]:
    records: list[dict[str, Any]] = []
    total = 0
    fetcher = fetch_cninfo_chunk if source == 'cninfo' else fetch_em_chunk
    for chunk_start, chunk_end in _quarter_chunks(start, end):
        chunk_records, chunk_total = fetcher(keyword, chunk_start, chunk_end)
        records.extend(chunk_records)
        total += chunk_total
    return records, total


def fetch_announcements(start: str, end: str, keywords: tuple[str, ...],
                        source: str = 'cninfo', partial_path: Path | None = None) -> tuple[pl.DataFrame, dict[str, Any]]:
    all_records: list[dict[str, Any]] = []
    meta: dict[str, Any] = {'source': source}
    for keyword in keywords:
        records, total = fetch_keyword(keyword, start, end, source)
        all_records.extend(records)
        meta[keyword] = {'total_hits': total, 'records': len(records)}
        logger.info('公告关键词 {}: total_hits={} records={}', keyword, total, len(records))
        if partial_path is not None and all_records:
            partial = pl.DataFrame(all_records, infer_schema_length=200000)
            partial.write_parquet(partial_path)
    frame = pl.DataFrame(all_records, infer_schema_length=200000)
    if frame.height:
        frame = frame.unique(subset=['art_code', 'stock_code'], keep='first')
    return frame, meta


def fetch_yjyg(start_year: int, end_year: int) -> pl.DataFrame:
    import akshare as ak

    rows: list[dict[str, Any]] = []
    for year in range(start_year, end_year + 1):
        for quarter in ('0331', '0630', '0930', '1231'):
            report_date = f'{year}{quarter}'
            frame = None
            for attempt in range(3):
                try:
                    frame = ak.stock_yjyg_em(date=report_date)
                    break
                except Exception as exc:  # noqa: BLE001 - 远程接口重试
                    logger.warning('业绩预告 {} 第 {} 次失败: {}', report_date, attempt + 1, exc)
                    time.sleep(2)
            if frame is None or frame.empty:
                continue
            for _, row in frame.iterrows():
                rows.append({
                    'stock_code': str(row.get('股票代码', '')).zfill(6),
                    'short_name': row.get('股票简称', ''),
                    'forecast_type': row.get('预告类型', ''),
                    'change_pct': row.get('业绩变动幅度', None),
                    'announce_date': str(row.get('公告日期', ''))[:10],
                    'report_date': report_date,
                })
            logger.info('业绩预告 {}: {} 行', report_date, len(frame))
    if not rows:
        return pl.DataFrame()
    frame = pl.DataFrame(rows)
    return frame.unique(subset=['stock_code', 'announce_date', 'forecast_type'], keep='first')


def main() -> None:
    parser = argparse.ArgumentParser(description='抓取公告与业绩预告样本')
    parser.add_argument('--source', choices=('cninfo', 'eastmoney'), default='cninfo')
    parser.add_argument('--start', default='2025-01-01')
    parser.add_argument('--end', default='2025-12-31')
    parser.add_argument('--yjyg-start-year', type=int, default=2024)
    parser.add_argument('--yjyg-end-year', type=int, default=2025)
    parser.add_argument('--keywords', nargs='*', default=list(DEFAULT_KEYWORDS))
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--sleep', type=float, default=0.25)
    parser.add_argument('--skip-yjyg', action='store_true')
    args = parser.parse_args()

    global WORKERS, REQUEST_SLEEP
    WORKERS, REQUEST_SLEEP = max(1, args.workers), max(0.05, args.sleep)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    started = time.time()
    announcements, meta = fetch_announcements(
        args.start, args.end, tuple(args.keywords), args.source,
        partial_path=DATA_DIR / 'announcements_partial.parquet')
    announcements.write_parquet(DATA_DIR / 'announcements_raw.parquet')
    logger.info('公告落盘 {} 行 → announcements_raw.parquet', announcements.height)

    yjyg = pl.DataFrame()
    if not args.skip_yjyg:
        yjyg = fetch_yjyg(args.yjyg_start_year, args.yjyg_end_year)
        yjyg.write_parquet(DATA_DIR / 'yjyg_raw.parquet')
        logger.info('业绩预告落盘 {} 行 → yjyg_raw.parquet', yjyg.height)

    meta.update({
        'range': [args.start, args.end],
        'announcement_rows': announcements.height,
        'yjyg_rows': yjyg.height,
        'elapsed_seconds': round(time.time() - started, 1),
    })
    (DATA_DIR / 'fetch_meta.json').write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info('抓取完成，耗时 {} 秒', meta['elapsed_seconds'])


if __name__ == '__main__':
    main()
