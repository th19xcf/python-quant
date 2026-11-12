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
import random
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
# 每个 (关键词, 区间) 开始前的随机抖动上限（秒），仅用于打散请求节奏、降低被限流概率，
# 不用于任何安全用途。0 表示不抖动。
REQUEST_JITTER = 0.0
#: 空响应（``totalRecordNum=0``）的最大尝试次数（含首次）。
#: 实测（2026-11-12）：同一 keyword+区间在不同时刻会返回 309 与 0 两种结果，
#: 因此「0」不能直接当「源端确实没有」。
EMPTY_RETRIES = 4
#: 空响应重试的指数退避基数与上限（秒）
EMPTY_BACKOFF_BASE = 2.0
EMPTY_BACKOFF_CAP = 30.0
#: 空响应重试耗尽后的默认处置：False = 抛错（不静默落空表），True = 只告警
ALLOW_EMPTY = False


class CninfoEmptyResponseError(RuntimeError):
    """巨潮返回 ``totalRecordNum=0``，重试耗尽仍无法与「源端确实没有」区分。"""


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


def _cninfo_total(payload: Any) -> int:
    """从响应里取 ``totalRecordNum``；取不到或类型不对时返回 0。"""
    if not isinstance(payload, dict):
        return 0
    try:
        return int(payload.get('totalRecordNum') or 0)
    except (TypeError, ValueError):
        return 0


def _cninfo_empty_reason(payload: Any) -> str:
    """返回「空响应」的结构异常原因；结构正常时返回空字符串。

    结构正常（``totalRecordNum`` 与 ``announcements`` 都在）的 0 与结构异常
    （非 dict、缺键、类型错）分开报告：后者只能判定为链路/接口异常，
    前者才可能是源端确实没有。
    """
    if not isinstance(payload, dict):
        return f'payload_not_dict:{type(payload).__name__}'
    if 'totalRecordNum' not in payload:
        return 'missing_totalRecordNum'
    if 'announcements' not in payload:
        return 'missing_announcements'
    raw = payload.get('totalRecordNum')
    if raw is not None:
        try:
            int(raw)
        except (TypeError, ValueError):
            return f'totalRecordNum_not_int:{raw!r}'
    return ''


def fetch_cninfo_chunk(keyword: str, start: str, end: str,
                       allow_empty: bool | None = None) -> tuple[list[dict[str, Any]], int]:
    """抓一个 (关键词, 区间) 的全部公告。

    ``totalRecordNum=0`` 视为**不可信的空响应**：按 ``EMPTY_RETRIES`` 次、
    指数退避（``EMPTY_BACKOFF_BASE`` 起、``EMPTY_BACKOFF_CAP`` 封顶）重试。
    重试耗尽后默认抛 :class:`CninfoEmptyResponseError`，不静默返回空表；
    ``allow_empty=True``（或模块常量 ``ALLOW_EMPTY=True``）才降级为告警。
    """
    params = {
        'pageNum': '1', 'pageSize': str(PAGE_SIZE), 'column': 'szse', 'tabName': 'fulltext',
        'plate': '', 'stock': '', 'searchkey': keyword, 'secid': '', 'category': '', 'trade': '',
        'seDate': f'{start}~{end}', 'sortName': '', 'sortType': '', 'isHLtitle': 'true',
    }
    first: dict[str, Any] | None = None
    total = 0
    reasons: list[str] = []
    delays: list[float] = []
    for attempt in range(EMPTY_RETRIES):
        payload = _cninfo_get(params)
        total = _cninfo_total(payload)
        if total > 0:
            first = payload
            break
        reasons.append(_cninfo_empty_reason(payload) or 'totalRecordNum=0')
        delay = min(EMPTY_BACKOFF_CAP, EMPTY_BACKOFF_BASE * (2 ** attempt))
        delays.append(delay)
        logger.warning(
            '巨潮空响应 keyword={} 区间={}~{} 第{}/{}次 totalRecordNum={} 结构={} 退避={}s',
            keyword, start, end, attempt + 1, EMPTY_RETRIES, total,
            _cninfo_empty_reason(payload) or 'normal', delay)
        if attempt + 1 < EMPTY_RETRIES:
            time.sleep(delay)
    if first is None:
        detail = (f'keyword={keyword} 区间={start}~{end} 尝试={EMPTY_RETRIES}次 '
                  f'退避={delays} 原因={reasons}')
        accept_empty = ALLOW_EMPTY if allow_empty is None else allow_empty
        if accept_empty:
            logger.warning('巨潮空响应重试耗尽，按「源端确实没有」接受: {}', detail)
            return [], 0
        logger.error('巨潮空响应重试耗尽，按失败处理（不落空表）: {}', detail)
        raise CninfoEmptyResponseError(detail)
    pages = min(MAX_PAGES_PER_CHUNK, math.ceil(total / PAGE_SIZE))
    records = _cninfo_records(first, keyword)
    if pages > 1:
        def _page(page: int) -> list[dict[str, Any]]:
            page_params = dict(params)
            page_params['pageNum'] = str(page)
            # 源端会间歇性返回 HTTP 200 + 空 announcements（实测 202112 股权激励
            # 第 2/4/7/10~14 页均为空），只靠重试 HTTP 层抓不到，会把整页静默丢掉。
            # 这里按「该页应有条数」判空重试，仍空则返回空并靠末尾告警暴露。
            expected = min(PAGE_SIZE, total - (page - 1) * PAGE_SIZE)
            for attempt in range(4):
                time.sleep(REQUEST_SLEEP * (attempt + 1))
                rows = _cninfo_records(_cninfo_get(page_params), keyword)
                if rows or expected <= 0:
                    return rows
                logger.warning('巨潮空页重试 keyword={} 页={} 第{}次', keyword, page, attempt + 1)
            return []

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = [pool.submit(_page, page) for page in range(2, pages + 1)]
            for future in as_completed(futures):
                records.extend(future.result())
    if len(records) < total:
        logger.warning('巨潮记录数不足 keyword={} 区间={}~{} total_hits={} records={}',
                       keyword, start, end, total, len(records))
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


def fetch_keyword(keyword: str, start: str, end: str, source: str,
                  allow_empty: bool | None = None) -> tuple[list[dict[str, Any]], int]:
    records: list[dict[str, Any]] = []
    total = 0
    for chunk_start, chunk_end in _quarter_chunks(start, end):
        if REQUEST_JITTER > 0:
            time.sleep(random.uniform(0, REQUEST_JITTER))  # noqa: S311
        if source == 'cninfo':
            chunk_records, chunk_total = fetch_cninfo_chunk(
                keyword, chunk_start, chunk_end, allow_empty=allow_empty)
        else:
            chunk_records, chunk_total = fetch_em_chunk(keyword, chunk_start, chunk_end)
        records.extend(chunk_records)
        total += chunk_total
    return records, total


def fetch_announcements(start: str, end: str, keywords: tuple[str, ...],
                        source: str = 'cninfo', partial_path: Path | None = None,
                        allow_empty: bool | None = None) -> tuple[pl.DataFrame, dict[str, Any]]:
    all_records: list[dict[str, Any]] = []
    meta: dict[str, Any] = {'source': source}
    for keyword in keywords:
        records, total = fetch_keyword(keyword, start, end, source, allow_empty=allow_empty)
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
