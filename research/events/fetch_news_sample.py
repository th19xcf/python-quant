#!/usr/bin/env python3

"""新闻源小样本探测：财联社电报、东方财富快讯、新浪财经 7x24、东财研报。

只做一次快照 + 有限翻页，不跑长期爬虫、不引入任何凭证、不落库全文。
判定维度：是否能直连、时间戳精度、是否有原文链接、返回条数与稳定性。

输出：
- ``data/news_source_sample.csv``：样本条目（来源/时间/标题/链接/正文长度）；
- ``data/news_source_probe.json``：各来源的可直连、精度、链接、条数结论。
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import polars as pl
import requests
from loguru import logger

DATA_DIR = Path(__file__).resolve().parent / 'data'
UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
SAMPLES: list[dict[str, Any]] = []
PROBE: dict[str, Any] = {}


def _record(source: str, published: Any, title: str, link: str, content: str) -> None:
    SAMPLES.append({
        'source': source,
        'publish_time': str(published),
        'title': (title or '').strip()[:120],
        'has_link': bool(link),
        'link': link or '',
        'content_len': len(content or ''),
    })


def probe_akshare_news() -> None:
    import akshare as ak

    specs = {
        'cls_telegraph': ('stock_info_global_cls', '发布时间'),
        'eastmoney_fastnews': ('stock_info_global_em', '发布时间'),
        'sina_7x24': ('stock_info_global_sina', '时间'),
    }
    for source, (func_name, time_column) in specs.items():
        try:
            frame = getattr(ak, func_name)()
            times = frame[time_column].astype(str).tolist() if time_column in frame.columns else []
            has_link_col = '链接' in frame.columns
            link_col = frame['链接'].astype(str).tolist() if has_link_col else []
            content_col = '内容' if '内容' in frame.columns else ('摘要' if '摘要' in frame.columns else None)
            for idx in range(min(5, len(frame))):
                _record(
                    source,
                    frame[time_column].iloc[idx] if time_column in frame.columns else '',
                    frame['标题'].iloc[idx] if '标题' in frame.columns else '',
                    link_col[idx] if has_link_col else '',
                    frame[content_col].iloc[idx] if content_col else '',
                )
            PROBE[source] = {
                'direct_access': True,
                'items': int(len(frame)),
                'columns': frame.columns.tolist(),
                'timestamp_field': time_column,
                'timestamp_sample': times[:2],
                'has_link_column': has_link_col,
                'precision': 'minute' if any(len(str(t).split(':')) >= 2 for t in times[:5]) else 'date',
            }
        except Exception as exc:  # noqa: BLE001 - 探测失败需记录而非中断
            PROBE[source] = {'direct_access': False, 'error': f'{type(exc).__name__}: {exc}'}
            logger.warning('{} 探测失败: {}', source, exc)


def probe_cls_raw() -> None:
    """财联社原始接口：确认 id/ctime，可拼接原文链接。"""
    params = {'app': 'CailianpressWeb', 'category': '0', 'last_time': '', 'os': 'web', 'rn': '20', 'sv': '8.4.4'}
    query = urlencode(sorted(params.items()))
    sign = hashlib.md5(hashlib.sha1(query.encode()).hexdigest().encode()).hexdigest()
    try:
        response = requests.get('https://www.cls.cn/v1/roll/get_roll_list', params={**params, 'sign': sign},
                                headers={**UA, 'Referer': 'https://www.cls.cn/telegraph'}, timeout=20)
        payload = response.json()
        rolls = payload.get('data', {}).get('roll_data') or []
        PROBE['cls_raw'] = {
            'status_code': response.status_code,
            'items': len(rolls),
            'has_id': bool(rolls and rolls[0].get('id')),
            'has_shareurl': bool(rolls and rolls[0].get('shareurl')),
            'ctime': rolls[0].get('ctime') if rolls else None,
            'detail_url_pattern': 'https://www.cls.cn/detail/{id}',
        }
        for item in rolls[:5]:
            published = datetime.fromtimestamp(item['ctime']) if item.get('ctime') else ''
            link = item.get('shareurl') or (f'https://www.cls.cn/detail/{item.get("id")}' if item.get('id') else '')
            _record('cls_raw', published, item.get('title') or item.get('brief') or '', link, item.get('content') or '')
    except Exception as exc:  # noqa: BLE001
        PROBE['cls_raw'] = {'direct_access': False, 'error': f'{type(exc).__name__}: {exc}'}
        logger.warning('财联社原始接口探测失败: {}', exc)


def probe_sina_raw() -> None:
    params = {'page': '1', 'page_size': '20', 'zhibo_id': '152', 'tag_id': '0', 'dire': 'f', 'dpc': '1'}
    try:
        response = requests.get('https://zhibo.sina.com.cn/api/zhibo/feed', params=params, headers=UA, timeout=20)
        payload = response.json()
        items = payload['result']['data']['feed']['list']
        PROBE['sina_raw'] = {
            'status_code': response.status_code,
            'items': len(items),
            'has_docurl': bool(items and items[0].get('docurl')),
            'timestamp_sample': [items[0].get('create_time')] if items else [],
        }
        for item in items[:5]:
            _record('sina_raw', item.get('create_time', ''), item.get('rich_text', '')[:60],
                    item.get('docurl', ''), item.get('rich_text', ''))
    except Exception as exc:  # noqa: BLE001
        PROBE['sina_raw'] = {'direct_access': False, 'error': f'{type(exc).__name__}: {exc}'}
        logger.warning('新浪 7x24 原始接口探测失败: {}', exc)


def probe_research_reports() -> None:
    """东财研报中心：验证是否有公开列表接口与发布时间。"""
    import akshare as ak

    try:
        frame = ak.stock_research_report_em(symbol='000001')
        PROBE['eastmoney_research'] = {
            'direct_access': True,
            'items': int(len(frame)),
            'columns': frame.columns.tolist(),
            'timestamp_sample': frame.iloc[0].astype(str).to_dict() if len(frame) else {},
        }
        logger.info('东财研报可用，{} 行', len(frame))
    except Exception as exc:  # noqa: BLE001
        PROBE['eastmoney_research'] = {'direct_access': False, 'error': f'{type(exc).__name__}: {exc}'}
        logger.warning('东财研报探测失败: {}', exc)


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    started = time.time()
    probe_akshare_news()
    probe_cls_raw()
    probe_sina_raw()
    probe_research_reports()
    PROBE['elapsed_seconds'] = round(time.time() - started, 1)

    frame = pl.DataFrame(SAMPLES, infer_schema_length=1000)
    frame.write_csv(DATA_DIR / 'news_source_sample.csv')
    (DATA_DIR / 'news_source_probe.json').write_text(
        json.dumps(PROBE, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    logger.info('新闻源探测完成:\n{}', json.dumps(PROBE, ensure_ascii=False, indent=2, default=str))


if __name__ == '__main__':
    main()
