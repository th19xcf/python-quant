#!/usr/bin/env python3

"""公告事件研究试点：事件类型 → 后续股价超额收益。

输入：``data/announcements_raw.parquet`` + ``data/yjyg_raw.parquet``；
输出：``data/announcement_events_normalized.parquet``、
``data/announcement_event_level_car.csv``、
``data/announcement_event_summary.csv``（市场模型）、
``data/announcement_event_summary_ma.csv``（市场调整，beta=1 对照）。

口径：
- 估计窗 [-120,-20]，事件窗 [-5,+20]，基准为沪深 300；
- 事件日 0 = 公告发布日之后的下一个交易日（盘后/夜间公告因此天然满足 T+1）；
- 建仓价 = 事件日 0 的开盘价（后复权），CAR0 为「开盘→收盘」的超额收益；
- 同一标的同一事件类型 30 个自然日内只保留最早一条，滤掉进展/结果类重复公告。
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from event_study_core import (  # noqa: E402
    Benchmark,
    PriceSeries,
    compute_event_car,
    load_price_series,
    summarize,
    write_csv,
)

from src.utils.code_converter import parse_code  # noqa: E402

DATA_DIR = Path(__file__).resolve().parent / 'data'
HORIZONS = (0, 1, 5, 20)
DEDUP_DAYS = 30

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

TITLE_FILTERS = {
    '减持': (r'减持', r'增持'),
    '增持': (r'增持', r'减持'),
    '股权激励': (r'股权激励|股票期权|限制性股票', None),
    '重大合同/中标': (r'中标|重大合同|框架协议|战略合作', None),
    '监管处罚': (r'处罚|监管措施|立案|警示函|责令改正|问询', None),
    '并购重组': (r'重组|并购|收购|吸收合并', None),
    '分红送转': (r'分红|利润分配|权益分派|派息', None),
}

POSITIVE_FORECAST = {'预增', '略增', '扭亏', '续盈'}
NEGATIVE_FORECAST = {'预减', '略减', '首亏', '续亏'}


def _to_ts_code(stock_code: str) -> str | None:
    try:
        symbol, market = parse_code(stock_code)
    except ValueError:
        return None
    return f'{symbol}.{market}'


def normalize_announcements(frame: pl.DataFrame, start: date, end: date) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in frame.to_dicts():
        keyword = item['keyword']
        event_type = KEYWORD_TO_TYPE.get(keyword)
        if event_type is None:
            continue
        title = item['title'] or ''
        include, exclude = TITLE_FILTERS.get(event_type, (None, None))
        if include and not re.search(include, title):
            continue
        if exclude and re.search(exclude, title):
            continue
        published = item['display_time']
        if published is None or not (start <= published.date() <= end):
            continue
        ts_code = _to_ts_code(item['stock_code'])
        if ts_code is None:
            continue
        rows.append({
            'event_id': f"{item['art_code']}-{item['stock_code']}",
            'ts_code': ts_code,
            'stock_code': item['stock_code'],
            'short_name': item['short_name'],
            'event_type': event_type,
            'publish_time': published,
            'title': title,
            'url': item['url'],
            'source': 'cninfo',
        })
    return pl.DataFrame(rows, infer_schema_length=100000)


def normalize_yjyg(frame: pl.DataFrame, start: date, end: date) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in frame.to_dicts():
        forecast_type = item['forecast_type']
        if forecast_type in POSITIVE_FORECAST:
            event_type = '业绩预增'
        elif forecast_type in NEGATIVE_FORECAST:
            event_type = '业绩预减'
        else:
            continue
        try:
            published = datetime.strptime(item['announce_date'][:10], '%Y-%m-%d')
        except (TypeError, ValueError):
            continue
        if not (start <= published.date() <= end):
            continue
        ts_code = _to_ts_code(item['stock_code'])
        if ts_code is None:
            continue
        rows.append({
            # 同一标的同一报告期可能同时存在「预增」「预减」，以及隔一段时间
            # 再发的修正/更新预告（公告日不同）。只按 report_date 作键会让多条
            # 不同事件共用 event_id，下游按 event_id 去重时会误删。事件类型与
            # 公告日必须一起进主键。
            'event_id': (f'YJYG-{item["stock_code"]}-{item["report_date"]}'
                         f'-{event_type}-{published:%Y%m%d}'),
            'ts_code': ts_code,
            'stock_code': item['stock_code'],
            'short_name': item['short_name'],
            'event_type': event_type,
            'publish_time': published,
            'title': f'{item["report_date"]} {forecast_type}',
            'url': '',
            'source': 'em_yjyg',
        })
    return pl.DataFrame(rows, infer_schema_length=100000)


def dedup_events(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height == 0:
        return frame
    records = sorted(frame.to_dicts(), key=lambda x: (x['ts_code'], x['event_type'], x['publish_time']))
    kept: list[dict[str, Any]] = []
    markers: dict[tuple[str, str], datetime] = {}
    for item in records:
        key = (item['ts_code'], item['event_type'])
        previous = markers.get(key)
        if previous is not None and item['publish_time'] - previous <= timedelta(days=DEDUP_DAYS):
            continue
        markers[key] = item['publish_time']
        kept.append(item)
    return pl.DataFrame(kept, infer_schema_length=100000)


def compute_all(events: pl.DataFrame) -> pl.DataFrame:
    benchmark = Benchmark('000300.SH')
    cache: dict[str, PriceSeries | None] = {}
    rows: list[dict[str, Any]] = []
    for item in events.to_dicts():
        ts_code = item['ts_code']
        if ts_code not in cache:
            cache[ts_code] = load_price_series(ts_code, kind='stock')
        series = cache[ts_code]
        if series is None:
            continue
        result = compute_event_car(series, benchmark, item['publish_time'], HORIZONS)
        if result is None:
            continue
        rows.append({**item, **result})
    return pl.DataFrame(rows, infer_schema_length=100000)


def main() -> None:
    parser = argparse.ArgumentParser(description='公告事件研究')
    parser.add_argument('--start', default='2025-01-01')
    parser.add_argument('--end', default='2025-12-31')
    args = parser.parse_args()
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)

    announcements = pl.read_parquet(DATA_DIR / 'announcements_raw.parquet')
    yjyg = pl.read_parquet(DATA_DIR / 'yjyg_raw.parquet') if (DATA_DIR / 'yjyg_raw.parquet').exists() else pl.DataFrame()
    events = normalize_announcements(announcements, start, end)
    if yjyg.height:
        events = pl.concat([events, normalize_yjyg(yjyg, start, end)], how='diagonal_relaxed')
    events = dedup_events(events)
    logger.info('规范化后事件 {} 条，类型分布 {}', events.height, events['event_type'].value_counts().to_dicts())
    events.write_parquet(DATA_DIR / 'announcement_events_normalized.parquet')

    level = compute_all(events)
    if level.height == 0:
        logger.error('没有事件可计算 CAR，请先运行 fetch_announcements.py')
        return
    level.write_parquet(DATA_DIR / 'announcement_event_level_car.parquet')
    write_csv(level, DATA_DIR / 'announcement_event_level_car.csv')

    summary = summarize(level, ['event_type'], HORIZONS)
    summary_ma = summarize(level, ['event_type'], HORIZONS, value_template='car_ma{h}')
    write_csv(summary, DATA_DIR / 'announcement_event_summary.csv')
    write_csv(summary_ma, DATA_DIR / 'announcement_event_summary_ma.csv')
    logger.info('事件级 {} 行，汇总 {} 行', level.height, summary.height)
    logger.info('汇总:\n{}', summary.to_pandas().to_string(index=False))


if __name__ == '__main__':
    main()
