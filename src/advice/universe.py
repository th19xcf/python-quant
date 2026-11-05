#!/usr/bin/env python3

"""股票池、名称与行业口径。

运行时不查远程库：股票池来自本地列存目录扫描（文件名即 ts_code），
名称可选读取本地 JSON 映射，缺失时回退为代码本身并标注 ``name_source='code'``。
行业无权威表时用「交易所板块 + 主板沪/深拆分」近似，并显式标注
``industry_source='code_prefix_board'``。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from loguru import logger

from src.data.local_store.store import resolve_store_root
from src.data.quality.code_migration import load_entity_index
from src.utils.code_converter import is_fund_code, is_index_code

INDUSTRY_SOURCE_BOARD = 'code_prefix_board'
INDUSTRY_SOURCE_TABLE = 'industry_table'
NAME_SOURCE_CODE = 'code'
NAME_SOURCE_LOCAL = 'local_names_json'


@dataclass(frozen=True)
class StockMeta:
    """单标的展示元数据。"""

    ts_code: str
    name: str
    name_source: str
    industry: str
    industry_source: str


def list_local_stock_codes(config: Any, markets: Iterable[str] = ('sh', 'sz', 'bj')) -> list[str]:
    """扫描本地列存 ``quotes/stock`` 目录，返回可交易股票代码（升序）。

    直接按文件系统扫描而不是读 ``meta/daily_index.parquet``：并行构建期间
    元数据可能尚未 flush，但行情文件已落盘，扫描能保证股票池立即可用。
    """
    root = resolve_store_root(config)
    base = root / 'quotes' / 'stock'
    entity_index = load_entity_index(root)
    codes: list[str] = []
    for market in markets:
        directory = base / market
        if not directory.is_dir():
            continue
        for path in sorted(directory.glob('*.parquet')):
            code = path.stem.upper()
            if is_index_code(code) or is_fund_code(code):
                continue
            if entity_index.is_superseded(code):
                # 旧码已被同一实体的新码完整覆盖，读侧只保留新码
                continue
            codes.append(code)
    return sorted(set(codes))


def load_name_map(path: str | Path | None) -> dict[str, str]:
    """读取本地名称映射（JSON: ``{"600000.SH": "浦发银行"}``）。

    文件缺失或解析失败返回空字典，不阻断推荐流程。
    """
    if not path:
        return {}
    file_path = Path(path)
    if not file_path.is_file():
        logger.warning(f"名称映射文件不存在，回退代码展示: {file_path}")
        return {}
    try:
        payload = json.loads(file_path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        logger.warning(f"名称映射读取失败，回退代码展示: {exc}")
        return {}
    if not isinstance(payload, dict):
        logger.warning(f"名称映射应为 JSON 对象，实际 {type(payload).__name__}")
        return {}
    return {str(key).upper(): str(value) for key, value in payload.items()
            if value}


def board_industry(ts_code: str) -> str:
    """按代码段近似板块（无权威行业表时的显式降级口径）。"""
    code = ts_code.split('.')[0]
    suffix = ts_code.rsplit('.', 1)[-1].upper() if '.' in ts_code else ''
    if suffix == 'BJ' or code.startswith(('83', '87', '88', '920')):
        return '北交所'
    if code.startswith(('688', '689')):
        return '科创板'
    if code.startswith(('300', '301')):
        return '创业板'
    if suffix == 'SH':
        return '沪主板'
    return '深主板'


def build_meta(codes: Iterable[str], name_map: dict[str, str] | None = None,
               industry_map: dict[str, str] | None = None) -> dict[str, StockMeta]:
    """构建代码 → 展示元数据映射。"""
    names = name_map or {}
    industries = industry_map or {}
    result: dict[str, StockMeta] = {}
    for code in codes:
        code = str(code).upper()
        name = names.get(code)
        if name:
            name_source = NAME_SOURCE_LOCAL
        else:
            name, name_source = code, NAME_SOURCE_CODE
        industry = industries.get(code)
        if industry:
            industry_source = INDUSTRY_SOURCE_TABLE
        else:
            industry, industry_source = board_industry(code), INDUSTRY_SOURCE_BOARD
        result[code] = StockMeta(
            ts_code=code,
            name=name,
            name_source=name_source,
            industry=industry,
            industry_source=industry_source,
        )
    return result
