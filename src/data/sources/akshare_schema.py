#!/usr/bin/env python3

"""
AkShare 上游接口契约

对 akshare 上游的全部依赖集中在本模块：
    - 上游接口改名、字段改名时，只需修改本文件的规格定义；
    - handler 层不再 import akshare，也不再出现上游中文列名；
    - 每次取数后断言必需列，缺失时抛出 AkShareContractError，
      把"上游变了"与"我方逻辑错了"直接区分开。

本模块不依赖数据库、不缓存数据，可独立单测。
"""

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable, List, Mapping, Sequence, Tuple

import polars as pl
from loguru import logger

from src.utils.exceptions import DataIntegrityError


class AkShareContractError(DataIntegrityError):
    """AkShare 端点或返回结构与约定不符（通常意味着上游接口变更）"""

    def __init__(self, endpoint: str, missing: Iterable[str] = (),
                 actual: Iterable[str] = (), reason: str | None = None):
        self.endpoint = endpoint
        self.missing = list(missing)
        self.actual = list(actual)
        parts = [f"AkShare 端点 {endpoint}"]
        if reason:
            parts.append(reason)
        if self.missing:
            parts.append(f"缺少必需列 {self.missing}")
        if self.actual:
            parts.append(f"实际列 {self.actual}")
        parts.append("上游接口很可能已变更，请更新 src/data/sources/akshare_schema.py 的规格定义")
        super().__init__("；".join(parts), constraint=f"akshare.{endpoint}")


@dataclass(frozen=True)
class EndpointSpec:
    """
    一个 AkShare 端点的输出契约

    Attributes:
        name: akshare 模块内的函数名
        required: 必需的上游列名，缺失即视为上游变更
        column_map: 标准列名 -> 上游列名
        date_format: 日期列的字符串格式，None 表示由调用方自行处理
        volume_unit: 上游成交量单位，'hand'(手) 或 'share'(股)
    """
    name: str
    required: Tuple[str, ...]
    column_map: Mapping[str, str] = field(default_factory=dict)
    date_format: str | None = None
    volume_unit: str = 'hand'

    def missing_columns(self, columns: Iterable[str]) -> List[str]:
        present = set(columns)
        return [c for c in self.required if c not in present]


# ----------------------------------------------------------------------
# 端点规格
# ----------------------------------------------------------------------

STOCK_BASIC = EndpointSpec(
    name='stock_info_a_code_name',
    required=('code', 'name'),
    column_map={'symbol': 'code', 'name': 'name'},
)

MARKET_BREADTH = EndpointSpec(
    name='stock_zh_a_spot_em',
    required=('涨跌幅',),
    column_map={'pct_chg': '涨跌幅'},
)

STOCK_HIST = EndpointSpec(
    name='stock_zh_a_hist',
    required=('日期', '开盘', '最高', '最低', '收盘',
              '昨收', '涨跌额', '涨跌幅', '成交量', '成交额'),
    column_map={
        'date': '日期', 'open': '开盘', 'high': '最高', 'low': '最低',
        'close': '收盘', 'pre_close': '昨收', 'change': '涨跌额',
        'pct_chg': '涨跌幅', 'volume': '成交量', 'amount': '成交额',
    },
    date_format='%Y-%m-%d',
    volume_unit='hand',
)

INDEX_BASIC = EndpointSpec(
    name='index_stock_info',
    required=('代码', '名称'),
    column_map={'ts_code': '代码', 'name': '名称'},
)

INDEX_DAILY = EndpointSpec(
    name='stock_zh_index_daily',
    required=('date', 'open', 'high', 'low', 'close', 'volume'),
    column_map={c: c for c in ('date', 'open', 'high', 'low', 'close', 'volume')},
    volume_unit='hand',
)

# stock_zh_index_daily_em 的列名随版本在中文/英文间切换，两种都声明，
# 由 select_spec() 按实际列名选择
INDEX_DAILY_EM_EN = EndpointSpec(
    name='stock_zh_index_daily_em',
    required=('date', 'open', 'high', 'low', 'close', 'volume'),
    column_map={
        'date': 'date', 'open': 'open', 'high': 'high', 'low': 'low',
        'close': 'close', 'volume': 'volume', 'amount': 'amount',
    },
    date_format='%Y-%m-%d',
    volume_unit='hand',
)

INDEX_DAILY_EM_CN = EndpointSpec(
    name='stock_zh_index_daily_em',
    required=('日期', '开盘', '最高', '最低', '收盘', '成交量'),
    column_map={
        'date': '日期', 'open': '开盘', 'high': '最高', 'low': '最低',
        'close': '收盘', 'volume': '成交量', 'amount': '成交额',
    },
    date_format='%Y-%m-%d',
    volume_unit='hand',
)

DIVIDEND = EndpointSpec(
    name='stock_fhps_detail_em',
    required=('报告期',),
)

ETF_SPOT = EndpointSpec(
    name='fund_etf_spot_em',
    required=('基金代码', '基金简称'),
    column_map={'symbol': '基金代码', 'name': '基金简称'},
)

# 分红接口字段在不同版本间存在多种命名，按候选顺序取第一个可用值
DIVIDEND_ALIASES: Dict[str, Tuple[str, ...]] = {
    'report_period': ('报告期',),
    'report_date': ('业绩披露日期', '公告日期'),
    'record_date': ('股权登记日',),
    'ex_date': ('除权除息日', '除权日'),
    'cash_div_ratio': ('现金分红-现金分红比例', '每10股派息(税前)', '每10股派息'),
    'share_div_ratio': ('送转股份-送转总比例', '每10股送转', '每10股送股', '每10股转增'),
    'rights_issue_price': ('配股价格', '配股价'),
    'rights_issue_ratio': ('配股比例', '每10股配股比例', '10配股'),
    'total_div': ('总派现金额(含税)', '派现总额', '分红总额'),
}


# ----------------------------------------------------------------------
# 取数与契约校验
# ----------------------------------------------------------------------

def _load_ak():
    """延迟加载 akshare，避免模块导入即触发网络依赖初始化"""
    import akshare as ak
    return ak


def fetch_raw(endpoint: str, **kwargs) -> pl.DataFrame:
    """
    调用 AkShare 端点并转为 Polars DataFrame（不做列校验）

    Args:
        endpoint: akshare 函数名
        **kwargs: 透传给上游的参数

    Returns:
        pl.DataFrame: 上游原始数据；上游返回 None/空时为空表

    Raises:
        AkShareContractError: 端点不存在
        Exception: 上游调用异常，原样抛出并记录端点名
    """
    ak = _load_ak()
    func = getattr(ak, endpoint, None)
    if func is None:
        raise AkShareContractError(endpoint, reason='端点不存在')

    try:
        raw = func(**kwargs)
    except Exception as e:
        logger.warning(f"AkShare 端点 {endpoint} 调用失败 {kwargs}: {e}")
        raise

    if raw is None:
        return pl.DataFrame()
    if isinstance(raw, pl.DataFrame):
        return raw
    return pl.from_pandas(raw)


def validate_columns(df: pl.DataFrame, spec: EndpointSpec) -> None:
    """
    断言必需列齐全

    Raises:
        AkShareContractError: 存在缺失列
    """
    if df.is_empty():
        return
    missing = spec.missing_columns(df.columns)
    if missing:
        raise AkShareContractError(spec.name, missing=missing, actual=df.columns)


def resolve(df: pl.DataFrame, spec: EndpointSpec) -> pl.DataFrame:
    """
    校验并把上游列名改写为标准列名（未映射的列原样保留）

    Args:
        df: 上游数据
        spec: 端点规格

    Returns:
        pl.DataFrame: 列名已标准化的数据
    """
    validate_columns(df, spec)
    renames = {
        upstream: standard
        for standard, upstream in spec.column_map.items()
        if upstream != standard and upstream in df.columns
    }
    return df.rename(renames) if renames else df


def fetch_spec(spec: EndpointSpec, **kwargs) -> pl.DataFrame:
    """取数并校验必需列（列名保持上游原名，供 _normalize_kline 映射使用）"""
    df = fetch_raw(spec.name, **kwargs)
    validate_columns(df, spec)
    return df


def select_spec(candidates: Sequence[EndpointSpec], df: pl.DataFrame) -> EndpointSpec:
    """
    从候选规格中选出与实际列名匹配的一个（用于上游中英文列名切换）

    Raises:
        AkShareContractError: 无候选匹配
    """
    for spec in candidates:
        if not spec.missing_columns(df.columns):
            return spec
    first = candidates[0]
    raise AkShareContractError(
        first.name,
        missing=first.missing_columns(df.columns),
        actual=df.columns,
        reason='候选规格（中/英文列）均不匹配',
    )


def pick_alias(row: Mapping, names: Sequence[str], default=None):
    """
    按候选顺序取第一个有效值，跳过 None / 空串 / NaN / NaT

    Args:
        row: 数据行
        names: 候选上游列名
        default: 全部无效时的返回值
    """
    for name in names:
        if name not in row:
            continue
        value = row[name]
        if value is None:
            continue
        if isinstance(value, float) and math.isnan(value):
            continue
        if isinstance(value, str) and value.strip() in ('', 'NaT', 'None', 'nan'):
            continue
        return value
    return default


def parse_date_value(value):
    """
    把上游日期值解析为 date，无法解析时返回 None

    兼容 datetime/Timestamp 实例与 'YYYY-MM-DD' 字符串，
    空串、'NaT'、'None'、'nan' 均视为缺失。
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    text = str(value).strip()
    if not text or text in {'NaT', 'None', 'nan'}:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def parse_number_value(value, default=0.0):
    """
    把上游数值解析为 float，无法解析时返回 default

    兼容数值类型与带千分位/单位（元、股、%）的字符串，
    空串、'None'、'nan'、'--' 均视为缺失。
    """
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(',', '')
    if not text or text in {'None', 'nan', 'NaN', '--'}:
        return default
    for suffix in ('元', '股', '%'):
        text = text.replace(suffix, '')
    try:
        return float(text)
    except (ValueError, TypeError):
        return default
