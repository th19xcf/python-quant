#!/usr/bin/env python3

"""按申万行业 PIT 区间表构造回测标的池（只读）。

回测入口 :func:`src.core.usecases.run_backtest.run_backtest` 没有行业选池参数，
``industry`` / ``industry_map`` 只用于风控的行业权重上限。本模块补上「按行业取
标的列表」这一层，产出直接喂给 ``ts_codes``。

数据来源只有一个：``{store_root}/meta/industry_pit/industry_pit_sw.parquet``
（申万 L1/L2/L3 区间表）。选池口径：

- 层级：``L1`` / ``L2`` / ``L3``，用名称或行业代码精确匹配；
- 时点：``in_date <= as_of <= out_date``，``out_date`` 为空视为仍在该区间；
- 退市：默认用 ``meta/stock_basic.parquet`` 的 ``delist_date`` 剔除
  ``delist_date <= as_of`` 的标的，``include_delisted=True`` 时不剔除。

为什么 2014-02-21 之前一律返回空：该日之前是申万 2003 版行业代码，1990~2013
年的记录名称覆盖只有约 55.7%，且与新版分类混用同一张区间表。此时返回部分成分
会把「没分类」误当成「不属于该行业」，因此本模块直接拒绝（``reason`` 给出明确
原因），不返回半份名单。

通达信行业表（``meta/industry/`` / ``industry_pit/industry_pit.parquet``）只有
L1/L2，原始树的三级在快照阶段已被丢弃，不能用于 L3 选池；概念板块成分表本地
不存在。详见 ``docs/行业选池说明.md``。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal

import polars as pl
from loguru import logger

Level = Literal['L1', 'L2', 'L3']

_LEVELS: tuple[str, ...] = ('L1', 'L2', 'L3')
_PIT_RELATIVE = Path('meta') / 'industry_pit' / 'industry_pit_sw.parquet'
_STOCK_BASIC_RELATIVE = Path('meta') / 'stock_basic.parquet'

#: 申万 2014 版分类树生效日。此前为 2003 版代码，名称覆盖仅约 55.7%。
SW_TREE_EFFECTIVE_DATE = date(2014, 2, 21)

#: 正常取到结果（集合仍可能为空，用 ``reason='no_member_at_as_of'`` 区分）。
REASON_BEFORE_TREE = 'before_sw_tree_effective'
REASON_UNKNOWN_INDUSTRY = 'industry_not_found'
REASON_NO_MEMBER = 'no_member_at_as_of'


@dataclass
class IndustryUniverse:
    """一次行业选池的结果。

    ``codes`` 是可直接传给 ``run_backtest(ts_codes=...)`` 的标的列表；
    ``reason`` 非空表示这次没有拿到可用名单，调用方**必须**按原因处理，
    不得把空列表当成「该行业没有成分」。
    """

    level: str
    name: str
    as_of: date
    codes: list[str] = field(default_factory=list)
    matched_industry_codes: list[str] = field(default_factory=list)
    excluded_delisted: list[str] = field(default_factory=list)
    reason: str | None = None
    message: str = ''

    def __len__(self) -> int:
        return len(self.codes)


def _resolve_store_root(store_root: str | Path | None) -> Path:
    if store_root is not None:
        return Path(store_root)
    # 按需 import，避免数据层模块在导入期互相牵连
    from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
    from src.utils.config import get_config  # noqa: PLC0415

    return resolve_store_root(get_config())


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(text[:10] if '-' in text else text[:8], fmt).date()
        except ValueError:
            continue
    raise ValueError(f'无法解析日期 {value!r}，支持 date / datetime / "YYYY-MM-DD" / "YYYYMMDD"')


def _load_parquet(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f'缺少 {path}；行业选池依赖申万 PIT 区间表，请先构建或检查 DATA__LOCAL_STORE_PATH')
    return pl.read_parquet(path)


def select_industry_universe(
    level: Level | str,
    name: str,
    as_of: date | datetime | str,
    *,
    include_delisted: bool = False,
    store_root: str | Path | None = None,
    pit: pl.DataFrame | None = None,
    stock_basic: pl.DataFrame | None = None,
) -> IndustryUniverse:
    """按申万 PIT 取某一级行业在某日的成分。

    Args:
        level: ``'L1'`` / ``'L2'`` / ``'L3'``（大小写不敏感）。
        name: 行业名称（如 ``'黄金'``）或行业代码（如 ``'240401'``），精确匹配。
        as_of: 查询日期；早于 :data:`SW_TREE_EFFECTIVE_DATE` 时直接返回空并给出
            ``reason='before_sw_tree_effective'``。
        include_delisted: 为 ``True`` 时保留 ``delist_date <= as_of`` 的已退市标的，
            只按行业区间过滤；默认 ``False``，即剔除该日已退市的标的。
        store_root: 列存根目录；默认按配置解析。
        pit: 申万区间表；显式传入时不读盘（测试用）。
        stock_basic: 股票基础表；显式传入时不读盘（测试用）。缺 ``delist_date``
            列时跳过退市过滤，并在 ``message`` 中注明。

    Returns:
        :class:`IndustryUniverse`。``codes`` 为按代码升序的标的列表，
        ``reason`` 为 ``None`` 表示取到了可用名单；否则为
        :data:`REASON_BEFORE_TREE` / :data:`REASON_UNKNOWN_INDUSTRY` /
        :data:`REASON_NO_MEMBER` 之一，并附中文 ``message``。

    Raises:
        ValueError: ``level`` 非法、``name`` 为空或 ``as_of`` 无法解析。
        FileNotFoundError: 未传 ``pit`` 且盘上缺少区间表。
    """
    as_of_date = _as_date(as_of)
    level_norm = str(level).strip().upper()
    if level_norm not in _LEVELS:
        raise ValueError(f'level 只支持 {_LEVELS}，收到 {level!r}')
    industry_name = str(name).strip()
    if not industry_name:
        raise ValueError('name 不能为空')

    base = IndustryUniverse(level=level_norm, name=industry_name, as_of=as_of_date)

    if as_of_date < SW_TREE_EFFECTIVE_DATE:
        base.reason = REASON_BEFORE_TREE
        base.message = (
            f'as_of={as_of_date} 早于申万 2014 版分类树生效日 {SW_TREE_EFFECTIVE_DATE}：'
            '此前为申万 2003 版代码，名称覆盖不完整且与新版分类混用，'
            f'无法可靠判定「{industry_name}」成分，故返回空名单而不是部分名单。')
        logger.warning(base.message)
        return base

    frame = pit if pit is not None else _load_parquet(
        _resolve_store_root(store_root) / _PIT_RELATIVE)
    name_col = f'industry_{level_norm.lower()}_name'
    code_col = f'industry_{level_norm.lower()}'
    if name_col not in frame.columns or code_col not in frame.columns:
        raise ValueError(f'区间表缺少 {name_col} / {code_col} 列，实际列：{frame.columns}')

    matched = frame.filter(
        pl.col(name_col).eq(industry_name) | pl.col(code_col).eq(industry_name))
    if matched.height == 0:
        base.reason = REASON_UNKNOWN_INDUSTRY
        base.message = f'申万 {level_norm} 层级中不存在名称或代码为 {industry_name!r} 的行业'
        logger.warning(base.message)
        return base
    base.matched_industry_codes = sorted(matched[code_col].unique().to_list())

    active = matched.filter(
        (pl.col('in_date') <= as_of_date)
        & (pl.col('out_date').is_null() | (pl.col('out_date') >= as_of_date)))
    codes = sorted(set(active['ts_code'].to_list()))
    if not codes:
        base.reason = REASON_NO_MEMBER
        base.message = (
            f'{industry_name} 在 {as_of_date} 没有区间内成分'
            f'（行业代码 {base.matched_industry_codes}）')
        return base

    if not include_delisted:
        basic = stock_basic
        if basic is None:
            path = _resolve_store_root(store_root) / _STOCK_BASIC_RELATIVE
            basic = pl.read_parquet(path) if path.exists() else None
        if basic is None or 'delist_date' not in basic.columns:
            base.message = '未找到 stock_basic.delist_date，本次未做退市剔除'
            logger.warning(base.message)
        else:
            dead = set(
                basic.filter(
                    pl.col('delist_date').is_not_null()
                    & (pl.col('delist_date') <= as_of_date)
                )['ts_code'].to_list())
            base.excluded_delisted = [code for code in codes if code in dead]
            codes = [code for code in codes if code not in dead]

    base.codes = codes
    return base
