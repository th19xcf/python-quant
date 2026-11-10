#!/usr/bin/env python3

"""
screen_stocks —— 选股用例（Headless API）。

用法：
    # 方式 1：直接传面板 DataFrame（调用方自己取数）
    screen_stocks(stocks_data=my_df, top_n=10)

    # 方式 2：传 top_n + period，本函数自动取面板数据
    screen_stocks(top_n=10, period='12个月')
"""

from __future__ import annotations

import asyncio
import calendar
from datetime import date, datetime
from typing import Any, Dict, List

import polars as pl
from loguru import logger

from src.data.quality.code_migration import load_entity_index
from src.recommendation.stock_recommender import StockRecommender

# 周期 → 回溯月数（锚点为最新交易日，不再是写死的 2024 年日期）
_PERIOD_MONTHS = {
    '1个月': 1,
    '3个月': 3,
    '6个月': 6,
    '12个月': 12,
}


def _months_before(anchor: date, months: int) -> date:
    """按自然月回溯，月末日期自动夹取到目标月最后一天（如 3-31 → 2-28）。"""
    year = anchor.year - (months // 12)
    month = anchor.month - (months % 12)
    if month <= 0:
        month += 12
        year -= 1
    day = min(anchor.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _period_range(period: str, anchor: date) -> tuple[date, date]:
    """把分析周期换算为 [开始日, 结束日]，结束日固定为最新交易日。"""
    months = _PERIOD_MONTHS.get(period, _PERIOD_MONTHS['12个月'])
    return _months_before(anchor, months), anchor


def _coerce_date(value: date | str | None) -> date | None:
    """把可选的 today 参数规范化为 date；无法解析时返回 None（交给 DB 锚点）。"""
    if value is None or isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            logger.warning(f"无法解析选股截止日 {value!r}，将使用数据库最新交易日")
    return None


def _latest_trade_date(db: Any) -> date:
    """取数据库中最新的交易日；数据库不可用/无数据时回退到当天日期。"""
    try:
        from sqlalchemy import func

        from src.database.models.stock import StockDaily

        session = db.get_session()
        latest = session.query(func.max(StockDaily.trade_date)).scalar()
        if isinstance(latest, datetime):
            latest = latest.date()
        if isinstance(latest, date):
            return latest
        logger.warning("数据库中没有可用的交易日数据，选股日期回退到当天")
    # 兜底理由：行情库不可达时选股仍需可运行，按当天日期降级
    except Exception as e:  # noqa: BLE001
        logger.warning(f"查询最新交易日失败，选股日期回退到当天: {e}")
    return date.today()


def _resolve_universe(data_manager: Any, top_n: int,
                      stock_codes: List[str] | None) -> tuple[List[str], pl.DataFrame]:
    """确定选股股票池并返回 (代码列表, stock_basic 面板)。

    默认口径：``stock_basic`` 中排除指数与基金后的全部代码，按 ``ts_code`` 升序
    取前 ``top_n * 3`` 只。升序保证同一数据快照下结果可复现，不再依赖表物理行序。
    显式传入 ``stock_codes`` 时按传入顺序去重后使用，不做截断。
    """
    stock_basic = data_manager.get_stock_basic()
    entity_index = load_entity_index()

    if stock_codes:
        normalized = list(dict.fromkeys(code for code in stock_codes if code))
        return [code for code in normalized if not entity_index.is_superseded(code)], stock_basic

    if stock_basic.is_empty() or 'ts_code' not in stock_basic.columns:
        return [], stock_basic

    from src.utils.code_converter import is_fund_code, is_index_code

    codes = [
        str(code) for code in stock_basic['ts_code'].to_list()
        if code and not is_index_code(str(code)) and not is_fund_code(str(code))
        and not entity_index.is_superseded(str(code))
    ]
    return sorted(set(codes))[:top_n * 3], stock_basic


def _fetch_panel(top_n: int, period: str, as_of: date | None = None, *,
                 data_manager: Any = None,
                 stock_codes: List[str] | None = None) -> pl.DataFrame | None:
    """
    取选股面板数据（后复权日线）。

    ``data_manager`` 由调用方注入（UI 传已有 DataManager 实例），本函数不再
    自建 ``Config`` / ``DatabaseManager``，避免每次选股重复建连并绕开依赖注入。

    Args:
        as_of: 分析截止日（最新交易日的显式覆盖，主要供测试固定 today）。
        stock_codes: 显式股票池；None 时按 ``_resolve_universe`` 的默认口径。
    """
    if data_manager is None:
        logger.error("screen_stocks: 未注入 data_manager，无法自动取数"
                     "（请由调用方传入 DataManager 实例）")
        return None

    db = getattr(data_manager, 'db_manager', None)
    anchor = as_of or _latest_trade_date(db)
    start_date, end_date = _period_range(period, anchor)
    start_date_str, end_date_str = start_date.isoformat(), end_date.isoformat()

    codes, stock_basic = _resolve_universe(data_manager, top_n, stock_codes)
    if not codes:
        logger.warning("screen_stocks: 股票池为空（stock_basic 无可用股票）")
        return None
    logger.info(f"选股股票池 {len(codes)} 只（period={period}, as_of={anchor}）")

    # 批量并发取数：一次调用覆盖全部代码，替代原逐股串行 get_stock_data
    frames = asyncio.run(data_manager.get_multiple_stocks_data_async(
        codes, start_date_str, end_date_str,
        frequency='1d', adjustment_type='hfq'))

    meta: Dict[str, tuple[str | None, str | None]] = {}
    if not stock_basic.is_empty() and 'ts_code' in stock_basic.columns:
        columns = [c for c in ('ts_code', 'name', 'industry') if c in stock_basic.columns]
        for row in stock_basic.select(columns).to_dicts():
            meta[row['ts_code']] = (row.get('name'), row.get('industry'))

    stocks_data_list = []
    for code, df in frames.items():
        if df is None or df.is_empty() or len(df) < 20:
            continue
        name, ind = meta.get(code, (None, None))
        stocks_data_list.append(df.with_columns([
            pl.lit(code).alias('stock_code'),
            pl.lit(name or f'股票{code}').alias('stock_name'),
            pl.lit(ind or '未知').alias('industry'),
        ]))

    if not stocks_data_list:
        return None
    return pl.concat(stocks_data_list)


def screen_stocks(
    stocks_data: pl.DataFrame | None = None,
    top_n: int = 10,
    algorithm: str = "多因子选股",
    industry: str = "全部行业",
    weights: Dict[str, float] | None = None,
    period: str = "12个月",
    today: date | str | None = None,
    data_manager: Any | None = None,
    stock_codes: List[str] | None = None,
    **params,
) -> List[Dict[str, Any]]:
    """
    选股 —— 返回 top_n 只候选股票的评分详情。

    Args:
        stocks_data: 股票面板 DataFrame。None 时自动取数（走 period 参数）。
        top_n:       返回前 N 只。
        algorithm:   '多因子选股' | '行业轮动' | '动量策略'。
        industry:    '全部行业' 或 具体行业名。
        weights:     因子权重字典。
        period:      自动取数时的分析周期：'1个月' / '3个月' / '6个月' / '12个月'。
        today:       分析截止日（默认取数据库最新交易日）；仅自动取数时生效，
                     显式传入可让选股区间可复现。
        data_manager: 数据管理器实例（自动取数时必填，用于批量取数与元数据查询）。
        stock_codes: 显式股票池；None 时用 stock_basic 按 ts_code 升序前 top_n*3 只。

    Returns:
        List[Dict] —— 每只股票含 score / risk / industry 等（不提供收益预测）。
    """
    if stocks_data is None:
        anchor = _coerce_date(today)
        panel_kwargs: Dict[str, Any] = {}
        if data_manager is not None:
            panel_kwargs['data_manager'] = data_manager
        if stock_codes is not None:
            panel_kwargs['stock_codes'] = stock_codes
        stocks_data = _fetch_panel(top_n, period, as_of=anchor, **panel_kwargs)
        if stocks_data is None:
            logger.error("screen_stocks: 无法获取面板数据")
            return []

    recommender = StockRecommender()
    results = recommender.recommend_stocks(
        stocks_data=stocks_data,
        top_n=top_n,
        algorithm=algorithm,
        industry=industry,
        weights=weights,
        **params,
    )
    logger.info(f"选股完成: algorithm={algorithm}, top_n={top_n}, 返回 {len(results)} 只")
    return results
