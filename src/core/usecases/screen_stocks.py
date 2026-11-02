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

import calendar
from datetime import date, datetime
from typing import Any, Dict, List

import polars as pl
from loguru import logger

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


def _fetch_panel(top_n: int, period: str, as_of: date | None = None) -> pl.DataFrame | None:
    """
    从 DB 取选股面板数据（取 top_n*3 只股票的后复权日线）。
    放在 usecases 层，UI 层不会碰到 DatabaseManager。

    Args:
        as_of: 分析截止日（最新交易日的显式覆盖，主要供测试固定 today）。
    """
    from src.data.data_manager import DataManager
    from src.database.db_manager import DatabaseManager
    from src.utils.config import Config

    config = Config()
    db = DatabaseManager(config)
    data_manager = DataManager(config, db)

    anchor = as_of or _latest_trade_date(db)
    start_date, end_date = _period_range(period, anchor)
    start_date_str, end_date_str = start_date.isoformat(), end_date.isoformat()

    try:
        stock_basic = data_manager.get_stock_basic()
        if stock_basic.is_empty():
            return None

        stock_codes = stock_basic['ts_code'].to_list()[:top_n * 3]
        stocks_data_list = []

        for code in stock_codes:
            try:
                df = data_manager.get_stock_data(code, start_date_str, end_date_str, adjustment_type='hfq')
                if not df.is_empty() and len(df) >= 20:
                    info = stock_basic.filter(pl.col('ts_code') == code)
                    name = info['name'].to_list()[0] if not info.is_empty() else f'股票{code}'
                    ind = info['industry'].to_list()[0] if 'industry' in info.columns else '未知'
                    df = df.with_columns([
                        pl.lit(code).alias('stock_code'),
                        pl.lit(name).alias('stock_name'),
                        pl.lit(ind).alias('industry'),
                    ])
                    stocks_data_list.append(df)
            # 兜底理由：单只股票取数失败跳过
            except Exception as e:  # noqa: BLE001
                logger.debug(f"取 {code} 数据失败: {e}")

        if not stocks_data_list:
            return None
        return pl.concat(stocks_data_list)

    finally:
        db.disconnect()


def screen_stocks(
    stocks_data: pl.DataFrame | None = None,
    top_n: int = 10,
    algorithm: str = "多因子选股",
    industry: str = "全部行业",
    weights: Dict[str, float] | None = None,
    period: str = "12个月",
    today: date | str | None = None,
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

    Returns:
        List[Dict] —— 每只股票含 score / risk / expected_return / industry 等。
    """
    if stocks_data is None:
        anchor = _coerce_date(today)
        stocks_data = _fetch_panel(top_n, period, as_of=anchor)
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
