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

from typing import Any, Dict, List

import polars as pl
from loguru import logger

from src.recommendation.stock_recommender import StockRecommender

# 周期 → 日期范围
_PERIOD_MAP = {
    '1个月':  ('2024-11-01', '2024-12-31'),
    '3个月':  ('2024-10-01', '2024-12-31'),
    '6个月':  ('2024-07-01', '2024-12-31'),
    '12个月': ('2024-01-01', '2024-12-31'),
}


def _fetch_panel(top_n: int, period: str) -> pl.DataFrame | None:
    """
    从 DB 取选股面板数据（取 top_n*3 只股票的后复权日线）。
    放在 usecases 层，UI 层不会碰到 DatabaseManager。
    """
    from src.data.data_manager import DataManager
    from src.database.db_manager import DatabaseManager
    from src.utils.config import Config

    config = Config()
    db = DatabaseManager(config)
    data_manager = DataManager(config, db)

    start_date, end_date = _PERIOD_MAP.get(period, _PERIOD_MAP['12个月'])

    try:
        stock_basic = data_manager.get_stock_basic()
        if stock_basic.is_empty():
            return None

        stock_codes = stock_basic['ts_code'].to_list()[:top_n * 3]
        stocks_data_list = []

        for code in stock_codes:
            try:
                df = data_manager.get_stock_data(code, start_date, end_date, adjustment_type='hfq')
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

    Returns:
        List[Dict] —— 每只股票含 score / risk / expected_return / industry 等。
    """
    if stocks_data is None:
        stocks_data = _fetch_panel(top_n, period)
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
