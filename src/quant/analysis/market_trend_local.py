"""市场趋势状态：本地日频数据装配与 CLI 入口（只读，不写 data/）。"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

import polars as pl
from loguru import logger

from src.data.local_store.store import resolve_store_root
from src.quant.analysis.market_trend_state import (
    PRIMARY_INDEX,
    evaluate_market_trend,
    format_state_detail,
)
from src.utils.config import get_config


def _default_root() -> Path:
    return resolve_store_root(get_config())


def load_local_panel(root: Path | str | None = None) -> dict[str, pl.DataFrame]:
    """读取判定所需的本地日频表；不新增外部采集。"""
    base = Path(root) if root is not None else _default_root()
    market = PRIMARY_INDEX.rsplit('.', 1)[-1].lower()
    return {
        'index': pl.read_parquet(
            base / 'quotes' / 'index' / market / f'{PRIMARY_INDEX}.parquet',
            columns=['date', 'close', 'amount'],
        ),
        'breadth_ma': pl.read_parquet(
            base / 'meta' / 'breadth' / 'breadth_ma_daily.parquet',
            columns=['date', 'pct_above_ma60', 'pct_above_ma120'],
        ),
        'breadth': pl.read_parquet(
            base / 'meta' / 'breadth' / 'breadth_daily.parquet',
            columns=[
                'trade_date', 'up_count', 'down_count', 'total_count',
                'limit_up_count', 'limit_down_count', 'new_high_250', 'new_low_250',
            ],
        ),
        'state_vector': pl.read_parquet(
            base / 'meta' / 'state' / 'state_vector_daily.parquet',
            columns=[
                'date', 'valuation_erp_q', 'val_earn_yield_q',
                'lev_margin_buy_ratio_q',
            ],
        ),
        'margin': pl.read_parquet(
            base / 'meta' / 'macro' / 'cn_market_margin_daily.parquet',
            columns=['date', 'market', 'fin_balance'],
        ),
    }


def run_cli(asof: str | None = None, panel_root: str | None = None) -> None:
    target = datetime.strptime(asof, '%Y-%m-%d').date() if asof else None
    state = evaluate_market_trend(load_local_panel(panel_root), asof=target)
    for line in format_state_detail(state).splitlines():
        logger.info(line)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='市场趋势状态判定（只读本地数据）')
    parser.add_argument('--asof', help='判定截止日，格式 YYYY-MM-DD')
    parser.add_argument('--panel-root', help='本地列存根目录，默认按配置解析')
    args = parser.parse_args(argv)
    run_cli(asof=args.asof, panel_root=args.panel_root)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
