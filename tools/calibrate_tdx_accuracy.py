#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
单股票通达信数据/指标校准脚本。

功能：
1. 比较通达信原始日线数据与项目数据（OHLCVA）
2. 计算项目指标（MA/MACD/RSI）
3. 可选对比通达信软件导出的指标CSV
4. 输出误差汇总与逐日明细到CSV
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List

import polars as pl
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data.data_manager import DataManager
from src.data.tdx_handler import TdxHandler
from src.tech_analysis.indicator_calculator import (calculate_ma_polars,
                                                    calculate_macd_polars,
                                                    calculate_rsi_polars)
from src.utils.config import get_config


def normalize_stock_code(stock_code: str) -> str:
    code = stock_code.strip().upper().replace(".SH", ".SH").replace(".SZ", ".SZ").replace(".BJ", ".BJ")
    if "." in code:
        left, right = code.split(".", 1)
        return f"{left}.{right.upper()}"
    if code.startswith("6"):
        return f"{code}.SH"
    if code.startswith("92") and len(code) == 6:
        return f"{code}.BJ"
    return f"{code}.SZ"


def ensure_date_column(df: pl.DataFrame) -> pl.DataFrame:
    if "date" in df.columns:
        return df.with_columns(
            pl.col("date").cast(pl.Date, strict=False)
        )

    if "trade_date" in df.columns:
        return df.with_columns(
            pl.col("trade_date").cast(pl.Utf8).str.strptime(pl.Date, format="%Y%m%d", strict=False).alias("date")
        )

    raise ValueError("数据缺少 date 或 trade_date 列")


def standardize_price_df(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    df = ensure_date_column(df)

    rename_map = {}
    if "vol" in df.columns and "volume" not in df.columns:
        rename_map["vol"] = "volume"
    if rename_map:
        df = df.rename(rename_map)

    base_cols = ["date", "open", "high", "low", "close", "volume", "amount"]
    available = [c for c in base_cols if c in df.columns]
    return df.select(available).sort("date")


def fetch_tdx_raw(ts_code: str, start_date: str, end_date: str) -> pl.DataFrame:
    config = get_config()
    tdx = TdxHandler(config=config, db_manager=None)
    lf = tdx.get_kline_data(ts_code, start_date, end_date, adjust="none")
    if hasattr(lf, "collect"):
        df = lf.collect()
    else:
        df = pl.DataFrame(lf)
    return standardize_price_df(df)


def fetch_project_data(ts_code: str, start_date: str, end_date: str) -> pl.DataFrame:
    config = get_config()
    manager = DataManager(config=config, db_manager=None, plugin_manager=None)
    df = manager.get_stock_data(
        stock_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        frequency="1d",
        adjustment_type="none",
    )
    if hasattr(df, "collect"):
        df = df.collect()
    return standardize_price_df(df)


def build_indicator_df(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    out = df
    out = calculate_ma_polars(out, windows=[5, 10, 20, 60])
    out = calculate_macd_polars(out, fast_period=12, slow_period=26, signal_period=9)
    out = calculate_rsi_polars(out, windows=[14])

    if hasattr(out, "collect"):
        out = out.collect()

    keep_cols = [
        "date",
        "ma5",
        "ma10",
        "ma20",
        "ma60",
        "macd",
        "macd_signal",
        "macd_hist",
        "rsi14",
    ]
    keep_cols = [c for c in keep_cols if c in out.columns]
    return out.select(keep_cols).sort("date")


def compare_frames(
    left: pl.DataFrame,
    right: pl.DataFrame,
    join_key: str,
    fields: List[str],
    left_suffix: str,
    right_suffix: str,
    tolerance: float,
) -> tuple[pl.DataFrame, Dict[str, Dict[str, float]]]:
    l_rename = {f: f"{f}_{left_suffix}" for f in fields if f in left.columns}
    r_rename = {f: f"{f}_{right_suffix}" for f in fields if f in right.columns}

    left2 = left.rename(l_rename)
    right2 = right.rename(r_rename)

    merged = left2.join(right2, on=join_key, how="inner")
    summary: Dict[str, Dict[str, float]] = {}

    for f in fields:
        lcol = f"{f}_{left_suffix}"
        rcol = f"{f}_{right_suffix}"
        if lcol not in merged.columns or rcol not in merged.columns:
            continue

        diff_col = f"abs_diff_{f}"
        merged = merged.with_columns((pl.col(lcol) - pl.col(rcol)).abs().alias(diff_col))

        diff_series = merged[diff_col]
        summary[f] = {
            "max_abs_diff": float(diff_series.max() or 0.0),
            "mean_abs_diff": float(diff_series.mean() or 0.0),
            "over_tolerance_count": int((diff_series > tolerance).sum()),
            "samples": int(diff_series.len()),
        }

    return merged, summary


def normalize_tdx_indicator_csv(path: Path) -> pl.DataFrame:
    df = pl.read_csv(path, infer_schema_length=5000)

    rename_candidates = {
        "日期": "date",
        "时间": "date",
        "DATE": "date",
        "Date": "date",
        "MA5": "ma5",
        "MA10": "ma10",
        "MA20": "ma20",
        "MA60": "ma60",
        "DIF": "macd",
        "DEA": "macd_signal",
        "MACD": "macd_hist",
        "RSI": "rsi14",
        "RSI14": "rsi14",
    }

    existing_map = {k: v for k, v in rename_candidates.items() if k in df.columns}
    if existing_map:
        df = df.rename(existing_map)

    if "date" not in df.columns:
        raise ValueError("通达信导出指标CSV缺少日期列（日期/date）")

    if df["date"].dtype != pl.Date:
        df = df.with_columns(
            pl.col("date").cast(pl.Utf8).str.replace_all("/", "-").str.strptime(pl.Date, strict=False).alias("date")
        )

    keep = ["date", "ma5", "ma10", "ma20", "ma60", "macd", "macd_signal", "macd_hist", "rsi14"]
    keep = [c for c in keep if c in df.columns]
    return df.select(keep).sort("date")


def save_summary(summary: Dict[str, Dict[str, float]], out_file: Path) -> None:
    rows = []
    for metric, stats in summary.items():
        rows.append(
            {
                "metric": metric,
                "max_abs_diff": stats["max_abs_diff"],
                "mean_abs_diff": stats["mean_abs_diff"],
                "over_tolerance_count": stats["over_tolerance_count"],
                "samples": stats["samples"],
            }
        )
    pl.DataFrame(rows).write_csv(out_file)


def main() -> None:
    parser = argparse.ArgumentParser(description="通达信单股票数据与指标校准")
    parser.add_argument("--stock", default="000001.SZ", help="股票代码，如 000001.SZ")
    parser.add_argument("--start", required=True, help="开始日期，格式 YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="结束日期，格式 YYYY-MM-DD")
    parser.add_argument("--tolerance", type=float, default=1e-6, help="数值误差阈值")
    parser.add_argument(
        "--tdx-indicator-csv",
        default="",
        help="可选：通达信软件导出的指标CSV路径，用于指标对账",
    )
    parser.add_argument(
        "--out-dir",
        default="logs/calibration",
        help="输出目录",
    )

    args = parser.parse_args()
    ts_code = normalize_stock_code(args.stock)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"开始校准: {ts_code}, 区间 {args.start} ~ {args.end}")

    tdx_df = fetch_tdx_raw(ts_code, args.start, args.end)
    project_df = fetch_project_data(ts_code, args.start, args.end)

    if tdx_df.is_empty() or project_df.is_empty():
        raise RuntimeError("通达信或项目数据为空，无法比对。请检查代码、日期区间与数据路径配置")

    price_fields = ["open", "high", "low", "close", "volume", "amount"]
    price_detail, price_summary = compare_frames(
        left=tdx_df,
        right=project_df,
        join_key="date",
        fields=price_fields,
        left_suffix="tdx",
        right_suffix="project",
        tolerance=args.tolerance,
    )

    save_summary(price_summary, out_dir / f"{ts_code}_price_summary.csv")
    price_detail.write_csv(out_dir / f"{ts_code}_price_detail.csv")

    logger.info("已完成K线字段比对")

    project_indicators = build_indicator_df(project_df)
    project_indicators.write_csv(out_dir / f"{ts_code}_project_indicators.csv")

    if args.tdx_indicator_csv:
        tdx_indicator_df = normalize_tdx_indicator_csv(Path(args.tdx_indicator_csv))
        indicator_fields = ["ma5", "ma10", "ma20", "ma60", "macd", "macd_signal", "macd_hist", "rsi14"]
        ind_detail, ind_summary = compare_frames(
            left=tdx_indicator_df,
            right=project_indicators,
            join_key="date",
            fields=indicator_fields,
            left_suffix="tdx_soft",
            right_suffix="project",
            tolerance=args.tolerance,
        )
        save_summary(ind_summary, out_dir / f"{ts_code}_indicator_summary.csv")
        ind_detail.write_csv(out_dir / f"{ts_code}_indicator_detail.csv")
        logger.info("已完成指标字段比对")
    else:
        logger.info("未提供 --tdx-indicator-csv，已跳过通达信软件指标对账")

    logger.info(f"校准完成，结果目录: {out_dir.resolve()}")


if __name__ == "__main__":
    main()
