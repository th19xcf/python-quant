#!/usr/bin/env python3

"""QuantStats 绩效 tearsheet（只读分析层）。

定位：给现有 ``ReportGenerator`` 增加一条**可选**的深报告路径，不替换、不重构
既有三个对话框（回测 / 推荐 / 因子）的报告逻辑。

口径说明：
1. 输入是回测的 ``equity_curve``（``[{date, equity}, ...]``）或等价的
   pandas Series/DataFrame；本项目回测输出即 list[dict]。
2. QuantStats 内部把净值转成**简单日收益**，指标均为**小数**（不是百分数）：
   ``max_drawdown`` 取绝对值后为正数小数（例如 0.12 表示回撤 12%），
   ``annual_return`` 为 CAGR 小数。与 ``PerformanceAnalyzer`` 的百分数输出
   相差 100 倍，跨模块对比时必须显式换算。
3. 净值序列必须：DatetimeIndex、升序、无重复、严格为正、至少 2 个点；
   空序列 / 单点 / 重复索引 / 非日期索引 / 非正净值都会显式抛 ValueError，
   禁止静默画出空图。
4. matplotlib 强制 ``Agg`` 后端，避免无 GUI 环境弹窗或报错。

缺失 ``analytics`` 可选依赖组时，本模块抛 RuntimeError，不影响回测主流程。
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

_QUANTSTATS_AVAILABLE = True
_QUANTSTATS_IMPORT_ERROR = ''
try:  # pragma: no cover - 依赖可选组，正常安装 analytics 时走 try 分支
    import matplotlib

    matplotlib.use('Agg', force=True)
    import quantstats as qs
except ImportError as _exc:  # pragma: no cover - 仅在未安装 analytics 组时触发
    _QUANTSTATS_AVAILABLE = False
    _QUANTSTATS_IMPORT_ERROR = str(_exc)
    qs = None  # type: ignore[assignment]


def _require_quantstats() -> Any:
    if not _QUANTSTATS_AVAILABLE:
        raise RuntimeError(
            "QuantStats 依赖不可用，请执行 `poetry install --with analytics` "
            f"（原始错误：{_QUANTSTATS_IMPORT_ERROR}）"
        )
    return qs


def _series_from_pandas_series(equity: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(equity.index):
        raise ValueError(
            "净值 Series 的索引必须是 DatetimeIndex，不能是数值索引"
            "（数值会被误解析为 1970 年纪元时间戳）")
    series = equity.astype(float).copy()
    index = pd.to_datetime(series.index, errors='coerce')
    if pd.isna(index).any():
        raise ValueError("净值 Series 的索引存在无法解析为日期的值")
    series.index = pd.DatetimeIndex(index, name='date')
    return series


def _series_from_dataframe(frame: pd.DataFrame, date_col: str,
                           equity_col: str) -> pd.Series:
    missing = [c for c in (date_col, equity_col) if c not in frame.columns]
    if missing:
        raise ValueError(
            f"权益 DataFrame 缺少列 {missing}；实际列 {list(frame.columns)}")
    frame = frame[[date_col, equity_col]].copy()
    frame[date_col] = pd.to_datetime(frame[date_col], errors='coerce')
    if frame[date_col].isna().any():
        raise ValueError("权益 DataFrame 的日期列存在无法解析的值")
    if frame[equity_col].isna().any():
        raise ValueError("权益 DataFrame 的净值列存在缺失值")
    return pd.Series(
        frame[equity_col].to_numpy(dtype=float),
        index=pd.DatetimeIndex(frame[date_col], name='date'),
        name=equity_col,
    )


def _series_from_records(records: Any, date_col: str, equity_col: str) -> pd.Series:
    if not records:
        raise ValueError("权益曲线为空，无法生成 QuantStats 报告")
    if not all(isinstance(item, dict) for item in records):
        raise ValueError("权益曲线条目必须为 dict（含 date/equity 字段）")
    dates = [item.get(date_col) for item in records]
    equities = [item.get(equity_col) for item in records]
    if any(value is None for value in dates):
        raise ValueError(f"权益曲线存在缺少 '{date_col}' 字段的条目")
    if any(value is None for value in equities):
        raise ValueError(f"权益曲线存在缺少 '{equity_col}' 字段的条目")
    parsed = pd.to_datetime(pd.Series(dates), errors='coerce')
    if parsed.isna().any():
        raise ValueError("权益曲线存在无法解析为日期的条目")
    return pd.Series(
        np.asarray(equities, dtype=float),
        index=pd.DatetimeIndex(parsed, name='date'),
        name=equity_col,
    )


def _validate_equity(series: pd.Series) -> pd.Series:
    series = series.replace([np.inf, -np.inf], np.nan)
    if series.isna().any():
        raise ValueError("净值序列存在 NaN/Inf，无法计算绩效")
    if (series <= 0).any():
        raise ValueError("净值序列必须严格为正（存在 <= 0 的净值）")
    if len(series) == 0:
        raise ValueError("权益曲线为空，无法生成 QuantStats 报告")
    if len(series) < 2:
        raise ValueError(
            f"权益曲线只有 {len(series)} 个净值点，至少需要 2 个（单日无法计算收益）")
    if series.index.has_duplicates:
        dup = series.index[series.index.duplicated()].unique()
        raise ValueError(
            f"净值序列存在 {len(dup)} 个重复日期，例如 {list(map(str, dup[:3]))}；"
            "请先按日期去重")
    if not series.index.is_monotonic_increasing:
        logger.info("[quantstats] 净值序列未按日期升序，已自动升序排序")
        series = series.sort_index()
    series.index.name = 'date'
    return series


def equity_curve_to_series(equity_curve: Any, date_col: str = 'date',
                           equity_col: str = 'equity') -> pd.Series:
    """把回测权益曲线统一成以 DatetimeIndex 为索引的净值 Series。

    支持四种输入：
    - ``list[dict]``：本项目回测输出的 ``[{date, equity}, ...]``；
    - ``pd.DataFrame``：含 ``date_col/equity_col`` 两列；
    - ``pd.Series``：索引为日期、值为净值；
    - ``pl.DataFrame``：转 pandas 后按 DataFrame 处理。

    Raises:
        TypeError: 输入类型不支持。
        ValueError: 空序列、单点、日期非法、重复日期、净值非正/含缺失。
    """
    if isinstance(equity_curve, pd.Series):
        return _validate_equity(_series_from_pandas_series(equity_curve))
    if isinstance(equity_curve, pd.DataFrame):
        return _validate_equity(
            _series_from_dataframe(equity_curve, date_col, equity_col))
    if isinstance(equity_curve, (list, tuple)):
        return _validate_equity(
            _series_from_records(equity_curve, date_col, equity_col))
    # Polars DataFrame 走 duck typing，避免强依赖 polars 类型
    if hasattr(equity_curve, 'to_pandas'):
        return equity_curve_to_series(
            equity_curve.to_pandas(), date_col, equity_col)
    raise TypeError(
        f"权益曲线必须为 list[dict] / pandas Series / DataFrame，"
        f"实际为 {type(equity_curve).__name__}")


def build_quantstats_metrics(equity_curve: Any) -> dict[str, Any]:
    """计算 QuantStats 关键指标（小数口径），不生成 HTML。

    与 ``PerformanceAnalyzer`` 的同名指标对比时注意：
    - 本函数 ``annual_return`` 是 QuantStats CAGR（按 252 个交易日折算）；
    - ``max_drawdown`` / ``volatility`` / ``win_rate`` 均为小数；
    - ``win_rate`` 是 QuantStats 的"上涨日占比"，不是回测的"配对交易胜率"。
    """
    qs = _require_quantstats()
    equity = equity_curve_to_series(equity_curve)
    returns = equity.pct_change().dropna()

    def _f(value: Any) -> float:
        value = float(value)
        return value

    metrics = {
        'start_date': equity.index[0].strftime('%Y-%m-%d'),
        'end_date': equity.index[-1].strftime('%Y-%m-%d'),
        'n_periods': int(len(returns)),
        'initial_equity': float(equity.iloc[0]),
        'final_equity': float(equity.iloc[-1]),
        'total_return': _f(equity.iloc[-1] / equity.iloc[0] - 1),
        'annual_return': _f(qs.stats.cagr(returns)),
        'sharpe': _f(qs.stats.sharpe(returns)),
        'sortino': _f(qs.stats.sortino(returns)),
        'max_drawdown': abs(_f(qs.stats.max_drawdown(returns))),
        'volatility': _f(qs.stats.volatility(returns)),
        'calmar': _f(qs.stats.calmar(returns)),
        'win_rate': _f(qs.stats.win_rate(returns)),
        'profit_factor': _f(qs.stats.profit_factor(returns)),
        'best_day': _f(qs.stats.best(returns)),
        'worst_day': _f(qs.stats.worst(returns)),
        'avg_win': _f(qs.stats.avg_win(returns)),
        'avg_loss': _f(qs.stats.avg_loss(returns)),
        'ulcer_index': _f(qs.stats.ulcer_index(returns)),
    }
    return metrics


def generate_quantstats_report(
    equity_curve: Any,
    output_path: str | Path | None = None,
    *,
    title: str = '策略绩效 Tearsheet',
    periods_per_year: int = 252,
    figfmt: str = 'svg',
) -> dict[str, Any]:
    """用 QuantStats 生成 HTML tearsheet，并返回指标 dict 供界面显示。

    Args:
        equity_curve: 回测权益曲线（list[dict] / DataFrame / Series）。
        output_path: HTML 输出路径；默认 ``reports/quantstats/quantstats_<时间戳>.html``。
        title: 报告标题。
        periods_per_year: 年化期数（日线 252；非日线回测需显式传入）。
        figfmt: 图表格式（svg/png）。

    Returns:
        dict: ``report_path`` / ``metrics``（见 ``build_quantstats_metrics``）。
    """
    qs = _require_quantstats()
    equity = equity_curve_to_series(equity_curve)
    returns = equity.pct_change().dropna()
    metrics = build_quantstats_metrics(equity)

    if output_path is None:
        out_dir = Path('reports') / 'quantstats'
        out_dir.mkdir(parents=True, exist_ok=True)
        output_path = out_dir / f'quantstats_{datetime.now():%Y%m%d_%H%M%S}.html'
    else:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

    qs.reports.html(
        returns,
        benchmark=None,
        output=str(output_path),
        title=title,
        download_filename=output_path.name,
        periods_per_year=periods_per_year,
        figfmt=figfmt,
    )
    logger.info(f"QuantStats tearsheet: {output_path}")
    return {
        'report_path': str(output_path),
        'metrics': metrics,
        'equity_curve': equity,
        'returns': returns,
    }


__all__ = [
    'equity_curve_to_series',
    'build_quantstats_metrics',
    'generate_quantstats_report',
]
