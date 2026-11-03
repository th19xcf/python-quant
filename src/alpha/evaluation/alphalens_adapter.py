#!/usr/bin/env python3

"""Alphalens 适配层（只读分析，不参与撮合与数据写入）。

口径说明（务必与回测撮合对齐，禁止引入"当日收盘成交"式未来函数）：

1. **价格口径**：面板里的 ``open/high/low/close`` 必须是后复权（hfq）价。
   项目规则要求量化场景统一使用后复权，K 线展示才用前复权；适配层不做任何
   复权换算，直接使用调用方传入的 hfq 面板。
2. **信号与建仓**：因子在 T 日收盘后由 [T 及之前] 的数据计算得出，信号只能在
   T+1 执行——与 ``BacktestEngine`` 的"t 日信号延迟到 t+1 日以开盘价成交"一致。
   默认 ``entry_price='open'``：把因子索引整体前移一个交易日（T → T+1），
   Alphalens 再用 T+1 的 open 价计算 T+1 → T+1+N 的收益，等价于
   "T+1 开盘建仓、持有 N 个交易日、第 N+1 个交易日开盘平仓"。
   若显式传 ``entry_price='close'``，则以 T+1 收盘价建仓，同样是 T+1 执行，
   不是"T 日收盘成交"。
3. **持有期**：``periods`` 以交易日（bar）计，不是自然日；N 日收益由
   Alphalens 的 ``prices.ffill().pct_change(N).shift(-N)`` 计算，停牌日按前值
   前向填充（这是因子分析的通行口径，与撮合的"停牌不可交易"不同，见告警）。
4. **IC 口径**：逐期截面 Spearman Rank IC（Alphalens
   ``factor_information_coefficient``），ICIR = mean(IC) / std(IC)（样本标准差
   ddof=1）。这与旧的单序列时序 IC（``FactorEvaluator``，模块内标注 ts_ic）
   完全不同：时序 IC 在选股场景不可信，仅作兼容保留。

matplotlib 强制 ``Agg`` 后端：服务器/无 GUI 环境不会弹窗，也不会因缺少
display 报错。缺少 ``analytics`` 可选依赖组时抛 RuntimeError，不影响回测主流程。
"""

from __future__ import annotations

import base64
import io
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, Sequence

import pandas as pd
import polars as pl
from loguru import logger

_ALPHALENS_AVAILABLE = True
_ALPHALENS_IMPORT_ERROR = ''
try:  # pragma: no cover - 依赖可选组，正常安装 analytics 时走 try 分支
    import matplotlib

    # 必须在 pyplot 之前设定，且 force=True 保证已有 backend 被覆盖
    matplotlib.use('Agg', force=True)
    import alphalens.performance as _aperf
    import alphalens.utils as _autils
    import matplotlib.pyplot as plt
except ImportError as _exc:  # pragma: no cover - 仅在未安装 analytics 组时触发
    _ALPHALENS_AVAILABLE = False
    _ALPHALENS_IMPORT_ERROR = str(_exc)
    plt = None  # type: ignore[assignment]
    _aperf = None  # type: ignore[assignment]
    _autils = None  # type: ignore[assignment]


_ENTRY_PRICES = ('open', 'close')


def _require_alphalens() -> tuple[Any, Any]:
    """惰性校验依赖：未安装 analytics 组时给出明确指引。"""
    if not _ALPHALENS_AVAILABLE:
        raise RuntimeError(
            "Alphalens 依赖不可用，请执行 `poetry install --with analytics` "
            f"（原始错误：{_ALPHALENS_IMPORT_ERROR}）"
        )
    return _autils, _aperf


@dataclass(frozen=True)
class AlphalensConfig:
    """因子评估配置（参与缓存/口径判定，不含路径类参数）。"""

    factor_column: str
    periods: tuple[int, ...] = (1, 5, 10, 20)
    quantiles: int = 5
    entry_price: Literal['open', 'close'] = 'open'
    max_loss: float = 0.35

    def __post_init__(self) -> None:
        if self.entry_price not in _ENTRY_PRICES:
            raise ValueError(
                f"entry_price 必须为 {_ENTRY_PRICES} 之一，实际为 {self.entry_price!r}")
        if not self.periods or any(int(p) <= 0 for p in self.periods):
            raise ValueError(f"periods 必须为正整数序列，实际为 {self.periods!r}")
        if int(self.quantiles) < 2:
            raise ValueError(f"quantiles 至少为 2，实际为 {self.quantiles}")


def _to_pandas(panel: Any) -> pd.DataFrame:
    """把 Polars/pandas 面板统一为 pandas（只做格式转换，不改数值）。"""
    if isinstance(panel, pl.DataFrame):
        return panel.to_pandas()
    if isinstance(panel, pd.DataFrame):
        return panel.copy()
    raise TypeError(f"面板必须为 Polars/pandas DataFrame，实际为 {type(panel).__name__}")


def _prepare_panel(panel: Any, config: AlphalensConfig, code_col: str,
                   date_col: str) -> pd.DataFrame:
    """校验列、清洗日期/数值、按 (date, asset) 去重（保留最后一条）。"""
    frame = _to_pandas(panel)
    if frame.empty:
        raise ValueError("因子面板为空，无法进行 Alphalens 评估")

    required = [code_col, date_col, config.factor_column, config.entry_price]
    missing = [col for col in required if col not in frame.columns]
    if missing:
        raise ValueError(
            f"因子面板缺少列 {missing}；实际列 {list(frame.columns)}")

    frame = frame[[code_col, date_col, config.factor_column, config.entry_price]].copy()
    frame[date_col] = pd.to_datetime(frame[date_col], errors='coerce')
    frame[config.factor_column] = pd.to_numeric(
        frame[config.factor_column], errors='coerce')
    frame[config.entry_price] = pd.to_numeric(
        frame[config.entry_price], errors='coerce')

    frame = frame.dropna(subset=[code_col, date_col])
    frame = frame[frame[date_col].notna()]
    # 只保留有限数值：inf 会污染 pivot 与相关系数
    factor_vals = frame[config.factor_column].to_numpy(dtype=float)
    price_vals = frame[config.entry_price].to_numpy(dtype=float)
    frame = frame[~(pd.Series(factor_vals, index=frame.index).isin(
        [float('inf'), float('-inf')]))]
    frame = frame[~(pd.Series(price_vals, index=frame.index).isin(
        [float('inf'), float('-inf')]))]

    duplicated = int(frame.duplicated(subset=[date_col, code_col], keep='last').sum())
    if duplicated:
        logger.warning(
            f"[alphalens] 面板存在 {duplicated} 条重复 (date, asset) 记录，已保留最后一条")
        frame = frame.drop_duplicates(subset=[date_col, code_col], keep='last')

    frame[date_col] = frame[date_col].dt.normalize()
    return frame.sort_values([date_col, code_col]).reset_index(drop=True)


def _shift_to_execution_dates(factor: pd.Series, price_index: pd.DatetimeIndex) -> pd.Series:
    """把因子索引从信号日 T 前移到执行日 T+1（T+1 不可执行则丢弃）。

    这是"信号 T 日产生、T+1 建仓"的唯一实现点：Alphalens 的 N 日收益是
    从因子索引所在日开始算的，索引前移一日即等价于 T+1 建仓。
    """
    dates = pd.DatetimeIndex(factor.index.get_level_values('date'))
    # side='right'：严格取下一个交易日；因子日不在价格索引中时同样落到其后首个交易日
    positions = price_index.searchsorted(dates, side='right')
    keep = positions < len(price_index)
    if not keep.any():
        raise ValueError(
            "所有因子信号日均无后继交易日，无法按 T+1 建仓口径评估（数据区间过短）")

    exec_dates = price_index[positions[keep]]
    assets = factor.index.get_level_values('asset')[keep]
    shifted = pd.Series(
        factor.to_numpy()[keep],
        index=pd.MultiIndex.from_arrays(
            [exec_dates, assets], names=['date', 'asset']),
        name=factor.name,
    )
    shifted = shifted[~shifted.index.duplicated(keep='last')].sort_index()
    return shifted


def build_alphalens_inputs(panel: Any, config: AlphalensConfig,
                           code_col: str = 'ts_code',
                           date_col: str = 'date',
                           ) -> tuple[pd.Series, pd.DataFrame]:
    """把长表因子面板整理成 Alphalens 的 ``factor`` / ``prices``。

    Args:
        panel: 含 ``code_col/date_col/factor_column/entry_price`` 的长表，
            价格列必须是后复权价。
        config: 评估配置（决定因子列与建仓价格）。
        code_col: 标的代码列名，默认 ``ts_code``。
        date_col: 交易日列名，默认 ``date``。

    Returns:
        (factor, prices):
            factor: MultiIndex(date, asset) 的因子值，date 已前移到执行日 T+1；
            prices: index=日期，columns=asset 的建仓价宽表（hfq）。

    Raises:
        ValueError: 空面板、缺列、日期不足两个交易日、无有效执行日等。
    """
    frame = _prepare_panel(panel, config, code_col, date_col)

    prices = frame.pivot_table(
        index=date_col, columns=code_col, values=config.entry_price,
        aggfunc='last',
    ).sort_index()
    prices = prices[~prices.index.duplicated(keep='last')]
    prices.index.name = 'date'
    # 全空列（该标的在整个区间都无有效价格）直接剔除
    prices = prices.dropna(axis=1, how='all')
    if prices.shape[0] < 2 or prices.shape[1] < 2:
        raise ValueError(
            f"有效数据不足：需要至少 2 个交易日、2 个标的，实际 "
            f"{prices.shape[0]} 日 × {prices.shape[1]} 标的")
    if not prices.index.is_monotonic_increasing:
        prices = prices.sort_index()

    factor = frame.set_index([date_col, code_col])[config.factor_column]
    factor.index = factor.index.set_names(['date', 'asset'])
    factor = factor.dropna()
    # 价格全空的资产不应参与 Alphalens（否则 ffill 会制造虚假收益）
    valid_assets = set(prices.columns)
    factor = factor[
        factor.index.get_level_values('asset').isin(valid_assets)]
    factor = _shift_to_execution_dates(factor, pd.DatetimeIndex(prices.index))
    if len(factor.index.levels[0]) < 2:
        raise ValueError(
            "按 T+1 建仓后有效信号日不足 2 个，无法计算 IC 序列")
    return factor, prices


def _analyze_factor(factor: pd.Series, prices: pd.DataFrame,
                    config: AlphalensConfig) -> tuple[dict[str, Any], dict[str, Any]]:
    """一次性计算指标与绘图所需的中间对象（避免重复跑 Alphalens）。"""
    _autils, _aperf = _require_alphalens()

    factor_data = _autils.get_clean_factor_and_forward_returns(
        factor,
        prices,
        quantiles=config.quantiles,
        periods=config.periods,
        max_loss=config.max_loss,
    )
    if factor_data.empty:
        raise ValueError("Alphalens 清洗后无有效样本（请检查价格区间与 max_loss）")

    period_cols = list(_autils.get_forward_returns_columns(factor_data.columns))
    if not period_cols:
        raise ValueError("Alphalens 未生成任何前向收益列")

    # 逐期截面 Rank IC：index=date, columns=period
    ic = _aperf.factor_information_coefficient(factor_data)
    ic_mean: dict[str, float] = {}
    ic_std: dict[str, float] = {}
    icir: dict[str, float] = {}
    ic_positive: dict[str, float] = {}
    for col in period_cols:
        series = ic[col].dropna()
        mean = float(series.mean()) if len(series) else float('nan')
        std = float(series.std(ddof=1)) if len(series) > 1 else float('nan')
        ic_mean[col] = mean
        ic_std[col] = std
        icir[col] = mean / std if std and math.isfinite(std) and std != 0 else float('nan')
        ic_positive[col] = float((series > 0).mean()) if len(series) else float('nan')

    # 分层收益：绝对收益（不按当日横截面去均值），index=factor_quantile
    mean_ret, _std_ret = _aperf.mean_return_by_quantile(
        factor_data, by_date=False, demeaned=False)
    if isinstance(mean_ret.index, pd.MultiIndex) and 'group' in mean_ret.index.names:
        mean_ret = mean_ret.xs(mean_ret.index.get_level_values('group')[0],
                              level='group', drop_level=True)
    quantile_returns: dict[str, dict[str, float]] = {}
    long_short: dict[str, float] = {}
    for col in period_cols:
        per_q = {str(int(q)): float(v) for q, v in mean_ret[col].items()}
        quantile_returns[col] = per_q
        quantiles_sorted = sorted(int(q) for q in mean_ret.index)
        if quantiles_sorted:
            long_short[col] = per_q[str(quantiles_sorted[-1])] - per_q[str(quantiles_sorted[0])]
        else:
            long_short[col] = float('nan')

    # 换手率：顶/底分位换手（1 期），以及因子秩自相关（越高越稳定）
    quantile_factor = factor_data['factor_quantile']
    top_turnover = _aperf.quantile_turnover(
        quantile_factor, quantile=int(quantile_factor.max()), period=1)
    bottom_turnover = _aperf.quantile_turnover(
        quantile_factor, quantile=int(quantile_factor.min()), period=1)
    rank_autocorr = _aperf.factor_rank_autocorrelation(factor_data, period=1)

    metrics = {
        'periods': period_cols,
        'n_observations': int(len(factor_data)),
        'n_dates': int(factor_data.index.get_level_values('date').nunique()),
        'n_assets': int(factor_data.index.get_level_values('asset').nunique()),
        'ic_mean': ic_mean,
        'ic_std': ic_std,
        'icir': icir,
        'ic_positive_ratio': ic_positive,
        # IC 衰减曲线 = 每个持有期的平均 IC
        'ic_decay': dict(ic_mean),
        'quantile_returns': quantile_returns,
        'long_short_return': long_short,
        'turnover': {
            'top_quantile_mean': float(top_turnover.mean()) if len(top_turnover) else float('nan'),
            'bottom_quantile_mean': float(bottom_turnover.mean()) if len(bottom_turnover) else float('nan'),
            'rank_autocorrelation_mean': float(rank_autocorr.mean()) if len(rank_autocorr) else float('nan'),
        },
    }
    artifacts = {'ic': ic, 'mean_ret': mean_ret, 'top_turnover': top_turnover}
    return metrics, artifacts


def evaluate_factor_cross_section(factor: pd.Series, prices: pd.DataFrame,
                                  config: AlphalensConfig) -> dict[str, Any]:
    """用 Alphalens 计算截面 IC / ICIR / IC 衰减 / 分层收益 / 换手率。

    不含 IO，可被单元测试直接调用；输出全部为 Python 原生类型。
    """
    metrics, _artifacts = _analyze_factor(factor, prices, config)
    return metrics


def _fig_to_base64(fig: Any) -> str:
    """把 matplotlib 图保存为 base64 PNG（不落临时文件）。"""
    buffer = io.BytesIO()
    fig.savefig(buffer, format='png', dpi=110, bbox_inches='tight')
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode('ascii')


def _render_tearsheet(metrics: dict[str, Any], artifacts: dict[str, Any],
                      config: AlphalensConfig, title: str) -> str:
    """生成轻量 HTML tearsheet（内嵌 PNG，无外部资源依赖）。"""
    ic = artifacts['ic']
    mean_ret = artifacts['mean_ret']
    top_turnover = artifacts['top_turnover']

    images: list[tuple[str, str]] = []
    period_cols = metrics['periods']
    primary_col = period_cols[min(1, len(period_cols) - 1)]

    # 1) IC 衰减曲线
    fig, ax = plt.subplots(figsize=(6, 3.2))
    values = [metrics['ic_mean'].get(col, float('nan')) for col in period_cols]
    ax.bar(period_cols, values, color='#2f6fbf')
    ax.axhline(0, color='#888', linewidth=0.8)
    ax.set_title('IC decay (mean rank IC per holding period)')
    ax.set_ylabel('mean IC')
    images.append(('IC 衰减', _fig_to_base64(fig)))

    # 2) 主持有期 IC 时序
    fig, ax = plt.subplots(figsize=(9, 3.2))
    ax.plot(ic.index, ic[primary_col], color='#2f6fbf', linewidth=1)
    ax.axhline(0, color='#888', linewidth=0.8)
    ax.set_title(f'Rank IC time series ({primary_col})')
    ax.set_ylabel('IC')
    images.append(('IC 时序', _fig_to_base64(fig)))

    # 3) 分层收益（主持有期）
    fig, ax = plt.subplots(figsize=(6, 3.2))
    qs = list(mean_ret.index)
    vals = [float(mean_ret.loc[q, primary_col]) for q in qs]
    ax.bar([str(int(q)) for q in qs], vals, color='#c0504d')
    ax.axhline(0, color='#888', linewidth=0.8)
    ax.set_title(f'Mean quantile returns ({primary_col})')
    ax.set_xlabel('factor quantile')
    ax.set_ylabel('return')
    images.append(('分层收益', _fig_to_base64(fig)))

    # 4) 顶分位换手率
    fig, ax = plt.subplots(figsize=(9, 3.0))
    ax.plot(top_turnover.index, top_turnover.to_numpy(), color='#4f8a4f', linewidth=1)
    ax.set_title('Top-quantile turnover (period=1)')
    ax.set_ylabel('turnover')
    images.append(('换手率', _fig_to_base64(fig)))

    rows = ''.join(
        f"<tr><td>{col}</td><td>{metrics['ic_mean'][col]:.4f}</td>"
        f"<td>{metrics['ic_std'][col]:.4f}</td><td>{metrics['icir'][col]:.3f}</td>"
        f"<td>{metrics['ic_positive_ratio'][col]:.1%}</td>"
        f"<td>{metrics['long_short_return'][col]:.4%}</td></tr>"
        for col in period_cols
    )
    blocks = ''.join(
        f'<section><h2>{name}</h2><img alt="{name}" src="data:image/png;base64,{data}"/></section>'
        for name, data in images
    )
    generated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>{title}</title>
<style>
body{{font-family:'Microsoft YaHei',Arial,sans-serif;background:#f5f5f5;margin:0;padding:20px}}
.container{{max-width:1180px;margin:0 auto;background:#fff;padding:28px;box-shadow:0 0 10px rgba(0,0,0,.1)}}
h1{{color:#333;border-bottom:2px solid #0066cc;padding-bottom:10px}}
h2{{color:#555;border-left:4px solid #0066cc;padding-left:10px;margin-top:28px}}
.meta{{color:#666;line-height:1.8}}
table{{width:100%;border-collapse:collapse;margin-top:10px}}
th,td{{border:1px solid #ddd;padding:8px;text-align:right}}
th:first-child,td:first-child{{text-align:left}}
th{{background:#f2f2f2}}
img{{width:100%;max-width:1000px}}
.footer{{margin-top:32px;color:#888;font-size:13px;border-top:1px solid #ddd;padding-top:14px}}
</style></head><body><div class="container">
<h1>{title}</h1>
<p class="meta">因子 <b>{config.factor_column}</b> ｜ 建仓口径：信号 T 日产生，T+1 以 <b>{config.entry_price}</b> 价建仓，持有 N 个交易日（N = {', '.join(period_cols)}）<br/>
样本：{metrics['n_dates']} 个交易日 × {metrics['n_assets']} 个标的，有效观测 {metrics['n_observations']}；
分位 {config.quantiles} 层；价格为后复权（hfq）。</p>
<h2>关键指标</h2>
<table><tr><th>持有期</th><th>IC 均值</th><th>IC 标准差</th><th>ICIR</th><th>IC &gt; 0 占比</th><th>多空收益(Q_top-Q_bottom)</th></tr>{rows}</table>
<p class="meta">换手率：顶分位均值 {metrics['turnover']['top_quantile_mean']:.1%}，底分位均值 {metrics['turnover']['bottom_quantile_mean']:.1%}，
因子秩自相关均值 {metrics['turnover']['rank_autocorrelation_mean']:.3f}。</p>
{blocks}
<div class="footer">报告生成时间: {generated} ｜ Alphalens-reloaded 截面因子评估（只读分析）</div>
</div></body></html>
"""


def _jsonable(value: Any) -> Any:
    """递归转换 numpy / NaN，保证 JSON 可序列化。"""
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, float):
        return None if not math.isfinite(value) else value
    if hasattr(value, 'item'):
        return _jsonable(value.item())
    return value


def run_alphalens_evaluation(
    panel: Any,
    factor_column: str,
    *,
    periods: Sequence[int] = (1, 5, 10, 20),
    quantiles: int = 5,
    entry_price: Literal['open', 'close'] = 'open',
    max_loss: float = 0.35,
    output_dir: str | Path | None = 'reports/alphalens',
    report_name: str | None = None,
    code_col: str = 'ts_code',
    date_col: str = 'date',
    title: str | None = None,
) -> dict[str, Any]:
    """端到端跑一次 Alphalens 截面评估，返回指标 dict 并可选落 JSON/HTML。

    Returns:
        dict: ``evaluate_factor_cross_section`` 的指标 + ``tearsheet_path`` /
        ``json_path`` / ``elapsed_seconds`` / ``entry_rule``。
    """
    _require_alphalens()
    config = AlphalensConfig(
        factor_column=factor_column,
        periods=tuple(int(p) for p in periods),
        quantiles=int(quantiles),
        entry_price=entry_price,
        max_loss=max_loss,
    )
    started = datetime.now()
    factor, prices = build_alphalens_inputs(
        panel, config, code_col=code_col, date_col=date_col)
    metrics, artifacts = _analyze_factor(factor, prices, config)
    metrics['factor'] = factor_column
    metrics['entry_rule'] = (
        f"信号 T 日产生 → T+1 以 {entry_price} 价建仓，持有 N 个交易日")
    metrics['price_adjustment'] = 'hfq'
    metrics['elapsed_seconds'] = (datetime.now() - started).total_seconds()

    html_path = None
    json_path = None
    if output_dir is not None:
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = started.strftime('%Y%m%d_%H%M%S')
        base = report_name or f'alphalens_{factor_column}_{stamp}'
        html_path = out_dir / f'{base}.html'
        html_path.write_text(
            _render_tearsheet(metrics, artifacts, config,
                              title or f'因子截面评估 - {factor_column}'),
            encoding='utf-8')
        json_path = out_dir / f'{base}.json'
        json_path.write_text(
            json.dumps(_jsonable(metrics), ensure_ascii=False, indent=2),
            encoding='utf-8')
        logger.info(f"Alphalens tearsheet: {html_path}")
        logger.info(f"Alphalens 指标 JSON: {json_path}")

    result = dict(metrics)
    result['tearsheet_path'] = str(html_path) if html_path else None
    result['json_path'] = str(json_path) if json_path else None
    return result


__all__ = [
    'AlphalensConfig',
    'build_alphalens_inputs',
    'evaluate_factor_cross_section',
    'run_alphalens_evaluation',
]
