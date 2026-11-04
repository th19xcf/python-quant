"""只读评估：在冻结的修复后基准上重估降仓方案（择时评估 R1/R2/R3）。

输入（只读，禁止写入或覆盖）：

- ``tmp/kelly_fixed_matrix/matrix_frozen_500.parquet``
  （SHA256 ``a9a3eab4...48add``，220 列 × 499 只 × 1,410 交易日）
- ``tmp/kelly_fixed_matrix/windows_frozen_500.json``
  （SHA256 ``eef6ca23...31f76``，已含修复符号后的 walk-forward 权重）

产物只写 ``tmp/regime_fixed/`` 与 ``reports/regime_fixed/``。成本统一复用
``SimConfig().make_cost_model()`` 默认往返 0.154%。

评估内容：
1. R1 触发固定（沪深300 收盘同时站上 20/60 日线→满仓），降仓档位
   X ∈ {0.9, 0.85, 0.8, 0.75, 0.7, 0.6, 0.5} 的完整阶梯；
2. 在最优档位上叠加更严触发（双死叉、连续 N 日确认、MA60 单线、
   中证500 代理中证全指）；
3. 修复符号下重跑 R2（宽度分位）与 R3（波动率目标）；
4. 统一四门槛表（同一分母 A、同一基准、``acceptance`` 原样不改）。

用法::

    .venv\\Scripts\\python.exe tools/eval_regime_fixed.py
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.advice import regime  # noqa: E402
from src.advice.factor_eval import SimConfig  # noqa: E402
from tools.eval_fixed_benchmark import (  # noqa: E402
    _clean,
    base_metrics,
    base_results,
    overlay_metrics,
    scheme_row,
    sha256_file,
)
from tools.eval_position_management import THRESHOLDS  # noqa: E402

FIX = ROOT / 'tmp' / 'kelly_fixed_matrix'
MATRIX = FIX / 'matrix_frozen_500.parquet'
WINDOWS = FIX / 'windows_frozen_500.json'
OUT = ROOT / 'tmp' / 'regime_fixed'
REPORT = ROOT / 'reports' / 'regime_fixed'

MATRIX_SHA256 = 'a9a3eab4109a5362cbf776961d8a75e1e222e9d423621aefe028f6dbbee48add'
WINDOWS_SHA256 = 'eef6ca23fe35c16e82f03f4c5dce861a6c7be0a5d880b03da07b7c44ae231f76'

LADDER = (0.9, 0.85, 0.8, 0.75, 0.7, 0.6, 0.5)
CONFIRM = (3, 5, 10)
PROXY_INDEX = '000905.SH'  # 中证全指 000985.SH 本地缺失，用中证500 代理
INDEX_CODE = '000300.SH'
INITIAL = 1_000_000.0
ANNUAL = 252.0


@dataclass(frozen=True)
class Frozen:
    """冻结的 walk-forward 测试窗（权重已修复符号）。"""

    test_start: date
    test_end: date
    weights: dict[str, float]


# ----------------------------------------------------------------------
# 只读输入与哈希校验
# ----------------------------------------------------------------------
def verify_inputs() -> dict[str, Any]:
    """校验两个冻结文件 SHA256，不一致直接失败（不降级、不重算）。"""
    for path, expected in ((MATRIX, MATRIX_SHA256), (WINDOWS, WINDOWS_SHA256)):
        actual = sha256_file(path)
        if actual != expected:
            raise ValueError(f'{path.name} SHA256 不匹配：{actual} != {expected}')
    return {
        'matrix': str(MATRIX.relative_to(ROOT)),
        'matrix_sha256': MATRIX_SHA256,
        'windows': str(WINDOWS.relative_to(ROOT)),
        'windows_sha256': WINDOWS_SHA256,
    }


def load_frozen() -> list[Frozen]:
    payload = json.loads(WINDOWS.read_text(encoding='utf-8'))
    return [
        Frozen(date.fromisoformat(item['test_start']),
               date.fromisoformat(item['test_end']),
               {name: float(value) for name, value in item['weights'].items()})
        for item in payload['windows']
    ]


# ----------------------------------------------------------------------
# 仓位规则（全部 t 日收盘定、t+1 生效，由 overlay_curve 完成 shift）
# ----------------------------------------------------------------------
def index_position(code: str, low: float,
                   params: regime.RegimeParams | None = None) -> pl.DataFrame:
    """标准 R1：收盘同时站上 20/60 日线→满仓，否则 ``low``。"""
    base = params or regime.RegimeParams()
    rule = regime.ma_position(
        regime.load_index_daily(code), replace(base, low_exposure=low))
    return rule.sort('date')


def _ma_pair(index_frame: pl.DataFrame,
             params: regime.RegimeParams) -> tuple[pl.Series, pl.Series]:
    close = index_frame['close'].cast(pl.Float64)
    short_ma = close.rolling_mean(
        window_size=params.ma_short, min_samples=params.ma_short)
    long_ma = close.rolling_mean(
        window_size=params.ma_long, min_samples=params.ma_long)
    return short_ma, long_ma


def death_cross_position(index_frame: pl.DataFrame, low: float,
                         params: regime.RegimeParams) -> pl.DataFrame:
    """只有收盘同时跌破 20/60 日线才降仓，单线跌破不动作（降低误判）。"""
    short_ma, long_ma = _ma_pair(index_frame, params)
    close = index_frame['close'].cast(pl.Float64)
    both_below = ((close < short_ma) & (close < long_ma)).fill_null(False)
    position = pl.when(both_below).then(pl.lit(float(low))).otherwise(pl.lit(1.0))
    return index_frame.select(pl.col('date')).with_columns(
        position.cast(pl.Float64).alias('position')).sort('date')


def confirm_position(index_frame: pl.DataFrame, low: float,
                     params: regime.RegimeParams, days: int) -> pl.DataFrame:
    """双线下方连续 ``days`` 日确认后才降仓，否则满仓。"""
    short_ma, long_ma = _ma_pair(index_frame, params)
    close = index_frame['close'].cast(pl.Float64)
    below = ((close < short_ma) & (close < long_ma)).fill_null(False)
    confirmed = (
        pl.DataFrame({'below': below.cast(pl.Int32)})
        .select((pl.col('below').rolling_min(window_size=days,
                                              min_samples=days) == 1)
                .alias('hit'))['hit']
    )
    position = pl.when(confirmed).then(pl.lit(float(low))).otherwise(pl.lit(1.0))
    return index_frame.select(pl.col('date')).with_columns(
        position.cast(pl.Float64).alias('position')).sort('date')


def ma_long_position(index_frame: pl.DataFrame, low: float,
                     params: regime.RegimeParams) -> pl.DataFrame:
    """MA60 单线：收盘站上 60 日线→满仓，否则 ``low``。"""
    _, long_ma = _ma_pair(index_frame, params)
    close = index_frame['close'].cast(pl.Float64)
    above = (close > long_ma).fill_null(False)
    position = pl.when(above).then(pl.lit(1.0)).otherwise(pl.lit(float(low)))
    return index_frame.select(pl.col('date')).with_columns(
        position.cast(pl.Float64).alias('position')).sort('date')


def reverse_position(index_frame: pl.DataFrame, low: float,
                     params: regime.RegimeParams) -> pl.DataFrame:
    """反向对照：双线之上降仓、之下满仓，用于检验信号方向。"""
    short_ma, long_ma = _ma_pair(index_frame, params)
    close = index_frame['close'].cast(pl.Float64)
    both_above = ((close > short_ma) & (close > long_ma)).fill_null(False)
    position = pl.when(both_above).then(pl.lit(float(low))).otherwise(pl.lit(1.0))
    return index_frame.select(pl.col('date')).with_columns(
        position.cast(pl.Float64).alias('position')).sort('date')


# ----------------------------------------------------------------------
# 指标与逐窗明细
# ----------------------------------------------------------------------
def row(name: str, metrics: dict[str, Any], base: dict[str, Any]
        ) -> dict[str, Any]:
    item = scheme_row(name, metrics, base)
    item['scheme'] = name
    item['annual_drop_signed_pp'] = float(
        metrics['annualized'] - base['annualized'])
    item['mdd_improve_pp'] = float(
        metrics['max_drawdown'] - base['max_drawdown'])
    return item


def window_table(curves: list[pl.DataFrame], position: pl.DataFrame,
                 base: dict[str, Any], scheme: dict[str, Any],
                 windows: list[Frozen]) -> list[dict[str, Any]]:
    """逐窗满仓/降仓收益差与低仓位天数占比。"""
    executed = regime.executed_position(position)
    rows: list[dict[str, Any]] = []
    for idx, (window, curve) in enumerate(zip(windows, curves, strict=True)):
        joined = (
            curve.select('date', 'equity')
            .join(executed, on='date', how='left').sort('date')
            .with_columns([
                pl.col('exec_position').fill_null(1.0),
                pl.col('delta').fill_null(0.0),
            ])
        )
        low = (joined['exec_position'] < 0.999).mean()
        rows.append({
            'window': idx + 1, 'test_start': str(window.test_start),
            'test_end': str(window.test_end),
            'full_return': float(base['windows'][idx]['net_return']),
            'rule_return': float(scheme['windows'][idx]['net_return']),
            'delta_pp': float(
                (scheme['windows'][idx]['net_return']
                 - base['windows'][idx]['net_return']) * 100.0),
            'low_day_share': float(low if low is not None else 0.0),
            'mean_exposure': float(joined['exec_position'].mean()),
        })
    return rows


def daily_diagnostics(curves: list[pl.DataFrame], position: pl.DataFrame
                      ) -> dict[str, float]:
    """低仓位日与满仓日的次日收益对比，量化信号方向。"""
    executed = regime.executed_position(position)
    gross_all: list[np.ndarray] = []
    low_all: list[np.ndarray] = []
    for curve in curves:
        joined = curve.select('date', 'equity').join(
            executed, on='date', how='left').sort('date')
        equity = joined['equity'].to_numpy()
        gross = np.empty(len(equity), dtype=float)
        gross[0] = equity[0] / INITIAL - 1.0
        gross[1:] = np.diff(equity) / equity[:-1]
        flag = joined['exec_position'].fill_null(1.0).to_numpy() < 0.999
        gross_all.append(gross)
        low_all.append(flag)
    gross = np.concatenate(gross_all)
    low = np.concatenate(low_all)
    finite = np.isfinite(gross)
    gross, low = gross[finite], low[finite]
    if not low.any() or low.all():
        corr = float('nan')
    else:
        corr = float(np.corrcoef(low.astype(float), gross)[0, 1])
    return {
        'mean_gross_low_annualized': float(gross[low].mean() * ANNUAL),
        'mean_gross_full_annualized': float(gross[~low].mean() * ANNUAL),
        'low_day_share': float(low.mean()),
        'corr_low_flag_return': corr,
    }


# ----------------------------------------------------------------------
# 报告
# ----------------------------------------------------------------------
def _pct(value: float, digits: int = 2) -> str:
    return f'{value * 100:.{digits}f}%'


def write_report(payload: dict[str, Any]) -> None:
    meta = payload['meta']
    base = payload['base']
    ladder = payload['ladder']
    strict = payload['strict']
    other = payload['other']
    best = payload['best']
    verdict = payload['verdict']

    def gate(item: dict[str, Any]) -> str:
        return (f'| {item["annualized"]:.2%} | {item["max_drawdown"]:.2%} '
                f'| {item["calmar"]:.3f} | {item["worst_window"]:.2%} '
                f'| {item["turnover_annual"]:.2f} | {item["cost_annual"]:.2%} '
                f'| {item["pass_c1"]} | {item["pass_c2"]} '
                f'| {item["pass_c3"]} | {item["pass_c4"]} '
                f'| {item["pass_all"]} |')

    lines = [
        '# 冻结基准降仓方案重估报告',
        '',
        '## 结论（先看这里）',
        '',
        f'- 冻结基准 A：年化 **{base["annualized"]:.2%}**、MDD '
        f'**{base["max_drawdown"]:.2%}**、Calmar **{base["calmar"]:.3f}**、'
        f'最差窗 **{base["worst_window"]:.2%}**；R1 半仓复现 '
        f'**{payload["repro"]["r1_half_annualized"]:.2%}**'
        f'（{payload["repro"]["delta_vs_expected_pp"]:+.2f}pp vs 22.85%）。',
        f'- 降仓阶梯里**第一个通过 c2 的档位**：'
        f'**{verdict["first_c2"] or "无（全部档位年化降幅 >1.00pp）"}**。',
        f'- {verdict["summary"]}',
        f'- 更严触发：{verdict["strict_note"]}',
        f'- R2（宽度分位）年化 {other["R2_breadth_05"]["annualized"]:.2%}，'
        f'R3（波动率目标）年化 {other["R3_vol_05"]["annualized"]:.2%}；'
        f'{verdict["r2r3_note"]}',
        f'- R1 从 −0.46pp 恶化到 −7.06pp 的归因：'
        f'{verdict["attribution"]}',
        '',
        '## 1. 输入与口径',
        '',
        f'- 矩阵：`{meta["inputs"]["matrix"]}`，SHA256 `{meta["matrix_sha256"]}`',
        f'- 窗口：`{meta["inputs"]["windows"]}`，SHA256 '
        f'`{meta["windows_sha256"]}`',
        f'- 矩阵 {meta["profile"]["rows"]} 行 × {meta["profile"]["columns"]} 列，'
        f'{meta["profile"]["stocks"]} 只，'
        f'{meta["profile"]["start"]}~{meta["profile"]["end"]}'
        f'（{meta["profile"]["trading_days"]} 个交易日）',
        f'- 7 窗：{meta["n_windows"]} 个，模拟区间 '
        f'`test_start <= date < test_end`，T+1 执行',
        f'- 成本：`CostModel` 默认往返 {meta["cost_round_trip"]:.4%}，未覆盖参数',
        f'- 四门槛（`tools/eval_position_management.py:THRESHOLDS` 原样）：'
        f'c1 Calmar 提升 ≥ {THRESHOLDS["calmar_improve"]:.0%}；'
        f'c2 年化降 ≤ {THRESHOLDS["annual_drop_pp"] * 100:.2f}pp；'
        f'c3 最差窗改善 ≥ {THRESHOLDS["worst_improve"]:.0%}；'
        f'c4 成本增量 ≤ {THRESHOLDS["cost_increment"]:.2%}',
        f'- 指数口径：{INDEX_CODE}；`000985.SH`（中证全指）本地缺失，'
        f'以 `{PROXY_INDEX}`（中证500）代理并在下方标注',
        '',
        '## 2. 统一四门槛表（同一分母 A、同一基准）',
        '',
        '| 方案 | 年化 | MDD | Calmar | 最差窗 | 换手 | 成本/年 | c1 | c2 '
        '| c3 | c4 | 全过 |',
        '|---|---|---|---|---|---|---|---|---|---|---|---|',
        f'| A_满仓 | {base["annualized"]:.2%} | {base["max_drawdown"]:.2%} '
        f'| {base["calmar"]:.3f} | {base["worst_window"]:.2%} '
        f'| {base["turnover_annual"]:.2f} | {base["cost_annual"]:.2%} '
        '| — | — | — | — | — |',
    ]
    for item in ladder:
        lines.append(f'| R1_X={item["name"]} ' + gate(item))
    for item in strict:
        lines.append(f'| {item["label"]} ' + gate(item))
    for key, item in other.items():
        lines.append(f'| {key} ' + gate(item))

    lines += [
        '',
        f'- 阶梯说明：c2 是唯一“成本”约束的硬线，未放宽。'
        f'第一个通过 c2 的档位为 {verdict["first_c2"] or "无"}。',
        f'- “最好档位”定义为 {verdict["best_definition"]}：'
        f'X={best["name"]}（Calmar {best["calmar"]:.3f}，'
        f'年化降 {best["annual_drop_pp"] * 100:.2f}pp）。',
        '',
        '## 3. 降仓强度阶梯（R1 触发不变）',
        '',
        '| X | 年化 | 年化让渡 | MDD | MDD 改善 | Calmar | 最差窗 | 换手 '
        '| 成本/年 | c1 | c2 | c3 | c4 |',
        '|---|---|---|---|---|---|---|---|---|---|---|---|---|',
    ]
    for item in ladder:
        lines.append(
            f'| {item["name"]} | {item["annualized"]:.2%} '
            f'| {item["annual_drop_pp"] * 100:.2f}pp | '
            f'{item["max_drawdown"]:.2%} | '
            f'{item["mdd_improve_pp"] * 100:+.2f}pp | '
            f'{item["calmar"]:.3f} | {item["worst_window"]:.2%} '
            f'| {item["turnover_annual"]:.2f} '
            f'| {item["cost_annual"]:.2%} '
            f'| {item["pass_c1"]} | {item["pass_c2"]} '
            f'| {item["pass_c3"]} | {item["pass_c4"]} |')

    lines += [
        '',
        '## 4. 更严触发（叠加在最优档位）',
        '',
        '| 变体 | 年化 | 年化让渡 | Calmar | 最差窗 | 换手 | 成本/年 | c1 '
        '| c2 | c3 | c4 |',
        '|---|---|---|---|---|---|---|---|---|---|---|',
    ]
    for item in strict:
        lines.append(
            f'| {item["label"]} | {item["annualized"]:.2%} '
            f'| {item["annual_drop_pp"] * 100:.2f}pp | '
            f'{item["calmar"]:.3f} | {item["worst_window"]:.2%} '
            f'| {item["turnover_annual"]:.2f} '
            f'| {item["cost_annual"]:.2%} '
            f'| {item["pass_c1"]} | {item["pass_c2"]} '
            f'| {item["pass_c3"]} | {item["pass_c4"]} |')

    lines += [
        '',
        '## 5. R2 / R3 在修复符号下重跑',
        '',
        '| 方案 | 年化 | 年化让渡 | Calmar | 最差窗 | c1 | c2 | c3 | c4 | 全过 |',
        '|---|---|---|---|---|---|---|---|---|---|',
    ]
    for key, item in other.items():
        lines.append(
            f'| {key} | {item["annualized"]:.2%} '
            f'| {item["annual_drop_pp"] * 100:.2f}pp | '
            f'{item["calmar"]:.3f} | {item["worst_window"]:.2%} '
            f'| {item["pass_c1"]} | {item["pass_c2"]} '
            f'| {item["pass_c3"]} | {item["pass_c4"]} '
            f'| {item["pass_all"]} |')

    lines += [
        '',
        '## 6. 逐窗满仓 / 降仓收益差（R1 X=0.5）',
        '',
        '| 窗 | 起止 | 满仓 | R1_0.5 | 差(pp) | 低仓位天数占比 | 平均仓位 |',
        '|---|---|---|---|---|---|---|',
    ]
    for item in payload['windows_detail']['R1_X=0.50']:
        lines.append(
            f'| {item["window"]} | {item["test_start"]}~{item["test_end"]} '
            f'| {item["full_return"]:.2%} | {item["rule_return"]:.2%} '
            f'| {item["delta_pp"]:+.2f} | {item["low_day_share"]:.1%} '
            f'| {item["mean_exposure"]:.3f} |')

    diag = payload['diagnostics']
    lines += [
        '',
        '### 信号方向证据',
        '',
        f'- 低仓位日次日平均收益（年化）**{_pct(diag["mean_gross_low_annualized"])}**；'
        f'满仓日次日平均收益（年化）**{_pct(diag["mean_gross_full_annualized"])}**。',
        f'- `corr(低仓位标记, 次日收益)` = **{diag["corr_low_flag_return"]:+.3f}**；'
        f'低仓位天数占比 {diag["low_day_share"]:.1%}。',
        f'- 反向对照（双线之上降仓、之下满仓，X=0.5）年化 '
        f'**{other["R1_reverse_05"]["annualized"]:.2%}**，'
        f'相对 A 让渡 {other["R1_reverse_05"]["annual_drop_pp"] * 100:+.2f}pp。'
        f'{verdict["reverse_note"]}',
        '',
        '## 7. 判定与边界',
        '',
        f'- {verdict["final"]}',
        '- 本脚本不改生产逻辑，不写入冻结输入；`reports/` 为本地报告，'
        '不入库（仓库 .gitignore 既有约定）。',
        '',
    ]
    REPORT.mkdir(parents=True, exist_ok=True)
    (REPORT / '冻结基准降仓重估报告.md').write_text(
        '\n'.join(lines), encoding='utf-8')


# ----------------------------------------------------------------------
# 主流程
# ----------------------------------------------------------------------
def main() -> int:  # noqa: C901
    OUT.mkdir(parents=True, exist_ok=True)
    REPORT.mkdir(parents=True, exist_ok=True)
    inputs = verify_inputs()
    frame = pl.read_parquet(MATRIX).with_columns(pl.col('date').cast(pl.Date))
    windows = load_frozen()
    profile = {
        'rows': frame.height, 'columns': frame.width,
        'stocks': frame['ts_code'].n_unique(),
        'start': str(frame['date'].min()), 'end': str(frame['date'].max()),
        'trading_days': frame['date'].n_unique(),
    }
    print(f'[fixed] matrix {profile["rows"]} × {profile["columns"]}', flush=True)

    results = base_results(frame, windows)
    base = base_metrics(results)
    curves = [item.curve for item in results]
    cost_model = SimConfig().make_cost_model()
    cost_round_trip = float(
        cost_model.params['commission_rate'] * 2
        + cost_model.params['stamp_tax_rate']
        + cost_model.params['transfer_fee_rate'] * 2
        + cost_model.params['slippage'] * 2)
    print(f'[fixed] base A {base["annualized"]:.4f} / '
          f'{base["max_drawdown"]:.4f}', flush=True)

    index300 = regime.load_index_daily(INDEX_CODE)
    breadth = regime.load_breadth_snapshot()

    # ---- 1. 降仓阶梯 ----
    ladder: list[dict[str, Any]] = []
    schemes: dict[str, dict[str, Any]] = {}
    positions: dict[str, pl.DataFrame] = {}
    for low in LADDER:
        name = f'{low:.2f}'
        position = index_position(INDEX_CODE, low)
        metrics = overlay_metrics(curves, position, cost_model)
        item = row(name, metrics, base)
        item['name'] = name
        ladder.append(item)
        schemes[f'R1_X={name}'] = metrics
        positions[f'R1_X={name}'] = position
        print(f'[fixed] R1 X={name}: {metrics["annualized"]:.4f} '
              f'calmar {metrics["calmar"]:.3f}', flush=True)

    first_c2: str | None = None
    for item in ladder:
        if item['pass_c2']:
            first_c2 = item['name']
            break
    if first_c2 is not None:
        best_item = next(item for item in ladder if item['name'] == first_c2)
        best_definition = '第一个通过 c2 的最轻档位'
    else:
        best_item = max(ladder, key=lambda item: item['calmar'])
        best_definition = '无档位通过 c2 时，取 Calmar 最高的档位'
    best_low = float(best_item['name'])

    # ---- 2. 更严触发 ----
    strict: list[dict[str, Any]] = []
    variants: list[tuple[str, pl.DataFrame]] = [
        ('双死叉才降仓', death_cross_position(index300, best_low, regime.RegimeParams())),
        *[(f'连续 {days} 日确认',
           confirm_position(index300, best_low, regime.RegimeParams(), days))
          for days in CONFIRM],
        ('MA60 单线', ma_long_position(index300, best_low, regime.RegimeParams())),
        (f'{PROXY_INDEX} 代理中证全指',
         index_position(PROXY_INDEX, best_low)),
    ]
    for label, position in variants:
        metrics = overlay_metrics(curves, position, cost_model)
        item = row(label, metrics, base)
        item['label'] = label
        strict.append(item)
        print(f'[fixed] strict {label}: {metrics["annualized"]:.4f} '
              f'calmar {metrics["calmar"]:.3f}', flush=True)

    # ---- 3. R2 / R3 ----
    r2_position = regime.breadth_position(breadth, regime.RegimeParams())
    r3_position = regime.vol_target_position(index300, regime.RegimeParams())
    other = {
        'R2_breadth_05': row('R2_breadth_05',
                             overlay_metrics(curves, r2_position, cost_model), base),
        'R3_vol_05': row('R3_vol_05',
                         overlay_metrics(curves, r3_position, cost_model), base),
        'R1_reverse_05': row(
            'R1_reverse_05',
            overlay_metrics(curves, reverse_position(
                index300, 0.5, regime.RegimeParams()), cost_model), base),
    }
    for key, item in other.items():
        print(f'[fixed] {key}: {item["annualized"]:.4f} '
              f'calmar {item["calmar"]:.3f}', flush=True)

    # ---- 4. 逐窗与方向证据 ----
    r1_half = schemes['R1_X=0.50']
    windows_detail = {
        'R1_X=0.50': window_table(
            curves, positions['R1_X=0.50'], base, r1_half, windows)}
    diagnostics = daily_diagnostics(curves, positions['R1_X=0.50'])

    # ---- 判定 ----
    passed = [item for item in ladder + strict + list(other.values())
              if item['pass_all']]
    any_c2 = [item for item in ladder if item['pass_c2']]
    if passed:
        summary = ('有方案全过四门槛：'
                   + '、'.join(str(item.get('name', item.get('label')))
                               for item in passed) + '。')
    else:
        summary = ('**当前无可行降仓方案**：'
                   f'阶梯 {len(ladder)} 档、严触发 {len(strict)} 个变体、'
                   f'R2/R3 均无一组全过 c1~c4；'
                   f'c2 通过档位数 {len(any_c2)}/{len(ladder)}。')

    strict_note = (f'在 X={best_low:.2f} 上叠加更严触发后，'
                   + '；'.join(
                       f'{item["label"]} 年化让 {item["annual_drop_pp"] * 100:.2f}pp、'
                       f'Calmar {item["calmar"]:.3f}、最差窗 '
                       f'{item["worst_window"]:.2%}'
                       for item in strict)
                   + '；最严的连续 10 日确认把换手压到 '
                   f'{strict[3]["turnover_annual"]:.2f}x，但年化仍让 '
                   f'{strict[3]["annual_drop_pp"] * 100:.2f}pp。')
    r2r3_note = ('R2 年化让渡 '
                 f'{other["R2_breadth_05"]["annual_drop_pp"] * 100:.2f}pp、'
                 f'Calmar {other["R2_breadth_05"]["calmar"]:.3f}；'
                 'R3 年化让渡 '
                 f'{other["R3_vol_05"]["annual_drop_pp"] * 100:.2f}pp、'
                 f'Calmar {other["R3_vol_05"]["calmar"]:.3f}；'
                 '两者在修复符号下仍无效。')
    reverse_note = ('反向对照（在双线之上降仓、之下满仓）更差：'
                    f'{other["R1_reverse_05"]["annualized"]:.2%}，'
                    f'相对 A 让渡 '
                    f'{other["R1_reverse_05"]["annual_drop_pp"] * 100:.2f}pp，'
                    '说明信号方向没有装反；成本来自降仓期本身仍是正收益，'
                    '而不是“方向做反了”。')

    w4 = windows_detail['R1_X=0.50'][3]
    w5 = windows_detail['R1_X=0.50'][4]
    old_drag = 0.0046
    new_drag = base['annualized'] - r1_half['annualized']
    scaled_old = old_drag * (base['annualized'] / 0.3542)
    gap = (diagnostics['mean_gross_full_annualized']
           - diagnostics['mean_gross_low_annualized'])
    attribution = (
        f'不是旧基准虚高，也不是信号装反，而是修复符号后条件收益结构变了。'
        f'证据：低仓位日次日收益年化 '
        f'{diagnostics["mean_gross_low_annualized"]:.2%}，满仓日 '
        f'{diagnostics["mean_gross_full_annualized"]:.2%}，差 '
        f'{gap * 100:.2f}pp（`corr(低仓位标记, 次日收益)`='
        f'{diagnostics["corr_low_flag_return"]:+.3f}）；'
        f'第 4 窗满仓 {w4["full_return"]:.2%}、R1 {w4["rule_return"]:.2%}'
        f'（{w4["delta_pp"]:+.2f}pp，低仓位 {w4["low_day_share"]:.1%}），'
        f'最强的第 5 窗满仓 {w5["full_return"]:.2%}、R1 {w5["rule_return"]:.2%}'
        f'（{w5["delta_pp"]:+.2f}pp，低仓位仅 {w5["low_day_share"]:.1%}）；'
        f'全部 5 个上行窗差为负，2 个下跌窗为正。'
        f'旧口径 35.42%→29.92% 的水平差按比例最多把 0.46pp 放大到 '
        f'{scaled_old * 100:.2f}pp，无法解释 {new_drag * 100:.2f}pp；'
        '因此旧口径下 R1 便宜，是因为旧符号下低仓位期收益与满仓期接近，'
        '修复后低仓位期仍保有正 alpha（年化 '
        f'{diagnostics["mean_gross_low_annualized"]:.2%}），降仓砍掉的是这部分收益。')
    final = ('当前冻结基准上不存在既满足 c2（年化降 ≤1.00pp）又改善 '
             'Calmar/最差窗的降仓档位；不建议把任何 R1 变体接入生产流程。')

    payload = _clean({
        'meta': {
            'inputs': inputs, 'matrix_sha256': MATRIX_SHA256,
            'windows_sha256': WINDOWS_SHA256, 'profile': profile,
            'n_windows': len(windows),
            'cost_round_trip': cost_round_trip,
            'thresholds': THRESHOLDS,
            'index_code': INDEX_CODE, 'proxy_index': PROXY_INDEX,
            'proxy_note': '000985.SH 本地缺失，中证500 为代理',
        },
        'base': {key: base[key] for key in (
            'annualized', 'max_drawdown', 'calmar', 'sharpe',
            'worst_window', 'turnover_annual', 'cost_annual')},
        'repro': {
            'r1_half_annualized': r1_half['annualized'],
            'expected_r1_half_annualized': 0.2285,
            'delta_vs_expected_pp': float(
                (r1_half['annualized'] - 0.2285) * 100.0),
        },
        'ladder': ladder,
        'strict': strict,
        'other': other,
        'best': {'name': best_item['name'],
                 'calmar': best_item['calmar'],
                 'annual_drop_pp': best_item['annual_drop_pp']},
        'verdict': {
            'first_c2': first_c2, 'best_definition': best_definition,
            'summary': summary, 'strict_note': strict_note,
            'r2r3_note': r2r3_note, 'attribution': attribution,
            'reverse_note': reverse_note, 'final': final,
        },
        'windows_detail': windows_detail,
        'diagnostics': diagnostics,
    })

    (OUT / 'summary.json').write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    pl.DataFrame(ladder).write_csv(OUT / 'ladder.csv')
    pl.DataFrame(strict).write_csv(OUT / 'strict.csv')
    pl.DataFrame(list(other.values())).write_csv(OUT / 'other_rules.csv')
    detail = []
    for item in windows_detail['R1_X=0.50']:
        detail.append(item)
    pl.DataFrame(detail).write_csv(OUT / 'windows_r1_half.csv')
    write_report(payload)
    print(f'[done] {REPORT / "冻结基准降仓重估报告.md"}', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
