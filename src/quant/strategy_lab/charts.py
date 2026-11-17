"""策略试算离线出图：净值对比 / 回撤 / 买卖标记三块面板共用横轴。

配色、画布底色与试算页三图一致（沿用 ``src/ui/advice/tokens.py`` 深色主题的
accent / muted / sell / hold 与 card 底色），因此页面看到的图和落盘图是同一套
视觉口径；图只读 ``run_rule`` 的 payload，不参与任何净值或撮合计算。

用法::

    .venv\\Scripts\\python.exe -m src.quant.strategy_lab.charts \\
        --rule config/strategy_lab/rules/示例_4%定投补仓_15%止盈.json \\
        --out outputs/strategy_lab/4%定投补仓_15%止盈_回测图_2026-11-17.png
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

from .backtest import run_rule  # noqa: E402
from .rules import load_rule, rule_text  # noqa: E402

#: 与试算页同一套深色主题色板（src/ui/advice/tokens.py::DARK）
PALETTE = {
    'bg': '#12151A', 'card': '#1C2128', 'border': '#2A323D', 'text': '#E6EDF5',
    'muted': '#8B98A8', 'accent': '#5B9DFF', 'buy': '#F2555F', 'sell': '#23C26B',
    'hold': '#E3B341',
}

_TICKS = 8


def _style_axes(ax: Any, title: str, ylabel: str) -> None:
    ax.set_facecolor(PALETTE['card'])
    ax.set_title(title, color=PALETTE['text'], fontsize=11, loc='left')
    ax.set_ylabel(ylabel, color=PALETTE['muted'], fontsize=9)
    ax.grid(True, color=PALETTE['border'], alpha=0.6, linewidth=0.6)
    for spine in ax.spines.values():
        spine.set_color(PALETTE['border'])
    ax.tick_params(colors=PALETTE['muted'], labelsize=8)
    legend = ax.legend(loc='upper left', fontsize=8, frameon=False)
    for text in legend.get_texts():
        text.set_color(PALETTE['muted'])


def _normalized(curve: List[List[Any]]) -> List[float]:
    if not curve:
        return []
    base = float(curve[0][1]) or 1.0
    return [float(value) / base for _, value in curve]


def _markers_by_reason(markers: List[Dict[str, Any]], nav_index: Dict[str, int],
                       nav_value: List[float]) -> Dict[str, tuple[List[int], List[float]]]:
    groups: Dict[str, tuple[List[int], List[float]]] = {
        'add': ([], []), 'take_profit': ([], [])}
    for marker in markers:
        reason = marker.get('reason')
        kind = 'take_profit' if reason == 'take_profit' else (
            'add' if reason == 'add' else None)
        x = nav_index.get(str(marker.get('date')))
        if kind is None or x is None:
            continue
        groups[kind][0].append(x)
        groups[kind][1].append(nav_value[x])
    return groups


def render(payload: Dict[str, Any], rule: Dict[str, Any], out_path: Path) -> Path:
    """按 payload 画三块面板并落盘为 PNG，返回实际路径。"""
    plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False

    nav = payload.get('nav') or []
    if len(nav) < 2:
        raise RuntimeError('payload 没有可画的净值曲线')
    dates = [str(row[0]) for row in nav]
    nav_value = _normalized(nav)
    hold_value = _normalized(payload.get('buy_hold_curve') or [])
    bench_value = _normalized(payload.get('benchmark_curve') or [])
    drawdown = payload.get('drawdown') or []

    fig, axes = plt.subplots(3, 1, figsize=(13.5, 9.6), sharex=True)
    fig.patch.set_facecolor(PALETTE['bg'])
    fig.subplots_adjust(left=0.07, right=0.98, top=0.90, bottom=0.09, hspace=0.30)
    fig.suptitle(f'策略试算｜{rule["name"]}', color=PALETTE['text'],
                 fontsize=13, x=0.07, y=0.975, ha='left')

    ax = axes[0]
    ax.plot(range(len(nav_value)), nav_value, color=PALETTE['accent'],
            linewidth=1.6, label='规则净值')
    if hold_value:
        ax.plot(range(len(hold_value)), hold_value, color=PALETTE['hold'],
                linewidth=1.2, linestyle='--', label='同区间买入持有')
    if bench_value:
        ax.plot(range(len(bench_value)), bench_value, color=PALETTE['muted'],
                linewidth=1.0, linestyle=':', label='沪深300（000300.SH）')
    _style_axes(ax, '净值对比（起点=1）', '净值')

    ax = axes[1]
    xs = list(range(len(drawdown)))
    ys = [float(value) * 100.0 for _, value in drawdown]
    ax.plot(xs, ys, color=PALETTE['sell'], linewidth=1.0, label='回撤')
    ax.fill_between(xs, ys, 0, color=PALETTE['sell'], alpha=0.22)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f'{v:.0f}%'))
    _style_axes(ax, '回撤', '回撤')

    ax = axes[2]
    ax.plot(range(len(nav_value)), nav_value, color=PALETTE['accent'],
            linewidth=1.0, alpha=0.7, label='净值')
    groups = _markers_by_reason(payload.get('markers') or [],
                                {day: i for i, day in enumerate(dates)}, nav_value)
    add_x, add_y = groups['add']
    exit_x, exit_y = groups['take_profit']
    if add_x:
        ax.scatter(add_x, add_y, marker='^', s=42, color=PALETTE['buy'],
                   edgecolors=PALETTE['bg'], linewidths=0.4,
                   label=f'补仓（{len(add_x)} 次）', zorder=3)
    if exit_x:
        ax.scatter(exit_x, exit_y, marker='v', s=52, color=PALETTE['sell'],
                   edgecolors=PALETTE['bg'], linewidths=0.4,
                   label=f'止盈清仓（{len(exit_x)} 次）', zorder=4)
    _style_axes(ax, '补仓 / 止盈清仓标记（标在净值曲线上）', '净值')

    ticks = sorted(set([int(len(dates) * i / _TICKS) for i in range(_TICKS)]
                       + [len(dates) - 1]))
    ax.set_xticks(ticks)
    ax.set_xticklabels([dates[i] for i in ticks], rotation=0)
    ax.set_xlabel(f'{dates[0]} ~ {dates[-1]}（交易日序号）',
                  color=PALETTE['muted'], fontsize=9)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150, facecolor=fig.get_facecolor())
    plt.close(fig)
    return out_path


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='策略试算出图（三面板 PNG）')
    parser.add_argument('--rule', type=Path, required=True, help='规则 JSON 路径')
    parser.add_argument('--out', type=Path, required=True, help='PNG 输出路径')
    args = parser.parse_args(argv)

    rule = load_rule(args.rule)
    result = run_rule(rule)
    if result.get('status') != 'ok':
        sys.stdout.write(json.dumps({'status': 'empty',
                                     'reason': result.get('reason')},
                                    ensure_ascii=False) + '\n')
        return 1
    path = render(result, rule, args.out)
    summary = {'rule_text': rule_text(rule), 'image': str(path),
               'plan_note': result.get('plan_note')}
    sys.stdout.write(json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
