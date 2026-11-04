"""筹码峰界面布局示意图（mockup），非正式界面实现。

产出两张图：
- `layout_mockup_main.png`：方案 A，主图 K 线 + 右侧筹码窄栏，共用价格纵轴
- `layout_mockup_window.png`：方案 B，独立窗口，价格纵轴 + 筹码横轴，看 200 箱细节

数据与筹码算法直接复用 `tools/chip_prototype.py`（衰减口径 vol/MA20 代理），
只读本地 quote，不改数据、不改 src/。

依赖 matplotlib，出图时需临时安装（`pip install matplotlib`），
项目常规依赖中未包含它，平时不要装进 .venv。

用法：
    .venv\\Scripts\\python.exe tools\\chip_layout_mockup.py
    .venv\\Scripts\\python.exe tools\\chip_layout_mockup.py --code 600519.SH --bars 160
"""
# ruff: noqa: T201  # 原型脚本面向终端输出

import argparse
import sys
from pathlib import Path

import matplotlib

matplotlib.use('Agg')

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from matplotlib import font_manager  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Rectangle  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'tools'))

from chip_prototype import (  # noqa: E402
    StockSpec,
    compute_stock,
    load_names,
    load_stock,
)

DEFAULT_OUT = PROJECT_ROOT / 'reports' / 'chip_prototype'
DPI = 150
FIG_SIZE = (16, 9)

# 界面示意配色
BG = '#f5f6f8'
PANEL_BG = '#ffffff'
BAR_BG = '#eceff3'
BORDER = '#d3d8de'
TEXT = '#252a31'
MUTED = '#6b7480'
UP = '#d05252'
DOWN = '#3f9e6a'
MA20_COLOR = '#3f6fb5'
MA60_COLOR = '#c08a2e'
DECAY_COLOR = '#e4572e'
STATIC_COLOR = '#4c78a8'

FONT_ZH = ('Microsoft YaHei', 'SimHei', 'Arial Unicode MS')

LABELS = {
    'zh': {
        'prev': '上一只', 'next': '下一只', 'day': '日线', 'week': '周线',
        'month': '月线', 'bars': '柱体数', 'add': '增加柱体', 'cut': '减少柱体',
        'qfq': '前复权',
        'chip_panel': '筹码分布栏（占宽约 25%）',
        'price': '现价 {p:.2f}',
        'peak': '主峰 {p:.2f}（{s:.1f}%）',
        'bins': '200 个价格箱（全历史，本窗口显示重叠段）',
        'button': '筹码',
        'button_note': '从此处开/关',
        'kline': '日 K 线（最近 {n} 根）',
        'ma': 'MA20 / MA60',
        'chip_x': '筹码占比（%）',
        'chip_legend_static': '静态 VAP',
        'chip_legend_decay': '衰减 VAP',
        'window_note': '窗口外老筹码已裁剪：静态 {s:.0f}% / 衰减 {d:.0f}% 仍在图外',
        'tradeoff': '取舍：空间大、200 箱细节完整；\n但全区间压缩后衰减筹码只占顶部约 4% 高度，\n且脱离 K 线，无法一眼对照现价与峰位',
        'bottom_items': ['分时', '日K', '指标', '画线', '筹码', '复权', '导出'],
        'status': '状态栏示意：筹码栏可随时开关，不影响主图缩放',
        'title': '{code} {name}｜布局示意（mockup，非真实界面）',
    },
    'en': {
        'prev': 'Prev', 'next': 'Next', 'day': 'Day', 'week': 'Week',
        'month': 'Month', 'bars': 'Bins', 'add': '+Bins', 'cut': '-Bins',
        'qfq': 'QFQ',
        'chip_panel': 'Chip panel (~25% width)',
        'price': 'Last {p:.2f}',
        'peak': 'Peak {p:.2f} ({s:.1f}%)',
        'bins': '200 price bins (full history; window shows overlap)',
        'button': 'Chip',
        'button_note': 'toggle here',
        'kline': 'Daily candles (last {n})',
        'ma': 'MA20 / MA60',
        'chip_x': 'Chip share (%)',
        'chip_legend_static': 'Static VAP',
        'chip_legend_decay': 'Decay VAP',
        'window_note': 'Off-window old chips cut: static {s:.0f}% / decay {d:.0f}% outside',
        'tradeoff': 'Trade-off: more room for 200 bins;\nbut decay chips occupy only top ~4% height\nand there is no K-line context for last price',
        'bottom_items': ['Time', 'Daily', 'Indic.', 'Draw', 'Chip', 'QFQ', 'Export'],
        'status': 'Status bar: chip panel toggles without changing the main chart',
        'title': '{code} {name} | layout mockup (not a real UI)',
    },
}


def setup_fonts() -> str:
    """优先中文字体，找不到则退回英文标注（返回实际使用的语言键）。"""
    available = {f.name for f in font_manager.fontManager.ttflist}
    for name in FONT_ZH:
        if name in available:
            plt.rcParams['font.sans-serif'] = [name, 'DejaVu Sans']
            plt.rcParams['axes.unicode_minus'] = False
            return 'zh'
    print('警告：未找到中文字体，改用英文标注')
    plt.rcParams['font.sans-serif'] = ['DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False
    return 'en'


def load_view(code: str, bars: int):
    """读取筹码结果 + 最近 bars 根 K 线（hfq 归一化到最新收盘）。"""
    names = load_names()
    res = compute_stock(StockSpec(code, ''), names, 200, None)
    df = load_stock(code)
    factor = df['hfq_factor'].to_numpy().astype(float)
    factor = factor / factor[-1]
    o = df['open'].to_numpy().astype(float) * factor
    h = df['high'].to_numpy().astype(float) * factor
    lo = df['low'].to_numpy().astype(float) * factor
    c = df['close'].to_numpy().astype(float) * factor
    ma20 = np.array([c[max(0, i - 19):i + 1].mean() for i in range(len(c))])
    ma60 = np.array([c[max(0, i - 59):i + 1].mean() for i in range(len(c))])
    dates = df['date'].to_list()

    idx = slice(max(0, len(c) - bars), len(c))
    return res, {
        'o': o[idx], 'h': h[idx], 'l': lo[idx], 'c': c[idx],
        'ma20': ma20[idx], 'ma60': ma60[idx], 'dates': dates[idx],
        'start': idx.start,
    }


def fig_xy(fig, ax, x: float, y: float) -> tuple[float, float]:
    """数据坐标 -> figure 坐标。"""
    disp = ax.transData.transform((x, y))
    return tuple(fig.transFigure.inverted().transform(disp))


def panel(fig, rect, facecolor=PANEL_BG) -> None:
    """画一块面板底。"""
    fig.add_artist(
        Rectangle(
            (rect[0], rect[1]), rect[2], rect[3],
            transform=fig.transFigure, facecolor=facecolor,
            edgecolor=BORDER, linewidth=0.9, clip_on=False, zorder=0,
        )
    )


def button(fig, x, y, w, h, label, active=False, size=9.5) -> None:
    """画一个示意按钮。"""
    fig.add_artist(
        FancyBboxPatch(
            (x, y), w, h, boxstyle='round,pad=0.004,rounding_size=0.006',
            transform=fig.transFigure,
            facecolor=('#dbe6f6' if active else '#fbfcfd'),
            edgecolor=('#8fb0d8' if active else BORDER), linewidth=0.9,
            clip_on=False, zorder=2,
        )
    )
    fig.text(x + w / 2, y + h / 2, label, ha='center', va='center',
             fontsize=size, color=('#1f4f86' if active else MUTED), zorder=3)


def callout(fig, ax, xy, text, text_xy, ha='left', size=10.5, color=TEXT) -> None:
    """带引线的小字标注：xy 为数据坐标，text_xy 为 figure 坐标。"""
    target = fig_xy(fig, ax, *xy)
    fig.add_artist(
        FancyArrowPatch(
            text_xy, target, transform=fig.transFigure,
            arrowstyle='-|>', mutation_scale=11, color='#7a8290',
            linewidth=1.0, clip_on=False, zorder=20,
        )
    )
    fig.text(*text_xy, text, ha=ha, va='center', fontsize=size, color=color,
             zorder=21,
             bbox={'facecolor': 'white', 'edgecolor': 'none', 'alpha': 0.82, 'pad': 2})


def draw_top_bar(fig, L, res, code) -> None:
    """顶部示意工具栏。"""
    panel(fig, (0.02, 0.918, 0.96, 0.062), BAR_BG)
    items = [L['prev'], L['next'], L['day'], L['week'], L['month'],
             L['bars'], L['add'], L['cut'], L['qfq']]
    w, gap = 0.070, 0.007
    for i, label in enumerate(items):
        button(fig, 0.031 + i * (w + gap), 0.929, w, 0.040, label, active=(i == 2), size=8.8)
    fig.text(0.945, 0.949, f'{code} {res.name}', ha='right', va='center',
             fontsize=11, color=TEXT, zorder=3)


def draw_bottom_bar(fig, L) -> None:
    """底部示意工具栏 + 筹码按钮。"""
    panel(fig, (0.02, 0.022, 0.96, 0.060), BAR_BG)
    labels = L['bottom_items']
    w, gap = 0.062, 0.006
    for i, label in enumerate(labels):
        button(fig, 0.031 + i * (w + gap), 0.031, w, 0.042, label,
               active=(i == 4), size=8.8)
    fig.text(0.60, 0.052, L['status'], ha='left',
             va='center', fontsize=8.5, color=MUTED, zorder=3)


def draw_candles(ax, view, L) -> None:
    """K 线 + MA20/MA60。"""
    o, h, lo, c = view['o'], view['h'], view['l'], view['c']
    n = len(c)
    x = np.arange(n)
    for i in range(n):
        color = UP if c[i] >= o[i] else DOWN
        ax.vlines(x[i], lo[i], h[i], color=color, linewidth=0.7, alpha=0.85, zorder=2)
        body_lo, body_h = min(o[i], c[i]), abs(c[i] - o[i])
        ax.add_patch(
            Rectangle((x[i] - 0.32, body_lo), 0.64, max(body_h, 1e-6),
                      facecolor=color, edgecolor=color, linewidth=0.6, zorder=3)
        )
    ax.plot(x, view['ma20'], color=MA20_COLOR, linewidth=1.1, label=L['ma'], zorder=4)
    ax.plot(x, view['ma60'], color=MA60_COLOR, linewidth=1.1, zorder=4)

    steps = max(1, n // 6)
    ticks = list(range(0, n, steps))
    ax.set_xticks(ticks)
    ax.set_xticklabels(
        [view['dates'][i].strftime('%y-%m-%d') for i in ticks],
        fontsize=8, color=MUTED,
    )
    ax.tick_params(axis='x', length=3, colors=MUTED, pad=2)
    ax.set_xlim(-2, n + 2)
    ax.grid(True, color='#eef1f4', linewidth=0.7)
    ax.set_axisbelow(True)
    for side in ('top', 'right'):
        ax.spines[side].set_visible(False)
    ax.spines['left'].set_color(BORDER)
    ax.spines['bottom'].set_color(BORDER)
    ax.tick_params(axis='y', labelsize=9, colors=MUTED, length=3)
    ax.set_ylabel('价格（hfq，归一化到最新收盘）', fontsize=9.5, color=MUTED)
    ax.legend(loc='upper left', fontsize=8.5, frameon=True, framealpha=0.9,
              edgecolor=BORDER, borderpad=0.4, handlelength=1.6)
    ax.set_title(L['kline'].format(n=n), fontsize=11, color=TEXT, loc='left', pad=6)


def draw_bars(ax, res, mask, L, legend=True) -> None:
    """水平筹码柱：衰减=柱条，静态=更浅的阶梯线（对照）。"""
    centers = 0.5 * (res.edges[:-1] + res.edges[1:])
    bin_w = res.edges[1] - res.edges[0]
    ax.barh(centers[mask], res.decay_pct[mask], height=bin_w * 0.58,
            color=DECAY_COLOR, alpha=0.78, linewidth=0, zorder=3,
            label=L['chip_legend_decay'])
    ax.plot(res.static_pct[mask], centers[mask], drawstyle='steps-mid',
            color=STATIC_COLOR, alpha=0.95, linewidth=1.25, zorder=4,
            label=L['chip_legend_static'])
    ax.set_xlim(0, max(res.decay_pct[mask].max(), res.static_pct[mask].max()) * 1.16)
    ax.grid(True, axis='x', color='#f0f2f5', linewidth=0.6)
    ax.set_axisbelow(True)
    if legend:
        ax.legend(loc='upper right', fontsize=8, frameon=True, framealpha=0.9,
                  edgecolor=BORDER, borderpad=0.35, handlelength=1.4)


def layout_a(fig, res, view, L, window) -> None:
    """方案 A：K 线 75% + 筹码栏 25%，共用价格纵轴。"""
    panel(fig, (0.02, 0.092, 0.96, 0.818))
    ax_k = fig.add_axes((0.052, 0.130, 0.638, 0.712))
    ax_c = fig.add_axes((0.706, 0.130, 0.238, 0.712), sharey=ax_k, facecolor='#fcfdff')

    draw_candles(ax_k, view, L)
    centers = 0.5 * (res.edges[:-1] + res.edges[1:])
    mask = (centers >= window[0]) & (centers <= window[1])
    draw_bars(ax_c, res, mask, L)

    lo, hi = window
    ax_k.set_ylim(lo, hi)
    for ax in (ax_k, ax_c):
        ax.set_yticks(np.linspace(lo, hi, 8))

    ax_c.set_xlabel(L['chip_x'], fontsize=9, color=MUTED)
    ax_c.tick_params(axis='x', labelsize=8, colors=MUTED, length=2)
    for side in ('top', 'left', 'bottom'):
        ax_c.spines[side].set_visible(False)
    ax_c.spines['right'].set_color(BORDER)
    ax_c.tick_params(axis='y', labelright=True, labelleft=False, right=True, left=False,
                     labelsize=8.5, colors=MUTED, length=2)

    # 现价与主峰：贯穿左右两区
    last = res.last_close
    peak = res.peaks_decay[0]
    for ax in (ax_k, ax_c):
        ax.axhline(last, color='#3a4048', linestyle=(0, (6, 4)), linewidth=1.2, zorder=6)
        ax.axhline(peak['price'], color=DECAY_COLOR, linestyle=(0, (2, 3)),
                   linewidth=1.0, alpha=0.85, zorder=5)

    fig.canvas.draw()

    # 布局标注
    n = len(view['c'])
    callout(fig, ax_c, (res.decay_pct[mask].max() * 0.62, peak['price']),
            L['peak'].format(p=peak['price'], s=peak['share'] * 100),
            (0.933, fig_xy(fig, ax_c, 0, peak['price'])[1] + 0.016), ha='right',
            color=DECAY_COLOR)
    callout(fig, ax_k, (n * 0.62, last), L['price'].format(p=last),
            (0.345, 0.855), size=11)
    fig.text(0.933, fig_xy(fig, ax_c, 0, last)[1], L['price'].format(p=last),
             ha='right', va='center', fontsize=10, color='#3a4048', zorder=21,
             bbox={'facecolor': 'white', 'edgecolor': 'none', 'alpha': 0.8, 'pad': 2})
    fig.text(0.942, 0.855, L['chip_panel'], ha='right', va='center', fontsize=11.5,
             color=TEXT, zorder=21,
             bbox={'facecolor': 'white', 'edgecolor': BORDER, 'alpha': 0.95, 'pad': 4})

    callout(fig, ax_c, (res.decay_pct[mask].max() * 0.72,
                        window[0] + (window[1] - window[0]) * 0.62),
            L['bins'], (0.60, 0.215), ha='right', size=9.5, color=MUTED)

    # 底部「筹码」按钮引线
    bx, by = 0.031 + 4 * 0.068, 0.052
    fig.add_artist(
        FancyArrowPatch((bx + 0.03, by), (0.745, 0.137), transform=fig.transFigure,
                        arrowstyle='-|>', mutation_scale=11, color='#7a8290',
                        linewidth=1.0, clip_on=False, zorder=20)
    )
    fig.text(bx + 0.045, 0.083, L['button_note'], fontsize=9, color=MUTED, zorder=21,
             bbox={'facecolor': 'white', 'edgecolor': 'none', 'alpha': 0.8, 'pad': 2})

    off_static = 100 - res.static_pct[mask].sum()
    off_decay = 100 - res.decay_pct[mask].sum()
    ax_k.text(0.006, 0.012, L['window_note'].format(s=off_static, d=off_decay),
              transform=ax_k.transAxes, ha='left', va='bottom', fontsize=8.2,
              color=MUTED, zorder=10,
              bbox={'facecolor': 'white', 'edgecolor': 'none', 'alpha': 0.85, 'pad': 2})


def layout_b(fig, res, L) -> None:
    """方案 B：独立窗口，价格纵轴（全历史）+ 筹码横轴。"""
    panel(fig, (0.02, 0.075, 0.96, 0.855))
    ax = fig.add_axes((0.085, 0.140, 0.700, 0.735), facecolor='#fcfdff')

    centers = 0.5 * (res.edges[:-1] + res.edges[1:])
    mask_all = np.ones_like(centers, dtype=bool)
    draw_bars(ax, res, mask_all, L)

    lo, hi = float(res.edges[0]), float(res.edges[-1])
    ax.set_ylim(lo, hi)
    ax.set_yticks(np.linspace(lo, hi, 10))
    ax.tick_params(axis='y', labelsize=9, colors=MUTED, length=3)
    ax.tick_params(axis='x', labelsize=9, colors=MUTED, length=3)
    ax.set_ylabel('价格（hfq，归一化到最新收盘）', fontsize=9.5, color=MUTED)
    ax.set_xlabel(L['chip_x'], fontsize=9.5, color=MUTED)
    ax.grid(True, color='#eef1f4', linewidth=0.7)
    ax.set_axisbelow(True)
    for side in ('top', 'right'):
        ax.spines[side].set_visible(False)
    ax.spines['left'].set_color(BORDER)
    ax.spines['bottom'].set_color(BORDER)
    ax.set_title('筹码分布（200 箱，全历史价格区间）', fontsize=12, color=TEXT,
                 loc='left', pad=8)

    last = res.last_close
    peak = res.peaks_decay[0]
    ax.axhline(last, color='#3a4048', linestyle=(0, (6, 4)), linewidth=1.2, zorder=6)
    ax.axhline(peak['price'], color=DECAY_COLOR, linestyle=(0, (2, 3)),
               linewidth=1.0, alpha=0.85, zorder=5)

    # 放大窗：全区间下衰减筹码只占顶部约 4% 高度，单独放一块放大视图看 200 箱细节
    inset = fig.add_axes((0.792, 0.285, 0.185, 0.330), facecolor='white')
    zoom = (peak['price'] - 0.9, peak['price'] + 0.5)
    mask_zoom = (centers >= zoom[0]) & (centers <= zoom[1])
    draw_bars(inset, res, mask_zoom, L, legend=False)
    inset.set_ylim(*zoom)
    inset.set_yticks(np.linspace(zoom[0], zoom[1], 3))
    inset.tick_params(axis='x', labelbottom=False)
    inset.tick_params(axis='y', labelleft=False, labelright=True, right=True,
                      labelsize=7.5, colors=MUTED, length=2)
    inset.tick_params(axis='x', length=2)
    for side in ('top', 'left'):
        inset.spines[side].set_visible(False)
    inset.spines['right'].set_color(BORDER)
    inset.spines['bottom'].set_color(BORDER)
    inset.set_title(f'放大 {zoom[0]:.2f}–{zoom[1]:.2f}（{int(mask_zoom.sum())} 箱）',
                    fontsize=8.5, color=MUTED, loc='left', pad=4)
    inset.axhline(last, color='#3a4048', linestyle=(0, (6, 4)), linewidth=1.0, zorder=6)

    fig.canvas.draw()
    callout(fig, ax, (res.decay_pct.max() * 0.50, peak['price']),
            L['peak'].format(p=peak['price'], s=peak['share'] * 100),
            (0.300, fig_xy(fig, ax, 0, peak['price'])[1] + 0.008), ha='right',
            color=DECAY_COLOR, size=11)
    callout(fig, ax, (res.decay_pct.max() * 0.12, last),
            L['price'].format(p=last), (0.140, fig_xy(fig, ax, 0, last)[1] + 0.010),
            size=11)
    fig.text(0.812, 0.905, '全历史 200 箱，静态/衰减叠加', ha='left', va='center',
             fontsize=10, color=MUTED, zorder=21)
    fig.text(0.812, 0.868,
             f'静态总量 {res.static_total:,.0f} 手\n衰减总量 {res.decay_total:,.0f} 手',
             ha='left', va='top', fontsize=9.5, color=MUTED, linespacing=1.6, zorder=21)
    fig.text(0.792, 0.775, L['tradeoff'], ha='left', va='top', fontsize=10,
             color=TEXT, zorder=21,
             bbox={'facecolor': '#fffaf4', 'edgecolor': '#f0d9bf', 'pad': 6})


def main() -> None:
    """入口：出两张布局示意图。"""
    parser = argparse.ArgumentParser(description='筹码峰界面布局示意（mockup）')
    parser.add_argument('--code', default='601398.SH', help='标的，默认 601398.SH')
    parser.add_argument('--bars', type=int, default=160, help='K 线根数，默认 160')
    parser.add_argument('--out', default=str(DEFAULT_OUT), help='输出目录')
    args = parser.parse_args()

    lang = setup_fonts()
    L = LABELS[lang]
    res, view = load_view(args.code, args.bars)

    span = float(res.edges[-1] - res.edges[0])
    candle_lo = float(view['l'].min())
    candle_hi = float(view['h'].max())
    pad = (candle_hi - candle_lo) * 0.14
    window = (candle_lo - pad, candle_hi + pad)
    centers = 0.5 * (res.edges[:-1] + res.edges[1:])
    mask = (centers >= window[0]) & (centers <= window[1])
    print(f'{args.code} {res.name} 全历史 hfq 区间 {res.edges[0]:.2f}~{res.edges[-1]:.2f} 箱宽 {span / 200:.3f}')
    print(f'K 线区间 {candle_lo:.2f}~{candle_hi:.2f}，共用纵轴窗口 {window[0]:.2f}~{window[1]:.2f}')
    print(f'窗口内筹码占比：静态 {res.static_pct[mask].sum():.1f}% / 衰减 {res.decay_pct[mask].sum():.1f}%')

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    # 方案 A
    fig = plt.figure(figsize=FIG_SIZE, dpi=DPI, facecolor=BG)
    draw_top_bar(fig, L, res, args.code)
    draw_bottom_bar(fig, L)
    layout_a(fig, res, view, L, window)
    fig.text(0.02, 0.985, L['title'].format(code=args.code, name=res.name),
             ha='left', va='top', fontsize=13, color=TEXT, zorder=25)
    path_a = out / 'layout_mockup_main.png'
    fig.savefig(path_a, dpi=DPI, facecolor=BG)
    plt.close(fig)

    # 方案 B
    fig = plt.figure(figsize=FIG_SIZE, dpi=DPI, facecolor=BG)
    draw_top_bar(fig, L, res, args.code)
    draw_bottom_bar(fig, L)
    layout_b(fig, res, L)
    fig.text(0.02, 0.985, L['title'].format(code=args.code, name=res.name),
             ha='left', va='top', fontsize=13, color=TEXT, zorder=25)
    path_b = out / 'layout_mockup_window.png'
    fig.savefig(path_b, dpi=DPI, facecolor=BG)
    plt.close(fig)

    # 校验说明：两 axes 使用同一 [bottom, height] 矩形 + sharey，刻度由同一 locator 生成
    print(f'saved {path_a}')
    print(f'saved {path_b}')


if __name__ == '__main__':
    main()
