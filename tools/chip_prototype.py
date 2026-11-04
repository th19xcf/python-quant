"""筹码分布（筹码峰）B 档原型试画：静态 VAP vs 衰减 VAP。

依赖说明：本脚本出图走 PySide6/Qt，不需要 matplotlib；同目录的
`tools/chip_layout_mockup.py` 依赖 matplotlib，出图时需临时安装
（`pip install matplotlib`），项目常规依赖中未包含它。

定位：形状验证原型，不是生产实现。只读
`data/local_store/quotes/stock/{sh,sz,bj}/*.parquet`，不写库、不改生产模块。

口径摘要：
- 价格轴：hfq（`price * hfq_factor`），再整体除以最新 `hfq_factor`，
  使最新 hfq 收盘价等于最新原始收盘价，便于与现价直接对比。
- 箱数 200，价格范围取该票全历史 hfq 的 min(low) ~ max(high)。
- 单日成交量按三角分布摊到价位箱，峰值取 `(H+L+C)/3`，支撑区间 `[L, H]`。
- 静态 VAP：不衰减，逐日累加。
- 衰减 VAP：`chip_t = chip_{t-1} * (1 - d_t) + 当日新增`。
  `d_t` 默认用代理口径：`r = volume / MA20(volume)`，
  `d = clip(0.03 * r, 0.02, 0.10)`。即量比 1（当日量等于 MA20 水平）对应 3%/日，
  相当于 33 个交易日左右的记忆长度；量比 >= 3.33 压到上限 10%，<= 0.67 保底 2%。
  MA20 不足 20 日时用 expanding mean 兜底，并在质量报告中计数。
- 停牌日：quote 里整行缺失。本原型按「无交易日 = 无换手 = 不衰减」处理，
  即缺失日不产生衰减步，只在有成交的交易日衰减一次；停牌时长单独统计。

换手率来源可替换：`--turnover-column turn` 时直接读该列（单位 %，
`d = clip(turn, 2, 10) / 100`），并行任务补齐 turn 后改一个参数即可切 A 档。

用法：
    .venv\\Scripts\\python.exe tools\\chip_prototype.py
    .venv\\Scripts\\python.exe tools\\chip_prototype.py --codes 600519.SH,601398.SH
    .venv\\Scripts\\python.exe tools\\chip_prototype.py --turnover-column turn
"""
# ruff: noqa: T201  # 原型脚本面向终端输出

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl
from scipy.signal import find_peaks

PROJECT_ROOT = Path(__file__).resolve().parent.parent
QUOTE_ROOT = PROJECT_ROOT / 'data' / 'local_store' / 'quotes' / 'stock'
NAMES_PATH = PROJECT_ROOT / 'data' / 'local_store' / 'meta' / 'names.json'
DEFAULT_OUT = PROJECT_ROOT / 'reports' / 'chip_prototype'

# Qt 渲染：默认 windows 平台（离屏 offscreen 平台在 Windows 上字体库为空，
# 中文会渲染成方块），QPainter 直接画在 QImage 上不需要可见窗口
from PySide6.QtCore import QPointF, QRectF, Qt  # noqa: E402
from PySide6.QtGui import (  # noqa: E402
    QBrush,
    QColor,
    QFont,
    QFontDatabase,
    QGuiApplication,
    QImage,
    QPainter,
    QPen,
    QPolygonF,
)

DECAY_MIN = 0.02
DECAY_MAX = 0.10
DECAY_BASE = 0.03  # 量比=1（当日量为 MA20 水平）对应 3%/日，A 股换手中位量级
TURNOVER_CLIP = (2.0, 10.0)

# Qt 应用对象必须持有引用，避免渲染结束后被 GC 回收
_QT_APP = None


@dataclass
class StockSpec:
    """标的与其选择理由。"""

    code: str
    reason: str


@dataclass
class QualityReport:
    """缺失/异常值统计，不允许静默吞掉。"""

    code: str
    rows: int
    date_from: str
    date_to: str
    null_price_rows: int
    nonpositive_volume_rows: int
    bad_high_low_rows: int
    warmup_rows: int
    suspension_gaps: list[tuple[str, int]]
    missing_calendar_days: int
    dropped_rows: int


@dataclass
class StockResult:
    """单票计算结果，供绘图与汇总使用。"""

    spec: StockSpec
    name: str
    quality: QualityReport
    edges: np.ndarray
    static_pct: np.ndarray
    decay_pct: np.ndarray
    static_total: float
    decay_total: float
    last_close: float
    peaks_static: list[dict]
    peaks_decay: list[dict]


STOCKS = [
    StockSpec('600519.SH', '长期上涨的行业龙头（白酒），hfq 口径下价格重心持续上移'),
    StockSpec('601398.SH', '长期横盘的大盘银行股，价格区间窄、换手稳定'),
    StockSpec('688795.SH', '2025-12-05 上市的科创板次新股（上市不足 1 年），覆盖新股场景'),
    StockSpec('920010.BJ', '北交所标的（2020-07 起有数据），检验 .BJ 数据与低流动性'),
    StockSpec('002695.SZ', '2016-09 高送转，hfq 单日跳变约 4 倍，检验复权处理'),
    StockSpec('000029.SZ', '曾停牌 1518 天、2020-11 复牌，检验缺失日衰减处理'),
]


def stock_path(code: str) -> Path:
    """按后缀定位本地 quote parquet。"""
    market = code.rsplit('.', maxsplit=1)[-1].lower()
    return QUOTE_ROOT / market / f'{code}.parquet'


def load_names() -> dict[str, str]:
    """读取本地股票名称表（缺失时返回空表）。"""
    if not NAMES_PATH.exists():
        return {}
    return json.loads(NAMES_PATH.read_text(encoding='utf-8'))


def load_stock(code: str) -> pl.DataFrame:
    """读取并校验单票日线。"""
    path = stock_path(code)
    if not path.exists():
        raise FileNotFoundError(f'找不到 quote 文件: {path}')
    df = pl.read_parquet(path).sort('date')
    required = {'date', 'close', 'high', 'low', 'volume', 'hfq_factor'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f'{code} 缺少列: {sorted(missing)}')
    return df


def clean_stock(df: pl.DataFrame) -> pl.DataFrame:
    """剔除不可用于计算的脏行（缺失价量、量<=0、high<low），保留原始行以便计数。"""
    needed = ['open', 'high', 'low', 'close', 'volume', 'hfq_factor']
    valid = pl.all_horizontal([pl.col(c).is_not_null() for c in needed])
    return df.filter(
        valid & (pl.col('volume') > 0) & (pl.col('high') >= pl.col('low'))
    ).sort('date')


def build_quality(df: pl.DataFrame, code: str, warmup_rows: int, dropped: int) -> QualityReport:
    """统计缺失/异常值，并汇总停牌断档。"""
    null_price = df.select(
        pl.any_horizontal(
            [
                pl.col(c).is_null()
                for c in ('open', 'high', 'low', 'close', 'volume', 'hfq_factor')
            ]
        ).sum()
    ).item()
    nonpositive = (df['volume'] <= 0).sum()
    bad_hl = (df['high'] < df['low']).sum()

    gaps = (
        df.with_columns((pl.col('date') - pl.col('date').shift(1)).dt.total_days().alias('gap'))
        .filter(pl.col('gap') > 3)
        .sort('gap', descending=True)
    )
    gap_list = [(str(r['date']), int(r['gap'])) for r in gaps.head(5).iter_rows(named=True)]
    missing_days = int((df['date'].diff().dt.total_days().fill_null(0) - 1).clip(0).sum())

    return QualityReport(
        code=code,
        rows=df.height,
        date_from=str(df['date'][0]),
        date_to=str(df['date'][-1]),
        null_price_rows=int(null_price),
        nonpositive_volume_rows=int(nonpositive),
        bad_high_low_rows=int(bad_hl),
        warmup_rows=warmup_rows,
        suspension_gaps=gap_list,
        missing_calendar_days=missing_days,
        dropped_rows=dropped,
    )


def decay_rates(df: pl.DataFrame, turnover_column: str | None) -> tuple[np.ndarray, int]:
    """计算每日衰减率 d_t，返回 (d 数组, MA20 预热行数)。

    turnover_column 为 None 时走代理口径 vol/MA20；否则直接读换手率列（单位 %）。
    """
    if turnover_column is not None:
        if turnover_column not in df.columns:
            raise ValueError(
                f'指定的换手率列 {turnover_column!r} 不存在，当前列为 {df.columns}'
            )
        turn = df[turnover_column].to_numpy().astype(float)
        d = np.clip(turn, *TURNOVER_CLIP) / 100.0
        return d, 0

    vol = df['volume'].to_numpy().astype(float)
    vol_ma = (
        df.select(
            pl.col('volume').rolling_mean(window_size=20, min_samples=1).alias('ma')
        )['ma']
        .to_numpy()
        .astype(float)
    )
    # 不足 20 个交易日时用 expanding mean 兜底，预热行数如实上报
    warmup = min(df.height, 19)
    ratio = np.divide(vol, vol_ma, out=np.ones_like(vol), where=vol_ma > 0)
    d = np.clip(DECAY_BASE * ratio, DECAY_MIN, DECAY_MAX)
    return d, warmup


def triangular_mass(
    lo: np.ndarray, hi: np.ndarray, mode: np.ndarray, edges: np.ndarray
) -> np.ndarray:
    """逐日三角分布在每个价位箱内的质量占比（未乘成交量），形状 (n_days, bins)。"""
    a = lo[:, None]
    b = hi[:, None]
    c = mode[:, None]
    span = np.maximum(b - a, 1e-12)
    x = edges[None, :]
    left = (x - a) ** 2 / (span * np.maximum(c - a, 1e-12))
    right = 1.0 - (b - x) ** 2 / (span * np.maximum(b - c, 1e-12))
    cdf = np.where(
        x <= a, 0.0, np.where(x <= c, left, np.where(x < b, right, 1.0))
    )
    mass = np.diff(cdf, axis=1)
    return np.clip(mass, 0.0, None)


def decay_accumulate(mass: np.ndarray, volume: np.ndarray, d: np.ndarray) -> np.ndarray:
    """衰减 VAP 递推：chip_t = chip_{t-1} * (1 - d_t) + 当日新增。

    缺失日（停牌）在数据里没有行，因此不会进入本循环，等价于「无成交则不衰减」。
    """
    chips = np.zeros(mass.shape[1], dtype=float)
    for i in range(mass.shape[0]):
        chips *= 1.0 - d[i]
        chips += mass[i] * volume[i]
    return chips


def find_chip_peaks(values: np.ndarray, edges: np.ndarray, top_n: int = 3) -> list[dict]:
    """找主峰/次峰（按质量占比排序，峰间至少相隔 6 个箱）。"""
    if values.sum() <= 0:
        return []
    idx, _ = find_peaks(values, distance=6, prominence=values.max() * 0.08)
    if idx.size == 0:
        idx = np.array([int(values.argmax())])
    centers = 0.5 * (edges[:-1] + edges[1:])
    order = idx[np.argsort(values[idx])[::-1]][:top_n]
    return [
        {
            'price': float(centers[i]),
            'share': float(values[i] / values.sum()),
            'bin': int(i),
        }
        for i in order
    ]


def compute_stock(spec: StockSpec, names: dict[str, str], bins: int, turn_col: str | None) -> StockResult:
    """单票完整计算流程。"""
    raw = load_stock(spec.code)
    df = clean_stock(raw)
    dropped = raw.height - df.height
    d, warmup = decay_rates(df, turn_col)
    quality = build_quality(raw, spec.code, warmup, dropped)

    factor = df['hfq_factor'].to_numpy().astype(float)
    factor = factor / factor[-1]
    lo = df['low'].to_numpy().astype(float) * factor
    hi = df['high'].to_numpy().astype(float) * factor
    close = df['close'].to_numpy().astype(float) * factor
    mode = (lo + hi + close) / 3.0
    volume = df['volume'].to_numpy().astype(float)

    edges = np.linspace(float(lo.min()), float(hi.max()), bins + 1)
    mass = triangular_mass(lo, hi, mode, edges)
    static = (mass * volume[:, None]).sum(axis=0)
    decay = decay_accumulate(mass, volume, d)

    return StockResult(
        spec=spec,
        name=names.get(spec.code, ''),
        quality=quality,
        edges=edges,
        static_pct=static / static.sum() * 100.0,
        decay_pct=decay / decay.sum() * 100.0,
        static_total=float(static.sum()),
        decay_total=float(decay.sum()),
        last_close=float(df['close'][-1]),
        peaks_static=find_chip_peaks(static, edges),
        peaks_decay=find_chip_peaks(decay, edges),
    )


# ---------------------------------------------------------------- 绘图

FONT_FAMILY = 'Microsoft YaHei'
COLOR_STATIC = QColor(76, 120, 168)
COLOR_DECAY = QColor(228, 87, 46)
COLOR_GRID = QColor(226, 230, 236)
COLOR_AXIS = QColor(96, 104, 116)
COLOR_TEXT = QColor(32, 36, 44)


def ensure_fonts() -> None:
    """确保中文字体可用，缺失时显式注册 msyh.ttc。"""
    if FONT_FAMILY in QFontDatabase.families():
        return
    for candidate in (r'C:\Windows\Fonts\msyh.ttc', r'C:\Windows\Fonts\simhei.ttf'):
        if Path(candidate).exists():
            QFontDatabase.addApplicationFont(candidate)
            if FONT_FAMILY in QFontDatabase.families():
                return
    raise RuntimeError(f'未找到中文字体 {FONT_FAMILY}，请检查系统字体')


def make_font(size: int, bold: bool = False) -> QFont:
    """统一字体。"""
    f = QFont(FONT_FAMILY, size)
    f.setBold(bold)
    return f


def draw_text(
    p: QPainter,
    x: float,
    y: float,
    text: str,
    size: int,
    color: QColor = COLOR_TEXT,
    bold: bool = False,
    align: int | Qt.AlignmentFlag = Qt.AlignLeft,
    width: float = 460.0,
    height: float = 40.0,
) -> None:
    """在 (x, y) 处画文本。"""
    p.setFont(make_font(size, bold))
    p.setPen(QPen(color))
    p.drawText(QRectF(x, y, width, height), align | Qt.AlignVCenter, text)


def step_polygon(values: np.ndarray, edges: np.ndarray, x2p, y2p) -> QPolygonF:
    """把逐箱数值拼成阶梯多边形。"""
    pts = []
    for i, v in enumerate(values):
        pts.append(QPointF(x2p(float(edges[i])), y2p(float(v))))
        pts.append(QPointF(x2p(float(edges[i + 1])), y2p(float(v))))
    pts.append(QPointF(x2p(float(edges[-1])), y2p(0.0)))
    pts.append(QPointF(x2p(float(edges[0])), y2p(0.0)))
    return QPolygonF(pts)


def draw_chart(p: QPainter, rect: QRectF, res: StockResult, compact: bool = False) -> None:
    """在给定矩形内画一张叠加图。"""
    if compact:
        margin = (40, 12, 46, 26)
        title_size, tick_size, note_size = 12, 8, 8
    else:
        margin = (88, 48, 110, 76)
        title_size, tick_size, note_size = 18, 9, 10

    plot = QRectF(
        rect.left() + margin[0],
        rect.top() + margin[2],
        rect.width() - margin[0] - margin[1],
        rect.height() - margin[2] - margin[3],
    )
    xmin, xmax = float(res.edges[0]), float(res.edges[-1])
    ymax = max(float(res.static_pct.max()), float(res.decay_pct.max())) * 1.12
    ymax = max(ymax, 1e-6)

    def x2p(x: float) -> float:
        return plot.left() + (x - xmin) / (xmax - xmin) * plot.width()

    def y2p(y: float) -> float:
        return plot.bottom() - y / ymax * plot.height()

    p.fillRect(plot, QBrush(QColor(252, 253, 255)))

    # 网格与刻度
    p.setFont(make_font(tick_size))
    for k in range(6):
        xv = xmin + (xmax - xmin) * k / 5
        px = x2p(xv)
        p.setPen(QPen(COLOR_GRID, 1))
        p.drawLine(QPointF(px, plot.top()), QPointF(px, plot.bottom()))
        p.setPen(QPen(COLOR_AXIS))
        p.drawText(
            QRectF(px - 40, plot.bottom() + 4, 80, 20), Qt.AlignHCenter | Qt.AlignTop, f'{xv:.2f}'
        )
    for k in range(5):
        yv = ymax * k / 4
        py = y2p(yv)
        p.setPen(QPen(COLOR_GRID, 1))
        p.drawLine(QPointF(plot.left(), py), QPointF(plot.right(), py))
        p.setPen(QPen(COLOR_AXIS))
        p.drawText(
            QRectF(plot.left() - 70, py - 9, 62, 18),
            Qt.AlignRight | Qt.AlignVCenter,
            f'{yv:.2f}%',
        )

    p.setPen(QPen(COLOR_AXIS, 1))
    p.drawRect(plot)

    # 两条曲线：静态蓝、衰减橙
    series = (
        (res.static_pct, COLOR_STATIC, 70, 1.0 if compact else 1.6),
        (res.decay_pct, COLOR_DECAY, 130, 1.0 if compact else 1.6),
    )
    for values, color, alpha, pen_w in series:
        poly = step_polygon(values, res.edges, x2p, y2p)
        fill = QColor(color)
        fill.setAlpha(alpha)
        p.setBrush(QBrush(fill))
        p.setPen(QPen(color, pen_w))
        p.drawPolygon(poly)

    # 现价竖线
    px_last = x2p(res.last_close)
    p.setPen(QPen(QColor(70, 78, 90), 1, Qt.DashLine))
    p.drawLine(QPointF(px_last, plot.top()), QPointF(px_last, plot.bottom()))
    if not compact:
        # 放在竖线底部，避开左上角图例
        draw_text(
            p,
            min(px_last + 6, plot.right() - 150),
            plot.bottom() - 26,
            f'现价 {res.last_close:.2f}',
            note_size,
            QColor(70, 78, 90),
            bold=True,
            width=150,
            height=18,
        )

    # 主峰标注
    for peaks, color, prefix in ((res.peaks_static, COLOR_STATIC, '静态主峰'),
                                  (res.peaks_decay, COLOR_DECAY, '衰减主峰')):
        if not peaks:
            continue
        main = peaks[0]
        px, py = x2p(main['price']), y2p(main['share'] * 100.0)
        p.setPen(QPen(color, 2))
        p.setBrush(QBrush(QColor(255, 255, 255)))
        p.drawEllipse(QPointF(px, py), 3.5 if compact else 5, 3.5 if compact else 5)
        if not compact:
            draw_text(
                p,
                min(px + 8, plot.right() - 150),
                max(py - 26, plot.top() + 2),
                f'{prefix} {main["price"]:.2f} ({main["share"] * 100:.1f}%)',
                note_size,
                color,
                bold=True,
                width=190,
                height=18,
            )

    # 标题区
    title_y = rect.top() + (2 if compact else 10)
    if compact:
        draw_text(
            p,
            rect.left() + 4,
            title_y,
            f'{res.spec.code} {res.name}',
            title_size,
            COLOR_TEXT,
            bold=True,
            width=rect.width() - 8,
            height=20,
        )
        draw_text(
            p,
            rect.left() + 4,
            title_y + 16,
            f'静态主峰 {res.peaks_static[0]["price"]:.2f}｜衰减主峰 '
            f'{res.peaks_decay[0]["price"]:.2f}｜现价 {res.last_close:.2f}'
            if res.peaks_static and res.peaks_decay
            else '无有效峰',
            note_size,
            QColor(110, 116, 128),
            width=rect.width() - 8,
            height=16,
        )
    else:
        draw_text(
            p,
            plot.left(),
            title_y,
            f'{res.spec.code} {res.name}｜静态 VAP vs 衰减 VAP',
            title_size,
            COLOR_TEXT,
            bold=True,
            width=plot.width(),
            height=30,
        )
        draw_text(
            p,
            plot.left(),
            title_y + 30,
            f'{res.quality.date_from} ~ {res.quality.date_to}｜{res.quality.rows} 行'
            f'｜{len(res.edges) - 1} 箱｜hfq 归一化到最新收盘｜三角分布峰值(H+L+C)/3',
            10,
            QColor(110, 116, 128),
            width=plot.width() * 1.2,
            height=20,
        )
        draw_text(
            p,
            plot.left(),
            plot.bottom() + 34,
            '价格（hfq，已归一化到最新收盘价）',
            10,
            COLOR_AXIS,
            align=Qt.AlignHCenter,
            width=plot.width(),
            height=20,
        )
        p.save()
        p.translate(plot.left() - 62, plot.top() + plot.height() / 2)
        p.rotate(-90)
        draw_text(p, 0, 0, '筹码占比（%）', 10, COLOR_AXIS, align=Qt.AlignHCenter, width=200, height=20)
        p.restore()

        # 图例
        lx, ly = plot.left() + 12, plot.top() + 12
        for i, (label, color) in enumerate(
            (('静态 VAP（不衰减）', COLOR_STATIC), ('衰减 VAP（vol/MA20 代理）', COLOR_DECAY))
        ):
            p.setPen(QPen(color, 1))
            p.setBrush(QBrush(QColor(color.red(), color.green(), color.blue(), 90)))
            p.drawRect(QRectF(lx, ly + i * 20, 18, 12))
            draw_text(p, lx + 24, ly + i * 20 - 3, label, note_size, COLOR_TEXT, width=260, height=18)

        # 口径与总量
        info = (
            f'静态总量 {res.static_total:,.0f} 手｜衰减总量 {res.decay_total:,.0f} 手'
            f'（两条曲线各自按总量归一化，仅比较形状）'
        )
        draw_text(p, plot.left(), plot.bottom() + 56, info, 9, QColor(110, 116, 128),
                  width=plot.width() * 1.5, height=18)


def render_images(results: list[StockResult], out_dir: Path) -> list[Path]:
    """逐票输出 PNG，并输出一张总览图。"""
    global _QT_APP
    out_dir.mkdir(parents=True, exist_ok=True)
    _QT_APP = QGuiApplication.instance() or QGuiApplication(sys.argv[:1])
    ensure_fonts()
    created: list[Path] = []

    for res in results:
        img = QImage(1800, 1000, QImage.Format.Format_ARGB32)
        img.fill(QColor(255, 255, 255))
        p = QPainter(img)
        p.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        p.setRenderHint(QPainter.RenderHint.TextAntialiasing, True)
        draw_chart(p, QRectF(0, 0, 1800, 1000), res)
        p.end()
        path = out_dir / f'{res.spec.code}_chip.png'
        img.save(str(path))
        created.append(path)

    overview = QImage(2400, 1500, QImage.Format.Format_ARGB32)
    overview.fill(QColor(255, 255, 255))
    p = QPainter(overview)
    p.setRenderHint(QPainter.RenderHint.Antialiasing, True)
    p.setRenderHint(QPainter.RenderHint.TextAntialiasing, True)
    draw_text(p, 24, 10, '筹码分布原型总览：静态 VAP（蓝）vs 衰减 VAP（橙）', 22, COLOR_TEXT,
              bold=True, width=1600, height=36)
    cell_w, cell_h = 800, 690
    for i, res in enumerate(results):
        row, col = divmod(i, 3)
        rect = QRectF(20 + col * cell_w, 60 + row * cell_h, cell_w - 20, cell_h - 10)
        draw_chart(p, rect, res, compact=True)
    p.end()
    overview_path = out_dir / 'overview_chip.png'
    overview.save(str(overview_path))
    created.append(overview_path)
    return created


def write_metrics(results: list[StockResult], path: Path, turn_col: str | None) -> None:
    """输出结构化指标，供 Markdown 汇总引用。"""
    payload = {
        'turnover_source': turn_col or 'proxy: volume/MA20 -> [0.02, 0.10]',
        'stocks': [],
    }
    for res in results:
        payload['stocks'].append(
            {
                'code': res.spec.code,
                'name': res.name,
                'reason': res.spec.reason,
                'rows': res.quality.rows,
                'date_from': res.quality.date_from,
                'date_to': res.quality.date_to,
                'null_price_rows': res.quality.null_price_rows,
                'nonpositive_volume_rows': res.quality.nonpositive_volume_rows,
                'bad_high_low_rows': res.quality.bad_high_low_rows,
                'warmup_rows': res.quality.warmup_rows,
                'suspension_gaps': res.quality.suspension_gaps,
                'missing_calendar_days': res.quality.missing_calendar_days,
                'last_close': round(res.last_close, 3),
                'static_total': round(res.static_total, 1),
                'decay_total': round(res.decay_total, 1),
                'peaks_static': [
                    {'price': round(x['price'], 3), 'share': round(x['share'] * 100, 2)}
                    for x in res.peaks_static
                ],
                'peaks_decay': [
                    {'price': round(x['price'], 3), 'share': round(x['share'] * 100, 2)}
                    for x in res.peaks_decay
                ],
            }
        )
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def parse_args() -> argparse.Namespace:
    """命令行参数。"""
    parser = argparse.ArgumentParser(description='筹码分布原型试画（静态 vs 衰减）')
    parser.add_argument('--codes', default='', help='逗号分隔的 ts_code，留空用内置 6 只')
    parser.add_argument('--bins', type=int, default=200, help='价格箱数，默认 200')
    parser.add_argument('--turnover-column', default=None,
                        help='换手率列名（单位 %%，如 turn）；留空走 vol/MA20 代理')
    parser.add_argument('--out', default=str(DEFAULT_OUT), help='输出目录')
    return parser.parse_args()


def main() -> None:
    """入口。"""
    args = parse_args()
    names = load_names()
    specs = STOCKS
    if args.codes:
        codes = [c.strip() for c in args.codes.split(',') if c.strip()]
        specs = [StockSpec(c, '命令行指定') for c in codes]

    results = [compute_stock(s, names, args.bins, args.turnover_column) for s in specs]
    for res in results:
        q = res.quality
        print(
            f'{res.spec.code} {res.name} rows={q.rows} {q.date_from}~{q.date_to} '
            f'null={q.null_price_rows} vol<=0={q.nonpositive_volume_rows} '
            f'bad_hl={q.bad_high_low_rows} warmup={q.warmup_rows} '
            f'missing_days={q.missing_calendar_days} gaps={q.suspension_gaps[:2]}'
        )
        print(
            f'  现价={res.last_close:.2f} 静态主峰={res.peaks_static[0]["price"]:.2f}'
            f'({res.peaks_static[0]["share"] * 100:.1f}%) '
            f'衰减主峰={res.peaks_decay[0]["price"]:.2f}({res.peaks_decay[0]["share"] * 100:.1f}%)'
        )

    out_dir = Path(args.out)
    created = render_images(results, out_dir)
    write_metrics(results, out_dir / 'metrics.json', args.turnover_column)
    for path in created:
        print('saved', path)


if __name__ == '__main__':
    main()
