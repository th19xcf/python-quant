#!/usr/bin/env python3

"""
主图右侧筹码分布栏：与实际价格轴同源、可按可视区间裁剪、可切全区间。
"""

import contextlib
import time
from collections.abc import Callable
from datetime import date

import numpy as np
import pyqtgraph as pg
from PySide6.QtCore import QPointF, QRectF, Qt, QTimer
from PySide6.QtGui import QColor, QPainter, QPicture, QResizeEvent
from PySide6.QtWidgets import QSizePolicy, QVBoxLayout, QWidget

from src.api.chip_api import (
    CLAMPED_WARN_THRESHOLD,
    DEFAULT_BINS,
    DEFAULT_TURNOVER_FLOOR_MODE,
    DEFAULT_TURNOVER_SOURCE,
    TURNOVER_FLOOR_LABELS,
    TURNOVER_FLOOR_PRESETS,
    TURNOVER_SOURCES,
    ChipProfile,
    compute_chip_profile,
    prepare_chip_frame,
)
from src.ui.task_manager import global_task_manager
from src.utils.logger import logger

# 与通达信接近的暖色获利盘 / 冷色套牢盘
PROFIT_COLOR = (255, 193, 0)
TRAPPED_COLOR = (63, 167, 255)
PRICE_LINE_COLOR = (235, 235, 235, 220)
UP_ARROW_COLOR = (255, 215, 0)

# 全区间开关打开时，主图价格轴按历史全区间扩展
FULL_RANGE_PADDING = 0.01
# 可视价格区间变化后重新分箱的防抖时间
REBIN_DEBOUNCE_MS = 180
# 首次贴图延后时间：保证 K 线先画出来，再算筹码
FIRST_REBUILD_DELAY_MS = 40
# 历史数据内存缓存上限（按 标的+复权 计）
HISTORY_CACHE_LIMIT = 4


class ChipBarsItem(pg.GraphicsObject):
    """横向筹码柱：x 为占比（%），y 为价格，用 QPicture 缓存后贴图。"""

    def __init__(self, profile: ChipProfile):
        super().__init__()
        self._picture = QPicture()
        self._bound = QRectF()
        self._build(profile)

    def _build(self, profile: ChipProfile) -> None:
        picture = QPicture()
        painter = QPainter(picture)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, False)
        painter.setPen(Qt.PenStyle.NoPen)

        max_share = max(profile.max_share, 1e-9)
        last_close = profile.last_close
        for idx, share in enumerate(profile.shares):
            if share <= 0:
                continue
            y_low = float(profile.edges[idx])
            height = float(profile.edges[idx + 1] - profile.edges[idx])
            center = 0.5 * (profile.edges[idx] + profile.edges[idx + 1])
            base = PROFIT_COLOR if center <= last_close else TRAPPED_COLOR
            # 按密度加深，模拟通达信的层次感
            alpha = int(150 + 100 * min(float(share) / max_share, 1.0))
            painter.fillRect(
                QRectF(0.0, y_low, float(share), height),
                QColor(base[0], base[1], base[2], alpha),
            )
        painter.end()
        self._picture = picture
        self._bound = QRectF(picture.boundingRect())

    def paint(self, painter, *args):  # noqa: ANN001 - pyqtgraph 固定签名
        painter.drawPicture(0, 0, self._picture)

    def boundingRect(self) -> QRectF:
        return self._bound


class ChipPanel(QWidget):
    """窄栏筹码面板。"""

    def __init__(self, main_plot_widget: pg.PlotWidget, parent=None):
        super().__init__(parent)
        self.setObjectName('chipPanel')
        self.setMinimumWidth(140)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        plot = pg.PlotWidget()
        plot.setObjectName('chipPlotWidget')
        plot.setBackground('#000000')
        plot.setMenuEnabled(False)
        plot.hideButtons()
        plot.setMouseEnabled(False, False)
        self.plot_widget = plot
        layout.addWidget(plot)

        # 左侧不再重复标价；把宽度压到 0 只保留绘图区
        left_axis = plot.getAxis('left')
        left_axis.setStyle(showValues=False, tickLength=0)
        left_axis.setWidth(0)
        left_axis.setTicks([[]])
        left_axis.setPen(pg.mkPen((0, 0, 0, 0)))

        # 底部与主图日期轴同高（20px），但刻度留空，保证上下边界像素对齐
        bottom_axis = plot.getAxis('bottom')
        bottom_axis.setHeight(20)
        bottom_axis.setStyle(showValues=False, tickLength=0)
        bottom_axis.setTicks([[]])
        bottom_axis.setPen(pg.mkPen('#555555'))

        view_box = plot.getViewBox()
        view_box.setMouseEnabled(x=False, y=False)
        view_box.setMenuEnabled(False)

        self._profile: ChipProfile | None = None
        self._bars: ChipBarsItem | None = None
        self._price_line: pg.InfiniteLine | None = None
        self._peak_marker: pg.ScatterPlotItem | None = None
        self._peak_label: pg.TextItem | None = None
        self._legend: pg.TextItem | None = None
        # 宽度变化回调：让右侧占位列与筹码栏保持同一列宽
        self._width_callback: Callable[[int], None] | None = None

        # 主图价格轴变化时，右栏同步
        main_plot_widget.getViewBox().sigYRangeChanged.connect(self._on_main_y_changed)

    @property
    def profile(self) -> ChipProfile | None:
        """当前渲染的筹码分布，主要供测试断言。"""
        return self._profile

    def is_visible(self) -> bool:
        """筹码栏是否处于显示状态。"""
        return not self.isHidden()

    def set_width_callback(self, callback: Callable[[int], None] | None) -> None:
        """注册宽度变化回调（副图右侧占位列据此同步列宽）。"""
        self._width_callback = callback

    def resizeEvent(self, event: QResizeEvent) -> None:
        super().resizeEvent(event)
        if self._width_callback is not None:
            self._width_callback(self.width())

    def _on_main_y_changed(self, _view_box, y_range) -> None:
        self.sync_y_range(y_range)

    def sync_y_range(self, y_range) -> None:
        """把右栏价格轴对齐到主图。"""
        low, high = float(y_range[0]), float(y_range[1])
        if not np.isfinite(low) or not np.isfinite(high) or high <= low:
            return
        self.plot_widget.getViewBox().setYRange(low, high, padding=0)

    def map_price_to_scene_y(self, price: float) -> float:
        """价格 -> 场景像素 y，用于左右两区对齐断言。"""
        point = self.plot_widget.getViewBox().mapViewToScene(QPointF(0.0, price))
        return float(point.y())

    def render_profile(self, profile: ChipProfile) -> None:
        """按最新分布重建全部图元。"""
        self._profile = profile
        plot = self.plot_widget
        plot.clear()

        self._bars = ChipBarsItem(profile)
        plot.addItem(self._bars)

        max_share = max(profile.max_share, 1.0)
        plot.setXRange(0.0, max_share * 1.35, padding=0)
        self.sync_y_range(plot.getViewBox().viewRange()[1])

        self._price_line = pg.InfiniteLine(
            pos=profile.last_close,
            angle=0,
            pen=pg.mkPen(PRICE_LINE_COLOR, width=1, style=Qt.PenStyle.DashLine),
            movable=False,
        )
        plot.addItem(self._price_line)

        self._legend = pg.TextItem(
            html=(
                '<span style="color:#FFC000;font-size:9pt;">■ 获利</span>'
                '<span style="color:#3FA7FF;font-size:9pt;">　■ 套牢</span>'
                '<span style="color:#888888;font-size:8pt;">　现价 '
                f'{profile.last_close:.2f}</span>'
            ),
            anchor=(0, 0),
        )
        plot.addItem(self._legend)

        if profile.peaks:
            peak = profile.peaks[0]
            peak_x = peak.share * 100.0
            self._peak_marker = pg.ScatterPlotItem(
                [peak_x],
                [peak.price],
                symbol='o',
                size=6,
                brush=pg.mkBrush(UP_ARROW_COLOR),
                pen=pg.mkPen('#000000'),
            )
            plot.addItem(self._peak_marker)
            self._peak_label = pg.TextItem(
                f'主峰 {peak.price:.2f} ({peak.share * 100:.1f}%)',
                color=UP_ARROW_COLOR,
                anchor=(1.0, 0.5),
            )
            plot.addItem(self._peak_label)

        self._position_texts()

    def _position_texts(self) -> None:
        """文字标注跟随当前价格视口，缩放后仍可见。"""
        low, high = self.plot_widget.getViewBox().viewRange()[1]
        span = max(float(high - low), 1e-9)
        max_share = max(self._profile.max_share, 1.0) if self._profile else 1.0

        if self._legend is not None:
            self._legend.setPos(0.03 * max_share, float(high) - 0.02 * span)
        if self._peak_label is not None and self._profile is not None and self._profile.peaks:
            peak = self._profile.peaks[0]
            bin_height = span / max(self._profile.bins, 1)
            # 右对齐贴住右边界，避免超出绘图区被裁掉
            self._peak_label.setPos(
                max_share * 1.32,
                peak.price + bin_height * 0.8,
            )

    def clear_profile(self) -> None:
        """清空筹码栏图元。"""
        self._profile = None
        self._bars = None
        self._price_line = None
        self._peak_marker = None
        self._peak_label = None
        self._legend = None
        self.plot_widget.clear()


class ChipController:
    """筹码栏与主图/工具栏/全区间开关的联动。"""

    def __init__(self, window, panel: ChipPanel,
                 turnover_source: str = DEFAULT_TURNOVER_SOURCE,
                 floor_mode: str = DEFAULT_TURNOVER_FLOOR_MODE):
        self.window = window
        self.panel = panel
        self._source_df = None
        self._source_key: tuple[str, str] | None = None
        self._source_adjustment = 'qfq'
        self._turnover_source = turnover_source
        self._floor_mode = floor_mode
        self._prepared_df = None
        self._prepared_key: tuple[str, str] | None = None
        self._stats_logged_key: tuple | None = None
        self._profile: ChipProfile | None = None
        self._visible = True
        self._full_range = False
        self._applying_main_range = False
        self._history_cache: dict[tuple[str, str], object] = {}
        self._history_handler: Callable[[str, object], None] | None = None
        self._history_task_id: str | None = None
        self._toggle_btn = None
        self._full_range_box = None
        self._floor_combo = None
        self._main_price_line: pg.InfiniteLine | None = None
        # 显隐切换后的布局回调：同步副图右侧占位列
        self._layout_callback: Callable[[], None] | None = None

        self._rebuild_timer = QTimer()
        self._rebuild_timer.setSingleShot(True)
        self._rebuild_timer.timeout.connect(self._rebuild)

        self.panel.plot_widget.getViewBox().setMouseEnabled(x=False, y=False)
        self.panel.plot_widget.getViewBox().setMenuEnabled(False)
        main_view = self._main_view_box()
        if main_view is not None:
            main_view.sigYRangeChanged.connect(self._on_main_y_range_changed)

    # ---------------------------------------------------------------- 外部接口

    def bind_controls(self, toggle_btn, full_range_box, floor_combo=None) -> None:
        """绑定下方工具栏的「筹码」按钮、「全区间」与衰减下限下拉。"""
        self._toggle_btn = toggle_btn
        self._full_range_box = full_range_box
        self._floor_combo = floor_combo

        toggle_btn.blockSignals(True)
        toggle_btn.setChecked(self._visible)
        toggle_btn.blockSignals(False)
        self._update_toggle_text()
        toggle_btn.toggled.connect(self._on_toggle_toggled)

        full_range_box.blockSignals(True)
        full_range_box.setChecked(self._full_range)
        full_range_box.blockSignals(False)
        full_range_box.toggled.connect(self._on_full_range_toggled)

        if floor_combo is not None:
            floor_combo.blockSignals(True)
            index = floor_combo.findData(self._floor_mode)
            if index >= 0:
                floor_combo.setCurrentIndex(index)
            floor_combo.blockSignals(False)
            floor_combo.currentIndexChanged.connect(self._on_floor_mode_changed)

    def is_visible(self) -> bool:
        """筹码栏当前是否显示。"""
        return self._visible

    def toggle_visible(self) -> None:
        """切换显示状态（Ctrl+U 与按钮共用）。"""
        self.set_visible(not self._visible)

    def set_layout_callback(self, callback: Callable[[], None] | None) -> None:
        """注册显隐后的布局回调（副图右侧占位列据此统一切换）。"""
        self._layout_callback = callback

    def set_visible(self, visible: bool) -> None:
        """显示/隐藏筹码栏；隐藏后主图铺满，显示后恢复视口。"""
        visible = bool(visible)
        if visible == self._visible and self.panel.is_visible() == visible:
            self._sync_toggle_checked(visible)
            return

        main_view = self._main_view_box()
        saved_range = main_view.viewRange() if main_view is not None else None

        self._visible = visible
        self.panel.setVisible(visible)
        self._sync_toggle_checked(visible)
        if self._layout_callback is not None:
            self._layout_callback()

        if visible:
            self.schedule_rebuild(0)
            if saved_range is not None:
                QTimer.singleShot(0, lambda: self._restore_view(saved_range))
        else:
            self.panel.clear_profile()
            self._remove_main_price_line()

    def set_source(self, df, stock_code: str, adjustment_type: str) -> None:
        """换标的/换复权后更新筹码数据源并安排重建。"""
        self._source_df = df
        self._source_key = (stock_code, adjustment_type)
        self._source_adjustment = adjustment_type
        self._prepared_df = None
        self._prepared_key = None
        self.schedule_rebuild(FIRST_REBUILD_DELAY_MS)

    def set_turnover_source(self, source: str) -> None:
        """切换衰减口径：``turn``（A 档，默认）/ ``proxy``（B 档，对照）。"""
        if source not in TURNOVER_SOURCES:
            raise ValueError(f'未知换手率来源 {source!r}，允许 {TURNOVER_SOURCES}')
        if source == self._turnover_source:
            return
        self._turnover_source = source
        self._prepared_df = None
        self._prepared_key = None
        self._stats_logged_key = None
        self.schedule_rebuild(0)

    def turnover_source(self) -> str:
        """当前衰减口径。"""
        return self._turnover_source

    def floor_mode(self) -> str:
        """当前 A 档换手下限档位。"""
        return self._floor_mode

    def set_floor_mode(self, mode: str) -> None:
        """切换 A 档换手下限：``raw`` / ``floor_0p05`` / ``floor_0p2``（默认）/
        ``floor_2`` / ``auto_p10``。"""
        if mode not in TURNOVER_FLOOR_PRESETS:
            raise ValueError(f'未知换手下限档位 {mode!r}，允许 {tuple(TURNOVER_FLOOR_PRESETS)}')
        if mode == self._floor_mode:
            return
        self._floor_mode = mode
        self._stats_logged_key = None
        if self._floor_combo is not None:
            self._floor_combo.blockSignals(True)
            index = self._floor_combo.findData(mode)
            if index >= 0:
                self._floor_combo.setCurrentIndex(index)
            self._floor_combo.blockSignals(False)
        self.schedule_rebuild(0)

    def _on_floor_mode_changed(self, index: int) -> None:
        mode = self._floor_combo.itemData(index) if self._floor_combo is not None else None
        if mode:
            self.set_floor_mode(str(mode))

    def set_full_range(self, enabled: bool) -> None:
        """切换「全区间」：打开后覆盖历史全价格区间。"""
        enabled = bool(enabled)
        if enabled == self._full_range:
            return
        self._full_range = enabled
        if not enabled:
            self._refit_main_y_range()
        self.schedule_rebuild(0)

    def request_history(self, stock_code: str, adjustment_type: str) -> None:
        """后台拉取全历史日线，避免阻塞主图。"""
        if not stock_code:
            return
        key = (stock_code, adjustment_type)
        cached = self._history_cache.get(key)
        if cached is not None:
            self.set_source(cached, stock_code, adjustment_type)
            return

        handler = self._on_history_completed
        previous = self._history_handler
        if previous is not None:
            with contextlib.suppress(RuntimeError, TypeError):
                global_task_manager.task_completed.disconnect(previous)
        self._history_handler = handler
        global_task_manager.task_completed.connect(handler)

        end_date = date.today().isoformat()

        def _fetch(task_id=None, signals=None) -> dict:  # noqa: ARG001 - 任务管理器固定注入
            try:
                data = self.window.data_manager.get_stock_data(
                    stock_code,
                    '1990-12-19',
                    end_date,
                    frequency='1d',
                    adjustment_type=adjustment_type,
                )
                return {
                    'success': True,
                    'code': stock_code,
                    'adjustment': adjustment_type,
                    'data': data,
                }
            except (OSError, RuntimeError, ValueError) as exc:
                logger.warning(f'筹码历史数据获取失败 {stock_code}: {exc}')
                return {'success': False, 'code': stock_code, 'adjustment': adjustment_type}

        task_id = global_task_manager.create_task(f'筹码历史: {stock_code}', _fetch, ())
        self._history_task_id = task_id

    def schedule_rebuild(self, delay_ms: int = FIRST_REBUILD_DELAY_MS) -> None:
        """延后重建，先让 K 线完成绘制。"""
        if not self._visible:
            return
        if delay_ms <= 0:
            self._rebuild_timer.stop()
            self._rebuild()
            return
        self._rebuild_timer.start(delay_ms)

    def rebuild_now(self) -> None:
        """立即重建（测试/显隐恢复用）。"""
        self._rebuild_timer.stop()
        self._rebuild()

    def clear(self) -> None:
        """绘图清空时同步清空筹码栏。"""
        self._rebuild_timer.stop()
        self._profile = None
        self.panel.clear_profile()
        self._remove_main_price_line()

    # ---------------------------------------------------------------- 内部实现

    def _main_plot_widget(self) -> pg.PlotWidget | None:
        widget = getattr(self.window, 'tech_plot_widget', None)
        return widget if isinstance(widget, pg.PlotWidget) else None

    def _main_view_box(self):
        widget = self._main_plot_widget()
        return widget.getViewBox() if widget is not None else None

    def _visible_price_range(self) -> tuple[float, float] | None:
        if self._full_range:
            return None
        view_box = self._main_view_box()
        if view_box is None:
            return None
        low, high = view_box.viewRange()[1]
        if not np.isfinite(low) or not np.isfinite(high) or high <= low:
            return None
        return float(low), float(high)

    def _prepared_source_frame(self):
        """A 档计算前补齐真实换手率；按数据源键缓存，避免缩放时反复读表。"""
        df = self._source_df
        if (self._turnover_source != 'turn' or self._source_key is None
                or df is None or getattr(df, 'height', 0) == 0):
            return df
        if self._prepared_key == self._source_key and self._prepared_df is not None:
            return self._prepared_df
        prepared = prepare_chip_frame(df, self._source_key[0])
        self._prepared_df = prepared
        self._prepared_key = self._source_key
        return prepared

    def _rebuild(self) -> None:
        if not self._visible:
            return
        df = self._prepared_source_frame()
        if df is None or getattr(df, 'height', 0) == 0:
            self.panel.clear_profile()
            return

        started = time.perf_counter()
        try:
            profile = compute_chip_profile(
                df,
                price_range=self._visible_price_range(),
                bins=DEFAULT_BINS,
                turnover_source=self._turnover_source,
                turnover_floor=TURNOVER_FLOOR_PRESETS[self._floor_mode],
                adjustment_type=self._source_adjustment,
            )
        except (ValueError, KeyError, TypeError, IndexError) as exc:
            logger.warning(f'筹码计算失败，跳过本次渲染: {exc}')
            self.panel.clear_profile()
            self._remove_main_price_line()
            return

        self._profile = profile
        if self._full_range:
            self._apply_full_price_range(profile)

        self.panel.render_profile(profile)
        self._sync_main_price_line(profile.last_close)
        elapsed_ms = (time.perf_counter() - started) * 1000
        logger.debug(
            f'筹码重建完成: {elapsed_ms:.1f}ms, 区间={profile.price_range}, '
            f'主峰={profile.peaks[0].price if profile.peaks else None}, '
            f'源={profile.turnover_source}, 真实换手占比={profile.turn_source_ratio:.1%}, '
            f'下限={profile.turnover_floor}, 钳制占比={profile.clamped_ratio:.1%}'
        )
        stats_key = (self._source_key, self._turnover_source, self._floor_mode)
        if self._turnover_source == 'turn' and stats_key != self._stats_logged_key:
            self._stats_logged_key = stats_key
            floor_label = TURNOVER_FLOOR_LABELS[self._floor_mode]
            logger.info(
                f'筹码 A 档口径: {self._source_key}, {floor_label}, '
                f'{profile.turn_source_ratio:.2%} 行使用真实 turn，'
                f'下限抬起 {profile.clamped_low_ratio:.2%}，'
                f'上限压住 {profile.clamped_high_ratio:.2%}'
            )
            if profile.clamped_ratio > CLAMPED_WARN_THRESHOLD:
                logger.warning(
                    f'筹码 A 档钳制占比 {profile.clamped_ratio:.1%} 超过 '
                    f'{CLAMPED_WARN_THRESHOLD:.0%}（{self._source_key}, {floor_label}），'
                    f'该票筹码形态仍依赖下限/上限假设'
                )

    def _apply_full_price_range(self, profile: ChipProfile) -> None:
        view_box = self._main_view_box()
        if view_box is None:
            return
        low, high = profile.full_range
        pad = (high - low) * FULL_RANGE_PADDING
        self._applying_main_range = True
        try:
            view_box.setYRange(low - pad, high + pad, padding=0)
        finally:
            self._applying_main_range = False

    def _sync_main_price_line(self, close: float) -> None:
        """主图也画一条现价虚线，与右栏同价位横向贯通。"""
        widget = self._main_plot_widget()
        if widget is None or not self._visible:
            return
        if self._main_price_line is None:
            line = pg.InfiniteLine(
                pos=close,
                angle=0,
                pen=pg.mkPen(PRICE_LINE_COLOR, width=1, style=Qt.PenStyle.DashLine),
                movable=False,
            )
            line.setZValue(10)
            widget.addItem(line)
            self._main_price_line = line
        else:
            self._main_price_line.setPos(close)

    def _remove_main_price_line(self) -> None:
        line = self._main_price_line
        if line is None:
            return
        widget = self._main_plot_widget()
        if widget is not None:
            with contextlib.suppress(RuntimeError, KeyError):
                widget.removeItem(line)
        self._main_price_line = None

    def _refit_main_y_range(self) -> None:
        """关闭全区间时回到当前显示 K 线的价格范围。"""
        view_box = self._main_view_box()
        if view_box is None:
            return
        kline = getattr(self.window, 'current_kline_data', None) or {}
        highs = kline.get('highs')
        lows = kline.get('lows')
        if highs is None or lows is None or len(highs) == 0 or len(lows) == 0:
            return

        y_min = float(np.min(lows))
        y_max = float(np.max(highs))
        ma_data = getattr(self.window, 'ma_data', None) or {}
        for values in ma_data.values():
            if len(values) == 0:
                continue
            valid = np.asarray(values, dtype=float)
            valid = valid[~np.isnan(valid)]
            if valid.size:
                y_min = min(y_min, float(valid.min()))
                y_max = max(y_max, float(valid.max()))

        span = y_max - y_min
        if span <= 0:
            span = max(abs(y_max) * 0.01, 0.01)
        self._applying_main_range = True
        try:
            view_box.setYRange(y_min - span * 0.05, y_max + span * 0.05, padding=0)
        finally:
            self._applying_main_range = False

    def _on_main_y_range_changed(self, _view_box, y_range) -> None:
        if self._applying_main_range:
            return
        self.panel.sync_y_range(y_range)
        if not self._visible or self._full_range or self._profile is None:
            return

        low, high = float(y_range[0]), float(y_range[1])
        prof_low, prof_high = self._profile.price_range
        span = max(prof_high - prof_low, 1e-9)
        # 只在视口明显越出已分箱区间时重算，避免普通缩放反复计算
        if low < prof_low - span * 0.25 or high > prof_high + span * 0.25:
            self._rebuild_timer.start(REBIN_DEBOUNCE_MS)
        else:
            self.panel._position_texts()

    def _on_toggle_toggled(self, checked: bool) -> None:
        self.set_visible(checked)

    def _on_full_range_toggled(self, checked: bool) -> None:
        self.set_full_range(checked)

    def _sync_toggle_checked(self, visible: bool) -> None:
        if self._toggle_btn is None:
            return
        self._toggle_btn.blockSignals(True)
        self._toggle_btn.setChecked(visible)
        self._toggle_btn.blockSignals(False)
        self._update_toggle_text()

    def _update_toggle_text(self) -> None:
        if self._toggle_btn is None:
            return
        if self._visible:
            self._toggle_btn.setText('📊 筹码 显示中')
            self._toggle_btn.setToolTip('隐藏筹码分布（Ctrl+U）')
        else:
            self._toggle_btn.setText('📊 筹码 已隐藏')
            self._toggle_btn.setToolTip('显示筹码分布（Ctrl+U）')

    def _restore_view(self, saved_range) -> None:
        view_box = self._main_view_box()
        if view_box is None:
            return
        x_range, y_range = saved_range
        self._applying_main_range = True
        try:
            view_box.setXRange(x_range[0], x_range[1], padding=0)
            if not self._full_range:
                view_box.setYRange(y_range[0], y_range[1], padding=0)
        finally:
            self._applying_main_range = False

    def _on_history_completed(self, task_id: str, result) -> None:
        if self._history_task_id is None or task_id != self._history_task_id:
            return  # 不是本次筹码请求，忽略（任务管理器是全局信号）
        self._history_task_id = None
        handler = self._history_handler
        if handler is not None:
            with contextlib.suppress(RuntimeError, TypeError):
                global_task_manager.task_completed.disconnect(handler)
            self._history_handler = None
        if not isinstance(result, dict) or not result.get('success'):
            return

        code = result.get('code', '')
        adjustment = result.get('adjustment', '')
        if self._source_key is not None and (code, adjustment) != self._source_key:
            return  # 已经切到别的标的，丢弃过期结果
        data = result.get('data')
        if data is None or getattr(data, 'height', 0) == 0:
            return

        key = (code, adjustment)
        self._history_cache[key] = data
        if len(self._history_cache) > HISTORY_CACHE_LIMIT:
            oldest = next(iter(self._history_cache))
            self._history_cache.pop(oldest, None)

        self.set_source(data, code, adjustment)
        logger.debug(f'筹码历史数据就绪: {code} {data.height} 行')
