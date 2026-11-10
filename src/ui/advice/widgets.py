#!/usr/bin/env python3

"""每日推荐页的卡片、分段控件、表格委托与右侧详情面板。"""

from __future__ import annotations

from typing import Any

from PySide6.QtCore import (
    QEasingCurve,
    QRectF,
    QSize,
    Qt,
    QTimer,
    QVariantAnimation,
    Signal,
)
from PySide6.QtGui import QColor, QFont, QFontMetrics, QLinearGradient, QPainter, QPen
from PySide6.QtWidgets import (
    QApplication,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSizePolicy,
    QStyle,
    QStyledItemDelegate,
    QStyleOptionViewItem,
    QVBoxLayout,
    QWidget,
)

from src.core.usecases.get_daily_recommendations import DailyRecommendationDTO
from src.ui.advice.table_model import (
    COL_CODE,
    COL_CONFIDENCE,
    COL_DIRECTION,
    COL_FACTORS,
    COL_NAME,
    COL_PRICE,
    COL_RISK,
    COL_SHARES,
    COL_WEIGHT,
    DTO_ROLE,
    display_value,
)
from src.ui.advice.tokens import TOKENS, DesignTokens, Palette

DIRECTION_COLORS = {
    'BUY': ('buy', 'buy_soft'),
    'SELL': ('sell', 'sell_soft'),
    'HOLD': ('hold', 'hold_soft'),
    'AVOID': ('avoid', 'avoid_soft'),
}

_NAME_SOURCE_LABELS = {'local_names_json': '本地名称表', 'code': '代码回退'}
_INDUSTRY_SOURCE_LABELS = {'code_prefix_board': '板块近似', 'table': '行业表'}


def _qcolor(value: str, alpha: int = 255) -> QColor:
    color = QColor(value)
    color.setAlpha(alpha)
    return color


def _direction_colors(palette: Palette, direction: str) -> tuple[QColor, QColor]:
    fg_key, bg_key = DIRECTION_COLORS.get(direction, ('avoid', 'avoid_soft'))
    return _qcolor(getattr(palette, fg_key)), _qcolor(getattr(palette, bg_key))


class AdviceItemDelegate(QStyledItemDelegate):
    """表格自绘：方向 pill、置信度细条、因子 mini bar、风险摘要。"""

    def __init__(self, palette: Palette, tokens: DesignTokens = TOKENS,
                 parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._pal = palette
        self.tokens = tokens

    def set_palette(self, palette: Palette) -> None:
        self._pal = palette

    def sizeHint(self, option: QStyleOptionViewItem, index) -> QSize:  # noqa: N802
        return QSize(option.rect.width(), self.tokens.row_height)

    def paint(self, painter: QPainter, option: QStyleOptionViewItem,
              index) -> None:
        dto: DailyRecommendationDTO | None = index.data(DTO_ROLE)
        if dto is None:
            super().paint(painter, option, index)
            return
        opt = QStyleOptionViewItem(option)
        self.initStyleOption(opt, index)
        opt.text = ''
        widget = opt.widget
        style = widget.style() if widget is not None else QApplication.style()
        style.drawControl(QStyle.ControlElement.CE_ItemViewItem, opt, painter, widget)

        painter.save()
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        rect = QRectF(opt.rect).adjusted(9, 3, -9, -3)
        column = index.column()
        if option.state & QStyle.StateFlag.State_Selected:
            # QSS 的 ::item:selected 在不同主题/焦点下表现不一致，这里显式铺底
            painter.fillRect(opt.rect, _qcolor(self._pal.selection))
        if column == COL_CODE and option.state & QStyle.StateFlag.State_Selected:
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(_qcolor(self._pal.accent))
            painter.drawRect(QRectF(opt.rect.left(), opt.rect.top(), 3,
                                    opt.rect.height()))
        if column == COL_CODE:
            self._draw_text(painter, rect, dto.ts_code, self.tokens.mono_family,
                            9, self._pal.muted, Qt.AlignmentFlag.AlignLeft)
        elif column == COL_NAME:
            self._draw_text(painter, rect, dto.name, self.tokens.font_family,
                            10, self._pal.text, Qt.AlignmentFlag.AlignLeft, bold=True)
        elif column == COL_DIRECTION:
            self._draw_pill(painter, rect, dto)
        elif column in (COL_WEIGHT, COL_SHARES, COL_PRICE):
            text = (f'{dto.target_weight_pct:.1f}%' if column == COL_WEIGHT
                    else f'{dto.shares:,}' if column == COL_SHARES
                    else f'{dto.ref_price:.2f}')
            self._draw_text(painter, rect, text, self.tokens.mono_family,
                            9.5, self._pal.text, Qt.AlignmentFlag.AlignRight)
        elif column == COL_CONFIDENCE:
            self._draw_confidence(painter, rect, dto)
        elif column == COL_FACTORS:
            self._draw_factors(painter, rect, dto)
        elif column == COL_RISK:
            self._draw_risk(painter, rect, dto)
        painter.restore()

    def _draw_text(self, painter: QPainter, rect: QRectF, text: str,
                   family: str, size: float, color: str, align: int,
                   bold: bool = False) -> None:
        font = QFont(family)
        font.setPointSizeF(size)
        font.setBold(bold)
        painter.setFont(font)
        painter.setPen(QPen(_qcolor(color)))
        metrics = QFontMetrics(font)
        elided = metrics.elidedText(text, Qt.TextElideMode.ElideRight, int(rect.width()))
        painter.drawText(rect, int(align | Qt.AlignmentFlag.AlignVCenter), elided)

    def _draw_pill(self, painter: QPainter, rect: QRectF,
                   dto: DailyRecommendationDTO) -> None:
        # 文本与 display_value 同源：延后/过期带后缀，颜色区分不可下单
        text = display_value(dto, COL_DIRECTION)
        blocked = dto.disposition == 'deferred' or dto.exec_status == 'expired'
        fg, bg = _direction_colors(
            self._pal, 'AVOID' if blocked else dto.direction)
        font = QFont(self.tokens.font_family)
        font.setPointSizeF(8.5)
        font.setBold(True)
        painter.setFont(font)
        metrics = QFontMetrics(font)
        width = metrics.horizontalAdvance(text) + 18
        height = 24
        pill = QRectF(rect.center().x() - width / 2,
                      rect.center().y() - height / 2, width, height)
        painter.setPen(QPen(fg, 1.0))
        painter.setBrush(bg)
        painter.drawRoundedRect(pill, height / 2, height / 2)
        painter.setPen(fg)
        painter.drawText(pill, int(Qt.AlignmentFlag.AlignCenter), text)

    def _draw_confidence(self, painter: QPainter, rect: QRectF,
                         dto: DailyRecommendationDTO) -> None:
        font = QFont(self.tokens.mono_family)
        font.setPointSizeF(9)
        painter.setFont(font)
        painter.setPen(QPen(_qcolor(self._pal.muted)))
        painter.drawText(QRectF(rect.left(), rect.top(), 38, rect.height()),
                         int(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter),
                         f'{dto.confidence:.2f}')
        track = QRectF(rect.left() + 42, rect.center().y() - 2.5,
                       rect.width() - 42, 5)
        painter.setPen(QPen(_qcolor(self._pal.border), 1))
        painter.setBrush(_qcolor(self._pal.surface))
        painter.drawRoundedRect(track, 2.5, 2.5)
        value = max(0.0, min(1.0, float(dto.confidence)))
        if value > 0:
            fill = QRectF(track.left(), track.top(),
                          track.width() * value, track.height())
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(_qcolor(self._pal.accent))
            painter.drawRoundedRect(fill, 2.5, 2.5)

    def _draw_factors(self, painter: QPainter, rect: QRectF,
                      dto: DailyRecommendationDTO) -> None:
        font = QFont(self.tokens.font_family)
        font.setPointSizeF(8.5)
        painter.setFont(font)
        painter.setPen(QPen(_qcolor(self._pal.muted)))
        text_width = rect.width() - 52
        metrics = QFontMetrics(font)
        text = metrics.elidedText(dto.top_factors or '—', Qt.TextElideMode.ElideRight,
                                  max(10, int(text_width)))
        painter.drawText(QRectF(rect.left(), rect.top(), text_width,
                                rect.height()),
                         int(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter), text)

        values = [float(item.get('value', 0.0))
                  for item in dto.factor_contributions[:3]]
        if not values:
            return
        peak = max([abs(value) for value in values] + [1e-6])
        bar_width = 6
        gap = 4
        base_x = rect.right() - (bar_width * 3 + gap * 2)
        baseline = rect.bottom() - 6
        max_height = rect.height() - 16
        for index, value in enumerate(values):
            height = max(3.0, max_height * abs(value) / peak)
            color = self._pal.buy if value >= 0 else self._pal.sell
            bar = QRectF(base_x + index * (bar_width + gap),
                         baseline - height, bar_width, height)
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(_qcolor(color, 210))
            painter.drawRoundedRect(bar, 2, 2)

    def _draw_risk(self, painter: QPainter, rect: QRectF,
                   dto: DailyRecommendationDTO) -> None:
        first = (dto.risk_notes or '').split('；')[0]
        chip_width = 42
        font = QFont(self.tokens.font_family)
        font.setPointSizeF(8.5)
        painter.setFont(font)
        painter.setPen(QPen(_qcolor(self._pal.muted)))
        metrics = QFontMetrics(font)
        text = metrics.elidedText(first, Qt.TextElideMode.ElideRight,
                                  max(10, int(rect.width() - chip_width - 10)))
        painter.drawText(QRectF(rect.left(), rect.top(),
                                rect.width() - chip_width - 8, rect.height()),
                         int(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter), text)

        chip = QRectF(rect.right() - chip_width, rect.center().y() - 10,
                      chip_width, 20)
        painter.setPen(QPen(_qcolor(self._pal.border), 1))
        painter.setBrush(_qcolor(self._pal.elevated))
        painter.drawRoundedRect(chip, 10, 10)
        painter.setPen(QPen(_qcolor(self._pal.faint)))
        painter.drawText(chip, int(Qt.AlignmentFlag.AlignCenter), f'{dto.validity_days}日')


class KpiCard(QFrame):
    """顶部 KPI 卡片：大数字 + 小标签。"""

    def __init__(self, caption: str, palette: Palette,
                 accent: str | None = None, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName('kpiCard')
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 13, 16, 13)
        layout.setSpacing(2)
        self.value_label = QLabel('—')
        self.value_label.setObjectName('kpiValue')
        self.caption_label = QLabel(caption)
        self.caption_label.setObjectName('kpiLabel')
        if accent:
            self.value_label.setStyleSheet(f'color: {accent};')
        layout.addWidget(self.value_label)
        layout.addWidget(self.caption_label)

    def set_value(self, value: str) -> None:
        self.value_label.setText(str(value))

    def set_caption(self, caption: str) -> None:
        self.caption_label.setText(str(caption))

    def set_accent(self, color: str | None) -> None:
        self.value_label.setStyleSheet(f'color: {color};' if color else '')


class SegmentedControl(QWidget):
    """档位 segmented control（仅中线/长线；短线已降级为只读观察）。"""

    changed = Signal(str)
    ITEMS = (
        ('mid', '中线', '1–3 月'),
        ('long', '长线', '6–12 月'),
    )

    def __init__(self, palette: Palette, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._pal = palette
        self._index = 0
        self._indicator = 0.0
        self.setFixedHeight(46)
        self.setMinimumWidth(330)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self._animation = QVariantAnimation(self)
        self._animation.setDuration(180)
        self._animation.setEasingCurve(QEasingCurve.Type.OutCubic)
        self._animation.valueChanged.connect(self._on_anim)

    def set_palette(self, palette: Palette) -> None:
        self._pal = palette
        self.update()

    def current_key(self) -> str:
        return self.ITEMS[self._index][0]

    def set_current(self, key: str, animate: bool = True) -> None:
        for index, item in enumerate(self.ITEMS):
            if item[0] == key:
                if index == self._index:
                    return
                self._index = index
                if animate:
                    self._animation.stop()
                    self._animation.setStartValue(self._indicator)
                    self._animation.setEndValue(float(index))
                    self._animation.start()
                else:
                    self._indicator = float(index)
                    self.update()
                return

    def _on_anim(self, value: Any) -> None:
        self._indicator = float(value)
        self.update()

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        rect = QRectF(self.rect()).adjusted(0, 2, 0, -2)
        painter.setPen(QPen(_qcolor(self._pal.border), 1))
        painter.setBrush(_qcolor(self._pal.surface))
        painter.drawRoundedRect(rect, 10, 10)

        segment = rect.width() / len(self.ITEMS)
        active = QRectF(rect.left() + 4 + self._indicator * segment,
                        rect.top() + 4, segment - 8, rect.height() - 8)
        painter.setPen(QPen(_qcolor(self._pal.accent), 1))
        painter.setBrush(_qcolor(self._pal.accent_soft))
        painter.drawRoundedRect(active, 8, 8)

        for index, (_, label, sub) in enumerate(self.ITEMS):
            cell = QRectF(rect.left() + index * segment, rect.top(),
                          segment, rect.height())
            is_active = index == self._index
            font = QFont(TOKENS.font_family)
            font.setPointSizeF(9.5)
            font.setBold(is_active)
            painter.setFont(font)
            painter.setPen(QPen(_qcolor(
                self._pal.accent if is_active else self._pal.muted)))
            painter.drawText(QRectF(cell.left(), cell.top() + 4, cell.width(), 22),
                             int(Qt.AlignmentFlag.AlignCenter), label)
            font.setPointSizeF(7.8)
            font.setBold(False)
            painter.setFont(font)
            painter.setPen(QPen(_qcolor(
                self._pal.accent if is_active else self._pal.faint)))
            painter.drawText(QRectF(cell.left(), cell.top() + 23, cell.width(), 18),
                             int(Qt.AlignmentFlag.AlignCenter), sub)

    def mousePressEvent(self, event) -> None:  # noqa: N802
        rect = QRectF(self.rect())
        segment = rect.width() / len(self.ITEMS)
        index = int((event.position().x() - rect.left()) / segment)
        index = max(0, min(len(self.ITEMS) - 1, index))
        if index != self._index:
            self.set_current(self.ITEMS[index][0])
            self.changed.emit(self.ITEMS[index][0])


class FactorBarsWidget(QWidget):
    """详情面板因子贡献横条图（正贡献向右、负贡献向左）。"""

    def __init__(self, palette: Palette, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._pal = palette
        self._items: list[dict[str, Any]] = []
        self.setMinimumHeight(116)

    def set_palette(self, palette: Palette) -> None:
        self._pal = palette
        self.update()

    def set_contributions(self, items: list[dict[str, Any]]) -> None:
        self._items = list(items or [])[:3]
        self.update()

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        if not self._items:
            painter.setPen(QPen(_qcolor(self._pal.faint)))
            painter.drawText(self.rect(), int(Qt.AlignmentFlag.AlignCenter), '暂无因子归因')
            return
        peak = max([abs(float(item.get('value', 0.0))) for item in self._items]
                   + [1e-6])
        label_width = 84
        value_width = 40
        bar_left = self.rect().left() + label_width
        bar_width = self.width() - label_width - value_width - 8
        zero = bar_left + bar_width / 2

        for index, item in enumerate(self._items):
            value = float(item.get('value', 0.0))
            label = str(item.get('label', ''))
            y = self.rect().top() + index * 34
            font = QFont(TOKENS.font_family)
            font.setPointSizeF(8.5)
            painter.setFont(font)
            painter.setPen(QPen(_qcolor(self._pal.muted)))
            metrics = QFontMetrics(font)
            painter.drawText(
                QRectF(self.rect().left(), y, label_width - 6, 26),
                int(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter),
                metrics.elidedText(label, Qt.TextElideMode.ElideRight, label_width - 6))

            painter.setPen(QPen(_qcolor(self._pal.border), 1))
            painter.drawLine(int(bar_left), y + 13, int(bar_left + bar_width),
                             y + 13)
            length = max(2.0, (bar_width / 2) * abs(value) / peak)
            color = self._pal.buy if value >= 0 else self._pal.sell
            bar = QRectF(zero if value >= 0 else zero - length, y + 7,
                         length, 12)
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(_qcolor(color, 220))
            painter.drawRoundedRect(bar, 3, 3)

            mono = QFont(TOKENS.mono_family)
            mono.setPointSizeF(8.5)
            painter.setFont(mono)
            painter.setPen(QPen(_qcolor(color)))
            painter.drawText(
                QRectF(self.rect().right() - value_width, y, value_width, 26),
                int(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter), f'{value:+.2f}')


class WeightBarsWidget(QWidget):
    """建议仓位构成：目标仓位 vs 现金缓冲。"""

    def __init__(self, palette: Palette, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._pal = palette
        self._weight = 0.0
        self.setMinimumHeight(58)

    def set_palette(self, palette: Palette) -> None:
        self._pal = palette
        self.update()

    def set_weight(self, weight_pct: float) -> None:
        self._weight = max(0.0, min(100.0, float(weight_pct)))
        self.update()

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        track = QRectF(self.rect().left(), self.rect().top() + 6,
                       self.width(), 14)
        painter.setPen(QPen(_qcolor(self._pal.border), 1))
        painter.setBrush(_qcolor(self._pal.elevated))
        painter.drawRoundedRect(track, 7, 7)
        if self._weight > 0:
            filled = QRectF(track.left(), track.top(),
                            track.width() * self._weight / 100.0, track.height())
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(_qcolor(self._pal.accent))
            painter.drawRoundedRect(filled, 7, 7)

        font = QFont(TOKENS.mono_family)
        font.setPointSizeF(8.8)
        painter.setFont(font)
        painter.setPen(QPen(_qcolor(self._pal.muted)))
        painter.drawText(
            QRectF(self.rect().left(), self.rect().top() + 26, self.width(), 24),
            int(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter),
            f'目标仓位 {self._weight:.1f}%   现金 {100.0 - self._weight:.1f}%')


class SkeletonWidget(QWidget):
    """首屏骨架占位（带流动微光）。"""

    def __init__(self, palette: Palette, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName('skeletonState')
        self._pal = palette
        self._phase = 0
        self._timer = QTimer(self)
        self._timer.setInterval(45)
        self._timer.timeout.connect(self._tick)

    def set_palette(self, palette: Palette) -> None:
        self._pal = palette
        self.update()

    def start(self) -> None:
        self._timer.start()

    def stop(self) -> None:
        self._timer.stop()

    def showEvent(self, event) -> None:  # noqa: N802
        self.start()
        super().showEvent(event)

    def hideEvent(self, event) -> None:  # noqa: N802
        self.stop()
        super().hideEvent(event)

    def _tick(self) -> None:
        self._phase = (self._phase + 18) % 1200
        self.update()

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        card = self.rect().adjusted(12, 12, -12, -12)
        bar_height = 16
        gap = 14
        widths = (0.24, 0.18, 0.12, 0.1, 0.1, 0.08, 0.18)
        for row in range(9):
            y = card.top() + 8 + row * (bar_height + gap)
            if y + bar_height > card.bottom():
                break
            x = card.left() + 12
            for ratio in widths:
                width = max(24, int(card.width() * ratio) - 14)
                rect = QRectF(x, y, width, bar_height)
                gradient = QLinearGradient(rect.left(), rect.top(),
                                           rect.right(), rect.top())
                base = _qcolor(self._pal.elevated)
                glow = _qcolor(self._pal.border)
                position = ((self._phase / 1200.0) * 2.0) - 0.4
                gradient.setColorAt(max(0.0, min(1.0, position)),
                                    _qcolor(self._pal.surface))
                gradient.setColorAt(max(0.0, min(1.0, position + 0.18)), glow)
                gradient.setColorAt(1.0, base)
                painter.setPen(Qt.PenStyle.NoPen)
                painter.setBrush(gradient)
                painter.drawRoundedRect(rect, 5, 5)
                x += width + 14
        painter.setPen(QPen(_qcolor(self._pal.faint)))
        font = QFont(TOKENS.font_family)
        font.setPointSizeF(9)
        painter.setFont(font)
        painter.drawText(self.rect().adjusted(16, 0, -16, -14),
                         int(Qt.AlignmentFlag.AlignBottom | Qt.AlignmentFlag.AlignRight),
                         '正在加载推荐数据…')


class EmptyIllustration(QWidget):
    """空状态图形：K 线柱 + 放大镜。"""

    def __init__(self, palette: Palette, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._pal = palette
        self.setFixedSize(120, 78)

    def set_palette(self, palette: Palette) -> None:
        self._pal = palette
        self.update()

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        base_y = 62
        bars = ((10, 34, '#4C8DFF'), (28, 22, '#23C26B'), (46, 40, '#E3B341'),
                (64, 18, '#5B9DFF'), (82, 30, '#F2555F'))
        for x, height, color in bars:
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(_qcolor(color, 190))
            painter.drawRoundedRect(QRectF(x, base_y - height, 10, height), 3, 3)
            painter.setPen(QPen(_qcolor(self._pal.faint), 1))
            painter.drawLine(int(x + 5), base_y - height - 6, int(x + 5), base_y - 2)
        painter.setPen(QPen(_qcolor(self._pal.border), 2))
        painter.setBrush(_qcolor(self._pal.surface, 200))
        painter.drawEllipse(QRectF(86, 8, 26, 26))
        painter.setPen(QPen(_qcolor(self._pal.muted), 3))
        painter.drawLine(108, 30, 118, 42)


class EmptyState(QWidget):
    """空状态：图形 + 明确提示 + 生成入口。"""

    generate_requested = Signal()

    def __init__(self, palette: Palette, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName('emptyState')
        self._pal = palette
        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(10)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.illustration = EmptyIllustration(palette)
        self.title_label = QLabel('暂无推荐结果')
        self.title_label.setObjectName('emptyTitle')
        self.title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.subtitle_label = QLabel('本地列存还没有可用数据，或今日清单尚未生成。')
        self.subtitle_label.setObjectName('emptySubtitle')
        self.subtitle_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.subtitle_label.setWordWrap(True)
        self.subtitle_label.setMinimumWidth(380)
        self.subtitle_label.setMaximumWidth(460)
        self.action_button = QPushButton('立即生成')
        self.action_button.setObjectName('emptyAction')
        self.action_button.setCursor(Qt.CursorShape.PointingHandCursor)
        self.action_button.clicked.connect(self.generate_requested)
        layout.addWidget(self.illustration, 0, Qt.AlignmentFlag.AlignHCenter)
        layout.addWidget(self.title_label)
        layout.addWidget(self.subtitle_label)
        layout.addSpacing(4)
        layout.addWidget(self.action_button, 0, Qt.AlignmentFlag.AlignHCenter)

    def set_palette(self, palette: Palette) -> None:
        self._pal = palette
        self.illustration.set_palette(palette)

    def set_message(self, title: str, subtitle: str,
                    show_action: bool = True) -> None:
        self.title_label.setText(title)
        self.subtitle_label.setText(subtitle)
        self.action_button.setVisible(show_action)


class DetailPanel(QFrame):
    """右侧详情：因子横条、仓位构成、风险与持有期。"""

    def __init__(self, palette: Palette, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName('detailCard')
        self._pal = palette
        self.setMinimumWidth(330)
        self.setMaximumWidth(410)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 16, 18, 16)
        layout.setSpacing(12)

        header = QHBoxLayout()
        name_box = QVBoxLayout()
        name_box.setSpacing(2)
        self.name_label = QLabel('选择一行查看详情')
        self.name_label.setObjectName('detailName')
        self.code_label = QLabel('—')
        self.code_label.setObjectName('detailCode')
        name_box.addWidget(self.name_label)
        name_box.addWidget(self.code_label)
        self.direction_label = QLabel('—')
        self.direction_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.direction_label.setFixedSize(58, 24)
        header.addLayout(name_box, 1)
        header.addWidget(self.direction_label, 0, Qt.AlignmentFlag.AlignTop)
        layout.addLayout(header)

        self.confidence_label = QLabel('置信度 —')
        self.confidence_label.setObjectName('mutedLabel')
        layout.addWidget(self.confidence_label)

        #: 执行状态：可下单 / 延后未成交 / 已过期，与表格 pill 同源判定
        self.state_label = QLabel('')
        self.state_label.setObjectName('mutedLabel')
        self.state_label.setWordWrap(True)
        layout.addWidget(self.state_label)

        self.metrics_grid = QGridLayout()
        self.metrics_grid.setHorizontalSpacing(12)
        self.metrics_grid.setVerticalSpacing(10)
        self._metrics: dict[str, QLabel] = {}
        for index, (key, caption) in enumerate((
                ('weight', '目标仓位'), ('shares', '参考股数'),
                ('price', '参考价'), ('validity', '有效期'))):
            value = QLabel('—')
            value.setObjectName('metricValue')
            label = QLabel(caption)
            label.setObjectName('metricLabel')
            box = QVBoxLayout()
            box.setSpacing(1)
            box.addWidget(value)
            box.addWidget(label)
            wrapper = QWidget()
            wrapper.setLayout(box)
            self.metrics_grid.addWidget(wrapper, index // 2, index % 2)
            self._metrics[key] = value
        layout.addLayout(self.metrics_grid)

        factor_title = QLabel('因子贡献 TOP3')
        factor_title.setObjectName('sectionTitle')
        layout.addWidget(factor_title)
        self.factor_bars = FactorBarsWidget(palette)
        layout.addWidget(self.factor_bars)

        weight_title = QLabel('建议仓位构成')
        weight_title.setObjectName('sectionTitle')
        layout.addWidget(weight_title)
        self.weight_bars = WeightBarsWidget(palette)
        layout.addWidget(self.weight_bars)

        risk_title = QLabel('风险提示 / 持有期')
        risk_title.setObjectName('sectionTitle')
        layout.addWidget(risk_title)
        self.risk_label = QLabel('—')
        self.risk_label.setObjectName('riskNote')
        self.risk_label.setWordWrap(True)
        layout.addWidget(self.risk_label)

        self.footer_label = QLabel('')
        self.footer_label.setObjectName('mutedLabel')
        self.footer_label.setWordWrap(True)
        layout.addWidget(self.footer_label)
        layout.addStretch(1)
        self.set_dto(None)

    def set_palette(self, palette: Palette) -> None:
        self._pal = palette
        self.factor_bars.set_palette(palette)
        self.weight_bars.set_palette(palette)
        self._style_direction(self.direction_label.text())

    def _style_direction(self, direction: str) -> None:
        fg, bg = _direction_colors(self._pal, direction)
        self.direction_label.setStyleSheet(
            f'color: {fg.name()}; background: {bg.name()};'
            f'border: 1px solid {fg.name()}; border-radius: 11px;'
            'font-size: 11px; font-weight: 700;')

    @staticmethod
    def _state_text(dto: DailyRecommendationDTO) -> str:
        """执行状态文案：延后/过期必须显式写「不可下单」。"""
        if dto.disposition == 'deferred':
            text = f'不可下单 · 延后至 {dto.defer_until or "下一交易日"}'
            return f'{text}（{dto.defer_reason}）' if dto.defer_reason else text
        if dto.exec_status == 'expired':
            return '不可下单 · 信号已过期（T+1 开盘已过）'
        if dto.actionable:
            return '可下单 · T+1 开盘执行'
        return '不可下单 · 非买卖方向'

    def set_dto(self, dto: DailyRecommendationDTO | None) -> None:
        if dto is None:
            self.name_label.setText('选择一行查看详情')
            self.code_label.setText('—')
            self.direction_label.setText('—')
            self.direction_label.setStyleSheet(
                f'color: {self._pal.faint}; border: 1px solid '
                f'{self._pal.border}; border-radius: 11px; font-size: 11px;')
            self.confidence_label.setText('置信度 —')
            self.state_label.setText('')
            for value in self._metrics.values():
                value.setText('—')
            self.factor_bars.set_contributions([])
            self.weight_bars.set_weight(0.0)
            self.risk_label.setText('—')
            self.footer_label.setText('')
            return

        self.name_label.setText(dto.name)
        self.code_label.setText(f'{dto.ts_code} · {dto.industry}')
        self.direction_label.setText(dto.direction)
        self._style_direction(dto.direction)
        self.confidence_label.setText(
            f'置信度 {dto.confidence:.2f} · 综合分 {dto.score:+.2f} · '
            f'截面分位 {dto.score_percentile:.0%}')
        self.state_label.setText(self._state_text(dto))
        self._metrics['weight'].setText(f'{dto.target_weight_pct:.1f}%')
        self._metrics['shares'].setText(f'{dto.shares:,}')
        self._metrics['price'].setText(f'{dto.ref_price:.2f}')
        self._metrics['validity'].setText(f'{dto.validity_days} 日')
        self.factor_bars.set_contributions(dto.factor_contributions)
        self.weight_bars.set_weight(dto.target_weight_pct)
        notes = [note for note in (dto.risk_notes or '').split('；') if note]
        self.risk_label.setText('\n'.join(f'• {note}' for note in notes) or '暂无')
        self.footer_label.setText(
            f'持有期档位：{dto.horizon} · '
            f'行业口径：{_INDUSTRY_SOURCE_LABELS.get(dto.industry_source, dto.industry_source)} · '
            f'名称来源：{_NAME_SOURCE_LABELS.get(dto.name_source, dto.name_source)}')
