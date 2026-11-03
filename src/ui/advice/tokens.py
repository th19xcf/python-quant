#!/usr/bin/env python3

"""每日推荐页设计令牌与局部 QSS。

所有颜色/字号/间距集中在此，QSS 选择器统一以 ``#advicePage`` 为根，
只作用于本页面，不污染其他页面样式。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Palette:
    """一套主题色板。"""

    bg: str
    surface: str
    card: str
    elevated: str
    border: str
    text: str
    muted: str
    faint: str
    accent: str
    accent_soft: str
    buy: str
    buy_soft: str
    sell: str
    sell_soft: str
    hold: str
    hold_soft: str
    avoid: str
    avoid_soft: str
    zebra: str
    hover: str
    selection: str
    shadow: str


DARK = Palette(
    bg='#12151A', surface='#171B21', card='#1C2128', elevated='#232A33',
    border='#2A323D', text='#E6EDF5', muted='#8B98A8', faint='#5D6978',
    accent='#5B9DFF', accent_soft='#1D2C45',
    buy='#F2555F', buy_soft='#3A1D23',
    sell='#23C26B', sell_soft='#16301F',
    hold='#E3B341', hold_soft='#332A14',
    avoid='#8B98A8', avoid_soft='#242A33',
    zebra='#1A1F26', hover='#232B36', selection='#24344D',
    shadow='#0B0D10',
)

LIGHT = Palette(
    bg='#F3F5F8', surface='#FFFFFF', card='#FFFFFF', elevated='#FAFBFD',
    border='#E2E7EE', text='#1B2430', muted='#5A6675', faint='#8B96A5',
    accent='#2563EB', accent_soft='#E8F0FF',
    buy='#D92D3F', buy_soft='#FDECEE',
    sell='#0E8A45', sell_soft='#E7F7EE',
    hold='#B7791F', hold_soft='#FDF3DE',
    avoid='#6B7280', avoid_soft='#EDF0F4',
    zebra='#F4F7FB', hover='#EAF1FB', selection='#D3E3FA',
    shadow='#D8DEE7',
)


@dataclass(frozen=True)
class DesignTokens:
    """字号/间距/圆角/行高。"""

    font_family: str = '"Microsoft YaHei UI", "Segoe UI", sans-serif'
    mono_family: str = '"Consolas", "SF Mono", "Menlo", monospace'
    font_kpi: int = 26
    font_title: int = 19
    font_section: int = 13
    font_body: int = 12
    font_small: int = 11
    font_micro: int = 10
    space_xs: int = 4
    space_sm: int = 8
    space_md: int = 12
    space_lg: int = 18
    space_xl: int = 24
    radius: int = 10
    radius_sm: int = 6
    row_height: int = 48
    header_height: int = 40


TOKENS = DesignTokens()


def palette_for(theme: str) -> Palette:
    return LIGHT if str(theme).lower() == 'light' else DARK


def advice_qss(theme: str) -> str:
    """生成本页面专用 QSS（根选择器限定在 ``#advicePage``）。"""
    p = palette_for(theme)
    t = TOKENS
    return f"""
QWidget#advicePage {{
    background: {p.bg};
    color: {p.text};
    font-family: {t.font_family};
    font-size: {t.font_body}px;
}}
QLabel#adviceTitle {{
    font-size: {t.font_title}px;
    font-weight: 700;
    color: {p.text};
}}
QLabel#adviceSubtitle, QLabel#mutedLabel {{
    color: {p.muted};
    font-size: {t.font_small}px;
}}
QLabel#freshBadge {{
    color: {p.hold};
    background: {p.hold_soft};
    border: 1px solid {p.hold};
    border-radius: {t.radius_sm}px;
    padding: 3px 9px;
    font-size: {t.font_small}px;
}}
QLabel#freshOk {{
    color: {p.sell};
    background: {p.sell_soft};
    border: 1px solid {p.sell};
    border-radius: {t.radius_sm}px;
    padding: 3px 9px;
    font-size: {t.font_small}px;
}}
QPushButton#refreshBtn {{
    background: {p.accent};
    color: #FFFFFF;
    border: none;
    border-radius: {t.radius_sm}px;
    padding: 8px 16px;
    font-size: {t.font_body}px;
    font-weight: 600;
}}
QPushButton#refreshBtn:hover {{ background: {p.accent_soft}; color: {p.accent}; }}
QPushButton#refreshBtn:disabled {{ background: {p.elevated}; color: {p.faint}; }}
QPushButton#themeBtn {{
    background: transparent;
    color: {p.muted};
    border: 1px solid {p.border};
    border-radius: {t.radius_sm}px;
    padding: 7px 12px;
    font-size: {t.font_small}px;
}}
QPushButton#themeBtn:hover {{ color: {p.text}; border-color: {p.accent}; }}
QFrame#kpiCard {{
    background: {p.card};
    border: 1px solid {p.border};
    border-radius: {t.radius}px;
}}
QLabel#kpiValue {{
    color: {p.text};
    font-family: {t.mono_family};
    font-size: {t.font_kpi}px;
    font-weight: 700;
}}
QLabel#kpiLabel {{
    color: {p.muted};
    font-size: {t.font_small}px;
}}
QFrame#tableCard, QFrame#detailCard {{
    background: {p.card};
    border: 1px solid {p.border};
    border-radius: {t.radius}px;
}}
QLineEdit#searchBox {{
    background: {p.surface};
    color: {p.text};
    border: 1px solid {p.border};
    border-radius: {t.radius_sm}px;
    padding: 8px 12px;
    selection-background-color: {p.accent};
}}
QLineEdit#searchBox:focus {{ border-color: {p.accent}; }}
QPushButton#filterChip {{
    background: transparent;
    color: {p.muted};
    border: 1px solid {p.border};
    border-radius: 14px;
    padding: 5px 12px;
    font-size: {t.font_small}px;
}}
QPushButton#filterChip:checked {{
    color: {p.accent};
    border-color: {p.accent};
    background: {p.accent_soft};
    font-weight: 600;
}}
QComboBox#sortCombo {{
    background: {p.surface};
    color: {p.text};
    border: 1px solid {p.border};
    border-radius: {t.radius_sm}px;
    padding: 6px 10px;
    font-size: {t.font_small}px;
}}
QComboBox#sortCombo QAbstractItemView {{
    background: {p.card};
    color: {p.text};
    selection-background-color: {p.accent_soft};
    selection-color: {p.accent};
}}
QTableView#adviceTable {{
    background: {p.card};
    alternate-background-color: {p.zebra};
    color: {p.text};
    border: none;
    gridline-color: transparent;
    selection-background-color: {p.selection};
    selection-color: {p.text};
    outline: none;
}}
QTableView#adviceTable::item {{
    padding: 4px 8px;
    border: none;
    border-bottom: 1px solid {p.border};
}}
QTableView#adviceTable::item:hover {{ background: {p.hover}; }}
QHeaderView#adviceHeader::section {{
    background: {p.surface};
    color: {p.muted};
    border: none;
    border-bottom: 1px solid {p.border};
    padding: 9px 8px;
    font-size: {t.font_small}px;
    font-weight: 600;
}}
QScrollBar:vertical {{
    background: transparent;
    width: 9px;
    margin: 4px 2px;
}}
QScrollBar::handle:vertical {{
    background: {p.border};
    min-height: 24px;
    border-radius: 4px;
}}
QScrollBar::handle:vertical:hover {{ background: {p.faint}; }}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height: 0; }}
QScrollBar:horizontal {{ height: 9px; background: transparent; }}
QScrollBar::handle:horizontal {{ background: {p.border}; border-radius: 4px; }}
QLabel#detailName {{
    font-size: 17px;
    font-weight: 700;
    color: {p.text};
}}
QLabel#detailCode {{
    color: {p.muted};
    font-family: {t.mono_family};
    font-size: {t.font_small}px;
}}
QLabel#sectionTitle {{
    color: {p.muted};
    font-size: {t.font_small}px;
    font-weight: 600;
    letter-spacing: 1px;
}}
QLabel#metricValue {{
    color: {p.text};
    font-family: {t.mono_family};
    font-size: 15px;
}}
QLabel#metricLabel {{
    color: {p.faint};
    font-size: {t.font_micro}px;
}}
QLabel#riskNote {{
    color: {p.muted};
    background: {p.surface};
    border: 1px solid {p.border};
    border-left: 3px solid {p.hold};
    border-radius: {t.radius_sm}px;
    padding: 9px 10px;
    font-size: {t.font_small}px;
}}
QPushButton#emptyAction {{
    background: {p.accent};
    color: #FFFFFF;
    border: none;
    border-radius: {t.radius_sm}px;
    padding: 10px 22px;
    font-weight: 600;
}}
QPushButton#emptyAction:hover {{ background: {p.accent_soft}; color: {p.accent}; }}
QWidget#emptyState, QWidget#skeletonState {{ background: {p.card}; }}
QLabel#emptyTitle {{ font-size: 16px; font-weight: 700; color: {p.text}; }}
QLabel#emptySubtitle {{ font-size: {t.font_small}px; color: {p.muted}; }}
"""
