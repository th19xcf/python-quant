#!/usr/bin/env python3

"""推荐表格的模型/代理/格式化与纯函数筛选排序。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from PySide6.QtCore import (
    QAbstractTableModel,
    QModelIndex,
    QPersistentModelIndex,
    QSortFilterProxyModel,
    Qt,
)
from PySide6.QtGui import QFont

from src.core.usecases.get_daily_recommendations import DailyRecommendationDTO

DTO_ROLE = Qt.ItemDataRole.UserRole + 1
SORT_ROLE = Qt.ItemDataRole.UserRole + 2
#: 模型顶层索引的共享默认值（避免在函数签名里重复构造 QModelIndex）
_NO_PARENT = QModelIndex()


@dataclass(frozen=True)
class Column:
    key: str
    title: str
    width: int
    align: int
    numeric: bool = False


COLUMNS: tuple[Column, ...] = (
    Column('ts_code', '代码', 92, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter),
    Column('name', '名称', 84, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter),
    Column('direction', '方向', 72, Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter),
    Column('target_weight_pct', '仓位', 70, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, True),
    Column('shares', '参考股数', 84, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, True),
    Column('ref_price', '参考价', 68, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, True),
    Column('confidence', '置信度', 100, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter),
    Column('top_factors', '因子贡献', 190, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter),
    Column('risk_notes', '风险 / 持有期', 210, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter),
)

COL_CODE = 0
COL_NAME = 1
COL_DIRECTION = 2
COL_WEIGHT = 3
COL_SHARES = 4
COL_PRICE = 5
COL_CONFIDENCE = 6
COL_FACTORS = 7
COL_RISK = 8

DIRECTION_FILTERS = ('全部', 'BUY', 'SELL', 'HOLD', 'AVOID')
ASSET_FILTERS = ('全部', '股票', 'ETF', '基金')


def _format_shares(value: int) -> str:
    return f'{int(value):,}'


def display_value(dto: DailyRecommendationDTO, column: int) -> str:
    if column == COL_CODE:
        return dto.ts_code
    if column == COL_NAME:
        return dto.name
    if column == COL_DIRECTION:
        return dto.direction
    if column == COL_WEIGHT:
        return f'{dto.target_weight_pct:.1f}%'
    if column == COL_SHARES:
        return _format_shares(dto.shares)
    if column == COL_PRICE:
        return f'{dto.ref_price:.2f}'
    if column == COL_CONFIDENCE:
        return f'{dto.confidence:.2f}'
    if column == COL_FACTORS:
        return dto.top_factors or '—'
    if column == COL_RISK:
        first = (dto.risk_notes or '').split('；')[0]
        return f'{first} · {dto.validity_days}日'
    return ''


def sort_value(dto: DailyRecommendationDTO, column: int) -> Any:
    if column == COL_CODE:
        return dto.ts_code
    if column == COL_NAME:
        return dto.name
    if column == COL_DIRECTION:
        order = {'BUY': 0, 'SELL': 1, 'HOLD': 2, 'AVOID': 3}
        return order.get(dto.direction, 9)
    if column == COL_WEIGHT:
        return dto.target_weight_pct
    if column == COL_SHARES:
        return dto.shares
    if column == COL_PRICE:
        return dto.ref_price
    if column == COL_CONFIDENCE:
        return dto.confidence
    if column == COL_FACTORS:
        return dto.top_factors
    if column == COL_RISK:
        return dto.risk_notes
    return ''


def match_filters(dto: DailyRecommendationDTO, direction: str = '全部',
                  asset_type: str = '全部', search: str = '') -> bool:
    """方向 / 资产类型 / 关键词筛选（纯函数，便于单测）。"""
    if direction and direction != '全部' and dto.direction != direction:
        return False
    if asset_type and asset_type != '全部' and dto.asset_type != asset_type:
        return False
    keyword = (search or '').strip().lower()
    if not keyword:
        return True
    haystack = ' '.join((dto.ts_code, dto.name, dto.industry,
                         dto.top_factors, dto.risk_notes)).lower()
    return keyword in haystack


def filter_rows(items: Iterable[DailyRecommendationDTO], direction: str = '全部',
                asset_type: str = '全部', search: str = '') -> list[DailyRecommendationDTO]:
    return [item for item in items
            if match_filters(item, direction, asset_type, search)]


def sort_rows(items: list[DailyRecommendationDTO], column: int,
              descending: bool = True) -> list[DailyRecommendationDTO]:
    return sorted(items, key=lambda item: sort_value(item, column),
                  reverse=descending)


class AdviceTableModel(QAbstractTableModel):
    """只持有 DTO 列表，不建逐格 widget。"""

    def __init__(self, items: list[DailyRecommendationDTO] | None = None) -> None:
        super().__init__()
        self._items: list[DailyRecommendationDTO] = list(items or [])

    def set_items(self, items: list[DailyRecommendationDTO]) -> None:
        self.beginResetModel()
        self._items = list(items)
        self.endResetModel()

    def items(self) -> list[DailyRecommendationDTO]:
        return list(self._items)

    def rowCount(  # noqa: N802
        self, parent: QModelIndex | QPersistentModelIndex = _NO_PARENT
    ) -> int:
        return 0 if parent.isValid() else len(self._items)

    def columnCount(  # noqa: N802
        self, parent: QModelIndex | QPersistentModelIndex = _NO_PARENT
    ) -> int:
        return 0 if parent.isValid() else len(COLUMNS)

    def data(  # noqa: N802
        self, index: QModelIndex | QPersistentModelIndex,
        role: int = Qt.ItemDataRole.DisplayRole
    ) -> Any:
        if not index.isValid() or not (0 <= index.row() < len(self._items)):
            return None
        dto = self._items[index.row()]
        column = index.column()
        if role == Qt.ItemDataRole.DisplayRole:
            return display_value(dto, column)
        if role == Qt.ItemDataRole.TextAlignmentRole:
            return COLUMNS[column].align
        if role == Qt.ItemDataRole.ToolTipRole:
            return f'{dto.ts_code} {dto.name}\n{dto.top_factors}\n{dto.risk_notes}'
        if role == Qt.ItemDataRole.FontRole and COLUMNS[column].numeric:
            font = QFont('Consolas')
            font.setPointSize(9)
            return font
        if role == DTO_ROLE:
            return dto
        if role == SORT_ROLE:
            return sort_value(dto, column)
        return None

    def headerData(self, section: int, orientation: Qt.Orientation,
                   role: int = Qt.ItemDataRole.DisplayRole) -> Any:  # noqa: N802
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return COLUMNS[section].title
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.TextAlignmentRole:
            return COLUMNS[section].align
        return None


class AdviceFilterProxyModel(QSortFilterProxyModel):
    """方向/资产/搜索过滤 + 按列排序。"""

    def __init__(self) -> None:
        super().__init__()
        self._direction = '全部'
        self._asset_type = '全部'
        self._search = ''
        self.setDynamicSortFilter(True)

    def _invalidate(self) -> None:
        # 三类过滤都变化时整体失效；invalidateFilter/RowsFilter 在 6.9 已废弃
        self.invalidate()

    def set_direction(self, value: str) -> None:
        self._direction = value
        self._invalidate()

    def set_asset_type(self, value: str) -> None:
        self._asset_type = value
        self._invalidate()

    def set_search(self, value: str) -> None:
        self._search = value
        self._invalidate()

    def filterAcceptsRow(  # noqa: N802
        self, source_row: int,
        source_parent: QModelIndex | QPersistentModelIndex
    ) -> bool:
        model = self.sourceModel()
        if not isinstance(model, AdviceTableModel):
            return True
        index = model.index(source_row, 0, source_parent)
        dto = model.data(index, DTO_ROLE)
        if dto is None:
            return True
        return match_filters(dto, self._direction, self._asset_type, self._search)

    def lessThan(  # noqa: N802
        self, left: QModelIndex | QPersistentModelIndex,
        right: QModelIndex | QPersistentModelIndex
    ) -> bool:
        model = self.sourceModel()
        left_dto = model.data(left, DTO_ROLE)
        right_dto = model.data(right, DTO_ROLE)
        if left_dto is None or right_dto is None:
            return False
        left_value = sort_value(left_dto, left.column())
        right_value = sort_value(right_dto, right.column())
        try:
            return left_value < right_value
        except TypeError:
            return str(left_value) < str(right_value)
