"""策略试算三图：X 轴联动、Y 轴独立、调仓标记不因联动丢失。"""

from __future__ import annotations

import os
from datetime import date, timedelta

os.environ.setdefault('QT_QPA_PLATFORM', 'offscreen')

import pytest

pytest.importorskip('pyqtgraph')

from PySide6.QtWidgets import QApplication

from src.ui.strategy_lab.page import StrategyLabPage


def _series(n: int = 200) -> tuple[list[str], list, list, list]:
    start = date(2024, 1, 2)
    days = [(start + timedelta(days=i)).isoformat() for i in range(n)]
    nav = [[day, 100.0 + i] for i, day in enumerate(days)]
    drawdown = [[day, -0.01 * (i % 7)] for i, day in enumerate(days)]
    markers = [{'date': days[i], 'position': 1.0} for i in range(0, n, 20)]
    return days, nav, drawdown, markers


def _render(page: StrategyLabPage, nav, drawdown, markers) -> None:
    page._plot_curves(nav, [])
    page._plot_drawdown(drawdown)
    page._plot_markers(markers)
    QApplication.processEvents()


def _assert_x(plot, lo: float, hi: float) -> None:
    view_lo, view_hi = plot.viewRange()[0]
    assert view_lo == pytest.approx(lo, abs=1e-3)
    assert view_hi == pytest.approx(hi, abs=1e-3)


def test_x_axis_linked_while_y_stays_independent() -> None:
    QApplication.instance() or QApplication([])
    page = StrategyLabPage()
    _, nav, drawdown, markers = _series()
    _render(page, nav, drawdown, markers)

    page.nav_plot.setXRange(40, 90, padding=0)
    QApplication.processEvents()
    _assert_x(page.drawdown_plot, 40, 90)
    _assert_x(page.marker_plot, 40, 90)

    dd_y = page.drawdown_plot.viewRange()[1]
    marker_y = page.marker_plot.viewRange()[1]
    page.nav_plot.setYRange(1.0, 1.2, padding=0)
    QApplication.processEvents()
    assert page.drawdown_plot.viewRange()[1] == pytest.approx(dd_y)
    assert page.marker_plot.viewRange()[1] == pytest.approx(marker_y)
    page.deleteLater()


def test_marker_points_survive_linked_full_range() -> None:
    QApplication.instance() or QApplication([])
    page = StrategyLabPage()
    _, nav, drawdown, markers = _series()
    _render(page, nav, drawdown, markers)

    page.nav_plot.setXRange(0, len(nav) - 1, padding=0)
    QApplication.processEvents()
    _assert_x(page.marker_plot, 0, len(nav) - 1)

    item = page.marker_plot.listDataItems()[0]
    xs, _ = item.getData()
    assert len(xs) == len(markers)
    assert min(xs) >= 0 and max(xs) <= len(nav) - 1
    page.deleteLater()


def test_link_persists_after_rerender() -> None:
    QApplication.instance() or QApplication([])
    page = StrategyLabPage()
    _render(page, *_series(200)[1:])
    _render(page, *_series(50)[1:])

    page.nav_plot.setXRange(5, 20, padding=0)
    QApplication.processEvents()
    _assert_x(page.drawdown_plot, 5, 20)
    _assert_x(page.marker_plot, 5, 20)
    page.deleteLater()
