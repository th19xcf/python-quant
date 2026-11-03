#!/usr/bin/env python3

"""离屏渲染「每日推荐」页面截图（真实 grab，不是设计稿）。

用法（仓库根目录）：
    .venv\\Scripts\\python.exe scripts\\render_advice_screenshots.py

输出：
    docs/screenshots/advice_dark.png   深色主界面（有数据 + 选中行 + 详情）
    docs/screenshots/advice_light.png  浅色对照
    docs/screenshots/advice_empty.png  深色空状态
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime
from pathlib import Path

os.environ.setdefault('QT_QPA_PLATFORM', 'offscreen')
os.environ.setdefault('QT_QPA_FONTDIR', r'C:\Windows\Fonts')
os.environ.setdefault('PYQTGRAPH_QT_OPENGL', 'software')

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from PySide6.QtCore import Qt  # noqa: E402
from PySide6.QtGui import QFont  # noqa: E402
from PySide6.QtWidgets import QApplication  # noqa: E402

from src.core.usecases.get_daily_recommendations import (  # noqa: E402
    DailyRecommendationBundle, get_daily_recommendations,
)
from src.ui.advice.page import AdvicePage  # noqa: E402


def _select_first_buy(bundle: DailyRecommendationBundle) -> str | None:
    for item in bundle.by_tier('short'):
        if item.direction == 'BUY':
            return item.ts_code
    return bundle.items[0].ts_code if bundle.items else None


def _render(page: AdvicePage, path: Path) -> None:
    page.setAttribute(Qt.WidgetAttribute.WA_DontShowOnScreen, True)
    page.resize(1440, 900)
    page.show()
    QApplication.processEvents()
    image = page.grab()
    image.save(str(path))
    page.hide()
    print(f'[screenshot] {path} {image.width()}x{image.height()}')


def main() -> int:
    out_dir = ROOT / 'docs' / 'screenshots'
    out_dir.mkdir(parents=True, exist_ok=True)

    app = QApplication.instance() or QApplication([])
    # 离屏平台默认不加载中文字体：显式注入系统字体并做族名替换
    QFont.insertSubstitution('Microsoft YaHei', 'SimHei')
    app.setFont(QFont('SimHei', 9))
    bundle = get_daily_recommendations(horizon='all')
    if not bundle.items:
        print('[screenshot] reports/advice/latest.json 为空，请先运行 '
              '`python -m src.advice --force`', file=sys.stderr)
        return 1

    target = _select_first_buy(bundle)
    dark = AdvicePage(host=None, theme='dark')
    dark.load_async = lambda refresh=False: None  # 截图脚本直接用快照，不触发后台
    dark.set_bundle(bundle, select_code=target)
    _render(dark, out_dir / 'advice_dark.png')

    light = AdvicePage(host=None, theme='light')
    light.load_async = lambda refresh=False: None
    light.set_bundle(bundle, select_code=target)
    _render(light, out_dir / 'advice_light.png')

    empty = AdvicePage(host=None, theme='dark')
    empty.load_async = lambda refresh=False: None
    empty.set_bundle(DailyRecommendationBundle(
        items=[], trade_date=date.today(),
        generated_at=datetime.now(), data_lag_days=0))
    _render(empty, out_dir / 'advice_empty.png')
    print(f'[screenshot] 数据日期 {bundle.trade_date}，{len(bundle.items)} 条')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
