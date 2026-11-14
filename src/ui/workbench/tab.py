"""回测工作台标签页：把本地回环服务页面嵌进主窗口右侧标签页容器。

页面与浏览器通道共用 ``src.ui.workbench.server`` 的同一份 Shell、队列与输出目录，
这里只负责起一个 QWebEngineView 并把主题切到当前色板。
"""
from __future__ import annotations

from src.utils.logger import logger


def create_workbench_tab(parent):
    """创建回测工作台标签页控件；缺少 QtWebEngine 时返回 None（由调用方提示）。"""
    try:
        from PySide6.QtCore import QUrl
        from PySide6.QtWebEngineWidgets import QWebEngineView
    except Exception as exc:  # noqa: BLE001  缺 QtWebEngine 时退化为不可用提示
        logger.warning(f'回测工作台不可用：{exc}')
        return None

    from src.ui.theme_manager import is_dark_palette
    from src.ui.workbench.server import ensure_server

    url = ensure_server()
    theme = 'dark' if is_dark_palette() else 'light'
    view = QWebEngineView(parent)
    view.setObjectName('quantWorkbenchView')
    view.setUrl(QUrl(f'{url}?theme={theme}'))
    logger.info(f'回测工作台标签页已创建：{url}({theme})')
    return view
