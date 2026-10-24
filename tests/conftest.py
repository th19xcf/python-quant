"""
pytest 全局配置

UI 相关测试使用 Qt 离屏平台，避免依赖真实显示器。
环境变量必须在导入 PySide6 之前设置，因此放在 conftest 顶部。
"""

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
os.environ.setdefault("PYQTGRAPH_QT_OPENGL", "software")

import pytest


@pytest.fixture(scope="session")
def qapp():
    """会话级 QApplication，供 UI 测试复用。"""
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])
    yield app
