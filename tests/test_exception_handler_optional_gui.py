"""GUI 可选化的前置验证：缺 PySide6 时异常处理与数据层仍可导入。

pyproject 尚未把 PySide6 拆成可选组，本用例先在子进程里屏蔽 PySide6，
确认 exception_handler 及其下游模块不因缺包抛 ImportError；
同时在进程内确认有 Qt 时弹窗路径仍被调用。
"""

import subprocess
import sys
import types
from pathlib import Path

# 子进程脚本：安装 meta_path 拦截器屏蔽 PySide6，再导入目标模块并跑一次异常处理。
_BLOCK_QT_SCRIPT = """
import sys


class _BlockPySide6:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "PySide6" or fullname.startswith("PySide6."):
            raise ImportError("PySide6 已被测试屏蔽")
        return None


for _name in [n for n in list(sys.modules) if n == "PySide6" or n.startswith("PySide6.")]:
    del sys.modules[_name]
sys.meta_path.insert(0, _BlockPySide6())

import src.data.local_store.share_capital  # noqa: E402, F401
import src.data.local_store.turnover  # noqa: E402, F401
import src.data.services.data_service  # noqa: E402, F401
import src.data.sources.akshare.etf  # noqa: E402, F401
import src.data.sources.baostock.session  # noqa: E402, F401
import src.utils.exception_handler as eh  # noqa: E402

handler = eh.GlobalExceptionHandler()
try:
    raise ValueError("boom")
except ValueError:
    handler.handle_exception(*sys.exc_info())

assert handler.get_error_stats()["total_errors"] == 1
sys.stdout.write("OK_NO_PYSIDE6\\n")
"""


def test_imports_and_handling_work_without_pyside6():
    """屏蔽 PySide6 后，exception_handler 与五个下游模块导入成功且异常处理不崩。"""
    root = Path(__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "-c", _BLOCK_QT_SCRIPT],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert proc.returncode == 0, f"stderr:\n{proc.stderr[-3000:]}"
    assert "OK_NO_PYSIDE6" in proc.stdout


def test_dialog_path_still_uses_qt(monkeypatch):
    """有 Qt 时，惰性导入仍按 sys.modules 当前内容取 QMessageBox 并弹窗。"""
    from src.utils.exception_handler import GlobalExceptionHandler

    calls: list[tuple[str, tuple]] = []

    class _StubApplication:
        @staticmethod
        def instance():
            return object()

    class _StubMessageBox:
        @staticmethod
        def critical(*args, **kwargs):
            calls.append(("critical", args))

        @staticmethod
        def warning(*args, **kwargs):
            calls.append(("warning", args))

    fake_widgets = types.ModuleType("PySide6.QtWidgets")
    fake_widgets.QApplication = _StubApplication
    fake_widgets.QMessageBox = _StubMessageBox
    monkeypatch.setitem(sys.modules, "PySide6.QtWidgets", fake_widgets)

    handler = GlobalExceptionHandler()
    handler._show_error_dialog({"error_severity": "CRITICAL", "error_message": "x"})
    handler._show_error_dialog({"error_severity": "ERROR", "error_message": "y"})

    assert [name for name, _ in calls] == ["critical", "warning"]
