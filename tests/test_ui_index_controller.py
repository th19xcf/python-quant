"""指数数据控制器任务回调测试（离屏，无网络/数据库）。"""

from types import SimpleNamespace

import pytest

import src.ui.data_index_controller as index_module
from src.ui.data_index_controller import IndexDataController


class _RecordingTable:
    """记录表格方法调用，用于断言未发生半成品渲染。"""

    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        def _record(*args, **kwargs):
            self.calls.append(name)

        return _record


class _FakeSignal:
    """最小信号实现：同步分发，便于断言异常与调用顺序。"""

    def __init__(self):
        self.handlers = []

    def connect(self, handler):
        self.handlers.append(handler)

    def disconnect(self, handler):
        if handler not in self.handlers:
            raise RuntimeError("handler not connected")
        self.handlers.remove(handler)

    def emit(self, *args):
        for handler in list(self.handlers):
            handler(*args)


class _FakeTaskManager:
    """只记录任务创建并返回固定 task_id，不真正启动后台线程。"""

    def __init__(self, task_id):
        self.task_id = task_id
        self.created = []
        self.task_progress = _FakeSignal()
        self.task_completed = _FakeSignal()
        self.task_error = _FakeSignal()

    def create_task(self, task_name, func, args=(), kwargs=None, timeout_seconds=60):
        self.created.append((task_name, func, args))
        return self.task_id


class _FakeLogger:
    def __init__(self):
        self.records = []

    def _record(self, level, message):
        self.records.append((level, message))

    def info(self, message):
        self._record("INFO", message)

    def error(self, message):
        self._record("ERROR", message)

    def warning(self, message):
        self._record("WARNING", message)

    def profile(self, message):
        self._record("PROFILE", message)

    def exception(self, message):
        self._record("ERROR", message)


class _Recorder:
    def __init__(self):
        self.calls = []

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))


def _make_window():
    window = SimpleNamespace()
    window.status_messages = []
    window.market_clear_calls = 0
    window.kline_clear_calls = 0
    window.stock_table = _RecordingTable()
    window.added_rows = []

    def _clear_market_table_display():
        window.market_clear_calls += 1

    def _clear_kline_display():
        window.kline_clear_calls += 1

    def _add_table_row(data_row):
        window.added_rows.append(data_row)

    window.statusBar = lambda: SimpleNamespace(
        showMessage=lambda msg, timeout=0: window.status_messages.append((msg, timeout))
    )
    window._clear_market_table_display = _clear_market_table_display
    window._clear_kline_display = _clear_kline_display
    window._add_table_row = _add_table_row
    return window


def _make_controller(monkeypatch, task_id="index-task-1"):
    manager = _FakeTaskManager(task_id)
    fake_logger = _FakeLogger()
    monkeypatch.setattr(index_module, "global_task_manager", manager)
    monkeypatch.setattr(index_module, "logger", fake_logger)
    window = _make_window()
    controller = IndexDataController(window)
    return controller, window, manager, fake_logger


def test_index_callback_ignores_unrelated_task_events(monkeypatch):
    """其他任务完成/报错/进度事件不得影响指数回调或 UI。"""
    controller, window, manager, fake_logger = _make_controller(monkeypatch)
    render_calls = _Recorder()
    monkeypatch.setattr(controller, "_render_index_result", render_calls)

    controller._load_index_data(market="all", title="沪深京指数")

    manager.task_completed.emit("advice-task-9", {"token": 1, "bundle": object()})
    manager.task_error.emit("advice-task-9", "boom")
    manager.task_progress.emit("advice-task-9", 50, 100)

    assert render_calls.calls == []
    assert window.market_clear_calls == 0
    assert window.status_messages == [("加载沪深京指数数据...", 0)]
    assert [r for r in fake_logger.records if r[0] == "ERROR"] == []


def test_index_callback_missing_success_logs_error(monkeypatch):
    """本指数任务返回值缺 success 字段时记录 ERROR 并清空行情表，不抛出异常。"""
    controller, window, manager, fake_logger = _make_controller(monkeypatch)
    render_calls = _Recorder()
    monkeypatch.setattr(controller, "_render_index_result", render_calls)

    controller._load_index_data(market="all", title="沪深京指数")

    manager.task_completed.emit("index-task-1", {"index_data": []})

    errors = [msg for level, msg in fake_logger.records if level == "ERROR"]
    assert len(errors) == 1
    assert "缺少 success 字段" in errors[0]
    assert render_calls.calls == []
    assert window.market_clear_calls == 1
    assert window.status_messages[-1] == ("获取指数数据失败: 任务返回结果格式异常（缺少 success 字段）", 5000)


def test_index_callback_success_still_renders(monkeypatch):
    """本指数任务成功结果仍交给渲染逻辑。"""
    controller, window, manager, fake_logger = _make_controller(monkeypatch)
    render_calls = _Recorder()
    monkeypatch.setattr(controller, "_render_index_result", render_calls)

    controller._load_index_data(market="all", title="沪深京指数")

    result = {"success": True, "index_data": [], "title": "沪深京指数"}
    manager.task_completed.emit("index-task-1", result)

    assert render_calls.calls == [((result,), {})]


def test_index_callback_failure_without_message_uses_fallback(monkeypatch):
    """本任务 success=False 且缺 message 时使用兜底文案并记录 ERROR。"""
    controller, window, manager, fake_logger = _make_controller(monkeypatch)
    render_calls = _Recorder()
    monkeypatch.setattr(controller, "_render_index_result", render_calls)

    controller._load_index_data(market="all", title="沪深京指数")

    manager.task_completed.emit("index-task-1", {"success": False})

    errors = [msg for level, msg in fake_logger.records if level == "ERROR"]
    assert errors == ["获取指数数据失败（任务未返回原因）"]
    assert window.market_clear_calls == 1
    assert window.status_messages[-1] == ("获取指数数据失败（任务未返回原因）", 5000)


def test_index_error_callback_ignores_unrelated_task_error(monkeypatch):
    """其他任务报错不得清空行情表或改写状态栏。"""
    controller, window, manager, fake_logger = _make_controller(monkeypatch)
    controller._load_index_data(market="all", title="沪深京指数")

    manager.task_error.emit("advice-task-9", "boom")

    assert window.market_clear_calls == 0
    assert window.status_messages == [("加载沪深京指数数据...", 0)]


def test_index_error_callback_handles_own_task_error(monkeypatch):
    """本指数任务报错时保持原有清空与提示行为。"""
    controller, window, manager, fake_logger = _make_controller(monkeypatch)
    controller._load_index_data(market="all", title="沪深京指数")

    manager.task_error.emit("index-task-1", "boom")

    assert window.market_clear_calls == 1
    assert window.status_messages[-1] == ("Error: boom", 5000)


def test_show_index_data_failure_without_message_uses_fallback(monkeypatch):
    """单指数 K 线任务 success=False 且缺 message 时使用兜底文案并记录 ERROR。"""
    controller, window, manager, fake_logger = _make_controller(monkeypatch)
    controller._show_index_data_impl("上证指数")

    manager.task_completed.emit("index-task-1", {"success": False})

    errors = [msg for level, msg in fake_logger.records if level == "ERROR"]
    assert errors == ["加载上证指数数据失败（任务未返回原因）"]
    assert window.kline_clear_calls == 1
    assert window.status_messages[-1] == ("加载上证指数数据失败（任务未返回原因）", 5000)


@pytest.mark.parametrize(
    "payload",
    [
        {"success": True, "title": "沪深京指数"},
        {"success": True, "index_data": []},
        {"success": True, "title": "沪深京指数", "index_data": 5},
    ],
)
def test_render_index_result_missing_fields_logs_error(monkeypatch, payload):
    """缺 index_data 或 title 时不渲染半成品，记录 ERROR 并给出可读提示。"""
    controller, window, manager, fake_logger = _make_controller(monkeypatch)

    controller._render_index_result(payload)

    errors = [msg for level, msg in fake_logger.records if level == "ERROR"]
    assert len(errors) == 1
    assert "缺少必要字段" in errors[0]
    assert window.stock_table.calls == []
    assert window.status_messages == [("显示指数数据失败: 任务返回结果缺少必要字段", 5000)]


def test_show_index_data_missing_success_logs_error(monkeypatch):
    """单指数任务结果缺 success 键时记录 ERROR 并提示，不再 KeyError。"""
    controller, window, manager, fake_logger = _make_controller(monkeypatch)
    controller._show_index_data_impl("上证指数")

    manager.task_completed.emit("index-task-1", {"data": object()})

    errors = [msg for level, msg in fake_logger.records if level == "ERROR"]
    assert len(errors) == 1
    assert "缺少 success 字段" in errors[0]
    assert window.status_messages[-1] == ("加载失败: 任务返回结果格式异常（缺少 success 字段）", 5000)
    assert window.kline_clear_calls == 0


def test_render_index_result_skips_short_rows(monkeypatch):
    """字段不足的行跳过并记录行号与标的，正常行仍然写入表格。"""
    controller, window, manager, fake_logger = _make_controller(monkeypatch)
    short_row = ["2026-11-02", "000001.SH", "上证指数"]
    valid_row = [f"v{index}" for index in range(13)]

    controller._render_index_result(
        {"success": True, "title": "沪深京指数", "index_data": [short_row, valid_row]}
    )

    warnings = [msg for level, msg in fake_logger.records if level == "WARNING"]
    assert len(warnings) == 1
    assert "行号=0" in warnings[0]
    assert "上证指数(000001.SH)" in warnings[0]
    assert "字段数=3" in warnings[0]
    assert window.added_rows == [valid_row]
