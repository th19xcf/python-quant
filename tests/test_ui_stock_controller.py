"""个股数据控制器任务回调测试（离屏，无网络/数据库）。"""

from types import SimpleNamespace

import src.ui.data_market_renderer as market_renderer_module
import src.ui.data_stock_controller as stock_module
from src.ui.data_market_renderer import MarketTableRenderer
from src.ui.data_stock_controller import StockDataController


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

    def debug(self, message):
        self._record("DEBUG", message)

    def exception(self, message):
        self._record("ERROR", message)


def _make_window():
    window = SimpleNamespace(
        current_kline_data={},
        current_kline_index=-1,
        current_stock_data=None,
        current_stock_name="",
        current_stock_code="",
    )
    window.status_messages = []
    window.plot_calls = []
    window.clear_calls = 0
    window.market_clear_calls = 0

    def _clear_kline_display():
        window.clear_calls += 1

    def _clear_market_table_display():
        window.market_clear_calls += 1

    def _plot_k_line(df, name, code):
        window.plot_calls.append((df, name, code))

    window.statusBar = lambda: SimpleNamespace(
        showMessage=lambda msg, timeout=0: window.status_messages.append((msg, timeout))
    )
    window.tab_widget = SimpleNamespace(setCurrentIndex=lambda index: None)
    window.plot_k_line = _plot_k_line
    window.stock_table = object()
    window._clear_kline_display = _clear_kline_display
    window._clear_market_table_display = _clear_market_table_display
    return window


class _FakeRenderer:
    """记录行情渲染调用，不触碰真实表格。"""

    def __init__(self):
        self.calls = []

    def _render_market_result(self, result):
        self.calls.append(result)


def _make_controller(monkeypatch, task_id="stock-task-1"):
    manager = _FakeTaskManager(task_id)
    fake_logger = _FakeLogger()
    monkeypatch.setattr(stock_module, "global_task_manager", manager)
    monkeypatch.setattr(stock_module, "logger", fake_logger)
    window = _make_window()
    controller = StockDataController(window, scanner=None, renderer=None)
    return controller, window, manager, fake_logger


def _make_market_controller(monkeypatch, task_id="market-task-1"):
    manager = _FakeTaskManager(task_id)
    fake_logger = _FakeLogger()
    monkeypatch.setattr(stock_module, "global_task_manager", manager)
    monkeypatch.setattr(stock_module, "logger", fake_logger)
    window = _make_window()
    renderer = _FakeRenderer()
    controller = StockDataController(window, scanner=None, renderer=renderer)
    return controller, window, manager, fake_logger, renderer


def test_unrelated_task_result_without_success_is_ignored(monkeypatch):
    """每日推荐等任务返回 {token, bundle} 时不得触发股票回调（原崩溃路径）。"""
    controller, window, manager, fake_logger = _make_controller(monkeypatch)
    controller.process_stock_data("600519", "贵州茅台")

    manager.task_completed.emit("advice-task-9", {"token": 1, "bundle": object()})

    assert window.plot_calls == []
    assert window.clear_calls == 0
    assert window.status_messages == [("加载 贵州茅台(600519) 数据...", 0)]
    assert [r for r in fake_logger.records if r[0] == "ERROR"] == []


def test_own_task_result_missing_success_logs_error(monkeypatch):
    """本任务返回值缺 success 字段时给出可读日志并清空K线，不抛出异常。"""
    controller, window, manager, fake_logger = _make_controller(monkeypatch)
    controller.process_stock_data("600519", "贵州茅台")

    manager.task_completed.emit("stock-task-1", {"data": object()})

    errors = [msg for level, msg in fake_logger.records if level == "ERROR"]
    assert len(errors) == 1
    assert "缺少 success 字段" in errors[0]
    assert window.clear_calls == 1
    assert window.status_messages[-1] == ("加载失败: 任务返回结果格式异常（缺少 success 字段）", 5000)
    assert window.plot_calls == []


def test_own_task_success_still_plots_k_line(monkeypatch):
    """正常成功路径行为不变。"""
    controller, window, manager, fake_logger = _make_controller(monkeypatch)
    controller.process_stock_data("600519", "贵州茅台")

    df = object()
    manager.task_completed.emit(
        "stock-task-1",
        {"success": True, "data": df, "stock_name": "贵州茅台", "stock_code": "600519"},
    )

    assert window.plot_calls == [(df, "贵州茅台", "600519")]
    assert window.status_messages[-1] == ("已加载 贵州茅台(600519) 的K线图", 3000)
    assert window.clear_calls == 0


def test_stock_error_callback_ignores_unrelated_task_error(monkeypatch):
    """其他任务报错不得清空 K 线或改写状态栏（item 3）。"""
    controller, window, manager, fake_logger = _make_controller(monkeypatch)
    controller.process_stock_data("600519", "贵州茅台")

    manager.task_error.emit("advice-task-9", "boom")

    assert window.clear_calls == 0
    assert window.status_messages == [("加载 贵州茅台(600519) 数据...", 0)]


def test_stock_error_callback_handles_own_task_error(monkeypatch):
    """本任务报错时保持原有清空与提示行为。"""
    controller, window, manager, fake_logger = _make_controller(monkeypatch)
    controller.process_stock_data("600519", "贵州茅台")

    manager.task_error.emit("stock-task-1", "boom")

    assert window.clear_calls == 1
    assert window.status_messages[-1] == ("加载失败: boom", 5000)


def test_market_callback_ignores_unrelated_task_events(monkeypatch):
    """其他任务完成/报错/进度事件不得影响行情回调或 UI。"""
    controller, window, manager, fake_logger, renderer = _make_market_controller(monkeypatch)
    controller._show_stock_data_by_type_impl("全部A股")

    manager.task_completed.emit("advice-task-9", {"token": 1, "bundle": object()})
    manager.task_error.emit("advice-task-9", "boom")
    manager.task_progress.emit("advice-task-9", 50, 100)

    assert renderer.calls == []
    assert window.market_clear_calls == 0
    assert window.status_messages == [("Loading 全部A股 data...", 0)]
    assert [r for r in fake_logger.records if r[0] == "ERROR"] == []


def test_market_callback_missing_success_logs_error(monkeypatch):
    """本行情任务返回值缺 success 字段时记录 ERROR 并清空行情表，不抛出异常。"""
    controller, window, manager, fake_logger, renderer = _make_market_controller(monkeypatch)
    controller._show_stock_data_by_type_impl("全部A股")

    manager.task_completed.emit("market-task-1", {"table_data": []})

    errors = [msg for level, msg in fake_logger.records if level == "ERROR"]
    assert len(errors) == 1
    assert "缺少 success 字段" in errors[0]
    assert renderer.calls == []
    assert window.market_clear_calls == 1
    assert window.status_messages[-1] == ("显示行情失败: 任务返回结果格式异常（缺少 success 字段）", 5000)


def test_market_callback_renders_own_task_result(monkeypatch):
    """本行情任务成功结果仍交给渲染器。"""
    controller, window, manager, fake_logger, renderer = _make_market_controller(monkeypatch)
    controller._show_stock_data_by_type_impl("全部A股")

    result = {"success": True, "table_data": [], "stock_type": "全部A股", "stock_count": 0}
    manager.task_completed.emit("market-task-1", result)

    assert renderer.calls == [result]


def test_market_error_callback_ignores_unrelated_task_error(monkeypatch):
    """其他任务报错不得清空行情表或改写状态栏。"""
    controller, window, manager, fake_logger, renderer = _make_market_controller(monkeypatch)
    controller._show_stock_data_by_type_impl("全部A股")

    manager.task_error.emit("advice-task-9", "boom")

    assert window.market_clear_calls == 0
    assert window.status_messages == [("Loading 全部A股 data...", 0)]


def test_market_error_callback_handles_own_task_error(monkeypatch):
    """本行情任务报错时保持原有清空与提示行为。"""
    controller, window, manager, fake_logger, renderer = _make_market_controller(monkeypatch)
    controller._show_stock_data_by_type_impl("全部A股")

    manager.task_error.emit("market-task-1", "boom")

    assert window.market_clear_calls == 1
    assert window.status_messages[-1] == ("显示全部A股数据失败: boom", 5000)


def test_market_renderer_ignores_result_without_success(monkeypatch):
    """渲染器对缺 success 的结果只记录 ERROR，不抛出异常、不产生 UI 副作用。"""
    fake_logger = _FakeLogger()
    monkeypatch.setattr(market_renderer_module, "logger", fake_logger)
    window = _make_window()
    renderer = MarketTableRenderer(window)

    renderer._render_market_result({"table_data": []})

    errors = [msg for level, msg in fake_logger.records if level == "ERROR"]
    assert len(errors) == 1
    assert "缺少 success 字段" in errors[0]
    assert window.market_clear_calls == 0
    assert window.status_messages == []


def test_market_renderer_missing_stock_stats_degrades(monkeypatch):
    """成功结果缺 stock_type/stock_count 时仍渲染表格，仅统计文案降级并记录 ERROR。"""
    fake_logger = _FakeLogger()
    monkeypatch.setattr(market_renderer_module, "logger", fake_logger)
    window = _make_window()
    renderer = MarketTableRenderer(window)
    appended = []
    monkeypatch.setattr(renderer, "_append_market_rows_to_table", lambda table, rows: appended.append(rows))

    renderer._render_market_result({"success": True, "table_data": []})

    errors = [msg for level, msg in fake_logger.records if level == "ERROR"]
    assert len(errors) == 1
    assert "stock_type" in errors[0] and "stock_count" in errors[0]
    assert appended == [[]]
    assert window.status_messages[-1] == ("行情数据显示完成（部分统计信息缺失）", 3000)
    assert window.market_clear_calls == 0

    renderer._render_market_result(
        {"success": True, "table_data": [], "stock_type": "全部A股", "stock_count": 0}
    )
    assert appended == [[], []]
    assert window.status_messages[-1] == ("成功显示0只全部A股股票的最新交易日数据", 3000)


def test_market_renderer_invalid_table_data_logs_error(monkeypatch):
    """table_data 缺失或非可迭代行集合时记录 ERROR 并 return，不部分渲染。"""
    fake_logger = _FakeLogger()
    monkeypatch.setattr(market_renderer_module, "logger", fake_logger)
    window = _make_window()
    renderer = MarketTableRenderer(window)
    appended = []
    monkeypatch.setattr(renderer, "_append_market_rows_to_table", lambda table, rows: appended.append(rows))

    renderer._render_market_result({"success": True})
    renderer._render_market_result(
        {"success": True, "table_data": "not-a-row-list", "stock_type": "全部A股", "stock_count": 1}
    )
    renderer._render_market_result(
        {"success": True, "table_data": [123], "stock_type": "全部A股", "stock_count": 1}
    )

    errors = [msg for level, msg in fake_logger.records if level == "ERROR"]
    assert len(errors) == 3
    assert all("table_data 非法" in msg for msg in errors)
    assert "非法行号=0" in errors[-1]
    assert appended == []
    assert window.market_clear_calls == 0
    assert window.status_messages == []


def test_build_market_cell_tolerates_unparsable_preclose(qapp):
    """昨收价无法解析为数字时按 0.0 处理，不因 float() 崩溃。"""
    renderer = MarketTableRenderer(_make_window())
    row = [f"v{index}" for index in range(13)]
    row[11] = "N/A"

    item = renderer._build_market_cell(4, "10.00", row)

    assert item.text() == "10.00"
