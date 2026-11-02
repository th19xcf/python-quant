"""MouseHandler 指标提示边界测试 + vol_tdx 注销回归。

崩溃现场：取消勾选 SAR/BOLL 后 window_indicators[2/3] 会被置为 None，
_update_tooltips 直接对 None 调用 .lower() 触发 AttributeError。
"""

import src.ui.event_handlers.mouse_handlers as mouse_handlers
from src.ui.event_handlers.mouse_handlers import MouseHandler

DATES = ['2024-01-01', '2024-01-02']
OPENS = [1.0, 2.0]
HIGHS = [1.0, 2.0]
LOWS = [1.0, 2.0]
CLOSES = [1.0, 2.0]


class _FakeLabel:
    """最小化 QLabel 替身，只记录最后一次 setText 的内容。"""

    def __init__(self):
        self.text = None

    def setText(self, text):
        self.text = text


class _FakeMainWindow:
    def __init__(self, indicators):
        self.window_indicators = indicators
        self.volume_values_label = _FakeLabel()
        self.kdj_values_label = _FakeLabel()
        self.current_volume_data = {
            'volume': [1000, 2000],
            'vol_ma5': [1000, 2000],
            'vol_ma10': [1000, 2000],
        }
        self.current_kdj_data = {'k': [80.0, 70.0], 'd': [60.0, 50.0], 'j': [90.0, 85.0]}

    def update_ma_values_display(self, *args):
        pass


class _RecordingLogger:
    """记录 warning/exception 的 logger 替身。"""

    def __init__(self):
        self.warnings = []
        self.exceptions = []

    def warning(self, message):
        self.warnings.append(message)

    def exception(self, message):
        self.exceptions.append(message)


def _make_handler(monkeypatch, indicators):
    recorder = _RecordingLogger()
    monkeypatch.setattr(mouse_handlers, 'logger', recorder)
    return MouseHandler(_FakeMainWindow(indicators)), recorder


def _render(handler, index=0):
    handler._update_tooltips(index, DATES, OPENS, HIGHS, LOWS, CLOSES)


def test_none_indicator_is_skipped_with_warning(monkeypatch):
    """取消勾选 SAR/BOLL 后的 None 指标只跳过告警，不再抛出 AttributeError。"""
    handler, recorder = _make_handler(monkeypatch, {2: None, 3: 'KDJ'})

    _render(handler)

    assert recorder.exceptions == []
    assert len(recorder.warnings) == 1
    assert '窗口2' in recorder.warnings[0]
    assert 'None' in recorder.warnings[0]
    # 另一个窗口不受影响，仍正常渲染
    assert 'K: 80.00' in handler.main_window.kdj_values_label.text


def test_unregistered_indicator_is_skipped_with_warning(monkeypatch):
    """未注册指标名不得影响另一个窗口，并且要留下告警。"""
    handler, recorder = _make_handler(monkeypatch, {2: 'NOT_EXIST', 3: 'KDJ'})

    _render(handler)

    assert recorder.exceptions == []
    assert len(recorder.warnings) == 1
    assert 'NOT_EXIST' in recorder.warnings[0]
    assert handler.main_window.volume_values_label.text is None
    assert 'K: 80.00' in handler.main_window.kdj_values_label.text


def test_empty_indicator_name_is_skipped_with_warning(monkeypatch):
    handler, recorder = _make_handler(monkeypatch, {2: '   ', 3: 'KDJ'})

    _render(handler)

    assert recorder.exceptions == []
    assert len(recorder.warnings) == 1
    assert '窗口2' in recorder.warnings[0]


def test_same_invalid_indicator_warns_only_once(monkeypatch):
    """鼠标逐根移动时同一无效指标不得重复刷日志。"""
    handler, recorder = _make_handler(monkeypatch, {2: None, 3: 'KDJ'})

    _render(handler, index=0)
    _render(handler, index=1)

    assert len(recorder.warnings) == 1


def test_vol_tdx_removed_from_ui_indicator_mappings():
    """vol_tdx 已从指标注册表注销，UI 侧不得残留计算/渲染/提示映射。"""
    from src.ui.chart_components.indicator_renderer import INDICATOR_CALC_MAP
    from src.ui.chart_components.renderers.volume import VolumeRenderMixin
    from src.ui.event_handlers.indicator_display import WINDOW2_TEXTS, WINDOW3_TEXTS

    assert 'VOL-TDX' not in INDICATOR_CALC_MAP
    assert not hasattr(VolumeRenderMixin, 'render_vol_tdx')
    assert 'VOL-TDX' not in WINDOW2_TEXTS
    assert 'VOL-TDX' not in WINDOW3_TEXTS
