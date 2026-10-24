"""指标提示文本渲染层测试：纯 Python，不需要 GUI 环境。"""

import subprocess
import sys
from pathlib import Path

# 展示层不得引入 Qt，因此测试模块导入前断言
assert 'PyQt5' not in sys.modules
assert 'PyQt6' not in sys.modules

from src.ui.event_handlers.indicator_display import (  # noqa: E402
    WINDOW2_TEXTS,
    WINDOW3_TEXTS,
    render_indicator_text,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DISPLAY_MODULE = REPO_ROOT / 'src' / 'ui' / 'event_handlers' / 'indicator_display.py'

EXPECTED_WINDOW2 = {
    "VOL", "MACD", "RSI", "KDJ", "WR", "BOLL", "SAR", "TRIX", "DMI", "DMA",
    "BRAR", "FSL", "VR", "CR", "OBV", "HSL", "LB", "ASI", "EMV", "CCI",
    "ROC", "MTM", "PSY", "MCST",
}

EXPECTED_WINDOW3 = {
    "KDJ", "RSI", "MACD", "WR", "BOLL", "DMI", "TRIX", "BRAR", "VR", "DMA",
    "SAR", "FSL", "CR", "OBV", "EXPMA", "BBI", "CYC", "CYS", "ASI", "EMV",
    "CCI", "ROC", "MTM", "PSY", "MCST", "HSL", "LB",
}


def fmt(value, format_str="{:.2f}"):
    """与 MouseHandler._format_value 行为一致的数字格式化函数。"""
    if value is None:
        return "--"
    try:
        return format_str.format(value)
    except (TypeError, ValueError):
        return "--"


def test_table_keys_exactly_match_migrated_indicators():
    """两个窗口的指标键集合必须与迁移结果完全一致。"""
    assert set(WINDOW2_TEXTS) == EXPECTED_WINDOW2
    assert set(WINDOW3_TEXTS) == EXPECTED_WINDOW3
    # 窗口2 不含仅窗口3 提供的指标，窗口3 不含 VOL
    assert EXPECTED_WINDOW3 - EXPECTED_WINDOW2 == {"EXPMA", "BBI", "CYC", "CYS"}
    assert EXPECTED_WINDOW2 - EXPECTED_WINDOW3 == {"VOL"}


def test_macd_window3_renders_three_values_with_colors():
    data = {'macd': [1.234], 'macd_signal': [0.5], 'macd_hist': [-0.5]}
    expected = (
        "<font color='white'>MACD(12,26,9) </font>"
        "<font color='#FFFFFF'>DIF: 1.23</font>  "
        "<font color='#FFFF00'>DEA: 0.50</font>  "
        "<font color='#00FF00'>MACD: -0.50</font>"
    )
    assert render_indicator_text(WINDOW3_TEXTS, "MACD", data, 0, fmt) == expected


def test_macd_window2_uses_raw_two_decimal_formatting():
    data = {'macd': [1.234], 'macd_signal': [0.5], 'macd_hist': [0.25]}
    expected = (
        "<font color='white'>MACD(12,26,9) </font>"
        "<font color='#FFFFFF'>DIF: 1.23</font>  "
        "<font color='#FFFF00'>DEA: 0.50</font>  "
        "<font color='#FF0000'>MACD: 0.25</font>"
    )
    assert render_indicator_text(WINDOW2_TEXTS, "MACD", data, 0, fmt) == expected


def test_kdj_window3_renders_three_values_with_colors():
    data = {'k': [80.5], 'd': [60.25], 'j': [95.0]}
    expected = (
        "<font color='white'>K: 80.50</font>  "
        "<font color='yellow'>D: 60.25</font>  "
        "<font color='magenta'>J: 95.00</font>"
    )
    assert render_indicator_text(WINDOW3_TEXTS, "KDJ", data, 0, fmt) == expected


def test_hsl_renders_threshold_color_and_ma_tails():
    data = {'hsl': [12.5], 'hsl_ma5': [10.0], 'hsl_ma10': [8.25]}
    expected = (
        "<font color='#FF0000'>HSL: 12.50%</font>  "
        "<font color='white'>MA5: 10.00%</font>  "
        "<font color='cyan'>MA10: 8.25%</font>"
    )
    assert render_indicator_text(WINDOW3_TEXTS, "HSL", data, 0, fmt) == expected
    # 阈值配色：>5 用橙色，<=5 用蓝色
    assert render_indicator_text(WINDOW3_TEXTS, "HSL", {'hsl': [6.0]}, 0, fmt) == (
        "<font color='#FFA500'>HSL: 6.00%</font>"
    )
    assert render_indicator_text(WINDOW3_TEXTS, "HSL", {'hsl': [5.0]}, 0, fmt) == (
        "<font color='#00BFFF'>HSL: 5.00%</font>"
    )


def test_lb_threshold_colors():
    expected_by_value = {
        2.5: '#FF0000',
        2.0: '#FFA500',
        1.5: '#00FF7F',
        1.0: '#00BFFF',
    }
    for value, color in expected_by_value.items():
        text = render_indicator_text(WINDOW2_TEXTS, "LB", {'lb': [value]}, 0, fmt)
        assert text == f"<font color='{color}'>LB: {value:.2f}</font>"


def test_missing_data_and_out_of_range_index_return_empty_string():
    data = {'macd': [1.0], 'macd_signal': [0.5], 'macd_hist': [0.5]}
    assert render_indicator_text(WINDOW3_TEXTS, "MACD", data, 1, fmt) == ""
    assert render_indicator_text(WINDOW3_TEXTS, "MACD", data, -1, fmt) == ""
    assert render_indicator_text(WINDOW3_TEXTS, "MACD", None, 0, fmt) == ""
    # 缺主键同样视为无数据
    assert render_indicator_text(WINDOW3_TEXTS, "MACD", {'macd_signal': [0.5]}, 0, fmt) == ""


def test_unregistered_indicator_returns_empty_string():
    assert render_indicator_text(WINDOW2_TEXTS, "NOT_EXIST", {'x': [1]}, 0, fmt) == ""
    assert render_indicator_text(WINDOW3_TEXTS, "VOL", {'volume': [1]}, 0, fmt) == ""


def test_display_module_import_does_not_load_gui_libraries():
    """直接按文件加载模块，绕开包 __init__，验证模块本身不依赖 Qt。"""
    code = (
        "import importlib.util, sys\n"
        f"path = r'{DISPLAY_MODULE}'\n"
        "spec = importlib.util.spec_from_file_location('indicator_display_probe', path)\n"
        "module = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(module)\n"
        "gui = ('PyQt5', 'PyQt6', 'PySide2', 'PySide6', 'pyqtgraph')\n"
        "print('|'.join(name for name in gui if name in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, '-c', code], cwd=REPO_ROOT, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == ""
