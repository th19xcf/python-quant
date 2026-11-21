"""分层守卫：ST / 分红加载已下沉到 data 层，backtest 与 quant 不再反向依赖 core。"""

from __future__ import annotations

import importlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_panel_inputs_importable_and_callable() -> None:
    mod = importlib.import_module('src.data.services.panel_inputs')
    assert callable(mod.load_st_status)
    assert callable(mod.load_local_dividends)


def test_callers_importable_without_core() -> None:
    # 导入即验证：两个调用点不再在模块级触发 src.core
    importlib.import_module('src.backtest.runner.pipeline')
    importlib.import_module('src.quant.strategy_lab.backtest')

    for rel in ('src/backtest/runner/pipeline.py',
                'src/quant/strategy_lab/backtest.py'):
        text = (ROOT / rel).read_text(encoding='utf-8')
        assert 'src.core' not in text, f'{rel} 仍引用 src.core'
        assert '_load_st_status' not in text, f'{rel} 仍引用 core 私有函数'
        assert '_load_local_dividends' not in text, f'{rel} 仍引用 core 私有函数'
