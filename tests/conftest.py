"""
pytest 全局配置

UI 相关测试使用 Qt 离屏平台，避免依赖真实显示器。
环境变量必须在导入 PySide6 之前设置，因此放在 conftest 顶部。
"""

import importlib
import os
import warnings

_external_qpa_platform = os.environ.get("QT_QPA_PLATFORM")
if _external_qpa_platform and _external_qpa_platform.lower() != "offscreen":
    warnings.warn(
        f"检测到外部已设置 QT_QPA_PLATFORM={_external_qpa_platform!r}，"
        "UI 测试可能跑在真实显示环境；如需离屏请显式设置 QT_QPA_PLATFORM=offscreen",
        stacklevel=2,
    )

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
os.environ.setdefault("PYQTGRAPH_QT_OPENGL", "software")

from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _isolate_advice_artifacts(tmp_path_factory, monkeypatch):
    """把推荐链路的默认写盘目录重定向到临时目录。

    `pipeline.generate` 调 `run_ic_monitor` 时未传 `out_dir`，默认写到仓库
    `reports/advice/ic_monitor`；全量 pytest 会因此把夹具快照写进真实产物。
    """
    root = tmp_path_factory.mktemp('advice_artifacts')
    base = root / 'reports' / 'advice'

    def _monitor_dir() -> Path:
        return base / 'ic_monitor'

    def _daily_dir(project_root=None) -> Path:
        return base / 'daily'

    def _default_dirs() -> tuple[Path, Path]:
        return base, base / 'state'

    from src.advice import ic_monitor, pipeline

    usecase = importlib.import_module('src.core.usecases.get_daily_recommendations')

    monkeypatch.setattr(ic_monitor, '_monitor_dir', _monitor_dir)
    monkeypatch.setattr(pipeline, 'default_daily_dir', _daily_dir)
    monkeypatch.setattr(usecase, '_default_dirs', _default_dirs)



@pytest.fixture(autouse=True)
def _allow_standalone_dataset_write(request, monkeypatch):
    """写入层闸门（``dataset_contracts.check_write_context``，2026-11-22 接入）要求
    调度令牌或显式放行。单测不经 ``tools/daily_update.py`` 调度，统一按「独立写」
    放行；ledger 落在各测试自己的 store 根（通常是 tmp_path）。

    需要验证闸门本身拒绝行为的用例，加 ``@pytest.mark.write_gate`` 即可跳过放行。
    """
    if request.node.get_closest_marker('write_gate'):
        return
    monkeypatch.delenv('QUANT_RUN_TOKEN', raising=False)
    monkeypatch.setenv('QUANT_ALLOW_STANDALONE', '1')
    monkeypatch.setenv('QUANT_STANDALONE_REASON', 'pytest')


@pytest.fixture(scope="session")
def qapp():
    """会话级 QApplication，供 UI 测试复用。"""
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])
    yield app
