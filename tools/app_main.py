#!/usr/bin/env python3
"""薄壳：转发到统一入口 ``python -m src.main``（浏览器通道）。

桌面通道是 ``python -m src.main`` 的默认行为，入口在菜单「量化分析 → 回测工作台」。
本文件只为兼容旧命令而保留，启动逻辑只有 ``src.main`` 一处。
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.main import main  # noqa: E402

if __name__ == '__main__':
    sys.argv = [sys.argv[0]] + (sys.argv[1:] or ['--workbench'])
    raise SystemExit(main())
