#!/usr/bin/env python3
"""双击启动入口：以同一入口 ``python -m src.main`` 拉起桌面主程序。

本文件不写任何启动逻辑，只用控制台解释器复跑统一入口（pythonw 下没有
stdout/stderr，日志与控制台输出需要真实的控制台解释器）。
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON = Path(sys.executable).with_name('python.exe')
if not PYTHON.is_file():
    PYTHON = Path(sys.executable)

if __name__ == '__main__':
    raise SystemExit(subprocess.call([str(PYTHON), '-m', 'src.main'], cwd=str(ROOT)))
