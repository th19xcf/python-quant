"""兼容封装（薄）：表级保鲜体检已并入 ``freshness``，本模块仅转发旧符号。

历史沿革：``freshness_check`` 曾是独立的硬编码体检实现；判据归一后，表级
13 行实现的代码迁到私有模块 ``_freshness_tables``，由 :mod:`src.data.quality.freshness`
统一对外。为不打断既有调用点与单元测试，保留本同名模块，属性按需惰性转发。

生产代码应直接从 ``src.data.quality.freshness`` 取 ``run_check`` / ``format_report``
等；本模块仅供旧路径与测试兼容。
"""

from __future__ import annotations

from src.data.quality import freshness as _freshness


def __getattr__(name: str):
    """把对旧符号（``run_check`` / ``TABLE_SPECS`` / ``main`` ...）的访问转发到 freshness。"""
    return getattr(_freshness, name)


if __name__ == '__main__':
    raise SystemExit(_freshness.main())
