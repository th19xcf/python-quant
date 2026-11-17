#!/usr/bin/env python3

"""每日推荐 UI 包：页面、设计令牌、表格模型与自绘控件。

``AdvicePage`` 延迟导入：``table_sizing`` 需要 ``advice.tokens``，若此处
在包初始化时立刻导入页面，会形成 ``table_sizing -> advice.page ->
table_sizing`` 的循环导入。
"""

__all__ = ['AdvicePage']


def __getattr__(name):
    if name == 'AdvicePage':
        from src.ui.advice.page import AdvicePage

        return AdvicePage
    raise AttributeError(name)
