#!/usr/bin/env python3

"""
多进程回测 worker：供 ProcessPoolExecutor 在子进程中调用的模块级函数。

注意（Windows 上 ProcessPoolExecutor 为 spawn 语义）：
- 本模块必须保持轻量、可安全导入，顶部禁止导入 PySide6/UI 等重资源模块；
- 引擎类等重资源的导入由 pickle 反序列化参数时按模块名自动完成，
  因此本模块顶部只保留标准库/typing 导入。
"""

from typing import Any, Dict


def run_backtest_worker(engine_cls, strategy, data, cost_model,
                        params: Dict[str, Any], initial_capital: float,
                        risk_engine=None) -> Dict[str, Any]:
    """
    在子进程中执行单次回测。

    只接收可 pickle 的普通参数（策略对象、Polars DataFrame、成本模型、风控引擎、标量配置），
    在子进程内新建引擎实例执行回测，避免传递持有不可 pickle 资源的引擎实例。

    Args:
        engine_cls: 回测引擎类（模块级类，按引用 pickle）
        strategy: 策略对象（仅用于取其类并重新实例化，与线程版语义保持一致）
        data: 回测数据（Polars DataFrame）
        cost_model: 成本模型对象
        params: 本次回测使用的参数字典
        initial_capital: 初始资金
        risk_engine: 风控引擎（None 表示不启用）

    Returns:
        Dict[str, Any]: 回测结果（附带 params）
    """
    strategy_copy = strategy.__class__()
    strategy_copy.set_params(params)

    engine = engine_cls(data)
    engine.set_strategy(strategy_copy)
    engine.set_cost_model(cost_model)
    engine.risk_engine = risk_engine

    result = engine.run_backtest(initial_capital)
    result['params'] = params
    return result
