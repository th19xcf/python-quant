#!/usr/bin/env python3

"""
多进程回测 worker：供 ProcessPoolExecutor 在子进程中调用的模块级函数。

注意（Windows 上 ProcessPoolExecutor 为 spawn 语义）：
- 本模块必须保持轻量、可安全导入，顶部禁止导入 PySide6/UI 等重资源模块；
- 引擎类等重资源的导入由 pickle 反序列化参数时按模块名自动完成，
  因此本模块顶部只保留标准库/typing 导入。
"""

import copy
from typing import Any, Dict


def clone_strategy(strategy, params: Dict[str, Any]):
    """
    按原策略完整参数深拷贝后覆盖本次网格参数。

    原实现用 ``strategy.__class__()`` 重新实例化，会丢弃未纳入 param_ranges
    的构造参数（如只优化 short_window 时 long_window 回落到类默认值），
    使优化子回测与主回测口径不一致。深拷贝失败（策略持有不可拷贝资源）时
    退化为空构造，保持旧行为而不是让整轮优化中断。
    """
    try:
        clone = copy.deepcopy(strategy)
    except Exception:  # noqa: BLE001 —— 策略可能持有不可深拷贝资源，退化为空构造
        clone = strategy.__class__()
    clone.set_params(params)
    return clone


def run_backtest_worker(engine_cls, strategy, data, cost_model,
                        params: Dict[str, Any], initial_capital: float,
                        risk_engine=None,
                        context: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """
    在子进程中执行单次回测。

    只接收可 pickle 的普通参数（策略对象、Polars DataFrame、成本模型、风控引擎、标量配置），
    在子进程内新建引擎实例执行回测，避免传递持有不可 pickle 资源的引擎实例。

    Args:
        engine_cls: 回测引擎类（模块级类，按引用 pickle）
        strategy: 策略对象（深拷贝后覆盖网格参数，保留未参与优化的原参数）
        data: 回测数据（Polars DataFrame）
        cost_model: 成本模型对象
        params: 本次回测使用的参数字典
        initial_capital: 初始资金
        risk_engine: 风控引擎（None 表示不启用）
        context: 主回测执行上下文 {ts_code, industry, is_st}。必须透传，
            否则子回测按 '__default__' 口径推导涨跌停（300/688/北交所
            一律按主板 10%）、行业风控缺失、ST 状态未知，与主回测不可比。

    Returns:
        Dict[str, Any]: 回测结果（附带 params）
    """
    strategy_copy = clone_strategy(strategy, params)

    engine = engine_cls(data)
    engine.set_strategy(strategy_copy)
    engine.set_cost_model(cost_model)
    engine.risk_engine = risk_engine

    ctx = context or {}
    result = engine.run_backtest(
        initial_capital,
        ts_code=ctx.get('ts_code', ''),
        industry=ctx.get('industry', ''),
        is_st=ctx.get('is_st'),
    )
    result['params'] = params
    return result
