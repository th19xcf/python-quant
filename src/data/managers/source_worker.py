#!/usr/bin/env python3

"""
数据源子进程执行器

远程纯网络数据源（AkShare / BaoStock）在独立子进程内执行，
避免第三方库挂死（无响应、阻塞 socket）拖垮主进程。

本模块顶层函数必须保持可 pickle：子进程通过 spawn 启动，
只接收 handler 类路径、方法名、kwargs 与配置对象，
在子进程内重新 import 并构造 handler（db_manager 传 None，仅取数不落库）。
"""

import importlib

import polars as pl
from loguru import logger


def call_source(payload: dict) -> tuple[bool, str, pl.DataFrame]:
    """
    子进程入口：重建数据源并调用取数方法。

    Args:
        payload: {
            'handler': 'src.data.akshare_handler:AkShareHandler',
            'method': 'get_stock_data',
            'kwargs': {...},
            'config': 配置对象（可为 None）,
        }

    Returns:
        (ok, error, df)：构造/调用失败时 ok=False 且 error 非空；
        成功时 ok=True（df 可能为空，空结果不算故障）。
    """
    error = ""
    try:
        handler_path = payload['handler']
        module_path, class_name = handler_path.split(':')
        module = importlib.import_module(module_path)
        handler_cls = getattr(module, class_name)
        handler = handler_cls(payload.get('config'), None)

        result = getattr(handler, payload['method'])(**payload['kwargs'])
        if hasattr(result, 'collect'):
            result = result.collect()
        if isinstance(result, pl.DataFrame):
            df = result
        else:
            df = pl.DataFrame(result) if result is not None else pl.DataFrame()
        return True, "", df
    # 兜底理由：子进程内任何异常都必须回传给主进程，不能直接崩溃
    except Exception as e:  # noqa: BLE001
        error = f"{type(e).__name__}: {e}"
        logger.warning(f"子进程数据源调用失败: {error}")
        return False, error, pl.DataFrame()
