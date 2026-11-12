#!/usr/bin/env python3

"""
数据源子进程执行器

远程纯网络数据源（AkShare / BaoStock）在独立子进程内执行，
避免第三方库挂死（无响应、阻塞 socket）拖垮主进程。

本模块顶层函数必须保持可 pickle：子进程通过 spawn 启动，
只接收 handler 类路径、方法名与 kwargs，
配置对象由 ``ProcessPoolExecutor(initializer=init_worker)`` 在建池时注入一次，
不再随每次 submit 的 payload 重复序列化。
"""

import importlib
from typing import Any

import polars as pl
from loguru import logger

# 子进程内的配置对象，由 init_worker 在建池时写入；进程内只此一份，不另设并行传参通道
_WORKER_CONFIG: Any = None


def init_worker(config: Any = None) -> None:
    """
    子进程池初始化入口：把配置对象注入本进程全局。

    由 ``ProcessPoolExecutor(initializer=init_worker, initargs=(config,))`` 调用。
    每个 worker 进程启动时执行一次；池被重建（超时终止后）时新进程会再次执行，
    重复注入同一对象属幂等操作。
    """
    global _WORKER_CONFIG
    _WORKER_CONFIG = config


def call_source(payload: dict) -> tuple[bool, str, pl.DataFrame]:
    """
    子进程入口：重建数据源并调用取数方法。

    Args:
        payload: {
            'handler': 'src.data.akshare_handler:AkShareHandler',
            'method': 'get_stock_data',
            'kwargs': {...},
        }
        配置对象不在此处，由 ``init_worker`` 注入到 ``_WORKER_CONFIG``。

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
        handler = handler_cls(_WORKER_CONFIG, None)

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
