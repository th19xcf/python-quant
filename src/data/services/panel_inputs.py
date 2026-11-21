"""回测数据准备的中立入口：ST 时间维表与本地分红快照。

从 ``src.core.usecases.run_backtest`` 下沉而来，行为与签名逐位不变；目的是让
``src.backtest`` / ``src.quant.strategy_lab`` 不再为读取这两类数据反向依赖
``src.core`` 的私有函数。

本模块只依赖 data 层与配置；对 ``src.backtest.engine.corporate_actions`` 的引用
保持在函数内（沿用原实现），不形成导入期依赖。
"""

from __future__ import annotations

from typing import Any, Dict, Union

import polars as pl
from loguru import logger


def load_st_status(
    st_status: Dict[str, Any] | None,
    data: Union[pl.DataFrame, Dict[str, pl.DataFrame]] | None,
    ts_code: str | None,
    start_date: str | None,
    end_date: str | None,
    db: Any,
    db_manager: Any,
    warnings_out: list[dict] | None = None,
) -> Dict[str, Any] | None:
    """ST 时间维表：未显式传入时按标的与日期范围加载。

    ``warnings_out`` 收集降级事件：P0-4 起加载失败不再只写日志后静默按
    "未知/无事件"处理，调用方必须能从回测结果里看到 ``data_quality.degraded``。

    优先读本地历史区间表（``meta/st``，含可信窗口内的显式非 ST 段），
    本地无记录再回退数据库维表；两者都没有才按未知处理。
    """
    if st_status is not None:
        return st_status
    if data is None:
        return None
    if isinstance(data, dict):
        codes = list(data.keys())
    else:
        codes = [ts_code] if ts_code else []
    if not codes:
        return None
    try:
        from src.data import st_history
        from src.data.local_store.store import resolve_store_root
        from src.utils.config import get_config

        local = st_history.intervals_for_backtest(
            resolve_store_root(get_config()), codes, start_date, end_date)
        if local:
            return local
    # 兜底理由：本地区间表读取失败时继续走数据库维表，不阻断回测
    except Exception as e:  # noqa: BLE001
        logger.warning(f"读取本地 ST 历史区间表失败，回退数据库维表: {e}")
        if warnings_out is not None:
            warnings_out.append({
                'stage': 'st_local_history', 'severity': 'fallback',
                'fallback': 'database_dimension', 'error': f'{type(e).__name__}: {e}',
            })
    st_db = db if db is not None else db_manager
    if st_db is None:
        return None
    try:
        from src.data.services.st_status_service import StStatusService

        return StStatusService(db_manager=st_db).get_status_intervals(
            codes, start_date, end_date,
        )
    # ST 维表加载失败会按「ST 状态未知」处理，涨跌停档位不可信 → 降级
    except Exception as e:  # noqa: BLE001
        logger.warning(f"加载 ST 时间维表失败，按未知处理: {e}")
        if warnings_out is not None:
            warnings_out.append({
                'stage': 'st_database_dimension', 'severity': 'degraded',
                'impact': 'ST 状态未知，涨跌停按板块默认幅度判定',
                'error': f'{type(e).__name__}: {e}',
            })
        return None


def load_local_dividends(
    codes: list[str], db: Any, db_manager: Any,
    warnings_out: list[dict] | None = None,
) -> Dict[str, pl.DataFrame]:
    """从本地列存分红快照加载除权事件（只读；任何失败均降级为空并留痕）。

    不经过 ``DatabaseManager`` / 远程接口：分红数据缺失只意味着本次回测不记账
    除权事件，不会阻塞回测，也不会静默从远端补数。
    """
    if not codes:
        return {}
    config = getattr(db, 'config', None) or getattr(db_manager, 'config', None)
    if config is None:
        try:
            from src.utils.config import Config
            config = Config()
        # 配置不可用 → 本次回测不记账任何分红，净值会偏离真实可交易结果
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"分红配置不可用，跳过除权事件: {exc}")
            if warnings_out is not None:
                warnings_out.append({
                    'stage': 'dividend_config', 'severity': 'degraded',
                    'impact': '不记账现金分红/送转/配股',
                    'error': f'{type(exc).__name__}: {exc}',
                })
            return {}
    try:
        from src.backtest.engine.corporate_actions import load_local_dividend_frames
        frames = load_local_dividend_frames(codes, config)
        if not frames and warnings_out is not None:
            # 快照整体缺失/未命中不再静默：结果里必须能看到降级标记
            warnings_out.append({
                'stage': 'dividend_local_snapshot',
                'severity': 'degraded',
                'impact': '所有标的除权事件按「无事件」处理，现金分红/送转缺失',
                'error': f'分红快照未命中任何标的（请求 {len(codes)} 只）',
            })
        return frames
    # 快照损坏/缺失依旧跑完回测，但结果必须标记 degraded
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"加载本地分红快照失败，跳过除权事件: {exc}")
        if warnings_out is not None:
            warnings_out.append({
                'stage': 'dividend_local_snapshot', 'severity': 'degraded',
                'impact': '除权除息事件按「无事件」处理，现金分红/送转缺失',
                'error': f'{type(exc).__name__}: {exc}',
            })
        return {}
