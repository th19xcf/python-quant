#!/usr/bin/env python3

"""
数据同步公共骨架与共享工具

股票 / 指数 / 基金三条业务线共用的同步流程骨架、代码判定与字段归一化，
以及通达信日线文件定位工具。

从 sync_service.py 拆出；对外仍由 sync_service 聚合导出，调用方无感。
"""

from dataclasses import dataclass
from typing import Callable

from loguru import logger

from src.data.tdx_day_parser import build_day_file_path, read_first_last_trade_date
from src.utils.code_converter import to_ts_code
from src.utils.exceptions import DataSaveError, DataSourceException

# ======================================================================
# 同步流程骨架：股票 / 指数同步共用会话生命周期、异常策略与统计日志
# ======================================================================

@dataclass
class SyncFlow:
    """一次市场同步的流程描述。

    prepare(session) 返回上下文对象；返回 None 表示前置条件不满足、终止同步。
    run(ctx, session) 执行实际同步阶段。
    log_stats(result) 输出结果统计。
    """
    title: str
    error_label: str
    result: dict
    prepare: Callable
    run: Callable
    log_stats: Callable


def _execute_sync_flow(flow, db_manager):
    """统一执行同步流程：会话创建、异常回滚、结果统计。"""
    logger.info("=" * 60)
    logger.info(f"开始{flow.title}")
    logger.info("=" * 60)

    session = None
    try:
        session = db_manager.get_session()
        ctx = flow.prepare(session)
        if ctx is None:
            return flow.result
        flow.run(ctx, session)
    except (DataSaveError, DataSourceException) as e:
        logger.exception(f"{flow.error_label}失败: {e}")
        if session:
            session.rollback()
    finally:
        flow.log_stats(flow.result)

    return flow.result


def _sync_each_with_recovery(items, handler, *, result, failed_key, session,
                             label, commit_interval=100, item_name=str):
    """逐项执行 handler：单项失败记入 failed_key 后继续，不中断整批同步。

    commit_interval 为 0 时不周期性提交，由调用方在阶段末尾统一提交。
    """
    total = len(items)
    for i, item in enumerate(items):
        try:
            handler(item)
            if commit_interval and (i + 1) % commit_interval == 0:
                session.commit()
                logger.info(f"已处理 {i + 1}/{total} 个{label}")
        # 兜底理由：单条记录同步失败不中断整批
        except Exception as e:  # noqa: BLE001
            logger.exception(f"处理{label} {item_name(item)} 失败: {e}")
            result[failed_key].append(item_name(item))



# ======================================================================
# 通用代码判定与归一化
# ======================================================================

def _is_index_ts_code(ts_code):
    """判断 ts_code 是否为指数代码。"""
    if not ts_code or '.' not in ts_code:
        return False

    symbol, market = ts_code.split('.', 1)
    market = market.upper()
    if market == 'SH':
        return symbol.startswith('000') or symbol.startswith('880')
    if market == 'SZ':
        return symbol.startswith('399')
    if market == 'BJ':
        return symbol.startswith('884') or symbol.startswith('899')
    return False


# 基金市场/类型标记：数据源可能返回中文或英文，统一归一化后再判定，
# 避免只认 '基金' 而漏掉 'Fund'/'ETF' 等字段口径。
_FUND_MARKET_TOKENS = {'基金', 'FUND', 'FUNDS', 'ETF', 'LOF'}


def _is_fund_ts_code(ts_code, name=None, market=None):
    """判断是否为基金代码。

    代码前缀是主判据（不依赖数据源文本字段）；market/name 仅作补充，
    且先做大小写归一化，数据源字段口径变化（如 '基金' -> 'Fund'）不再失效。
    """
    if not ts_code or '.' not in ts_code:
        return False

    symbol, suffix = ts_code.split('.', 1)
    suffix = suffix.upper()
    mkt = (market or '').strip()
    nm = (name or '').strip()

    if suffix == 'SH' and symbol.startswith(('50', '51', '52', '56', '58')):
        return True
    if suffix == 'SZ' and symbol.startswith(('15', '16', '18')):
        return True

    if mkt.upper() in _FUND_MARKET_TOKENS:
        return True
    return bool(any(token in nm.upper() for token in ('ETF', 'LOF')) or '基金' in nm)


def _to_ts_code_or_none(code):
    """将外部数据源代码统一转换为标准 ts_code，非法格式返回 None。"""
    try:
        return to_ts_code(str(code).strip())
    except (TypeError, ValueError):
        return None


def _normalize_text_value(value, max_len=None):
    """标准化文本字段值。"""
    text = str(value or '').strip()
    if not text:
        return ''
    if max_len is not None:
        return text[:max_len]
    return text


def _normalize_date_value(value):
    """标准化日期字段值，支持 date/datetime/常见字符串格式。"""
    from datetime import date, datetime

    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    text = str(value).strip()
    if not text or text in {'0000-00-00', 'None', 'nan', 'NaT'}:
        return None

    for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%Y%m%d'):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    return None


# ======================================================================
# 通达信日线文件
# ======================================================================

def _get_tdx_day_file_path(config, ts_code):
    """根据 ts_code 生成通达信日线文件路径（委托统一路径构造）。"""
    if not ts_code or '.' not in ts_code:
        return None

    symbol, market = ts_code.split('.', 1)
    market = market.upper()
    if market not in {'SH', 'SZ', 'BJ'}:
        return None

    return build_day_file_path(config.data.tdx_data_path, market.lower(), symbol)


def _extract_tdx_first_last_trade_date(config, ts_code):
    """读取通达信日线文件首末交易日（委托统一解析入口）。"""
    day_file = _get_tdx_day_file_path(config, ts_code)
    if not day_file or not day_file.exists():
        return None, None

    return read_first_last_trade_date(day_file)
