#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
中国股市量化分析系统主入口
"""

# 标准库导入
import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

# 第三方库导入
from loguru import logger
from PySide6.QtWidgets import QApplication

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data.data_manager import DataManager
from src.database.db_manager import DatabaseManager
from src.plugin.plugin_manager import PluginManager
from src.ui.main_window import MainWindow
from src.ui.theme_manager import ThemeManager
from src.utils.cache_monitor import log_cache_stats
# 内部模块导入
from src.utils.config import config_manager, get_config
from src.utils.event_bus import EventType, publish, shutdown_event_bus
from src.utils.exception_handler import setup_global_exception_handler
from src.utils.logger import setup_logger
from src.utils.memory_manager import global_memory_manager


def _fetch_external_stock_basic(baostock_handler, akshare_handler):
    """获取外部股票基本信息

    Args:
        baostock_handler: Baostock处理器
        akshare_handler: AkShare处理器

    Returns:
        tuple: (baostock_basic, akshare_basic, baostock_industry_map)
    """
    baostock_basic = None
    akshare_basic = None
    baostock_industry_map = {}

    logger.info("开始从 Baostock 获取股票基本信息...")
    try:
        baostock_basic = baostock_handler.update_stock_basic()
        if baostock_basic is not None:
            logger.info(f"从 Baostock 获取到 {baostock_basic.height} 条股票信息")
    except Exception as e:
        logger.warning(f"从 Baostock 获取股票基本信息失败: {e}")

    try:
        baostock_industry_map = _fetch_baostock_industry_map(baostock_handler)
        if baostock_industry_map:
            logger.info(f"从 Baostock 行业接口获取到 {len(baostock_industry_map)} 条行业映射")
    except Exception as e:
        logger.warning(f"从 Baostock 获取行业映射失败: {e}")

    logger.info("开始从 AkShare 获取股票基本信息...")
    try:
        akshare_basic = akshare_handler.update_stock_basic()
        if akshare_basic is not None:
            logger.info(f"从 AkShare 获取到 {akshare_basic.height} 条股票信息")
    except Exception as e:
        logger.warning(f"从 AkShare 获取股票基本信息失败: {e}")

    return baostock_basic, akshare_basic, baostock_industry_map


def _fetch_baostock_industry_map(baostock_handler):
    """从 Baostock 获取股票行业映射（ts_code -> industry）。"""
    import baostock as bs
    import polars as pl

    industry_map = {}
    if not baostock_handler or not baostock_handler._ensure_baostock_login():
        return industry_map

    rs = bs.query_stock_industry()
    industry_pd = rs.get_data()
    industry_df = pl.from_pandas(industry_pd)
    if industry_df.is_empty():
        return industry_map

    for row in industry_df.iter_rows(named=True):
        ts_code = _parse_baostock_code(row.get('code', ''), {})
        if not ts_code:
            continue
        if _is_index_ts_code(ts_code) or _is_fund_ts_code(ts_code):
            continue
        industry = str(row.get('industry', '') or '').strip()
        if industry:
            industry_map[ts_code] = industry

    return industry_map


def _fetch_akshare_industry_map(tdx_stock_codes):
    """从 AkShare 行业板块成分获取行业映射（ts_code -> industry）。"""
    import akshare as ak
    import polars as pl

    industry_map = {}
    tdx_market_map = _build_tdx_market_map(tdx_stock_codes)

    try:
        board_pd = ak.stock_board_industry_name_em()
        board_df = pl.from_pandas(board_pd)
        if board_df.is_empty():
            logger.warning("AkShare 行业板块列表为空")
            return industry_map

        board_names = []
        for row in board_df.iter_rows(named=True):
            name = row.get('板块名称') or row.get('名称') or row.get('name')
            if name:
                board_names.append(str(name).strip())

        logger.info(f"AkShare 行业板块数量: {len(board_names)}")

        for i, board_name in enumerate(board_names):
            if not board_name:
                continue
            try:
                cons_pd = ak.stock_board_industry_cons_em(symbol=board_name)
                cons_df = pl.from_pandas(cons_pd)
                if cons_df.is_empty():
                    continue

                for row in cons_df.iter_rows(named=True):
                    raw_code = row.get('代码') or row.get('code') or row.get('symbol')
                    if not raw_code:
                        continue
                    ts_code = _parse_akshare_code(str(raw_code).strip(), tdx_market_map)
                    if not ts_code:
                        continue
                    if _is_index_ts_code(ts_code) or _is_fund_ts_code(ts_code):
                        continue
                    if ts_code not in industry_map:
                        industry_map[ts_code] = board_name

                if (i + 1) % 20 == 0:
                    logger.info(
                        f"AkShare行业映射进度: {i + 1}/{len(board_names)}，"
                        f"当前映射 {len(industry_map)} 只股票"
                    )
            except Exception as board_e:
                logger.warning(f"获取行业板块 {board_name} 成分失败: {board_e}")
                continue

    except Exception as e:
        logger.warning(f"从 AkShare 获取行业映射失败: {e}")

    return industry_map


def _create_stock_basic_from_info(ts_code, stock_info):
    """从股票信息创建StockBasic对象

    Args:
        ts_code: 股票代码
        stock_info: 股票信息字典

    Returns:
        StockBasic: 数据库模型对象
    """
    from src.database.models.stock import StockBasic, StockDaily

    name = stock_info.get('name', '未知')
    name = name[:45] if len(name) > 45 else name

    symbol = stock_info.get('symbol', ts_code.split('.')[0])
    symbol = symbol[:9] if len(symbol) > 9 else symbol

    return StockBasic(
        ts_code=ts_code,
        symbol=symbol,
        name=name,
        area=stock_info.get('area'),
        industry=stock_info.get('industry'),
        market=stock_info.get('market'),
        list_date=stock_info.get('list_date'),
        delist_date=stock_info.get('delist_date'),
        status=stock_info.get('status', 'L')
    )


@dataclass
class StockBatchContext:
    """股票批处理上下文"""
    new_codes: list
    merged_basic: dict
    existing_codes: set
    session: object
    baostock_handler: object
    akshare_handler: object
    result: dict


def _process_single_stock_batch(ctx):
    """处理单批股票

    Args:
        ctx: 股票批处理上下文
    """
    import time

    from src.database.models.stock import StockBasic

    for i, ts_code in enumerate(ctx.new_codes):
        try:
            logger.info(f"[{i+1}/{len(ctx.new_codes)}] 处理股票: {ts_code}")

            if ts_code in ctx.existing_codes:
                logger.info(f"股票 {ts_code} 已存在，跳过")
                continue

            stock_info = get_stock_info_from_apis(
                ts_code, ctx.merged_basic, ctx.baostock_handler, ctx.akshare_handler
            )

            if stock_info is None:
                logger.warning(f"无法获取股票 {ts_code} 的信息，跳过")
                ctx.result['failed_stocks'].append(ts_code)
                continue

            stock = _create_stock_basic_from_info(ts_code, stock_info)
            ctx.session.add(stock)
            ctx.result['inserted_stocks'] += 1
            ctx.existing_codes.add(ts_code)

            if (i + 1) % 50 == 0:
                try:
                    ctx.session.commit()
                    logger.info(f"已提交 {i + 1} 条股票记录到数据库")
                except Exception as commit_e:
                    logger.exception(f"提交数据库时失败: {commit_e}")
                    ctx.session.rollback()

            time.sleep(0.1)

        except Exception as e:
            logger.exception(f"处理股票 {ts_code} 失败: {e}")
            ctx.result['failed_stocks'].append(ts_code)
            continue


def _finalize_database_commit(session, result):
    """完成数据库最终提交

    Args:
        session: 数据库会话
        result: 结果字典
    """
    try:
        session.commit()
    except Exception as commit_e:
        logger.exception(f"最终提交数据库时失败: {commit_e}")
        session.rollback()
    logger.info(
        "股票信息同步完成，"
        f"新增 {result['inserted_stocks']} 条，"
        f"更新 {result['updated_stocks']} 条，"
        f"回填字段 {result.get('filled_blank_fields', 0)} 个，"
        f"刷新字段 {result.get('refreshed_fields', 0)} 个"
    )


def _log_sync_result(result):
    """记录同步结果

    Args:
        result: 结果字典
    """
    logger.info("=" * 60)
    logger.info("同步结果统计:")
    logger.info(f"  通达信股票总数: {result['total_tdx_stocks']}")
    logger.info(f"  数据库已有股票: {result['existing_stocks']}")
    logger.info(f"  新增股票数量: {result['inserted_stocks']}")
    logger.info(f"  更新股票数量: {result['updated_stocks']}")
    logger.info(f"  回填空白字段数量: {result.get('filled_blank_fields', 0)}")
    logger.info(f"  刷新已有字段数量: {result.get('refreshed_fields', 0)}")
    logger.info(f"  清理非股票记录数量: {result['deleted_non_stock_rows']}")
    logger.info(f"  失败股票数量: {len(result['failed_stocks'])}")
    if result['failed_stocks']:
        logger.warning(f"  失败股票列表: {result['failed_stocks'][:10]}...")
    logger.info("=" * 60)


def _normalize_stock_status(status):
    """将外部状态值归一化到 L/D/P。"""
    raw = str(status or '').strip().upper()
    if raw in {'L', '1', 'LISTED'}:
        return 'L'
    if raw in {'D', '0', 'DELIST', 'DELISTED'}:
        return 'D'
    if raw in {'P', 'SUSPEND', 'PAUSE'}:
        return 'P'
    return 'L'


def _infer_stock_market_from_ts_code(ts_code):
    """根据 ts_code 推断市场板块。"""
    if not ts_code or '.' not in ts_code:
        return ''

    symbol, market = ts_code.split('.', 1)
    market = market.upper()

    if market == 'BJ':
        return '北交所'
    if market == 'SH':
        if symbol.startswith('688'):
            return '科创板'
        return '主板'
    if market == 'SZ':
        if symbol.startswith('300'):
            return '创业板'
        if symbol.startswith('002'):
            return '中小板'
        return '主板'
    return ''


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


def _update_stock_text_field(row, field_name, source_value, result, max_len=None):
    """更新股票文本字段；空值视为回填，非空且变化视为刷新。"""
    new_value = _normalize_text_value(source_value, max_len)
    if not new_value:
        return False

    current_value = _normalize_text_value(getattr(row, field_name, None), max_len)
    if current_value == new_value:
        return False

    setattr(row, field_name, new_value)
    if current_value:
        result['refreshed_fields'] += 1
    else:
        result['filled_blank_fields'] += 1
    return True


def _update_stock_date_field(row, field_name, source_value, result):
    """更新股票日期字段；空值视为回填，非空且变化视为刷新。"""
    new_value = _normalize_date_value(source_value)
    if new_value is None:
        return False

    current_value = _normalize_date_value(getattr(row, field_name, None))
    if current_value == new_value:
        return False

    setattr(row, field_name, new_value)
    if current_value is None:
        result['filled_blank_fields'] += 1
    else:
        result['refreshed_fields'] += 1
    return True


@dataclass
class StockSyncContext:
    """股票同步上下文"""
    config: object
    db_manager: object
    result: dict
    session: object = None
    tdx_handler: object = None
    baostock_handler: object = None
    akshare_handler: object = None
    existing_codes: set = None
    new_codes: list = None
    tdx_stock_codes: list = None
    merged_basic: dict = None


def sync_tdx_stock_to_database(config, db_manager):
    """
    从通达信数据源同步股票信息到数据库

    工作流程：
    1. 从通达信数据源提取所有股票代码
    2. 与数据库 stock_basic 表进行匹配比较
    3. 对于通达信有但数据库没有的股票：
       a. 使用 baostock 和 akshare API 获取完整股票信息
       b. 收集所有相关的财务和市场数据字段
       c. 将完整的股票信息插入 stock_basic 表

    Args:
        config: 配置对象
        db_manager: 数据库管理器实例

    Returns:
        dict: 同步结果统计
    """
    from src.data.akshare_handler import AkShareHandler
    from src.data.baostock_handler import BaostockHandler
    from src.data.tdx_handler import TdxHandler
    from src.database.models.stock import StockBasic

    result = {
        'total_tdx_stocks': 0,
        'existing_stocks': 0,
        'new_stocks': 0,
        'deleted_non_stock_rows': 0,
        'failed_stocks': [],
        'inserted_stocks': 0,
        'updated_stocks': 0,
        'filled_blank_fields': 0,
        'refreshed_fields': 0,
    }

    logger.info("=" * 60)
    logger.info("开始从通达信数据源同步股票信息到数据库")
    logger.info("=" * 60)

    session = None
    try:
        tdx_handler = TdxHandler(config, db_manager)

        raw_tdx_codes = tdx_handler.get_stock_list()
        tdx_stock_codes = [
            code for code in raw_tdx_codes
            if (not _is_index_ts_code(code)) and (not _is_fund_ts_code(code))
        ]
        result['total_tdx_stocks'] = len(tdx_stock_codes)
        logger.info(
            f"通达信原始代码数: {len(raw_tdx_codes)}，"
            f"过滤基金/指数后个股数: {len(tdx_stock_codes)}"
        )

        if not tdx_stock_codes:
            logger.warning("通达信数据源没有找到任何股票数据")
            return result

        session = db_manager.get_session()

        all_existing_rows = session.query(StockBasic).all()
        non_stock_rows = [
            row for row in all_existing_rows
            if _is_index_ts_code(row.ts_code) or _is_fund_ts_code(row.ts_code, row.name, row.market)
        ]
        for row in non_stock_rows:
            session.delete(row)
        result['deleted_non_stock_rows'] = len(non_stock_rows)
        if non_stock_rows:
            logger.info(f"已从 stock_basic 清理 {len(non_stock_rows)} 条基金/指数记录")

        non_stock_code_set = {row.ts_code for row in non_stock_rows}
        existing_codes = {row.ts_code for row in all_existing_rows if row.ts_code not in non_stock_code_set}
        result['existing_stocks'] = len(existing_codes)
        logger.info(f"数据库 stock_basic 表已有 {len(existing_codes)} 只股票")

        new_codes = [code for code in tdx_stock_codes if code not in existing_codes]
        result['new_stocks'] = len(new_codes)
        logger.info(f"发现 {len(new_codes)} 只新股票需要添加到数据库")

        baostock_handler = BaostockHandler(config, db_manager)
        akshare_handler = AkShareHandler(config, db_manager)

        baostock_basic, akshare_basic, baostock_industry_map = _fetch_external_stock_basic(
            baostock_handler, akshare_handler
        )

        # 构建全量映射（不排除existing），用于新增与存量回填两阶段。
        merged_basic = merge_stock_basic(baostock_basic, akshare_basic, set(), tdx_stock_codes)

        # 行业字段优先采用 Baostock 行业映射（覆盖空值或占位值）
        if baostock_industry_map:
            for ts_code, industry in baostock_industry_map.items():
                if ts_code not in merged_basic:
                    symbol = ts_code.split('.')[0] if '.' in ts_code else ts_code
                    merged_basic[ts_code] = {
                        'symbol': symbol,
                        'name': '',
                        'area': '',
                        'industry': industry,
                        'market': '',
                        'list_date': None,
                        'status': 'L',
                        'source': 'baostock_industry'
                    }
                else:
                    existing_industry = str(merged_basic[ts_code].get('industry', '') or '').strip()
                    if (not existing_industry) or existing_industry in {'未知', '--', 'N/A'}:
                        merged_basic[ts_code]['industry'] = industry

        # 若行业字段仍缺失较多，使用 AkShare 行业板块成分兜底
        missing_industry_count = 0
        for ts_code in tdx_stock_codes:
            industry_text = str(merged_basic.get(ts_code, {}).get('industry', '') or '').strip()
            if not industry_text:
                missing_industry_count += 1

        if missing_industry_count > 0:
            logger.info(f"仍有 {missing_industry_count} 只股票缺少行业字段，尝试 AkShare 行业成分兜底")
            akshare_industry_map = _fetch_akshare_industry_map(tdx_stock_codes)
            if akshare_industry_map:
                for ts_code, industry in akshare_industry_map.items():
                    if ts_code not in merged_basic:
                        symbol = ts_code.split('.')[0] if '.' in ts_code else ts_code
                        merged_basic[ts_code] = {
                            'symbol': symbol,
                            'name': '',
                            'area': '',
                            'industry': industry,
                            'market': '',
                            'list_date': None,
                            'status': 'L',
                            'source': 'akshare_industry'
                        }
                    else:
                        existing_industry = str(merged_basic[ts_code].get('industry', '') or '').strip()
                        if not existing_industry:
                            merged_basic[ts_code]['industry'] = industry
                logger.info(f"AkShare 行业兜底完成，补充映射 {len(akshare_industry_map)} 只股票")

        if new_codes:
            ctx = StockBatchContext(
                new_codes, merged_basic, existing_codes, session,
                baostock_handler, akshare_handler, result
            )
            _process_single_stock_batch(ctx)
        else:
            logger.info("所有通达信股票都已存在，跳过新增阶段，继续执行存量回填")

        # 第二阶段：回填并刷新 stock_basic 存量信息
        all_stocks = session.query(StockBasic).all()
        for row in all_stocks:
            try:
                if _is_index_ts_code(row.ts_code) or _is_fund_ts_code(row.ts_code, row.name, row.market):
                    continue

                source_info = merged_basic.get(row.ts_code) or {}

                changed = False
                source_name = str(source_info.get('name', '') or '').strip()
                source_area = str(source_info.get('area', '') or '').strip()
                source_industry = str(source_info.get('industry', '') or '').strip()
                source_market = str(source_info.get('market', '') or '').strip()
                source_list_date = source_info.get('list_date')
                source_delist_date = source_info.get('delist_date')

                raw_status = str(source_info.get('status', '') or '').strip()
                source_status = _normalize_stock_status(raw_status) if raw_status else ''

                # 名称保守更新：空值、占位名时更新为外部值
                current_name = str(row.name or '').strip()
                if source_name and (
                    (not current_name)
                    or current_name.startswith('股票')
                    or current_name in {'未知', '--', 'N/A'}
                ):
                    if _update_stock_text_field(row, 'name', source_name, result, max_len=50):
                        changed = True

                # area / industry / market 允许外部源刷新已有值
                if _update_stock_text_field(row, 'area', source_area, result, max_len=20):
                    changed = True
                if _update_stock_text_field(row, 'industry', source_industry, result, max_len=50):
                    changed = True
                if _update_stock_text_field(row, 'market', source_market, result, max_len=20):
                    changed = True

                inferred_market = _infer_stock_market_from_ts_code(row.ts_code)
                if inferred_market and not source_market:
                    if _update_stock_text_field(row, 'market', inferred_market, result, max_len=20):
                        changed = True

                if _update_stock_date_field(row, 'list_date', source_list_date, result):
                    changed = True

                if not row.list_date:
                    first_trade_date, _ = _extract_tdx_first_last_trade_date(config, row.ts_code)
                    if _update_stock_date_field(row, 'list_date', first_trade_date, result):
                        changed = True

                # 兜底：若仍缺少上市日期，尝试用 stock_daily 最早交易日回填
                if not row.list_date:
                    try:
                        from sqlalchemy import func

                        earliest_trade_date = (
                            session.query(func.min(StockDaily.trade_date))
                            .filter(StockDaily.ts_code == row.ts_code)
                            .scalar()
                        )
                        if _update_stock_date_field(row, 'list_date', earliest_trade_date, result):
                            changed = True
                    except Exception as trade_date_e:
                        logger.debug(f"从 stock_daily 回填上市日期失败 {row.ts_code}: {trade_date_e}")

                if _update_stock_date_field(row, 'delist_date', source_delist_date, result):
                    changed = True

                if not row.status and source_status:
                    row.status = source_status
                    changed = True
                    result['filled_blank_fields'] += 1
                elif source_status and row.status != source_status:
                    row.status = source_status
                    changed = True
                    result['refreshed_fields'] += 1

                if changed:
                    result['updated_stocks'] += 1

            except Exception as backfill_e:
                logger.exception(f"回填股票 {row.ts_code} 信息失败: {backfill_e}")
                result['failed_stocks'].append(row.ts_code)

        _finalize_database_commit(session, result)

    except Exception as e:
        logger.exception(f"同步股票信息失败: {e}")
        if session:
            session.rollback()

    _log_sync_result(result)

    return result


def _build_tdx_market_map(tdx_stock_codes):
    """从通达信股票代码中提取市场信息

    Args:
        tdx_stock_codes: 通达信股票代码列表

    Returns:
        dict: 股票代码到市场信息的映射
    """
    tdx_market_map = {}
    if tdx_stock_codes:
        for ts_code in tdx_stock_codes:
            if '.' in ts_code:
                symbol, market = ts_code.split('.')
                tdx_market_map[symbol] = market
    return tdx_market_map


def _parse_baostock_code(code, tdx_market_map):
    """解析Baostock格式的股票代码

    Args:
        code: 原始股票代码
        tdx_market_map: 通达信市场信息映射

    Returns:
        str or None: 解析后的股票代码，解析失败返回None
    """
    if not code:
        return None

    symbol = code

    if '.' in code:
        parts = code.split('.')
        if len(parts) == 2:
            exchange, symbol = parts
            if exchange == 'sh':
                return f"{symbol}.SH"
            elif exchange == 'sz':
                return f"{symbol}.SZ"
            elif exchange == 'bj':
                return f"{symbol}.BJ"
        return None

    if code in tdx_market_map:
        market = tdx_market_map[code]
        return f"{code}.{market}"

    if code.startswith('6'):
        return f"{code}.SH"
    elif code.startswith('8') and len(code) == 6:
        num = int(code)
        if 800000 <= num <= 899999:
            return f"{code}.BJ"
        return f"{code}.SZ"
    elif code.startswith('92') and len(code) == 6:
        num = int(code)
        if 920000 <= num <= 920999:
            return f"{code}.BJ"
        return f"{code}.SZ"

    return f"{code}.SZ"


def _parse_akshare_code(code, tdx_market_map):
    """解析AkShare格式的股票代码

    Args:
        code: 原始股票代码
        tdx_market_map: 通达信市场信息映射

    Returns:
        str or None: 解析后的股票代码，解析失败返回None
    """
    if not code:
        return None

    symbol = code

    if code in tdx_market_map:
        market = tdx_market_map[code]
        return f"{code}.{market}"

    if code.startswith('6'):
        return f"{code}.SH"
    elif code.startswith('92') and len(code) == 6:
        num = int(code)
        if 920000 <= num <= 920999:
            return f"{code}.BJ"
        return f"{code}.SZ"
    elif code.startswith('8') and len(code) == 6:
        num = int(code)
        if 800000 <= num <= 899999:
            return f"{code}.BJ"
        return f"{code}.SZ"
    elif code.startswith('4') and len(code) == 6:
        return f"{code}.BJ"
    elif code.startswith('9'):
        return f"{code}.BJ"
    elif len(code) == 6:
        return f"{code}.SZ"

    return None


def _build_stock_info(ts_code, symbol, row, source):
    """构建股票信息字典

    Args:
        ts_code: 股票代码
        symbol: 股票符号
        row: 原始数据行
        source: 数据来源

    Returns:
        dict: 股票信息字典
    """
    name = (
        row.get('code_name')
        or row.get('name')
        or row.get('名称')
        or ''
    )
    area = (
        row.get('area')
        or row.get('region')
        or row.get('地域')
        or row.get('地区')
        or ''
    )
    industry = (
        row.get('industry')
        or row.get('所属行业')
        or row.get('行业')
        or ''
    )
    market = (
        row.get('market')
        or row.get('市场')
        or ''
    )
    list_date = row.get('list_date') or row.get('ipoDate') or row.get('上市日期')
    delist_date = row.get('delist_date') or row.get('outDate') or row.get('退市日期')
    status = row.get('status') or row.get('上市状态') or 'L'

    return {
        'symbol': symbol,
        'name': name,
        'area': area,
        'industry': industry,
        'market': market,
        'list_date': list_date,
        'delist_date': delist_date,
        'status': status,
        'source': source
    }


def merge_stock_basic(baostock_df, akshare_df, existing_codes=None, tdx_stock_codes=None):
    """
    合并 Baostock 和 AkShare 的股票基本信息

    Args:
        baostock_df: Baostock 股票信息 DataFrame
        akshare_df: AkShare 股票信息 DataFrame
        existing_codes: 已存在的股票代码集合（用于过滤）
        tdx_stock_codes: 通达信股票代码列表，包含市场信息（如 600000.SH、000001.SZ、920181.BJ）

    Returns:
        dict: 股票代码到信息的映射
    """
    merged = {}

    if existing_codes is None:
        existing_codes = set()

    tdx_market_map = _build_tdx_market_map(tdx_stock_codes)

    if baostock_df is not None and not baostock_df.is_empty():
        for row in baostock_df.iter_rows(named=True):
            code = row.get('code', '')
            ts_code = _parse_baostock_code(code, tdx_market_map)

            if ts_code is None or ts_code in existing_codes:
                continue

            merged[ts_code] = _build_stock_info(ts_code, code, row, 'baostock')

    if akshare_df is not None and not akshare_df.is_empty():
        for row in akshare_df.iter_rows(named=True):
            code = row.get('code') or row.get('代码') or row.get('symbol') or ''
            ts_code = _parse_akshare_code(code, tdx_market_map)

            if ts_code is None or ts_code in existing_codes:
                continue

            if ts_code in merged:
                merged_info = _build_stock_info(ts_code, code, row, 'akshare')
                merged[ts_code].update({
                    'name': merged_info.get('name', merged[ts_code].get('name', '')),
                    'area': merged_info.get('area', merged[ts_code].get('area', '')),
                    'industry': merged_info.get('industry', merged[ts_code].get('industry', '')),
                    'market': merged_info.get('market', merged[ts_code].get('market', '')),
                    'list_date': merged_info.get('list_date', merged[ts_code].get('list_date')),
                    'delist_date': merged_info.get('delist_date', merged[ts_code].get('delist_date')),
                    'status': merged_info.get('status', merged[ts_code].get('status', 'L')),
                })
            else:
                merged_info = _build_stock_info(ts_code, code, row, 'akshare')
                merged[ts_code] = {
                    'symbol': code,
                    'name': merged_info.get('name', ''),
                    'area': merged_info.get('area', ''),
                    'industry': merged_info.get('industry', ''),
                    'market': merged_info.get('market', ''),
                    'list_date': merged_info.get('list_date'),
                    'delist_date': merged_info.get('delist_date'),
                    'status': merged_info.get('status', 'L'),
                    'source': 'akshare'
                }

    logger.info(f"合并后共有 {len(merged)} 条股票基本信息（已过滤已存在的 {len(existing_codes)} 条）")
    return merged


def get_stock_info_from_apis(ts_code, merged_basic, baostock_handler, akshare_handler):
    """
    从 API 获取单个股票的详细信息
    
    Args:
        ts_code: 股票代码 (如 600000.SH)
        merged_basic: 合并后的股票基本信息
        baostock_handler: Baostock 处理器
        akshare_handler: AkShare 处理器
        
    Returns:
        dict: 股票信息字典
    """
    import polars as pl
    
    symbol = ts_code.split('.')[0]
    
    if ts_code in merged_basic:
        info = merged_basic[ts_code].copy()
        
        # 确保返回的ts_code与输入的ts_code一致
        info['ts_code'] = ts_code
        
        if info.get('list_date') and isinstance(info['list_date'], str):
            try:
                from datetime import datetime
                info['list_date'] = datetime.strptime(info['list_date'], '%Y-%m-%d').date()
            except:
                info['list_date'] = None
        
        return info
    
    for mapped_code, info in merged_basic.items():
        if info.get('symbol') == symbol:
            result = info.copy()
            # 确保返回的ts_code与输入的ts_code一致
            result['ts_code'] = ts_code
            return result
    
    logger.warning(f"在 Baostock/AkShare 数据中未找到股票 {ts_code} 的信息")
    if symbol.startswith('880'):
        # 880开头的股票是行业指数，使用更有意义的名称
        return {
            'ts_code': ts_code,
            'symbol': symbol,
            'name': f'行业指数{symbol}',
            'status': 'L'
        }
    elif symbol.startswith('92') and len(symbol) == 6:
        # 920开头的股票是北交所新股
        return {
            'ts_code': ts_code,
            'symbol': symbol,
            'name': f'北交所新股{symbol}',
            'status': 'L'
        }
    elif symbol.startswith('8') and len(symbol) == 6:
        # 8开头的股票是北交所股票
        return {
            'ts_code': ts_code,
            'symbol': symbol,
            'name': f'北交所股票{symbol}',
            'status': 'L'
        }
    elif symbol.startswith('51') or symbol.startswith('58'):
        # 上海 ETF
        return {
            'ts_code': ts_code,
            'symbol': symbol,
            'name': f'ETF{symbol}',
            'status': 'L'
        }
    elif symbol.startswith('15') or symbol.startswith('16'):
        # 深圳 ETF
        return {
            'ts_code': ts_code,
            'symbol': symbol,
            'name': f'ETF{symbol}',
            'status': 'L'
        }
    else:
        return {
            'ts_code': ts_code,
            'symbol': symbol,
            'name': f'股票{symbol}',
            'status': 'L'
        }


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


def _normalize_baostock_index_code(code):
    """将 Baostock 指数代码标准化为 000001.SH 格式。"""
    if not code:
        return None

    code = str(code).strip()
    if '.' in code:
        part1, part2 = code.split('.', 1)
        part1 = part1.lower()
        part2 = part2.strip()
        if part1 in {'sh', 'sz', 'bj'} and part2.isdigit() and len(part2) == 6:
            return f"{part2}.{part1.upper()}"

    if '.' in code:
        symbol, market = code.split('.', 1)
        market = market.upper()
        if symbol.isdigit() and len(symbol) == 6 and market in {'SH', 'SZ', 'BJ'}:
            return f"{symbol}.{market}"

    return None


def _normalize_akshare_index_code(code):
    """将 AkShare 指数代码标准化为 000001.SH 格式。"""
    if code is None:
        return None

    code = str(code).strip()
    if not code:
        return None

    if '.' in code:
        symbol, market = code.split('.', 1)
        market = market.upper()
        if symbol.isdigit() and len(symbol) == 6 and market in {'SH', 'SZ', 'BJ'}:
            return f"{symbol}.{market}"
        return None

    if not code.isdigit() or len(code) != 6:
        return None

    if code.startswith('000'):
        return f"{code}.SH"
    if code.startswith('399'):
        return f"{code}.SZ"
    if code.startswith('884') or code.startswith('899'):
        return f"{code}.BJ"
    return None


def _normalize_index_ts_code(code):
    """统一规范指数代码格式为 000001.SH。"""
    return _normalize_baostock_index_code(code) or _normalize_akshare_index_code(code)


FORCED_INDEX_NAME_MAP = {
    '899050.BJ': '北证50',
    '899601.BJ': '北证专精特新',
}


def _parse_index_date(value):
    """解析指数日期字段，返回 date 或 None。"""
    from datetime import datetime

    if value is None:
        return None

    text = str(value).strip()
    if not text or text in {'None', 'nan', 'NaT', '0000-00-00'}:
        return None

    for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%Y%m%d'):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _infer_index_meta(ts_code, index_name):
    """根据代码和名称推断指数元信息。"""
    market = ts_code.split('.')[-1].upper() if ts_code and '.' in ts_code else ''
    if market == 'SH':
        publisher = '上交所'
    elif market == 'SZ':
        publisher = '深交所'
    elif market == 'BJ':
        publisher = '北交所'
    else:
        publisher = '交易所'

    name = (index_name or '').strip()
    if '行业' in name:
        index_type = '行业指数'
        category = '行业'
    elif '主题' in name:
        index_type = '主题指数'
        category = '主题'
    elif '红利' in name:
        index_type = '策略指数'
        category = '红利'
    elif '创业板' in name:
        index_type = '板块指数'
        category = '创业板'
    elif '科创' in name:
        index_type = '板块指数'
        category = '科创板'
    elif '北证' in name:
        index_type = '板块指数'
        category = '北交所'
    elif '中证' in name or '沪深' in name or '上证' in name or '深证' in name:
        index_type = '综合指数'
        category = '宽基'
    else:
        index_type = '综合指数'
        category = '通用'

    return {
        'publisher': publisher,
        'index_type': index_type,
        'category': category,
        'weight_rule': '市值加权',
    }


def _collect_external_index_name_map(config):
    """收集外部数据源的指数信息映射。"""
    import baostock as bs
    import polars as pl

    from src.data.akshare_handler import AkShareHandler
    from src.data.baostock_handler import BaostockHandler

    baostock_handler = BaostockHandler(config, None)
    akshare_handler = AkShareHandler(config, None)

    merged_info_map = {}

    def _get_or_create(ts_code):
        if ts_code not in merged_info_map:
            merged_info_map[ts_code] = {
                'name': '',
                'list_date': None,
                'base_date': None,
            }
        return merged_info_map[ts_code]

    logger.info("开始从 Baostock 获取指数基本信息...")
    try:
        if baostock_handler._ensure_baostock_login():
            rs = bs.query_stock_basic(code_name="")
            baostock_pd = rs.get_data()
            baostock_df = pl.from_pandas(baostock_pd)
            if not baostock_df.is_empty():
                for row in baostock_df.iter_rows(named=True):
                    ts_code = _normalize_baostock_index_code(row.get('code'))
                    name = (row.get('code_name') or '').strip()
                    if ts_code and name and _is_index_ts_code(ts_code):
                        item = _get_or_create(ts_code)
                        item['name'] = name

                        ipo_date = _parse_index_date(row.get('ipoDate'))
                        if ipo_date and (item['list_date'] is None or ipo_date < item['list_date']):
                            item['list_date'] = ipo_date
                        if ipo_date and (item['base_date'] is None or ipo_date < item['base_date']):
                            item['base_date'] = ipo_date
            logger.info(f"Baostock指数信息映射数量: {len(merged_info_map)}")
    except Exception as e:
        logger.warning(f"从 Baostock 获取指数基本信息失败: {e}")

    logger.info("开始从 AkShare 获取指数基本信息...")
    try:
        akshare_df = akshare_handler.update_index_basic()
        if akshare_df is not None and not akshare_df.is_empty():
            ak_count = 0
            for row in akshare_df.iter_rows(named=True):
                raw_code = row.get('代码', row.get('index_code'))
                raw_name = row.get('名称', row.get('display_name'))
                ts_code = _normalize_akshare_index_code(raw_code)
                name = (raw_name or '').strip() if raw_name is not None else ''
                if ts_code and name and _is_index_ts_code(ts_code):
                    item = _get_or_create(ts_code)
                    if not item['name']:
                        item['name'] = name

                    publish_date = _parse_index_date(row.get('publish_date', row.get('发布日期')))
                    if publish_date and (item['list_date'] is None or publish_date < item['list_date']):
                        item['list_date'] = publish_date
                    if publish_date and (item['base_date'] is None or publish_date < item['base_date']):
                        item['base_date'] = publish_date
                    ak_count += 1
            logger.info(f"AkShare可用指数名称映射数量: {ak_count}")
    except Exception as e:
        logger.warning(f"从 AkShare 获取指数基本信息失败: {e}")
    finally:
        try:
            baostock_handler._logout_baostock()
        except Exception:
            pass

    return merged_info_map


def sync_tdx_index_to_database(config, db_manager):
    """以通达信指数代码为基准同步 index_basic。"""
    from src.data.tdx_handler import TdxHandler
    from src.database.models.index import IndexBasic

    result = {
        'total_tdx_indexes': 0,
        'updated_indexes': 0,
        'inserted_indexes': 0,
        'deleted_indexes': 0,
        'unchanged_indexes': 0,
        'fallback_named_indexes': 0,
        'filled_blank_fields': 0,
        'failed_indexes': []
    }

    logger.info("=" * 60)
    logger.info("开始以通达信指数代码为基准同步 index_basic")
    logger.info("=" * 60)

    session = None
    try:
        tdx_handler = TdxHandler(config, db_manager)
        tdx_codes = tdx_handler.get_stock_list()
        tdx_index_codes = sorted({code for code in tdx_codes if _is_index_ts_code(code)})
        tdx_index_code_set = set(tdx_index_codes)
        result['total_tdx_indexes'] = len(tdx_index_codes)
        logger.info(f"通达信指数代码数量: {len(tdx_index_codes)}")

        if not tdx_index_codes:
            logger.warning("通达信未发现指数代码，跳过同步")
            return result

        external_info_map = _collect_external_index_name_map(config)
        logger.info(f"外部数据源可用指数信息映射总数: {len(external_info_map)}")

        session = db_manager.get_session()

        for i, ts_code in enumerate(tdx_index_codes):
            try:
                existing = session.query(IndexBasic).filter(IndexBasic.ts_code == ts_code).first()
                source_info = external_info_map.get(ts_code, {})
                source_name = FORCED_INDEX_NAME_MAP.get(ts_code, str(source_info.get('name', '') or '').strip())
                if not source_name:
                    source_name = f"指数{ts_code.split('.')[0]}"
                    result['fallback_named_indexes'] += 1

                market = ts_code.split('.')[-1]
                source_list_date = source_info.get('list_date')
                source_base_date = source_info.get('base_date')
                inferred = _infer_index_meta(ts_code, source_name)
                default_desc = f"{source_name}({ts_code})，通达信基准，Baostock/AkShare补全"

                if existing:
                    changed = False
                    if existing.name != source_name:
                        existing.name = source_name
                        changed = True
                    if existing.market != market:
                        existing.market = market
                        changed = True

                    # 仅回填空白列，避免覆盖人工维护数据
                    if not existing.publisher and inferred['publisher']:
                        existing.publisher = inferred['publisher']
                        changed = True
                        result['filled_blank_fields'] += 1
                    if not existing.index_type and inferred['index_type']:
                        existing.index_type = inferred['index_type']
                        changed = True
                        result['filled_blank_fields'] += 1
                    if not existing.category and inferred['category']:
                        existing.category = inferred['category']
                        changed = True
                        result['filled_blank_fields'] += 1
                    if not existing.weight_rule and inferred['weight_rule']:
                        existing.weight_rule = inferred['weight_rule']
                        changed = True
                        result['filled_blank_fields'] += 1
                    if not existing.list_date and source_list_date:
                        existing.list_date = source_list_date
                        changed = True
                        result['filled_blank_fields'] += 1
                    if not existing.base_date and source_base_date:
                        existing.base_date = source_base_date
                        changed = True
                        result['filled_blank_fields'] += 1
                    if not existing.desc:
                        existing.desc = default_desc
                        changed = True
                        result['filled_blank_fields'] += 1

                    if changed:
                        result['updated_indexes'] += 1
                    else:
                        result['unchanged_indexes'] += 1
                else:
                    new_index = IndexBasic(
                        ts_code=ts_code,
                        name=source_name,
                        market=market,
                        publisher=inferred['publisher'],
                        index_type=inferred['index_type'],
                        category=inferred['category'],
                        list_date=source_list_date,
                        base_date=source_base_date,
                        weight_rule=inferred['weight_rule'],
                        desc=default_desc,
                    )
                    session.add(new_index)
                    result['inserted_indexes'] += 1

                if (i + 1) % 100 == 0:
                    session.commit()
                    logger.info(f"已处理 {i + 1}/{len(tdx_index_codes)} 个指数")

            except Exception as row_e:
                logger.exception(f"处理指数 {ts_code} 失败: {row_e}")
                result['failed_indexes'].append(ts_code)

        # 第二阶段：对 index_basic 存量数据执行空白字段回填（不覆盖已有值）
        all_index_rows = session.query(IndexBasic).all()
        for row in all_index_rows:
            try:
                changed = False

                normalized_code = _normalize_index_ts_code(row.ts_code) or row.ts_code
                source_info = external_info_map.get(normalized_code, {})
                forced_name = FORCED_INDEX_NAME_MAP.get(normalized_code, '')

                if not row.name:
                    source_name = forced_name or str(source_info.get('name', '') or '').strip()
                    if not source_name and normalized_code and '.' in normalized_code:
                        source_name = f"指数{normalized_code.split('.')[0]}"
                    if source_name:
                        row.name = source_name
                        changed = True
                        result['filled_blank_fields'] += 1

                normalized_name = row.name or str(source_info.get('name', '') or '').strip()
                inferred = _infer_index_meta(normalized_code, normalized_name)

                if not row.market and normalized_code and '.' in normalized_code:
                    row.market = normalized_code.split('.')[-1]
                    changed = True
                    result['filled_blank_fields'] += 1
                if not row.publisher and inferred['publisher']:
                    row.publisher = inferred['publisher']
                    changed = True
                    result['filled_blank_fields'] += 1
                if not row.index_type and inferred['index_type']:
                    row.index_type = inferred['index_type']
                    changed = True
                    result['filled_blank_fields'] += 1
                if not row.category and inferred['category']:
                    row.category = inferred['category']
                    changed = True
                    result['filled_blank_fields'] += 1
                if not row.weight_rule and inferred['weight_rule']:
                    row.weight_rule = inferred['weight_rule']
                    changed = True
                    result['filled_blank_fields'] += 1

                source_list_date = source_info.get('list_date')
                source_base_date = source_info.get('base_date')
                if not row.list_date and source_list_date:
                    row.list_date = source_list_date
                    changed = True
                    result['filled_blank_fields'] += 1
                if not row.base_date and source_base_date:
                    row.base_date = source_base_date
                    changed = True
                    result['filled_blank_fields'] += 1
                if not row.desc:
                    safe_name = normalized_name or f"指数{normalized_code.split('.')[0]}" if normalized_code and '.' in normalized_code else '指数'
                    row.desc = f"{safe_name}({normalized_code})，通达信基准，Baostock/AkShare补全"
                    changed = True
                    result['filled_blank_fields'] += 1

                if changed and row.ts_code not in tdx_index_codes:
                    result['updated_indexes'] += 1

            except Exception as backfill_e:
                logger.exception(f"回填指数 {row.ts_code} 空白字段失败: {backfill_e}")
                result['failed_indexes'].append(row.ts_code)

        # 第三阶段：删除不在通达信指数代码白名单中的历史记录
        stale_rows = []
        for row in all_index_rows:
            normalized_code = _normalize_index_ts_code(row.ts_code)
            if not normalized_code or normalized_code not in tdx_index_code_set:
                stale_rows.append(row)

        for stale_row in stale_rows:
            session.delete(stale_row)
        result['deleted_indexes'] = len(stale_rows)
        if stale_rows:
            logger.info(f"已删除 {len(stale_rows)} 条不在通达信指数代码中的 index_basic 记录")

        session.commit()

    except Exception as e:
        logger.exception(f"同步指数信息失败: {e}")
        if session:
            session.rollback()
    finally:
        logger.info("=" * 60)
        logger.info("指数同步结果统计:")
        logger.info(f"  通达信指数总数: {result['total_tdx_indexes']}")
        logger.info(f"  新增指数数量: {result['inserted_indexes']}")
        logger.info(f"  删除指数数量: {result['deleted_indexes']}")
        logger.info(f"  更新指数数量: {result['updated_indexes']}")
        logger.info(f"  未变化数量: {result['unchanged_indexes']}")
        logger.info(f"  回退命名数量: {result['fallback_named_indexes']}")
        logger.info(f"  回填空白字段数量: {result['filled_blank_fields']}")
        logger.info(f"  失败数量: {len(result['failed_indexes'])}")
        if result['failed_indexes']:
            logger.warning(f"  失败列表(前10): {result['failed_indexes'][:10]}")
        logger.info("=" * 60)

    return result


def parse_args():
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(description='中国股市量化分析系统')
    parser.add_argument('--init-db', action='store_true', help='初始化数据库表')
    parser.add_argument('--update-stock', action='store_true', help='从通达信数据源同步股票信息到数据库')
    parser.add_argument('--update-index', action='store_true', help='以通达信指数代码为基准更新 index_basic（Baostock/AkShare 补全）')
    parser.add_argument('--update-fund', action='store_true', help='将 stock_basic 中基金迁移到 fund_basic 并补全信息')
    parser.add_argument('--plugins', action='store_true', help='显示已加载的插件')
    return parser.parse_args()


def _initialize_system(config):
    """初始化系统组件

    Args:
        config: 配置对象

    Returns:
        tuple: (plugin_manager, cpu_count)
    """
    import os

    setup_global_exception_handler()
    logger.info("全局异常处理器初始化成功")

    config_manager.start_watching(interval=5)
    logger.info("配置热加载功能已启动")

    setup_logger(config)
    logger.info("日志系统初始化成功")

    plugin_manager = PluginManager(config)
    logger.info("插件管理器初始化成功")

    loaded_count = plugin_manager.load_plugins()
    logger.info(f"插件加载完成，共加载 {loaded_count} 个插件")

    cpu_count = os.cpu_count() or 4
    os.environ['POLARS_MAX_THREADS'] = str(cpu_count)
    logger.info(f"已通过环境变量设置Polars线程池大小为 {cpu_count}")

    initialized_count = plugin_manager.initialize_plugins()
    logger.info(f"插件初始化完成，共初始化 {initialized_count} 个插件")

    return plugin_manager, cpu_count


def _display_plugins(plugin_manager):
    """显示已加载的插件

    Args:
        plugin_manager: 插件管理器
    """
    logger.info("已加载的插件列表:")
    for plugin_info in plugin_manager.get_all_plugin_info():
        logger.info(f"  - {plugin_info['name']} (v{plugin_info['version']}) - {plugin_info['description']}")


def _handle_update_stock_arg(config):
    """处理股票更新参数

    Args:
        config: 配置对象

    Returns:
        bool: 是否执行了股票更新
    """
    logger.info("检测到 --update-stock 参数，将执行股票信息同步")

    db_manager = None
    try:
        db_manager = DatabaseManager(config)
        db_manager.connect()
        logger.info("数据库连接成功")

        try:
            db_manager.create_tables()
            logger.info("数据库表创建/更新成功")
        except (OSError, RuntimeError) as table_e:
            logger.warning(f"创建数据库表时发生错误: {table_e}")

        sync_result = sync_tdx_stock_to_database(config, db_manager)

        from src.data.akshare_handler import AkShareHandler
        akshare_handler = AkShareHandler(config, db_manager)
        logger.info("开始更新 ETF 基本信息...")
        try:
            etf_result = akshare_handler.update_etf_basic()
            if etf_result is not None:
                logger.info(f"ETF 基本信息更新完成")
        except Exception as etf_e:
            logger.warning(f"更新 ETF 基本信息失败: {etf_e}")

        db_manager.cleanup()
        logger.info("数据库资源已清理")
        return True

    except (OSError, RuntimeError) as db_e:
        logger.error(f"数据库连接失败，无法执行同步: {db_e}")
        return True


def _handle_update_index_arg(config):
    """处理指数更新参数。"""
    logger.info("检测到 --update-index 参数，将执行指数信息同步")

    db_manager = None
    try:
        db_manager = DatabaseManager(config)
        db_manager.connect()
        logger.info("数据库连接成功")

        try:
            db_manager.create_tables()
            logger.info("数据库表创建/更新成功")
        except (OSError, RuntimeError) as table_e:
            logger.warning(f"创建数据库表时发生错误: {table_e}")

        sync_tdx_index_to_database(config, db_manager)
        db_manager.cleanup()
        logger.info("数据库资源已清理")
        return True

    except (OSError, RuntimeError) as db_e:
        logger.error(f"数据库连接失败，无法执行指数同步: {db_e}")
        return True


def _handle_update_fund_arg(config):
    """处理基金迁移参数。"""
    logger.info("检测到 --update-fund 参数，将执行基金迁移和补全")

    db_manager = None
    try:
        db_manager = DatabaseManager(config)
        db_manager.connect()
        logger.info("数据库连接成功")

        try:
            db_manager.create_tables()
            logger.info("数据库表创建/更新成功")
        except (OSError, RuntimeError) as table_e:
            logger.warning(f"创建数据库表时发生错误: {table_e}")

        sync_stock_fund_to_fund_basic(config, db_manager)
        db_manager.cleanup()
        logger.info("数据库资源已清理")
        return True

    except (OSError, RuntimeError) as db_e:
        logger.error(f"数据库连接失败，无法执行基金迁移: {db_e}")
        return True


def _initialize_data_manager(config, plugin_manager):
    """初始化数据管理器

    Args:
        config: 配置对象
        plugin_manager: 插件管理器

    Returns:
        DataManager: 数据管理器实例
    """
    db_manager = None
    try:
        db_manager = DatabaseManager(config)
        db_manager.connect()
        logger.info("数据库连接成功")

        try:
            db_manager.create_tables()
            logger.info("数据库表创建/更新成功")
        except (OSError, RuntimeError) as table_e:
            logger.warning(f"创建数据库表时发生错误: {table_e}")

        data_manager = DataManager(config, db_manager, plugin_manager)
        logger.info("数据管理器初始化成功")
    except (OSError, RuntimeError) as db_e:
        logger.warning(f"数据库连接失败，将以离线模式运行: {db_e}")
        data_manager = DataManager(config, None, plugin_manager)
        logger.info("数据管理器（离线模式）初始化成功")

    return data_manager


def _cleanup_system(plugin_manager, db_manager):
    """清理系统资源

    Args:
        plugin_manager: 插件管理器
        db_manager: 数据库管理器
    """
    try:
        global_memory_manager.stop_monitoring()
        memory_usage = global_memory_manager.get_memory_usage()
        logger.info(f"最终内存使用: {memory_usage['process_rss_mb']:.2f} MB ({memory_usage['process_percent']:.1f}%)")
    except Exception as e:
        logger.warning(f"停止内存监控时发生错误: {e}")

    publish(EventType.SYSTEM_SHUTDOWN)
    log_cache_stats()

    try:
        if db_manager:
            db_manager.cleanup()
    except (OSError, RuntimeError) as cleanup_e:
        logger.warning(f"清理数据库资源时发生错误: {cleanup_e}")

    if plugin_manager:
        plugin_manager.shutdown_plugins()

    config_manager.stop_watching()
    logger.info("配置热加载功能已停止")

    shutdown_event_bus()
    logger.info("事件总线已关闭")

    logger.info("系统已关闭")


def main():
    """主函数"""
    args = parse_args()

    plugin_manager = None
    db_manager = None

    try:
        config = get_config()
        logger.info("配置加载成功")

        plugin_manager, cpu_count = _initialize_system(config)

        if args.plugins:
            _display_plugins(plugin_manager)
            return

        if args.update_stock:
            _handle_update_stock_arg(config)
            return

        if args.update_index:
            _handle_update_index_arg(config)
            return

        if args.update_fund:
            _handle_update_fund_arg(config)
            return

        data_manager = _initialize_data_manager(config, plugin_manager)

        app = QApplication(sys.argv)
        app.setApplicationName("中国股市量化分析系统")

        ThemeManager.set_dark_theme(app)

        main_window = MainWindow(config, data_manager, plugin_manager)
        main_window.showMaximized()

        publish(EventType.SYSTEM_INIT, app=app, config=config, data_manager=data_manager)
        logger.info("中国股市量化分析系统启动成功")

        global_memory_manager.start_monitoring()
        logger.info("内存监控已启动")

        memory_usage = global_memory_manager.get_memory_usage()
        logger.info(f"初始内存使用: {memory_usage['process_rss_mb']:.2f} MB ({memory_usage['process_percent']:.1f}%)")

        log_cache_stats()

        sys.exit(app.exec())

    except (OSError, RuntimeError) as e:
        logger.exception(f"系统启动失败: {e}")
        sys.exit(1)
    finally:
        _cleanup_system(plugin_manager, db_manager)


def _is_fund_ts_code(ts_code, name=None, market=None):
    """判断是否为基金代码。"""
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

    if '基金' in mkt:
        return True
    if any(token in nm.upper() for token in ('ETF', 'LOF')) or ('基金' in nm):
        return True

    return False


def _infer_fund_type(symbol, name):
    """推断基金类型。"""
    nm = (name or '').upper()
    if 'ETF' in nm:
        return 'ETF'
    if 'LOF' in nm or symbol.startswith(('16', '18')):
        return 'LOF'
    return '基金'


def _parse_tdx_day_to_date(day_value):
    """将通达信 YYYYMMDD 整数转换为 date。"""
    from datetime import datetime

    try:
        text = str(int(day_value))
        if len(text) != 8:
            return None
        return datetime.strptime(text, '%Y%m%d').date()
    except Exception:
        return None


def _get_tdx_day_file_path(config, ts_code):
    """根据 ts_code 生成通达信日线文件路径。"""
    if not ts_code or '.' not in ts_code:
        return None

    symbol, market = ts_code.split('.', 1)
    market = market.upper()
    if market not in {'SH', 'SZ', 'BJ'}:
        return None

    market_prefix = market.lower()
    return Path(config.data.tdx_data_path) / market_prefix / 'lday' / f"{market_prefix}{symbol}.day"


def _extract_tdx_first_last_trade_date(config, ts_code):
    """读取通达信日线文件首末交易日。"""
    import struct

    day_file = _get_tdx_day_file_path(config, ts_code)
    if not day_file or not day_file.exists():
        return None, None

    try:
        with open(day_file, 'rb') as f:
            f.seek(0, 2)
            size = f.tell()
            if size < 32:
                return None, None

            f.seek(0)
            first_record = f.read(32)
            first_raw = struct.unpack('I', first_record[0:4])[0]

            f.seek(size - 32)
            last_record = f.read(32)
            last_raw = struct.unpack('I', last_record[0:4])[0]

        return _parse_tdx_day_to_date(first_raw), _parse_tdx_day_to_date(last_raw)
    except Exception:
        return None, None


def sync_stock_fund_to_fund_basic(config, db_manager):
    """将 stock_basic 中基金记录迁移到 fund_basic，并补全信息。"""
    from src.data.akshare_handler import AkShareHandler
    from src.database.models.fund import FundBasic
    from src.database.models.stock import StockBasic

    result = {
        'stock_fund_candidates': 0,
        'inserted_funds': 0,
        'updated_funds': 0,
        'deleted_from_stock_basic': 0,
        'akshare_enriched': 0,
        'list_date_filled': 0,
        'delist_date_filled': 0,
        'failed': 0,
    }

    logger.info("=" * 60)
    logger.info("开始执行基金迁移: stock_basic -> fund_basic")
    logger.info("=" * 60)

    session = None
    try:
        session = db_manager.get_session()

        all_stocks = session.query(StockBasic).all()
        fund_stocks = [
            row for row in all_stocks
            if _is_fund_ts_code(row.ts_code, row.name, row.market)
        ]
        result['stock_fund_candidates'] = len(fund_stocks)
        logger.info(f"识别到 {len(fund_stocks)} 条基金候选记录")

        existing_fund_map = {f.ts_code: f for f in session.query(FundBasic).all()}

        for stock in fund_stocks:
            try:
                fund = existing_fund_map.get(stock.ts_code)
                symbol = (stock.symbol or stock.ts_code.split('.')[0])[:9]
                name = (stock.name or symbol)[:20]
                inferred_type = _infer_fund_type(symbol, name)
                inferred_market = stock.ts_code.split('.')[-1]

                if fund:
                    changed = False
                    if not fund.symbol and symbol:
                        fund.symbol = symbol
                        changed = True
                    if (not fund.name or fund.name.startswith('股票')) and name:
                        fund.name = name
                        changed = True
                    if not fund.fund_type:
                        fund.fund_type = inferred_type
                        changed = True
                    if not fund.market:
                        fund.market = inferred_market
                        changed = True
                    if not fund.list_date and stock.list_date:
                        fund.list_date = stock.list_date
                        changed = True
                    if not fund.delist_date and stock.delist_date:
                        fund.delist_date = stock.delist_date
                        changed = True
                    if not fund.status:
                        fund.status = stock.status or 'L'
                        changed = True
                    if changed:
                        result['updated_funds'] += 1
                else:
                    fund = FundBasic(
                        ts_code=stock.ts_code,
                        symbol=symbol,
                        name=name,
                        fund_type=inferred_type,
                        market=inferred_market,
                        list_date=stock.list_date,
                        delist_date=stock.delist_date,
                        status=stock.status or 'L',
                    )
                    session.add(fund)
                    existing_fund_map[stock.ts_code] = fund
                    result['inserted_funds'] += 1

            except Exception as row_e:
                logger.exception(f"迁移基金记录失败 {stock.ts_code}: {row_e}")
                result['failed'] += 1

        # 使用 AkShare ETF 列表补全名称/类型（离线模式只取数不落库）
        try:
            akshare_handler = AkShareHandler(config, None)
            etf_df = akshare_handler.update_etf_basic()
            if etf_df is not None and not etf_df.is_empty():
                for row in etf_df.iter_rows(named=True):
                    symbol = str(row.get('基金代码', '')).strip()
                    name = str(row.get('基金简称', '')).strip()
                    if not symbol:
                        continue

                    if symbol.startswith('51') or symbol.startswith('58'):
                        ts_code = f"{symbol}.SH"
                    elif symbol.startswith('15') or symbol.startswith('16'):
                        ts_code = f"{symbol}.SZ"
                    else:
                        continue

                    fund = existing_fund_map.get(ts_code)
                    if not fund:
                        fund = FundBasic(
                            ts_code=ts_code,
                            symbol=symbol[:9],
                            name=(name or symbol)[:20],
                            fund_type='ETF',
                            market=ts_code.split('.')[-1],
                            status='L',
                        )
                        session.add(fund)
                        existing_fund_map[ts_code] = fund
                        result['inserted_funds'] += 1
                        result['akshare_enriched'] += 1
                    else:
                        changed = False
                        if name and ((not fund.name) or fund.name.startswith('股票')):
                            fund.name = name[:20]
                            changed = True
                        if not fund.fund_type:
                            fund.fund_type = 'ETF'
                            changed = True
                        if changed:
                            result['updated_funds'] += 1
                            result['akshare_enriched'] += 1
        except Exception as e:
            logger.warning(f"使用 AkShare 补全基金信息失败: {e}")

        # 使用通达信日线数据补全日期信息：
        # list_date <- 首个交易日；delist_date 仅对退市状态补最后交易日。
        tdx_date_cache = {}
        all_funds = session.query(FundBasic).all()
        for fund in all_funds:
            try:
                status = str(fund.status or '').strip().upper()
                is_delisted = status in {'D', '0', 'DELIST', 'DELISTED'}

                need_list_date = fund.list_date is None
                need_delist_date = fund.delist_date is None and is_delisted
                if not need_list_date and not need_delist_date:
                    continue

                if fund.ts_code not in tdx_date_cache:
                    tdx_date_cache[fund.ts_code] = _extract_tdx_first_last_trade_date(config, fund.ts_code)

                first_trade_date, last_trade_date = tdx_date_cache[fund.ts_code]
                changed = False

                if need_list_date and first_trade_date:
                    fund.list_date = first_trade_date
                    result['list_date_filled'] += 1
                    changed = True

                if need_delist_date and last_trade_date:
                    fund.delist_date = last_trade_date
                    result['delist_date_filled'] += 1
                    changed = True

                if changed:
                    result['updated_funds'] += 1

            except Exception as date_fill_e:
                logger.exception(f"回填基金日期失败 {fund.ts_code}: {date_fill_e}")
                result['failed'] += 1

        # 从 stock_basic 删除已迁移基金，完成“移动”语义
        for stock in fund_stocks:
            session.delete(stock)
        result['deleted_from_stock_basic'] = len(fund_stocks)

        session.commit()

    except Exception as e:
        logger.exception(f"基金迁移失败: {e}")
        if session:
            session.rollback()
        result['failed'] += 1
    finally:
        logger.info("=" * 60)
        logger.info("基金迁移结果统计:")
        logger.info(f"  stock_basic 基金候选: {result['stock_fund_candidates']}")
        logger.info(f"  fund_basic 新增: {result['inserted_funds']}")
        logger.info(f"  fund_basic 更新: {result['updated_funds']}")
        logger.info(f"  AkShare 补全: {result['akshare_enriched']}")
        logger.info(f"  list_date 回填: {result['list_date_filled']}")
        logger.info(f"  delist_date 回填: {result['delist_date_filled']}")
        logger.info(f"  stock_basic 删除: {result['deleted_from_stock_basic']}")
        logger.info(f"  失败数: {result['failed']}")
        logger.info("=" * 60)

    return result


if __name__ == "__main__":
    main()
