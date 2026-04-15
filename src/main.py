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
        tuple: (baostock_basic, akshare_basic)
    """
    baostock_basic = None
    akshare_basic = None

    logger.info("开始从 Baostock 获取股票基本信息...")
    try:
        baostock_basic = baostock_handler.update_stock_basic()
        if baostock_basic is not None:
            logger.info(f"从 Baostock 获取到 {baostock_basic.height} 条股票信息")
    except Exception as e:
        logger.warning(f"从 Baostock 获取股票基本信息失败: {e}")

    logger.info("开始从 AkShare 获取股票基本信息...")
    try:
        akshare_basic = akshare_handler.update_stock_basic()
        if akshare_basic is not None:
            logger.info(f"从 AkShare 获取到 {akshare_basic.height} 条股票信息")
    except Exception as e:
        logger.warning(f"从 AkShare 获取股票基本信息失败: {e}")

    return baostock_basic, akshare_basic


def _create_stock_basic_from_info(ts_code, stock_info):
    """从股票信息创建StockBasic对象

    Args:
        ts_code: 股票代码
        stock_info: 股票信息字典

    Returns:
        StockBasic: 数据库模型对象
    """
    from src.database.models.stock import StockBasic

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
            ctx.result['updated_stocks'] += 1
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
    logger.info(f"股票信息同步完成，共新增 {result['updated_stocks']} 条记录")


def _log_sync_result(result):
    """记录同步结果

    Args:
        result: 结果字典
    """
    logger.info("=" * 60)
    logger.info("同步结果统计:")
    logger.info(f"  通达信股票总数: {result['total_tdx_stocks']}")
    logger.info(f"  数据库已有股票: {result['existing_stocks']}")
    logger.info(f"  新增股票数量: {result['updated_stocks']}")
    logger.info(f"  失败股票数量: {len(result['failed_stocks'])}")
    if result['failed_stocks']:
        logger.warning(f"  失败股票列表: {result['failed_stocks'][:10]}...")
    logger.info("=" * 60)


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
        'failed_stocks': [],
        'updated_stocks': 0
    }

    logger.info("=" * 60)
    logger.info("开始从通达信数据源同步股票信息到数据库")
    logger.info("=" * 60)

    session = None
    try:
        tdx_handler = TdxHandler(config, db_manager)

        tdx_stock_codes = tdx_handler.get_stock_list()
        result['total_tdx_stocks'] = len(tdx_stock_codes)
        logger.info(f"通达信数据源共有 {len(tdx_stock_codes)} 只股票")

        if not tdx_stock_codes:
            logger.warning("通达信数据源没有找到任何股票数据")
            return result

        session = db_manager.get_session()

        existing_codes = set()
        for stock in session.query(StockBasic.ts_code).all():
            existing_codes.add(stock.ts_code)
        result['existing_stocks'] = len(existing_codes)
        logger.info(f"数据库 stock_basic 表已有 {len(existing_codes)} 只股票")

        new_codes = [code for code in tdx_stock_codes if code not in existing_codes]
        result['new_stocks'] = len(new_codes)
        logger.info(f"发现 {len(new_codes)} 只新股票需要添加到数据库")

        if not new_codes:
            logger.info("所有通达信股票都已在数据库中，无需同步")
            return result

        baostock_handler = BaostockHandler(config, db_manager)
        akshare_handler = AkShareHandler(config, db_manager)

        baostock_basic, akshare_basic = _fetch_external_stock_basic(
            baostock_handler, akshare_handler
        )

        merged_basic = merge_stock_basic(baostock_basic, akshare_basic, existing_codes, tdx_stock_codes)

        ctx = StockBatchContext(
            new_codes, merged_basic, existing_codes, session,
            baostock_handler, akshare_handler, result
        )
        _process_single_stock_batch(ctx)

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
    return {
        'symbol': symbol,
        'name': row.get('code_name', ''),
        'area': row.get('area', ''),
        'industry': row.get('industry', ''),
        'market': row.get('market', ''),
        'list_date': row.get('list_date'),
        'status': row.get('status', 'L'),
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
            code = row.get('code', '')
            ts_code = _parse_akshare_code(code, tdx_market_map)

            if ts_code is None or ts_code in existing_codes:
                continue

            if ts_code in merged:
                merged[ts_code].update({
                    'name': row.get('name', merged[ts_code].get('name', '')),
                    'area': row.get('area', merged[ts_code].get('area', '')),
                    'industry': row.get('industry', merged[ts_code].get('industry', '')),
                })
            else:
                merged[ts_code] = {
                    'symbol': code,
                    'name': row.get('name', ''),
                    'area': row.get('area', ''),
                    'industry': row.get('industry', ''),
                    'market': '',
                    'list_date': None,
                    'status': 'L',
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
        return symbol.startswith('000')
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


if __name__ == "__main__":
    main()
