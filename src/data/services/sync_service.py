#!/usr/bin/env python3

"""
数据同步服务模块

从 main.py 抽离的股票/指数/基金同步业务逻辑。
main.py 仅保留应用组装与启动，数据同步的领域逻辑收敛于此。

公开接口：
    - sync_tdx_stock_to_database(config, db_manager)
    - sync_tdx_index_to_database(config, db_manager)
    - sync_stock_fund_to_fund_basic(config, db_manager)
"""

import contextlib
from dataclasses import dataclass
from typing import Callable

from loguru import logger

from src.data.tdx_day_parser import build_day_file_path, read_first_last_trade_date
from src.utils.code_converter import MARKET_BJ, MARKET_SH, MARKET_SZ, to_ts_code
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
        except (DataSaveError, Exception) as e:
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


# ======================================================================
# 股票基本信息同步
# ======================================================================

def _fetch_external_stock_basic(baostock_handler, akshare_handler):
    """获取外部股票基本信息 + Baostock 行业映射。"""
    baostock_basic = None
    akshare_basic = None
    baostock_industry_map = {}

    logger.info("开始从 Baostock 获取股票基本信息...")
    try:
        baostock_basic = baostock_handler.update_stock_basic()
        if baostock_basic is not None:
            logger.info(f"从 Baostock 获取到 {baostock_basic.height} 条股票信息")
    except DataSourceException as e:
        logger.warning(f"从 Baostock 获取股票基本信息失败: {e}")

    try:
        baostock_industry_map = _fetch_baostock_industry_map(baostock_handler)
        if baostock_industry_map:
            logger.info(f"从 Baostock 行业接口获取到 {len(baostock_industry_map)} 条行业映射")
    except (DataSourceException, Exception) as e:
        logger.warning(f"从 Baostock 获取行业映射失败: {e}")

    logger.info("开始从 AkShare 获取股票基本信息...")
    try:
        akshare_basic = akshare_handler.update_stock_basic()
        if akshare_basic is not None:
            logger.info(f"从 AkShare 获取到 {akshare_basic.height} 条股票信息")
    except DataSourceException as e:
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
        ts_code = _to_ts_code_or_none(row.get('code'))
        if not ts_code:
            continue
        if _is_index_ts_code(ts_code) or _is_fund_ts_code(ts_code):
            continue
        industry = str(row.get('industry', '') or '').strip()
        if industry:
            industry_map[ts_code] = industry

    return industry_map


def _select_akshare_column(frame, candidates):
    """从 AkShare DataFrame 中选第一个存在且有值的候选列。"""
    return next(
        (c for c in candidates if c in frame.columns and frame[c].notna().any()),
        None,
    )


def _collect_akshare_board_names(board_pd):
    """从行业板块列表提取板块名称，列表不可用时返回 None。"""
    if board_pd is None or board_pd.empty:
        logger.warning("AkShare 行业板块列表为空")
        return None

    name_col = _select_akshare_column(board_pd, ('板块名称', '名称', 'name'))
    if name_col is None:
        logger.warning("AkShare 行业板块列表缺少名称列")
        return None

    return [str(v).strip() for v in board_pd[name_col].tolist() if v and str(v).strip()]


def _add_board_codes_to_industry_map(industry_map, raw_codes, board_name):
    """把单板块成分股代码写入行业映射，已存在的代码不覆盖。"""
    for raw_code in raw_codes:
        if not raw_code:
            continue
        ts_code = _to_ts_code_or_none(raw_code)
        if not ts_code:
            continue
        if _is_index_ts_code(ts_code) or _is_fund_ts_code(ts_code):
            continue
        if ts_code not in industry_map:
            industry_map[ts_code] = board_name


def _collect_board_industry_map(ak, board_names):
    """逐板块拉取成分股，生成 ts_code -> 行业映射。"""
    industry_map = {}
    for i, board_name in enumerate(board_names):
        if not board_name:
            continue
        try:
            cons_pd = ak.stock_board_industry_cons_em(symbol=board_name)
            if cons_pd is None or cons_pd.empty:
                continue

            # 只需成分股代码一列：直接读 pandas 列，跳过 Polars 编组与逐行 named dict
            code_col = _select_akshare_column(cons_pd, ('代码', 'code', 'symbol'))
            if code_col is None:
                continue

            _add_board_codes_to_industry_map(
                industry_map, cons_pd[code_col].tolist(), board_name
            )

            if (i + 1) % 20 == 0:
                logger.info(
                    f"AkShare行业映射进度: {i + 1}/{len(board_names)}，"
                    f"当前映射 {len(industry_map)} 只股票"
                )
        except DataSourceException as board_e:
            logger.warning(f"获取行业板块 {board_name} 成分失败: {board_e}")
            continue

    return industry_map


def _fetch_akshare_industry_map():
    """从 AkShare 行业板块成分获取行业映射（ts_code -> industry）。"""
    import akshare as ak

    try:
        board_pd = ak.stock_board_industry_name_em()
        board_names = _collect_akshare_board_names(board_pd)
        if board_names is None:
            return {}

        logger.info(f"AkShare 行业板块数量: {len(board_names)}")
        return _collect_board_industry_map(ak, board_names)
    except DataSourceException as e:
        logger.warning(f"从 AkShare 获取行业映射失败: {e}")
        return {}


def _create_stock_basic_from_info(ts_code, stock_info):
    """从股票信息字典创建 StockBasic 模型对象。"""
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
    """处理单批股票（新增阶段）。"""
    import time

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
                except (DataSaveError, Exception) as commit_e:
                    logger.exception(f"提交数据库时失败: {commit_e}")
                    ctx.session.rollback()

            time.sleep(0.1)

        except Exception as e:
            logger.exception(f"处理股票 {ts_code} 失败: {e}")
            ctx.result['failed_stocks'].append(ts_code)
            continue


def _finalize_database_commit(session, result):
    """完成数据库最终提交。"""
    try:
        session.commit()
    except (DataSaveError, Exception) as commit_e:
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
    """记录股票同步结果统计。"""
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


def _first_stock_field(row, *keys):
    """按候选字段顺序取第一个非空值，全部缺失返回空串。"""
    for key in keys:
        value = row.get(key)
        if value:
            return value
    return ''


def _build_stock_info(ts_code, symbol, row, source):
    """构建股票信息字典（统一多源字段名）。"""
    return {
        'symbol': symbol,
        'name': _first_stock_field(row, 'code_name', 'name', '名称'),
        'area': _first_stock_field(row, 'area', 'region', '地域', '地区'),
        'industry': _first_stock_field(row, 'industry', '所属行业', '行业'),
        'market': _first_stock_field(row, 'market', '市场'),
        'list_date': _first_stock_field(row, 'list_date', 'ipoDate', '上市日期') or None,
        'delist_date': _first_stock_field(row, 'delist_date', 'outDate', '退市日期') or None,
        'status': _first_stock_field(row, 'status', '上市状态') or 'L',
        'source': source
    }


def _merge_baostock_basic(merged, baostock_df, existing_codes):
    """并入 Baostock 基本信息，已存在或非法代码跳过。"""
    if baostock_df is None or baostock_df.is_empty():
        return

    for row in baostock_df.iter_rows(named=True):
        code = row.get('code', '')
        ts_code = _to_ts_code_or_none(code)

        if ts_code is None or ts_code in existing_codes:
            continue

        merged[ts_code] = _build_stock_info(ts_code, code, row, 'baostock')


def _merge_akshare_basic(merged, akshare_df, existing_codes):
    """并入 AkShare 基本信息，已存在代码按字段补全。"""
    if akshare_df is None or akshare_df.is_empty():
        return

    for row in akshare_df.iter_rows(named=True):
        code = _first_stock_field(row, 'code', '代码', 'symbol')
        ts_code = _to_ts_code_or_none(code)

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


def merge_stock_basic(baostock_df, akshare_df, existing_codes=None):
    """合并 Baostock 和 AkShare 的股票基本信息。"""
    if existing_codes is None:
        existing_codes = set()

    merged = {}
    _merge_baostock_basic(merged, baostock_df, existing_codes)
    _merge_akshare_basic(merged, akshare_df, existing_codes)

    logger.info(f"合并后共有 {len(merged)} 条股票基本信息（已过滤已存在的 {len(existing_codes)} 条）")
    return merged


def get_stock_info_from_apis(ts_code, merged_basic, baostock_handler, akshare_handler):
    """从合并后的基本信息中获取单个股票的详细信息。"""
    symbol = ts_code.split('.')[0]

    if ts_code in merged_basic:
        info = merged_basic[ts_code].copy()
        info['ts_code'] = ts_code

        if info.get('list_date') and isinstance(info['list_date'], str):
            try:
                from datetime import datetime
                info['list_date'] = datetime.strptime(info['list_date'], '%Y-%m-%d').date()
            except Exception:
                info['list_date'] = None

        return info

    for _mapped_code, info in merged_basic.items():
        if info.get('symbol') == symbol:
            result = info.copy()
            result['ts_code'] = ts_code
            return result

    logger.warning(f"在 Baostock/AkShare 数据中未找到股票 {ts_code} 的信息，跳过该标的")
    return None


def _prune_non_stock_rows(session, result):
    """清理 stock_basic 中的基金/指数行，返回 (existing_codes, non_stock_code_set)。"""
    from src.database.models.stock import StockBasic

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
    existing_codes = {
        row.ts_code for row in all_existing_rows if row.ts_code not in non_stock_code_set
    }
    result['existing_stocks'] = len(existing_codes)
    logger.info(f"数据库 stock_basic 表已有 {len(existing_codes)} 只股票")
    return existing_codes, non_stock_code_set


def _apply_baostock_industry(merged_basic, baostock_industry_map):
    """把 Baostock 行业映射并入 merged_basic，仅在缺失或未知时覆盖。"""
    if not baostock_industry_map:
        return

    for ts_code, industry in baostock_industry_map.items():
        if ts_code not in merged_basic:
            symbol = ts_code.split('.')[0] if '.' in ts_code else ts_code
            merged_basic[ts_code] = {
                'symbol': symbol, 'name': '', 'area': '', 'industry': industry,
                'market': '', 'list_date': None, 'status': 'L', 'source': 'baostock_industry'
            }
        else:
            existing_industry = str(merged_basic[ts_code].get('industry', '') or '').strip()
            if (not existing_industry) or existing_industry in {'未知', '--', 'N/A'}:
                merged_basic[ts_code]['industry'] = industry


def _apply_akshare_industry_fallback(merged_basic, tdx_stock_codes):
    """按需用 AkShare 行业板块成分兜底缺失行业，返回补充映射条数。"""
    missing_industry_count = sum(
        1 for ts_code in tdx_stock_codes
        if not str(merged_basic.get(ts_code, {}).get('industry', '') or '').strip()
    )
    if missing_industry_count <= 0:
        return 0

    logger.info(f"仍有 {missing_industry_count} 只股票缺少行业字段，尝试 AkShare 行业成分兜底")
    akshare_industry_map = _fetch_akshare_industry_map()
    if akshare_industry_map:
        for ts_code, industry in akshare_industry_map.items():
            if ts_code not in merged_basic:
                symbol = ts_code.split('.')[0] if '.' in ts_code else ts_code
                merged_basic[ts_code] = {
                    'symbol': symbol, 'name': '', 'area': '', 'industry': industry,
                    'market': '', 'list_date': None, 'status': 'L', 'source': 'akshare_industry'
                }
            else:
                existing_industry = str(merged_basic[ts_code].get('industry', '') or '').strip()
                if not existing_industry:
                    merged_basic[ts_code]['industry'] = industry
        logger.info(f"AkShare 行业兜底完成，补充映射 {len(akshare_industry_map)} 只股票")
    return len(akshare_industry_map)


def _backfill_text_fields(row, source_info, result):
    """回填存量股票的名称/地区/行业/市场字段，返回是否有改动。"""
    changed = False

    source_name = _normalize_text_value(source_info.get('name'))
    source_area = _normalize_text_value(source_info.get('area'))
    source_industry = _normalize_text_value(source_info.get('industry'))
    source_market = _normalize_text_value(source_info.get('market'))

    current_name = _normalize_text_value(row.name)
    if source_name and (
        (not current_name)
        or current_name.startswith('股票')
        or current_name in {'未知', '--', 'N/A'}
    ):
        if _update_stock_text_field(row, 'name', source_name, result, max_len=50):
            changed = True

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

    return changed


def _backfill_list_date(row, source_info, session, config, result):
    """按外部字段 -> 通达信首末日期 -> stock_daily 最早交易日三档回填上市日期。"""
    changed = False

    if _update_stock_date_field(row, 'list_date', source_info.get('list_date'), result):
        changed = True

    if not row.list_date:
        first_trade_date, _ = _extract_tdx_first_last_trade_date(config, row.ts_code)
        if _update_stock_date_field(row, 'list_date', first_trade_date, result):
            changed = True

    if not row.list_date:
        try:
            from sqlalchemy import func

            from src.database.models.stock import StockDaily

            earliest_trade_date = (
                session.query(func.min(StockDaily.trade_date))
                .filter(StockDaily.ts_code == row.ts_code)
                .scalar()
            )
            if _update_stock_date_field(row, 'list_date', earliest_trade_date, result):
                changed = True
        except (DataSaveError, Exception) as trade_date_e:
            logger.debug(f"从 stock_daily 回填上市日期失败 {row.ts_code}: {trade_date_e}")

    return changed


def _backfill_status(row, source_info, result):
    """回填或刷新存量股票状态，返回是否有改动。"""
    raw_status = str(source_info.get('status', '') or '').strip()
    source_status = _normalize_stock_status(raw_status) if raw_status else ''

    if not row.status and source_status:
        row.status = source_status
        result['filled_blank_fields'] += 1
        return True

    if source_status and row.status != source_status:
        row.status = source_status
        result['refreshed_fields'] += 1
        return True

    return False


def _backfill_stock_row(row, source_info, session, config, result):
    """回填单只存量股票的文本、上市/退市日期与状态，返回是否有改动。"""
    changed = _backfill_text_fields(row, source_info, result)

    if _backfill_list_date(row, source_info, session, config, result):
        changed = True

    if _update_stock_date_field(row, 'delist_date', source_info.get('delist_date'), result):
        changed = True

    if _backfill_status(row, source_info, result):
        changed = True

    return changed


def sync_tdx_stock_to_database(config, db_manager):
    """
    从通达信数据源同步股票信息到数据库。

    工作流程：
    1. 从通达信提取所有股票代码
    2. 与数据库 stock_basic 表匹配
    3. 新增股票：通过 baostock/akshare 获取完整信息后插入
    4. 存量股票：回填/刷新空白字段
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

    def prepare(session):
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
            return None

        existing_codes, _ = _prune_non_stock_rows(session, result)

        new_codes = [code for code in tdx_stock_codes if code not in existing_codes]
        result['new_stocks'] = len(new_codes)
        logger.info(f"发现 {len(new_codes)} 只新股票需要添加到数据库")

        baostock_handler = BaostockHandler(config, db_manager)
        akshare_handler = AkShareHandler(config, db_manager)

        baostock_basic, akshare_basic, baostock_industry_map = _fetch_external_stock_basic(
            baostock_handler, akshare_handler
        )

        # 两个外部基本信息源全部失败时终止同步：继续执行只会把占位/缺失字段
        # 当作完整数据写入 stock_basic，且会误删存量记录
        if baostock_basic is None and akshare_basic is None:
            logger.error(
                "Baostock 与 AkShare 股票基本信息均获取失败，终止本次股票同步，"
                "避免以部分数据覆盖 stock_basic"
            )
            return None

        merged_basic = merge_stock_basic(baostock_basic, akshare_basic, set())

        # 行业字段优先采用 Baostock 行业映射，随后按需用 AkShare 兜底
        _apply_baostock_industry(merged_basic, baostock_industry_map)
        _apply_akshare_industry_fallback(merged_basic, tdx_stock_codes)

        return StockSyncContext(
            config=config,
            db_manager=db_manager,
            result=result,
            session=session,
            baostock_handler=baostock_handler,
            akshare_handler=akshare_handler,
            existing_codes=existing_codes,
            new_codes=new_codes,
            tdx_stock_codes=tdx_stock_codes,
            merged_basic=merged_basic,
        )

    def run(ctx, session):
        if ctx.new_codes:
            _process_single_stock_batch(StockBatchContext(
                ctx.new_codes, ctx.merged_basic, ctx.existing_codes, session,
                ctx.baostock_handler, ctx.akshare_handler, result
            ))
        else:
            logger.info("所有通达信股票都已存在，跳过新增阶段，继续执行存量回填")

        # 第二阶段：回填并刷新 stock_basic 存量信息
        def _backfill(row):
            if _is_index_ts_code(row.ts_code) or _is_fund_ts_code(row.ts_code, row.name, row.market):
                return
            source_info = ctx.merged_basic.get(row.ts_code, {})
            if _backfill_stock_row(row, source_info, session, config, result):
                result['updated_stocks'] += 1

        _sync_each_with_recovery(
            session.query(StockBasic).all(), _backfill,
            result=result, failed_key='failed_stocks', session=session,
            label='股票', commit_interval=0, item_name=lambda row: row.ts_code,
        )

        _finalize_database_commit(session, result)

    return _execute_sync_flow(
        SyncFlow(
            title="从通达信数据源同步股票信息到数据库",
            error_label="同步股票信息",
            result=result,
            prepare=prepare,
            run=run,
            log_stats=_log_sync_result,
        ),
        db_manager,
    )


# ======================================================================
# 指数基本信息同步
# ======================================================================

FORCED_INDEX_NAME_MAP = {
    '899050.BJ': '北证50',
    '899601.BJ': '北证专精特新',
}


def _normalize_index_ts_code(code):
    """
    统一规范 Baostock / AkShare 指数代码格式为 000001.SH。

    显式带市场标识（sh.000001 / 000001.SH）时委托统一代码转换工具；
    纯数字代码不能用个股规则推断市场（000001 是上证指数而非平安银行），
    因此按指数前缀规则处理：000 -> SH、399 -> SZ、884/899 -> BJ。
    """
    if code is None:
        return None

    text = str(code).strip()
    if not text:
        return None

    if '.' in text:
        return _to_ts_code_or_none(text)

    if not text.isdigit() or len(text) != 6:
        return None
    if text.startswith('000'):
        return f"{text}.{MARKET_SH}"
    if text.startswith('399'):
        return f"{text}.{MARKET_SZ}"
    if text.startswith(('884', '899')):
        return f"{text}.{MARKET_BJ}"
    return None


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


def _infer_index_publisher(ts_code):
    """按市场后缀推断指数发布方。"""
    market = ts_code.split('.')[-1].upper() if ts_code and '.' in ts_code else ''
    if market == 'SH':
        return '上交所'
    if market == 'SZ':
        return '深交所'
    if market == 'BJ':
        return '北交所'
    return '交易所'


def _infer_index_type_category(index_name):
    """按名称关键词推断指数类型与类别。"""
    name = (index_name or '').strip()
    if '行业' in name:
        return '行业指数', '行业'
    if '主题' in name:
        return '主题指数', '主题'
    if '红利' in name:
        return '策略指数', '红利'
    if '创业板' in name:
        return '板块指数', '创业板'
    if '科创' in name:
        return '板块指数', '科创板'
    if '北证' in name:
        return '板块指数', '北交所'
    if '中证' in name or '沪深' in name or '上证' in name or '深证' in name:
        return '综合指数', '宽基'
    return '综合指数', '通用'


def _infer_index_meta(ts_code, index_name):
    """根据代码和名称推断指数元信息。"""
    index_type, category = _infer_index_type_category(index_name)
    return {
        'publisher': _infer_index_publisher(ts_code),
        'index_type': index_type,
        'category': category,
        'weight_rule': '市值加权',
    }


def _merge_index_info(merged_info_map, ts_code, name, date_value, overwrite_name):
    """并入单个指数信息：名称可覆盖或仅补空，日期取更早值。"""
    item = merged_info_map.get(ts_code)
    if item is None:
        item = {'name': '', 'list_date': None, 'base_date': None}
        merged_info_map[ts_code] = item

    if overwrite_name or not item['name']:
        item['name'] = name

    if date_value:
        if item['list_date'] is None or date_value < item['list_date']:
            item['list_date'] = date_value
        if item['base_date'] is None or date_value < item['base_date']:
            item['base_date'] = date_value


def _merge_baostock_index_row(merged_info_map, row):
    """并入单行 Baostock 指数信息。"""
    ts_code = _normalize_index_ts_code(row.get('code'))
    name = (row.get('code_name') or '').strip()
    if not (ts_code and name and _is_index_ts_code(ts_code)):
        return
    _merge_index_info(
        merged_info_map, ts_code, name, _parse_index_date(row.get('ipoDate')), True
    )


def _collect_baostock_index_info(merged_info_map, baostock_handler):
    """从 Baostock 收集指数名称与日期信息。"""
    import baostock as bs
    import polars as pl

    logger.info("开始从 Baostock 获取指数基本信息...")
    try:
        if not baostock_handler._ensure_baostock_login():
            return
        rs = bs.query_stock_basic(code_name="")
        baostock_df = pl.from_pandas(rs.get_data())
        if not baostock_df.is_empty():
            for row in baostock_df.iter_rows(named=True):
                _merge_baostock_index_row(merged_info_map, row)
        logger.info(f"Baostock指数信息映射数量: {len(merged_info_map)}")
    except DataSourceException as e:
        logger.warning(f"从 Baostock 获取指数基本信息失败: {e}")


def _merge_akshare_index_row(merged_info_map, row):
    """并入单行 AkShare 指数信息，成功计入时返回 True。"""
    ts_code = _normalize_index_ts_code(row.get('代码', row.get('index_code')))
    raw_name = row.get('名称', row.get('display_name'))
    name = (raw_name or '').strip() if raw_name is not None else ''
    if not (ts_code and name and _is_index_ts_code(ts_code)):
        return False

    _merge_index_info(
        merged_info_map, ts_code, name,
        _parse_index_date(row.get('publish_date', row.get('发布日期'))), False
    )
    return True


def _collect_akshare_index_info(merged_info_map, akshare_handler):
    """从 AkShare 收集指数名称与日期信息。"""
    logger.info("开始从 AkShare 获取指数基本信息...")
    try:
        akshare_df = akshare_handler.update_index_basic()
        if akshare_df is not None and not akshare_df.is_empty():
            ak_count = 0
            for row in akshare_df.iter_rows(named=True):
                if _merge_akshare_index_row(merged_info_map, row):
                    ak_count += 1
            logger.info(f"AkShare可用指数名称映射数量: {ak_count}")
    except DataSourceException as e:
        logger.warning(f"从 AkShare 获取指数基本信息失败: {e}")


def _collect_external_index_name_map(config):
    """收集外部数据源的指数信息映射。"""
    from src.data.akshare_handler import AkShareHandler
    from src.data.baostock_handler import BaostockHandler

    baostock_handler = BaostockHandler(config, None)
    akshare_handler = AkShareHandler(config, None)

    merged_info_map = {}
    _collect_baostock_index_info(merged_info_map, baostock_handler)
    try:
        _collect_akshare_index_info(merged_info_map, akshare_handler)
    finally:
        with contextlib.suppress(Exception):
            baostock_handler._logout_baostock()

    return merged_info_map


def _index_name_from_code(ts_code):
    """由指数代码生成占位名，代码无市场后缀时返回空串。"""
    if ts_code and '.' in ts_code:
        return f"指数{ts_code.split('.')[0]}"
    return ''


def _index_desc_text(index_name, ts_code):
    """生成指数描述文本。"""
    safe_name = index_name or _index_name_from_code(ts_code) or '指数'
    return f"{safe_name}({ts_code})，通达信基准，Baostock/AkShare补全"


def _fill_index_blank_fields(index_row, inferred, source_list_date,
                             source_base_date, default_desc, result):
    """回填指数空白字段（仅空值），返回是否有改动。"""
    changed = False

    for field_name in ('publisher', 'index_type', 'category', 'weight_rule'):
        if not getattr(index_row, field_name) and inferred.get(field_name):
            setattr(index_row, field_name, inferred[field_name])
            changed = True
            result['filled_blank_fields'] += 1

    if not index_row.list_date and source_list_date:
        index_row.list_date = source_list_date
        changed = True
        result['filled_blank_fields'] += 1
    if not index_row.base_date and source_base_date:
        index_row.base_date = source_base_date
        changed = True
        result['filled_blank_fields'] += 1
    if not index_row.desc:
        index_row.desc = default_desc
        changed = True
        result['filled_blank_fields'] += 1

    return changed


def _upsert_index_row(existing, ts_code, source_name, source_info, inferred,
                      market, default_desc, session, result):
    """新增或更新单条 index_basic 记录，并累计统计计数。"""
    from src.database.models.index import IndexBasic

    if existing is None:
        session.add(IndexBasic(
            ts_code=ts_code, name=source_name, market=market,
            publisher=inferred['publisher'], index_type=inferred['index_type'],
            category=inferred['category'], list_date=source_info.get('list_date'),
            base_date=source_info.get('base_date'), weight_rule=inferred['weight_rule'],
            desc=default_desc,
        ))
        result['inserted_indexes'] += 1
        return

    changed = False
    if existing.name != source_name:
        existing.name = source_name
        changed = True
    if existing.market != market:
        existing.market = market
        changed = True

    if _fill_index_blank_fields(
        existing, inferred, source_info.get('list_date'),
        source_info.get('base_date'), default_desc, result
    ):
        changed = True

    if changed:
        result['updated_indexes'] += 1
    else:
        result['unchanged_indexes'] += 1


def _backfill_index_row(row, external_info_map, tdx_index_code_set, result):
    """回填单条存量指数空白字段，返回是否有改动。"""
    changed = False
    normalized_code = _normalize_index_ts_code(row.ts_code) or row.ts_code
    source_info = external_info_map.get(normalized_code, {})
    forced_name = FORCED_INDEX_NAME_MAP.get(normalized_code, '')

    if not row.name:
        source_name = forced_name or _normalize_text_value(source_info.get('name'))
        if not source_name:
            source_name = _index_name_from_code(normalized_code)
        if source_name:
            row.name = source_name
            changed = True
            result['filled_blank_fields'] += 1

    normalized_name = row.name or _normalize_text_value(source_info.get('name'))
    inferred = _infer_index_meta(normalized_code, normalized_name)

    if not row.market and normalized_code and '.' in normalized_code:
        row.market = normalized_code.split('.')[-1]
        changed = True
        result['filled_blank_fields'] += 1

    if _fill_index_blank_fields(
        row, inferred, source_info.get('list_date'), source_info.get('base_date'),
        _index_desc_text(normalized_name, normalized_code), result
    ):
        changed = True

    if changed and row.ts_code not in tdx_index_code_set:
        result['updated_indexes'] += 1

    return changed


def _delete_stale_index_rows(session, all_index_rows, tdx_index_code_set, result):
    """删除不在通达信指数代码白名单中的历史记录。"""
    stale_rows = [
        row for row in all_index_rows
        if not (_normalize_index_ts_code(row.ts_code) and
                _normalize_index_ts_code(row.ts_code) in tdx_index_code_set)
    ]
    for stale_row in stale_rows:
        session.delete(stale_row)
    result['deleted_indexes'] = len(stale_rows)
    if stale_rows:
        logger.info(f"已删除 {len(stale_rows)} 条不在通达信指数代码中的 index_basic 记录")


@dataclass
class IndexSyncContext:
    """指数同步上下文"""
    tdx_index_codes: list
    tdx_index_code_set: set
    external_info_map: dict
    existing_index_map: dict


def _log_index_sync_result(result):
    """记录指数同步结果统计。"""
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

    def prepare(session):
        tdx_handler = TdxHandler(config, db_manager)
        tdx_codes = tdx_handler.get_stock_list()
        tdx_index_codes = sorted({code for code in tdx_codes if _is_index_ts_code(code)})
        tdx_index_code_set = set(tdx_index_codes)
        result['total_tdx_indexes'] = len(tdx_index_codes)
        logger.info(f"通达信指数代码数量: {len(tdx_index_codes)}")

        if not tdx_index_codes:
            logger.warning("通达信未发现指数代码，跳过同步")
            return None

        external_info_map = _collect_external_index_name_map(config)
        logger.info(f"外部数据源可用指数信息映射总数: {len(external_info_map)}")

        # 外部指数信息全失败时终止：继续执行会用 "指数XXXXXX" 占位名覆盖
        # index_basic，把部分数据当成完整同步结果
        if not external_info_map:
            logger.error(
                "Baostock 与 AkShare 指数基本信息均获取失败，终止 index_basic 同步，"
                "避免写入占位名称"
            )
            return None

        existing_index_map = {
            row.ts_code: row
            for row in session.query(IndexBasic).filter(IndexBasic.ts_code.in_(tdx_index_codes)).all()
        }

        return IndexSyncContext(
            tdx_index_codes, tdx_index_code_set, external_info_map, existing_index_map
        )

    def run(ctx, session):
        def _upsert(ts_code):
            source_info = ctx.external_info_map.get(ts_code, {})
            source_name = FORCED_INDEX_NAME_MAP.get(
                ts_code, _normalize_text_value(source_info.get('name'))
            )
            if not source_name:
                source_name = _index_name_from_code(ts_code)
                result['fallback_named_indexes'] += 1

            market = ts_code.split('.')[-1]
            inferred = _infer_index_meta(ts_code, source_name)
            default_desc = _index_desc_text(source_name, ts_code)

            _upsert_index_row(
                ctx.existing_index_map.get(ts_code), ts_code, source_name,
                source_info, inferred, market, default_desc, session, result
            )

        _sync_each_with_recovery(
            ctx.tdx_index_codes, _upsert,
            result=result, failed_key='failed_indexes', session=session, label='指数',
        )

        # 第二阶段：存量空白字段回填
        all_index_rows = session.query(IndexBasic).all()
        _sync_each_with_recovery(
            all_index_rows,
            lambda row: _backfill_index_row(
                row, ctx.external_info_map, ctx.tdx_index_code_set, result
            ),
            result=result, failed_key='failed_indexes', session=session,
            label='指数', commit_interval=0, item_name=lambda row: row.ts_code,
        )

        # 第三阶段：删除不在通达信指数代码白名单中的历史记录
        _delete_stale_index_rows(session, all_index_rows, ctx.tdx_index_code_set, result)

        session.commit()

    return _execute_sync_flow(
        SyncFlow(
            title="以通达信指数代码为基准同步 index_basic",
            error_label="同步指数信息",
            result=result,
            prepare=prepare,
            run=run,
            log_stats=_log_index_sync_result,
        ),
        db_manager,
    )


# ======================================================================
# 基金同步
# ======================================================================

def _infer_fund_type(symbol, name):
    """推断基金类型。"""
    nm = (name or '').upper()
    if 'ETF' in nm:
        return 'ETF'
    if 'LOF' in nm or symbol.startswith(('16', '18')):
        return 'LOF'
    return '基金'


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


def _migrate_fund_stock_tuple(stock):
    """由 stock_basic 记录派生基金的代码、名称、类型与市场。"""
    symbol = (stock.symbol or stock.ts_code.split('.')[0])[:9]
    name = (stock.name or symbol)[:20]
    return symbol, name, _infer_fund_type(symbol, name), stock.ts_code.split('.')[-1]


def _fill_fund_from_stock(fund, stock, symbol, name, inferred_type, inferred_market):
    """用 stock_basic 记录补全 fund_basic 空白字段，返回是否有改动。"""
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
    return changed


def _migrate_fund_stock_row(stock, existing_fund_map, session, result):
    """把单条 stock_basic 基金记录迁移或更新到 fund_basic。"""
    from src.database.models.fund import FundBasic

    symbol, name, inferred_type, inferred_market = _migrate_fund_stock_tuple(stock)

    fund = existing_fund_map.get(stock.ts_code)
    if fund is None:
        fund = FundBasic(
            ts_code=stock.ts_code, symbol=symbol, name=name,
            fund_type=inferred_type, market=inferred_market,
            list_date=stock.list_date, delist_date=stock.delist_date,
            status=stock.status or 'L',
        )
        session.add(fund)
        existing_fund_map[stock.ts_code] = fund
        result['inserted_funds'] += 1
        return

    if _fill_fund_from_stock(fund, stock, symbol, name, inferred_type, inferred_market):
        result['updated_funds'] += 1


def _infer_fund_ts_code_by_symbol(symbol):
    """按基金代码前缀推断 ETF 的 ts_code，无法识别返回 None。"""
    if symbol.startswith('51') or symbol.startswith('58'):
        return f"{symbol}.SH"
    if symbol.startswith('15') or symbol.startswith('16'):
        return f"{symbol}.SZ"
    return None


def _merge_etf_row(row, existing_fund_map, session, result):
    """用 AkShare ETF 行补全或新增 fund_basic 记录。"""
    from src.database.models.fund import FundBasic

    symbol = str(row.get('基金代码', '')).strip()
    name = str(row.get('基金简称', '')).strip()
    if not symbol:
        return

    ts_code = _infer_fund_ts_code_by_symbol(symbol)
    if not ts_code:
        return

    fund = existing_fund_map.get(ts_code)
    if fund is None:
        fund = FundBasic(
            ts_code=ts_code, symbol=symbol[:9], name=(name or symbol)[:20],
            fund_type='ETF', market=ts_code.split('.')[-1], status='L',
        )
        session.add(fund)
        existing_fund_map[ts_code] = fund
        result['inserted_funds'] += 1
        result['akshare_enriched'] += 1
        return

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


def _enrich_funds_from_akshare(config, existing_fund_map, session, result):
    """使用 AkShare ETF 列表补全基金名称与类型。"""
    from src.data.akshare_handler import AkShareHandler

    try:
        akshare_handler = AkShareHandler(config, None)
        etf_df = akshare_handler.update_etf_basic()
        if etf_df is not None and not etf_df.is_empty():
            for row in etf_df.iter_rows(named=True):
                _merge_etf_row(row, existing_fund_map, session, result)
    except DataSourceException as e:
        logger.warning(f"使用 AkShare 补全基金信息失败: {e}")


def _fill_fund_date_row(fund, config, tdx_date_cache, result):
    """用通达信日线首末日期回填基金日期，返回是否有改动。"""
    status = str(fund.status or '').strip().upper()
    is_delisted = status in {'D', '0', 'DELIST', 'DELISTED'}

    need_list_date = fund.list_date is None
    need_delist_date = fund.delist_date is None and is_delisted
    if not need_list_date and not need_delist_date:
        return False

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

    return changed


def _fill_fund_dates(config, session, result):
    """使用通达信日线数据补全基金日期信息。"""
    from src.database.models.fund import FundBasic

    tdx_date_cache = {}
    all_funds = session.query(FundBasic).all()
    for fund in all_funds:
        try:
            if _fill_fund_date_row(fund, config, tdx_date_cache, result):
                result['updated_funds'] += 1
        except Exception as date_fill_e:
            logger.exception(f"回填基金日期失败 {fund.ts_code}: {date_fill_e}")
            result['failed'] += 1


def _delete_migrated_funds(session, fund_stocks, result):
    """从 stock_basic 删除已迁移的基金记录。"""
    for stock in fund_stocks:
        session.delete(stock)
    result['deleted_from_stock_basic'] = len(fund_stocks)


def sync_stock_fund_to_fund_basic(config, db_manager):
    """将 stock_basic 中基金记录迁移到 fund_basic，并补全信息。"""
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
                _migrate_fund_stock_row(stock, existing_fund_map, session, result)
            except (DataSaveError, Exception) as row_e:
                logger.exception(f"迁移基金记录失败 {stock.ts_code}: {row_e}")
                result['failed'] += 1

        # 使用 AkShare ETF 列表补全名称/类型
        _enrich_funds_from_akshare(config, existing_fund_map, session, result)

        # 使用通达信日线数据补全日期信息
        _fill_fund_dates(config, session, result)

        # 从 stock_basic 删除已迁移基金
        _delete_migrated_funds(session, fund_stocks, result)

        session.commit()

    except (DataSaveError, DataSourceException) as e:
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
