#!/usr/bin/env python3

"""
股票基本信息同步业务线

从 sync_service.py 拆出：外部基本信息合并、行业映射补全、存量字段回填
与 sync_tdx_stock_to_database 编排。对外由 sync_service 聚合导出。
"""

from dataclasses import dataclass

from loguru import logger

from src.data.services.sync_common import (
    SyncFlow,
    _execute_sync_flow,
    _extract_tdx_first_last_trade_date,
    _is_fund_ts_code,
    _is_index_ts_code,
    _normalize_date_value,
    _normalize_text_value,
    _sync_each_with_recovery,
    _to_ts_code_or_none,
)
from src.utils.exceptions import DataSourceException

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
    # 兜底理由：行业映射获取失败降级为空映射
    except Exception as e:  # noqa: BLE001
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
                # 兜底理由：周期提交失败回滚后继续
                except Exception as commit_e:  # noqa: BLE001
                    logger.exception(f"提交数据库时失败: {commit_e}")
                    ctx.session.rollback()

            time.sleep(0.1)

        # 兜底理由：单只股票处理失败不中断整批
        except Exception as e:  # noqa: BLE001
            logger.exception(f"处理股票 {ts_code} 失败: {e}")
            ctx.result['failed_stocks'].append(ts_code)
            continue


def _finalize_database_commit(session, result):
    """完成数据库最终提交。"""
    try:
        session.commit()
    # 兜底理由：最终提交失败回滚，由调用方读取失败清单
    except Exception as commit_e:  # noqa: BLE001
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
            except (ValueError, TypeError):
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
        # 兜底理由：回填上市日期失败仅告警
        except Exception as trade_date_e:  # noqa: BLE001
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
