#!/usr/bin/env python3

"""
股票基本信息同步业务线

从 sync_service.py 拆出：外部基本信息合并、行业映射补全、存量字段回填
与 sync_tdx_stock_to_database 编排。对外由 sync_service 聚合导出。
"""

import re
from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from src.data.services.sync_common import (
    SyncFlow,
    _execute_sync_flow,
    _extract_tdx_first_last_trade_date,
    _is_fund_ts_code,
    _is_index_ts_code,
    _normalize_date_value,
    _normalize_text_value,
    _remote_sync_blocked,
    _sync_each_with_recovery,
    _to_ts_code_or_none,
)
from src.data.sources import baostock_schema as schema
from src.utils.exceptions import DataSourceException

# ======================================================================
# 股票基本信息同步
# ======================================================================

def _fetch_external_stock_basic(baostock_handler: Any, akshare_handler: Any) -> tuple[Any, Any, dict[str, str]]:
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


def _fetch_baostock_industry_map(baostock_handler: Any) -> dict[str, str]:
    """从 Baostock 获取股票行业映射（ts_code -> industry）。"""
    import baostock as bs
    import polars as pl

    industry_map: dict[str, str] = {}
    if not baostock_handler or not baostock_handler._ensure_baostock_login():
        return industry_map

    rs = bs.query_stock_industry()
    industry_pd = schema.result_to_frame(rs)
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


def _select_akshare_column(frame: Any, candidates: Any) -> str | None:
    """从 AkShare DataFrame 中选第一个存在且有值的候选列。"""
    return next(
        (c for c in candidates if c in frame.columns and frame[c].notna().any()),
        None,
    )


def _collect_akshare_board_names(board_pd: Any) -> list[str] | None:
    """从行业板块列表提取板块名称，列表不可用时返回 None。"""
    if board_pd is None or board_pd.empty:
        logger.warning("AkShare 行业板块列表为空")
        return None

    name_col = _select_akshare_column(board_pd, ('板块名称', '名称', 'name'))
    if name_col is None:
        logger.warning("AkShare 行业板块列表缺少名称列")
        return None

    return [str(v).strip() for v in board_pd[name_col].tolist() if v and str(v).strip()]


def _add_board_codes_to_industry_map(industry_map: dict[str, str], raw_codes: Any, board_name: str) -> None:
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


def _collect_board_industry_map(ak: Any, board_names: list[str]) -> dict[str, str]:
    """逐板块拉取成分股，生成 ts_code -> 行业映射。"""
    industry_map: dict[str, str] = {}
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
        # 兜底理由：单个板块请求异常（连接中断/解析失败）跳过该板块，继续兜底其余板块
        except Exception as board_e:  # noqa: BLE001
            logger.warning(f"获取行业板块 {board_name} 成分失败: {board_e}")
            continue

    return industry_map


def _fetch_akshare_industry_map() -> dict[str, str]:
    """从 AkShare 行业板块成分获取行业映射（ts_code -> industry）。"""
    import akshare as ak

    try:
        board_pd = ak.stock_board_industry_name_em()
        board_names = _collect_akshare_board_names(board_pd)
        if board_names is None:
            return {}

        logger.info(f"AkShare 行业板块数量: {len(board_names)}")
        return _collect_board_industry_map(ak, board_names)
    # 兜底理由：行业字段只是补充信息，板块接口偶发连接中断不能让整轮同步终止
    except Exception as e:  # noqa: BLE001
        logger.warning(f"从 AkShare 获取行业映射失败: {e}")
        return {}


def _create_stock_basic_from_info(ts_code: str, stock_info: dict[str, Any]) -> Any:
    """从股票信息字典创建 StockBasic 模型对象。"""
    from src.database.models.stock import StockBasic

    name = stock_info.get('name', '未知')
    name = name[:45] if len(name) > 45 else name

    symbol = stock_info.get('symbol', ts_code.split('.')[0])
    symbol = symbol[:9] if len(symbol) > 9 else symbol

    # 两个外部源都不提供板块字段，新增行直接按代码推断，避免写入 market=None
    market = _normalize_text_value(stock_info.get('market'), 20) or _infer_stock_market_from_ts_code(ts_code)

    return StockBasic(
        ts_code=ts_code,
        symbol=symbol,
        name=name,
        area=stock_info.get('area'),
        industry=stock_info.get('industry'),
        market=market or None,
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


STOCK_INSERT_BATCH_SIZE = 200


def _flush_stock_batch(session: Any, pending: list[Any], result: dict[str, Any]) -> None:
    """批量写入一批 StockBasic 记录（一次 add_all + 一次 commit）。

    原先逐股 ``session.add`` 且每只固定 ``sleep(0.1)``，5000 只股票仅 sleep
    就有 500s 的硬性下限。改为攒批写入，批量大小与网络/DB 能力挂钩，
    不再有与标的数量线性相关的固定等待。
    """
    if not pending:
        return
    try:
        session.add_all([stock for _, stock in pending])
        session.commit()
        logger.info(f"已批量写入 {len(pending)} 条股票记录到数据库")
    # 兜底理由：批量提交失败回滚，本批计入失败清单
    except Exception as commit_e:  # noqa: BLE001
        logger.exception(f"批量写入股票记录失败: {commit_e}")
        session.rollback()
        result['inserted_stocks'] -= len(pending)
        result['failed_stocks'].extend(ts_code for ts_code, _ in pending)


def _process_single_stock_batch(ctx: StockBatchContext) -> None:
    """处理单批股票（新增阶段）：攒批写入，逐股仅做内存字典查询。"""
    pending = []

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
            pending.append((ts_code, stock))
            ctx.result['inserted_stocks'] += 1
            ctx.existing_codes.add(ts_code)

            if len(pending) >= STOCK_INSERT_BATCH_SIZE:
                _flush_stock_batch(ctx.session, pending, ctx.result)
                pending = []

        # 兜底理由：单只股票处理失败不中断整批
        except Exception as e:  # noqa: BLE001
            logger.exception(f"处理股票 {ts_code} 失败: {e}")
            ctx.result['failed_stocks'].append(ts_code)
            continue

    _flush_stock_batch(ctx.session, pending, ctx.result)


def _finalize_database_commit(session: Any, result: dict[str, Any]) -> None:
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


def _log_sync_result(result: dict[str, Any]) -> None:
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


def _normalize_stock_status(status: Any) -> str:
    """将外部状态值归一化到 L/D/P。"""
    raw = str(status or '').strip().upper()
    if raw in {'L', '1', 'LISTED'}:
        return 'L'
    if raw in {'D', '0', 'DELIST', 'DELISTED'}:
        return 'D'
    if raw in {'P', 'SUSPEND', 'PAUSE'}:
        return 'P'
    return 'L'


def _infer_stock_market_from_ts_code(ts_code: str | None) -> str:
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
        if symbol.startswith(('300', '301')):
            return '创业板'
        return '主板'
    return ''


def _update_stock_text_field(row: Any, field_name: str, source_value: Any, result: dict[str, Any], max_len: int | None = None) -> bool:
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


def _update_stock_date_field(row: Any, field_name: str, source_value: Any, result: dict[str, Any]) -> bool:
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
    session: object | None = None
    tdx_handler: object | None = None
    baostock_handler: object | None = None
    akshare_handler: object | None = None
    existing_codes: set[str] = field(default_factory=set)
    new_codes: list[str] | None = None
    tdx_stock_codes: list[str] | None = None
    merged_basic: dict[str, Any] = field(default_factory=dict)


def _first_stock_field(row: Any, *keys: str) -> Any:
    """按候选字段顺序取第一个非空值，全部缺失返回空串。"""
    for key in keys:
        value = row.get(key)
        if value:
            return value
    return ''


def _is_blank_stock_field(value: Any) -> bool:
    """判断字段是否为空/未知占位值（用于「只补空缺、不覆盖」的合并口径）。"""
    if value is None:
        return True
    text = str(value).strip()
    return not text or text in {'未知', '--', 'N/A'}


def _build_stock_info(ts_code: str, symbol: str, row: Any, source: str) -> dict[str, Any]:
    """构建股票信息字典（统一多源字段名）。

    `industry_source` 仅在内存中标记行业字段的提供方，用于后续合并时执行
    「Baostock 非空优先、AkShare 只补空缺」的口径；不写入数据库列。
    """
    industry = _first_stock_field(row, 'industry', '所属行业', '行业')
    return {
        'symbol': symbol,
        'name': _first_stock_field(row, 'code_name', 'name', '名称'),
        'area': _first_stock_field(row, 'area', 'region', '地域', '地区'),
        'industry': industry,
        'industry_source': source if industry else '',
        'market': _first_stock_field(row, 'market', '市场'),
        'list_date': _first_stock_field(row, 'list_date', 'ipoDate', '上市日期') or None,
        'delist_date': _first_stock_field(row, 'delist_date', 'outDate', '退市日期') or None,
        'status': _first_stock_field(row, 'status', '上市状态') or 'L',
        'source': source
    }


def _merge_baostock_basic(merged: dict[str, Any], baostock_df: Any, existing_codes: set[str]) -> None:
    """并入 Baostock 基本信息，已存在或非法代码跳过。"""
    if baostock_df is None or baostock_df.is_empty():
        return

    for row in baostock_df.iter_rows(named=True):
        code = row.get('code', '')
        ts_code = _to_ts_code_or_none(code)

        if ts_code is None or ts_code in existing_codes:
            continue

        merged[ts_code] = _build_stock_info(ts_code, code, row, 'baostock')


def _merge_akshare_basic(merged: dict[str, Any], akshare_df: Any, existing_codes: set[str]) -> None:
    """并入 AkShare 基本信息；已有 Baostock 值时只补空缺，不覆盖。"""
    if akshare_df is None or akshare_df.is_empty():
        return

    for row in akshare_df.iter_rows(named=True):
        code = _first_stock_field(row, 'code', '代码', 'symbol')
        ts_code = _to_ts_code_or_none(code)

        if ts_code is None or ts_code in existing_codes:
            continue

        merged_info = _build_stock_info(ts_code, code, row, 'akshare')
        if ts_code in merged:
            target = merged[ts_code]
            for key in ('name', 'area', 'industry', 'market',
                        'list_date', 'delist_date', 'status'):
                if not _is_blank_stock_field(target.get(key)):
                    continue
                new_value = merged_info.get(key)
                if _is_blank_stock_field(new_value):
                    continue
                target[key] = new_value
                if key == 'industry':
                    target['industry_source'] = 'akshare'
        else:
            merged[ts_code] = {
                'symbol': code,
                'name': merged_info.get('name', ''),
                'area': merged_info.get('area', ''),
                'industry': merged_info.get('industry', ''),
                'industry_source': merged_info.get('industry_source', ''),
                'market': merged_info.get('market', ''),
                'list_date': merged_info.get('list_date'),
                'delist_date': merged_info.get('delist_date'),
                'status': merged_info.get('status', 'L'),
                'source': 'akshare'
            }


def merge_stock_basic(baostock_df: Any, akshare_df: Any, existing_codes: set[str] | None = None) -> dict[str, Any]:
    """合并 Baostock 和 AkShare 的股票基本信息（Baostock 非空优先）。"""
    if existing_codes is None:
        existing_codes = set()

    merged: dict[str, Any] = {}
    _merge_baostock_basic(merged, baostock_df, existing_codes)
    _merge_akshare_basic(merged, akshare_df, existing_codes)

    source_stats: dict[str, int] = {}
    for info in merged.values():
        if _is_blank_stock_field(info.get('industry')):
            continue
        key = str(info.get('industry_source') or 'unknown')
        source_stats[key] = source_stats.get(key, 0) + 1

    logger.info(
        f"合并后共有 {len(merged)} 条股票基本信息（已过滤已存在的 {len(existing_codes)} 条）；"
        f"行业来源统计: {source_stats}"
    )
    return merged


def get_stock_info_from_apis(ts_code: str, merged_basic: dict[str, Any], baostock_handler: Any, akshare_handler: Any) -> dict[str, Any] | None:
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


def _prune_non_stock_rows(session: Any, result: dict[str, Any]) -> tuple[set[str], set[str]]:
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


def _apply_baostock_industry(merged_basic: dict[str, Any], baostock_industry_map: dict[str, str]) -> None:
    """把 Baostock 行业映射并入 merged_basic（非空优先，覆盖 AkShare 填充值）。"""
    if not baostock_industry_map:
        return

    for ts_code, industry in baostock_industry_map.items():
        if ts_code not in merged_basic:
            symbol = ts_code.split('.')[0] if '.' in ts_code else ts_code
            merged_basic[ts_code] = {
                'symbol': symbol, 'name': '', 'area': '', 'industry': industry,
                'industry_source': 'baostock',
                'market': '', 'list_date': None, 'status': 'L', 'source': 'baostock_industry'
            }
        else:
            target = merged_basic[ts_code]
            current_source = str(target.get('industry_source') or '')
            # Baostock 非空值优先：仅当前行业为空/未知，或由 AkShare 填入时覆盖；
            # 来源未知的既有有效行业保持不变
            if _is_blank_stock_field(target.get('industry')) or current_source == 'akshare':
                target['industry'] = industry
                target['industry_source'] = 'baostock'


def _apply_akshare_industry_fallback(merged_basic: dict[str, Any], tdx_stock_codes: list[str]) -> int:
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
                    'industry_source': 'akshare',
                    'market': '', 'list_date': None, 'status': 'L', 'source': 'akshare_industry'
                }
            else:
                # AkShare 兜底只补真正的空值，不覆盖任何既有行业文本
                if not str(merged_basic[ts_code].get('industry', '') or '').strip():
                    merged_basic[ts_code]['industry'] = industry
                    merged_basic[ts_code]['industry_source'] = 'akshare'
        logger.info(f"AkShare 行业兜底完成，补充映射 {len(akshare_industry_map)} 只股票")
    return len(akshare_industry_map)


def _backfill_text_fields(row: Any, source_info: dict[str, Any], result: dict[str, Any]) -> bool:
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

    # 行业口径：Baostock 非空值优先，AkShare 只补空缺、不覆盖存量值
    if source_industry and str(source_info.get('industry_source') or '') == 'akshare':
        if not _is_blank_stock_field(row.industry):
            source_industry = ''
    if _update_stock_text_field(row, 'industry', source_industry, result, max_len=50):
        changed = True
    if _update_stock_text_field(row, 'market', source_market, result, max_len=20):
        changed = True

    inferred_market = _infer_stock_market_from_ts_code(row.ts_code)
    if inferred_market and not source_market:
        if _update_stock_text_field(row, 'market', inferred_market, result, max_len=20):
            changed = True

    return changed


def _backfill_list_date(row: Any, source_info: dict[str, Any], session: Any, config: Any, result: dict[str, Any]) -> bool:
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


def _backfill_status(row: Any, source_info: dict[str, Any], result: dict[str, Any]) -> bool:
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


def _backfill_stock_row(row: Any, source_info: dict[str, Any], session: Any, config: Any, result: dict[str, Any]) -> bool:
    """回填单只存量股票的文本、上市/退市日期与状态，返回是否有改动。"""
    changed = _backfill_text_fields(row, source_info, result)

    if _backfill_list_date(row, source_info, session, config, result):
        changed = True

    if _update_stock_date_field(row, 'delist_date', source_info.get('delist_date'), result):
        changed = True

    if _backfill_status(row, source_info, result):
        changed = True

    return changed


def _prepare_stock_sync(config: Any, db_manager: Any, session: Any, result: dict[str, Any]) -> StockSyncContext | None:
    """同步准备阶段：扫描通达信代码、清理非股票记录并准备外部基本信息。"""
    from src.data.akshare_handler import AkShareHandler
    from src.data.baostock_handler import BaostockHandler
    from src.data.tdx_handler import TdxHandler

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

    # 外部 handler 只用它们取数，不传 db_manager：两者在持有数据库会话时会把
    # 股票基本信息直接写库（AkShare 仅写 ts_code/symbol/name，Baostock 只写默认
    # 三只），绕开本流程的字段回填，且先于同步提交，导致后续批次因主键冲突
    # 整批回滚、新增行只剩名称。stock_basic 的唯一写入方是本同步流程。
    baostock_handler = BaostockHandler(config, None)
    akshare_handler = AkShareHandler(config, None)

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


def _run_stock_sync(ctx: StockSyncContext, session: Any, config: Any, result: dict[str, Any]) -> None:
    """同步执行阶段：新增股票并回填 stock_basic 存量信息，最后提交。"""
    from src.database.models.stock import StockBasic

    if ctx.new_codes:
        _process_single_stock_batch(StockBatchContext(
            ctx.new_codes, ctx.merged_basic, ctx.existing_codes, session,
            ctx.baostock_handler, ctx.akshare_handler, result
        ))
    else:
        logger.info("所有通达信股票都已存在，跳过新增阶段，继续执行存量回填")

    # 第二阶段：回填并刷新 stock_basic 存量信息
    def _backfill(row: Any) -> None:
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


def sync_tdx_stock_to_database(config: Any, db_manager: Any) -> dict[str, Any]:
    """
    从通达信数据源同步股票信息到数据库。

    工作流程：
    1. 从通达信提取所有股票代码
    2. 与数据库 stock_basic 表匹配
    3. 新增股票：通过 baostock/akshare 获取完整信息后插入
    4. 存量股票：回填/刷新空白字段
    """
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
        'skipped_reason': '',
    }

    # 股票基本信息依赖 Baostock/AkShare 补全，远程开关关闭时显式跳过而非空跑
    if _remote_sync_blocked(config, "同步股票基本信息"):
        result['skipped_reason'] = 'remote_sources_disabled'
        return result

    def prepare(session: Any) -> StockSyncContext | None:
        return _prepare_stock_sync(config, db_manager, session, result)

    def run(ctx: StockSyncContext, session: Any) -> None:
        _run_stock_sync(ctx, session, config, result)

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
# stock_basic 存量修复（area / industry / market）
# ======================================================================

# 证监会行业门类写法：单个门类字母 + 空白 + 门类名，如 "C 制造业"、"I 信息技术"
_INDUSTRY_CATEGORY_PATTERN = re.compile(r'^\s*[A-Za-z]\s')


def _is_industry_category_value(value: Any) -> bool:
    """判断 area 字段的值是否被误写成了行业门类。"""
    return bool(_INDUSTRY_CATEGORY_PATTERN.match(str(value or '')))


def _build_repair_industry_map(config: Any) -> dict[str, str]:
    """合并 Baostock 行业分类与 AkShare 行业板块成分，作为存量行业回填来源。"""
    from src.data.baostock_handler import BaostockHandler

    industry_map: dict[str, str] = {}
    try:
        industry_map.update(
            _fetch_baostock_industry_map(BaostockHandler(config, None))
        )
        logger.info(f"Baostock 行业映射 {len(industry_map)} 条")
    # 兜底理由：单一行业源失败不应阻断另一源
    except Exception as e:  # noqa: BLE001
        logger.warning(f"获取 Baostock 行业映射失败: {e}")

    akshare_map = _fetch_akshare_industry_map()
    for ts_code, industry in akshare_map.items():
        industry_map.setdefault(ts_code, industry)
    logger.info(f"行业回填来源合计 {len(industry_map)} 只股票")
    return industry_map


@dataclass
class StockRepairContext:
    """stock_basic 存量修复上下文"""
    rows: list[Any]
    industry_map: dict[str, str]


def _fetch_industry_from_cninfo(ak: Any, ts_code: str) -> str:
    """逐只查询巨潮公司概况获取所属行业，失败或无数据返回空串。"""
    symbol = ts_code.split('.')[0]
    try:
        profile_pd = ak.stock_profile_cninfo(symbol=symbol)
    # 兜底理由：单只个股资料接口失败（退市/无覆盖/连接中断）跳过该只
    except Exception as e:  # noqa: BLE001
        logger.debug(f"获取 {ts_code} 巨潮公司概况失败: {e}")
        return ''

    if profile_pd is None or profile_pd.empty:
        return ''
    return _normalize_text_value(profile_pd.iloc[0].get('所属行业'), 50)


def _build_industry_fetcher() -> Any:
    """构造逐只行业兜底抓取器，附带进度日志。"""
    import akshare as ak

    attempts = {'count': 0, 'success': 0}

    def fetch(ts_code: str) -> str:
        attempts['count'] += 1
        industry = _fetch_industry_from_cninfo(ak, ts_code)
        if industry:
            attempts['success'] += 1
        if attempts['count'] % 50 == 0:
            logger.info(
                f"逐只行业兜底进度: {attempts['count']} 次查询，"
                f"成功 {attempts['success']} 条"
            )
        return industry

    return fetch


def _repair_stock_row(row: Any, industry_map: dict[str, str], result: dict[str, Any],
                      industry_fetcher: Any = None) -> None:
    """修复单只股票：清空误写的 area、推断缺失的 market、补齐缺失的 industry。

    industry_fetcher 为可选的逐只兜底抓取器（ts_code -> industry），
    仅在批量映射也覆盖不到时才会被调用。
    """
    if _is_industry_category_value(row.area):
        row.area = None
        result['cleared_area'] += 1

    if not _normalize_text_value(row.market):
        inferred_market = _infer_stock_market_from_ts_code(row.ts_code)
        if inferred_market:
            row.market = inferred_market
            result['filled_market'] += 1

    if not _normalize_text_value(row.industry):
        industry = _normalize_text_value(industry_map.get(row.ts_code), 50)
        if not industry and industry_fetcher is not None:
            industry = industry_fetcher(row.ts_code)
        if industry:
            row.industry = industry
            result['filled_industry'] += 1


def _log_repair_result(result: dict[str, Any]) -> None:
    """记录 stock_basic 修复结果统计。"""
    logger.info("=" * 60)
    logger.info("stock_basic 修复结果统计:")
    logger.info(f"  待修复股票数量: {result['total_stocks']}")
    logger.info(f"  清空错误地域数量: {result['cleared_area']}")
    logger.info(f"  补全市场类型数量: {result['filled_market']}")
    logger.info(f"  补全行业数量: {result['filled_industry']}")
    logger.info(f"  逐只兜底查询数量: {result['deep_fetched']}")
    logger.info(f"  失败股票数量: {len(result['failed_stocks'])}")
    if result['failed_stocks']:
        logger.warning(f"  失败股票列表: {result['failed_stocks'][:10]}...")
    logger.info("=" * 60)


def repair_stock_basic(config: Any, db_manager: Any, deep_industry: bool = False) -> dict[str, Any]:
    """一次性修复 stock_basic 存量记录的 area/industry/market 缺口。

    - area：该列语义为地域，被误写成证监会行业门类的值直接置空，不做地域抓取
    - market：空值按股票代码推断板块
    - industry：空值用 Baostock 行业分类 + AkShare 行业板块成分批量补全
    - deep_industry=True 时，批量源覆盖不到的再逐只查巨潮公司概况兜底
    """
    from src.database.models.stock import StockBasic

    result: dict[str, Any] = {
        'total_stocks': 0,
        'cleared_area': 0,
        'filled_market': 0,
        'filled_industry': 0,
        'deep_fetched': 0,
        'failed_stocks': [],
        'skipped_reason': '',
    }

    # 行业回填依赖 Baostock/AkShare（deep_industry 还会逐只查巨潮），
    # 远程开关关闭时显式跳过，避免离线运行偷偷联网
    if _remote_sync_blocked(config, "修复 stock_basic"):
        result['skipped_reason'] = 'remote_sources_disabled'
        return result

    def prepare(session: Any) -> StockRepairContext:
        rows = [
            row for row in session.query(StockBasic).all()
            if not _is_index_ts_code(row.ts_code)
            and not _is_fund_ts_code(row.ts_code, row.name, row.market)
        ]
        result['total_stocks'] = len(rows)
        logger.info(f"stock_basic 待修复股票 {len(rows)} 只")
        return StockRepairContext(rows, _build_repair_industry_map(config))

    def run(ctx: StockRepairContext, session: Any) -> None:
        industry_fetcher = None
        if deep_industry:
            logger.info("已开启逐只行业兜底，批量源未覆盖的股票将逐只查询巨潮公司概况")
            fetch_industry = _build_industry_fetcher()

            def industry_fetcher(ts_code: str) -> str:
                result['deep_fetched'] += 1
                return fetch_industry(ts_code)

        _sync_each_with_recovery(
            ctx.rows,
            lambda row: _repair_stock_row(row, ctx.industry_map, result, industry_fetcher),
            result=result, failed_key='failed_stocks', session=session,
            label='股票', commit_interval=100, item_name=lambda row: row.ts_code,
        )
        try:
            session.commit()
            logger.info("stock_basic 修复结果已提交")
        # 兜底理由：提交失败回滚，由调用方读取失败清单
        except Exception as commit_e:  # noqa: BLE001
            logger.exception(f"提交 stock_basic 修复结果失败: {commit_e}")
            session.rollback()

    return _execute_sync_flow(
        SyncFlow(
            title="修复 stock_basic 存量的地域/行业/市场字段",
            error_label="修复 stock_basic",
            result=result,
            prepare=prepare,
            run=run,
            log_stats=_log_repair_result,
        ),
        db_manager,
    )
