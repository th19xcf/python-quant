#!/usr/bin/env python3

"""
指数基本信息同步业务线

从 sync_service.py 拆出：指数代码归一化、外部名称/日期收集、
index_basic 增删改与 sync_tdx_index_to_database 编排。
对外由 sync_service 聚合导出。
"""

import contextlib
from dataclasses import dataclass

from loguru import logger

from src.data.services.sync_common import (
    SyncFlow,
    _execute_sync_flow,
    _is_index_ts_code,
    _normalize_text_value,
    _sync_each_with_recovery,
    _to_ts_code_or_none,
)
from src.utils.code_converter import MARKET_BJ, MARKET_SH, MARKET_SZ
from src.utils.exceptions import DataSourceException

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
