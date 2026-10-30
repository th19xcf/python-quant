#!/usr/bin/env python3

"""
数据同步服务模块（聚合入口）

从 main.py 抽离的股票/指数/基金同步业务逻辑，按业务线拆分到：
    - sync_common.py  同步流程骨架、代码判定、字段归一化、通达信日线定位
    - sync_stock.py   股票基本信息同步
    - sync_index.py   指数基本信息同步
    - sync_fund.py    基金基本信息同步

本模块只做聚合导出，公开接口与拆分前保持一致：
    - sync_tdx_stock_to_database(config, db_manager)
    - sync_tdx_index_to_database(config, db_manager)
    - sync_stock_fund_to_fund_basic(config, db_manager)
"""

from src.data.services.sync_common import (
    SyncFlow,
    _execute_sync_flow,
    _extract_tdx_first_last_trade_date,
    _get_tdx_day_file_path,
    _is_fund_ts_code,
    _is_index_ts_code,
    _normalize_date_value,
    _normalize_text_value,
    _sync_each_with_recovery,
    _to_ts_code_or_none,
)
from src.data.services.sync_fund import (
    _delete_migrated_funds,
    _enrich_funds_from_akshare,
    _fill_fund_date_row,
    _fill_fund_dates,
    _fill_fund_from_stock,
    _infer_fund_ts_code_by_symbol,
    _infer_fund_type,
    _merge_etf_row,
    _migrate_fund_stock_row,
    _migrate_fund_stock_tuple,
    sync_stock_fund_to_fund_basic,
)
from src.data.services.sync_index import (
    FORCED_INDEX_NAME_MAP,
    IndexSyncContext,
    _backfill_index_row,
    _collect_akshare_index_info,
    _collect_baostock_index_info,
    _collect_external_index_name_map,
    _delete_stale_index_rows,
    _fill_index_blank_fields,
    _index_desc_text,
    _index_name_from_code,
    _infer_index_meta,
    _infer_index_publisher,
    _infer_index_type_category,
    _log_index_sync_result,
    _merge_akshare_index_row,
    _merge_baostock_index_row,
    _merge_index_info,
    _normalize_index_ts_code,
    _parse_index_date,
    _upsert_index_row,
    sync_tdx_index_to_database,
)
from src.data.services.sync_stock import (
    StockBatchContext,
    StockSyncContext,
    _add_board_codes_to_industry_map,
    _apply_akshare_industry_fallback,
    _apply_baostock_industry,
    _backfill_list_date,
    _backfill_status,
    _backfill_stock_row,
    _backfill_text_fields,
    _build_stock_info,
    _collect_akshare_board_names,
    _collect_board_industry_map,
    _create_stock_basic_from_info,
    _fetch_akshare_industry_map,
    _fetch_baostock_industry_map,
    _fetch_external_stock_basic,
    _finalize_database_commit,
    _first_stock_field,
    _infer_stock_market_from_ts_code,
    _log_sync_result,
    _merge_akshare_basic,
    _merge_baostock_basic,
    _normalize_stock_status,
    _process_single_stock_batch,
    _prune_non_stock_rows,
    _select_akshare_column,
    _update_stock_date_field,
    _update_stock_text_field,
    get_stock_info_from_apis,
    merge_stock_basic,
    sync_tdx_stock_to_database,
)
