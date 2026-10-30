#!/usr/bin/env python3

"""
基金基本信息同步业务线

从 sync_service.py 拆出：stock_basic 基金记录迁移、AkShare ETF 补全、
通达信日期回填与 sync_stock_fund_to_fund_basic 编排。
对外由 sync_service 聚合导出。
"""

from loguru import logger

from src.data.services.sync_common import (
    _extract_tdx_first_last_trade_date,
    _is_fund_ts_code,
)
from src.utils.exceptions import DataSaveError, DataSourceException

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
        # 兜底理由：单只基金日期回填失败计数后继续
        except Exception as date_fill_e:  # noqa: BLE001
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
            # 兜底理由：单条基金记录迁移失败计数后继续
            except Exception as row_e:  # noqa: BLE001
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
