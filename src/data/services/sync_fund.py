#!/usr/bin/env python3

"""
基金基本信息同步业务线

从 sync_service.py 拆出：stock_basic 基金记录迁移、AkShare ETF 补全、
通达信日期回填与 sync_stock_fund_to_fund_basic 编排。
对外由 sync_service 聚合导出。
"""

from datetime import date
from typing import Any, cast

from loguru import logger
from sqlalchemy.orm import Session

from src.data.services.sync_common import (
    _extract_tdx_first_last_trade_date,
    _is_fund_ts_code,
    _remote_sync_blocked,
)
from src.utils.exceptions import DataSaveError, DataSourceException

# ======================================================================
# 基金同步
# ======================================================================

def _infer_fund_type(symbol: str, name: str | None) -> str:
    """推断基金类型。"""
    nm = (name or '').upper()
    if 'ETF' in nm:
        return 'ETF'
    if 'LOF' in nm or symbol.startswith(('16', '18')):
        return 'LOF'
    return '基金'


def _is_placeholder_fund_name(name: str | None, symbol: str) -> bool:
    """判断基金名称是否为占位（空 / 等于代码 / 「股票+代码」）。"""
    text = (name or '').strip()
    return not text or text == symbol or text == f"股票{symbol}"


def _migrate_fund_stock_tuple(stock: Any) -> tuple[str, str, str, str]:
    """由 stock_basic 记录派生基金的代码、名称、类型与市场。

    占位名不迁移：宁可 name 为空并记为缺名，也不把代码伪装成名称。
    """
    symbol = (stock.symbol or stock.ts_code.split('.')[0])[:9]
    raw_name = (stock.name or '').strip()
    name = '' if _is_placeholder_fund_name(raw_name, symbol) else raw_name[:20]
    return symbol, name, _infer_fund_type(symbol, name), stock.ts_code.split('.')[-1]


def _fill_fund_from_stock(fund: Any, stock: Any, symbol: str, name: str,
                          inferred_type: str, inferred_market: str) -> bool:
    """用 stock_basic 记录补全 fund_basic 空白字段，返回是否有改动。"""
    changed = False
    if not fund.symbol and symbol:
        fund.symbol = symbol
        changed = True
    if (not fund.name or _is_placeholder_fund_name(fund.name, symbol)) and name:
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


def _migrate_fund_stock_row(stock: Any, existing_fund_map: dict[str, Any], session: Session,
                            result: dict[str, Any]) -> None:
    """把单条 stock_basic 基金记录迁移或更新到 fund_basic。"""
    from src.database.models.fund import FundBasic

    symbol, name, inferred_type, inferred_market = _migrate_fund_stock_tuple(stock)

    # 无真实名称时不新建记录：避免把代码/空串写成基金名称，缺名标的单独记账
    if not name:
        result['missing_name_funds'].append(stock.ts_code)
        return

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


def _infer_fund_ts_code_by_symbol(symbol: str) -> str | None:
    """按基金代码前缀推断 ETF 的 ts_code，无法识别返回 None。"""
    if symbol.startswith('51') or symbol.startswith('58'):
        return f"{symbol}.SH"
    if symbol.startswith('15') or symbol.startswith('16'):
        return f"{symbol}.SZ"
    return None


def _merge_etf_row(row: dict[str, Any], existing_fund_map: dict[str, Any], session: Session,
                   result: dict[str, Any]) -> None:
    """用 AkShare ETF 行补全或新增 fund_basic 记录。"""
    from src.database.models.fund import FundBasic

    symbol = str(row.get('基金代码', '')).strip()
    name = str(row.get('基金简称', '')).strip()
    if not symbol:
        return

    ts_code = _infer_fund_ts_code_by_symbol(symbol)
    if not ts_code:
        return

    # 上游缺名时不入库，避免把代码伪装成名称
    if _is_placeholder_fund_name(name, symbol):
        result['missing_name_funds'].append(ts_code)
        return

    fund = existing_fund_map.get(ts_code)
    if fund is None:
        fund = FundBasic(
            ts_code=ts_code, symbol=symbol[:9], name=name[:20],
            fund_type='ETF', market=ts_code.split('.')[-1], status='L',
        )
        session.add(fund)
        existing_fund_map[ts_code] = fund
        result['inserted_funds'] += 1
        result['akshare_enriched'] += 1
        return

    changed = False
    if name and ((not fund.name) or _is_placeholder_fund_name(fund.name, symbol)):
        fund.name = name[:20]
        changed = True
    if not fund.fund_type:
        fund.fund_type = 'ETF'
        changed = True
    if changed:
        result['updated_funds'] += 1
        result['akshare_enriched'] += 1


def _enrich_funds_from_akshare(config: Any, existing_fund_map: dict[str, Any], session: Session,
                               result: dict[str, Any]) -> None:
    """使用 AkShare ETF 列表补全基金名称与类型。"""
    # 基金迁移本身只读写本地库，仅 AkShare 补全属于远程能力，受同一开关控制
    if _remote_sync_blocked(config, "使用 AkShare 补全基金信息"):
        return

    from src.data.akshare_handler import AkShareHandler

    try:
        akshare_handler = AkShareHandler(config, None)
        etf_df = akshare_handler.update_etf_basic()
        if etf_df is not None and not etf_df.is_empty():
            for row in etf_df.iter_rows(named=True):
                _merge_etf_row(row, existing_fund_map, session, result)
    except DataSourceException as e:
        logger.warning(f"使用 AkShare 补全基金信息失败: {e}")


def _fill_fund_date_row(fund: Any, config: Any, tdx_date_cache: dict[str, Any],
                        result: dict[str, Any]) -> bool:
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


def _fill_fund_dates(config: Any, session: Session, result: dict[str, Any]) -> None:
    """使用通达信日线数据补全基金日期信息。"""
    from src.database.models.fund import FundBasic

    tdx_date_cache: dict[str, tuple[date | None, date | None]] = {}
    all_funds = session.query(FundBasic).all()
    for fund in all_funds:
        try:
            if _fill_fund_date_row(fund, config, tdx_date_cache, result):
                result['updated_funds'] += 1
        # 兜底理由：单只基金日期回填失败计数后继续
        except Exception as date_fill_e:  # noqa: BLE001
            logger.exception(f"回填基金日期失败 {fund.ts_code}: {date_fill_e}")
            result['failed'] += 1


def _delete_migrated_funds(session: Session, fund_stocks: list[Any],
                           result: dict[str, Any]) -> None:
    """从 stock_basic 删除已迁移的基金记录。"""
    for stock in fund_stocks:
        session.delete(stock)
    result['deleted_from_stock_basic'] = len(fund_stocks)


def _classify_local_fund_symbol(symbol: str, name: str | None = None,
                                ) -> tuple[str | None, str] | None:
    """判定本地基金的类型与归属表，无法可靠判定返回 None。

    名称可得时以名称优先（代码段无法区分同段内的分级与转型后 LOF）：
    - 名称含「分级」→ 分级（母份额与 A/B 子份额同为分级，分子母靠名称后缀，
      不新增词表值；见 2026-11-10 fund_type 核查）
    - 名称含「联接」或「LOF」→ LOF（ETF 联接基金是 LOF 份额，不是 ETF）

    名称缺失时按代码段兜底：
    - ETF：沪 51/52/56/58/530/551，深 159/158（530=上证系列、551=科创债，
      经 tushare fund_basic(market=E) 逐只核实，各 14/12 只）
    - LOF：沪 501/506/508，深 16/18（REITs 沿用库内既有的 LOF 归类）
    - 分级：沪 502（502 段既有分级母/子份额，也有整改后的普通 LOF，故有名称时按名称判）
    - 封闭式：沪 500/505（无类型来源，仅补基础表，不猜 fund_type）
    """
    text = (name or '').strip()
    if text:
        if '分级' in text:
            return '分级', 'fund_basic'
        if '联接' in text or 'LOF' in text.upper():
            return 'LOF', 'fund_basic'
    if symbol.startswith(('51', '52', '56', '58', '530', '551')) or symbol.startswith(('159', '158')):
        return 'ETF', 'fund_basic'
    if symbol.startswith(('501', '506', '508', '16', '18')):
        return 'LOF', 'fund_basic'
    if symbol.startswith(('150', '502')):
        return '分级', 'fund_basic'
    if symbol.startswith(('500', '505')):
        return None, 'closed_fund_basic'
    return None


def backfill_fund_basic_from_local_tdx(config: Any, db_manager: Any,
                                       dry_run: bool = False) -> dict[str, Any]:
    """用本地通达信基金 ``.day`` 补齐 fund_basic / closed_fund_basic。

    只补「确定可得」的字段：代码、市场、首个本地行情日、按代码段推断的类型。
    名称与退市状态没有本地权威来源，一律留空并在计划清单中标注，不猜。
    已存在的记录只填空字段，不覆盖任何已有值（upsert 语义）。
    """
    from pathlib import Path

    from src.data.local_store.build import discover_local_files
    from src.data.tdx_day_parser import read_first_last_trade_date
    from src.database.models.fund import ClosedFundBasic, FundBasic

    logs_dir = Path('logs')
    logs_dir.mkdir(exist_ok=True)
    plan_path = logs_dir / 'fund_backfill_plan.txt'

    result: dict[str, Any] = {
        'scanned': 0, 'inserted_fund_basic': 0, 'inserted_closed': 0,
        'updated': 0, 'unknown_type': 0, 'by_type': {}, 'dry_run': dry_run,
        'plan_path': str(plan_path),
    }

    discovered = discover_local_files(Path(config.data.tdx_data_path), 'fund')
    session = db_manager.get_session()
    try:
        existing_fund = {f.ts_code: f for f in session.query(FundBasic).all()}
        existing_closed = {f.ts_code: f for f in session.query(ClosedFundBasic).all()}

        plan_lines: list[str] = []
        for _kind, ts_code, path in sorted(discovered, key=lambda x: x[1]):
            result['scanned'] += 1
            symbol, market = ts_code.split('.')
            market = market.lower()
            classified = _classify_local_fund_symbol(symbol)
            if classified is None:
                result['unknown_type'] += 1
                plan_lines.append(f'{ts_code}\t-\t-\t\t\tSKIP\t代码段无法可靠归类')
                continue
            fund_type, table = classified
            first, last = read_first_last_trade_date(path)
            last_note = ''
            if last and last < date(2025, 1, 1):
                last_note = f'尾部日期 {last}（本地停更/退市无法区分，status 留空）'

            if table == 'closed_fund_basic':
                obj = existing_closed.get(ts_code)
                is_new = obj is None
                action = 'INSERT' if is_new else 'EXIST'
                if is_new and not dry_run:
                    obj = ClosedFundBasic(ts_code=ts_code, symbol=symbol,
                                          market=market.upper(), list_date=first)
                    session.add(obj)
                    existing_closed[ts_code] = obj
                if obj is not None:
                    changed = _fill_fund_backfill_fields(obj, symbol, market, first)
                    if changed and not dry_run:
                        result['updated'] += 1
                if is_new:
                    result['inserted_closed'] += 1
            else:
                obj = existing_fund.get(ts_code)
                is_new = obj is None
                action = 'INSERT' if is_new else 'EXIST'
                if is_new and not dry_run:
                    obj = FundBasic(ts_code=ts_code, symbol=symbol, name=None,
                                    fund_type=fund_type, market=market.upper(),
                                    list_date=first, delist_date=None, status=None)
                    session.add(obj)
                    existing_fund[ts_code] = obj
                if obj is not None:
                    changed = _fill_fund_backfill_fields(obj, symbol, market, first)
                    if changed and not dry_run:
                        result['updated'] += 1
                if is_new:
                    result['inserted_fund_basic'] += 1
                result['by_type'][fund_type] = result['by_type'].get(fund_type, 0) + 1

            plan_lines.append(
                f'{ts_code}\t{table}\t{fund_type or "-"}\t{first}\t{last}\t{action}'
                f'{"(dry-run)" if dry_run else ""}\t{last_note}')

        if not dry_run:
            session.commit()
        else:
            session.rollback()

        plan_path.write_text('\n'.join(plan_lines) + '\n', encoding='utf-8')
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    logger.info(
        f"基金基础表补齐: 扫描 {result['scanned']}，"
        f"新增 fund_basic {result['inserted_fund_basic']}，"
        f"新增 closed_fund_basic {result['inserted_closed']}，"
        f"更新 {result['updated']}，无法归类 {result['unknown_type']}，"
        f"dry_run={dry_run}，计划清单 {plan_path}"
    )
    return result


def _fill_fund_backfill_fields(fund: Any, symbol: str, market: str,
                               first: date | None) -> bool:
    """只补空字段，不覆盖已有名称/状态/类型，返回是否有改动。"""
    changed = False
    if not fund.symbol:
        fund.symbol = symbol
        changed = True
    if not fund.market:
        fund.market = market.upper()
        changed = True
    if not fund.list_date and first:
        fund.list_date = first
        changed = True
    if hasattr(fund, 'fund_type') and not fund.fund_type:
        classified = _classify_local_fund_symbol(symbol, getattr(fund, 'name', None))
        if classified and classified[0]:
            fund.fund_type = classified[0]
            changed = True
    return changed


def sync_stock_fund_to_fund_basic(config: Any, db_manager: Any) -> dict[str, Any]:
    """将 stock_basic 中基金记录迁移到 fund_basic，并补全信息。"""
    from src.database.models.fund import FundBasic
    from src.database.models.stock import StockBasic

    result: dict[str, Any] = {
        'stock_fund_candidates': 0,
        'inserted_funds': 0,
        'updated_funds': 0,
        'deleted_from_stock_basic': 0,
        'akshare_enriched': 0,
        'list_date_filled': 0,
        'delist_date_filled': 0,
        'failed': 0,
        'missing_name_funds': [],
    }

    logger.info("=" * 60)
    logger.info("开始执行基金迁移: stock_basic -> fund_basic")
    logger.info("=" * 60)

    session: Session | None = None
    try:
        session = db_manager.get_session()

        all_stocks = cast(Any, session.query(StockBasic).all())
        fund_stocks = [
            row for row in all_stocks
            if _is_fund_ts_code(row.ts_code, row.name, row.market)
        ]
        result['stock_fund_candidates'] = len(fund_stocks)
        logger.info(f"识别到 {len(fund_stocks)} 条基金候选记录")

        existing_fund_map = {
            f.ts_code: f for f in cast(Any, session.query(FundBasic).all())
        }

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

        # 从 stock_basic 删除已迁移基金（缺名未入库的保留，避免静默丢数据）
        _delete_migrated_funds(
            session,
            [s for s in fund_stocks if s.ts_code in existing_fund_map],
            result,
        )

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
        logger.info(f"  缺名跳过入库: {len(result['missing_name_funds'])}")
        if result['missing_name_funds']:
            logger.warning(
                f"  缺名基金清单(前20): {result['missing_name_funds'][:20]}"
            )
        logger.info(f"  失败数: {result['failed']}")
        logger.info("=" * 60)

    return result
