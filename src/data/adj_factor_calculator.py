#!/usr/bin/env python3

"""
复权因子计算和存储模块
用于计算股票的前复权和后复权因子，并存储到数据库
统一使用Polars DataFrame
"""

from datetime import date, datetime
from typing import List

import numpy as np
import polars as pl
from loguru import logger
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from src.database.db_manager import DatabaseManager
from src.database.models.stock import StockAdjFactor, StockDaily, StockDividend
from src.database.upsert import upsert_rows
from src.utils.memory_optimizer import MemoryOptimizer


def calc_dividend_event_factor(prev_close, cash_div, share_div,
                               rights_ratio=0.0, rights_price=0.0,
                               max_factor: float = 10.0) -> float | None:
    """
    计算单次除权除息事件的前复权因子（所有复权计算的唯一公式收口点）。

    公式（含配股）：
        factor = (prev_close - cash_div + rights_price * rights_ratio)
                 / (prev_close * (1 + share_div + rights_ratio))

    前复权：ex_date 之前的所有行累乘 factor；
    后复权：ex_date 及之后的所有行累乘 1/factor。

    Args:
        prev_close: 除权除息日前一交易日收盘价
        cash_div: 每股现金分红
        share_div: 每股送转股比例
        rights_ratio: 配股比例
        rights_price: 配股价
        max_factor: 因子上限。个股默认 10；基金份额反向折算（1:0.01）可达 100，
            调用方需显式放宽，避免真实事件被静默丢弃。

    Returns:
        float: 事件因子；输入非法（价格缺失/分母为0/因子越界）时返回 None
    """
    if prev_close is None or prev_close <= 0:
        return None

    cash_div = cash_div or 0.0
    share_div = share_div or 0.0
    rights_ratio = rights_ratio or 0.0
    rights_price = rights_price or 0.0

    # 无任何权益变更
    if cash_div == 0 and share_div == 0 and rights_ratio == 0:
        return None

    numerator = prev_close - cash_div + rights_price * rights_ratio
    denominator = prev_close * (1 + share_div + rights_ratio)

    if denominator == 0:
        return None

    factor = numerator / denominator
    if factor <= 0 or factor > max_factor:
        return None
    return factor


class AdjFactorCalculator:
    """
    复权因子计算器

    功能：
    1. 计算前复权因子和后复权因子
    2. 计算复权价格
    3. 存储到数据库
    4. 支持增量更新
    5. 支持通达信数据源
    """

    def __init__(self, db_manager: DatabaseManager, tdx_handler=None):
        """
        初始化

        Args:
            db_manager: 数据库管理器实例
            tdx_handler: 通达信处理器实例（可选）
        """
        self.db_manager = db_manager
        self.session = db_manager.get_session()
        self.tdx_handler = tdx_handler

    def calculate_adj_factors(self, ts_code: str,
                             start_date: datetime | None = None,
                             end_date: datetime | None = None,
                             use_tdx: bool = True) -> pl.DataFrame | None:
        """
        计算单只股票的复权因子

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            use_tdx: 是否使用通达信数据（默认True）

        Returns:
            pl.DataFrame: 包含复权因子和复权价格
        """
        try:
            # 获取分红数据
            dividends = self._get_dividends(ts_code)
            if dividends is None or dividends.is_empty():
                logger.warning(f"{ts_code} 没有分红数据，复权因子设为1.0")
                return self._create_default_factors(ts_code, start_date, end_date, use_tdx)

            # 获取价格数据
            if use_tdx and self.tdx_handler:
                prices = self._get_prices_from_tdx(ts_code, start_date, end_date)
            else:
                prices = self._get_prices_from_db(ts_code, start_date, end_date)

            if prices is None or prices.is_empty():
                logger.warning(f"{ts_code} 没有价格数据")
                return None

            # 计算复权因子
            result = self.calculate_factors(prices, dividends)

            logger.info(f"{ts_code} 复权因子计算完成，共 {len(result)} 条记录")
            return result

        except (OSError, RuntimeError) as e:
            logger.exception(f"计算 {ts_code} 复权因子失败: {e}")
            return None

    def _get_dividends(self, ts_code: str) -> pl.DataFrame | None:
        """获取分红数据"""
        dividends = self.session.query(StockDividend).filter_by(ts_code=ts_code).order_by(StockDividend.ex_date).all()

        if not dividends:
            return None

        # 使用Polars直接构建DataFrame
        # 数值列统一转 float：`x or 0` 在 0/None 时产生 int，与小数混列会导致
        # polars 严格类型推断失败；转 float 不改变数值，只固定为 Float64。
        data = {
            'ts_code': [d.ts_code for d in dividends],
            'ex_date': [d.ex_date for d in dividends],
            'cash_div': [float(d.cash_div or 0) for d in dividends],
            'share_div': [float(d.share_div or 0) for d in dividends],
            'rights_issue_ratio': [float(d.rights_issue_ratio or 0) for d in dividends],
            'rights_issue_price': [float(d.rights_issue_price or 0) for d in dividends],
        }

        return pl.DataFrame(data)

    def _get_prices_from_db(self, ts_code: str, start_date: datetime | None, end_date: datetime | None) -> pl.DataFrame | None:
        """从数据库获取价格数据"""
        query = self.session.query(StockDaily).filter_by(ts_code=ts_code)

        if start_date:
            query = query.filter(StockDaily.trade_date >= start_date)
        if end_date:
            query = query.filter(StockDaily.trade_date <= end_date)

        prices = query.order_by(StockDaily.trade_date).all()

        if not prices:
            return None

        # 使用Polars直接构建DataFrame
        data = {
            'ts_code': [p.ts_code for p in prices],
            'trade_date': [p.trade_date for p in prices],
            'open': [p.open for p in prices],
            'high': [p.high for p in prices],
            'low': [p.low for p in prices],
            'close': [p.close for p in prices],
        }

        return pl.DataFrame(data)

    def _get_prices_from_tdx(self, ts_code: str, start_date: datetime | None, end_date: datetime | None) -> pl.DataFrame | None:
        """从通达信获取价格数据"""
        if not self.tdx_handler:
            logger.warning("通达信处理器未初始化")
            return None

        try:
            # 转换日期格式（通达信使用 YYYY-MM-DD 格式）
            start_str = start_date.strftime('%Y-%m-%d') if start_date else '1990-01-01'
            end_str = end_date.strftime('%Y-%m-%d') if end_date else datetime.now().strftime('%Y-%m-%d')

            # 获取股票代码（不带后缀）
            symbol = ts_code.split('.')[0]

            # 从通达信获取数据（get_kline_data 返回 LazyFrame，旧版本为 list[dict]）
            result = self.tdx_handler.get_kline_data(symbol, start_str, end_str)
            if isinstance(result, pl.LazyFrame):
                frame = result.collect()
            elif isinstance(result, pl.DataFrame):
                frame = result
            else:
                data_list = result or []
                if not data_list:
                    logger.warning(f"{ts_code} 从通达信获取数据失败")
                    return None
                frame = pl.DataFrame({
                    'date': [item['date'] for item in data_list],
                    'open': [item['open'] for item in data_list],
                    'high': [item['high'] for item in data_list],
                    'low': [item['low'] for item in data_list],
                    'close': [item['close'] for item in data_list],
                })

            required = {'date', 'open', 'high', 'low', 'close'}
            if frame.is_empty() or not required.issubset(set(frame.columns)):
                logger.warning(f"{ts_code} 从通达信获取数据为空或缺少列")
                return None

            frame = frame.select('date', 'open', 'high', 'low', 'close')
            return frame.rename({'date': 'trade_date'}).with_columns(
                pl.lit(ts_code).alias('ts_code')
            ).select('ts_code', 'trade_date', 'open', 'high', 'low', 'close')

        except (OSError, RuntimeError, TypeError) as e:
            logger.exception(f"从通达信获取价格数据失败: {e}")
            return None

    def _create_default_factors(self, ts_code: str, start_date: datetime | None, end_date: datetime | None, use_tdx: bool = True) -> pl.DataFrame | None:
        """创建默认复权因子（全部为1.0）"""
        # 获取价格数据
        if use_tdx and self.tdx_handler:
            prices_df = self._get_prices_from_tdx(ts_code, start_date, end_date)
        else:
            prices_df = self._get_prices_from_db(ts_code, start_date, end_date)

        if prices_df is None or prices_df.is_empty():
            return None

        # 添加默认复权因子
        result = prices_df.with_columns([
            pl.lit(1.0).alias('qfq_factor'),
            pl.lit(1.0).alias('hfq_factor'),
        ])

        # 内存优化：转换数据类型
        return MemoryOptimizer.optimize_dataframe(result)

    @staticmethod
    def calculate_factors(prices_df: pl.DataFrame, dividends_df: pl.DataFrame,
                          max_factor: float = 10.0,
                          as_of: date | None = None) -> pl.DataFrame:
        """
        计算复权因子（事件级向量化版本，静态方法，供各处复用）。

        实现方式：numpy searchsorted 把每个除权日映射到行号（O(E log N)），
        再对事件因子做后缀乘积，最后一趟 searchsorted 把因子广播回每行（O(N)）。
        相比逐事件 with_columns（每事件 2 次全表表达式），消除了 O(E×N) 的表扫描：
        实测 40 个事件、2500 行 5.14ms→0.67ms（7.7×），5200 行 4.21ms→0.82ms
        （5.1×，其余为 MemoryOptimizer 固定开销）。输出与旧实现逐行完全一致
        （拆股、空 ex_date、重复除权日、事件落在首行之前/末行之后等场景均已核对）。

        单次事件公式统一走模块级 calc_dividend_event_factor()。

        算法：
            - 前复权因子 qfq_factor：行 t 的因子 = prod{ f_e | ex_date_e > t }
            - 后复权因子 hfq_factor：行 t 的因子 = prod{ 1/f_e | ex_date_e <= t }

        Args:
            prices_df: 行情帧，trade_date 未排序时内部按升序排序。
            dividends_df: 除权事件帧，ex_date 为空的行忽略。
            max_factor: 单事件因子上限，超过视为数据异常并丢弃。
            as_of: 观察基准日；事件晚于 min(行情末行, as_of) 时一律不参与计算。
                传 None 时只以行情末行为边界（快照/现算场景下价格帧末行就是作用域边界）。

        事件截断口径（与 ``fund_factors.prepare_events_for_code`` 的
        ``event_date > last_date → continue`` 一致）：
            晚于行情末行（或 as_of）的除权事件不参与 qfq 计算，避免末行因子被尚未
            发生的事件提前改写。hfq 不受影响：hfq(t) = qfq(t) / prod{全部事件因子}，
            未来事件在分子与分母同时出现而抵消，截断前后逐行数值不变。
        """
        # --- 排序断言 ---
        # polars 2.0 移除了 DataFrame.is_sorted_by，改用列的 Series.is_sorted()
        if not prices_df['trade_date'].is_sorted():
            prices_df = prices_df.sort('trade_date')

        ones = [pl.lit(1.0).alias('qfq_factor'), pl.lit(1.0).alias('hfq_factor')]
        if dividends_df is None or dividends_df.is_empty():
            return MemoryOptimizer.optimize_dataframe(prices_df.with_columns(ones))

        if not dividends_df['ex_date'].is_sorted():
            dividends_df = dividends_df.sort('ex_date')

        divs = dividends_df.filter(pl.col('ex_date').is_not_null())
        # no_ex_rights：分红表保留原始事件行（share_div 原样不动），但该事件
        # 不参与除权因子计算（典型场景：退市/重整转增用于抵债，原股东不获得、
        # 原始价不除权）。标记列缺省视为 False，不影响既有调用方。
        if not divs.is_empty() and 'no_ex_rights' in divs.columns:
            divs = divs.filter(~pl.col('no_ex_rights').fill_null(False))
        if divs.is_empty():
            return MemoryOptimizer.optimize_dataframe(prices_df.with_columns(ones))
        # *_fix：修正列非空时覆盖同名字段（原始 share_div/cash_div 保留在快照里
        # 可追溯）。优先级写死为 no_ex_rights（剔除） > *_fix > 原值；列缺省时
        # 保持既有调用方行为不变。修正只影响事件聚积，不触碰 as_of 截断与 PIT 回滚。
        if 'share_div_fix' in divs.columns and 'share_div' in divs.columns:
            divs = divs.with_columns(
                pl.col('share_div_fix').fill_null(pl.col('share_div')).alias('share_div'))
        if 'cash_div_fix' in divs.columns and 'cash_div' in divs.columns:
            divs = divs.with_columns(
                pl.col('cash_div_fix').fill_null(pl.col('cash_div')).alias('cash_div'))

        # 事件截断：晚于行情末行（或 as_of）的事件不参与计算。
        # searchsorted(side='left') 对这类事件会返回 pos == N，若不截断会被广播到
        # 全部行（末行因子被未来事件提前改写）。日期统一转 ISO 字符串比较，
        # 兼容 Date / Datetime / 字符串三种 schema。
        cutoff = str(prices_df['trade_date'][-1])[:10]
        if as_of is not None:
            cutoff = min(cutoff, as_of.strftime('%Y-%m-%d'))
        divs = divs.filter(
            pl.col('ex_date').cast(pl.Utf8).str.slice(0, 10) <= cutoff)
        if divs.is_empty():
            return MemoryOptimizer.optimize_dataframe(prices_df.with_columns(ones))

        trade_dates = prices_df['trade_date'].to_numpy()
        closes = prices_df['close'].to_numpy()
        ex_dates = divs['ex_date'].to_numpy()
        div_rows = divs.to_dicts()

        # pos 为第一条 trade_date >= ex_date 的行号；pos == 0 说明该事件之前
        # 没有行情可取前收，与旧实现（filter 后 tail(1) 为空）一致地丢弃。
        pos = np.searchsorted(trade_dates, ex_dates, side='left')
        kept_ex = []
        factors = []
        for i in np.flatnonzero(pos > 0):
            div = div_rows[i]
            factor = calc_dividend_event_factor(
                closes[pos[i] - 1],
                div.get('cash_div'),
                div.get('share_div'),
                div.get('rights_issue_ratio'),
                div.get('rights_issue_price'),
                max_factor,
            )
            if factor is None:
                continue
            kept_ex.append(ex_dates[i])
            factors.append(factor)

        if not factors:
            return MemoryOptimizer.optimize_dataframe(prices_df.with_columns(ones))

        # suffix[k] = prod(factors[k:])，suffix[len] = 1；cnt 为该行之前已完成的事件数
        suffix = np.append(np.cumprod(np.asarray(factors, dtype=np.float64)[::-1])[::-1], 1.0)
        cnt = np.searchsorted(np.asarray(kept_ex), trade_dates, side='right')
        qfq = suffix[cnt]
        # prod{e <= t} (1/f_e) = qfq / prod(all f)
        hfq = qfq / suffix[0]

        result = prices_df.with_columns([
            pl.Series('qfq_factor', qfq, dtype=pl.Float64),
            pl.Series('hfq_factor', hfq, dtype=pl.Float64),
        ])
        # 内存优化：转换数据类型
        return MemoryOptimizer.optimize_dataframe(result)

    def save_adj_factors(self, df: pl.DataFrame, batch_size: int = 1000,
                         commit: bool = True):
        """
        保存复权因子到数据库（原子 upsert）。

        写入语义（与旧实现 _batch_insert 对齐）：
            - 键 (ts_code, trade_date) 已存在 → 覆盖 qfq_factor / hfq_factor；
            - 键不存在 → 新增，created_at / updated_at 取当前时间；
            - 冲突更新保留 created_at，只刷新 updated_at（数据库 NOW()）。

        旧实现是「先 SELECT 再 setattr/add_all」的非原子查重，并发下可能插入
        重复行；这里改用 upsert_rows（MySQL ON DUPLICATE KEY UPDATE /
        SQLite ON CONFLICT DO UPDATE），重复键不再产生 1062/IntegrityError。
        唯一约束由迁移 0012_stock_adj_factor_unique 提供。

        Args:
            df: 包含复权因子的 Polars DataFrame
            batch_size: 单次 upsert 提交的行数（upsert_rows 内部再按 400 行分批）
            commit: 是否在本方法内提交。为 False 时由调用方统一提交，
                用于把 stock_adj_factor 与 stock_daily 的写入放进同一事务
        """
        if df is None or df.is_empty():
            logger.warning("没有数据需要保存")
            return

        try:
            rows = [
                {
                    'ts_code': row['ts_code'],
                    'trade_date': row['trade_date'],
                    'qfq_factor': row['qfq_factor'],
                    'hfq_factor': row['hfq_factor'],
                }
                for row in df.to_dicts()
            ]

            for start in range(0, len(rows), batch_size):
                upsert_rows(
                    self.session, StockAdjFactor,
                    rows[start:start + batch_size],
                    key_fields=('ts_code', 'trade_date'),
                    value_fields=('qfq_factor', 'hfq_factor'),
                )

            if commit:
                self.session.commit()
            logger.info(f"成功保存 {len(rows)} 条复权因子记录")

        except IntegrityError as e:
            # 原子 upsert 已消除重复键冲突；此处兜底其他完整性错误（如 NOT NULL）
            logger.exception(f"保存复权因子违反完整性约束: {e}")
            self.session.rollback()
            if not commit:
                raise
        except (OSError, RuntimeError, SQLAlchemyError) as e:
            logger.exception(f"保存复权因子失败: {e}")
            self.session.rollback()
            if not commit:
                raise

    def update_stock_daily_adj(self, df: pl.DataFrame, commit: bool = True):
        """
        更新stock_daily表的复权价格和因子

        Args:
            df: 包含复权因子的Polars DataFrame
            commit: 是否在本方法内提交。为 False 时由调用方统一提交，
                用于把 stock_adj_factor 与 stock_daily 的写入放进同一事务
        """
        if df is None or df.is_empty():
            return

        try:
            rows = df.to_dicts()

            # 批量加载现有记录，避免逐行 SELECT（N+1）
            ts_codes = list({row['ts_code'] for row in rows})
            trade_dates = list({row['trade_date'] for row in rows})
            existing_records = self.session.query(StockDaily).filter(
                StockDaily.ts_code.in_(ts_codes),
                StockDaily.trade_date.in_(trade_dates)
            ).all()
            existing_map = {(rec.ts_code, rec.trade_date): rec for rec in existing_records}

            fields = ('qfq_factor', 'hfq_factor')
            updated = 0
            for row in rows:
                daily = existing_map.get((row['ts_code'], row['trade_date']))
                if daily:
                    for field in fields:
                        setattr(daily, field, row.get(field))
                    updated += 1

            if commit:
                self.session.commit()
            logger.info(f"成功更新 {updated} 条stock_daily记录")

        except (OSError, RuntimeError) as e:
            logger.exception(f"更新stock_daily失败: {e}")
            self.session.rollback()
            if not commit:
                raise

    def _get_dividends_bulk(self, ts_codes: List[str]) -> pl.DataFrame:
        """批量获取多只股票的分红数据（单次查询，避免逐股 N+1）"""
        rows = (
            self.session.query(
                StockDividend.ts_code,
                StockDividend.ex_date,
                StockDividend.cash_div,
                StockDividend.share_div,
                StockDividend.rights_issue_ratio,
                StockDividend.rights_issue_price,
            )
            .filter(StockDividend.ts_code.in_(ts_codes))
            .order_by(StockDividend.ts_code, StockDividend.ex_date)
            .all()
        )
        if not rows:
            return pl.DataFrame(schema={
                'ts_code': pl.String, 'ex_date': pl.Date,
                'cash_div': pl.Float64, 'share_div': pl.Float64,
                'rights_issue_ratio': pl.Float64, 'rights_issue_price': pl.Float64,
            })
        # 数值列统一转 float，避免 0/None 的 int 与小数混列导致 polars 推断失败
        return pl.DataFrame({
            'ts_code': [r[0] for r in rows],
            'ex_date': [r[1] for r in rows],
            'cash_div': [float(r[2] or 0) for r in rows],
            'share_div': [float(r[3] or 0) for r in rows],
            'rights_issue_ratio': [float(r[4] or 0) for r in rows],
            'rights_issue_price': [float(r[5] or 0) for r in rows],
        })

    def _get_prices_bulk(self, ts_codes: List[str],
                         start_date: datetime | None,
                         end_date: datetime | None) -> pl.DataFrame:
        """批量获取多只股票的日线价格（单次查询，避免逐股 N+1）"""
        query = self.session.query(
            StockDaily.ts_code, StockDaily.trade_date, StockDaily.open,
            StockDaily.high, StockDaily.low, StockDaily.close,
        ).filter(StockDaily.ts_code.in_(ts_codes))
        if start_date:
            query = query.filter(StockDaily.trade_date >= start_date)
        if end_date:
            query = query.filter(StockDaily.trade_date <= end_date)

        rows = query.order_by(StockDaily.ts_code, StockDaily.trade_date).all()
        if not rows:
            return pl.DataFrame(schema={
                'ts_code': pl.String, 'trade_date': pl.Date,
                'open': pl.Float64, 'high': pl.Float64,
                'low': pl.Float64, 'close': pl.Float64,
            })
        return pl.DataFrame({
            'ts_code': [r[0] for r in rows],
            'trade_date': [r[1] for r in rows],
            'open': [r[2] for r in rows],
            'high': [r[3] for r in rows],
            'low': [r[4] for r in rows],
            'close': [r[5] for r in rows],
        })

    def batch_calculate_and_save(self, ts_codes: List[str],
                                  start_date: datetime | None = None,
                                  end_date: datetime | None = None,
                                  update_daily: bool = True,
                                  chunk_size: int = 20):
        """
        批量计算并保存复权因子

        分块预取价格/分红数据，每块一个事务提交：把逐股 2 次查询 + 逐股提交
        降为每块 2 次查询 + 1 次提交，CPU 仍按单股向量化计算。

        Args:
            ts_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            update_daily: 是否同时更新stock_daily表
            chunk_size: 每块处理的股票数（越大内存占用越高）
        """
        total = len(ts_codes)
        success = 0
        failed = 0

        for offset in range(0, total, chunk_size):
            chunk = ts_codes[offset:offset + chunk_size]
            logger.info(f"[{offset+1}-{min(offset + chunk_size, total)}/{total}] "
                        f"批量处理 {len(chunk)} 只股票")

            try:
                # 每块各一次查询，替代逐股查询
                dividends_bulk = self._get_dividends_bulk(chunk)
                prices_bulk: pl.DataFrame | None = None
                if not self.tdx_handler:
                    prices_bulk = self._get_prices_bulk(chunk, start_date, end_date)

                frames = []
                for ts_code in chunk:
                    if self.tdx_handler:
                        prices = self._get_prices_from_tdx(ts_code, start_date, end_date)
                    elif prices_bulk is not None:
                        prices = prices_bulk.filter(pl.col('ts_code') == ts_code)
                    else:
                        prices = None

                    if prices is None or prices.is_empty():
                        logger.warning(f"{ts_code} 没有价格数据")
                        failed += 1
                        continue

                    dividends = dividends_bulk.filter(pl.col('ts_code') == ts_code)
                    if dividends.is_empty():
                        frame = prices.with_columns([
                            pl.lit(1.0).alias('qfq_factor'),
                            pl.lit(1.0).alias('hfq_factor'),
                        ])
                        frame = MemoryOptimizer.optimize_dataframe(frame)
                    else:
                        frame = self.calculate_factors(prices, dividends)
                    frames.append(frame)

                if not frames:
                    continue

                merged = pl.concat(frames)
                # stock_adj_factor 与 stock_daily 必须同一事务提交：
                # 任一表写入失败都回滚，避免两表复权因子不一致
                with self.db_manager.transaction():
                    self.save_adj_factors(merged, commit=False)
                    if update_daily:
                        self.update_stock_daily_adj(merged, commit=False)

                success += len(frames)

            except (OSError, RuntimeError, SQLAlchemyError) as e:
                self.session.rollback()
                logger.error(f"批量处理失败（{chunk[0]}..{chunk[-1]}）: {e}")
                failed += len(chunk)

        logger.info(f"批量复权因子计算完成: 成功 {success}，失败 {failed}，共 {total}")


if __name__ == "__main__":
    # 测试代码
    from utils.config import Config

    config = Config()
    db_manager = DatabaseManager(config)
    db_manager.connect()

    calculator = AdjFactorCalculator(db_manager)

    # 测试单只股票
    ts_code = "000001.SZ"
    result = calculator.calculate_adj_factors(ts_code)

    if result is not None:
        logger.info(f"\n{ts_code} 复权因子计算结果:\n{result.select(['trade_date', 'close', 'qfq_factor', 'hfq_factor']).tail(10)}")

        # 保存到数据库
        calculator.save_adj_factors(result)

    db_manager.disconnect()
