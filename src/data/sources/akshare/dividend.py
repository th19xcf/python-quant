#!/usr/bin/env python3

"""
AkShare 分红配股数据 Mixin

分红行清洗、双键去重入库与批量同步。
"""

from typing import Any, List

from loguru import logger

from src.data.sources import akshare_schema as schema
from src.data.sources.akshare.constants import _DEFAULT_STOCK_CODES
from src.utils.code_converter import to_symbol


class AkShareDividendMixin:
    """AkShare 分红配股数据同步与入库。"""

    session: Any

    def _save_dividend_rows(self, ts_code: str, rows: List[dict]):
        """
        分红配股数据入库（双键去重）

        匹配顺序：
            1. 有除权除息日：按 ts_code + ex_date 匹配
            2. 未命中且有报告期年度：匹配同年度 ex_date 为空的占位记录并回填 ex_date
            3. 无除权除息日：匹配同年度已有记录，避免同一年度重复插入
            4. 仍未命中：新增记录

        一次性预加载该股票全部已有记录，避免原实现逐行查询；
        更新时不会用缺失的 ex_date 覆盖已有日期。

        Args:
            ts_code: 标准股票代码
            rows: 已清洗的分红行字典列表

        Returns:
            Tuple[int, int]: (新增条数, 更新条数)
        """
        if not rows:
            return 0, 0

        from src.database.models.stock import StockDividend

        existing = self.session.query(StockDividend).filter_by(ts_code=ts_code).all()
        by_ex_date = {d.ex_date: d for d in existing if d.ex_date}

        # 同年度可能有多条，占位记录（ex_date 为空）取 id 最大的一条
        placeholders = {}
        # 同年度全部记录（含已有 ex_date 的），仅在本次缺 ex_date 时用于去重
        by_year = {}
        for item in existing:
            if not item.dividend_year:
                continue
            current = by_year.get(item.dividend_year)
            if current is None or (item.id or 0) > (current.id or 0):
                by_year[item.dividend_year] = item
            if not item.ex_date:
                current = placeholders.get(item.dividend_year)
                if current is None or (item.id or 0) > (current.id or 0):
                    placeholders[item.dividend_year] = item

        inserted = 0
        updated = 0

        for row in rows:
            target = None
            if row['ex_date']:
                target = by_ex_date.get(row['ex_date'])
                if target is None and row['dividend_year']:
                    # 用实施公告的 ex_date 回填早前的占位记录
                    target = placeholders.get(row['dividend_year'])
            elif row['dividend_year']:
                target = by_year.get(row['dividend_year'])

            values = {
                'report_date': row['report_date'],
                'record_date': row['record_date'],
                'pay_date': row['pay_date'],
                'cash_div': row['cash_div'],
                'share_div': row['share_div'],
                'total_div': row['total_div'],
                'rights_issue_price': row['rights_issue_price'],
                'rights_issue_ratio': row['rights_issue_ratio'],
            }
            if row['ex_date']:
                # 仅在本次确实带除权除息日时写入，避免缺失值覆盖已有日期
                values['ex_date'] = row['ex_date']

            if target is None:
                target = StockDividend(
                    ts_code=ts_code,
                    symbol=row['symbol'],
                    name=row['name'],
                    dividend_year=row['dividend_year'],
                    **values,
                )
                self.session.add(target)
                inserted += 1
            else:
                for field, value in values.items():
                    setattr(target, field, value)
                updated += 1

            # 同步索引，保证同一批内后续行能命中刚新增或刚回填的记录
            if target.ex_date:
                by_ex_date[target.ex_date] = target
                if placeholders.get(target.dividend_year) is target:
                    # 占位记录已回填 ex_date，不再作为占位参与后续回合
                    placeholders.pop(target.dividend_year)
            if target.dividend_year:
                by_year[target.dividend_year] = target

        return inserted, updated

    @staticmethod
    def _pick_number(row, keys, default=0.0):
        """按候选上游列名取值并解析为数值"""
        return schema.parse_number_value(
            schema.pick_alias(row, keys, default), default)

    def _dividend_row_to_record(self, row, symbol, stock_name):
        """
        分红行 -> 待入库记录

        比例类字段统一由「每 10 股」换算为「每股」口径。
        """
        aliases = schema.DIVIDEND_ALIASES

        # 报告期作为分红年度
        dividend_year = None
        report_period = schema.pick_alias(row, aliases['report_period'])
        if report_period:
            if isinstance(report_period, str):
                dividend_year = report_period[:4]  # 提取年份
            else:
                dividend_year = str(report_period.year)

        report_date = schema.parse_date_value(
            schema.pick_alias(row, aliases['report_date']))
        record_date = schema.parse_date_value(
            schema.pick_alias(row, aliases['record_date']))
        ex_date = schema.parse_date_value(
            schema.pick_alias(row, aliases['ex_date']))

        # 现金分红（10派X -> 每股X/10）
        cash_div_ratio = self._pick_number(row, aliases['cash_div_ratio'])
        cash_div = cash_div_ratio / 10.0 if cash_div_ratio else 0.0

        # 送转比例（10送转X -> 每股X/10）
        share_div_ratio = self._pick_number(row, aliases['share_div_ratio'])
        share_div = share_div_ratio / 10.0 if share_div_ratio else 0.0

        # 配股信息（10配X -> 每股X/10）
        rights_issue_price = self._pick_number(
            row, aliases['rights_issue_price'])
        rights_issue_ratio_raw = self._pick_number(
            row, aliases['rights_issue_ratio'])
        rights_issue_ratio = (
            rights_issue_ratio_raw / 10.0 if rights_issue_ratio_raw else 0.0)

        # 总派现金额（单位存在差异，仅在字段存在时保存）
        total_div = self._pick_number(row, aliases['total_div'], default=0.0)

        return {
            'symbol': symbol,
            'name': stock_name,
            'dividend_year': dividend_year,
            'report_date': report_date,
            'record_date': record_date,
            'ex_date': ex_date,
            'pay_date': ex_date,  # 派息日（使用除权除息日作为派息日）
            'cash_div': cash_div,
            'share_div': share_div,
            'total_div': total_div,
            'rights_issue_price': rights_issue_price if rights_issue_price > 0 else None,
            'rights_issue_ratio': rights_issue_ratio if rights_issue_ratio > 0 else None,
        }

    def update_stock_dividend(self, ts_codes: List[str] = None):  # noqa: C901
        """
        更新股票分红配股数据

        Args:
            ts_codes: 股票代码列表，None表示更新所有股票

        Returns:
            dict: 股票代码到分红数据的映射（离线模式下）
        """
        try:
            # 离线模式下的默认股票代码列表
            default_ts_codes = _DEFAULT_STOCK_CODES

            # 如果没有指定股票代码，从数据库获取所有股票代码
            if not ts_codes:
                if self.session:
                    from src.database.models.stock import StockBasic
                    stock_basics = self.session.query(StockBasic.ts_code).all()
                    ts_codes = [sb.ts_code for sb in stock_basics]
                else:
                    # 离线模式下使用默认股票代码
                    ts_codes = default_ts_codes
                    logger.info(f"离线模式下，使用默认股票代码列表: {ts_codes}")

            if not ts_codes:
                logger.warning("没有需要更新的股票代码")
                return

            logger.info(f"开始更新{len(ts_codes)}只股票的分红配股数据")

            # 存储结果（离线模式下返回）
            result = {}

            # 遍历股票代码，获取分红配股数据
            for ts_code in ts_codes:
                try:
                    logger.info(f"正在获取{ts_code}的分红配股数据")

                    symbol = to_symbol(ts_code)

                    # 使用AkShare获取股票分红数据（契约见 akshare_schema.DIVIDEND）
                    # 接口: stock_fhps_detail_em - 获取历史分红数据(东方财富)
                    try:
                        dividend_df = schema.fetch_spec(schema.DIVIDEND, symbol=symbol)
                    except TypeError as e:
                        logger.warning(f"{ts_code}的分红配股数据获取失败，发生类型错误: {e}")
                        continue
                    except schema.AkShareContractError as e:
                        logger.error(f"{ts_code}的分红配股数据结构不符: {e}")
                        continue
                    # 兜底理由：第三方接口异常类型不可预知，单只失败不中断批量
                    except Exception as e:  # noqa: BLE001
                        logger.warning(f"{ts_code}的分红配股数据获取失败: {e}")
                        continue

                    # 检查结果是否为空
                    if dividend_df.is_empty():
                        logger.warning(f"{ts_code}没有分红配股数据")
                        continue

                    logger.info(f"获取到{ts_code}的{dividend_df.height}条分红配股数据")

                    # 离线模式下，只获取数据不存储
                    if not self.session:
                        result[ts_code] = dividend_df
                        logger.info(f"离线模式下，获取{ts_code}的分红配股数据完成")
                        continue

                    # 数据清洗和标准化
                    from src.database.models.stock import StockBasic

                    # 获取股票名称
                    stock_basic = self.session.query(StockBasic).filter_by(ts_code=ts_code).first()
                    stock_name = stock_basic.name if stock_basic else ""

                    # 遍历数据，进行清洗和存储
                    dividend_rows = []
                    for row in dividend_df.iter_rows(named=True):
                        try:
                            # 检查row是否为None
                            if row is None:
                                logger.warning(f"处理{ts_code}的分红配股数据时，row为None，跳过")
                                continue

                            dividend_rows.append(
                                self._dividend_row_to_record(row, symbol, stock_name))

                        except TypeError as row_e:
                            logger.exception(f"处理{ts_code}的分红配股数据时发生类型错误: {row_e}")
                            continue
                        except (ConnectionError, TimeoutError, OSError) as row_e:
                            logger.exception(f"处理股票分红配股数据失败: {row_e}")
                            continue
                        # 兜底理由：第三方接口异常类型不可预知，单只失败不中断批量
                        except Exception as row_e:  # noqa: BLE001
                            logger.exception(f"处理{ts_code}的分红配股数据时发生未知错误: {row_e}")
                            continue

                    # 双键去重后批量写入
                    inserted, updated = self._save_dividend_rows(ts_code, dividend_rows)

                    # 每只股票提交一次事务
                    self.session.commit()
                    logger.info(f"{ts_code}的分红配股数据更新完成（新增{inserted}条，更新{updated}条）")

                except TypeError as e:
                    if self.session:
                        self.session.rollback()
                    logger.exception(f"获取{ts_code}的分红配股数据时发生类型错误: {e}")
                    continue
                except (ConnectionError, TimeoutError, OSError) as e:
                    if self.session:
                        self.session.rollback()
                    logger.exception(f"获取{ts_code}的分红配股数据失败: {e}")
                    continue
                # 兜底理由：第三方接口异常类型不可预知，单只失败不中断批量
                except Exception as e:  # noqa: BLE001
                    if self.session:
                        self.session.rollback()
                    logger.exception(f"获取{ts_code}的分红配股数据时发生未知错误: {e}")
                    continue

            # 离线模式下返回结果
            if not self.session and result:
                return result

        except (ConnectionError, TimeoutError, OSError) as e:
            if self.session:
                self.session.rollback()
            logger.exception(f"更新股票分红配股数据失败: {e}")
            raise
