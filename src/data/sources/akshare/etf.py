#!/usr/bin/env python3

"""
AkShare ETF 数据 Mixin

ETF 实时行情获取与基本信息同步。
"""

from typing import Any

from loguru import logger

from src.data.sources import akshare_schema as schema
from src.utils.code_converter import to_ts_code
from src.utils.exception_handler import handle_exception_with_retry
from src.utils.exceptions import DataSourceConnectionError


class AkShareEtfMixin:
    """AkShare ETF 基本信息同步。"""

    session: Any

    @handle_exception_with_retry(max_retries=3, retry_delay=2)
    def _fetch_etf_spot(self):
        """
        获取 ETF 实时行情（带统一重试装饰器）。

        网络类异常包装为 DataSourceConnectionError（可重试），
        重试 3 次后仍失败则抛出，由调用方捕获处理。
        """
        try:
            return schema.fetch_spec(schema.ETF_SPOT)
        except (ConnectionError, TimeoutError, OSError) as e:
            raise DataSourceConnectionError(
                source="akshare", message=f"获取 ETF 实时行情失败: {e}", cause=e) from e

    def update_etf_basic(self):
        """
        更新 ETF 基本信息
        使用 AkShare 的 fund_etf_spot_em() 获取 ETF 列表和名称
        """
        try:
            etf_df = self._fetch_etf_spot()
        # 兜底理由：第三方接口异常类型不可预知，失败返回 None
        except Exception as e:  # noqa: BLE001
            logger.error(f"从 AkShare 获取 ETF 基本信息失败，重试后仍失败: {e}")
            return None

        if etf_df.is_empty():
            logger.warning("从 AkShare 获取的 ETF 信息为空")
            return None

        logger.info(f"从 AkShare 获取到 {etf_df.height} 条 ETF 信息")

        logger.info(f"ETF DataFrame 列名: {etf_df.columns}")

        # 列名以 akshare_schema.ETF_SPOT 为准（离线/入库均用标准列名）
        etf_df = schema.resolve(etf_df, schema.ETF_SPOT)

        # 离线模式下，只获取数据不存储
        if not self.session:
            logger.info("离线模式下，跳过 ETF 基本信息存储")
            return etf_df

        from src.database.models.stock import StockBasic

        # 遍历数据，进行清洗和存储
        updated_count = 0
        for row in etf_df.iter_rows(named=True):
            try:
                # 提取 ETF 代码和名称
                symbol = str(row['symbol']).strip()
                name = row['name']

                if not symbol:
                    continue

                # 判断 ETF 所属市场（统一市场推断：51/58 -> SH，15/16 -> SZ）
                try:
                    ts_code = to_ts_code(symbol)
                except ValueError:
                    logger.warning(f"无法判断 ETF {symbol} 的市场，跳过")
                    continue

                # 查询 ETF 是否已存在
                stock = self.session.query(StockBasic).filter_by(ts_code=ts_code).first()

                if stock:
                    # 更新现有 ETF 名称
                    if stock.name != name:
                        logger.info(f"更新 ETF 名称: {ts_code} - {stock.name} -> {name}")
                        stock.name = name
                        updated_count += 1
                else:
                    # 创建新 ETF 信息
                    logger.info(f"新增 ETF: {ts_code} - {name}")
                    stock = StockBasic(
                        ts_code=ts_code,
                        symbol=symbol[:9] if len(symbol) > 9 else symbol,
                        name=name[:45] if len(name) > 45 else name,
                        status='L'
                    )
                    self.session.add(stock)
                    updated_count += 1

            # 兜底理由：单行记录写库失败跳过，不中断整批
            except Exception as row_e:  # noqa: BLE001
                logger.exception(f"处理 ETF 基本信息失败: {row_e}")
                continue

        # 提交事务
        self.session.commit()
        logger.info(f"ETF 基本信息更新完成，共更新 {updated_count} 条记录")
        return etf_df
