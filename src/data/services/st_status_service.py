#!/usr/bin/env python3

"""
ST 状态服务

维护 StockStatusHistory 时间维表，并提供按 (code, start, end) 查询的
区间列表，供回测撮合内核按 trade_date 精确判定涨跌停幅度。

数据源现状（2026-10 探测结论）：
    akshare 只有 ``stock_zh_a_st_em``（当前风险警示板名单），
    ``stock_info_change_name`` 仅返回曾用名、不含日期，
    因此无法重建历史戴帽/摘帽区间。

本服务因此采用"当前状态 + 显式覆盖起点"的近似方案：
    - refresh_current 把当前 ST 名单写入 [coverage_start, None) 区间；
    - 调用方必须明确 coverage_start 的语义（默认 = 探测当日），
      更早的日期仍返回未知（None），由回测 st_unknown 标记数据质量。
"""

from datetime import date, datetime, timedelta
from typing import Any, Dict, List

from loguru import logger
from sqlalchemy import or_

from src.database.models.stock import StockStatusHistory

# 数据来源标识
SOURCE_AKSHARE_CURRENT = 'akshare_st_em_current'


def normalize_ts_code(raw: Any) -> str | None:
    """把 6 位代码 / sh.600000 等格式统一为 600000.SH"""
    code = str(raw).strip()
    if not code:
        return None

    lower = code.lower()
    for prefix, suffix in (('sh.', '.SH'), ('sz.', '.SZ'), ('bj.', '.BJ')):
        if lower.startswith(prefix):
            digits = code[3:]
            return f'{digits}{suffix}' if digits else None

    if '.' in code:
        return code.upper()

    if len(code) == 6 and code.isdigit():
        if code[0] in ('6', '9'):
            return f'{code}.SH'
        if code[0] in ('0', '3'):
            return f'{code}.SZ'
        if code[0] in ('4', '8'):
            return f'{code}.BJ'
    return None


def _parse_date(value: Any) -> date | None:
    """字符串/date/datetime 统一转 date，无法解析返回 None"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(str(value), fmt).date()
        except ValueError:
            continue
    return None


class StStatusService:
    """ST 时间维表的写入与查询"""

    def __init__(self, db_manager=None, session=None):
        self.db_manager = db_manager
        self._session = session

    @property
    def session(self):
        """优先使用注入的 session，否则从 db_manager 取（连接失败返回 None）"""
        if self._session is not None:
            return self._session
        if self.db_manager is None:
            return None
        try:
            return self.db_manager.get_session()
        # 兜底理由：离线/无数据库场景下查询 ST 应降级为无数据
        except Exception as e:  # noqa: BLE001
            logger.warning(f"获取数据库会话失败，ST 状态查询降级为空: {e}")
            return None

    @staticmethod
    def _fetch_current_st_codes() -> List[str]:
        """拉取当前风险警示板名单（akshare），返回统一格式代码"""
        import akshare as ak

        df = ak.stock_zh_a_st_em()
        code_col = next((c for c in ('代码', 'code', 'symbol') if c in df.columns), None)
        if code_col is None:
            raise ValueError(f"akshare ST 名单缺少代码列: {list(df.columns)}")

        codes = []
        for raw in df[code_col].tolist():
            ts_code = normalize_ts_code(raw)
            if ts_code:
                codes.append(ts_code)
        return codes

    def refresh_current(self, as_of: Any = None, coverage_start: Any = None) -> Dict[str, Any]:
        """
        用当前 ST 名单刷新时间维表。

        Args:
            as_of: 探测基准日（默认今天）
            coverage_start: 当前 ST 区间的开始日期（默认 = as_of）。
                由于没有历史戴帽日期，调用方可显式传入一个保守起点，
                表示"从该日起该标的按 ST 处理"。

        Returns:
            dict: ok / mode / st_count / opened / closed / coverage_start / as_of
        """
        as_of_date = _parse_date(as_of) or date.today()
        coverage = _parse_date(coverage_start) or as_of_date

        try:
            current_codes = self._fetch_current_st_codes()
        # 兜底理由：数据源不可用时保留已有维表，不阻断回测
        except Exception as e:  # noqa: BLE001
            logger.warning(f"拉取当前 ST 名单失败，保留已有 ST 维表: {e}")
            return {
                'ok': False, 'mode': 'current_only', 'error': str(e),
                'st_count': 0, 'opened': 0, 'closed': 0,
                'coverage_start': coverage, 'as_of': as_of_date,
            }

        session = self.session
        if session is None:
            return {
                'ok': False, 'mode': 'current_only', 'error': '数据库不可用',
                'st_count': len(current_codes), 'opened': 0, 'closed': 0,
                'coverage_start': coverage, 'as_of': as_of_date,
            }

        opened = closed = 0
        try:
            open_rows = (
                session.query(StockStatusHistory)
                .filter(StockStatusHistory.end_date.is_(None),
                        StockStatusHistory.is_st.is_(True))
                .all()
            )
            open_by_code = {row.ts_code: row for row in open_rows}
            still_st = set(current_codes)

            for ts_code in current_codes:
                if ts_code in open_by_code:
                    continue
                session.add(StockStatusHistory(
                    ts_code=ts_code,
                    start_date=coverage,
                    end_date=None,
                    is_st=True,
                    source=SOURCE_AKSHARE_CURRENT,
                ))
                opened += 1

            for ts_code, row in open_by_code.items():
                if ts_code in still_st:
                    continue
                # 区间右端用探测前一日，避免新状态日重叠
                row.end_date = as_of_date - timedelta(days=1)
                closed += 1

            session.commit()
        # 兜底理由：写入失败回滚并保留旧数据
        except Exception as e:  # noqa: BLE001
            session.rollback()
            logger.warning(f"刷新 ST 时间维表失败: {e}")
            return {
                'ok': False, 'mode': 'current_only', 'error': str(e),
                'st_count': len(current_codes), 'opened': opened, 'closed': closed,
                'coverage_start': coverage, 'as_of': as_of_date,
            }

        logger.warning(
            "akshare 无历史 ST 戴帽/摘帽日期，仅写入当前状态的近似区间："
            f"coverage_start={coverage}, as_of={as_of_date}, "
            f"ST={len(current_codes)} 只（新增 {opened}、摘帽 {closed}）；"
            f"早于 {coverage} 的日期仍按 ST 未知处理。"
        )
        return {
            'ok': True, 'mode': 'current_only',
            'st_count': len(current_codes), 'opened': opened, 'closed': closed,
            'coverage_start': coverage, 'as_of': as_of_date,
        }

    def get_status_intervals(self, codes: List[str], start_date: Any = None,
                             end_date: Any = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        查询与 [start_date, end_date] 有交集的 ST 区间。

        Returns:
            {ts_code: [{'start_date': date, 'end_date': date|None, 'is_st': bool}]}
            未命中任何区间的标的不出现在结果中（回测按未知处理）。
        """
        session = self.session
        if session is None or not codes:
            return {}

        start = _parse_date(start_date)
        end = _parse_date(end_date)
        try:
            query = session.query(StockStatusHistory).filter(
                StockStatusHistory.ts_code.in_(list(codes))
            )
            if end is not None:
                query = query.filter(StockStatusHistory.start_date <= end)
            if start is not None:
                query = query.filter(or_(
                    StockStatusHistory.end_date.is_(None),
                    StockStatusHistory.end_date >= start,
                ))
            rows = query.order_by(StockStatusHistory.ts_code,
                                  StockStatusHistory.start_date).all()
        # 兜底理由：维表查询失败按未知处理，不阻断回测
        except Exception as e:  # noqa: BLE001
            logger.warning(f"查询 ST 时间维表失败，按未知处理: {e}")
            return {}

        intervals: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            intervals.setdefault(row.ts_code, []).append({
                'start_date': row.start_date,
                'end_date': row.end_date,
                'is_st': bool(row.is_st),
            })
        return intervals
