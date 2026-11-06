#!/usr/bin/env python3

r"""外部依赖数据契约占位（停复牌 / 行业 PIT / 指数成分历史）。

问题
====
回测与选股里有几条约束被**隐式假设**成立，但项目并没有对应的权威本地数据：

- 「停牌不可买」——没有停复牌日表，撮合引擎只能按有行情即认为可交易；
- 「行业中性化 / 分组回测」——`meta/industry/` 只有近期日快照，没有按生效日
  的 PIT 区间表，用当前行业回填历史会引入前视偏差；
- 「指数成分内选股」——没有成分进出区间，用当前成分回测历史会有成分股
  幸存者偏差。

此前这些缺口既不报错也不告警，等价于「没数据 ⇒ 约束不生效」，结论看起来
正常但存在系统性偏差。本模块把缺口登记成**显式契约**：声明权威路径、必需
列、粒度、候选来源与缺失影响，并提供只读巡检，使「未就绪」成为可观测状态。

只读：不写 ``data/**``，不采集、不修复；``ready`` 只在文件存在、必需列齐全
且 ``coverage.json`` 声明 ``complete=true`` 时才成立。占位期一律报
``missing`` / ``partial``，业务代码不得据此放行相关约束。

用法::

    python -m src.data.quality.extended_contracts --json
    python -m src.data.quality.extended_contracts --fail-on-gap   # 可挂 CI
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger
from polars.exceptions import PolarsError

from src.data.local_store.paths import default_store_root


@dataclass(frozen=True)
class ContractSpec:
    """一份尚未落地的数据契约声明。"""

    key: str
    label: str
    #: 相对列存根目录的权威产物路径
    path: str
    #: 必需列（缺一即为 partial）
    required_columns: tuple[str, ...]
    grain: str
    #: 可选覆盖声明文件（相对列存根目录），需含 complete=true
    coverage_path: str
    source_candidates: tuple[str, ...]
    impact_if_missing: str
    #: 覆盖声明 ``end`` 允许滞后的自然日上限；超期即视为不新鲜
    max_lag_days: int = 10


CONTRACTS: tuple[ContractSpec, ...] = (
    ContractSpec(
        key='suspension',
        label='停复牌日表',
        path='meta/suspension/suspension_daily.parquet',
        required_columns=('ts_code', 'trade_date', 'is_suspended'),
        grain='(ts_code, trade_date)',
        coverage_path='meta/suspension/coverage.json',
        source_candidates=('沪深交易所停复牌公告', '东方财富', 'BaoStock'),
        impact_if_missing='停牌日仍可能被撮合成交，回测虚增可交易性与成交达成率',
    ),
    ContractSpec(
        key='industry_pit',
        label='行业分类 PIT 区间表',
        # 独立目录：`meta/industry/` 下的 `industry_*.parquet` 是日快照，
        # 被 freshness 的 latest_glob 直接读取（依赖 as_of 列）。PIT 区间表
        # 列名不同，放在同目录同前缀会污染快照巡检。
        path='meta/industry_pit/industry_pit.parquet',
        required_columns=('ts_code', 'effective_date', 'industry_code', 'level'),
        grain='(ts_code, effective_date, level)',
        coverage_path='meta/industry_pit/coverage.json',
        source_candidates=('申万/中信行业分类历史', '巨潮资讯'),
        impact_if_missing='行业中性化与分组回测用当前行业回填历史，引入前视偏差',
    ),
    ContractSpec(
        key='index_membership',
        label='指数成分进出区间表',
        path='meta/index_membership/membership_intervals.parquet',
        required_columns=('index_code', 'ts_code', 'in_date', 'out_date'),
        grain='(index_code, ts_code, in_date)',
        coverage_path='meta/index_membership/coverage.json',
        source_candidates=('中证指数官网', '沪深交易所指数成分公告'),
        impact_if_missing='指数成分内选股用当前成分回测历史，引入成分股幸存者偏差',
    ),
)


def _read_columns(path: Path) -> list[str] | None:
    try:
        return list(pl.read_parquet_schema(path).keys())
    except (OSError, ValueError, TypeError, PolarsError):
        return None


def _coverage_complete(path: Path) -> bool | None:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return False
    return bool(payload.get('complete')) if isinstance(payload, dict) else False


def _read_coverage(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _coverage_lag_days(payload: dict[str, Any], as_of: date) -> int | None:
    """覆盖声明 ``end`` 距观察日的自然日差；缺失或不可解析返回 None。"""
    raw = str(payload.get('end') or '')[:10]
    if not raw:
        return None
    try:
        declared = date.fromisoformat(raw)
    except ValueError:
        return None
    return max(0, (as_of - declared).days)


def check_contracts(store_root: Path | str | None = None, *,
                    as_of: date | None = None) -> list[dict[str, Any]]:
    """只读巡检所有契约，返回逐项状态与阻塞原因。"""
    root = Path(store_root) if store_root is not None else default_store_root()
    today = as_of or date.today()
    results: list[dict[str, Any]] = []
    for spec in CONTRACTS:
        data_path = root / spec.path
        coverage_path = root / spec.coverage_path
        blockers: list[str] = []

        if not data_path.is_file():
            blockers.append(f'权威产物缺失：{spec.path}')
            columns: list[str] = []
            rows: int | None = None
        else:
            columns = _read_columns(data_path) or []
            if not columns:
                blockers.append('Parquet schema 不可读')
            missing_cols = [c for c in spec.required_columns if c not in columns]
            if missing_cols:
                blockers.append(f'缺少必需列：{missing_cols}')
            try:
                rows = int(pl.scan_parquet(data_path).select(pl.len()).collect().item())
                if rows == 0:
                    blockers.append('产物为空表')
            except (OSError, ValueError, TypeError, PolarsError):
                rows = None
                blockers.append('行数统计失败')

        payload = _read_coverage(coverage_path)
        complete = bool(payload.get('complete')) if payload else (
            None if not coverage_path.is_file() else False)
        if complete is None:
            blockers.append(f'缺少覆盖声明：{spec.coverage_path}')
        elif not complete:
            blockers.append('覆盖声明 complete=false（区间不完整）')
        lag_days = _coverage_lag_days(payload, today)
        if lag_days is not None and lag_days > spec.max_lag_days:
            # 只建过一次就永远「ready」是最危险的：把过期也当成阻塞项
            blockers.append(
                f'覆盖声明 end={payload.get("end")} 滞后 {lag_days} 天'
                f'（>{spec.max_lag_days}），需重新生成')

        if not data_path.is_file():
            status = 'missing'
        elif blockers:
            status = 'partial'
        else:
            status = 'ready'

        results.append({
            **asdict(spec),
            'status': status,
            'exists': data_path.is_file(),
            'bytes': data_path.stat().st_size if data_path.is_file() else 0,
            'rows': rows,
            'columns': columns,
            'coverage_complete': complete,
            'coverage_end': payload.get('end'),
            'lag_days': lag_days,
            'blockers': blockers,
        })
    return results


def summarize(results: list[dict[str, Any]]) -> dict[str, int]:
    return {
        'total': len(results),
        'ready': sum(r['status'] == 'ready' for r in results),
        'partial': sum(r['status'] == 'partial' for r in results),
        'missing': sum(r['status'] == 'missing' for r in results),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='外部依赖数据契约只读巡检')
    parser.add_argument('--root', default=None, help='列存根目录（缺省用配置默认值）')
    parser.add_argument('--json', action='store_true', help='以 JSON 输出')
    parser.add_argument('--fail-on-gap', action='store_true',
                        help='存在 missing/partial 时返回非零退出码')
    parser.add_argument('--as-of', default=None,
                        help='观察日 YYYY-MM-DD（缺省今天，用于判定覆盖是否过期）')
    args = parser.parse_args(argv)

    as_of = None
    if args.as_of:
        try:
            as_of = date.fromisoformat(args.as_of[:10])
        except ValueError:
            logger.error(f'--as-of 无法解析: {args.as_of}')
            return 2

    results = check_contracts(args.root, as_of=as_of)
    summary = summarize(results)
    if args.json:
        sys.stdout.write(json.dumps({'summary': summary, 'contracts': results},
                                    ensure_ascii=False, indent=2) + '\n')
    else:
        logger.info(f'[contracts] 汇总：{summary}')
        for item in results:
            level = logger.warning if item['status'] != 'ready' else logger.info
            level(f'[contracts] {item["key"]} {item["status"]}: '
                  f'{item["blockers"] or ["就绪"]}；缺失影响：{item["impact_if_missing"]}')
    gaps = summary['missing'] + summary['partial']
    if args.fail_on_gap and gaps:
        logger.error(f'[contracts] {gaps} 项扩展数据契约未就绪；'
                     '相关业务约束不得据此放行')
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
