#!/usr/bin/env python3

"""日更 / 补数单入口：一条命令跑完当日全部数据维护，一行汇总收尾。

步骤顺序（与 ``STEP_PLAN`` 一致，dry-run 会原样打印）::

     1. 证券码表        tools/build_names_json.py → tools/build_name_history.py → tools/build_stock_basic.py
     2. 行业快照        python -m src.data.industry refresh
     3. 行情采集入库    src.data.tencent.daily_fill.fill_daily_gap(workers=1) + LocalQuoteStore.flush_meta()
     4. 本地行情构建    python -m src.data.local_store build（增量 + 换手派生 + 异常清单）
                       收尾断言：meta/daily_index 末日 == 交易日历最近已完成交易日，不等即判失败
                       本步带 --skip-factor-verify：因子副本一致性断言依赖第 6 步结果，见下
     5. 基金行情刷新    python -m src.data.local_store build --data-type fund（增量，末日已最新则跳过）
     6. 复权因子刷新    factor_rebuild / fund_factors 仅重算「行情末日 > 因子末日」的标的（已最新则跳过）
                       刷新后复验行情内嵌因子副本与快照逐行一致（个股/指数/基金）
     7. 分红日频增量    tools/refresh_dividend_daily.py（基金年度批量 + 个股近两报告期，
                       只追加/补缺并重建受影响标的因子；非致命，失败只告警）
     8. ST 快照与区间表  src.data.risk_flags refresh + src.data.st_history build
     9. 名录每日检查    src.main --update-fund / --update-index（有变更才落盘）
    10. 估值增量        tools/collect_financials.py --only valuation --since auto
    11. 市场宽度增量    src.data.services.breadth_service.refresh
    12. 候选池周更      tools/build_multi_asset_etf_pool.py --apply（未到期不跑）
    13. 回写与校验      manifest 回写 → 逐件 rows/bytes/sha256 对账 → 预期差集为 0
    14. 洁净集快照      python -m src.data.quality.clean_set build
    15. 保鲜体检        src.data.quality.freshness_check --summary
    16. 版本记录        src.data.data_version（失败只告警，不改数据）
    17. 备份            tools/backup_critical_assets.py（必须在所有写 data/ 的步骤之后）
    18. 门禁            tools/daily_data_gate.py（必须在备份之后）

口径与边界::

    - 步骤顺序不是随意的：第 4 步 build 的收尾门禁「行情内嵌因子副本 == 因子
      快照」需要基金行情与基金因子都已刷新到目标日，而这两件事分别在第 5、6
      步。放在第 4 步等于让断言依赖后面步骤的结果——基金滞后时第 4 步必然失败，
      修复窗口（第 6 步）却排在其后，形成死锁（2026-11-17 两次补跑均卡在此处）。
      因此第 4 步只做「本地构建 + 末日断言」，因子副本一致性断言统一挪到第 6 步
      末尾：刷新完成后再校验，覆盖本次运行刷新的全部标的，也仍能拦住真正的滞后。
    - 每步独立记录成败，某步失败只记录并继续后续步骤；最后一行汇总给出
      「N 步 ok / 失败 M（致命 K）」与总耗时，失败步逐条列出明细。
    - 致命步（行情采集、本地构建、基金行情刷新、复权因子刷新、回写校验、
      备份、门禁）任一失败，退出码为 1；其余步骤失败只记录，不影响退出码。
      备份/门禁的先后顺序是硬约束，新增步骤只能插在它们之前。
    - 采集固定 ``workers=1``（单连接串行），限流参数沿用 TencentClient 默认
      ``min_interval=0.05`` / ``max_retries=3``，本入口不暴露也不修改。
    - 补数先按代码清单把缺口落到 ``_staging/<批次>/``（``gaps.parquet`` +
      ``input_set.json``，后者带输入集合哈希），再一次 apply；同一批次名
      不允许换一份输入集合复用，防止「补一批 → 复跑 → 再补」。
    - 校验不抽样、不跑全库 ``manifest_tool --check``：只核本次改动件。
    - ``quotes/manifest.json`` 是人工维护的全市场总账（仓库内无产者，含
      futures 口径），本入口只做只读对账（universe 分市场求和 == universe
      合计），不代写数值；该件与磁盘的偏差按 WARN 打印实测差额。
    - 候选池是筛选结果（视图层），不承担数据完整性；被排除标的的行情/分红
      由保鲜体检的全集判据负责。
    - 默认不打印各步过程日志，失败时才回显该步末尾输出；``--verbose`` 打开
      INFO/DEBUG 与子进程原样输出。

用法::

    # dry-run（默认，不联网、不落盘；打印完整步骤清单与顺序）
    python tools/daily_update.py
    # 日更 apply（一条命令跑完全部步骤，目标交易日自动探测）
    python tools/daily_update.py --apply --allow-remote
    # 日更 apply 后顺带出 v3 目标持仓清单（只出清单与台账，不下单）
    python tools/daily_update.py --apply --allow-remote --target-holdings --capital 1000000000
    # 补数（按代码清单；缺口先落 _staging/<批次>/ 再一次 apply）
    python tools/daily_update.py --codes 600000.SH,000001.SZ --end 2026-11-13 \
        --batch gap_20261113 --apply --allow-remote
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from datetime import time as dtime
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import polars as pl  # noqa: E402
from loguru import logger  # noqa: E402

from src.data.local_store.paths import default_store_root  # noqa: E402
from src.data.local_store.store import (  # noqa: E402
    LocalFactorStore,
    LocalQuoteStore,
    verify_quote_factor_consistency,
)
from src.data.manifest_refresh import file_facts, refresh_manifest_files  # noqa: E402

DATA_SUFFIXES = ('.parquet', '.csv', '.tsv', '.txt')
STAT_KEYS = ('rows', 'bytes', 'sha256')
STORE_PREFIX = 'data/local_store'
#: 日线收盘判定口径（与 ``src.data.tencent.daily_fill`` 一致）：15:05 前当日 K
#: 线未完成，最近「已完成」交易日取前一交易日。
CLOSE_CUTOFF = dtime(15, 5)
#: 末根距目标交易日超过该天数视为长期停更/退市，不进待补清单（与 fill_daily_gap 默认同口径）。
DEFAULT_MAX_GAP_DAYS = 90
#: 采集写盘后由两条既有链路产出的非行情件（构建收尾 / 派生 / 台账）。
SIDE_OUTPUTS = (
    'meta/daily_index.parquet',
    'meta/daily_index.manifest.json',
    'meta/state.json',
    'meta/anomalies/manifest.json',
    'factors/stock_turnover.parquet',
    'factors/manifest.json',
    'factors/meta.json',
    'factors/covered_codes.txt',
    # 目录级登记：本次写盘的 meta 件按磁盘实测回填（见 _refresh）
    'meta/manifest.json',
)
#: 构建收尾会整目录重写的台账（内容随行情变动，逐件白名单不现实）。
SIDE_PREFIXES = ('meta/anomalies/', 'factors/rebuild/',
                 'meta/financials/parts/valuation/', 'meta/industry/')
#: ST 区间表日更（risk_flags refresh + st_name_history build + st_history build）会写的件。
#: supplement 由 ``st_name_history build`` 重建，覆盖窗口随行情末日推进；不重建会
#: 让沪市/北交所 2,324 只的 ``coverage_end`` 停在最后一次手工构建日。
AUX_ST_OUTPUTS = (
    'meta/st/st_intervals.parquet',
    'meta/st/st_coverage.parquet',
    'meta/st/coverage.json',
    'meta/st/manifest.json',
    'meta/st/supplement/supplement_intervals.parquet',
    'meta/st/supplement/supplement_coverage.parquet',
    'meta/st/supplement/unresolved.csv',
    'meta/st/supplement/manifest.json',
    'meta/st_status/st_status_history.parquet',
    'meta/st_status/manifest.json',
)
#: 估值日更会写的件（最终表 + 断点；分片按 ``meta/financials/parts/valuation/`` 前缀计入）。
AUX_VALUATION_OUTPUTS = (
    'meta/financials/valuation_daily.parquet',
    'meta/financials/parts/progress.json',
)
#: 复权因子刷新会写的件（个股 + 基金快照、sidecar 与目录级 manifest；
#: 分块按 ``factors/rebuild/`` 前缀计入）。行情副本由同一步一并重写。
AUX_FACTOR_OUTPUTS = (
    'factors/stock_adj_factor.parquet',
    'factors/fund_adj_factor.parquet',
    'factors/fund_covered_codes.txt',
    'factors/fund_meta.json',
    'factors/rebuild/manifest.json',
)
#: 分红日频增量会写的件（两张事件表 + 状态台账；受影响标的的复权因子与
#: 行情副本由同一步重建，按 ``AUX_FACTOR_OUTPUTS`` 与行情路径另行登记）。
AUX_DIVIDEND_OUTPUTS = (
    'factors/stock_dividend.parquet',
    'factors/fund_dividend.parquet',
    'meta/dividend_status/daily_refresh_state.json',
)
#: 证券码表日更会写的件（通达信当日码表 → 名称历史 → 名录）。
AUX_CODE_TABLE_OUTPUTS = (
    'meta/names.json',
    'meta/name_history.parquet',
    'meta/name_history.manifest.json',
    'meta/name_history_coverage.json',
    'meta/stock_basic.parquet',
    'meta/stock_basic.manifest.json',
)
#: 名录周更会写的件（首件用于判断距上次刷新是否超过阈值）。
AUX_BASIC_OUTPUTS = {
    'fund': ('meta/fund_basic.parquet', 'meta/fund_basic.manifest.json'),
    'index': ('meta/index_basic.parquet', 'meta/index_basic.manifest.json'),
}
#: 名录默认检查周期（自然日）：每天检查一次，有变更才落盘；体检按最后检查日期判
DEFAULT_BASIC_REFRESH_DAYS = 1
#: 候选池周更会写的件（首件用于判断距上次重建是否超过阈值）。
AUX_POOL_OUTPUTS = (
    'meta/multi_asset/etf_pool.parquet',
    'meta/multi_asset/etf_pool.csv',
    'meta/multi_asset/etf_gap_detail.csv',
    'meta/multi_asset/etf_event_gaps.csv',
    'meta/multi_asset/etf_pool_excluded.json',
    'meta/multi_asset/manifest.json',
)
#: 候选池默认重建周期（自然日）
DEFAULT_POOL_REFRESH_DAYS = 7
#: 市场宽度日更会写的件（增量补齐，行情末根未前进时不落盘）。
AUX_BREADTH_OUTPUTS = (
    'meta/breadth/breadth_daily.parquet',
    'meta/breadth/coverage.json',
    'meta/breadth/manifest.json',
)
#: 增量清点跳过 ``backtest_runs``：回测运行记录是结果归档（json/csv/log 与索引），
#: 不是数据表，日更既不产出也不消费它，计入会让「本次新增文件」混入非数据资产。
SKIP_DIRS = {'_backup', '__pycache__', 'backtest_runs'}

#: 单入口步骤顺序（dry-run 与 apply 共用）：(键, 名称, 是否致命)。
#: 硬约束：备份在所有写 ``data/`` 的步骤之后，门禁在备份之后。
#: 顺序理由：``build``（第 4 步）跑的是 ``--skip-factor-verify``，因为它自带的
#: 「行情内嵌因子副本 == 因子快照」断言依赖第 5 步（基金行情刷新）与第 6 步
#: （复权因子刷新）。断言若留在第 4 步，基金滞后时第 4 步必红、而能修复它的
#: 第 6 步排在后面，形成死锁；故断言收窄到第 6 步刷新完成后执行。
STEP_PLAN: tuple[tuple[str, str, bool], ...] = (
    ('code_table', '证券码表（names.json / name_history / stock_basic）', False),
    ('industry', '行业快照', False),
    ('fill', '行情采集入库', True),
    ('build', '本地行情构建 + 末日断言', True),
    ('fund_quote', '基金行情刷新（通达信本地增量）', True),
    ('factor', '复权因子刷新（个股 + 基金）', True),
    ('dividend', '分红日频增量（基金年度批量 + 个股近两报告期）', False),
    ('st', 'ST 快照与区间表', False),
    ('basic', '名录每日检查（fund / index）', False),
    ('valuation', '全市场估值增量', False),
    ('breadth', '市场宽度增量', False),
    ('pool', '候选池周更', False),
    ('manifest', 'manifest 回写与校验', True),
    ('clean_set', '洁净集快照', False),
    ('freshness', '保鲜体检', False),
    ('version', '数据版本记录', False),
    ('backup', '不可重建资产备份', True),
    ('gate', '每日数据门禁', True),
)


# ----------------------------------------------------------------------
# 小工具
# ----------------------------------------------------------------------
def _parse_date(text: str | None) -> date | None:
    if not text:
        return None
    raw = str(text).strip().replace('-', '')
    if len(raw) != 8 or not raw.isdigit():
        raise argparse.ArgumentTypeError(f'日期需为 YYYY-MM-DD 或 YYYYMMDD：{text}')
    return date(int(raw[:4]), int(raw[4:6]), int(raw[6:]))


def _sha256_text(items: Iterable[str]) -> str:
    digest = hashlib.sha256()
    for item in items:
        digest.update(item.encode('utf-8'))
        digest.update(b'\n')
    return digest.hexdigest()


def _coverage(root: Path) -> dict[tuple[str, str], dict[str, Any]]:
    """读 ``meta/daily_index.parquet`` 作为覆盖度（口径以该表为准）。"""
    import polars as pl  # noqa: PLC0415

    path = root / 'meta' / 'daily_index.parquet'
    if not path.is_file():
        return {}
    frame = pl.read_parquet(path, columns=['ts_code', 'data_type', 'rows', 'start', 'end'])
    return {(row['data_type'], row['ts_code']): row for row in frame.to_dicts()}


def _to_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _daily_index_end(root: Path, kinds: Sequence[str] | None = None) -> date | None:
    """``meta/daily_index.parquet`` 的末日（可按 data_type 过滤）；只读。"""
    path = root / 'meta' / 'daily_index.parquet'
    if not path.is_file():
        return None
    try:
        frame = pl.read_parquet(path, columns=['data_type', 'end'])
    except (OSError, pl.exceptions.PolarsError):
        return None
    if kinds is not None:
        frame = frame.filter(pl.col('data_type').is_in(list(kinds)))
    if frame.is_empty():
        return None
    return _to_date(frame['end'].max())


def _check_daily_index_end(root: Path, expected: date) -> tuple[bool, str]:
    """断言行情末日 == 最近交易日；返回 (是否通过, 说明)。

    只读 ``meta/daily_index.parquet``，不做任何写入，负向用例可直接构造
    一份末日不符的 daily_index 证明它会红。
    """
    actual = _daily_index_end(root)
    if actual is None:
        return False, 'meta/daily_index.parquet 缺失或无 end'
    if actual != expected:
        return False, f'daily_index 末日 {actual} != 最近交易日 {expected}'
    return True, f'daily_index 末日 {actual} = 最近交易日 {expected}'


def _recent_completed_trade_day(root: Path, now: datetime | None = None) -> date:
    """交易日历里最近一个「已完成」交易日（15:05 前当日 K 线未完成，取前一交易日）。"""
    from src.data.trading_calendar import get_calendar  # noqa: PLC0415

    moment = now or datetime.now()
    calendar = get_calendar(root, as_of=moment.date())
    day = moment.date()
    if calendar.is_trading_day(day) and moment.time() < CLOSE_CUTOFF:
        previous = calendar.previous(day, inclusive=False)
    else:
        previous = calendar.previous(day, inclusive=True)
    if previous is None:
        raise RuntimeError(f'交易日历在 {day} 之前没有可用交易日')
    return previous


def _quote_end_by_code(root: Path, kind: str) -> dict[str, date]:
    ends: dict[str, date] = {}
    for (data_type, code), row in _coverage(root).items():
        if data_type != kind:
            continue
        value = _to_date(row.get('end'))
        if value is not None:
            ends[code] = value
    return ends


def _factor_end_by_code(path: Path) -> dict[str, date]:
    if not path.is_file():
        return {}
    try:
        frame = (pl.scan_parquet(str(path)).select('ts_code', 'trade_date')
                 .group_by('ts_code').agg(pl.col('trade_date').max()).collect())
    except (OSError, pl.exceptions.PolarsError):
        return {}
    ends: dict[str, date] = {}
    for row in frame.iter_rows(named=True):
        value = _to_date(row['trade_date'])
        if value is not None:
            ends[str(row['ts_code'])] = value
    return ends


def _stale_factor_codes(root: Path, kind: str) -> list[str]:
    """因子末日落后于该标的行情末日的代码（= 本次需增量重算的集合）。"""
    quote_ends = _quote_end_by_code(root, kind)
    if not quote_ends:
        return []
    name = 'stock_adj_factor.parquet' if kind == 'stock' else 'fund_adj_factor.parquet'
    factor_ends = _factor_end_by_code(root / 'factors' / name)
    return sorted(code for code, quote_end in quote_ends.items()
                  if factor_ends.get(code) is None or factor_ends[code] < quote_end)


def _register_quote_paths(plan: _Plan, kind: str, codes: Iterable[str]) -> None:
    """把本步会重写的行情件登记进 manifest 校验白名单。"""
    for code in codes:
        rel = plan.store.quote_path(kind, code).relative_to(plan.root).as_posix()
        plan.expected.add(rel)
        plan.expected.add(Path(rel).with_suffix('.manifest.json').as_posix())
        plan.plan_quote.add(rel)


def _load_codes(args: argparse.Namespace) -> set[str] | None:
    codes: set[str] = set()
    if args.codes:
        codes.update(part.strip().upper() for part in args.codes.split(',') if part.strip())
    if args.code_file:
        path = Path(args.code_file)
        if not path.is_file():
            raise SystemExit(f'代码清单不存在：{path}')
        codes.update(
            line.strip().upper() for line in path.read_text(encoding='utf-8').splitlines()
            if line.strip() and not line.strip().startswith('#'))
    return codes or None


def _walk(root: Path, marker_ts: float) -> dict[str, int]:
    """返回 mtime 晚于开工标记的文件 → 字节数（跳过 ``_backup``）。"""
    changed: dict[str, int] = {}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [name for name in dirnames if name not in SKIP_DIRS]
        for name in filenames:
            if name.endswith(('.tmp', '.temp')):
                continue
            path = Path(dirpath) / name
            try:
                stat = path.stat()
            except OSError:
                continue
            if stat.st_mtime > marker_ts:
                changed[path.relative_to(root).as_posix()] = stat.st_size
    return changed


# ----------------------------------------------------------------------
# 计划：缺口定位 + 输入集合哈希（落 _staging/<批次>/）
# ----------------------------------------------------------------------
def _plan_gaps(coverage: dict[tuple[str, str], dict[str, Any]], latest: date,
               data_types: Sequence[str], codes: set[str] | None,
               max_gap_days: int | None = DEFAULT_MAX_GAP_DAYS
               ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """缺口清单：返回 (待补, 长期停更跳过)。闸门与 fill_daily_gap 的 max_gap_days 同口径。"""
    gaps: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for (kind, code), row in coverage.items():
        if kind not in data_types:
            continue
        if codes is not None and code not in codes:
            continue
        local_end = row.get('end')
        if local_end is not None and local_end >= latest:
            continue
        item = {
            'data_type': kind,
            'ts_code': code,
            'local_end': None if local_end is None else str(local_end),
            'target_date': str(latest),
            'gap_days': None if local_end is None else (latest - local_end).days,
        }
        if (max_gap_days is not None and item['gap_days'] is not None
                and item['gap_days'] > max_gap_days):
            skipped.append(item)
        else:
            gaps.append(item)
    covered = {key[1] for key in coverage}
    for code in sorted(codes or ()):
        if code in covered:
            continue
        gaps.append({
            'data_type': 'stock', 'ts_code': code, 'local_end': None,
            'target_date': str(latest), 'gap_days': None,
        })
    key = lambda item: (item['data_type'], item['ts_code'])  # noqa: E731
    return sorted(gaps, key=key), sorted(skipped, key=key)


def _input_set(coverage: dict[tuple[str, str], dict[str, Any]], *,
               codes: set[str] | None, data_types: Sequence[str],
               start: date | None, end: date) -> dict[str, Any]:
    coverage_digest = _sha256_text(
        f'{kind}|{code}|{row.get("rows")}|{row.get("end")}'
        for (kind, code), row in sorted(coverage.items()))
    payload = {
        'codes': None if codes is None else sorted(codes),
        'data_types': list(data_types),
        'since': None if start is None else start.isoformat(),
        'target_date': end.isoformat(),
        'coverage_digest': coverage_digest,
    }
    payload['input_hash'] = _sha256_text(
        [f'{key}={json.dumps(value, ensure_ascii=False)}'
         for key, value in sorted(payload.items())])
    return payload


def _write_staging(batch_dir: Path, gaps: list[dict[str, Any]],
                   inputs: dict[str, Any]) -> None:
    """把缺口清单与输入集合哈希落到 ``_staging/<批次>/``（一次 apply 的输入）。"""
    import polars as pl  # noqa: PLC0415

    from src.data.atomic_write import write_text_atomic  # noqa: PLC0415

    plan_path = batch_dir / 'input_set.json'
    if plan_path.is_file():
        existing = json.loads(plan_path.read_text(encoding='utf-8'))
        if existing.get('status') == 'applied':
            raise SystemExit(
                f'批次 {batch_dir.name} 已于 {existing.get("applied_at")} 执行过一次 apply。'
                f'禁止在同一批次上「补一批 → 复跑 → 再补」：请换一个新批次名重跑。')
        if existing.get('input_hash') != inputs['input_hash']:
            raise SystemExit(
                f'批次 {batch_dir.name} 已存在且输入集合不同（{existing.get("input_hash")} '
                f'!= {inputs["input_hash"]}）。按 DG-35 不得在同一批次上「补一批 → 复跑 → '
                f'再补」：请换一个新批次名重跑。')
        logger.info('[daily] 批次 {} 输入集合一致，按原清单执行', batch_dir.name)
    batch_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(gaps, schema={
        'data_type': pl.String, 'ts_code': pl.String, 'local_end': pl.String,
        'target_date': pl.String, 'gap_days': pl.Int64,
    }).write_parquet(batch_dir / 'gaps.parquet')
    payload = {**inputs, 'status': 'planned', 'gaps': len(gaps),
               'generated_at': datetime.now().isoformat(timespec='seconds')}
    write_text_atomic(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', plan_path)


def _mark_applied(batch_dir: Path, summary: dict[str, Any]) -> None:
    from src.data.atomic_write import write_text_atomic  # noqa: PLC0415

    plan_path = batch_dir / 'input_set.json'
    if not plan_path.is_file():
        return
    payload = json.loads(plan_path.read_text(encoding='utf-8'))
    payload.update({'status': 'applied',
                    'applied_at': datetime.now().isoformat(timespec='seconds'),
                    **summary})
    write_text_atomic(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', plan_path)


# ----------------------------------------------------------------------
# 执行：采集 → 回填 → 本地构建
# ----------------------------------------------------------------------
def _latest_local(coverage: dict[tuple[str, str], dict[str, Any]]) -> date | None:
    ends = [row['end'] for row in coverage.values() if row.get('end') is not None]
    return max(ends) if ends else None


def _run_fill(config: Any, store: LocalQuoteStore, latest: date, *,
              codes: set[str] | None, data_types: Sequence[str],
              start: date | None, max_gap_days: int | None) -> dict[str, Any]:
    from src.data.tencent.client import TencentClient  # noqa: PLC0415
    from src.data.tencent.daily_fill import fill_daily_gap  # noqa: PLC0415

    # 单连接串行：workers=1；min_interval 取客户端默认，不在此改写。
    return fill_daily_gap(
        config, store, TencentClient(), codes=codes, data_types=data_types,
        since=start, latest_trade_date=latest, workers=1,
        max_gap_days=max_gap_days)


def _run_module(cmd: list[str], label: str, *, verbose: bool = False) -> int:
    """跑一个独立子进程，返回退出码。

    默认吞掉过程输出（单入口只留一行汇总），仅在该步失败时回显末尾几行；
    ``verbose`` 打开后原样透传到 stderr。
    """
    logger.info('[daily] {}：{}', label, ' '.join(cmd[1:]))
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True,
                          encoding='utf-8', errors='replace')
    if verbose:
        for stream in (proc.stdout, proc.stderr):
            if stream:
                sys.stderr.write(stream if stream.endswith('\n') else stream + '\n')
    elif proc.returncode != 0:
        tail = '\n'.join((proc.stderr or proc.stdout or '').strip().splitlines()[-5:])
        if tail:
            sys.stderr.write(f'[daily] {label} 失败输出（末尾 5 行）：\n{tail}\n')
    return proc.returncode


def _age_days(path: Path) -> int | None:
    """文件距今天数（按本地日期）；文件不存在返回 None。"""
    if not path.is_file():
        return None
    return (date.today() - date.fromtimestamp(path.stat().st_mtime)).days


def _basic_checked_age(root: Path, rels: tuple[str, str]) -> int | None:
    """距上次「检查」的天数：优先 manifest 的 ``last_checked_at``。

    名录每日检查、有变更才落盘，无变更时 parquet 的 mtime 不动；若按 mtime 判
    到期，会把「今天已检查过」误当成「没检查过」而重复跑。
    """
    manifest = root / rels[1]
    if manifest.is_file():
        try:
            payload = json.loads(manifest.read_text(encoding='utf-8'))
            value = payload.get('last_checked_at')
            if isinstance(value, str):
                return (date.today() - date.fromisoformat(value[:10])).days
        except (OSError, ValueError):
            pass
    return _age_days(root / rels[0])


def _basic_check_record(root: Path, rels: tuple[str, str]) -> dict[str, Any]:
    """读名录 manifest 的最近一次检查记录（检查日期与是否有变更）。"""
    manifest = root / rels[1]
    if not manifest.is_file():
        return {}
    try:
        payload = json.loads(manifest.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {}
    record = dict(payload.get('last_check') or {})
    record['last_checked_at'] = payload.get('last_checked_at')
    return record


def _basic_due(root: Path, refresh_days: int) -> dict[str, tuple[str, str]]:
    """距上次检查超过 ``refresh_days`` 天的名录（文件缺失也算到期）。"""
    due: dict[str, tuple[str, str]] = {}
    for key, rels in AUX_BASIC_OUTPUTS.items():
        age = _basic_checked_age(root, rels)
        if age is None or age >= refresh_days:
            due[key] = rels
    return due


def _aux_expected(root: Path, refresh_days: int,
                  pool_refresh_days: int = DEFAULT_POOL_REFRESH_DAYS) -> set[str]:
    """新增旁路会写的件，并入日更预期清单（只作白名单，不强制必须出现）。"""
    expected = set(AUX_ST_OUTPUTS)
    expected.update(AUX_BREADTH_OUTPUTS)
    for key, rels in _basic_due(root, refresh_days).items():
        expected.update(rels)
    if _pool_due(root, pool_refresh_days):
        expected.update(AUX_POOL_OUTPUTS)
    return expected


def _pool_due(root: Path, refresh_days: int) -> bool:
    """候选池距上次重建是否已到周更阈值（文件缺失也算到期）。"""
    age = _age_days(root / AUX_POOL_OUTPUTS[0])
    return age is None or age >= refresh_days


def _pool_codes(path: Path) -> set[str]:
    """候选池当前成分（用于重建前后对比进出）。"""
    if not path.is_file():
        return set()
    try:
        return set(pl.read_parquet(path, columns=['ts_code'])['ts_code'].to_list())
    except (OSError, pl.exceptions.PolarsError):
        return set()


def _excluded_codes(root: Path) -> tuple[int, int]:
    """排除清单：返回（排除只数，带原因与评估日期的条目数）。"""
    path = root / 'meta' / 'multi_asset' / 'etf_pool_excluded.json'
    if not path.is_file():
        return 0, 0
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return 0, 0
    return int(payload.get('count') or len(payload.get('codes') or [])), len(
        payload.get('items') or [])


def _run_pool_refresh(plan: _Plan, args: argparse.Namespace) -> str:
    """候选池按周重建：到期才跑，输出一行进出变动；失败只告警。"""
    root = plan.root
    if args.skip_pool_refresh:
        logger.info('[daily] 已跳过候选池重建（--skip-pool-refresh）')
        return ''
    if not _pool_due(root, args.pool_refresh_days):
        logger.info('[daily] 候选池未到期（阈值 {} 天），本轮不重建', args.pool_refresh_days)
        return ''
    pool_path = root / AUX_POOL_OUTPUTS[0]
    before = _pool_codes(pool_path)
    code = _run_module(
        [sys.executable, str(ROOT / 'tools' / 'build_multi_asset_etf_pool.py'), '--apply'],
        '候选池重建')
    after = _pool_codes(pool_path)
    if code != 0 or not after:
        logger.warning('[daily] 候选池重建未完成（返回 {}），只告警', code)
        return '[候选池] 重建未完成（只告警，不影响日更结果）'
    entered = len(after - before) if before else 0
    exited = len(before - after) if before else 0
    excluded, with_reason = _excluded_codes(root)
    as_of = '-'
    try:
        as_of = str(pl.read_parquet(pool_path, columns=['coverage_end'])['coverage_end']
                    .max())[:10]
    except (OSError, pl.exceptions.PolarsError):
        pass
    return (f'[候选池] as_of {as_of} | 池内 {len(after)} 只 | 新入池 {entered} / 出池 {exited} | '
            f'按规则排除 {excluded} 只（{with_reason} 条带原因与评估日期，见 '
            'etf_pool_excluded.json）')


def _valuation_last_date(root: Path) -> date | None:
    """读估值表末日（只读一列）；文件缺失或读取失败返回 None。"""
    path = root / 'meta' / 'financials' / 'valuation_daily.parquet'
    if not path.is_file():
        return None
    try:
        value = pl.read_parquet(path, columns=['trade_date'])['trade_date'].max()
    except (OSError, pl.exceptions.PolarsError):
        return None
    return value if isinstance(value, date) else None


def _run_valuation(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """全市场日频估值增量：起点由脚本从本地估值末日 + 1 自动推算，终点取最近交易日。

    失败只记录、不影响后续步骤；源端当日未更新时本次落 0 行、末日不动，
    次日起点不变、终点前移，不会把未采到的日期永久标记完成。
    """
    if args.skip_valuation:
        return True, '已跳过（--skip-valuation）'
    if not args.allow_remote:
        return True, '未允许联网，跳过（只告警）'
    last = _valuation_last_date(plan.root)
    if last is None:
        return False, '估值表缺失，无法推算起点'
    code = _run_module(
        [sys.executable, str(ROOT / 'tools' / 'collect_financials.py'),
         '--only', 'valuation', '--since', 'auto'], '全市场估值增量')
    after = _valuation_last_date(plan.root)
    ok = code == 0 and after is not None and after >= plan.latest
    return ok, f'末日 {last}→{after or "-"} | rc={code}'


def _run_breadth(plan: _Plan) -> tuple[bool, str]:
    """市场宽度日更：增量补齐快照，行情末根未前进时不动盘；失败只告警。"""
    path = plan.root / AUX_BREADTH_OUTPUTS[0]
    try:
        before = path.stat().st_mtime_ns
    except OSError:
        before = 0
    try:
        from src.data.services.breadth_service import refresh  # noqa: PLC0415
        frame = refresh(plan.root)
    except Exception as exc:  # noqa: BLE001 - 派生表失败不能反噬日更
        return False, f'刷新异常：{type(exc).__name__}: {exc}'
    if frame.is_empty() or 'trade_date' not in frame.columns:
        return True, '无行情可算，未落盘'
    last = str(frame['trade_date'].max())[:10]
    try:
        after = path.stat().st_mtime_ns
    except OSError:
        after = 0
    state = '已补齐' if after != before else '无新增未落盘'
    return True, f'末日 {last} | {frame.height:,} 行 | {state}'


def _step_st(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """ST 快照 + supplement 重建 + 区间表日更；失败只记录，不改日更结果。

    顺序固定为「快照 → supplement → 规范表」：``st_history build`` 只负责把
    supplement 并入规范表，若先合并再重建 supplement，规范表的覆盖窗口仍停在
    上次构建日。这里用 ``build`` 而不是 ``apply``：``apply`` = build + 再次
    ``st_history build``，与紧随其后的规范表重建重复，还会多打一份快照。
    """
    if args.skip_st_refresh:
        return True, '已跳过（--skip-st-refresh）'
    store_args = ['--store-root', str(plan.root)]
    snapshot = _run_module(
        [sys.executable, '-m', 'src.data.risk_flags', 'refresh',
         '--as-of', plan.latest.isoformat(), *store_args], 'ST 快照刷新')
    if snapshot != 0:
        return False, f'快照 rc={snapshot} / supplement 与规范表未重建'
    supplement = _run_module(
        [sys.executable, '-m', 'src.data.st_name_history', *store_args, 'build'],
        'ST 补充覆盖重建')
    if supplement != 0:
        # supplement 写盘是「区间 → 覆盖 → csv → manifest」的事务，失败可能停在
        # 中间态；此时不重建规范表，避免把半套 supplement 并进正式表。次日重跑即可。
        logger.warning('[daily] ST 补充覆盖重建失败（rc={}），本轮跳过规范表重建；'
                       'supplement 可能停留在上一版或中间态，需重跑第 8 步', supplement)
        return False, f'快照 rc=0 / supplement rc={supplement} / 规范表未重建'
    build = _run_module(
        [sys.executable, '-m', 'src.data.st_history', *store_args, 'build'],
        'ST 区间表重建')
    return build == 0, f'快照 rc=0 / supplement rc=0 / 重建 rc={build}'


def _step_basic(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """fund_basic / index_basic 名录每日检查（有变更才落盘）。"""
    if args.skip_basic_refresh:
        return True, '已跳过（--skip-basic-refresh）'
    due = _basic_due(plan.root, args.basic_refresh_days)
    if not due:
        return True, '未到检查周期'
    problems: list[str] = []
    done: list[str] = []
    for key, rels in due.items():
        flag = '--update-fund' if key == 'fund' else '--update-index'
        code = _run_module([sys.executable, '-m', 'src.main', flag],
                           f'{key}_basic 名录检查')
        record = _basic_check_record(plan.root, rels)
        checked_today = (str(record.get('last_checked_at') or '')[:10]
                         == date.today().isoformat())
        changed = bool(record.get('changed'))
        if code != 0 or not checked_today:
            problems.append(f'{key} rc={code}（检查日期未推进）')
        else:
            done.append(f'{key}={"有变更" if changed else "无变更"}')
    return not problems, ' | '.join(done + problems) or '无到期名录'


def _run_freshness(root: Path) -> tuple[bool, str]:
    """日更收尾跑一次保鲜体检，返回（是否无 error/warning，单行状态）。"""
    cmd = [sys.executable, '-m', 'src.data.quality.freshness_check', '--summary',
           '--store-root', str(root)]
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True,
                          encoding='utf-8', errors='replace')
    if proc.returncode not in (0, 1):
        return False, f'体检异常（rc={proc.returncode}）：{(proc.stderr or "").strip()[:200]}'
    line = (proc.stdout or '').strip().splitlines()
    return proc.returncode == 0, (line[-1] if line else '无输出')


def _run_version(root: Path, changed) -> tuple[bool, str]:
    """记录数据版本，并给本次重建的派生表打「基于哪版数据」标记。

    只写 ``meta/data_version.json`` 与 ``meta/derived_versions.json``，不改数据、
    不触发任何重建或重跑；失败只告警，不影响日更结果。
    """
    try:
        from src.data.data_version import (
            audit_derived,
            format_audit_line,
            record_version,
            stamp_rebuilt,
        )
        record_version(root)
        stamped = stamp_rebuilt(root, changed)
        return True, f'{format_audit_line(audit_derived(root))} | 本次标记派生 {len(stamped)} 张'
    except Exception as exc:  # noqa: BLE001 - 标记失败不能反噬日更
        return False, f'{type(exc).__name__}: {exc}'


def _run_build(*, since: date | None, end: date | None,
               codes: set[str] | None, skip_factor_verify: bool = False) -> int:
    cmd = [sys.executable, '-m', 'src.data.local_store', 'build']
    if codes:
        cmd += ['--codes', ','.join(sorted(codes))]
    elif since is not None:
        cmd += ['--since', since.isoformat()]
    else:
        cmd += ['--incremental']
    if end is not None:
        cmd += ['--max-date', end.isoformat()]
    if skip_factor_verify:
        cmd.append('--skip-factor-verify')
    return _run_module(cmd, '本地构建')


def _build_targets(plan: _Plan) -> set[str]:
    """本轮 build 会重写的行情件（与 ``build._target_is_stale`` 同一判定）。

    预期集合必须与构建写盘口径一致：源 ``.day`` 被重新同步后，即使末根日期
    没前进，只要末行内容变了，构建也会重写该件；只按日期推导会把这些正常
    刷新误报成「预期外改动」。在构建前一刻取当前覆盖度，避免计划时点与构建
    时点之间的采集写入造成口径差。
    """
    tdx_root = getattr(getattr(getattr(plan, 'config', None), 'data', None),
                       'tdx_data_path', None)
    if not tdx_root or not Path(tdx_root).is_dir():
        return set()
    from src.data.local_store.build import stale_targets  # noqa: PLC0415

    rels: set[str] = set()
    for kind, ts_code, _path in stale_targets(
            Path(tdx_root), plan.store, data_type='all',
            codes=plan.codes, max_date=plan.latest):
        rel = plan.store.quote_path(kind, ts_code).relative_to(plan.root).as_posix()
        rels.add(rel)
        rels.add(Path(rel).with_suffix('.manifest.json').as_posix())
    return rels


# ----------------------------------------------------------------------
# 校验：逐件对 manifest + 汇总对账 + 预期差集
# ----------------------------------------------------------------------
def _declared_nodes(payload: Any, rel: str, root: Path,
                    base: Path | None = None) -> list[dict[str, Any]]:
    """递归找出所有声明了 ``rel`` 且带统计字段的节点。"""
    hits: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        node = payload
        local_base = base
        declared_dir = node.get('dir')
        if isinstance(declared_dir, str):
            local_base = root.joinpath(*[p for p in declared_dir.replace('\\', '/').split('/')
                                         if p and p not in ('data', 'local_store')])
        if any(key in node for key in STAT_KEYS):
            declared = node.get('path') or node.get('source_parquet')
            if isinstance(declared, str):
                text = declared.replace('\\', '/')
                if text.startswith(STORE_PREFIX + '/'):
                    text = text[len(STORE_PREFIX) + 1:]
                if not os.path.isabs(text):
                    for prefix in (root, local_base, root.parent):
                        if prefix is None:
                            continue
                        candidate = (prefix / text).resolve()
                        if candidate == (root / rel).resolve():
                            hits.append(node)
                            break
        for value in node.values():
            if isinstance(value, (dict, list)):
                hits.extend(_declared_nodes(value, rel, root, local_base))
    elif isinstance(payload, list):
        for item in payload:
            hits.extend(_declared_nodes(item, rel, root, base))
    return hits


def _find_entry(root: Path, rel: str) -> dict[str, Any] | None:
    """找声明该文件的登记节点：先同级 sidecar，再同目录 manifest.json。"""
    path = root / rel
    for manifest in (path.with_suffix('.manifest.json'), path.parent / 'manifest.json'):
        if not manifest.is_file():
            continue
        try:
            payload = json.loads(manifest.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            continue
        nodes = _declared_nodes(payload, rel, root)
        if not nodes and manifest.name == 'manifest.json':
            # 同目录以文件名为键的写法：{"files": {"a.parquet": {...}}}
            files = payload.get('files') if isinstance(payload, dict) else None
            if isinstance(files, dict):
                node = files.get(path.name)
                if isinstance(node, dict) and any(key in node for key in STAT_KEYS):
                    nodes = [node]
        if nodes:
            return nodes[0]
    return None


def _refresh(root: Path, changed: dict[str, int]) -> int:
    """把本次改动件按磁盘实测回写进各自的 manifest（sidecar 优先，其次目录级）。"""
    grouped: dict[str, list[str]] = {}
    for rel in sorted(changed):
        if not rel.startswith('quotes/') or not rel.endswith('.parquet'):
            continue
        side_rel = Path(rel).with_suffix('.manifest.json').as_posix()
        if (root / side_rel).is_file():
            grouped.setdefault(side_rel, []).append(rel)
    for rel in sorted(changed):
        if not rel.startswith('meta/') or not rel.endswith('.parquet'):
            continue
        parent_rel = (Path(rel).parent / 'manifest.json').as_posix()
        if (root / parent_rel).is_file():
            grouped.setdefault(parent_rel, []).append(rel)
    anomalies = [rel for rel in changed if rel.startswith('meta/anomalies/')
                 and rel.endswith(('.csv', '.tsv'))]
    if anomalies and (root / 'meta' / 'anomalies' / 'manifest.json').is_file():
        grouped['meta/anomalies/manifest.json'] = anomalies
    if 'meta/daily_index.parquet' in changed:
        grouped['meta/daily_index.manifest.json'] = ['meta/daily_index.parquet']
    refreshed = 0
    for manifest_rel, files in grouped.items():
        manifest = root / manifest_rel
        if not manifest.is_file():
            continue
        refreshed += len(refresh_manifest_files(
            manifest, root, [root / item for item in files]))
    return refreshed


def _summary_problems(root: Path) -> list[str]:
    """同级 manifest 汇总字段对账（只核自洽：分块求和 == 合计）。"""
    problems: list[str] = []
    quotes = root / 'quotes' / 'manifest.json'
    if quotes.is_file():
        try:
            payload = json.loads(quotes.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            payload = {}
        for kind, block in (payload.get('universe') or {}).items():
            detail = block.get('markets_detail') or {}
            if not isinstance(detail, dict) or not detail:
                continue
            for field in ('files', 'rows', 'bytes'):
                if field not in block:
                    continue
                total = sum(int(item.get(field) or 0) for item in detail.values())
                if total != int(block.get(field) or 0):
                    problems.append(
                        f'quotes/manifest.json universe.{kind}.{field} 分块和 {total} '
                        f'!= 合计 {block.get(field)}')
    factors = root / 'factors' / 'manifest.json'
    if factors.is_file():
        try:
            payload = json.loads(factors.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            payload = {}
        totals = payload.get('totals') if isinstance(payload, dict) else None
        if isinstance(totals, dict):
            tables = [item for item in (payload.get('files') or {}).values()
                      if isinstance(item, dict) and item.get('kind') == 'parquet']
            for field in ('rows', 'bytes'):
                if field in totals and sum(int(item.get(field) or 0) for item in tables) != int(totals[field]):
                    problems.append(
                        f'factors/manifest.json totals.{field} 与 parquet 条目求和不符')
    return problems


def _verify(root: Path, changed: dict[str, int], expected: set[str],
            before: dict[tuple[str, str], dict[str, Any]],
            after: dict[tuple[str, str], dict[str, Any]]) -> dict[str, Any]:
    mismatches: list[str] = []
    unregistered: list[str] = []
    for rel in sorted(changed):
        if not rel.endswith(DATA_SUFFIXES) or rel.endswith('.manifest.json'):
            continue
        node = _find_entry(root, rel)
        if node is None:
            unregistered.append(rel)
            continue
        facts = file_facts(root / rel)
        for key in STAT_KEYS:
            if key in node and node[key] != facts[key]:
                mismatches.append(f'{rel}:{key}(manifest={node[key]} disk={facts[key]})')
    unexpected = sorted(
        rel for rel in changed
        if rel not in expected and not rel.startswith(SIDE_PREFIXES))
    added = deleted = 0
    for key in set(before) | set(after):
        old = int((before.get(key) or {}).get('rows') or 0)
        new = int((after.get(key) or {}).get('rows') or 0)
        added += max(0, new - old)
        deleted += max(0, old - new)
    problems = mismatches + [f'预期外改动 {rel}' for rel in unexpected] + _summary_problems(root)
    return {
        'added': added, 'deleted': deleted, 'problems': problems,
        'unregistered': unregistered, 'unexpected': unexpected,
    }


# ----------------------------------------------------------------------
# 主流程
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class _StepOutcome:
    key: str
    name: str
    critical: bool
    ok: bool
    detail: str
    seconds: float


def _run_steps(steps: Sequence[tuple[str, str, bool, Callable[[], tuple[bool, str]]]]
               ) -> list[_StepOutcome]:
    """按顺序执行步骤：单步失败或异常只记录，继续跑后续步骤。"""
    outcomes: list[_StepOutcome] = []
    for key, name, critical, func in steps:
        started = time.perf_counter()
        try:
            ok, detail = func()
        except Exception as exc:  # noqa: BLE001 - 单步异常不得中断后续步骤
            ok, detail = False, f'{type(exc).__name__}: {exc}'
        outcomes.append(_StepOutcome(key, name, critical, ok, detail,
                                     time.perf_counter() - started))
        logger.info('[daily] 步骤 {} {}：{}', key, 'ok' if ok else '失败', detail)
    return outcomes


@dataclass(frozen=True)
class _Plan:
    root: Path
    config: Any
    store: LocalQuoteStore
    factor_store: LocalFactorStore
    data_types: list[str]
    codes: set[str] | None
    latest: date
    coverage: dict[tuple[str, str], dict[str, Any]]
    gaps: list[dict[str, Any]]
    skipped: list[dict[str, Any]]
    inputs: dict[str, Any]
    expected: set[str]
    plan_quote: set[str]
    range_label: str
    marker_ts: float


def _build_plan(args: argparse.Namespace) -> _Plan:
    root = Path(args.store_root) if args.store_root else Path(default_store_root())
    if not root.is_dir():
        raise SystemExit(f'local_store 根不存在：{root}')
    data_types = [item.strip() for item in args.data_types.split(',') if item.strip()]
    codes = _load_codes(args)

    from src.utils.config import Config  # noqa: PLC0415

    config = Config()
    store = LocalQuoteStore(root)
    factor_store = LocalFactorStore(root, enabled=True)
    marker_ts = datetime.now().timestamp()

    coverage = _coverage(root)
    latest = args.end
    if latest is None:
        if args.allow_remote and args.apply:
            from src.data.tencent.client import TencentClient  # noqa: PLC0415
            from src.data.tencent.daily_fill import resolve_latest_trade_date  # noqa: PLC0415

            latest = resolve_latest_trade_date(TencentClient())
        else:
            latest = _latest_local(coverage)
    if latest is None:
        raise SystemExit('无法确定目标交易日：请显式给 --end，或加 --allow-remote 在 apply 时自动探测')

    gaps, skipped = _plan_gaps(coverage, latest, data_types, codes, args.max_gap_days)
    inputs = _input_set(coverage, codes=codes, data_types=data_types,
                        start=args.start, end=latest)
    expected = {store.quote_path(item['data_type'], item['ts_code']).relative_to(root).as_posix()
                for item in gaps}
    plan_quote = set(expected)
    for rel in list(expected):
        expected.add(Path(rel).with_suffix('.manifest.json').as_posix())
    expected.update(SIDE_OUTPUTS)
    expected.update(AUX_VALUATION_OUTPUTS)
    expected.update(AUX_DIVIDEND_OUTPUTS)
    expected.update(AUX_CODE_TABLE_OUTPUTS)
    expected |= _aux_expected(root, args.basic_refresh_days, args.pool_refresh_days)
    if codes:
        # --codes 模式下 build 会无条件重写这些标的，属显式声明的写目标。
        known: dict[str, set[str]] = {}
        for (kind, code) in coverage:
            known.setdefault(code, set()).add(kind)
        for code in sorted(codes):
            for kind in sorted(known.get(code) or {'stock'}):
                rel = store.quote_path(kind, code).relative_to(root).as_posix()
                expected.add(rel)
                expected.add(Path(rel).with_suffix('.manifest.json').as_posix())
                plan_quote.add(rel)

    return _Plan(root=root, config=config, store=store, factor_store=factor_store,
                 data_types=data_types, codes=codes, latest=latest,
                 coverage=coverage, gaps=gaps,
                 skipped=skipped, inputs=inputs, expected=expected,
                 plan_quote=plan_quote,
                 range_label=f'{(args.start or "全部")}~{latest}', marker_ts=marker_ts)


def _run_target_holdings(trade_day: date, capital: float) -> int:
    """日更收尾后顺带生成 v3 目标持仓清单（独立子进程，只读消费、不改日更结果）。"""
    script = ROOT / 'tools' / 'target_holdings.py'
    try:
        proc = subprocess.run(
            [sys.executable, str(script), '--date', trade_day.isoformat(),
             '--capital', str(capital)],
            cwd=str(ROOT), check=False)
    # 兜底理由：清单生成是可选旁路，任何异常都只告警，不影响日更原有退出码
    except Exception as exc:  # noqa: BLE001
        logger.warning('[daily] 目标持仓清单生成异常（日更结果不受影响）：{}', exc)
        return 1
    if proc.returncode != 0:
        logger.warning('[daily] 目标持仓清单生成返回非零 {}（日更结果不受影响）',
                       proc.returncode)
    return proc.returncode


def _step_code_table(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """证券码表：通达信当日码表 → PIT 名称历史 → 股票名录。"""
    root = plan.root
    notes: list[str] = []
    names = _run_module([sys.executable, str(ROOT / 'tools' / 'build_names_json.py'),
                         '--store-root', str(root)], '证券码表 names.json')
    notes.append(f'names.json rc={names}')
    history = 0
    if args.allow_remote:
        history = _run_module([sys.executable, str(ROOT / 'tools' / 'build_name_history.py'),
                               '--apply'], '名称历史 name_history')
        notes.append(f'name_history rc={history}')
    else:
        notes.append('name_history 跳过（未允许联网）')
    basic = _run_module([sys.executable, str(ROOT / 'tools' / 'build_stock_basic.py'),
                         '--store-root', str(root), '--promote'], '股票名录 stock_basic')
    notes.append(f'stock_basic rc={basic}')
    return names == 0 and history == 0 and basic == 0, ' | '.join(notes)


def _step_industry(plan: _Plan) -> tuple[bool, str]:
    code = _run_module(
        [sys.executable, '-m', 'src.data.industry', 'refresh',
         '--as-of', plan.latest.isoformat(), '--store-root', str(plan.root)], '行业快照')
    return code == 0, f'as_of {plan.latest} | rc={code}'


def _step_fill(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    if not args.allow_remote:
        return True, '未允许联网，跳过采集'
    stats = _run_fill(plan.config, plan.store, plan.latest, codes=plan.codes,
                      data_types=plan.data_types, start=args.start,
                      max_gap_days=args.max_gap_days)
    if stats.get('rows_added'):
        plan.store.flush_meta()  # 采集写盘后立即回填 daily_index 与其 manifest
    failed = int(stats.get('failed_symbols') or 0)
    aborted = stats.get('aborted')
    detail = f"新增 {int(stats.get('rows_added') or 0):,} 行 | 失败 {failed} 个"
    failed_codes = stats.get('failed_codes') or []
    if failed_codes:
        detail += f"（{', '.join(str(code) for code in failed_codes[:5])}）"
    if aborted:
        return False, f'{detail} | 闸门拦下：{aborted}'
    return failed == 0, detail


def _step_build(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    # 只做构建 + 末日断言，不带 build 自带的因子副本一致性门禁：那条断言要求
    # 基金行情（第 5 步）与基金因子（第 6 步）都已是目标日，放在这里会先于
    # 修复步骤失败并形成死锁。一致性断言统一在第 6 步刷新完成后执行。
    # 预期写目标与构建同判定、取构建前一刻的覆盖度，源文件同日刷新才不会误报。
    rewritten = _build_targets(plan)
    plan.expected.update(rewritten)
    plan.plan_quote.update(rewritten)
    code = _run_build(since=args.start, end=plan.latest, codes=plan.codes,
                      skip_factor_verify=True)
    if code != 0:
        return False, f'rc={code}'
    expected = _recent_completed_trade_day(plan.root)
    if args.end is not None and args.end < expected:
        return True, (f'rc=0 | 回补模式（--end {args.end} 早于最近交易日 {expected}），'
                      '跳过末日断言')
    ok, note = _check_daily_index_end(plan.root, expected)
    return ok, f'rc=0 | {note}'


def _step_fund_quotes(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """基金行情刷新：通达信本地 lday 增量；末日已到目标则跳过不重算（致命步）。"""
    if args.skip_fund_refresh:
        return True, '已跳过（--skip-fund-refresh）'
    target = plan.latest
    before = _daily_index_end(plan.root, kinds=('fund',))
    if before is not None and before >= target:
        return True, f'末日 {before} 已到 {target}，跳过不重算'
    if before is None:
        return False, '基金行情在 meta/daily_index 中缺失（无 fund 覆盖）'
    behind = sorted(code for code, end in _quote_end_by_code(plan.root, 'fund').items()
                    if end < target)
    _register_quote_paths(plan, 'fund', behind)
    code = _run_module([
        sys.executable, '-m', 'src.data.local_store', 'build',
        '--data-type', 'fund', '--incremental', '--max-date', target.isoformat(),
        '--skip-derive-turnover', '--skip-anomaly-lists', '--skip-factor-verify',
    ], '基金行情刷新')
    after = _daily_index_end(plan.root, kinds=('fund',))
    ok = code == 0 and after is not None and after >= target
    return ok, f'末日 {before}→{after or "-"} | 落后 {len(behind)} 只 | rc={code}'


def _read_optional_frame(path: Path) -> pl.DataFrame:
    if not path.is_file():
        return pl.DataFrame()
    try:
        return pl.read_parquet(path)
    except (OSError, pl.exceptions.PolarsError):
        return pl.DataFrame()


def _factor_copy_mismatch(plan: _Plan) -> dict[str, int]:
    """刷新后复验行情内嵌因子副本 vs 因子快照；返回各类型不一致标的数。"""
    quote_store = plan.store or LocalQuoteStore(plan.root, enabled=True)
    factor_store = plan.factor_store or LocalFactorStore(plan.root, enabled=True)
    result = verify_quote_factor_consistency(quote_store, factor_store)
    if 'error' in result:
        raise RuntimeError(str(result['error']))
    return {kind: int(info.get('mismatch_codes') or 0)
            for kind, info in result.items()}


def _step_factors(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """复权因子刷新 → 收尾复验行情内嵌副本（致命步）。

    第 4 步 build 已改带 ``--skip-factor-verify``：它自带的副本一致性断言依赖
    本步结果。断言在这里执行——个股/指数/基金刷新完之后再校验，覆盖本次运行
    刷新的范围；真正的滞后（重算没推进末日或副本仍不一致）照样判红。
    """
    notes: list[str] = []
    remaining_stock: list[str] = []
    remaining_fund: list[str] = []
    if args.skip_factor_refresh:
        notes.append('已跳过刷新（--skip-factor-refresh）')
    else:
        stock_codes = _stale_factor_codes(plan.root, 'stock')
        fund_codes = _stale_factor_codes(plan.root, 'fund')
        if not stock_codes and not fund_codes:
            notes.append(
                f'个股/基金因子末日均对齐行情（{_daily_index_end(plan.root)}），跳过不重算')
        else:
            _register_quote_paths(plan, 'stock', stock_codes)
            _register_quote_paths(plan, 'fund', fund_codes)
            plan.expected.update(AUX_FACTOR_OUTPUTS)
            if stock_codes:
                from src.data.local_store.factor_rebuild import (  # noqa: PLC0415
                    rebuild_factors_from_local,
                )

                stats = rebuild_factors_from_local(
                    plan.store, plan.factor_store, codes=stock_codes, include_existing=True)
                notes.append(f'个股 {len(stock_codes)} 只 → 新 {int(stats.get("codes_added") or 0)} 只'
                             f' / {int(stats.get("rows_added") or 0):,} 行')
            if fund_codes:
                from src.data.local_store.fund_factors import (  # noqa: PLC0415
                    rebuild_fund_factors,
                )

                dividends = _read_optional_frame(plan.root / 'factors' / 'fund_dividend.parquet')
                splits = _read_optional_frame(plan.root / 'factors' / 'fund_share_change.parquet')
                stats = rebuild_fund_factors(plan.root, dividends, splits, codes=fund_codes)
                notes.append(f'基金 {len(fund_codes)} 只 / {int(stats.get("factor_rows") or 0):,} 行')
            remaining_stock = _stale_factor_codes(plan.root, 'stock')
            remaining_fund = _stale_factor_codes(plan.root, 'fund')
            if remaining_stock or remaining_fund:
                notes.append(f'仍落后 个股 {len(remaining_stock)} 只 / 基金 {len(remaining_fund)} 只')
    mismatch = _factor_copy_mismatch(plan)
    if any(mismatch.values()):
        notes.append('行情内嵌因子副本与快照不一致：个股 {} 只 / 指数 {} 只 / 基金 {} 只'.format(
            mismatch.get('stock', 0), mismatch.get('index', 0), mismatch.get('fund', 0)))
        return False, ' | '.join(notes)
    notes.append('副本一致（个股/指数/基金 0/0/0）')
    return not (remaining_stock or remaining_fund), ' | '.join(notes)


def _step_dividend(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """分红日频增量（非致命）：基金年度批量 + 个股近两报告期，只追加/补缺。

    脚本内按源隔离失败：失败源不写盘、不删行，另一源照常；受影响标的的复权
    因子与行情内嵌副本在同一步重建。这里只把会写的件登记进 manifest 白名单，
    并从状态台账取受影响标的登记行情路径。
    """
    if args.skip_dividend_refresh:
        return True, '已跳过（--skip-dividend-refresh）'
    if not args.allow_remote:
        return True, '未允许联网，跳过（只告警）'
    plan.expected.update(AUX_DIVIDEND_OUTPUTS)
    code = _run_module(
        [sys.executable, str(ROOT / 'tools' / 'refresh_dividend_daily.py'),
         '--store-root', str(plan.root), '--date', plan.latest.isoformat(),
         '--bootstrap-days', str(args.dividend_bootstrap_days)], '分红日频增量')
    state_path = plan.root / 'meta' / 'dividend_status' / 'daily_refresh_state.json'
    try:
        state = json.loads(state_path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        state = {}
    last = state.get('last_result') or {}
    sources = (last.get('sources') if last.get('target') == plan.latest.isoformat() else None) or {
        kind: state.get(kind) or {} for kind in ('fund', 'stock')}
    notes: list[str] = []
    for kind in ('fund', 'stock'):
        item = sources.get(kind) or {}
        codes = [str(value) for value in (item.get('affected_codes') or [])]
        if codes:
            _register_quote_paths(plan, kind, codes)
            plan.expected.update(AUX_FACTOR_OUTPUTS)
        if item.get('ok') and not item.get('skipped'):
            rows = item.get('added_rows', item.get('appended_rows', 0))
            notes.append(f'{kind} 新增 {int(rows or 0)} 行 / 影响 {len(codes)} 只')
        elif not item.get('ok'):
            notes.append(f'{kind} 失败：{str(item.get("error") or "-")[:60]}')
    detail = ' | '.join(notes) or '无增量'
    return code == 0, f'{detail} | rc={code}'


def _step_pool(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    line = _run_pool_refresh(plan, args)
    return '未完成' not in line, (line or '未到期/已跳过')


def _step_manifest(plan: _Plan, args: argparse.Namespace,
                   state: dict[str, Any]) -> tuple[bool, str]:
    """回写本次改动件的 manifest，并做逐件对账与预期差集检查（致命步）。"""
    root = plan.root
    changed = _walk(root, plan.marker_ts)
    refreshed = _refresh(root, changed)
    report = _verify(root, changed, plan.expected, plan.coverage, _coverage(root))
    digest = _sha256_text(f'{rel}:{file_facts(root / rel)["sha256"]}'
                          for rel in sorted(changed)
                          if (root / rel).is_file())
    for rel in report['unregistered']:
        logger.warning('[daily] 未登记（manifest 无该件条目）：{}', rel)
    remaining, _ = _plan_gaps(_coverage(root), plan.latest, plan.data_types,
                              plan.codes, args.max_gap_days)
    if remaining:
        logger.warning('[daily] 仍有缺口 {} 个（采集侧无新增/未服务，按 DG-35 不再复跑，'
                       '需补请另开批次）', len(remaining))
    state.update(changed=changed, report=report, digest=digest)
    detail = (f'变更 {len(changed)} 件 | manifest 回写 {refreshed} 条 | '
              f"+{report['added']:,}/-{report['deleted']:,} 行 | 差异 {len(report['problems'])}")
    if report['problems']:
        detail += ' | ' + '；'.join(report['problems'][:3])
    return not report['problems'], detail


def _step_clean_set(plan: _Plan) -> tuple[bool, str]:
    code = _run_module(
        [sys.executable, '-m', 'src.data.quality.clean_set', 'build',
         '--as-of', plan.latest.isoformat(), '--root', str(plan.root)], '洁净集快照')
    return code == 0, f'as_of {plan.latest} | rc={code}'


def _step_backup() -> tuple[bool, str]:
    code = _run_module([sys.executable, str(ROOT / 'tools' / 'backup_critical_assets.py')],
                       '不可重建资产备份')
    return code == 0, f'rc={code}'


def _step_gate(plan: _Plan) -> tuple[bool, str]:
    code = _run_module([sys.executable, str(ROOT / 'tools' / 'daily_data_gate.py'),
                        '--store-root', str(plan.root)], '每日数据门禁')
    return code == 0, f'rc={code}'


def _step_list(plan: _Plan, args: argparse.Namespace,
               state: dict[str, Any]) -> list[tuple[str, str, bool,
                                                    Callable[[], tuple[bool, str]]]]:
    """按 STEP_PLAN 组装可执行步骤（顺序与 dry-run 展示一致）。"""
    runners: dict[str, Callable[[], tuple[bool, str]]] = {
        'code_table': lambda: _step_code_table(plan, args),
        'industry': lambda: _step_industry(plan),
        'fill': lambda: _step_fill(plan, args),
        'build': lambda: _step_build(plan, args),
        'fund_quote': lambda: _step_fund_quotes(plan, args),
        'factor': lambda: _step_factors(plan, args),
        'dividend': lambda: _step_dividend(plan, args),
        'st': lambda: _step_st(plan, args),
        'basic': lambda: _step_basic(plan, args),
        'valuation': lambda: _run_valuation(plan, args),
        'breadth': lambda: _run_breadth(plan),
        'pool': lambda: _step_pool(plan, args),
        'manifest': lambda: _step_manifest(plan, args, state),
        'clean_set': lambda: _step_clean_set(plan),
        'freshness': lambda: _run_freshness(plan.root),
        'version': lambda: _run_version(plan.root, state.get('changed') or {}),
        'backup': _step_backup,
        'gate': lambda: _step_gate(plan),
    }
    return [(key, name, critical, runners[key])
            for key, name, critical in STEP_PLAN]


def _summary_line(plan: _Plan, outcomes: Sequence[_StepOutcome],
                  state: dict[str, Any], seconds: float) -> str:
    failed = [item for item in outcomes if not item.ok]
    fatal = [item for item in failed if item.critical]
    report = state.get('report') or {}
    changed = len(state.get('changed') or ())
    return (f'[日更] {plan.range_label} | {len(outcomes)} 步 '
            f'ok {len(outcomes) - len(failed)} / 失败 {len(failed)}（致命 {len(fatal)}）'
            f' | 变更 {changed} 件 +{report.get("added", 0):,}/-{report.get("deleted", 0):,} 行'
            f' | 总耗时 {seconds:.1f}s')


def _print_run_summary(plan: _Plan, outcomes: Sequence[_StepOutcome],
                       state: dict[str, Any], seconds: float) -> int:
    print(_summary_line(plan, outcomes, state, seconds))
    for item in outcomes:
        if not item.ok:
            mark = '致命' if item.critical else '只记录'
            print(f'[失败] {item.name}（{mark}）| {item.detail}')
    return 1 if any(item.critical and not item.ok for item in outcomes) else 0


def _dividend_skip_note(args: argparse.Namespace) -> str:
    if args.skip_dividend_refresh:
        return '  ← 本轮跳过（--skip-dividend-refresh）'
    if not args.allow_remote:
        return '  ← 本轮跳过（未加 --allow-remote）'
    return f'  ← 基金年度批量 + 个股近两报告期，增量窗口 {args.dividend_bootstrap_days} 天兜底'


def _skip_note(key: str, plan: _Plan, args: argparse.Namespace) -> str:
    if key == 'fill' and not args.allow_remote:
        return '  ← 本轮跳过（未加 --allow-remote）'
    if key == 'fund_quote':
        if args.skip_fund_refresh:
            return '  ← 本轮跳过（--skip-fund-refresh）'
        end = _daily_index_end(plan.root, kinds=('fund',))
        if end is None:
            return '  ← 无 fund 覆盖，执行时会判失败'
        if end >= plan.latest:
            return f'  ← 末日 {end} 已最新，跳过'
        behind = sum(1 for value in _quote_end_by_code(plan.root, 'fund').values()
                     if value < plan.latest)
        return f'  ← 末日 {end} → {plan.latest}，落后 {behind} 只'
    if key == 'factor':
        if args.skip_factor_refresh:
            return '  ← 本轮跳过（--skip-factor-refresh）'
        stock_stale = len(_stale_factor_codes(plan.root, 'stock'))
        fund_stale = len(_stale_factor_codes(plan.root, 'fund'))
        if stock_stale + fund_stale == 0:
            return '  ← 因子末日均已对齐行情，跳过'
        return f'  ← 个股 {stock_stale} 只 / 基金 {fund_stale} 只待增量重算'
    if key == 'dividend':
        return _dividend_skip_note(args)
    if key == 'valuation' and (args.skip_valuation or not args.allow_remote):
        return '  ← 本轮跳过'
    if key == 'st' and args.skip_st_refresh:
        return '  ← 本轮跳过'
    if key == 'basic' and (args.skip_basic_refresh
                           or not _basic_due(plan.root, args.basic_refresh_days)):
        return '  ← 本轮跳过（未到期/已关闭）'
    if key == 'pool' and (args.skip_pool_refresh
                          or not _pool_due(plan.root, args.pool_refresh_days)):
        return '  ← 本轮跳过（未到期/已关闭）'
    return ''


def _print_dry_plan(plan: _Plan, args: argparse.Namespace) -> None:
    print(f'[日更 dry-run] {plan.range_label} | 覆盖 {len(plan.coverage)} 个标的 | '
          f'待补 {len(plan.gaps)} 个 | 计划写 {len(plan.plan_quote)} 件 | '
          f'输入集合哈希 {plan.inputs["input_hash"][:12]}')
    for index, (key, name, critical) in enumerate(STEP_PLAN, 1):
        mark = '（致命）' if critical else ''
        print(f'  {index:>2}. {name}{mark}{_skip_note(key, plan, args)}')
    print(f'  顺序约束：备份（第 {len(STEP_PLAN) - 1} 步）在所有写 data/ 的步骤之后，'
          f'门禁（第 {len(STEP_PLAN)} 步）在备份之后；任一步失败只记录并继续，'
          '致命步失败返回 1。')


def _run_apply(plan: _Plan, args: argparse.Namespace) -> int:
    batch = args.batch or f'daily_{plan.latest.strftime("%Y%m%d")}'
    batch_dir = ROOT / '_staging' / batch
    _write_staging(batch_dir, plan.gaps, plan.inputs)

    state: dict[str, Any] = {}
    started = time.perf_counter()
    outcomes = _run_steps(_step_list(plan, args, state))
    seconds = time.perf_counter() - started

    report = state.get('report') or {}
    _mark_applied(batch_dir, {
        'changed_files': len(state.get('changed') or ()),
        'rows_added': report.get('added', 0),
        'rows_deleted': report.get('deleted', 0),
        'diffs': len(report.get('problems') or ()),
        'agg_sha256': state.get('digest'),
    })
    code = _print_run_summary(plan, outcomes, state, seconds)
    if args.target_holdings:
        _run_target_holdings(plan.latest, args.capital)
    return code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='日更/补数单入口（一条命令 + 一行状态）')
    parser.add_argument('--start', type=_parse_date, default=None,
                        help='起始日期（YYYY-MM-DD；作为采集 since 与构建 --since 下界）')
    parser.add_argument('--end', type=_parse_date, default=None,
                        help='目标交易日（缺省：允许联网时自动探测，否则取本地最新末根）')
    parser.add_argument('--codes', default='', help='逗号分隔的 ts_code 清单（补数用）')
    parser.add_argument('--code-file', default='', help='代码清单文件（一行一个，补数用）')
    parser.add_argument('--batch', default='', help='补数批次名，缺口落到 _staging/<批次>/')
    parser.add_argument('--data-types', default='stock,index',
                        help='参与采集的资产类型（默认 stock,index）')
    parser.add_argument('--max-gap-days', type=int, default=DEFAULT_MAX_GAP_DAYS,
                        help='末根距目标交易日超过该天数视为长期停更，不进待补清单（默认 90）')
    parser.add_argument('--store-root', default=None, help='local_store 根（默认取项目配置）')
    parser.add_argument('--apply', action='store_true', help='真正写盘（缺省 dry-run）')
    parser.add_argument('--allow-remote', action='store_true',
                        help='显式允许联网采集（缺省只跑本地构建与校验）')
    parser.add_argument('--verbose', action='store_true',
                        help='打开 INFO/DEBUG 过程日志（缺省静默，只留一行状态与告警）')
    parser.add_argument('--target-holdings', action='store_true',
                        help='apply 成功后顺带生成 v3 每日目标持仓清单（只读消费，'
                             '失败不改日更原有退出码）')
    parser.add_argument('--capital', type=float, default=1e9,
                        help='目标持仓清单的资金规模（元，仅 --target-holdings 生效）')
    parser.add_argument('--skip-st-refresh', action='store_true',
                        help='跳过 ST 区间表日更（risk_flags refresh + st_history build）')
    parser.add_argument('--skip-fund-refresh', action='store_true',
                        help='跳过基金行情刷新（通达信本地增量）')
    parser.add_argument('--skip-factor-refresh', action='store_true',
                        help='跳过复权因子刷新（个股 + 基金增量重算）')
    parser.add_argument('--skip-dividend-refresh', action='store_true',
                        help='跳过分红日频增量（基金年度批量 + 个股近两报告期）')
    parser.add_argument('--dividend-bootstrap-days', type=int, default=30,
                        help='分红增量首次运行或断档时的回溯天数（默认 30，仅作窗口兜底）')
    parser.add_argument('--skip-valuation', action='store_true',
                        help='跳过全市场日频估值增量（collect_financials.py --only valuation）')
    parser.add_argument('--skip-basic-refresh', action='store_true',
                        help='跳过 fund_basic / index_basic 名录每日检查')
    parser.add_argument('--basic-refresh-days', type=int, default=DEFAULT_BASIC_REFRESH_DAYS,
                        help='名录距上次检查超过该自然日数才检查（默认 1，即每天检查）')
    parser.add_argument('--skip-pool-refresh', action='store_true',
                        help='跳过多资产 ETF 候选池周更')
    parser.add_argument('--pool-refresh-days', type=int, default=DEFAULT_POOL_REFRESH_DAYS,
                        help='候选池距上次重建超过该天数才重建（默认 7）')
    args = parser.parse_args(argv)

    logger.remove()
    logger.add(sys.stderr, level='DEBUG' if args.verbose else 'WARNING')

    plan = _build_plan(args)
    logger.info('[daily] 覆盖 {} 个标的，目标交易日 {}，待补 {} 个（长期停更跳过 {} 个），输入集合哈希 {}',
                len(plan.coverage), plan.latest, len(plan.gaps), len(plan.skipped),
                plan.inputs['input_hash'])

    if not args.apply:
        # dry-run：只盘点，不联网、不落盘（含 _staging）；打印完整步骤清单与顺序
        _print_dry_plan(plan, args)
        return 0

    return _run_apply(plan, args)


if __name__ == '__main__':
    raise SystemExit(main())
