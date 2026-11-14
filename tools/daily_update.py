#!/usr/bin/env python3

"""日更 / 补数单入口：一条命令跑完，一行状态收尾。

链路复用（采集与限流实现原样复用，本入口只做起编排与校验）::

    1. 采集 + 写行情 parquet        src.data.tencent.daily_fill.fill_daily_gap(workers=1)
    2. 回填 daily_index + manifest  LocalQuoteStore.flush_meta()（采集写盘后立即回填）
    3. 通达信增量构建 + 换手派生 + 异常清单  python -m src.data.local_store build build
    4. 登记回写                     src.data.manifest_refresh.refresh_manifest_files
    5. 校验                         逐件 rows/bytes/sha256 与 manifest 一致、
                                    同级 manifest 汇总字段对账、
                                    mtime 晚于开工标记的文件与预期清单求差为 0

口径与边界::

    - 采集固定 ``workers=1``（单连接串行），限流参数沿用 TencentClient 默认
      ``min_interval=0.05`` / ``max_retries=3``，本入口不暴露也不修改。
    - 补数先按代码清单把缺口落到 ``_staging/<批次>/``（``gaps.parquet`` +
      ``input_set.json``，后者带输入集合哈希），再一次 apply；同一批次名
      不允许换一份输入集合复用，防止「补一批 → 复跑 → 再补」。
    - 校验不抽样、不跑全库 ``manifest_tool --check``：只核本次改动件。
    - ``quotes/manifest.json`` 是人工维护的全市场总账（仓库内无产者，含
      futures 口径），本入口只做只读对账（universe 分市场求和 == universe
      合计），不代写数值；该件与磁盘的偏差按 WARN 打印实测差额。

用法::

    # dry-run（默认，不联网、不落盘）
    python tools/daily_update.py --start 2026-11-12 --end 2026-11-13
    # 日更 apply（采集 + 本地构建 + 校验）
    python tools/daily_update.py --start 2026-11-12 --end 2026-11-13 --apply --allow-remote
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
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from loguru import logger  # noqa: E402

from src.data.local_store.paths import default_store_root  # noqa: E402
from src.data.local_store.store import LocalQuoteStore  # noqa: E402
from src.data.manifest_refresh import file_facts, refresh_manifest_files  # noqa: E402

DATA_SUFFIXES = ('.parquet', '.csv', '.tsv', '.txt')
STAT_KEYS = ('rows', 'bytes', 'sha256')
STORE_PREFIX = 'data/local_store'
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
)
#: 构建收尾会整目录重写的台账（内容随行情变动，逐件白名单不现实）。
SIDE_PREFIXES = ('meta/anomalies/', 'factors/rebuild/')
#: 增量清点跳过 ``backtest_runs``：回测运行记录是结果归档（json/csv/log 与索引），
#: 不是数据表，日更既不产出也不消费它，计入会让「本次新增文件」混入非数据资产。
SKIP_DIRS = {'_backup', '__pycache__', 'backtest_runs'}


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


def _run_build(*, since: date | None, end: date | None,
               codes: set[str] | None) -> int:
    cmd = [sys.executable, '-m', 'src.data.local_store', 'build']
    if codes:
        cmd += ['--codes', ','.join(sorted(codes))]
    elif since is not None:
        cmd += ['--since', since.isoformat()]
    else:
        cmd += ['--incremental']
    if end is not None:
        cmd += ['--max-date', end.isoformat()]
    logger.info('[daily] 本地构建：{}', ' '.join(cmd[1:]))
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True,
                          encoding='utf-8', errors='replace')
    for stream in (proc.stdout, proc.stderr):
        if stream:
            sys.stderr.write(stream if stream.endswith('\n') else stream + '\n')
    return proc.returncode


def _tdx_stale(root: Path, tdx_path: Path | None,
               coverage: dict[tuple[str, str], dict[str, Any]],
               codes: set[str] | None) -> set[str]:
    """按通达信源末行日期算本次构建会写的行情件（只读探测，不落盘）。"""
    if tdx_path is None or not tdx_path.is_dir():
        return set()
    from concurrent.futures import ThreadPoolExecutor  # noqa: PLC0415

    from src.data.local_store.build import discover_local_files  # noqa: PLC0415
    from src.data.tdx_day_parser import read_first_last_trade_date  # noqa: PLC0415

    store = LocalQuoteStore(root)

    def probe(item: tuple[str, str, Path]) -> str | None:
        kind, ts_code, path = item
        if codes is not None and ts_code not in codes:
            return None
        try:
            _first, last = read_first_last_trade_date(path)
        except (OSError, ValueError):
            return None
        row = coverage.get((kind, ts_code))
        local_end = None if row is None else row.get('end')
        if last is None or (local_end is not None and last <= local_end):
            return None
        return store.quote_path(kind, ts_code).relative_to(root).as_posix()

    with ThreadPoolExecutor(max_workers=8) as pool:
        return {rel for rel in pool.map(probe, discover_local_files(tdx_path)) if rel}


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
    """把本次改动件按磁盘实测回写进各自的 manifest（sidecar 优先）。"""
    grouped: dict[str, list[str]] = {}
    for rel in sorted(changed):
        if not rel.startswith('quotes/') or not rel.endswith('.parquet'):
            continue
        side_rel = Path(rel).with_suffix('.manifest.json').as_posix()
        if (root / side_rel).is_file():
            grouped.setdefault(side_rel, []).append(rel)
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
def _format(range_label: str, changed: int, planned: int, added: int,
            deleted: int, diffs: int, digest: str, apply: bool) -> str:
    head = '[日更]' if apply else '[日更 dry-run]'
    count = f'变更 {changed} 件' if apply else f'变更 0 件（计划 {planned} 件）'
    return (f'{head} {range_label} | {count} | +{added:,}/-{deleted:,} 行 | '
            f'差异 {diffs} | 汇总哈希 {digest}')


@dataclass(frozen=True)
class _Plan:
    root: Path
    config: Any
    store: LocalQuoteStore
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
    tdx_root = getattr(config.data, 'tdx_data_path', None)
    tdx_stale = _tdx_stale(root, Path(tdx_root) if tdx_root else None, coverage, codes)
    expected |= tdx_stale
    plan_quote |= tdx_stale
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

    return _Plan(root=root, config=config, store=store, data_types=data_types,
                 codes=codes, latest=latest, coverage=coverage, gaps=gaps,
                 skipped=skipped, inputs=inputs, expected=expected,
                 plan_quote=plan_quote,
                 range_label=f'{(args.start or "全部")}~{latest}', marker_ts=marker_ts)


def _run_apply(plan: _Plan, args: argparse.Namespace) -> int:
    root = plan.root
    batch = args.batch or f'daily_{plan.latest.strftime("%Y%m%d")}'
    batch_dir = ROOT / '_staging' / batch
    _write_staging(batch_dir, plan.gaps, plan.inputs)

    fill_stats: dict[str, Any] = {}
    if args.allow_remote:
        fill_stats = _run_fill(plan.config, plan.store, plan.latest, codes=plan.codes,
                               data_types=plan.data_types, start=args.start,
                               max_gap_days=args.max_gap_days)
        if fill_stats.get('rows_added'):
            plan.store.flush_meta()  # 采集写盘后立即回填 daily_index 与其 manifest
        if fill_stats.get('aborted'):
            logger.error('[daily] 采集被前台闸门拦下：{}', fill_stats.get('aborted'))
        if fill_stats.get('failed_symbols'):
            logger.error('[daily] 采集失败标的 {} 个：{}',
                         fill_stats['failed_symbols'], fill_stats.get('failed_codes'))
    else:
        logger.warning('[daily] 未加 --allow-remote：跳过联网采集，只跑本地构建与校验')

    code = _run_build(since=args.start, end=plan.latest, codes=plan.codes)
    if code != 0:
        logger.error('[daily] 本地构建返回非零：{}', code)

    changed = _walk(root, plan.marker_ts)
    logger.info('[daily] 改动 {} 件，回写 manifest 登记 {} 条',
                len(changed), _refresh(root, changed))

    report = _verify(root, changed, plan.expected, plan.coverage, _coverage(root))
    digest = _sha256_text(f'{rel}:{file_facts(root / rel)["sha256"]}'
                          for rel in sorted(changed)
                          if (root / rel).is_file())
    for item in report['problems']:
        logger.error('[daily] 差异：{}', item)
    for rel in report['unregistered']:
        logger.warning('[daily] 未登记（manifest 无该件条目）：{}', rel)

    remaining, _ = _plan_gaps(_coverage(root), plan.latest, plan.data_types,
                              plan.codes, args.max_gap_days)
    if remaining:
        # 采集侧 no_new（源端确实无新增）与失败不同：只告警，不判本次失败，也不复跑。
        logger.warning('[daily] 仍有缺口 {} 个（采集侧无新增/未服务，按 DG-35 不再复跑，需补请另开批次）',
                       len(remaining))

    _mark_applied(batch_dir, {
        'changed_files': len(changed), 'rows_added': report['added'],
        'rows_deleted': report['deleted'], 'diffs': len(report['problems']),
        'agg_sha256': digest,
    })
    print(_format(plan.range_label, len(changed), len(plan.plan_quote), report['added'],
                  report['deleted'], len(report['problems']), digest[:12], apply=True))
    failed = int(fill_stats.get('failed_symbols') or 0)
    return 0 if (code == 0 and not report['problems'] and not failed) else 1


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
    args = parser.parse_args(argv)

    logger.remove()
    logger.add(sys.stderr, level='DEBUG' if args.verbose else 'WARNING')

    plan = _build_plan(args)
    logger.info('[daily] 覆盖 {} 个标的，目标交易日 {}，待补 {} 个（长期停更跳过 {} 个），输入集合哈希 {}',
                len(plan.coverage), plan.latest, len(plan.gaps), len(plan.skipped),
                plan.inputs['input_hash'])

    if not args.apply:
        # dry-run：只盘点，不联网、不落盘（含 _staging）
        if plan.gaps:
            logger.warning('[daily] 待补 {} 个，前 5：{}', len(plan.gaps),
                           ', '.join(f'{item["ts_code"]}({item["gap_days"]}d)'
                                     for item in plan.gaps[:5]))
        logger.debug('{}', json.dumps({'target_date': str(plan.latest),
                                       'input_hash': plan.inputs['input_hash'],
                                       'planned_files': len(plan.plan_quote),
                                       'gaps': plan.gaps}, ensure_ascii=False))
        print(_format(plan.range_label, 0, len(plan.plan_quote), 0, 0, 0, '-', apply=False))
        return 0

    return _run_apply(plan, args)


if __name__ == '__main__':
    raise SystemExit(main())
