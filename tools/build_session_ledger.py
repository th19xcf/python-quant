#!/usr/bin/env python3

r"""构建交易日 session 台账（``meta/trading_calendar/session_ledger.parquet``）。

为什么只登记例外
================
交易日日期集由**一条代码规则**给出：``∪ BENCHMARK_CODES 的指数实际日期 +
台账 status=session − 台账 status=closed``（见 ``src/data/trading_calendar.py``）。
台账只登记「指数并集给不出的裁定」——1991 深市周六开市、1992 上游漏输出的
session、基金净值占位周末、以及唯一一处两源冲突未决日。普通交易日不入表，
因此台账共 74 行（58 session + 16 closed），不随日历每天长大。

层级职责
========
- 日期级（本表）：该自然日是否进网格；
- 行级（``docs/evidence/g112_nontrading_bars/nontrading_bars_verdict.csv``）：
  该 ``(ts_code, date)`` bar 是否可用。日历含该日 ≠ 该 bar 有效。

输入与迁移
==========
一次性从 ``meta/anomalies/weekend_calendar_registry.json`` 迁移（DG-1 收敛为一份）。
该 JSON 已按 DG-13 退役到仓库根 ``_backup/meta/anomalies_registry_20261113/``，
此后重建台账须用 ``--registry`` 显式指向退役副本——代码里没有任何默认读
``_backup`` 的路径常量（DG-14）。

复现::

    .venv\\Scripts\\python.exe tools\\build_session_ledger.py            # 只建候选
    .venv\\Scripts\\python.exe tools\\build_session_ledger.py --apply    # 校验并原子写入
    .venv\\Scripts\\python.exe tools\\build_session_ledger.py --registry <退役副本.json>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.data.atomic_write import write_parquet_atomic, write_text_atomic  # noqa: E402
from src.data.staging import PROJECT_ROOT, staging_dir  # noqa: E402

TASK_NAME = 'session_ledger_20261113'
FORMAL_REL = Path('meta') / 'trading_calendar'
LEDGER_NAME = 'session_ledger.parquet'

#: 迁移源（唯一写入口再跑一次时须用 --registry 显式指向退役副本）
REGISTRY_REL = Path('meta') / 'anomalies' / 'weekend_calendar_registry.json'
#: 行级判定表（只用于取 evidence_path 与行级冲突说明，不决定日期级 status）
G112_VERDICT_REL = Path('docs') / 'evidence' / 'g112_nontrading_bars' / 'nontrading_bars_verdict.csv'
#: 1991/1992 周末口径审计文档（kind_rule 出处）
AUDIT_DOC = 'docs/早期深市日历口径审计_2026-11-12.md'

#: 并集成员（与 src/data/trading_calendar.py 的 BENCHMARK_CODES 一致）
BENCHMARK_CODES: tuple[str, ...] = (
    '000001.SH', '399001.SZ', '399002.SZ', '399003.SZ', '000300.SH')

#: 台账 schema（顺序即落盘顺序）
COLUMNS: dict[str, pl.DataType] = {
    'date': pl.Date,
    'status': pl.String,
    'kind': pl.String,
    'evidence': pl.String,
    'source': pl.String,
    'evidence_path': pl.String,
    'effective_from': pl.Date,
    'effective_to': pl.Date,
    'unresolved': pl.Boolean,
    'decided_at': pl.Date,
    'note': pl.String,
}

STATUS_VALUES = ('session', 'closed')
DECIDED_AT = date(2026, 11, 13)
EFFECTIVE_FROM = date(1990, 1, 1)
EFFECTIVE_TO = date(2099, 12, 31)

#: 预期行数（迁移断言用；与设计稿 §2.3 一致）
EXPECTED_ROWS = 74
EXPECTED_SESSION = 58
EXPECTED_CLOSED = 16
#: registry 覆盖行数（去重前）
EXPECTED_REGISTRY_ROWS = 1079
#: 1993 年未覆盖行数（不迁移，写进 manifest；01-03 已单列为 session 行，故 49 → 48）
UNCOVERED_1993_ROWS = 48

#: 台账内直接登记的 session（registry 无对应 kind，判据固化在 NOTE_1993_01_03）
EXTRA_SESSIONS: dict[date, str] = {
    date(1993, 1, 3): 'weekend_session_1993_sz_only',
}

KIND_EVIDENCE: dict[str, str] = {
    'weekend_session_1991': '1991 年深市周六/周日实际开市（六天工作周），当日有真实成交 bar',
    'upstream_omitted_session': '上游日线缺该日行，但下一行 preclose 等于本地当日收盘，上游漏输出一个 session',
    'weekend_session_1992_sz_only': '1992-02-01 深市侧 session 成立：东财 stock_zh_a_hist 当日行与本地/TDX 逐值全等，'
                                    '东财 02-02/02-03 涨跌额闭合，新浪日历对周末无表征能力；沪市侧无 bar，非全市场开市',
    'weekend_session_1993_sz_only': '1993-01-03 深市侧元旦调休开市：6 只深市标的与 01-04 后链闭合、TDX 深市 9 文件命中；'
                                    '沪市侧无 bar，非全市场开市',
    'unresolved_session': '两源冲突且无第三证据（DG-18）时的挂起行；当前台账已无此类行',
    'nav_placeholder_weekend': '基金净值占位行（volume=0、价格冻结），非交易 session，必须排除',
}

#: 1992-02-01 第三源取证结论（parent 2026-11-13 裁定），完整写进 note 与 manifest。
NOTE_1992_02_01 = (
    '第三源判 session 成立：① 东财 stock_zh_a_hist 02-01 行 '
    'O31.15/H32.00/L31.15/C32.00/V2,228/额7,129,600，与本地及 TDX 逐值全等；'
    '② 东财 02-02 涨跌额 +0.32 = 32.32−32.00、02-03 −0.32 = 32.00−32.32，'
    '形成 01-31→02-01→02-02→02-03 闭合链；'
    '③ 新浪交易日历 1992 年周六 0/52、周日 0/52（1990–1999 每年 0/52 或 0/53），'
    '对周末开市无表征能力，故 registry「上游休市」不成立；'
    '对照 02-04~02-06 春节五源全无行，说明这些源能表示休市、只是不表示周末。'
    '限定：深市侧 session 成立，不是全市场开市——TDX 18 个 1992 年文件中沪市侧 0 个含 02-01，'
    '新浪上证指数 01-31(313.24)→02-03(开 313.24) 缝内 0 个 session。'
    'preclose 链未印证该日，不作判据。'
)

#: 1993-01-03 判据（parent 2026-11-13 裁定），完整写进 note 与 manifest。
NOTE_1993_01_03 = (
    '深市侧元旦调休开市（不是全市场开市）：① 6/6 后链闭合——1993-01-04 的 pre_close 逐只等于 '
    '01-03 的 close（000002/000004/000009/000012/000017/000505），若 01-03 是残留 bar，'
    '01-04 前收应回到 12-31 而不是 01-03 的收盘；'
    '② TDX 深市 lday 9 个文件含 19930103（6 只股票 + 399001/399002/399003），沪市 0 个文件命中；'
    '③ 元旦调休形态：01-01（周五）与 01-02（周六）本地均 0 根 bar；'
    '行级 G-112 判 keep 6/6，当日量额非零且与 TDX 逐值全等。'
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _index_path(ts_code: str) -> str:
    market = ts_code.rsplit('.', 1)[-1].lower()
    return f'quotes/index/{market}/{ts_code}.parquet'


def _index_dates(root: Path) -> dict[str, set[date]]:
    """各成员指数的实际日期集合（文件缺失按空集合，不报错）。"""
    out: dict[str, set[date]] = {}
    for code in BENCHMARK_CODES:
        path = root / _index_path(code)
        if not path.is_file():
            logger.warning(f'[ledger] 并集成员缺失 {path}')
            out[code] = set()
            continue
        out[code] = set(pl.read_parquet(path, columns=['date'])['date'].to_list())
    return out


def _g112_counts() -> dict[date, dict[str, int]]:
    """G-112 行级判定按日期汇总（keep/drop 行数），用于 note 与 source 判定。"""
    path = PROJECT_ROOT / G112_VERDICT_REL
    if not path.is_file():
        return {}
    verdict = pl.read_csv(path).with_columns(pl.col('date').str.to_date())
    out: dict[date, dict[str, int]] = {}
    for row in verdict.iter_rows(named=True):
        slot = out.setdefault(row['date'], {'keep': 0, 'drop': 0})
        slot[row['verdict']] = slot.get(row['verdict'], 0) + 1
    return out


def _parse_dates(items: list[str]) -> list[date]:
    return [date.fromisoformat(str(item)[:10]) for item in items]


def _source_of(kind: str, members: list[str], counts: dict[str, int]) -> tuple[str, str]:
    """按 kind 优先级给出 (source, evidence_path)；有并集成员时以第一个成员的文件为证据。"""
    if kind == 'weekend_session_1992_sz_only':
        return 'em+tdx', str(G112_VERDICT_REL)
    if kind == 'weekend_session_1993_sz_only':
        return 'tdx+g112', str(G112_VERDICT_REL)
    if members:
        return f'index:{members[0]}', _index_path(members[0])
    if kind == 'upstream_omitted_session':
        return 'baostock+sina', AUDIT_DOC
    if kind == 'unresolved_session' or counts.get('keep'):
        return 'tdx_only' if kind == 'unresolved_session' else 'g112', str(G112_VERDICT_REL)
    return 'manual', AUDIT_DOC


def build_rows(root: Path, registry: dict[str, Any]) -> list[dict[str, Any]]:
    """按「日期级取 session 优先」把 registry 六类折叠成 74 行。"""
    weekend_dates = registry['weekend_dates']
    sessions_1991 = set(_parse_dates(weekend_dates.get('weekend_session_1991', [])))
    omitted_1992 = set(_parse_dates(weekend_dates.get('upstream_omitted_session_1992', [])))
    closed_bar_1992 = set(_parse_dates(weekend_dates.get('upstream_closed_local_bar_1992', [])))
    shift_1992 = set(_parse_dates(weekend_dates.get('weekend_date_shift_1992', [])))
    zero_vol_1992 = set(_parse_dates(weekend_dates.get('weekend_zero_volume_placeholder_1992', [])))
    nav_placeholder = set(_parse_dates(weekend_dates.get('nav_placeholder_weekend', [])))

    index_dates = _index_dates(root)
    g112 = _g112_counts()

    decided: dict[date, dict[str, str]] = {}
    # 1) 有 session 证据的 1991 周末（六天工作周）
    for day in sorted(sessions_1991):
        decided[day] = {'status': 'session', 'kind': 'weekend_session_1991'}
    # 2) 1992 上游漏输出的 session（preclose 链证据，优先级高于「本地独有 bar」）
    for day in sorted(omitted_1992):
        decided[day] = {'status': 'session', 'kind': 'upstream_omitted_session'}
    # 3) 仅「上游休市 / 本地独有 bar」证据：02-01 已由第三源判定为深市侧 session
    for day in sorted(closed_bar_1992 - omitted_1992):
        decided[day] = {'status': 'session', 'kind': 'weekend_session_1992_sz_only'}
    # 4) 基金净值占位周末：必须排除
    for day in sorted(nav_placeholder):
        decided[day] = {'status': 'closed', 'kind': 'nav_placeholder_weekend'}
    # 5) 台账内直接登记的 session（registry 无对应 kind）
    for day, kind in EXTRA_SESSIONS.items():
        decided[day] = {'status': 'session', 'kind': kind}

    rows: list[dict[str, Any]] = []
    for day in sorted(decided):
        slot = decided[day]
        kind = slot['kind']
        status = slot['status']
        members = [code for code in BENCHMARK_CODES if day in index_dates[code]]
        counts = g112.get(day, {})
        source, evidence_path = _source_of(kind, members, counts)

        notes: list[str] = []
        if len(members) > 1:
            notes.append(f'并集成员 {",".join(members)}')
        if counts.get('drop'):
            notes.append(f'行级 G-112 判 drop {counts["drop"]} 行（日期级仍按 {status}）')
        if kind == 'weekend_session_1992_sz_only':
            notes.append(NOTE_1992_02_01)
        if kind == 'weekend_session_1993_sz_only':
            notes.append(NOTE_1993_01_03)
        if day in shift_1992:
            notes.append('registry 另登记 weekend_date_shift_1992（已结案，日期级见 changelog）')
        if day in zero_vol_1992:
            notes.append('registry 另登记 weekend_zero_volume_placeholder_1992（行级属性，不单列）')

        rows.append({
            'date': day,
            'status': status,
            'kind': kind,
            'evidence': KIND_EVIDENCE[kind],
            'source': source,
            'evidence_path': evidence_path,
            'effective_from': EFFECTIVE_FROM,
            'effective_to': EFFECTIVE_TO,
            'unresolved': kind == 'unresolved_session',
            'decided_at': DECIDED_AT,
            'note': '；'.join(notes) if notes else None,
        })
    return rows


def _to_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    frame = pl.DataFrame(rows, schema=COLUMNS)
    return frame.sort('date')


def _check_consistency(frame: pl.DataFrame) -> list[str]:
    """schema 级硬约束（与行数无关，便于用小型夹具做反例自测）。"""
    problems: list[str] = []
    missing = [c for c in COLUMNS if c not in frame.columns]
    if missing:
        return [f'缺列 {missing}']
    if frame['date'].n_unique() != frame.height:
        problems.append('date 不唯一')
    for column in COLUMNS:
        nulls = int(frame[column].null_count())
        if nulls and column != 'note':
            problems.append(f'{column} 有 {nulls} 个空值')
    unknown_status = sorted(set(frame['status'].unique().to_list()) - set(STATUS_VALUES))
    if unknown_status:
        problems.append(f'status 越界 {unknown_status}')
    need_note = frame.filter(pl.col('unresolved') & pl.col('note').is_null())
    if need_note.height:
        problems.append(f'unresolved 行缺 note {need_note.height} 行')
    bad_pending = frame.filter(pl.col('unresolved') & (pl.col('status') != 'session'))
    if bad_pending.height:
        problems.append(f'unresolved 行必须 status=session，违反 {bad_pending.height} 行')
    bad_nav = frame.filter((pl.col('kind') == 'nav_placeholder_weekend')
                           & (pl.col('status') != 'closed'))
    if bad_nav.height:
        problems.append(f'nav_placeholder 必须 closed，违反 {bad_nav.height} 行')
    forbidden = {date(1993, 5, 1), date(1993, 5, 2), date(1992, 4, 18)}
    leaked = sorted(set(frame['date'].to_list()) & forbidden)
    if leaked:
        problems.append(f'台账含禁止日期 {[d.isoformat() for d in leaked]}')
    return problems


def validate(frame: pl.DataFrame, registry: dict[str, Any]) -> dict[str, Any]:
    """台账强约束：行数守恒 + 一致性 + registry 覆盖行守恒。"""
    summary: dict[str, Any] = {
        'rows': frame.height,
        'session_rows': int((frame['status'] == 'session').sum()),
        'closed_rows': int((frame['status'] == 'closed').sum()),
        'unresolved_rows': int(frame['unresolved'].sum()),
        'kind_rows': {row['kind']: int(row['len']) for row in
                      frame.group_by('kind').len().sort('kind').iter_rows(named=True)},
        'status_values': sorted(frame['status'].unique().to_list()),
    }
    problems = _check_consistency(frame)
    if frame.height != EXPECTED_ROWS:
        problems.append(f'行数 {frame.height} != {EXPECTED_ROWS}')
    if summary['session_rows'] != EXPECTED_SESSION:
        problems.append(f'session 行 {summary["session_rows"]} != {EXPECTED_SESSION}')
    if summary['closed_rows'] != EXPECTED_CLOSED:
        problems.append(f'closed 行 {summary["closed_rows"]} != {EXPECTED_CLOSED}')
    registry_rows = sum(int(v) for v in registry['kind_rows'].values()) if 'kind_rows' in registry else -1
    if registry_rows != EXPECTED_REGISTRY_ROWS:
        problems.append(f'registry 覆盖行 {registry_rows} != {EXPECTED_REGISTRY_ROWS}')
    summary['registry_covered_rows'] = registry_rows
    summary['uncovered_1993_rows'] = UNCOVERED_1993_ROWS
    if problems:
        raise RuntimeError('台账校验失败: ' + '；'.join(problems))
    return summary


def _manifest(root: Path, frame: pl.DataFrame, summary: dict[str, Any],
              registry_path: Path, ledger: Path) -> dict[str, Any]:
    upstream = {}
    for code in BENCHMARK_CODES:
        path = root / _index_path(code)
        upstream[_index_path(code)] = {
            'exists': path.is_file(),
            'sha256': _sha256(path) if path.is_file() else None,
        }
    upstream[str(G112_VERDICT_REL)] = {
        'exists': (PROJECT_ROOT / G112_VERDICT_REL).is_file(),
        'sha256': (_sha256(PROJECT_ROOT / G112_VERDICT_REL)
                   if (PROJECT_ROOT / G112_VERDICT_REL).is_file() else None),
    }
    upstream[str(REGISTRY_REL)] = {
        'exists': False,
        'sha256': _sha256(registry_path) if registry_path.is_file() else None,
        'retired_to': '_backup/meta/anomalies_registry_20261113/',
        'note': 'DG-1/DG-13：registry 已退役，台账为其唯一继承者；迁移源 SHA256 留证',
    }
    return {
        'semantic': 'trading_calendar_session_ledger',
        'generated_at': datetime.now(UTC).isoformat(),
        'rows': frame.height,
        'columns': list(COLUMNS),
        'status_counts': {'session': summary['session_rows'], 'closed': summary['closed_rows']},
        'kind_counts': summary['kind_rows'],
        'unresolved_rows': summary['unresolved_rows'],
        'decisions': [{
            'date': '1992-02-01',
            'status': 'session',
            'kind': 'weekend_session_1992_sz_only',
            'decided_at': DECIDED_AT.isoformat(),
            'criteria': [
                '东财 stock_zh_a_hist 02-01 行 O31.15/H32.00/L31.15/C32.00/V2,228/额7,129,600 '
                '与本地及 TDX 逐值全等',
                '东财 02-02 涨跌额 +0.32 = 32.32−32.00、02-03 −0.32 = 32.00−32.32，'
                '形成 01-31→02-01→02-02→02-03 闭合链',
                '新浪交易日历 1992 年周六 0/52、周日 0/52 对周末开市无表征能力，'
                'registry「上游休市」不成立；对照 02-04~02-06 春节五源全无行',
            ],
            'limitation': '深市侧 session 成立，不是全市场开市（TDX 18 个 1992 年文件沪市侧 0 个含 '
                          '02-01；新浪上证指数 01-31(313.24)→02-03(开 313.24) 缝内 0 个 session）',
            'not_used_as_evidence': 'preclose 链未印证 02-01（baostock 02-03 前收 32.35、'
                                    '东财反推 32.32，指向的都是 02-02 的 32.32）',
            'rollback': '已取消：第三源已取到，不再保留「删行回落 8,818」的分支',
        }, {
            'date': '1993-01-03',
            'status': 'session',
            'kind': 'weekend_session_1993_sz_only',
            'decided_at': DECIDED_AT.isoformat(),
            'criteria': [
                '6/6 后链闭合：1993-01-04 的 pre_close 逐只等于 01-03 的 close'
                '（000002/000004/000009/000012/000017/000505）；若 01-03 为残留 bar，'
                '01-04 前收应回到 12-31',
                'TDX 深市 lday 9 个文件含 19930103（6 只股票 + 399001/399002/399003），沪市 0 个文件',
                '元旦调休形态：01-01（周五）与 01-02（周六）本地均 0 根 bar；'
                '行级 G-112 判 keep 6/6，量额非零且与 TDX 逐值全等',
            ],
            'limitation': '深市侧 session 成立，不是全市场开市（沪市侧无 bar）',
            'calendar_effect': '无：该日原已由 399001.SZ 并集纳入网格，补台账不改日历天数与该日 membership',
        }],
        'parameters': {
            'rule': '日期集 = ∪BENCHMARK_CODES 指数日期 ∪ ledger(status=session) '
                    '− ledger(status=closed)；台账只登记例外，普通交易日不落表',
            'benchmark_codes': list(BENCHMARK_CODES),
            'status_priority': 'closed 高于并集：即使指数当日有 bar 也剔除',
            'effective_window': [EFFECTIVE_FROM.isoformat(), EFFECTIVE_TO.isoformat()],
        },
        'coverage': {
            'registry_covered_rows_migrated_from': summary['registry_covered_rows'],
            'uncovered_1993_rows': UNCOVERED_1993_ROWS,
            'uncovered_1993_note': '1993 全年 48 行（含 1993-05 位移块）未纳入；6 个周六由 G-112 行级挡住；'
                                   '01-03 已单列为 session 行（weekend_session_1993_sz_only），故 49 → 48',
            'not_migrated_kinds': ['weekend_date_shift_1992', 'weekend_zero_volume_placeholder_1992'],
        },
        'files': {
            LEDGER_NAME: {
                'rows': frame.height,
                'sha256': _sha256(ledger),
                'bytes': ledger.stat().st_size,
            },
        },
        'upstream': upstream,
        'final_dir': str(root / FORMAL_REL),
    }


def _promote(ledger: Path, manifest_path: Path, root: Path) -> None:
    target_dir = root / FORMAL_REL
    target_dir.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(pl.read_parquet(ledger), target_dir / LEDGER_NAME)
    write_text_atomic(manifest_path.read_text(encoding='utf-8'), target_dir / 'manifest.json')
    if _sha256(target_dir / LEDGER_NAME) != _sha256(ledger):
        raise RuntimeError('台账提升后 SHA256 不一致')
    logger.info(f'[ledger] 正式路径 {target_dir / LEDGER_NAME} 已写入')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='构建交易日 session 台账（73 行例外裁定）')
    parser.add_argument('--store-root', default=str(PROJECT_ROOT / 'data' / 'local_store'))
    parser.add_argument('--registry', default=None, help='registry JSON 路径（缺省用正式路径）')
    parser.add_argument('--task-dir', default=TASK_NAME)
    parser.add_argument('--apply', action='store_true', help='校验通过后原子写入正式路径')
    args = parser.parse_args(argv)

    root = Path(args.store_root).resolve()
    registry_path = Path(args.registry) if args.registry else root / REGISTRY_REL
    if not registry_path.is_file():
        raise SystemExit(f'[ledger] 迁移源不存在：{registry_path}')
    registry = json.loads(registry_path.read_text(encoding='utf-8'))
    logger.info(f'[ledger] 迁移源 {registry_path}（SHA256 {_sha256(registry_path)[:12]}…）')

    rows = build_rows(root, registry)
    frame = _to_frame(rows)
    summary = validate(frame, registry)

    staging = staging_dir(args.task_dir)
    staging.mkdir(parents=True, exist_ok=True)
    ledger = staging / LEDGER_NAME
    write_parquet_atomic(frame, ledger)
    manifest = _manifest(root, frame, summary, registry_path, ledger)
    manifest_path = staging / 'manifest.json'
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')

    print('=== LEDGER SUMMARY ===')
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if not args.apply:
        logger.info(f'[ledger] 候选已写入 {staging}；加 --apply 提升到正式路径')
        return 0
    _promote(ledger, manifest_path, root)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
