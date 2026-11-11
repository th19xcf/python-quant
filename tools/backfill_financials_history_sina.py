#!/usr/bin/env python3
"""新浪财经三大报表 2010 年前历史补全（合并口径，只落 ``end_date < 2010-01-01``）。

背景
----
本地 ``meta/financials/{income,balance,cashflow}.parquet`` 由东方财富批量接口采集，
最早报告期 2010-03-31；1990–2009 全为 0 行（52 个红格）。新浪
``CompanyFinanceService.getFinanceReport2022`` 对老股票可回溯到 1990-06-30，
且单请求（``num=1000``）即返回该股全部历史报告期。

口径约定（写死，不随行业变化）
------------------------------
- ``type=0`` 实测 100% 返回 ``rType="合并期末"``，即**只有合并报表**。本工具不采母公司表，
  每条落盘记录的 ``source`` 统一带 ``:consolidated`` 后缀（schema 无 ``report_type`` 列，
  以 ``source`` 作为等价的逐行标注），与母公司口径严格区分。
- ``ann_date`` 取新浪 ``publish_date``（真实公告日），``ann_date_source="sina_publish"``。
- 金额单位与原表一致（人民币元，非万元）。
- 只写入 ``end_date < 2010-01-01`` 的行：重叠年份（2010+）仅用于对拍，绝不覆盖存量。

映射表
------
按「字段候选链 → 逐值比对」标定（``--verify`` 可复现）：每条链上第一个非空值胜出。
四条链分别对应工业 / 银行 / 保险 / 证券四种行业口径（新浪按行业返回不同字段代码），
同一本地列在四个行业里往往是 2~3 个不同 ``item_field``，故需要 4 套映射而非 1 套。

派生列（新浪不直接给，按与存量完全一致的口径自行推导，已用存量数据校验到 100% 等值）：
- ``gross_profit = revenue - operating_cost``（与 ``collector.py`` 的 ``normalize_bulk_statement`` 一致）
- ``debt_ratio = total_liab / total_assets * 100``（对存量 267,924 行逐值相等）
- ``*_yoy = (本期 / 去年同期 - 1) * 100``（单位为百分数；对存量 253,060 行中 94.9% 完全相等，
  其余为存量源的追溯重述差异，非本工具计算错误）

用法
----
    .venv\\Scripts\\python.exe tools\\backfill_financials_history_sina.py --verify
    .venv\\Scripts\\python.exe tools\\backfill_financials_history_sina.py --collect
    .venv\\Scripts\\python.exe tools\\backfill_financials_history_sina.py --collect --limit 20
    .venv\\Scripts\\python.exe tools\\backfill_financials_history_sina.py --build

断点与失败清单
--------------
``_staging/financials_history_20261111/``（仓库根，不在 ``data/`` 下）::

    progress.csv     每个 (报表, 股票) 一行，status ∈ ok/empty/failed，中断后跳过 ok
    failed.csv       失败明细（代码、原因、HTTP 状态、重试次数）
    BLOCKED.json     出现 403/429/连续超时/连续空时的停压记录
    parts/{statement}/{batch:04d}.parquet   已归一化的候选分片
"""

# ruff: noqa: T201 - CLI 面向终端输出

from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import time
from datetime import date, datetime
from pathlib import Path

import polars as pl
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data.atomic_write import write_parquet_atomic  # noqa: E402
from src.data.financials import store  # noqa: E402

SINA_URL = ('https://quotes.sina.cn/cn/api/openapi.php/'
            'CompanyFinanceService.getFinanceReport2022')
SOURCE_TAG = 'sina:getFinanceReport2022:consolidated'
ANN_SOURCE = 'sina_publish'
HISTORY_CUTOFF = date(2010, 1, 1)          # 只落这一天之前的报告期
# 各表正式写入的报告期窗口下界（含），**按报表分别声明，不设统一开关**。
# 两边性质不同，混在一个开关里迟早会把不该放的放进来：
# - 现金流量表下界 1996-01-01：原判定「1998 年前结构性不存在」是错的，实测源端确有
#   1996–1997 共 9 行 / 7 只（000563.SZ、000900.SZ、000921.SZ、600181.SH、600207.SH、
#   600217.SH、601857.SH），属个别公司追溯披露。2026-11-11 裁定补入（数据已在分片
#   缓存内，零额外请求）。分片缓存实测无 1996 年前的行，故该下界即「只放 1996–1997」。
# - 利润表/资产负债表下界保持 1990-01-01：1989-12-31 共 4 行（000001.SZ、600603.SH）
#   早于本次数据集声明起点，属干净的窗口边界，继续排除。
WINDOW_FLOOR = {'income': date(1990, 1, 1), 'balance': date(1990, 1, 1),
                'cashflow': date(1996, 1, 1)}
STAGING = PROJECT_ROOT / '_staging' / 'financials_history_20261111'
PARTS = STAGING / 'parts'
PROGRESS = STAGING / 'progress.csv'
FAILED = STAGING / 'failed.csv'
BLOCKED = STAGING / 'BLOCKED.json'
BATCH_SIZE = 200
DRY_RUN = False        # --dry-run：跑完全部断言与证据但不写盘、不备份

STATEMENT_SOURCE = {'income': 'lrb', 'balance': 'fzb', 'cashflow': 'llb'}
STATEMENT_CN = {'income': '利润表', 'balance': '资产负债表', 'cashflow': '现金流量表'}

# 直接映射：本地列 → 新浪 item_field 候选链（首个非空胜出）
# 链长 > 1 即为「4 套行业口径」的兜底；每条的标定证据见 --verify 输出。
MAPPING: dict[str, dict[str, tuple[str, ...]]] = {
    'income': {
        'revenue': ('BIZTOTINCO', 'BIZINCO'),
        'operating_cost': ('BIZCOST',),
        'selling_expense': ('SALESEXPE',),
        'admin_expense': ('MANAEXPE', 'BIZADMINEXPEN'),
        'finance_expense': ('FINEXPE',),
        'operate_profit': ('PERPROFIT', 'OPERPROFIT'),
        'total_profit': ('TOTPROFIT',),
        # 真合并净利润。2026-11-11 裁定：2010–2026 的存量归母值一并改写，
        # 使该列名副其实且无断点；缺失/为 0 时回退归母并逐行标注（见 net_profit_flag）。
        'net_profit': ('NETPROFIT',),
        'parent_net_profit': ('PARENETP', 'NETPARECOMPPROF', 'NETPARESHARPROF'),
    },
    'balance': {
        'monetary_funds': ('CURFDS', 'CASHANDDEPOCENB'),
        'accounts_receivable': ('ACCORECE',),
        'inventory': ('INVE',),
        'total_assets': ('TOTASSET',),
        'accounts_payable': ('ACCOPAYA',),
        'advance_receipts': ('ADVAPAYM',),
        'total_liab': ('TOTLIAB',),
        'total_equity': ('RIGHAGGR', 'TOTSHAREQUI', 'TOTALOWNEREQUI'),
    },
    'cashflow': {
        'net_cash_flow': ('CASHNETR', 'CASHEQUINETINCR'),
        'operating_cf': ('MANANETR',),
        'investing_cf': ('INVNETCASHFLOW',),
        'financing_cf': ('FINNETCFLOW',),
    },
}

# 派生列：(运算, 左列, 右列, 倍数)；yoy 类单独处理
DERIVED_PRODUCT: dict[str, dict[str, tuple[str, str, str, float | None]]] = {
    'income': {'gross_profit': ('sub', 'revenue', 'operating_cost', None)},
    'balance': {'debt_ratio': ('div', 'total_liab', 'total_assets', 100.0)},
    'cashflow': {},
}
DERIVED_YOY = {
    'income': {'revenue_yoy': 'revenue', 'net_profit_yoy': 'net_profit'},
    'balance': {'total_assets_yoy': 'total_assets', 'total_liab_yoy': 'total_liab'},
    'cashflow': {'net_cash_flow_yoy': 'net_cash_flow'},
}

# 行业样本（工业 / 银行 / 保险 / 证券），仅用于 --verify 与标定复现
VERIFY_SAMPLES = {'600601.SH': '工业', '600036.SH': '银行',
                  '601318.SH': '保险', '600030.SH': '证券'}


class BlockedError(RuntimeError):
    """源端出现限速/封禁/连续异常，必须停压。"""


def paper_code(ts_code: str) -> str:
    """``600601.SH`` → ``sh600601``。"""
    num, _, ex = ts_code.partition('.')
    return f'{ex.lower()}{num}'


def load_universe(statement: str) -> list[str]:
    """按报表返回采集标的池。

    - 利润表：全池。2010–2026 存量 ``net_profit`` 要全列改写，任何一只漏采都会
      让该列停留在归母口径。
    - 资产负债表 / 现金流量表：只采「2010 年前上市」的代码，因为本表任务只补
      ``end_date < 2010-01-01`` 的行。实测抽样（603881 / 300750 / 688981）三个
      2010 年后上市标的新浪最早报告期分别为 2011/2014/2012，即结构上不存在
      1990–2009 报告期；按 ``stock_basic.list_date`` 过滤不会丢任何一行。
      另外并入「本地财报表有、但不在 stock_basic 里」的 11~12 只代码。
    """
    basic = PROJECT_ROOT / 'data' / 'local_store' / 'meta' / 'stock_basic.parquet'
    frame = pl.read_parquet(basic, columns=['ts_code', 'list_date'])
    codes = set(frame['ts_code'].to_list())
    local: set[str] = set()
    for each in MAPPING:
        path = store.table_path(each)
        if path.exists():
            local |= set(pl.read_parquet(path, columns=['ts_code'])['ts_code'].to_list())
    if statement == 'income':
        universe = codes | local
    else:
        early = set(frame.filter(pl.col('list_date') < HISTORY_CUTOFF)['ts_code'].to_list())
        universe = (codes & early) | (local - codes)
    if not universe:
        raise RuntimeError('标的池为空，停止采集（不静默降级）')
    return sorted(universe)


class SinaClient:
    """单连接串行 + 节流的取数客户端，遇限速立即停压。"""

    def __init__(self, interval: float = 0.5, jitter: float = 0.15,
                 timeout: float = 40.0) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                           'AppleWebKit/537.36 (KHTML, like Gecko) '
                           'Chrome/122.0 Safari/537.36'),
            'Referer': 'https://vip.stock.finance.sina.com.cn/',
        })
        self.interval = interval
        self.jitter = jitter
        self.timeout = timeout
        self.requests_made = 0
        self.consecutive_failures = 0
        self.consecutive_empty = 0

    def fetch(self, paper: str, source: str) -> dict:
        params = {'paperCode': paper, 'source': source, 'type': '0',
                  'page': '1', 'num': '1000'}
        # 抖动只为打散请求节奏，不用于任何安全用途
        time.sleep(self.interval + random.uniform(0, self.jitter))  # noqa: S311
        self.requests_made += 1
        try:
            resp = self.session.get(SINA_URL, params=params, timeout=self.timeout)
        except requests.RequestException as exc:
            self.consecutive_failures += 1
            if self.consecutive_failures >= 8:
                raise BlockedError(f'连续 {self.consecutive_failures} 次请求异常: {exc}') from exc
            raise
        if resp.status_code in (403, 429):
            raise BlockedError(f'HTTP {resp.status_code} 限速/封禁，已停压')
        if resp.status_code != 200:
            self.consecutive_failures += 1
            raise RuntimeError(f'HTTP {resp.status_code}')
        self.consecutive_failures = 0
        payload = resp.json()
        status_code = (payload.get('result') or {}).get('status', {}).get('code')
        if status_code not in (0, '0', None):
            raise RuntimeError(f'sina status={status_code}: {str(payload)[:200]}')
        data = (payload.get('result') or {}).get('data')
        if data is None:
            # 实测：不存在的代码返回 data=null 且 status.code=0，属「源端无此代码」，
            # 与「链路被限流」不同；连续大量出现才判为异常。
            self.consecutive_empty += 1
            if self.consecutive_empty >= 30:
                raise BlockedError(
                    f'连续 {self.consecutive_empty} 个代码返回 data=null，疑似链路异常，已停压')
            return {}
        self.consecutive_empty = 0
        return data


def _to_float(raw: object) -> float | None:
    if raw in (None, '', '--', '—'):
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _add_derived(frame: pl.DataFrame, statement: str) -> pl.DataFrame:
    """补派生的毛利润 / 资产负债率 / 各项同比（口径与存量逐值对齐）。"""
    for column, (op, left, right, scale) in DERIVED_PRODUCT[statement].items():
        expr = (pl.col(left) - pl.col(right) if op == 'sub'
                else pl.col(left) / pl.col(right))
        if scale is not None:
            expr = expr * scale
        frame = frame.with_columns(expr.alias(column))
    # 同比：按 (月日) 对齐上一年同报告期；分母为 0/空时留空，不做兜底猜测
    for column, base in DERIVED_YOY[statement].items():
        frame = frame.with_columns(
            pl.col('end_date').dt.strftime('%m-%d').alias('_md'),
            pl.col('end_date').dt.year().alias('_yr'))
        prev = frame.select('ts_code', '_md', (pl.col('_yr') + 1).alias('_yr'),
                            pl.col(base).alias('_prev'))
        frame = frame.join(prev, on=['ts_code', '_md', '_yr'], how='left')
        frame = frame.with_columns(
            pl.when(pl.col('_prev').is_not_null() & (pl.col('_prev') != 0))
            .then((pl.col(base) / pl.col('_prev') - 1.0) * 100.0)
            .otherwise(None).alias(column))
        frame = frame.drop('_md', '_yr', '_prev')
    return frame


def normalize(data: dict, statement: str, ts_code: str,
              fetched_at: datetime) -> tuple[pl.DataFrame, int]:
    """把单票单表 payload 归一化为事实表 schema 的帧（含派生列，未按 cutoff 过滤）。

    返回 ``(帧, 该票原始报告期数)``。
    """
    reports = data.get('report_list') or {}
    if not reports:
        return pl.DataFrame(), 0
    records: list[dict] = []
    for date_value, body in reports.items():
        if len(date_value) != 8 or not date_value.isdigit():
            continue
        values: dict[str, float | None] = {}
        for item in body.get('data') or []:
            field = item.get('item_field') or ''
            if not field:
                continue          # 大类标题行：无 item_field、无值
            value = _to_float(item.get('item_value'))
            if value is not None and field not in values:
                values[field] = value   # 同名字段重复时取首个非空（如「国内票证结算」）
        row: dict[str, object] = {
            'ts_code': ts_code,
            'end_date': datetime.strptime(date_value, '%Y%m%d').date(),
            'ann_date': (datetime.strptime(body['publish_date'], '%Y%m%d').date()
                         if body.get('publish_date') else None),
            'ann_date_source': ANN_SOURCE if body.get('publish_date') else None,
            'source': SOURCE_TAG,
            'fetched_at': fetched_at,
        }
        for column, chain in MAPPING[statement].items():
            row[column] = next((values[f] for f in chain if values.get(f) is not None), None)
        if statement == 'income':
            # DG-31 显式兜底：合并净利润缺失或为 0 时才回退归母，且逐行打标，
            # 绝不用 coalesce 让未命中行静默变 null。
            raw = row['net_profit']
            if raw is not None and raw != 0:
                row['_np_flag'] = 'consolidated'
            elif row['parent_net_profit'] is not None:
                row['net_profit'] = row['parent_net_profit']
                row['_np_flag'] = 'parent_fallback'
            else:
                row['net_profit'] = None
                row['_np_flag'] = 'missing'
        records.append(row)
    if not records:
        return pl.DataFrame(), 0
    # 必须扫全量行推断类型：老股票 124 期里，前 100 期某科目全为整数、后面出现小数时，
    # 默认 infer_schema_length=100 会让整只股票报「could not append f64」而失败。
    frame = pl.DataFrame(records, infer_schema_length=None)
    frame = _add_derived(frame, statement)
    frame = frame.with_columns(pl.col('end_date').cast(pl.Date),
                               pl.col('ann_date').cast(pl.Date))
    aligned = store.coerce_schema(frame, statement)
    if '_np_flag' in frame.columns:
        # 归母回退标记留在 staging 分片里（正式 schema 无此列，落正式表前丢弃）
        aligned = aligned.with_columns(frame['_np_flag'].alias('net_profit_flag'))
    return aligned, len(reports)


def _ensure_header(path: Path, header: list[str]) -> None:
    if not path.exists():
        with path.open('w', encoding='utf-8', newline='') as fh:
            csv.writer(fh).writerow(header)


def load_progress() -> dict[tuple[str, str], str]:
    if not PROGRESS.exists():
        return {}
    done: dict[tuple[str, str], str] = {}
    with PROGRESS.open('r', encoding='utf-8', newline='') as fh:
        for row in csv.DictReader(fh):
            done[(row['statement'], row['ts_code'])] = row['status']
    return done


def reconcile_progress(done: dict[tuple[str, str], str]) -> dict[tuple[str, str], str]:
    """撤销「progress 已标 ok、但分片里查不到该代码」的条目。

    进度是逐票追加的，分片却是每 ``BATCH_SIZE`` 只才落盘一次；进程在两次落盘之间
    被杀时，内存缓冲里那几十只票会同时出现「进度 ok / 数据没有」。若不撤销，
    断点续跑会跳过它们，产生**静默缺行**（DG-7 / DG-31 同类风险）。
    这里以分片实际内容为准，幂等，可每次启动都跑。
    """
    for statement in MAPPING:
        files = sorted((PARTS / statement).glob('*.parquet'))
        if files:
            persisted = set(
                pl.scan_parquet([str(f) for f in files])
                .select('ts_code').unique().collect()['ts_code'].to_list())
        else:
            persisted = set()
        stale = [k for k, v in done.items()
                 if k[0] == statement and v == 'ok' and k[1] not in persisted]
        for key in stale:
            done[key] = 'pending'
        if stale:
            print(f'[{statement}] 撤销 {len(stale)} 条「标 ok 但分片无数据」的进度，将重采',
                  flush=True)
    return done


def append_progress(rows: list[dict]) -> None:
    header = ['statement', 'ts_code', 'status', 'periods', 'pre2010_rows',
              'elapsed_s', 'error']
    _ensure_header(PROGRESS, header)
    with PROGRESS.open('a', encoding='utf-8', newline='') as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        writer.writerows(rows)


def append_failed(rows: list[dict]) -> None:
    header = ['statement', 'ts_code', 'reason', 'http_status', 'attempts', 'at']
    _ensure_header(FAILED, header)
    with FAILED.open('a', encoding='utf-8', newline='') as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        writer.writerows(rows)


def collect(limit: int | None, interval: float) -> int:
    PARTS.mkdir(parents=True, exist_ok=True)
    for statement in MAPPING:
        (PARTS / statement).mkdir(parents=True, exist_ok=True)
    done = reconcile_progress(load_progress())
    client = SinaClient(interval=interval)
    started = time.time()
    total_pre2010 = 0
    try:
        for statement, source in STATEMENT_SOURCE.items():
            universe = load_universe(statement)
            if limit:
                universe = universe[:limit]
            todo = [c for c in universe if done.get((statement, c)) != 'ok']
            print(f'[{statement}] 标的池 {len(universe)} 只，待采 {len(todo)} 只（已完成 '
                  f'{len(universe) - len(todo)} 只）', flush=True)
            buffer: list[pl.DataFrame] = []
            for idx, ts_code in enumerate(todo, 1):
                t0 = time.time()
                try:
                    data = client.fetch(paper_code(ts_code), source)
                    frame, periods = normalize(data, statement, ts_code,
                                               datetime.now())
                    # 分片保留全史：pre2010 用于补行，2010+ 用于 net_profit 全列修正，
                    # 两者同一请求内，不额外消耗请求额度。
                    if frame.height:
                        buffer.append(frame)
                    pre2010_rows = (frame.filter(pl.col('end_date') < HISTORY_CUTOFF).height
                                    if frame.height else 0)
                    total_pre2010 += pre2010_rows
                    append_progress([{
                        'statement': statement, 'ts_code': ts_code,
                        'status': 'ok' if periods else 'empty',
                        'periods': periods, 'pre2010_rows': pre2010_rows,
                        'elapsed_s': round(time.time() - t0, 2), 'error': '',
                    }])
                except BlockedError:
                    raise
                except Exception as exc:  # noqa: BLE001 - 逐票容错，明细进 failed.csv
                    append_failed([{
                        'statement': statement, 'ts_code': ts_code,
                        'reason': type(exc).__name__, 'http_status': '',
                        'attempts': 1, 'at': datetime.now().isoformat(timespec='seconds'),
                    }])
                    append_progress([{
                        'statement': statement, 'ts_code': ts_code,
                        'status': 'failed', 'periods': 0, 'pre2010_rows': 0,
                        'elapsed_s': round(time.time() - t0, 2), 'error': str(exc)[:200],
                    }])
                if idx % BATCH_SIZE == 0 or idx == len(todo):
                    if buffer:
                        # 分片号取「已存在分片数」，保证断点续跑不会覆盖既有分片；
                        # 原子写避免被杀时留下半截 parquet 拖垮后续 scan。
                        part_dir = PARTS / statement
                        batch_no = len(list(part_dir.glob('*.parquet')))
                        out = part_dir / f'{batch_no:04d}.parquet'
                        write_parquet_atomic(
                            pl.concat(buffer, how='diagonal_relaxed'), out,
                            compression='zstd')
                        buffer.clear()
                    spent = time.time() - started
                    rate = client.requests_made / spent if spent else 0
                    print(f'  {idx}/{len(todo)} 累计请求 {client.requests_made} '
                          f'({rate:.2f}/s) 已落 pre2010 行 {total_pre2010:,} '
                          f'用时 {spent / 60:.1f} 分钟', flush=True)
    except BlockedError as exc:
        BLOCKED.write_text(json.dumps({
            'at': datetime.now().isoformat(timespec='seconds'),
            'reason': str(exc), 'requests_made': client.requests_made,
            'hint': '已停压，未做任何重试加压；排查后删除本文件再从断点续跑',
        }, ensure_ascii=False, indent=1), encoding='utf-8')
        print(f'\n[BLOCKED] {exc}', flush=True)
        return 2
    print(f'\n采集结束：请求 {client.requests_made} 次，pre2010 行 {total_pre2010:,}，'
          f'耗时 {(time.time() - started) / 60:.1f} 分钟')
    return 0


def build() -> int:
    """合并分片，产出两份候选（均落 staging，不写 ``data/``）。

    1. ``{statement}_pre2010.parquet``：1990–2009 待补行（三表）；
    2. ``net_profit_full.parquet``：利润表全史 (代码, 报告期, 合并净利润,
       归母净利润, 来源标记)，供 2010–2026 存量 ``net_profit`` 全列修正。
    """
    for statement in MAPPING:
        files = sorted((PARTS / statement).glob('*.parquet'))
        if not files:
            print(f'[{statement}] 无分片，跳过')
            continue
        lazy = pl.scan_parquet([str(f) for f in files])
        in_window = lazy.filter(pl.col('end_date') < HISTORY_CUTOFF)
        frame = in_window.filter(pl.col('end_date') >= WINDOW_FLOOR[statement]).collect()
        dropped = in_window.filter(pl.col('end_date') < WINDOW_FLOOR[statement]).collect()
        if dropped.height:
            print(f'[{statement}] 窗口外剔除 {dropped.height} 行'
                  f'（{dropped["end_date"].min()} ~ {dropped["end_date"].max()}，'
                  f'{dropped["ts_code"].n_unique()} 只，按声明窗口不落盘）')
        frame = store.coerce_schema(frame, statement)
        key = ['ts_code', 'end_date', 'ann_date']
        frame = frame.unique(subset=key, keep='last').sort(['ts_code', 'end_date'])
        out = STAGING / f'{statement}_pre2010.parquet'
        frame.write_parquet(out, compression='zstd')
        print(f'[{statement}] pre2010 {frame.height:,} 行 → {out}')
        if statement != 'income':
            continue
        full = (lazy.select('ts_code', 'end_date', 'net_profit',
                            'parent_net_profit', 'net_profit_flag')
                .collect()
                .filter(pl.col('end_date') >= HISTORY_CUTOFF)
                .unique(subset=['ts_code', 'end_date'], keep='last')
                .sort(['ts_code', 'end_date']))
        nf = STAGING / 'net_profit_full.parquet'
        full.write_parquet(nf, compression='zstd')
        counts = full.group_by('net_profit_flag').len().sort('net_profit_flag')
        print(f'[income] 2010+ 净利润映射 {full.height:,} 行 → {nf}')
        for row in counts.iter_rows(named=True):
            print(f'   {row["net_profit_flag"]}: {row["len"]:,}')
    return 0


def verify(interval: float) -> int:
    """用 4 个行业样本复现映射标定结果（对拍对象为本地存量 2010+ 数据）。

    优先读 ``_staging/.../raw/`` 下的探测缓存，避免重复消耗请求额度。
    """
    import gzip

    client = SinaClient(interval=interval)
    raw_dir = STAGING / 'raw'

    def load(paper: str, source: str) -> dict:
        cache = raw_dir / f'{paper}_{source}.json.gz'
        if cache.exists():
            with gzip.open(cache, 'rt', encoding='utf-8') as fh:
                return (json.load(fh).get('result') or {}).get('data') or {}
        return client.fetch(paper, source)

    lines = ['# 新浪三表映射标定证据', '',
             f'生成时间：{datetime.now():%Y-%m-%d %H:%M:%S}',
             '对拍对象：本地 `meta/financials/*.parquet`（东方财富来源）2010+ 存量', '']
    for statement, source in STATEMENT_SOURCE.items():
        frames = []
        for ts_code in VERIFY_SAMPLES:
            data = load(paper_code(ts_code), source)
            frame, _ = normalize(data, statement, ts_code, datetime.now())
            if frame.height:
                frames.append(frame)
        if not frames:
            continue
        cand = pl.concat(frames, how='diagonal_relaxed')
        local = store.read_table(statement)
        joined = cand.join(local.select('ts_code', 'end_date', pl.exclude(
            ['ts_code', 'end_date', 'ann_date', 'ann_date_source', 'source',
             'fetched_at'])), on=['ts_code', 'end_date'], how='inner',
            suffix='_local')
        lines.append(f'\n## {statement}（{STATEMENT_CN[statement]}）'
                     f'重叠 {joined.height} 个 (代码, 报告期)\n')
        lines.append('| 本地列 | 新浪字段链 | 覆盖(样本非空) | 重叠可比 | 逐值相等 | 相等率 |')
        lines.append('|---|---|---|---|---|---|')
        for column, chain in MAPPING[statement].items():
            got = joined[column]
            truth = joined[f'{column}_local']
            both = got.is_not_null() & truth.is_not_null()
            n = int(both.sum())
            exact = int((both & ((got - truth).abs() <= 1.0)).sum())
            ratio = f'{exact / n * 100:.2f}%' if n else '-'
            lines.append(f'| `{column}` | `{"` → `".join(chain)}` | '
                         f'{int(got.is_not_null().sum())} | {n} | {exact} | {ratio} |')
        for column in [*DERIVED_PRODUCT[statement], *DERIVED_YOY[statement]]:
            got = joined[column]
            truth = joined[f'{column}_local']
            both = got.is_not_null() & truth.is_not_null()
            n = int(both.sum())
            exact = int((both & ((got - truth).abs() <= 1.0)).sum())
            ratio = f'{exact / n * 100:.2f}%' if n else '-'
            lines.append(f'| `{column}` | 派生（见模块文档） | '
                         f'{int(got.is_not_null().sum())} | {n} | {exact} | {ratio} |')
    text = '\n'.join(lines)
    out = STAGING / 'mapping_evidence.md'
    out.write_text(text, encoding='utf-8')
    print(text)
    print(f'\n实际请求 {client.requests_made} 次 → {out}')
    return 0


def _sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open('rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _backup(table: str, src: Path) -> Path:
    """替换前把正式文件 copy2 到仓库根 ``_backup/financials_<表>_<时间戳>/``。

    ``_backup/`` 只做回滚快照，不进入任何取数、缓存、回测、展示链路（DG-12），
    因此目录名带日期与表名，便于与既有快照区分。
    """
    import shutil

    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    target = PROJECT_ROOT / '_backup' / f'financials_{table}_{stamp}'
    if DRY_RUN:
        return target
    target.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, target / src.name)
    return target


def _assert_no_new_nulls(before: pl.Series, after: pl.Series, label: str) -> dict:
    """断言「没有任何一行的非空值被改写成 null」，并返回空值计数。

    该断言必须能对「空值从 0 变 N」失败：末行用合成序列自测，见
    ``--selftest``。
    """
    became_null = int((before.is_not_null() & after.is_null()).sum())
    if became_null:
        raise AssertionError(
            f'{label}: {became_null} 行原本非空却被写成 null（空值数 '
            f'{int(before.is_null().sum())} → {int(after.is_null().sum())}）')
    return {'before_null': int(before.is_null().sum()),
            'after_null': int(after.is_null().sum())}


def _assert_rows_unchanged(base: pl.DataFrame, target: pl.DataFrame,
                           key: list[str], label: str,
                           skip: tuple[str, ...] = ()) -> None:
    """逐列断言 ``base`` 的每一行在 ``target`` 里原样保留（只增行、不改存量）。

    数值列按 0.5 元容差比对，其余列按值相等比对；任何「非空 → null」都报错。
    """
    joined = base.join(target, on=key, how='inner', suffix='_new')
    if joined.height != base.height:
        raise AssertionError(f'{label}: 按键对齐后行数 {joined.height} != {base.height}'
                             '（键重复或存量行丢失）')
    for column in base.columns:
        if column in key or column in skip:
            continue
        truth, got = joined[column], joined[f'{column}_new']
        if truth.is_not_null().sum() and got.dtype.is_numeric():
            both = truth.is_not_null() & got.is_not_null()
            if int((both & ((truth - got).abs() > 0.5)).sum()):
                raise AssertionError(f'{label}.{column}: 存量数值被改动')
        else:
            both = truth.is_not_null() & got.is_not_null()
            if int((both & (truth != got)).sum()):
                raise AssertionError(f'{label}.{column}: 存量值被改动')
        if int((truth.is_not_null() & got.is_null()).sum()):
            raise AssertionError(f'{label}.{column}: 存量值被写成 null')


def apply_netprofit() -> int:
    """把 2010–2026 存量 ``net_profit`` 全列修正为真合并口径。

    改造来源是新浪 ``NETPROFIT``（与 1990–2009 新补行同一口径），因此改完
    该列全区间 1990–2026 无断点。三方证据：新浪合并 / 新浪归母 / 改前东财值。
    """
    from src.data.atomic_write import write_parquet_atomic

    src = store.table_path('income')
    old = pl.read_parquet(src)
    old_sha = _sha256(src)
    mapping = pl.read_parquet(STAGING / 'net_profit_full.parquet').rename({
        'net_profit': 'np_sina', 'parent_net_profit': 'parent_sina',
        'net_profit_flag': 'flag'})
    backup = _backup('income', src)

    merged = old.join(mapping, on=['ts_code', 'end_date'], how='left')
    # 显式兜底：未命中任何分支一律写回原值，禁止 coalesce 让未命中行变 null
    corrected = (merged
                 .with_columns(pl.col('flag').fill_null('no_sina_row').alias('_flag'))
                 .with_columns(
        pl.when((pl.col('_flag') == 'consolidated') & pl.col('np_sina').is_not_null())
        .then(pl.col('np_sina'))
        .when((pl.col('_flag') == 'parent_fallback') & pl.col('np_sina').is_not_null())
        .then(pl.col('np_sina'))
        .otherwise(pl.col('net_profit')).alias('net_profit_new'))
                 .with_columns(
        pl.when(pl.col('_flag') == 'parent_fallback')
        .then(pl.col('source') + '|net_profit=parent_fallback')
        .when((pl.col('_flag') == 'missing') | (pl.col('_flag') == 'no_sina_row'))
        .then(pl.col('source') + '|net_profit=keep_local')
        .otherwise(pl.col('source')).alias('source_new')))

    new = corrected.with_columns(pl.col('net_profit_new').alias('net_profit'),
                                 pl.col('source_new').alias('source')).drop(
        'np_sina', 'parent_sina', 'flag', '_flag', 'net_profit_new', 'source_new')

    # ---- DG-20 / DG-31 断言 ----
    if new.height != old.height:
        raise AssertionError(f'行数不守恒：{old.height:,} → {new.height:,}')
    _assert_rows_unchanged(old, new, ['ts_code', 'end_date'], 'income',
                           skip=('net_profit', 'source'))
    null_stat = _assert_no_new_nulls(old['net_profit'], new['net_profit'], 'net_profit')

    diff = (new['net_profit'] - old['net_profit'])
    changed = int((diff.abs() > 0.5).sum())
    filled = int((old['net_profit'].is_null() & new['net_profit'].is_not_null()).sum())
    flags = corrected.group_by('_flag').len().sort('_flag')

    if DRY_RUN:
        print('[DRY-RUN] 未写入正式文件')
    else:
        write_parquet_atomic(new.sort(['ts_code', 'end_date']), src,
                             compression='zstd', compression_level=3)
    new_sha = _sha256(src)

    lines = [
        '# net_profit 全列修正证据（2010–2026 存量 → 真合并口径）', '',
        *(['> **DRY-RUN：未写入正式文件，前后 SHA256 相同属预期**', '']
          if DRY_RUN else []),
        f'生成时间：{datetime.now():%Y-%m-%d %H:%M:%S}',
        f'正式路径：`{src.relative_to(PROJECT_ROOT)}`',
        '候选来源：`_staging/financials_history_20261111/net_profit_full.parquet`'
        '（新浪 `NETPROFIT`，同一批请求内获得，0 额外请求）', '',
        '| 项 | 数值 |', '|---|---|',
        f'| 行数（改前 → 改后） | {old.height:,} → {new.height:,}（Δ{new.height - old.height}） |',
        f'| `net_profit` 改动单元格数 | {changed:,} |',
        f'| 其中「空 → 有值」 | {filled:,} |',
        f'| 空值数（改前 → 改后） | {null_stat["before_null"]:,} → {null_stat["after_null"]:,} |',
        '| 非空被写成 null | 0 |',
        '| 其它列改动（`source` 之外） | 0（逐列逐值对拍，容差 0.5 元） |',
        f'| SHA256（改前） | `{old_sha}` |',
        f'| SHA256（改后） | `{new_sha}` |',
        f'| 回滚备份 | `{backup.relative_to(PROJECT_ROOT)}/income.parquet` |', '',
        '## 分支命中', '',
        '| 分支 | 行数 | 处置 |', '|---|---|---|',
    ]
    meaning = {'consolidated': '写入新浪合并净利润', 'parent_fallback':
               '新浪合并缺失/为 0 → 保留归母值，`source` 追加 '
               '`|net_profit=parent_fallback`', 'missing':
               '新浪两列皆空 → 保留原值，`source` 追加 `|net_profit=keep_local`',
               'no_sina_row': '新浪无该报告期 → 保留原值，`source` 追加 '
               '`|net_profit=keep_local`'}
    for row in flags.iter_rows(named=True):
        lines.append(f'| `{row["_flag"]}` | {row["len"]:,} | {meaning.get(row["_flag"], "")} |')
    lines += ['', '## 三方样本对拍', '',
              '| 代码 | 报告期 | 东财原值（归母） | 新浪 `NETPROFIT`（合并） | '
              '新浪归母 | 修正后 | 差（合并−归母） |', '|---|---|---|---|---|---|---|']
    sample = merged.filter(
        pl.col('ts_code').is_in(['601318.SH', '600601.SH', '000002.SZ', '600036.SH'])
        & pl.col('end_date').is_in([date(2010, 6, 30), date(2010, 12, 31)])
    ).sort(['ts_code', 'end_date'])
    for row in sample.iter_rows(named=True):
        lines.append(
            f'| {row["ts_code"]} | {row["end_date"]} | {row["net_profit"]:,.0f} | '
            f'{row["np_sina"] if row["np_sina"] is not None else "—"} | '
            f'{row["parent_sina"] if row["parent_sina"] is not None else "—"} | '
            f'{(row["np_sina"] if row["np_sina"] is not None else row["net_profit"]):,.0f} | '
            f'{row["np_sina"] - row["parent_sina"] if row["np_sina"] is not None else 0:,.0f} |')
    text = '\n'.join(lines) + '\n'
    (STAGING / 'net_profit_evidence.md').write_text(text, encoding='utf-8')
    if int((corrected['_flag'] == 'parent_fallback').sum()) or int(
            (corrected['_flag'] == 'missing').sum()):
        fb = corrected.filter(pl.col('_flag').is_in(['parent_fallback', 'missing']))
        append_failed([{'statement': 'income', 'ts_code': rec['ts_code'],
                        'reason': f'net_profit_{rec["_flag"]}@{rec["end_date"]}',
                        'http_status': '', 'attempts': 0,
                        'at': datetime.now().isoformat(timespec='seconds')}
                       for rec in fb.iter_rows(named=True)])
    print(text)
    return 0


def apply_pre2010(only: str | None = None,
                  end_before: date | None = None) -> int:
    """把 1990–2009 候选行追加进三张正式表（只增行，不改任何存量单元格）。

    ``only`` / ``end_before`` 用于把一次运行收窄到指定报表 / 报告期，
    便于在已落盘后只补某一小段（如现金流量表 1996–1997 的 9 行）；
    此时证据文件另存 ``pre2010_evidence_{only}.md``，不覆盖全量证据。
    """
    from src.data.atomic_write import write_parquet_atomic

    scope = STATEMENT_CN.get(only, '') if only else ''
    if end_before is not None:
        scope = f'{scope}<{end_before}'
    lines = ['# 1990–2009 三表补行证据', '',
             *([f'> **本次范围收窄：{scope}**', ''] if scope else []),
             f'生成时间：{datetime.now():%Y-%m-%d %H:%M:%S}', '']
    for statement in ([only] if only else list(MAPPING)):
        cand_path = STAGING / f'{statement}_pre2010.parquet'
        if not cand_path.exists():
            print(f'[{statement}] 无候选，跳过')
            continue
        src = store.table_path(statement)
        old = pl.read_parquet(src)
        old_sha = _sha256(src)
        cand = pl.read_parquet(cand_path)
        if end_before is not None:
            cand = cand.filter(pl.col('end_date') < end_before)
        cand = cand.select(old.columns)
        if cand.height == 0:
            raise AssertionError(f'{statement}: 收窄后候选 0 行，拒绝空跑')
        backup = _backup(statement, src)

        overlap = old.select('ts_code', 'end_date').join(
            cand.select('ts_code', 'end_date'), on=['ts_code', 'end_date'],
            how='inner')
        if overlap.height:
            raise AssertionError(
                f'{statement}: 候选与存量重叠 {overlap.height} 行，拒绝直接追加')
        key = ['ts_code', 'end_date']
        new = pl.concat([old, cand], how='vertical_relaxed').sort(key)
        if new.height != old.height + cand.height:
            raise AssertionError(f'{statement}: 行数不守恒')

        # 存量逐值零改动
        _assert_rows_unchanged(old, new, key, statement)

        out_path = src
        if DRY_RUN:
            print(f'[DRY-RUN] {statement} 未写入正式文件')
        else:
            write_parquet_atomic(new, out_path, compression='zstd',
                                 compression_level=3)
        new_sha = _sha256(src)
        lines += [f'## {statement}', '', '| 项 | 数值 |', '|---|---|',
                  f'| 行数（改前 → 改后） | {old.height:,} → {new.height:,}（+{cand.height:,}） |',
                  f'| 新增最小/最大报告期 | {cand["end_date"].min()} / {cand["end_date"].max()} |',
                  f'| 涉及代码数 | {cand["ts_code"].n_unique():,} |',
                  '| 存量改动单元格 | 0（逐列逐值，容差 0.5 元） |',
                  '| 存量非空被写成 null | 0 |',
                  f'| SHA256（改前） | `{old_sha}` |',
                  f'| SHA256（改后） | `{new_sha}` |',
                  f'| 回滚备份 | `{backup.relative_to(PROJECT_ROOT)}/{src.name}` |', '']
        # 空值计数前后对照，能对「空值从 0 变 N」失败
        for column in cand.columns:
            if column in key:
                continue
            before = int(old[column].is_null().sum())
            after = int(new[column].is_null().sum())
            added = int(cand[column].is_null().sum())
            if after != before + added:
                raise AssertionError(f'{statement}.{column}: 空值数 {before}+{added} != {after}')
        lines += ['', f'空值计数已逐列校验：改后 = 改前 + 新增行空值（{statement}）。', '']
    text = '\n'.join(lines) + '\n'
    evidence = (STAGING / f'pre2010_evidence_{only}.md' if only
                else STAGING / 'pre2010_evidence.md')
    evidence.write_text(text, encoding='utf-8')
    print(text)
    return 0


def selftest() -> int:
    """自测断言本身：给一段「非空被写成 null」的序列，断言必须报错。"""
    before = pl.Series('x', [1.0, 2.0, None, 4.0])
    after = pl.Series('x', [1.0, None, None, 4.0])
    try:
        _assert_no_new_nulls(before, after, 'selftest')
    except AssertionError as exc:
        print(f'PASS 断言能捕获「非空→null」：{exc}')
        return 0
    print('FAIL 断言未能捕获「非空→null」')
    return 1


def coverage() -> int:
    """输出「标的 × 年份 × 报表」覆盖表，证明 52 个红格由红转有值。

    改前口径直接取项目自带的可用性矩阵缓存（``tmp/audit_coverage_summary_2026-11-10
    .json``）里的 ``actual``/``denom``，分母 = 该年上市标的数（剔除占位桩），
    与 2026-11-10 矩阵完全同源；改后由当前正式表重算，两者可比。
    """
    summary_path = PROJECT_ROOT / 'tmp' / 'audit_coverage_summary_2026-11-10.json'
    old = json.loads(summary_path.read_text(encoding='utf-8'))
    lines = ['# 1990–2009 三大报表覆盖变化（标的 × 年份 × 报表）', '',
             f'生成时间：{datetime.now():%Y-%m-%d %H:%M:%S}',
             '口径：分子 = 该年有报告的标的数；分母 = 该年上市标的数（剔除占位桩）；'
             '与 `数据可用性矩阵_2026-11-10.md` 完全同源。', '']
    filled_cells = 0
    target_cells = {
        'balance': range(1990, 2010),
        'income': range(1990, 2010),
        # 现金流 1990–1997 不在 52 格目标内：1996–1997 的 9 行属追溯披露，
        # 已于 2026-11-11 补入正式表（原「1998 年前结构性不存在」判定有误）；
        # 1990–1995 实测源端无任何行。
        'cashflow': range(1998, 2010),
    }
    for statement in ('balance', 'income', 'cashflow'):
        meta = old['metrics'][statement]
        denom = meta['denom']
        before_codes = meta['actual']
        before_rows = meta['rows']
        frame = pl.read_parquet(store.table_path(statement)).with_columns(
            pl.col('end_date').dt.year().alias('y'))
        # 改后 = 正式表现状 ∪ staging 候选（落盘前也必须能自证红转绿；
        # 落盘后再跑一次应与本表逐格相同）
        cand_path = STAGING / f'{statement}_pre2010.parquet'
        union = frame.select('ts_code', 'end_date')
        if cand_path.exists():
            union = pl.concat([
                union,
                pl.read_parquet(cand_path, columns=['ts_code', 'end_date']),
            ]).unique()
        agg = (union.with_columns(pl.col('end_date').dt.year().alias('y'))
               .group_by('y').agg(pl.col('ts_code').len().alias('codes'),
                                  pl.len().alias('rows')))
        after = dict(zip(agg['y'].to_list(), agg['codes'].to_list(), strict=True))
        after_rows = dict(zip(agg['y'].to_list(), agg['rows'].to_list(), strict=True))
        lines += [f'## {meta["label"]}', '',
                  '| 年份 | 分母 | 改前有值 | 改后标的数 | 改前行数 | 改后行数 | 结果 |',
                  '|---|---|---|---|---|---|---|']
        for year in range(1990, 2011):
            key = str(year)
            d = denom.get(key, 0)
            b = before_codes.get(key, 0)
            a = after.get(year, 0)
            if year in target_cells[statement]:
                if b == 0 and a > 0:
                    filled_cells += 1
                    verdict = '✅ 红转有值'
                elif a > 0:
                    verdict = '已有'
                else:
                    verdict = '❌ 仍为空'
            elif year == 2010:
                verdict = '边界年（存量起点）'
            elif a > 0:
                verdict = '不在 52 格目标内（1996–1997 追溯披露，已补）'
            else:
                verdict = '不在 52 格目标内（源端无行）'
            lines.append(f'| {year} | {d} | {b} | {a} | {before_rows.get(key, 0)} | '
                         f'{after_rows.get(year, 0)} | {verdict} |')
        lines.append('')
    lines += [f'**目标 52 格已由红转有值：{filled_cells} / 52**', '']
    text = '\n'.join(lines)
    (STAGING / 'coverage_table.md').write_text(text, encoding='utf-8')
    print(text)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description='新浪三表 2010 年前历史补全')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--verify', action='store_true', help='复现映射标定证据')
    group.add_argument('--collect', action='store_true', help='全量采集（断点续跑）')
    group.add_argument('--build', action='store_true', help='合并分片为候选表')
    group.add_argument('--apply-netprofit', action='store_true',
                       help='把 2010–2026 存量 net_profit 修正为合并口径')
    group.add_argument('--apply-pre2010', action='store_true',
                       help='把 1990–2009 候选行追加进三张正式表')
    group.add_argument('--selftest', action='store_true', help='自测空值断言')
    group.add_argument('--coverage', action='store_true',
                       help='输出标的 × 年份 × 报表 覆盖变化表')
    parser.add_argument('--statement', choices=sorted(MAPPING), default=None,
                        help='与 --apply-pre2010 合用，把范围收窄到单张报表')
    parser.add_argument('--end-before', type=date.fromisoformat, default=None,
                        help='与 --apply-pre2010 合用，只落 end_date 早于该日期的候选行')
    parser.add_argument('--limit', type=int, default=None, help='只采前 N 只（冒烟用）')
    parser.add_argument('--dry-run', action='store_true',
                        help='跑完断言与证据但不写盘、不备份')
    parser.add_argument('--interval', type=float, default=0.5,
                        help='单请求最小间隔秒数（默认 0.5，约 2.5 小时跑完 17,742 次）')
    args = parser.parse_args()
    global DRY_RUN
    DRY_RUN = args.dry_run
    if args.verify:
        return verify(args.interval)
    if args.collect:
        return collect(args.limit, args.interval)
    if args.build:
        return build()
    if args.apply_netprofit:
        return apply_netprofit()
    if args.apply_pre2010:
        return apply_pre2010(args.statement, args.end_before)
    if args.coverage:
        return coverage()
    return selftest()


if __name__ == '__main__':
    sys.exit(main())
