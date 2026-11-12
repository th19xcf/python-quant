#!/usr/bin/env python3
"""manifest 通用核对 / 重算工具。

用法::

    python tools/manifest_tool.py --check                  # 只读核对全库
    python tools/manifest_tool.py --check --json out.json  # 附带机读明细
    python tools/manifest_tool.py --check --only factors   # 只看指定子树

设计口径（与项目数据治理规则一致）：

* **遍历范围**：``data/local_store/**`` 下所有 ``manifest.json`` 与
  ``*.manifest.json``，两者都算。
* **路径解析**：manifest 内登记的路径一律相对 ``data/local_store``；同时兼容
  历史写法（带 ``data/local_store/`` 前缀、Windows 绝对路径、``files`` 字典
  以文件名为键时的同目录相对路径）。
* **逐文件核对**：``rows``（parquet 元数据）、``bytes``（``os.stat``）、
  ``sha256``（1 MiB 分块）、``columns`` / ``cols``、``date_range``（仅在登记时）。
* **汇总恒等**：顶层 ``files`` / ``rows`` / ``bytes`` 与逐文件求和恒等；
  ``universe.<kind>`` 与其 ``markets_detail`` 三项各自恒等。
* **只报不猜**：语义项（``dataset`` / ``source`` / ``authority`` /
  ``semantics``）缺失只报「未登记」，不用目录名或文件名反推填充；找不到
  数据文件的 manifest 归入「无法定位」而不是猜路径。
* **人工字段白名单**：``known_issue`` / ``check_result_*`` / ``note`` /
  ``notes`` / ``authority`` / ``registered_at`` / ``registered_reason`` /
  ``local_adjustments`` / ``known_gaps`` / ``caveats`` 及任何未知字段一律
  原样透传，本工具不删除、不改写任何登记内容。
* **只读**：``--check`` 全程不写任何文件（``--json`` 指定的输出文件除外）。
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

STORE_PARTS = ('data', 'local_store')
STORE_PREFIX = 'data/local_store'
DATA_EXTS = ('.parquet', '.csv', '.json', '.txt', '.xls', '.xlsx')
SEMANTIC_KEYS = ('dataset', 'source', 'authority', 'semantics')
DATE_COLUMN_CANDIDATES = (
    'trade_date', 'date', 'end_date', 'ex_date', 'start_date',
    'effective_date', 'in_date', 'out_date', 'coverage_start',
    'record_date', 'period', 'pay_date', 'publish_date',
)
# 历史/溯源/回滚类子树：里面的路径是「当时」的，不是当前正式数据，不参与核对。
SKIP_SUBTREES = frozenset({
    'upstream', 'provenance', 'previous_manifest', 'previous',
    'row_count_history', 'method', 'counting', 'sources', 'notes',
    'known_gaps', 'caveats', 'local_adjustments', 'fields', 'column_basis',
    'schema', 'field_sources', 'rebuild', 'reproduce', 'migration',
    'amount_fix', 'cross_check_treasury', 'cross_check_10y', 'crosscheck',
    'catchup_20261112', 'rebuild_20261112', 'backfill_2026_11_11',
    'tcode_decisions', 'label_fix_20261111', 'replacement_new_factors_20261112',
    'refresh_analysis_monthly_20261111', 'staging', 'detail',
    'factor_table_sync', 'archived_inputs', 'numeric_definitions',
    'scripts', 'audit', 'backup', 'backup_dir', 'backup_path', 'rollback',
})
STAT_KEYS = ('rows', 'bytes', 'sha256', 'columns', 'cols', 'date_range')


# --------------------------------------------------------------------------- #
# 基础工具
# --------------------------------------------------------------------------- #
def store_root() -> Path:
    return Path(__file__).resolve().parents[1].joinpath(*STORE_PARTS)


def sha256_of(path: Path, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for block in iter(lambda: handle.read(chunk), b''):
            digest.update(block)
    return digest.hexdigest()


def norm_date(value: Any) -> str:
    """把 ``20261111`` / ``2026-11-11`` / ``2026-11-11T00:00:00`` 归一成 ISO 日。"""
    text = str(value).strip()
    if len(text) >= 8 and text[:8].isdigit():
        return f'{text[:4]}-{text[4:6]}-{text[6:8]}'
    return text[:10]


def looks_data_file(name: str) -> bool:
    return name.lower().endswith(DATA_EXTS)


def is_placeholder(text: str) -> bool:
    """``{name}`` / ``<YYYYMMDD>`` / ``*.parquet`` 这类通配模板不是真实路径。

    ``<...>`` 与 ``{...}`` 一样是「日期快照族」的路径模板写法（如
    ``meta/st_status/st_status_<YYYYMMDD>.parquet``），磁盘上不存在同名文件，
    按真实路径核对只会得到假 ``file_missing``。
    """
    return any(token in text for token in ('{', '}', '*', '<', '>'))


def human(n: Any) -> str:
    if isinstance(n, int):
        return f'{n:,}'
    return str(n)


# --------------------------------------------------------------------------- #
# 非数据集产物（manifest 自声明 artifact_kind=non_dataset）与三条守门
# --------------------------------------------------------------------------- #
# 判定口径（硬约束，不靠目录名 / 字段缺失去猜）：
#   ① manifest 必须自己写 ``"artifact_kind": "non_dataset"``；
#   ② 三条守门逐条实测，全过才算「非数据集产物」，任一条不过即仍旧计入语义未登记：
#      ① 无数据消费读取 —— src/ 与 tools/ 的 .py 里没有该路径/该目录的「消费型」字面量；
#         备份/镜像工具为登记或搬运路径而出现的字面量不算消费（见 MIRROR_TOOL_PATHS）；
#      ② 不在链路上      —— src/ 与 config/ 的 py/json/yaml/toml 里没有该字面量；
#      ③ 权威清单登记    —— docs/数据权威路径清单.md 的「非数据资产」小节内，有按路径精确
#         匹配的登记行（全文出现关键词不算）；目录级 manifest 比目录，单文件比去后缀路径；
#      ④ 已声明用途      —— 类别必填项：manifest 的 non_dataset.purpose 非空。
# 说明：① 用严格字面量匹配，不做「像不像读点」的猜测；只把「备份/镜像类工具的路径
# 登记」显式排除，且必须在输出里列出被排除的命中行，宁可错杀不可放水。
NON_DATASET_KIND = 'non_dataset'
NON_DATASET_DOC = Path('docs') / '数据权威路径清单.md'
NON_DATASET_DOC_TAG = '非数据资产'
CODE_DIRS = ('src', 'tools')
CODE_EXTS = ('.py',)
CHAIN_DIRS = ('src', 'config')
CHAIN_EXTS = ('.py', '.json', '.yaml', '.yml', '.toml')
#: 备份 / 镜像 / 跨机同步工具：命中这些文件的字面量只是「路径登记」，不是数据消费。
MIRROR_TOOL_PATHS = frozenset({
    'tools/backup_critical_assets.py',
    'tools/sync_daily_non_git.py',
})

# 已知误伤（记录在案，按裁决不细化到「只认真读行」）：
# ``meta/code_migration_manifest.json`` 的 ① 判 FAIL，命中行是 ``code_migration.py:27`` 的
# 模块文档字符串（列举产出物）与 ``:51`` 的 ``MANIFEST_REL`` 路径常量，二者都不是「读该
# manifest 的内容」；该文件真实的数据读取是 ``:115-126 load_mapping`` 读
# ``meta/code_migration.parquet``。所以它不是「无读取方的操作产物」，而是「真数据集的配套
# manifest」，处理方式是补语义而不是剔除。保留误伤的理由：误差方向是「少剔除」，不会让
# 未登记数虚假变小。


class TextCache:
    """同一次核对内按路径缓存文件正文，避免反复读同一批源码。"""

    def __init__(self) -> None:
        self._items: dict[str, str] = {}

    def text(self, path: Path) -> str:
        key = str(path).lower()
        if key not in self._items:
            try:
                self._items[key] = path.read_text(encoding='utf-8', errors='ignore')
            except OSError:
                self._items[key] = ''
        return self._items[key]


_TEXT_CACHE = TextCache()


@dataclass
class NonDatasetCheck:
    """一份 manifest 的非数据集声明与三条守门实测结果。"""

    declared: bool = False
    purpose: str | None = None
    gate_pass: dict[str, bool] = field(default_factory=dict)
    gate_evidence: dict[str, str] = field(default_factory=dict)

    @property
    def qualified(self) -> bool:
        return self.declared and bool(self.gate_pass) and all(self.gate_pass.values())

    @property
    def failed_gates(self) -> list[str]:
        return [k for k, ok in self.gate_pass.items() if not ok]


def _iter_text_files(root: Path, dirs: Iterable[str], exts: Iterable[str]) -> Iterable[Path]:
    for name in dirs:
        base = root / name
        if not base.is_dir():
            continue
        for path in sorted(base.rglob('*')):
            if path.is_file() and path.suffix.lower() in tuple(exts):
                yield path


def _scan_tokens(root: Path, dirs: Iterable[str], exts: Iterable[str],
                 tokens: list[str], cache: TextCache) -> list[str]:
    hits: list[str] = []
    for path in _iter_text_files(root, dirs, exts):
        text = cache.text(path)
        for lineno, line in enumerate(text.splitlines(), 1):
            if any(tok and tok in line for tok in tokens):
                shown = path.relative_to(root).as_posix()
                hits.append(f'{shown}:{lineno}')
    return hits


def _non_dataset_rows(doc_text: str) -> list[tuple[int, str]]:
    """取出「非数据资产」小节内的表格行 (行号, 原文)；小节外的行一律不算。"""
    rows: list[tuple[int, str]] = []
    in_section = False
    for lineno, line in enumerate(doc_text.splitlines(), 1):
        stripped = line.strip()
        if stripped.startswith('#'):
            in_section = NON_DATASET_DOC_TAG in stripped
            continue
        if in_section and stripped.startswith('|'):
            rows.append((lineno, stripped))
    return rows


def _registered_path_token(manifest_rel: str) -> str:
    """登记行里应当出现的路径：目录级 manifest 取目录，单文件 manifest 取去掉 .[manifest.]json 的路径。"""
    path = Path(manifest_rel)
    if path.name == 'manifest.json':
        return path.parent.as_posix()
    name = path.name
    for suffix in ('.manifest.json', '.json'):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    parent = path.parent.as_posix()
    return name if parent == '.' else f'{parent}/{name}'


def verify_non_dataset(root: Path, manifest_rel: str, data: dict[str, Any],
                       cache: TextCache) -> NonDatasetCheck:
    """按 manifest 自己的声明核三条守门；未声明则原样返回（不猜）。"""
    check = NonDatasetCheck(declared=str(data.get('artifact_kind', '')).strip() == NON_DATASET_KIND)
    if not check.declared:
        return check
    declared = data.get('non_dataset')
    declared = declared if isinstance(declared, dict) else {}
    check.purpose = str(declared.get('purpose', '')).strip() or None

    parent = Path(manifest_rel).parent.as_posix()
    name = Path(manifest_rel).name
    tokens = [manifest_rel]
    if name == 'manifest.json' and parent.count('/') >= 1:
        tokens.append(parent)   # 目录级 manifest 按目录判定，避免只查一个文件名
    else:
        tokens.append(name)     # 单文件 manifest 再按文件名判一次，兼容 Path('meta')/'x.json' 拼法

    # 扫描根必须是仓库根：src/ 与 tools/ 都在仓库根下，不在 data/local_store 里。
    repo = root.parents[1] if len(root.parents) > 1 else root
    hits = _scan_tokens(repo, CODE_DIRS, CODE_EXTS, tokens, cache)
    mirror = [h for h in hits if h.split(':')[0] in MIRROR_TOOL_PATHS]
    consume = [h for h in hits if h.split(':')[0] not in MIRROR_TOOL_PATHS]
    check.gate_pass['① 无数据消费读取'] = not consume
    detail = '零引用' if not consume else f'{len(consume)} 处：' + '；'.join(consume[:5])
    if mirror:
        detail += f'（另 {len(mirror)} 处为备份/镜像登记，不计入：' + '；'.join(mirror[:5]) + '）'
    check.gate_evidence['① 无数据消费读取'] = detail

    chain = _scan_tokens(repo, CHAIN_DIRS, CHAIN_EXTS, tokens, cache)
    check.gate_pass['② 不在取数/回测/展示链路'] = not chain
    check.gate_evidence['② 不在取数/回测/展示链路'] = ('零引用' if not chain
                                                      else f'{len(chain)} 处：' + '；'.join(chain[:5]))

    doc = repo / NON_DATASET_DOC
    token = _registered_path_token(manifest_rel)
    needles = (f'`data/local_store/{token}/`', f'`data/local_store/{token}`')
    line_no = None
    rows = _non_dataset_rows(cache.text(doc)) if doc.is_file() else []
    for no, row in rows:
        if any(needle in row for needle in needles):
            line_no = no
            break
    check.gate_pass['③ 权威清单登记为非数据资产'] = line_no is not None
    check.gate_evidence['③ 权威清单登记为非数据资产'] = (
        f'{NON_DATASET_DOC.as_posix()}:L{line_no}（「{NON_DATASET_DOC_TAG}」节内按路径精确匹配）'
        if line_no is not None
        else f'「{NON_DATASET_DOC_TAG}」节内无 `data/local_store/{token}` 的登记行（全文关键词命中不算）')

    # 类别必填项：非数据集产物必须在 manifest 里写清用途，否则无法判断「它是什么操作产物」。
    check.gate_pass['④ 已声明用途（non_dataset 必填）'] = bool(check.purpose)
    check.gate_evidence['④ 已声明用途（non_dataset 必填）'] = (
        check.purpose or 'non_dataset.purpose 缺失，按类别必填项判不合格')
    return check


# --------------------------------------------------------------------------- #
# 文件事实（按路径缓存，避免同一次核对重复读同一文件）
# --------------------------------------------------------------------------- #
@dataclass
class FileFacts:
    path: Path
    exists: bool
    is_file: bool
    bytes: int | None = None
    rows: int | None = None
    sha256: str | None = None
    columns: list[str] | None = None

    def sha(self) -> str | None:
        if self.sha256 is None and self.exists and self.is_file:
            self.sha256 = sha256_of(self.path)
        return self.sha256


class FactsCache:
    def __init__(self) -> None:
        self._items: dict[str, FileFacts] = {}

    def get(self, path: Path) -> FileFacts:
        key = str(path).lower()
        cached = self._items.get(key)
        if cached is not None:
            return cached
        exists = path.exists()
        is_file = path.is_file() if exists else False
        facts = FileFacts(path=path, exists=exists, is_file=is_file)
        if is_file:
            facts.bytes = path.stat().st_size
            if path.suffix.lower() == '.parquet':
                facts.rows, facts.columns = _parquet_meta(path)
        self._items[key] = facts
        return facts


def _parquet_meta(path: Path) -> tuple[int | None, list[str] | None]:
    try:
        import pyarrow.parquet as pq
    except Exception:  # noqa: BLE001 - 缺少 pyarrow 时降级到 polars，不中断核对
        try:
            import polars as pl

            frame = pl.read_parquet(path)
            return frame.height, list(frame.columns)
        except Exception:  # noqa: BLE001 - 单文件读不动只让该条核对降级
            return None, None
    try:
        meta = pq.ParquetFile(str(path))
        return meta.metadata.num_rows, list(meta.schema_arrow.names)
    except Exception:  # noqa: BLE001 - 损坏或不支持的 parquet 只让该条核对降级
        return None, None


def column_min_max(path: Path, column: str) -> tuple[str, str] | None:
    try:
        import polars as pl

        series = pl.read_parquet(path, columns=[column])[column]
        low, high = series.min(), series.max()
        if low is None or high is None:
            return None
        return norm_date(low), norm_date(high)
    except Exception:  # noqa: BLE001 - 取不到 min/max 时由调用方报「无可核对」
        return None


# --------------------------------------------------------------------------- #
# 核对结果
# --------------------------------------------------------------------------- #
@dataclass
class Issue:
    kind: str          # bytes / rows / sha256 / columns / cols / date_range /
                       # file_missing / unresolved_locator / summary / parse_error
    target: str        # 文件相对路径或汇总块名
    detail: str


@dataclass
class Record:
    label: str         # manifest 内的容器位置，如 files.stock_adj_factor.parquet
    raw_path: str | None
    resolved: Path | None
    fields: dict[str, Any]


@dataclass
class ManifestResult:
    rel: str
    data: dict[str, Any]
    records: list[Record] = field(default_factory=list)
    issues: list[Issue] = field(default_factory=list)
    missing_semantics: list[str] = field(default_factory=list)
    ok_records: int = 0
    non_dataset: NonDatasetCheck | None = None

    @property
    def bad(self) -> int:
        return sum(1 for i in self.issues if i.kind not in ('unresolved_locator', 'rows_unverifiable'))

    @property
    def unresolved(self) -> int:
        return sum(1 for i in self.issues if i.kind == 'unresolved_locator')


# --------------------------------------------------------------------------- #
# 路径解析
# --------------------------------------------------------------------------- #
def resolve(raw: str, root: Path) -> Path:
    text = raw.strip().replace('\\', '/')
    path = Path(text)
    if path.is_absolute() or (len(text) > 1 and text[1] == ':'):
        return Path(text.replace('/', os.sep))
    if text.startswith(STORE_PREFIX + '/'):
        return root.joinpath(*text[len(STORE_PREFIX) + 1:].split('/'))
    return root.joinpath(*[p for p in text.split('/') if p])


# --------------------------------------------------------------------------- #
# record 收集
# --------------------------------------------------------------------------- #
def _container_items(container: Any) -> list[tuple[str, dict[str, Any], str | None]]:
    """把一个 files / tables 容器展开成 (label, 字段字典, 显式路径)。"""
    out: list[tuple[str, dict[str, Any], str | None]] = []
    if isinstance(container, list):
        for idx, item in enumerate(container):
            if isinstance(item, dict):
                raw = item.get('path') or item.get('source_parquet')
                out.append((f'[{idx}]', item, raw))
    elif isinstance(container, dict):
        for key, item in container.items():
            if isinstance(item, dict):
                raw = item.get('path') or item.get('source_parquet')
                out.append((key, item, raw))
            elif isinstance(item, str) and item.lower().endswith('.parquet') \
                    and not is_placeholder(item):
                out.append((key, {}, item))
    return out


def collect_records(data: dict[str, Any], manifest_dir: Path, root: Path) -> list[Record]:  # noqa: C901
    """收集 manifest 中所有「声明了数据文件」的节点。

    三条识别规则，全部是结构性的、可复核的：

    1. 节点自带 ``path`` / ``source_parquet``；
    2. ``files`` 字典以数据文件名为键（同 ``dir`` 或同 manifest 所在目录）；
    3. 顶层 ``dir`` 声明时，只写文件名的 ``path`` 挂到该目录下。
    """
    declared_dir = data.get('dir')
    base = resolve(declared_dir, root) if isinstance(declared_dir, str) else manifest_dir
    records: list[Record] = []

    def bare_name_path(raw: str) -> Path | None:
        """``dir`` 已声明时，把「只有文件名」的 path 挂到该目录下。"""
        text = raw.strip().replace('\\', '/')
        if '/' in text or (len(text) > 1 and text[1] == ':') or not isinstance(declared_dir, str):
            return None
        return base / text

    def visit(node: Any, prefix: str, depth: int = 0) -> None:
        if depth > 6:
            return
        if isinstance(node, dict):
            for key, value in node.items():
                if key in SKIP_SUBTREES:
                    continue
                label = f'{prefix}.{key}' if prefix else key
                if isinstance(value, dict):
                    explicit = value.get('path') or value.get('source_parquet')
                    if isinstance(explicit, str) and looks_data_file(explicit):
                        records.append(Record(label, explicit,
                                              bare_name_path(explicit) or resolve(explicit, root),
                                              value))
                        continue
                    if looks_data_file(key) and any(k in value for k in STAT_KEYS):
                        records.append(Record(label, None, base / key, value))
                        continue
                    visit(value, label, depth + 1)
                elif isinstance(value, list) and value and isinstance(value[0], dict):
                    for idx, item in enumerate(value):
                        if not isinstance(item, dict):
                            continue
                        explicit = item.get('path') or item.get('source_parquet')
                        if isinstance(explicit, str) and looks_data_file(explicit):
                            records.append(Record(f'{label}[{idx}]', explicit,
                                                  bare_name_path(explicit) or resolve(explicit, root),
                                                  item))
                elif isinstance(value, str) and value.lower().endswith('.parquet') \
                        and not is_placeholder(value) and ('/' in value or '\\' in value):
                    text = value.replace('\\', '/')
                    if '_staging/' in text or '_backup/' in text:
                        continue  # 中间态 / 回滚快照，不是正式数据
                    records.append(Record(label, value, resolve(value, root), {}))

    visit(data, '')
    return records


def self_describing_record(data: dict[str, Any], root: Path) -> Record | None:
    """自述型 manifest：顶层统计字段描述的是同一个文件。"""
    raw: str | None = None
    for key in ('path', 'source_parquet'):
        value = data.get(key)
        if isinstance(value, str) and looks_data_file(value):
            raw = value
            break
    if raw is None:
        files = data.get('files')
        if isinstance(files, dict):
            value = files.get('parquet')
            if isinstance(value, str) and value.lower().endswith('.parquet'):
                raw = value
    if raw is None:
        return None
    if not any(k in data for k in ('rows', 'bytes', 'sha256', 'source_parquet_sha256')):
        return None
    fields = {
        'rows': data.get('rows'),
        'bytes': data.get('bytes', data.get('source_parquet_bytes')),
        'sha256': data.get('sha256', data.get('source_parquet_sha256')),
        'columns': data.get('columns'),
        'cols': data.get('cols'),
        'date_range': data.get('date_range'),
        'date_column': data.get('date_column'),
    }
    fields = {k: v for k, v in fields.items() if v is not None}
    if not fields:
        return None
    return Record('top', raw, resolve(raw, root), fields)


# --------------------------------------------------------------------------- #
# 单项核对
# --------------------------------------------------------------------------- #
def check_record(record: Record, root: Path, cache: FactsCache) -> list[Issue]:
    path = record.resolved
    if path is None:
        return [Issue('unresolved_locator', record.label, '登记了统计值但没有可定位的路径')]
    facts = cache.get(path)
    shown = path.relative_to(root).as_posix() if _is_under(path, root) else str(path)
    issues: list[Issue] = []
    if not facts.exists:
        issues.append(Issue('file_missing', shown, f'登记路径不存在（{shown}）'))
        return issues
    if not facts.is_file:
        return issues

    want_bytes = record.fields.get('bytes')
    if isinstance(want_bytes, int) and facts.bytes != want_bytes:
        issues.append(Issue('bytes', shown, f'登记 {human(want_bytes)} vs 磁盘 {human(facts.bytes)}'))

    want_rows = record.fields.get('rows')
    if isinstance(want_rows, int):
        if facts.rows is None:
            issues.append(Issue('rows_unverifiable', shown, '登记了 rows，但该文件不是 parquet，无法核对'))
        elif facts.rows != want_rows:
            issues.append(Issue('rows', shown, f'登记 {human(want_rows)} vs 磁盘 {human(facts.rows)}'))

    want_sha = record.fields.get('sha256')
    if isinstance(want_sha, str) and len(want_sha) == 64:
        actual = facts.sha()
        if actual != want_sha:
            issues.append(Issue('sha256', shown, f'登记 {want_sha[:12]}… vs 磁盘 {str(actual)[:12]}…'))

    want_cols = record.fields.get('columns')
    if isinstance(want_cols, list) and facts.columns is not None:
        if list(want_cols) != facts.columns:
            missing = [c for c in want_cols if c not in facts.columns]
            extra = [c for c in facts.columns if c not in want_cols]
            same_set = not missing and not extra
            note = '列集合一致但顺序不同' if same_set else f'缺 {missing[:5]} 多 {extra[:5]}'
            issues.append(Issue('columns', shown, note))

    want_cols_n = record.fields.get('cols')
    if isinstance(want_cols_n, int) and facts.columns is not None \
            and want_cols_n != len(facts.columns):
        issues.append(Issue('cols', shown, f'登记 {want_cols_n} vs 磁盘 {len(facts.columns)}'))

    want_range = record.fields.get('date_range')
    if isinstance(want_range, list) and len(want_range) == 2:
        issues.extend(_check_date_range(record, path, facts, shown, want_range))
    return issues


def _check_date_range(record: Record, path: Path, facts: FileFacts,
                      shown: str, want: list[Any]) -> list[Issue]:
    want_pair = [norm_date(want[0]), norm_date(want[1])]
    column = record.fields.get('date_column')
    if isinstance(column, str) and facts.columns and column in facts.columns:
        actual = column_min_max(path, column)
        if actual is None:
            return [Issue('date_range', shown, '登记了 date_range，但该列取不到 min/max')]
        if list(actual) != want_pair:
            return [Issue('date_range', shown,
                          f'登记 {want_pair} vs 磁盘 {list(actual)}（列 {column}）')]
        return []
    candidates = [c for c in DATE_COLUMN_CANDIDATES if facts.columns and c in facts.columns]
    actuals = {c: column_min_max(path, c) for c in candidates}
    hits = [c for c, pair in actuals.items() if pair and list(pair) == want_pair]
    if hits:
        return []
    if not actuals:
        return [Issue('date_range', shown, '登记了 date_range，但无可核对的日期列')]
    detail = '；'.join(f'{c}={list(p) if p else None}' for c, p in actuals.items())
    return [Issue('date_range', shown,
                  f'登记 {want_pair} vs 候选列实测均不匹配（未登记 date_column）：{detail}')]


def _is_under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


# --------------------------------------------------------------------------- #
# 汇总恒等
# --------------------------------------------------------------------------- #
def check_summaries(data: dict[str, Any], root: Path, cache: FactsCache) -> list[Issue]:
    issues: list[Issue] = []
    issues.extend(_rule_dir_totals(data, root, cache))
    issues.extend(_rule_universe(data))
    issues.extend(_rule_totals_block(data))
    issues.extend(_rule_total_rows_bytes(data))
    return issues


def _rule_dir_totals(data: dict[str, Any], root: Path, cache: FactsCache) -> list[Issue]:
    """规则 1：顶层 files / rows / bytes 是整数的目录级汇总（quotes）。"""
    issues: list[Issue] = []
    if not (isinstance(data.get('files'), int) and isinstance(data.get('path'), str)):
        return issues
    target = resolve(data['path'], root)
    if not target.is_dir():
        return issues
    total_files, total_rows, total_bytes = _walk_parquet(target, cache)
    actual = {'files': total_files, 'rows': total_rows, 'bytes': total_bytes}
    for index, value in actual.items():
        if isinstance(data.get(index), int) and data[index] != value:
            issues.append(Issue('summary', index,
                                f'登记 {human(data[index])} vs 磁盘 {human(value)}'))
    return issues


def _rule_universe(data: dict[str, Any]) -> list[Issue]:
    """规则 2：universe.<kind> 与 markets_detail 三项恒等，并回加总到顶层。"""
    issues: list[Issue] = []
    universe = data.get('universe')
    if not isinstance(universe, dict):
        return issues
    if not any(isinstance(b, dict) and isinstance(b.get('markets_detail'), dict)
               for b in universe.values()):
        return issues
    sums = {'files': 0, 'rows': 0, 'bytes': 0}
    for kind, block in universe.items():
        if not isinstance(block, dict) or not isinstance(block.get('markets_detail'), dict):
            continue
        for index in ('files', 'rows', 'bytes'):
            part = sum(int(m.get(index, 0)) for m in block['markets_detail'].values()
                       if isinstance(m, dict))
            parent = block.get(index)
            if isinstance(parent, int) and parent != part:
                issues.append(Issue('summary', f'universe.{kind}.{index}',
                                    f'登记 {human(parent)} vs markets_detail 求和 {human(part)}'))
            sums[index] += int(parent) if isinstance(parent, int) else part
    for index in ('files', 'rows', 'bytes'):
        top = data.get(index)
        if isinstance(top, int) and top != sums[index]:
            issues.append(Issue('summary', f'universe→{index}',
                                f'顶层 {human(top)} vs universe 分项求和 {human(sums[index])}'))
    return issues


def _rule_totals_block(data: dict[str, Any]) -> list[Issue]:
    """规则 3：factors.totals（文件数、行数、字节数只统计 parquet 条目）。"""
    issues: list[Issue] = []
    totals = data.get('totals')
    container = data.get('files')
    if not (isinstance(totals, dict) and isinstance(container, dict)):
        return issues
    parquet_entries = [(k, v) for k, v in container.items()
                       if isinstance(v, dict) and (v.get('kind') == 'parquet'
                                                   or str(k).lower().endswith('.parquet'))]
    parquet_keys = {k for k, _ in parquet_entries}
    other_count = sum(1 for k, v in container.items()
                      if isinstance(v, dict) and k not in parquet_keys)
    if 'parquet_files' in totals and totals['parquet_files'] != len(parquet_entries):
        issues.append(Issue('summary', 'totals.parquet_files',
                            f'登记 {totals["parquet_files"]} vs 条目数 {len(parquet_entries)}'))
    if 'other_files' in totals and totals['other_files'] != other_count:
        issues.append(Issue('summary', 'totals.other_files',
                            f'登记 {totals["other_files"]} vs 条目数 {other_count}'))
    for index in ('rows', 'bytes'):
        if not isinstance(totals.get(index), int):
            continue
        part = sum(int(v.get(index, 0)) for _, v in parquet_entries
                   if isinstance(v.get(index), int))
        if part != totals[index]:
            issues.append(Issue('summary', f'totals.{index}',
                                f'登记 {human(totals[index])} vs parquet 条目求和 {human(part)}'))
    return issues


def _rule_total_rows_bytes(data: dict[str, Any]) -> list[Issue]:
    """规则 4：顶层 total_rows / total_bytes 与本 manifest 的 files 容器求和恒等。"""
    issues: list[Issue] = []
    for index, stat in (('total_rows', 'rows'), ('total_bytes', 'bytes')):
        if not isinstance(data.get(index), int):
            continue
        parts = [fields.get(stat) for _, fields, _raw in _container_items(data.get('files'))
                 if isinstance(fields.get(stat), int)]
        if parts and sum(parts) != data[index]:
            issues.append(Issue('summary', index,
                                f'登记 {human(data[index])} vs files 条目求和 {human(sum(parts))}'))
    return issues


def _walk_parquet(directory: Path, cache: FactsCache) -> tuple[int, int, int]:
    files = rows = size = 0
    for path in sorted(directory.rglob('*.parquet')):
        facts = cache.get(path)
        if not facts.is_file:
            continue
        files += 1
        size += facts.bytes or 0
        rows += facts.rows or 0
    return files, rows, size


# --------------------------------------------------------------------------- #
# 覆盖率与语义项
# --------------------------------------------------------------------------- #
def covered_paths(results: Iterable[ManifestResult], root: Path) -> set[str]:
    covered: set[str] = set()
    for result in results:
        for record in result.records:
            if record.resolved is not None:
                covered.add(str(record.resolved.resolve()).lower())
    return covered


def coverage_gaps(results: list[ManifestResult], root: Path) -> dict[str, dict[str, list[str]]]:
    """未被任何 record 定位的 parquet，按「目录是否本身有 manifest」分两类。

    ``no_manifest``：该目录下没有任何 manifest 文件（完全没登记）。
    ``not_located``：目录里有 manifest，但它没有指向这个文件（含只写了统计值
    却没写可定位路径的 manifest）。

    ``quotes/`` 不享受整段豁免：早期口径假设「quotes 由一份汇总 manifest 覆盖」，
    但 ``quotes/manifest.json`` 只有目录级汇总、没有逐文件 record，整段跳过会让
    「无侧车 manifest 的分片」永远显示 0，单分片被静默改写时无人发现（G-106）。
    现在与其它子树一视同仁，逐目录判定。
    """
    covered = covered_paths(results, root)
    manifest_dirs = {path.parent.resolve() for path in discover(root)}
    gaps: dict[str, dict[str, list[str]]] = {'no_manifest': {}, 'not_located': {}}
    for path in sorted(root.rglob('*.parquet')):
        rel = path.relative_to(root).as_posix()
        if str(path.resolve()).lower() in covered:
            continue
        bucket = 'not_located' if path.parent.resolve() in manifest_dirs else 'no_manifest'
        gaps[bucket].setdefault(path.parent.relative_to(root).as_posix(), []).append(rel)
    return gaps


# --------------------------------------------------------------------------- #
# 主流程
# --------------------------------------------------------------------------- #
def discover(root: Path) -> list[Path]:
    found = []
    for path in root.rglob('*.json'):
        name = path.name
        if name.endswith('manifest.json'):
            found.append(path)
    return sorted(found, key=lambda p: p.relative_to(root).as_posix())


def check_manifest(path: Path, root: Path, cache: FactsCache) -> ManifestResult:
    rel = path.relative_to(root).as_posix()
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError, UnicodeDecodeError) as exc:
        result = ManifestResult(rel=rel, data={})
        result.issues.append(Issue('parse_error', rel, f'JSON 解析失败：{exc}'))
        return result
    if not isinstance(data, dict):
        result = ManifestResult(rel=rel, data={})
        result.issues.append(Issue('parse_error', rel, '顶层不是 JSON 对象'))
        return result

    result = ManifestResult(rel=rel, data=data)
    records = collect_records(data, path.parent, root)
    own = self_describing_record(data, root)
    if own is not None:
        records.insert(0, own)

    if not records and any(k in data for k in ('rows', 'bytes', 'sha256')):
        top_path = data.get('path')
        is_dataset_dir = isinstance(top_path, str) and resolve(top_path, root).is_dir()
        if not is_dataset_dir:
            result.issues.append(Issue(
                'unresolved_locator', rel,
                '登记了 rows/bytes/sha256，但没有任何可定位的数据文件 path'))

    for record in records:
        issues = check_record(record, root, cache)
        result.records.append(record)
        if issues:
            result.issues.extend(issues)
        else:
            result.ok_records += 1

    result.issues.extend(check_summaries(data, root, cache))
    result.missing_semantics = [
        k for k in SEMANTIC_KEYS
        if k not in data or str(data[k]).strip() in ('', '未登记', 'TBD')
    ]
    result.non_dataset = verify_non_dataset(root, rel, data, _TEXT_CACHE)
    if result.non_dataset.qualified:
        # 三条守门全过才允许按「非数据集产物」计；否则语义项照旧保留。
        result.missing_semantics = []
    return result


def _print_issues(result: ManifestResult, limit: int) -> None:
    shown = [i for i in result.issues if i.kind != 'rows_unverifiable']
    print(f'  {result.rel}')
    for issue in shown[:limit]:
        print(f'    - {issue.kind:18s} {issue.target}: {issue.detail}')
    if len(shown) > limit:
        print(f'    … 另有 {len(shown) - limit} 条（--json 导出完整明细）')


def print_non_dataset(declared: list[ManifestResult]) -> None:
    """打印自声明为非数据集产物的 manifest 及其三条守门实测结果。"""
    if not declared:
        return
    print('非数据集产物声明（artifact_kind=non_dataset，三条守门逐条实测，未过即仍计入语义未登记）')
    for result in declared:
        nd = result.non_dataset
        if nd is None:
            continue
        print(f'  {result.rel:52s} {"通过" if nd.qualified else "未通过"}')
        for name, ok in nd.gate_pass.items():
            print(f'      {"PASS" if ok else "FAIL"} {name}：{nd.gate_evidence.get(name, "")}')
        if nd.purpose:
            print(f'      用途：{nd.purpose}')
    print('')


def render(results: list[ManifestResult], gaps: dict[str, dict[str, list[str]]],
           max_details: int, json_path: Path | None,
           coverage_checked: bool = True) -> int:
    total_records = sum(len(r.records) for r in results)
    total_issues = sum(len(r.issues) for r in results)
    unresolved = sum(r.unresolved for r in results)
    unverifiable = sum(1 for r in results for i in r.issues if i.kind == 'rows_unverifiable')
    gap_no_manifest = sum(len(v) for v in gaps.get('no_manifest', {}).values())
    gap_not_located = sum(len(v) for v in gaps.get('not_located', {}).values())
    missing_sem = sum(len(r.missing_semantics) for r in results)

    def failing(result: ManifestResult) -> list[Issue]:
        return [i for i in result.issues if i.kind != 'rows_unverifiable']

    print('manifest 核对（只读）')
    print(f'  根目录                  : {store_root()}')
    print(f'  manifest 份数           : {len(results)}')
    print(f'  文件记录                : {human(total_records)}（无法定位 {unresolved}）')
    print(f'  不一致条目              : {human(total_issues - unresolved - unverifiable)}')
    print(f'  无 manifest 的 parquet  : {human(gap_no_manifest)} 个 / '
          f'{len(gaps.get("no_manifest", {}))} 个目录')
    print(f'  有 manifest 但未定位    : {human(gap_not_located)} 个 / '
          f'{len(gaps.get("not_located", {}))} 个目录')
    print('')

    ordered = [r for r in results if failing(r)]
    if ordered:
        print(f'[首份不一致] {ordered[0].rel}')
        _print_issues(ordered[0], max_details)
        print('')

    print('按 manifest 分组')
    header = (f'  {"manifest":52s} {"记录":>5s} {"一致":>5s} {"不一致":>6s} '
              f'{"无法定位":>8s} {"不可核对":>8s} {"语义未登记":>10s}')
    print(header)
    for result in results:
        print(f'  {result.rel:52s} {len(result.records):5d} {result.ok_records:5d} '
              f'{result.bad:6d} {result.unresolved:8d} '
              f'{len(result.issues) - len(failing(result)):8d} '
              f'{len(result.missing_semantics):10d}')
    print('')

    rest = ordered[1:]
    if rest:
        print('其余不一致明细')
        for result in rest:
            _print_issues(result, max_details)
        print('')

    if not coverage_checked:
        print('（已按 --only 过滤：本次未做全库覆盖率统计）')
        print('')
    for bucket, title in (('no_manifest', '完全无 manifest 的目录（只报不改）'),
                          ('not_located', '目录有 manifest 但未定位该文件（只报不改）')):
        block = gaps.get(bucket, {})
        if not block or not coverage_checked:
            continue
        count = sum(len(v) for v in block.values())
        print(f'{title}：{count} 个 parquet / {len(block)} 个目录')
        for directory in sorted(block):
            files = block[directory]
            shown = ', '.join(f.split('/')[-1] for f in files[:6])
            more = '' if len(files) <= 6 else f' … +{len(files) - 6}'
            print(f'  {directory:28s} {len(files):3d}  {shown}{more}')
        print('')

    sem = [r for r in results if r.missing_semantics]
    if sem:
        print('语义项未登记（只报，不用目录名/文件名反推）')
        for result in sem:
            print(f'  {result.rel:52s} 缺 {", ".join(result.missing_semantics)}')
        print('')

    declared = [r for r in results if r.non_dataset is not None and r.non_dataset.declared]
    print_non_dataset(declared)

    bad = total_issues + gap_no_manifest + gap_not_located - unverifiable
    verdict = 'PASS' if bad == 0 else 'FAIL'
    passed_nd = sum(1 for r in declared if r.non_dataset is not None and r.non_dataset.qualified)
    print(f'结论: {verdict}  manifest {len(results)} 份 / 文件记录 {human(total_records)} 条 / '
          f'不一致 {human(total_issues - unresolved - unverifiable)} 条 / '
          f'无法定位 {unresolved} 条 / 无 manifest {human(gap_no_manifest)} 个 / '
          f'未定位 {human(gap_not_located)} 个 / 语义未登记 {human(missing_sem)} 项 / '
          f'非数据集声明 {len(declared)} 份（通过 {passed_nd}）')

    if json_path is not None:
        payload = {
            'root': str(store_root()),
            'manifests': [
                {
                    'manifest': r.rel,
                    'records': len(r.records),
                    'ok_records': r.ok_records,
                    'issues': [i.__dict__ for i in r.issues],
                    'missing_semantics': r.missing_semantics,
                    'non_dataset': (None if r.non_dataset is None or not r.non_dataset.declared else {
                        'declared': True,
                        'qualified': r.non_dataset.qualified,
                        'purpose': r.non_dataset.purpose,
                        'gates': r.non_dataset.gate_pass,
                        'evidence': r.non_dataset.gate_evidence,
                    }),
                }
                for r in results
            ],
            'coverage_gaps': gaps,
            'verdict': verdict,
        }
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                             encoding='utf-8', newline='\n')
        print(f'明细已写入 {json_path}')
    return 0 if verdict == 'PASS' else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='manifest 通用核对 / 重算工具（当前仅实现只读 --check）')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--check', action='store_true', help='只读核对全库（默认）')
    mode.add_argument('--write', action='store_true', help='回写 manifest（尚未接线）')
    parser.add_argument('--only', action='append', default=None,
                        help='只核对指定子树，可重复，如 --only factors --only meta/macro')
    parser.add_argument('--json', dest='json_path', default=None,
                        help='把完整明细写成 JSON（唯一的写文件动作）')
    parser.add_argument('--max-details', type=int, default=8,
                        help='每份 manifest 打印的明细条数上限（默认 8）')
    args = parser.parse_args(argv)

    if args.write:
        print('--write 尚未接线：本工具当前只有 --check。')
        return 2

    root = store_root()
    if not root.is_dir():
        print(f'找不到数据根目录：{root}')
        return 2

    manifests = discover(root)
    if args.only:
        prefixes = tuple(p.strip('/') for p in args.only)
        manifests = [p for p in manifests
                     if p.relative_to(root).as_posix().startswith(prefixes)]
    cache = FactsCache()
    results = [check_manifest(path, root, cache) for path in manifests]
    scoped = bool(args.only)
    gaps = ({'no_manifest': {}, 'not_located': {}} if scoped
            else coverage_gaps(results, root))
    json_path = Path(args.json_path) if args.json_path else None
    return render(results, gaps, args.max_details, json_path, coverage_checked=not scoped)


if __name__ == '__main__':
    with contextlib.suppress(Exception):
        sys.stdout.reconfigure(errors='replace')  # type: ignore[attr-defined]
    raise SystemExit(main())
