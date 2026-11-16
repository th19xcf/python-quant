"""五个分析页的落盘缓存（``logs/analysis_cache``，不写 ``data/``）。

- 缓存键 = ``sha1(页面名 + asof + dv 版本 + 输入指纹)``；``dv`` 取
  ``meta/data_version.json`` 的 ``current.version``。
- 无 TTL：键不匹配即重算并覆盖；``dv`` 缺失时不写缓存，按原逻辑现算。
- 原子写（同目录临时文件 + ``os.replace``），半成品不留在缓存目录。
- 只在本缓存目录内按命名空间保留最近 ``KEEP_VERSIONS`` 个文件。
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import pickle
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

#: 仓库根目录（本文件位于 ``src/quant/analysis/`` 下三层）
ROOT = Path(__file__).resolve().parents[3]
#: 缓存根目录：只落在 ``logs/`` 下，不污染 ``data/``
CACHE_ROOT = ROOT / 'logs' / 'analysis_cache'
#: 缓存结构版本：结构变化时整体作废
CACHE_SCHEMA = 'analysis_cache/v1'
#: 每个命名空间保留的最近版本数
KEEP_VERSIONS = 3

__all__ = [
    'CACHE_ROOT',
    'CACHE_SCHEMA',
    'KEEP_VERSIONS',
    'ROOT',
    'CacheMeta',
    'CacheResult',
    'build_key',
    'cache_dir',
    'cached',
    'data_version',
    'describe',
    'dv_missing_note',
    'file_stamp',
    'panel_end',
    'save_object',
]


@dataclass(frozen=True)
class CacheMeta:
    """一次取数的来源信息：是否命中、数据版本与计算时间。"""

    page: str
    namespace: str
    key: str
    dv: str | None
    asof: str | None
    computed_at: str
    hit: bool
    path: str | None = None


@dataclass(frozen=True)
class CacheResult:
    """缓存取数结果：值 + 来源信息。"""

    value: Any
    meta: CacheMeta


def _timestamp() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def store_root(root: Path | str | None = None) -> Path:
    """本地列存根目录（默认按项目配置解析）。"""
    if root is not None:
        return Path(root)
    from src.data.local_store.store import resolve_store_root
    from src.utils.config import get_config

    return resolve_store_root(get_config())


def data_version(root: Path | str | None = None) -> str | None:
    """读取 ``meta/data_version.json`` 的 ``current.version``；缺失返回 None。"""
    path = store_root(root) / 'meta' / 'data_version.json'
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return None
    current = payload.get('current') if isinstance(payload, dict) else None
    version = current.get('version') if isinstance(current, dict) else None
    return str(version) if version else None


def panel_end(root: Path | str | None = None) -> str:
    """行情末日：``meta/daily_index.parquet`` 的 ``end.max()``；缺失回退指数行情。"""
    base = store_root(root)
    ledger = base / 'meta' / 'daily_index.parquet'
    if ledger.is_file():
        try:
            import polars as pl

            frame = pl.read_parquet(ledger, columns=['end'])
            value = frame['end'].max()
            if value is not None:
                return str(value)[:10]
        except Exception:  # noqa: BLE001, S110 - 面板末日只作指纹，读取失败不阻断计算
            pass
    for code in ('000985.SH', '000300.SH'):
        files = sorted((base / 'quotes' / 'index').glob(f'**/{code}.parquet'))
        if not files:
            continue
        try:
            import polars as pl

            frame = pl.read_parquet(files[0], columns=['date'])
            value = frame['date'].max()
            if value is not None:
                return str(value)[:10]
        except Exception:  # noqa: BLE001, S112
            continue
    return ''


def file_stamp(path: Path) -> str:
    """文件指纹 = ``大小-int(mtime_ns)``；不存在返回空串。"""
    try:
        stat = Path(path).stat()
    except OSError:
        return ''
    return f'{stat.st_size}-{stat.st_mtime_ns}'


def build_key(page: str, asof: str | None = None, dv: str | None = None,
              fingerprint: dict[str, Any] | None = None) -> str:
    """缓存键：页面名 + asof + dv 版本 + 输入指纹的 sha1。"""
    payload = {
        'schema': CACHE_SCHEMA,
        'page': page,
        'asof': asof or '',
        'dv': dv or '',
        'fingerprint': fingerprint or {},
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha1(raw.encode('utf-8')).hexdigest()


def cache_dir(page: str) -> Path:
    return CACHE_ROOT / page


def _object_path(page: str, namespace: str, key: str) -> Path:
    return cache_dir(page) / f'{namespace}-{key}.pkl'


def _inside_cache(path: Path) -> bool:
    """只允许操作缓存目录内的路径（防止误删其它目录）。"""
    try:
        path.resolve().relative_to(CACHE_ROOT.resolve())
    except (ValueError, OSError):
        return False
    return True


def _atomic_write(path: Path, payload: bytes) -> None:
    """同目录临时文件 + ``os.replace``：半成品不留在缓存目录。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix='.tmp-', suffix='.pkl')
    try:
        with os.fdopen(fd, 'wb') as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.remove(tmp)
        raise


def _prune(page: str, namespace: str) -> None:
    """按命名空间保留最近 ``KEEP_VERSIONS`` 个文件；只动缓存目录内文件。"""
    directory = cache_dir(page)
    if not directory.is_dir():
        return
    files = [item for item in directory.glob(f'{namespace}-*.pkl')
             if item.is_file() and _inside_cache(item)]
    files.sort(key=lambda item: item.stat().st_mtime_ns, reverse=True)
    for stale in files[KEEP_VERSIONS:]:
        with contextlib.suppress(OSError):
            stale.unlink()


def load_object(page: str, namespace: str, key: str) -> dict[str, Any] | None:
    """读取缓存对象；文件缺失、损坏或结构不符一律视为未命中。"""
    path = _object_path(page, namespace, key)
    try:
        with path.open('rb') as handle:
            payload = pickle.load(handle)  # noqa: S301 - 本地自建缓存，非外部输入
    except (OSError, pickle.UnpicklingError, EOFError, AttributeError, ValueError):
        return None
    if not isinstance(payload, dict) or payload.get('schema') != CACHE_SCHEMA:
        return None
    if payload.get('key') != key or payload.get('namespace') != namespace:
        return None
    if 'value' not in payload:
        return None
    return payload


def save_object(page: str, namespace: str, key: str, value: Any, *,
                dv: str | None, asof: str | None, computed_at: str) -> Path | None:
    """原子写入缓存对象并清理该命名空间的过期版本。"""
    path = _object_path(page, namespace, key)
    payload = {
        'schema': CACHE_SCHEMA,
        'page': page,
        'namespace': namespace,
        'key': key,
        'dv': dv,
        'asof': asof,
        'computed_at': computed_at,
        'value': value,
    }
    try:
        _atomic_write(path, pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL))
    except (OSError, pickle.PicklingError):
        return None
    _prune(page, namespace)
    return path


def cached(page: str, namespace: str, *, asof: str | None,
           fingerprint: dict[str, Any], compute: Callable[[], Any],
           dv: str | None = None, refresh: bool = False) -> CacheResult:
    """命中缓存返回原值与原计算时间；否则现算并在有 dv 时写回。"""
    if dv is None:
        dv = data_version()
    key = build_key(page, asof=asof, dv=dv, fingerprint=fingerprint)
    if dv and not refresh:
        payload = load_object(page, namespace, key)
        if payload is not None:
            meta = CacheMeta(
                page=page, namespace=namespace, key=key, dv=dv, asof=asof,
                computed_at=str(payload.get('computed_at') or ''), hit=True,
                path=str(_object_path(page, namespace, key)),
            )
            return CacheResult(payload['value'], meta)

    value = compute()
    computed_at = _timestamp()
    path: Path | None = None
    if dv:
        path = save_object(page, namespace, key, value, dv=dv, asof=asof,
                           computed_at=computed_at)
    meta = CacheMeta(
        page=page, namespace=namespace, key=key, dv=dv, asof=asof,
        computed_at=computed_at, hit=False, path=str(path) if path else None,
    )
    return CacheResult(value, meta)


def dv_missing_note() -> str:
    return '数据版本缺失，未写缓存'


def describe(meta: CacheMeta | None) -> str:
    """状态栏补充文案：数据版本 + 计算时间（命中显示原计算时间）。"""
    if meta is None:
        return ''
    if not meta.dv:
        return f' ｜ {dv_missing_note()} ｜ 计算时间 {meta.computed_at}'
    if meta.hit:
        return f' ｜ 缓存命中 ｜ 数据版本 {meta.dv} ｜ 原计算时间 {meta.computed_at}'
    return f' ｜ 数据版本 {meta.dv} ｜ 计算时间 {meta.computed_at}'
