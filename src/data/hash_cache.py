"""按 ``(path, size, mtime)`` 复用磁盘事实（sha256 / rows / bytes）的缓存。

日更 ``manifest`` 步要对全树 walk 后逐件算 sha256 与行数，同一文件在一轮里会被
回写、对账、汇总摘要各算一次。本模块只做一件事：把「算过的事实」连同当时的
``st_size`` 与 ``st_mtime_ns`` 一起存盘，下一次三项全一致才复用。

判据纪律（不放宽门禁）
- 复用必须 ``size`` 与 ``mtime_ns`` **同时**精确相等；任一项不同即丢弃旧值重算。
- 缓存只影响「同一个文件的事实怎么得到」，不影响「哪些文件算变更」（那仍由
  ``daily_update._walk`` 按 mtime 与开工标记比较）与「manifest 登记值是否相符」
  （仍逐件比对 ``rows`` / ``bytes`` / ``sha256``）。
- 路径消失时不得命中：文件读不到就抛 ``FileNotFoundError``，不会拿旧值顶替。
- ``QUANT_MANIFEST_FULL_HASH=1``（或构造时 ``read_cached=False``）跳过读取、
  强制全量重算，并把新值写回缓存——用于怀疑缓存本身失效时的兜底。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

CACHE_VERSION = 1
#: 缓存文件路径覆盖（默认由调用方给出，日更落在 ``work/cache/manifest_facts.json``）。
ENV_CACHE_PATH = 'QUANT_MANIFEST_HASH_CACHE'
#: 置 1/true/yes/on 时强制全量重算，忽略已有缓存。
ENV_FULL_RECOMPUTE = 'QUANT_MANIFEST_FULL_HASH'
_TRUTHY = {'1', 'true', 'yes', 'on'}


def full_recompute_requested() -> bool:
    """环境变量是否要求强制全量重算。"""
    return os.environ.get(ENV_FULL_RECOMPUTE, '').strip().lower() in _TRUTHY


def _key(path: Path) -> str:
    try:
        return os.path.normcase(str(path.resolve()))
    except OSError:
        return os.path.normcase(str(path.absolute()))


class FactsCache:
    """``file_facts`` 的事实缓存；命中要求 size 与 mtime_ns 同时一致。"""

    def __init__(self, path: Path | str, *, read_cached: bool = True):
        self.path = Path(path)
        self.read_cached = read_cached
        self.hits = 0
        self.misses = 0
        self.recomputed = 0
        self._entries: dict[str, dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        if not self.read_cached or not self.path.is_file():
            return
        try:
            payload = json.loads(self.path.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            return
        if not isinstance(payload, dict) or payload.get('version') != CACHE_VERSION:
            return
        entries = payload.get('entries')
        if isinstance(entries, dict):
            self._entries = {key: value for key, value in entries.items()
                             if isinstance(value, dict)}

    def get(self, path: Path, stat: os.stat_result) -> dict[str, Any] | None:
        """三项一致才返回缓存事实；否则丢弃旧条目并返回 None。"""
        if not self.read_cached:
            self.misses += 1
            return None
        key = _key(path)
        entry = self._entries.get(key)
        if entry is None:
            self.misses += 1
            return None
        try:
            same = (int(entry['size']) == int(stat.st_size)
                    and int(entry['mtime_ns']) == int(stat.st_mtime_ns))
        except (KeyError, TypeError, ValueError):
            same = False
        if not same or not isinstance(entry.get('facts'), dict):
            self._entries.pop(key, None)
            self.misses += 1
            return None
        self.hits += 1
        return dict(entry['facts'])

    def put(self, path: Path, stat: os.stat_result, facts: dict[str, Any]) -> None:
        self.recomputed += 1
        self._entries[_key(path)] = {
            'size': int(stat.st_size),
            'mtime_ns': int(stat.st_mtime_ns),
            'facts': facts,
        }

    def save(self) -> None:
        if not self._entries:
            return
        text = json.dumps({'version': CACHE_VERSION, 'entries': self._entries},
                          ensure_ascii=False, sort_keys=True)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + '.tmp')
        tmp.write_text(text, encoding='utf-8')
        os.replace(tmp, self.path)
