"""代码地图防过期守卫：docs/代码地图.md 里的「文件 → 符号」引用必须仍然成立。

地图是人工维护的，代码改了地图不会自己跟着变；本用例把地图里每条文件引用解析成
(相对路径, 符号名) 后逐条断言：路径存在、符号名确实出现在该文件文本里。符号被删除
或改名时这里必须失败，不允许写成恒真。

解析约定（新增/修改地图行时照此书写）：
- 文件引用写成 `[标签](file:///e:/code/python/quant/<相对路径>#L行号)`，或写成裸的
  `模块名.py` / `包/模块.py`（裸名必须能在 src、tools 下唯一定位）；
- 符号名写在该引用之后，用反引号包裹，如 `build()` / `LocalStore.write_daily()`；
  若引用后面跟的是产出物、列名等非符号内容，则视为路径级引用，只校路径。
凡机器解析不出来的引用必须登记进 WHITELIST 并写明原因，禁止静默跳过。
"""

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
MAP_FILE = REPO_ROOT / 'docs' / '代码地图.md'

_PATH_PREFIX = 'file:///e:/code/python/quant/'
# 索引范围限定在源码目录，避免 _staging/、tmp/、reports/ 里的历史副本造成同名歧义
_INDEX_ROOTS = ('src', 'tools')

_LINK_RE = re.compile(r'\[[^\]]*\]\((file:///e:/code/python/quant/[^)\s]+)\)')
_BARE_PY_RE = re.compile(r'(?<![\[/\w.\-])((?:[\w.\-]+/)*[\w\-]+\.py)')
_CODE_RE = re.compile(r'`([^`]+)`')
_LINE_MARK_RE = re.compile(r'#L\d+')
_IDENT_RE = re.compile(r'^[A-Za-z_]\w*$')
# 反引号里以这些后缀结尾的是数据文件/产物，不是符号名
_DATA_SUFFIXES = {
    'py', 'parquet', 'json', 'csv', 'md', 'yaml', 'yml', 'toml', 'jsonl', 'txt', 'log',
}

# 机器无法唯一解析的引用：{地图里出现的原始写法: 原因}
# 目前地图全部引用均可唯一解析；新增时必须在此写清为什么不能解析。
WHITELIST: dict[str, str] = {}


def _source_index() -> dict[str, list[str]]:
    """文件名 → src/tools 下的相对路径清单。"""
    index: dict[str, list[str]] = {}
    for root in _INDEX_ROOTS:
        for path in (REPO_ROOT / root).rglob('*.py'):
            if '__pycache__' in path.parts:
                continue
            rel = path.relative_to(REPO_ROOT).as_posix()
            index.setdefault(path.name, []).append(rel)
    return index


_INDEX = _source_index()


def _resolve_bare(token: str) -> str | None:
    """把地图里的裸文件名/部分路径解析成相对仓库根的真实路径；无法唯一解析返回 None。"""
    candidates = _INDEX.get(Path(token).name, [])
    if '/' in token:
        candidates = [rel for rel in candidates if rel.endswith(token)]
    return candidates[0] if len(candidates) == 1 else None


def _as_symbol(token: str) -> str | None:
    """把反引号内容规范成符号名；不是标识符（路径、列表达式、产物名）返回 None。"""
    name = re.sub(r'\(.*\)$', '', token.strip())
    if not name or any(ch in name for ch in '/ *<>{}:()[]'):
        return None
    parts = name.split('.')
    if parts[-1] in _DATA_SUFFIXES:
        return None
    if not all(_IDENT_RE.match(part) for part in parts):
        return None
    return name


def _symbols_in(segment: str) -> list[str]:
    """取一段文本里的符号名：段首第一个反引号标识符，以及 #L 行号标记后的标识符。"""
    tokens = list(_CODE_RE.finditer(segment))
    found: list[str] = []
    for i, match in enumerate(tokens):
        gap = segment[tokens[i - 1].end() if i else 0:match.start()]
        if i and not _LINE_MARK_RE.search(gap):
            continue
        name = _as_symbol(match.group(1))
        if name:
            found.append(name)
    return found


def _collect() -> tuple[list[tuple[int, str, str]], list[tuple[int, str]]]:
    """解析整张地图，返回 (引用清单, 无法解析清单)，引用为 (行号, 相对路径, 符号名)。"""
    references: list[tuple[int, str, str]] = []
    unresolved: list[tuple[int, str]] = []
    for lineno, line in enumerate(MAP_FILE.read_text(encoding='utf-8').splitlines(), 1):
        if not line.startswith('- '):
            continue
        link_spans = [(m.start(), m.end()) for m in _LINK_RE.finditer(line)]
        # (起止位置, 相对路径或 None（无法解析）, 原始写法)
        refs: list[tuple[int, int, str | None, str]] = []
        for match in _LINK_RE.finditer(line):
            rel = match.group(1)[len(_PATH_PREFIX):].split('#')[0]
            if rel.endswith('.py'):
                refs.append((match.start(), match.end(), rel, rel))
        for match in _BARE_PY_RE.finditer(line):
            if any(start <= match.start() < end for start, end in link_spans):
                continue
            raw = match.group(1)
            refs.append((match.start(), match.end(), _resolve_bare(raw), raw))
        refs.sort()
        for i, (_, end, rel, raw) in enumerate(refs):
            next_start = refs[i + 1][0] if i + 1 < len(refs) else len(line)
            symbols = _symbols_in(line[end:next_start])
            if rel is None:
                unresolved.append((lineno, raw))
                continue
            for symbol in symbols or ['']:
                references.append((lineno, rel, symbol))
    return references, unresolved


REFERENCES, UNRESOLVED = _collect()


_IDS = [f'L{lineno}:{relpath}:{symbol or "路径"}' for lineno, relpath, symbol in REFERENCES]


def test_map_is_parseable_at_all():
    """防止解析器整体失效后所有断言变成空跑恒真"""
    assert REFERENCES, '未从代码地图解析出任何文件引用，解析规则可能已失效'
    assert len(REFERENCES) >= 80, f'解析到的引用只有 {len(REFERENCES)} 条，明显少于地图实际条目'
    assert len(UNRESOLVED) == len(WHITELIST), (
        '地图存在无法解析且未登记的引用: ' f'{[(n, r) for n, r in UNRESOLVED if r not in WHITELIST]}'
    )


def test_unresolved_refs_are_whitelisted_explicitly():
    """白名单本身不能腐烂：登记项必须仍是地图里真实存在的无法解析写法"""
    stale = sorted(pattern for pattern in WHITELIST if pattern not in {raw for _, raw in UNRESOLVED})
    assert not stale, f'白名单里这些写法已不存在或已可解析，应删除登记: {stale}'


@pytest.mark.parametrize('lineno, relpath, symbol', REFERENCES, ids=_IDS)
def test_referenced_file_exists(lineno: int, relpath: str, symbol: str):
    """地图第 lineno 行引用的文件必须真实存在于仓库"""
    target = REPO_ROOT / relpath
    assert target.is_file(), f'代码地图第 {lineno} 行引用的文件不存在: {relpath}'


_SYMBOL_REFS = [ref for ref in REFERENCES if ref[2]]
_SYMBOL_IDS = [f'L{lineno}:{relpath}:{symbol}' for lineno, relpath, symbol in _SYMBOL_REFS]


@pytest.mark.parametrize('lineno, relpath, symbol', _SYMBOL_REFS, ids=_SYMBOL_IDS)
def test_referenced_symbol_present(lineno: int, relpath: str, symbol: str):
    """地图声称的符号必须仍出现在该文件里（改名或删除都会失败）。

    带接收者的写法（LocalStore.write_daily）逐段校验，避免只为格式差异报错。
    """
    text = (REPO_ROOT / relpath).read_text(encoding='utf-8', errors='replace')
    missing = [part for part in symbol.split('.') if part not in text]
    assert not missing, (
        f'代码地图第 {lineno} 行引用的符号已不在 {relpath} 中: {symbol}'
        f'（缺失片段 {missing}），请同轮更新地图该行'
    )
