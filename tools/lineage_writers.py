"""写者鉴定：只认「写调用 + 可解析的落盘目标」为证据。

血缘图旧口径（``build_lineage.writers_for`` + ``resolve_edges2`` 的表键）有三类缺陷，
本模块把它们从判定规则里显式排除：

1. **表名子串**：``'sh' in '<call:snapshot_path>'`` 曾把 snapshot / shares / shard /
   refresh 全部算成 ``quotes/*/sh/{code}.parquet`` 的写者；
2. **就近提及**：表名出现在 docstring、函数名（``def load_st_intervals``）或输入路径
   列表里就被当成写边；
3. **未解析目标**：``<call:refresh>``、``<path>`` 这类表达式解析不出文件名时仍产边。

现在的判定只有一条：写调用的落盘目标能被解析成「至少 目录 + 文件名」两段，
且与表路径逐段（从右往左）兼容，文件名段必须兼容。解析不出文件名 → 不产边。

目标解析覆盖：字符串常量、``Path / 'x'`` 运算、f-string、同模块/同类 helper 的返回值、
包装函数（``_atomic_write(frame, path)``）的调用方实参、注册表（``SCHEMAS[table]``）键展开。
"""
from __future__ import annotations

import ast
import fnmatch
import re
import warnings
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

#: 写调用名。``save`` / ``load`` 这类过于通用的不入表：真正的落盘语句在 helper 内部，
#: 由本模块对 helper 的调用方回填解析。
WRITE_FUNCS = {
    'write_parquet_atomic', 'write_text_atomic', 'write_bytes_atomic',
    'write_parquet', 'write_csv', 'write_json', 'write_excel',
    'to_parquet', 'to_csv', 'sink_parquet',
}

#: 临时文件 + 改名落盘的写法（``tmp.write_text(...); tmp.replace(path)``）：
#: 只认单实参形态，避免与 ``str.replace(old, new)`` 混淆。
RENAME_FUNCS = {'replace', 'rename'}
#: 文本写者：目标以 ``.parquet`` 结尾说明解析串了，按调用类型剔除
_TEXT_CALLS = {'write_text_atomic', 'write_csv', 'to_csv', 'write_json'}

#: 透传路径的函数：取第一个实参当目标。
_TRANSPARENT = {'Path', 'str', 'resolve', 'absolute', 'expanduser', 'with_suffix', 'PosixPath'}

#: 运行时未知值的占位符（单个路径段）。与字符串里字面出现的 ``*`` 不是一回事：
#: 前者来自未解析变量，可匹配任意一段；后者来自代码里的 glob 字面量，只认同形。
UNK = '\x01'
#: 函数形参占位符，用于向调用方回填实参。
PARAM = '\x02'

#: 非正式层：目标是这些目录时不产边（staging / 备份 / 测试目录不是正式表的写者）。
NON_FORMAL = ('_staging', '_backup', 'tests', 'tmp')

#: 注册表字典名（用于把 ``f'{table}.parquet'`` 展开成具体表名）。
_REGISTRY_NAME = re.compile(r'(SCHEM|TABLE|FILES|DATASETS)', re.I)

_MAX_COMBOS = 64
#: 表达式/函数链解析深度上限。只用于兜底防爆，不用于限制造访层级：函数递归由
#: ``seen`` 逐名阻断，深度太小会把 ``write → _atomic_write → table_path →
#: financials_dir`` 这类两层 helper 的目标目录截断成 ``\\x01/\\x01.parquet``。
_MAX_DEPTH = 24


@dataclass
class WriteSite:
    """一处写盘语句及其可解析目标。"""

    file: str          # 相对仓库根的 posix 路径
    line: int
    func: str
    call: str
    targets: list[str]  # 归一化后的目标 glob（可能多条：同模块 helper 多 return）
    origin: str        # routine / staging


@dataclass
class _Func:
    node: ast.AST
    params: list[str]
    env: dict[str, ast.AST] = field(default_factory=dict)
    domains: dict[str, list[str]] = field(default_factory=dict)


class _Module:
    """单个 .py 文件的解析上下文。"""

    def __init__(self, rel: str, tree: ast.Module) -> None:
        self.rel = rel
        self.module_env: dict[str, ast.AST] = {}
        self.dicts: dict[str, list[str]] = {}
        self.funcs: dict[str, _Func] = {}
        self.props: dict[str, ast.AST] = {}
        self.callers: dict[str, list[dict[str, ast.AST]]] = {}
        #: ``parser.add_argument('--out', default=...)`` 的默认值表达式：``--out`` →
        #: default。``--out`` 的默认值就是代码里写明的落盘路径，与字符串常量同级证据。
        self.arg_defaults: dict[str, ast.AST] = {}
        self._collect(tree)
        self.callers = _collect_callers(self)

    def _collect(self, tree: ast.Module) -> None:
        # 第一遍：模块级赋值（常量、注册表字典）先入表，供函数内的域展开使用
        for node in tree.body:
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                val = node.value
                targets = [node.target] if isinstance(node, ast.AnnAssign) else node.targets
                name = (targets[0].id
                        if len(targets) == 1 and isinstance(targets[0], ast.Name) else None)
                if not name:
                    continue
                self.module_env[name] = val
                if isinstance(val, ast.Dict) and _REGISTRY_NAME.search(name):
                    keys = [k.value for k in val.keys
                            if isinstance(k, ast.Constant) and isinstance(k.value, str)]
                    if keys:
                        self.dicts[name] = keys
        # argparse 默认值：``add_argument('--out', default=X)`` 的 X 即代码声明的路径
        self._collect_arg_defaults(tree)
        # 第二遍：函数与属性
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self._add_func(node, node.name)
            elif isinstance(node, ast.ClassDef):
                for sub in node.body:
                    if not isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        continue
                    if any(_is_property(d) for d in sub.decorator_list):
                        self.props[sub.name] = _first_return(sub)
                    else:
                        self._add_func(sub, sub.name)

    def _collect_arg_defaults(self, tree: ast.Module) -> None:
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call) and _call_name(node) == 'add_argument'):
                continue
            opt = node.args[0] if node.args else None
            default = next((k.value for k in node.keywords if k.arg == 'default'), None)
            if default is None:
                continue
            flags = opt.elts if isinstance(opt, (ast.List, ast.Tuple)) else [opt]
            for flag in flags:
                if isinstance(flag, ast.Constant) and isinstance(flag.value, str):
                    # ``--output-dir`` 在代码里以 ``args.output_dir`` 访问，两种写法都登记
                    key = flag.value.lstrip('-')
                    self.arg_defaults[key] = default
                    self.arg_defaults[key.replace('-', '_')] = default

    def _add_func(self, node: ast.AST, name: str) -> None:
        args = node.args  # type: ignore[attr-defined]
        params = [a.arg for a in list(getattr(args, 'posonlyargs', [])) + list(args.args)]
        env: dict[str, ast.AST] = {}
        for stmt in ast.walk(node):
            if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1:
                t = stmt.targets[0]
                if isinstance(t, ast.Name):
                    env[t.id] = stmt.value
            elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name) and stmt.value:
                env[stmt.target.id] = stmt.value
        func = _Func(node=node, params=params, env=env)
        func.domains = self._param_domains(node, params)
        func.domains.update(self._loop_domains(node, env))
        self.funcs[name] = func

    def _loop_domains(self, node: ast.AST, env: dict[str, ast.AST]) -> dict[str, list[str]]:
        """``for name in [字面量]`` / ``for layer in LAYER_ORDER``：循环变量取值域。

        循环变量的样本来自代码里写死的文件名清单（``names = ['etf_fee.parquet', ...]``）
        或注册表键（``for layer in LAYER_FILES``），与 ``SCHEMAS[table]`` 同属「值在源码里
        写明」的证据；不是猜运行期取值。
        """
        domains: dict[str, list[str]] = {}
        for sub in ast.walk(node):
            if not isinstance(sub, ast.For):
                continue
            found: dict[str, list[str]] = {}
            if isinstance(sub.target, ast.Name):
                found[sub.target.id] = _literal_values(sub.iter, env, self.module_env)
            elif isinstance(sub.target, ast.Tuple) and isinstance(sub.iter, (ast.Tuple, ast.List)):
                # ``for frame, name, columns in ((a, FILE_A, ...), (b, FILE_B, ...))``
                for pos, elt in enumerate(sub.target.elts):
                    if not isinstance(elt, ast.Name):
                        continue
                    values = []
                    for row in sub.iter.elts:
                        if isinstance(row, ast.Tuple) and pos < len(row.elts):
                            values += _literal_values(row.elts[pos], env, self.module_env)
                    found[elt.id] = values
            for name, values in found.items():
                if values:
                    domains.setdefault(name, [])
                    domains[name] += [v for v in values if v not in domains[name]]
        return domains

    def _param_domains(self, node: ast.AST, params: list[str]) -> dict[str, list[str]]:
        """形参被当作注册表键使用时（``SCHEMAS[table]``），用注册表键当取值域。"""
        domains: dict[str, list[str]] = {}
        for sub in ast.walk(node):
            if not isinstance(sub, ast.Subscript):
                continue
            if not isinstance(sub.slice, ast.Name) or sub.slice.id not in params:
                continue
            base = sub.value
            if isinstance(base, ast.Name) and base.id in self.dicts:
                domains[sub.slice.id] = list(self.dicts[base.id])
        return domains


def _is_property(dec: ast.AST) -> bool:
    return isinstance(dec, ast.Name) and dec.id == 'property'


def _first_return(node: ast.AST) -> ast.AST:
    for sub in ast.walk(node):
        if isinstance(sub, ast.Return) and sub.value is not None:
            return sub.value
    return ast.Constant(value='')


def _call_name(node: ast.Call) -> str:
    f = node.func
    if isinstance(f, ast.Attribute):
        return f.attr
    if isinstance(f, ast.Name):
        return f.id
    return ''


def _returns(func: _Func) -> list[ast.AST]:
    return [s.value for s in ast.walk(func.node)
            if isinstance(s, ast.Return) and s.value is not None]


def _dict_literal(node: ast.AST | None, env: dict[str, ast.AST],
                  module_env: dict[str, ast.AST]) -> ast.Dict | None:
    """把 ``LAYER_FILES`` / 局部 ``MAPPING`` 这类名字还原成字典字面量。"""
    seen: set[int] = set()
    while isinstance(node, ast.Name) and id(node) not in seen:
        seen.add(id(node))
        node = env.get(node.id) or module_env.get(node.id)
    return node if isinstance(node, ast.Dict) else None


def _literal_values(node: ast.AST | None, env: dict[str, ast.AST],
                    module_env: dict[str, ast.AST],
                    seen: set[int] | None = None) -> list[str]:
    """容器字面量里的字符串元素（字典取键），用于循环变量取值域。"""
    seen = set() if seen is None else seen
    while isinstance(node, ast.Name) and id(node) not in seen:
        seen.add(id(node))
        node = env.get(node.id) or module_env.get(node.id)
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.Dict):
        return [k.value for k in node.keys
                if isinstance(k, ast.Constant) and isinstance(k.value, str)]
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        return [e.value for e in node.elts
                if isinstance(e, ast.Constant) and isinstance(e.value, str)]
    if (isinstance(node, ast.Call) and id(node) not in seen and node.args
            and _call_name(node) in ('tuple', 'list', 'set', 'sorted')):
        seen.add(id(node))
        return _literal_values(node.args[0], env, module_env, seen)
    return []


def _file_form(node: ast.AST | None, mod: _Module) -> str | None:
    """把 ``Path(__file__)[.resolve()].parents[k]`` 还原成相对仓库根的目录。

    ``Path(__file__).parent / 'raw'`` 曾是假阳性来源：``__file__`` 解析成未知段时，
    目标退化成 ``\\x01/raw/*``，会被任何以 ``raw`` 结尾的表目录吸收。这里把模块自身
    路径当已知量代进去，让 staging 下的写目标能被 :data:`NON_FORMAL` 正确排除。
    """
    if isinstance(node, ast.Name) and node.id == '__file__':
        return mod.rel
    if isinstance(node, ast.Call) and _call_name(node) == 'Path' and len(node.args) == 1:
        return _file_form(node.args[0], mod)
    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr in _TRANSPARENT):
        return _file_form(node.func.value, mod)
    if isinstance(node, ast.Attribute) and node.attr in ('parent', 'name', 'stem'):
        inner = _file_form(node.value, mod)
        if inner is not None:
            p = PurePosixPath(inner)
            return str(getattr(p, node.attr))
    if (isinstance(node, ast.Subscript) and isinstance(node.value, ast.Attribute)
            and node.value.attr == 'parents'
            and isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, int)):
        inner = _file_form(node.value.value, mod)
        if inner is not None:
            parts = PurePosixPath(inner).parts
            keep = len(parts) - 1 - node.slice.value
            return '/'.join(parts[:keep]) or '.'
    return None


def _resolve(node: ast.AST | None, mod: _Module, env: dict[str, ast.AST],  # noqa: C901
             params: set[str], domains: dict[str, list[str]] | None = None,
             depth: int = 0, seen: frozenset[str] = frozenset()) -> list[str]:
    """把路径表达式解析成若干候选字符串；未知处用 UNK / PARAM 占位。

    按 AST 节点类型分派（常量 / 名字 / 属性 / 下标 / IfExp / 除法 / f-string / 调用），
    每个分支只做一件事；拆成多个函数反而要在各分支间传递 ``env / params /
    domains / depth / seen`` 五个上下文参数，故保留单函数分派。
    """
    domains = domains or {}
    if node is None or depth > _MAX_DEPTH:
        return [UNK]
    anchor = _file_form(node, mod)
    if anchor is not None:
        return [anchor]
    if isinstance(node, ast.Constant):
        return [node.value] if isinstance(node.value, str) else [UNK]
    if isinstance(node, ast.Name):
        if node.id in domains:
            return list(domains[node.id])
        if node.id in params:
            return [f'{PARAM}{node.id}{PARAM}']
        if node.id in env:
            return _resolve(env[node.id], mod, env, params, domains, depth + 1, seen)
        if node.id in mod.module_env:
            return _resolve(mod.module_env[node.id], mod, {}, params, domains, depth + 1, seen)
        return [UNK]
    if isinstance(node, ast.Attribute):
        base = node.value
        if isinstance(base, ast.Name) and base.id == 'args' and node.attr in mod.arg_defaults:
            return _resolve(mod.arg_defaults[node.attr], mod, {}, set(), {}, depth + 1, seen)
        if isinstance(base, ast.Name) and base.id in ('self', 'cls') and node.attr in mod.props:
            return _resolve(mod.props[node.attr], mod, {}, params, domains, depth + 1, seen)
        if isinstance(base, ast.Name) and base.id in mod.module_env:
            return _resolve(mod.module_env[base.id], mod, env, params, domains, depth + 1, seen)
        return [UNK]
    if isinstance(node, ast.Subscript):
        table = _dict_literal(node.value, env, mod.module_env)
        if table is not None:
            wanted = _literal_values(node.slice, {}, {}) if not isinstance(node.slice, ast.Name) \
                else list(domains.get(node.slice.id, []))
            if isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
                wanted = [node.slice.value]
            hits = [v for k, v in zip(table.keys, table.values, strict=False)
                    if isinstance(k, ast.Constant) and k.value in wanted]
            if hits:
                out: list[str] = []
                for hit in hits:
                    out += _resolve(hit, mod, env, params, domains, depth + 1, seen)
                return out
        return _resolve(node.value, mod, env, params, domains, depth + 1, seen)
    if isinstance(node, ast.IfExp):
        return _resolve(node.body, mod, env, params, domains, depth + 1, seen) + \
            _resolve(node.orelse, mod, env, params, domains, depth + 1, seen)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
        left = _resolve(node.left, mod, env, params, domains, depth + 1, seen)
        right = _resolve(node.right, mod, env, params, domains, depth + 1, seen)
        return [_join(left_part, right_part)
                for left_part in left for right_part in right][:_MAX_COMBOS]
    if isinstance(node, ast.JoinedStr):
        combos = ['']
        for part in node.values:
            opts = [str(part.value)] if isinstance(part, ast.Constant) else [UNK]
            combos = [c + o for c in combos for o in opts][:_MAX_COMBOS]
        return combos
    if isinstance(node, ast.Call):
        name = _call_name(node)
        if name in _TRANSPARENT and node.args:
            return _resolve(node.args[0], mod, env, params, domains, depth + 1, seen)
        if name in ('joinpath', 'join'):
            combos = ['']
            for arg in node.args:
                opts = _resolve(arg, mod, env, params, domains, depth + 1, seen)
                combos = [_join(c, o) for c in combos for o in opts][:_MAX_COMBOS]
            return combos
        if name in mod.funcs and name not in seen and depth < _MAX_DEPTH:
            func = mod.funcs[name]
            out: list[str] = []
            for ret in _returns(func):
                out += _resolve(ret, mod, func.env, set(func.params), func.domains,
                                depth + 1, seen | {name})
            # helper 自身形参用其调用方实参回填（多跳：write → _atomic_write → table_path）
            return _fill_params(out[:_MAX_COMBOS], mod, func, mod.callers,
                                depth + 1, seen | {name}) or [UNK]
        return [UNK]
    return [UNK]


def _join(left: str, right: str) -> str:
    return left.rstrip('/') + '/' + right.lstrip('/')


def _norm(text: str) -> str:
    text = text.replace('\\', '/')
    text = re.sub(r'/+', '/', text)
    return text.strip()


def _fill_params(patterns: list[str], mod: _Module, func: _Func,
                 callers: dict[str, list[dict[str, ast.AST]]],
                 depth: int = 0, seen: frozenset[str] = frozenset()) -> list[str]:
    """把 ``\\x02param\\x02`` 用同文件调用方的实参回填（一层，够覆盖包装函数）。"""
    token = re.compile(f'{PARAM}(\\w+){PARAM}')
    out: list[str] = []
    for pat in patterns:
        names = set(token.findall(pat))
        options = []
        for name in sorted(names):
            vals: list[str] = []
            if name in func.params:
                idx = func.params.index(name)
                for caller in callers.get(_func_key(func), []):
                    arg = caller['args'][idx] if idx < len(caller['args']) else caller['kwargs'].get(name)
                    if arg is not None:
                        vals += _resolve(arg, mod, caller['env'], set(caller['params']),
                                         caller['domains'], depth + 1, seen)
            options.append((name, vals or [UNK]))
        combos = [pat]
        for name, vals in options:
            vals = list(dict.fromkeys(vals))  # 去重：多分支解析会产出大量同值
            combos = [c.replace(f'{PARAM}{name}{PARAM}', v) for c in combos for v in vals][:_MAX_COMBOS]
        out += combos
    return list(dict.fromkeys(out))


def _func_key(func: _Func) -> str:
    return getattr(func.node, 'name', '')


def _normalize_targets(patterns: list[str], call: str = '') -> list[str]:
    """占位符归一化 + 过滤非正式层目标。

    文件名段保留「纯未知」形态（``\\x01.parquet``）：它只在「表本身就是逐个代码文件
    的目录」时才可能命中，由 :func:`covers` 判定；其余情况一律不产边。
    ``call`` 是写调用名：文本写者产出 ``.parquet`` 属自相矛盾，按调用类型剔除。
    """
    out: list[str] = []
    for pat in patterns:
        pat = re.sub(rf'{PARAM}\w+{PARAM}', UNK, pat)
        if not pat or pat == UNK:
            continue
        if not re.search(r'\.(parquet|csv|json)$', pat):
            continue
        if call in _TEXT_CALLS and pat.endswith('.parquet'):
            continue
        segs = [s for s in _norm(pat).split('/') if s not in ('', '.')]
        if len(segs) < 2:
            continue
        if any(s in NON_FORMAL for s in segs if UNK not in s):
            continue
        out.append('/'.join(segs))
    return list(dict.fromkeys(out))


def _pure_unknown(seg: str) -> bool:
    """整段来自未解析变量（``\\x01`` 或 ``\\x01.parquet``），不含任何字面成分。"""
    stem = seg.rsplit('.', 1)[0] if '.' in seg else seg
    return stem in ('', UNK)


def _seg_ok(table_seg: str, tgt_seg: str) -> bool:
    """表段（可含 glob）与目标段（可含 UNK / 字面 glob）是否兼容。"""
    if tgt_seg == UNK:
        return bool(table_seg)
    if table_seg == UNK:
        return tgt_seg == UNK
    if '*' in tgt_seg:  # 代码里的字面 glob：只认同形，避免过度覆盖
        return tgt_seg == table_seg
    return fnmatch.fnmatchcase(tgt_seg, table_seg)


#: 目录级归属要求的最少「具体目录段」数（见 :func:`covers`）
_MIN_CONCRETE_DIRS = 1


def covers(target: str, table_glob: str) -> bool:
    """目标 glob 是否覆盖表 glob：从右往左逐段兼容。

    三条硬约束：
    - 目标至少要有一个「落到实处」的目录段，否则目录全未知时任何表都能对上
      （``\\x01/st_coverage.parquet`` 曾因此命中 ``meta/st/raw/*.parquet``）；
    - 文件名整段未知（``\\x01/meta/financials/\\x01.parquet``）时按「目录级写者」处理：
      只要求目录链兼容且至少 ``_MIN_CONCRETE_DIRS`` 个具体目录段，归属到该目录内的表。
      这种情况下文件名本身是运行期变量（``SCHEMAS[table]`` 之流），按目录归属是唯一
      能不猜的判法；
    - 文件名可解析时必须逐段兼容，不允许用通配「兜住」具体表名。
    """
    ts = [s for s in target.split('/') if s]
    bs = [s for s in table_glob.split('/') if s]
    if len(ts) < 2 or len(bs) < 2:
        return False
    if _pure_unknown(ts[-1]):
        dirs = ts[:-1]
        depth = min(len(dirs), len(bs) - 1)
        if depth < 1:
            return False
        aligned = dirs[-depth:]
        if sum(s != UNK for s in aligned) < _MIN_CONCRETE_DIRS:
            return False
        return all(_seg_ok(bs[-1 - i], dirs[-i]) for i in range(1, depth + 1))
    head = min(len(ts), len(bs)) - 1
    if head < 1 or not any(s != UNK for s in ts[-head - 1:-1]):
        return False
    return all(_seg_ok(bs[-i], ts[-i]) for i in range(1, min(len(ts), len(bs)) + 1))


def table_globs(path: str) -> list[str]:
    """把表路径模板展开成 glob 列表（去掉 store 前缀，占位符归一化）。"""
    raw = path.replace('\\', '/')
    prefix = 'data/local_store/'
    if raw.startswith(prefix):
        raw = raw[len(prefix):]
    raw = re.sub(r'<[^>]*>', '*', raw)
    return list(dict.fromkeys(_norm(item) for item in _brace_expand(raw)))


def _brace_expand(text: str) -> list[str]:
    """展开 ``{a,b,c}`` 与 ``{0..5}``。"""
    match = re.search(r'\{([^{}]*)\}', text)
    if not match:
        return [text]
    body = match.group(1)
    if '..' in body and ',' not in body:
        lo, hi = body.split('..', 1)
        try:
            options = [str(i) for i in range(int(lo), int(hi) + 1)]
        except ValueError:
            options = ['*']
    else:
        options = body.split(',')
    out: list[str] = []
    for opt in options:
        out += _brace_expand(text[:match.start()] + opt + text[match.end():])
    return out


def _collect_callers(mod: _Module) -> dict[str, list[dict[str, ast.AST]]]:
    """同文件内「函数名 → 调用点实参」，用于包装函数的目标回填。"""
    callers: dict[str, list[dict[str, ast.AST]]] = {}
    for name, func in mod.funcs.items():
        for node in ast.walk(func.node):
            if isinstance(node, ast.Call):
                callee = _call_name(node)
                if callee in mod.funcs:
                    callers.setdefault(callee, []).append({
                        'args': list(node.args),
                        'kwargs': {k.arg: k.value for k in node.keywords if k.arg},
                        'env': func.env,
                        'params': list(func.params),
                        'domains': func.domains,
                    })
    return callers


def _is_write(node: ast.Call) -> bool:
    name = _call_name(node)
    if name in WRITE_FUNCS:
        return True
    return name in RENAME_FUNCS and len(node.args) == 1


def scan_file(path: Path, rel: str, origin: str) -> list[WriteSite]:
    src = path.read_text(encoding='utf-8', errors='ignore')
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', SyntaxWarning)  # 只解析，别人的非法转义不关本工具
            tree = ast.parse(src)
    except SyntaxError:
        return []
    mod = _Module(rel, tree)
    callers = _collect_callers(mod)
    sites: list[WriteSite] = []

    def visit(func: _Func) -> None:
        for node in ast.walk(func.node):
            if not isinstance(node, ast.Call) or not _is_write(node):
                continue
            cand: list[str] = []
            for arg in list(node.args) + [k.value for k in node.keywords if k.arg]:
                resolved = _resolve(arg, mod, func.env, set(func.params), func.domains)
                cand += _fill_params(resolved, mod, func, callers)
            targets = _normalize_targets(cand, _call_name(node))
            # 目标解析不出来也照样登记：它是「当前源码里确有写调用」的证据，
            # 用于校验上游快照记录的行号是否已经漂移（见 detect_writers）。
            sites.append(WriteSite(file=rel, line=node.lineno, func=_func_key(func),
                                   call=_call_name(node), targets=targets, origin=origin))

    for func in mod.funcs.values():
        visit(func)
    for node in ast.walk(tree):  # 模块级调用
        if isinstance(node, ast.Call) and _is_write(node):
            cand: list[str] = []
            for arg in list(node.args) + [k.value for k in node.keywords if k.arg]:
                cand += _resolve(arg, mod, {}, set())
            targets = _normalize_targets(cand, _call_name(node))
            sites.append(WriteSite(file=rel, line=node.lineno, func='<module>',
                                   call=_call_name(node), targets=targets, origin=origin))
    return sites


def scan(root: Path, dirs: tuple[str, ...] = ('src', 'tools', '_staging')) -> list[WriteSite]:
    """扫描仓库内所有 .py 的写盘语句。

    ``_staging/ci_cov_sim_*`` 是整仓副本，``tests`` 写的是 tmp 目录：两者都不是
    正式表的写者，跳过。
    """
    sites: list[WriteSite] = []
    for base in dirs:
        top = root / base
        if not top.is_dir():
            continue
        origin = 'staging' if base == '_staging' else 'routine'
        for path in sorted(top.rglob('*.py')):
            if any(part in ('__pycache__', '.venv', 'node_modules') for part in path.parts):
                continue
            rel = path.relative_to(root).as_posix()
            if 'ci_cov_sim_' in rel or '/tests/' in rel or rel.startswith('tests/'):
                continue
            sites += scan_file(path, rel, origin)
    return sites


def normalize_expr(expr: str) -> str | None:
    """把写调用记录里的路径表达式归一化成目标 glob（``<...>`` / ``{...}`` → 未知段）。"""
    if not expr:
        return None
    text = re.sub(r'<[^>]*>|\{[^{}]*\}|\x02\w+\x02', UNK, expr)
    pats = _normalize_targets([text])
    return pats[0] if pats else None


def _unique_basenames(inventory: list[str]) -> dict[str, str]:
    """清单内「文件名唯一」的表：目录解析不出来时，唯一文件名仍可安全归属。"""
    seen: dict[str, set[str]] = {}
    for path in inventory:
        for glob in table_globs(path):
            base = glob.rsplit('/', 1)[-1]
            if '*' in base or UNK in base:
                continue
            seen.setdefault(base, set()).add(path)
    return {base: next(iter(paths)) for base, paths in seen.items() if len(paths) == 1}


def _skip_record(rec: dict) -> bool:
    """上游记录里的整仓副本 / 测试目录不参与归属。"""
    rel = rec.get('file', '')
    return 'ci_cov_sim_' in rel or '/tests/' in rel or rel.startswith('tests/')


def detect_writers(table_path: str, sites: list[WriteSite],
                   records: list[dict] | None = None,
                   inventory: list[str] | None = None, limit: int = 12) -> list[dict]:
    """按证据给出一张表的写者清单（同一写盘语句只留一条）。

    证据有两个来源，都只提供「写调用 + 目标」，归属仍由 :func:`covers` 判定：
    - ``sites``：本次 AST 解析出的写调用（含 helper 目标回填）；
    - ``records``：上游 ``edges_raw2.json`` 的写调用记录（``file/line/call/path_expr``），
      只取路径表达式，**不使用它的表键归属**——那套归属就是假阳性的来源。
    目标目录不可解析、但文件名在清单内唯一时，按唯一文件名归属（不是猜：同名表只有一张）。
    """
    globs = table_globs(table_path)
    unique = _unique_basenames(inventory or [])
    #: 上游 ``edges_raw2.json`` 是某一时点的快照；源码此后增删行会让记录的行号指到
    #: 注释、续行或 print 上（实测 13 条记录里有 7 条如此）。已被本次扫描覆盖的文件，
    #: 行号必须以当前 AST 的写调用为准，否则整条记录丢弃。
    ast_files = {s.file for s in sites}
    ast_lines = {(s.file, s.line) for s in sites}
    by_site: dict[tuple[str, int], dict] = {}
    for site in sites:
        by_site.setdefault((site.file, site.line), {
            'file': site.file, 'line': site.line, 'call': site.call,
            'origin': site.origin, 'targets': [],
        })['targets'].extend(site.targets)
    for rec in records or []:
        if _skip_record(rec):
            continue
        if rec['file'] in ast_files and (rec['file'], rec['line']) not in ast_lines:
            continue
        target = normalize_expr(rec.get('path_expr', ''))
        if target is None:
            continue
        key = (rec['file'], rec['line'])
        item = by_site.setdefault(key, {
            'file': rec['file'], 'line': rec['line'], 'call': rec.get('call', ''),
            'origin': 'staging' if rec['file'].startswith('_staging/') else 'routine',
            'targets': [],
        })
        item['targets'].append(target)

    hits: dict[tuple[str, int], dict] = {}
    for key, item in by_site.items():
        target = next((t for t in item['targets'] if any(covers(t, g) for g in globs)), None)
        if target is None:
            target = next((t for t in item['targets']
                           if UNK not in t.rsplit('/', 1)[-1]
                           and unique.get(t.rsplit('/', 1)[-1]) == table_path), None)
        if target is None:
            continue
        hits[key] = {
            'file': item['file'],
            'line': item['line'],
            'mode': 'oneoff' if item['origin'] == 'staging' else 'routine',
            'call': item['call'],
            'target': target,
        }
    ordered = sorted(hits.values(), key=lambda h: (h['mode'] != 'routine', h['file'], h['line']))
    return ordered[:limit]
