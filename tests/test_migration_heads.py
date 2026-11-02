#!/usr/bin/env python3

"""Alembic 迁移图守卫：head 必须唯一。

历史教训：仓库曾同时存在 0001 与 0001_baseline 两条独立根迁移，
0002/0003 只挂在后者，导致 `alembic upgrade head` 因多 head 歧义失败，
且启动路径把该错误降级为 warning，schema 演进被长期阻塞。

本测试静态解析 migrations/versions 下的 revision/down_revision，
不连接数据库、不导入迁移模块，随 pytest 全量套件在 CI 中执行。
"""

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
VERSIONS_DIR = REPO_ROOT / 'migrations' / 'versions'


def _literal_value(node: ast.expr):
    """把 revision 赋值右侧的常量表达式还原为 str / tuple / None。"""
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, (ast.Tuple, ast.List)):
        return tuple(_literal_value(elt) for elt in node.elts)
    return None


def _module_level_assignments(tree: ast.Module) -> dict[str, object]:
    """读取模块顶层 `name = value` 与 `name: T = value` 赋值。"""
    values: dict[str, object] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    values[target.id] = _literal_value(node.value)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.value is not None:
                values[node.target.id] = _literal_value(node.value)
    return values


def load_revision_graph() -> dict[str, tuple[str, ...]]:
    """返回 {revision: 父 revision 元组}。"""
    graph: dict[str, tuple[str, ...]] = {}
    for path in sorted(VERSIONS_DIR.glob('*.py')):
        if path.name.startswith('__'):
            continue
        tree = ast.parse(path.read_text(encoding='utf-8-sig'))
        values = _module_level_assignments(tree)
        revision = values.get('revision')
        down_revision = values.get('down_revision')
        assert isinstance(revision, str) and revision, (
            f'{path.name}: 缺少合法的 revision 变量'
        )
        assert revision not in graph, f'重复的 revision id: {revision} ({path.name})'
        if down_revision is None:
            parents: tuple[str, ...] = ()
        elif isinstance(down_revision, str):
            parents = (down_revision,)
        else:
            parents = tuple(str(p) for p in down_revision)
        graph[revision] = parents
    return graph


def test_alembic_has_exactly_one_head():
    """head（未被任何 revision 引用的节点）必须唯一，防止再次分叉。"""
    graph = load_revision_graph()
    referenced = {parent for parents in graph.values() for parent in parents}
    heads = sorted(set(graph) - referenced)
    assert len(heads) == 1, (
        f'检测到 {len(heads)} 个 Alembic head: {heads}。'
        '请新增 merge 迁移（down_revision 指向全部 head）后再继续。'
    )


def test_all_down_revisions_exist():
    """每个 down_revision 都必须指向真实 revision，防止拼写错误造成假 head。"""
    graph = load_revision_graph()
    missing = sorted({
        parent for parents in graph.values() for parent in parents
        if parent not in graph
    })
    assert not missing, f'down_revision 指向不存在的 revision: {missing}'


def test_revision_ids_fit_version_num_column():
    """alembic_version.version_num 默认 VARCHAR(32)，超长会在写版本时失败。"""
    graph = load_revision_graph()
    too_long = sorted(rev for rev in graph if len(rev) > 32)
    assert not too_long, f'revision id 超过 32 字符，无法写入 alembic_version: {too_long}'


def test_every_revision_reachable_from_head():
    """所有 revision 必须能从唯一 head 反向遍历到，避免孤立分支。"""
    graph = load_revision_graph()
    referenced = {parent for parents in graph.values() for parent in parents}
    head = (set(graph) - referenced).pop()
    seen: set[str] = set()
    stack = [head]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        stack.extend(graph[current])
    assert seen == set(graph), f'存在无法从 head 到达的 revision: {sorted(set(graph) - seen)}'
