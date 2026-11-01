"""文档一致性守卫：README 的项目结构与仓库实际保持同步。

防止 README 再次残留已删除目录（如 src/business），或漏写新增模块，
导致新人按文档找不到代码；同时锁定覆盖率门禁数值只有一处口径。
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
README = REPO_ROOT / 'README.md'
PYPROJECT = REPO_ROOT / 'pyproject.toml'

_SRC_BLOCK_END = '```'
_SRC_DIR_ENTRY = re.compile(r'^    [├└]── ([a-z_]+)/')


def _src_dir_entries() -> list[str]:
    """解析 README 中 `└── src/` 代码块，返回其中列出的直接子目录名。"""
    lines = README.read_text(encoding='utf-8').splitlines()
    start = next(
        (i for i, line in enumerate(lines) if line.strip().startswith('└── src/')),
        None,
    )
    if start is None:
        return []

    entries = []
    for line in lines[start + 1:]:
        if line.strip() == _SRC_BLOCK_END:
            break
        match = _SRC_DIR_ENTRY.match(line)
        if match:
            entries.append(match.group(1))
    return entries


def test_readme_src_dirs_exist():
    """README 项目结构里列出的 src 子目录必须真实存在"""
    entries = _src_dir_entries()
    assert entries, 'README 中未解析到 src 目录树，文档结构可能已被破坏'

    missing = [name for name in entries if not (REPO_ROOT / 'src' / name).is_dir()]
    assert not missing, f'README 列出的 src 目录不存在: {missing}'


def test_readme_has_no_removed_business_module():
    """已删除的 src/business 不得再出现在 README"""
    text = README.read_text(encoding='utf-8')
    assert 'business/' not in text, 'README 仍引用已删除的 business/ 模块'


def test_readme_coverage_gate_matches_pyproject():
    """README 声明的覆盖率门禁必须与 pyproject 的 fail_under 一致"""
    match = re.search(r'fail_under\s*=\s*(\d+)', PYPROJECT.read_text(encoding='utf-8'))
    assert match, 'pyproject.toml 未配置 [tool.coverage.report] fail_under'

    gate = match.group(1)
    readme_text = README.read_text(encoding='utf-8')
    assert f'fail_under = {gate}' in readme_text, (
        f'README 声明的覆盖率门禁与 pyproject（fail_under = {gate}）不一致'
    )
