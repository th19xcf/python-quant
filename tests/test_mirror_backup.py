"""备份镜像工具的只增不删约束（红线回归用例）。"""

from __future__ import annotations

from pathlib import Path

from tools.mirror_backup import mirror_subdir, plan, rel_files


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding='utf-8')


def test_backup_only_file_is_never_deleted(tmp_path: Path) -> None:
    """备份端存在、源端没有的文件：只登记，跑完后必须仍然存在且内容不变。"""
    src, dst = tmp_path / 'src', tmp_path / 'dst'
    _write(src / 'reports' / 'keep' / 'a.csv', 'src-a')
    _write(dst / 'reports' / 'keep' / 'a.csv', 'src-a')
    only_backup = dst / 'reports' / 'keep' / 'only_in_backup.parquet'
    _write(only_backup, 'backup-only-payload')

    result = mirror_subdir('reports', src_base=src, dst_base=dst)

    assert only_backup.is_file(), '备份侧独有文件被删除，违反只增不删红线'
    assert only_backup.read_text(encoding='utf-8') == 'backup-only-payload'
    assert result.extra == ['keep/only_in_backup.parquet']


def test_backup_only_directory_files_all_survive(tmp_path: Path) -> None:
    """整个目录都只存在于备份端时，目录内每个文件都必须留下。"""
    src, dst = tmp_path / 'src', tmp_path / 'dst'
    _write(src / 'data' / 'local_store' / 'meta' / 'm.json', '{}')
    for name in ('README.md', 'parts/p1.parquet', 'parts/p2.parquet'):
        _write(dst / 'data' / 'local_store' / '_staging' / name, name)

    result = mirror_subdir('data/local_store', src_base=src, dst_base=dst)

    for name in ('README.md', 'parts/p1.parquet', 'parts/p2.parquet'):
        assert (dst / 'data' / 'local_store' / '_staging' / name).is_file()
    assert len(result.extra) == 3


def test_missing_source_file_is_copied(tmp_path: Path) -> None:
    """仅源端存在的文件要被补齐，且补齐后两份内容一致。"""
    src, dst = tmp_path / 'src', tmp_path / 'dst'
    _write(src / 'reports' / 'r' / 'new.csv', 'payload')
    (dst / 'reports' / 'r').mkdir(parents=True, exist_ok=True)

    result = mirror_subdir('reports', src_base=src, dst_base=dst)

    assert (dst / 'reports' / 'r' / 'new.csv').read_text(encoding='utf-8') == 'payload'
    assert result.copied == ['r/new.csv']
    assert result.mismatch == []


def test_dry_run_copies_nothing(tmp_path: Path) -> None:
    src, dst = tmp_path / 'src', tmp_path / 'dst'
    _write(src / 'reports' / 'r' / 'new.csv', 'payload')
    (dst / 'reports').mkdir(parents=True, exist_ok=True)

    result = mirror_subdir('reports', src_base=src, dst_base=dst, dry_run=True)

    assert result.copied == ['r/new.csv']
    assert not (dst / 'reports' / 'r' / 'new.csv').exists()


def test_plan_reports_both_directions(tmp_path: Path) -> None:
    src, dst = tmp_path / 'src', tmp_path / 'dst'
    _write(src / 'x' / 'only_src.txt', '1')
    _write(dst / 'x' / 'only_dst.txt', '2')

    missing, extra = plan(src, dst)

    assert missing == ['x/only_src.txt']
    assert extra == ['x/only_dst.txt']
    assert rel_files(src) == {'x/only_src.txt'}


def test_module_has_no_delete_calls() -> None:
    """静态守护：镜像模块不得出现任何删除实现。"""
    source = Path(__file__).resolve().parents[1] / 'tools' / 'mirror_backup.py'
    text = source.read_text(encoding='utf-8')
    for token in ('unlink(', 'rmtree(', 'os.remove(', '.remove(', 'git clean'):
        assert token not in text, f'mirror_backup.py 出现删除调用 {token}'
