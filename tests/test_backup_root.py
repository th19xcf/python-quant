"""`tools/backup_root.py` 路径解析顺序与解析后校验的回归用例。"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from tools import backup_root

REMOTE_URL = 'https://github.com/th19xcf/python-quant.git'


def _make_repo(tmp_path: Path, *, with_git: bool = True, url: str = REMOTE_URL) -> Path:
    """造一个最小「备份仓」：有 .git，origin 指向预期仓库。"""
    repo = tmp_path / 'repo'
    repo.mkdir()
    if with_git:
        subprocess.run(['git', '-C', str(repo), 'init', '-q'], check=True)
        subprocess.run(['git', '-C', str(repo), 'remote', 'add', 'origin', url], check=True)
    return repo


def _isolate(monkeypatch: pytest.MonkeyPatch, config_path: Path) -> None:
    monkeypatch.delenv(backup_root.ENV_VAR, raising=False)
    monkeypatch.setattr(backup_root, 'CONFIG_PATH', config_path)


def test_env_var_wins_over_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo = _make_repo(tmp_path)
    config = tmp_path / 'backup.local.json'
    config.write_text(json.dumps({backup_root.CONFIG_KEY: 'X:/不该用到'}), encoding='utf-8')
    _isolate(monkeypatch, config)
    monkeypatch.setenv(backup_root.ENV_VAR, str(repo))

    root, source = backup_root.resolve_backup_root_with_source()

    assert root == repo.resolve()
    assert backup_root.ENV_VAR in source


def test_config_file_used_when_env_absent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo = _make_repo(tmp_path)
    config = tmp_path / 'backup.local.json'
    config.write_text(json.dumps({backup_root.CONFIG_KEY: str(repo)}), encoding='utf-8')
    _isolate(monkeypatch, config)

    assert backup_root.resolve_backup_root() == repo.resolve()


def test_no_source_raises_with_clear_message(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate(monkeypatch, tmp_path / 'missing.json')

    with pytest.raises(backup_root.BackupRootError) as excinfo:
        backup_root.resolve_backup_root()

    message = str(excinfo.value)
    assert backup_root.ENV_VAR in message
    assert backup_root.CONFIG_KEY in message


def test_missing_directory_is_not_created(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    target = tmp_path / 'not_here'
    _isolate(monkeypatch, tmp_path / 'missing.json')
    monkeypatch.setenv(backup_root.ENV_VAR, str(target))

    with pytest.raises(backup_root.BackupRootError, match='不存在'):
        backup_root.resolve_backup_root()

    assert not target.exists(), '解析失败不得自动创建目录'


def test_directory_without_git_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo = _make_repo(tmp_path, with_git=False)
    _isolate(monkeypatch, tmp_path / 'missing.json')
    monkeypatch.setenv(backup_root.ENV_VAR, str(repo))

    with pytest.raises(backup_root.BackupRootError, match=r'\.git'):
        backup_root.resolve_backup_root()


def test_wrong_origin_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo = _make_repo(tmp_path, url='https://github.com/someone/else.git')
    _isolate(monkeypatch, tmp_path / 'missing.json')
    monkeypatch.setenv(backup_root.ENV_VAR, str(repo))

    with pytest.raises(backup_root.BackupRootError, match='origin'):
        backup_root.resolve_backup_root()


def test_cli_quiet_prints_resolved_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = _make_repo(tmp_path)
    _isolate(monkeypatch, tmp_path / 'missing.json')
    monkeypatch.setenv(backup_root.ENV_VAR, str(repo))

    assert backup_root.main(['--quiet']) == 0

    assert capsys.readouterr().out.strip() == str(repo.resolve())
