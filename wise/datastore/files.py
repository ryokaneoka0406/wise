"""Utilities for writing artifacts to the local datastore."""

from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union

PROJECT_ROOT_DIRNAME = "project"
PathLike = Union[str, Path]


def _base_dir(base_dir: Optional[PathLike] = None) -> Path:
    """Return the base directory used for storing project artifacts."""

    if base_dir is not None:
        return Path(base_dir)
    return Path.cwd()


def ensure_directory(path: Path) -> Path:
    """Create ``path`` (and parents) when it does not yet exist."""

    path.mkdir(parents=True, exist_ok=True)
    return path


def project_root(project_id: str, *, base_dir: Optional[PathLike] = None) -> Path:
    """Return the directory that stores outputs for ``project_id``."""

    if not project_id:
        raise ValueError("project_id は必須です。")
    return _base_dir(base_dir) / PROJECT_ROOT_DIRNAME / project_id


def metadata_path(project_id: str, *, base_dir: Optional[PathLike] = None) -> Path:
    """Return the metadata.md path for ``project_id``."""

    return project_root(project_id, base_dir=base_dir) / "metadata.md"


def write_text(path: PathLike, content: str) -> Path:
    """Write ``content`` to ``path`` ensuring parent directories exist."""

    target = Path(path)
    ensure_directory(target.parent)
    target.write_text(content, encoding="utf-8")
    return target


def _timestamp_for_backup() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def create_backup(path: PathLike) -> Path:
    """Create a timestamped backup copy of ``path`` and return it."""

    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(f"バックアップ対象が存在しません: {source}")
    backup = source.with_name(f"{source.name}.bak.{_timestamp_for_backup()}")
    ensure_directory(backup.parent)
    shutil.copy2(source, backup)
    return backup


def save(path: PathLike, content: str) -> Path:
    """Compatibility wrapper around :func:`write_text`."""

    return write_text(path, content)
