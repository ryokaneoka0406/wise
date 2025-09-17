"""Render and persist BigQuery metadata snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from ..datastore import files as datastore_files

__all__ = ["MetadataWriteResult", "render_metadata", "save_metadata"]


@dataclass(frozen=True)
class MetadataWriteResult:
    """Result of writing a metadata document to disk."""

    path: Path
    backup_path: Path | None = None


def _current_timestamp() -> str:
    """Return an ISO8601 UTC timestamp without microseconds."""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _escape_table_cell(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    return text.replace("|", "\\|").replace("\n", "<br>")


def _render_schema(fields: Iterable[dict[str, Any]]) -> list[str]:
    lines = ["", "#### フィールド定義", ""]
    field_list = list(fields or [])
    if not field_list:
        lines.append("_スキーマ情報が利用できません。_")
        lines.append("")
        return lines

    lines.append("| 名前 | 型 | モード | 説明 |")
    lines.append("| --- | --- | --- | --- |")
    for field in field_list:
        lines.append(
            "| {name} | {type} | {mode} | {description} |".format(
                name=_escape_table_cell(field.get("name")),
                type=_escape_table_cell(field.get("type")),
                mode=_escape_table_cell(field.get("mode")),
                description=_escape_table_cell(field.get("description")),
            )
        )
    lines.append("")
    return lines


def _schema_column_names(fields: Iterable[dict[str, Any]]) -> list[str]:
    names = [f.get("name") for f in fields if f.get("name")]
    return [str(name) for name in names if name]


def _render_sample_rows(fields: Iterable[dict[str, Any]], rows: Iterable[dict[str, Any]]) -> list[str]:
    lines = ["", "#### サンプル行", ""]
    row_list = list(rows or [])
    if not row_list:
        lines.append("_サンプル行は取得できませんでした。_")
        lines.append("")
        return lines

    columns = _schema_column_names(fields)
    if not columns:
        keys = set()
        for row in row_list:
            keys.update(key for key in row.keys() if key)
        columns = sorted(keys)

    if not columns:
        lines.append("_サンプル行を表形式で表示できません。_")
        lines.append("")
        return lines

    header = "| " + " | ".join(_escape_table_cell(col) for col in columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    lines.extend([header, separator])
    for row in row_list:
        values = [_escape_table_cell(row.get(col)) for col in columns]
        lines.append("| " + " | ".join(values) + " |")
    lines.append("")
    return lines


def _render_dataset(dataset_id: str, dataset_entry: dict[str, Any]) -> list[str]:
    lines = ["", f"## データセット `{dataset_id}`", ""]
    tables = dataset_entry.get("tables") or {}
    if not tables:
        lines.append("_テーブルが存在しません。_")
        lines.append("")
        return lines

    for table_id, table_entry in sorted(tables.items()):
        lines.extend(
            [
                "",
                f"### テーブル `{dataset_id}.{table_id}`",
            ]
        )
        schema = table_entry.get("schema") or []
        samples = table_entry.get("sampleRows") or []
        lines.extend(_render_schema(schema))
        lines.extend(_render_sample_rows(schema, samples))
    return lines


def render_metadata(snapshot: dict[str, Any]) -> str:
    """Render a Markdown document from ``metadata_snapshot`` output."""

    if not snapshot:
        raise ValueError("snapshot が空です。")
    project_id = snapshot.get("projectId")
    if not project_id:
        raise ValueError("snapshot には projectId が必要です。")

    location = snapshot.get("location") or "未指定"
    datasets = snapshot.get("datasets") or {}
    dataset_items = sorted(datasets.items(), key=lambda item: item[0])
    generated_at = _current_timestamp()

    lines: list[str] = [f"# BigQuery メタデータ: `{project_id}`", "", "## プロジェクト概要", ""]
    lines.append(f"- プロジェクト ID: `{project_id}`")
    lines.append(f"- ロケーション: `{location}`")
    lines.append(f"- データセット数: {len(dataset_items)}")
    lines.append(f"- 生成日時 (UTC): {generated_at}")
    lines.append("")
    lines.append("## 対象データセット一覧")
    lines.append("")

    if dataset_items:
        lines.append("| データセット ID | テーブル数 |")
        lines.append("| --- | --- |")
        for dataset_id, dataset_entry in dataset_items:
            tables = dataset_entry.get("tables") or {}
            lines.append(f"| `{dataset_id}` | {len(tables)} |")
    else:
        lines.append("_データセットが見つかりませんでした。_")
    lines.append("")

    for dataset_id, dataset_entry in dataset_items:
        lines.extend(_render_dataset(dataset_id, dataset_entry))

    return "\n".join(lines).rstrip() + "\n"


def save_metadata(
    snapshot: dict[str, Any],
    *,
    base_dir: Path | str | None = None,
    backup: bool = True,
) -> MetadataWriteResult:
    """Persist rendered metadata to ``project/{project_id}/metadata.md``."""

    content = render_metadata(snapshot)
    project_id = snapshot.get("projectId")
    if not project_id:
        raise ValueError("snapshot には projectId が必要です。")
    path = datastore_files.metadata_path(str(project_id), base_dir=base_dir)
    backup_path: Path | None = None
    if backup and path.exists():
        backup_path = datastore_files.create_backup(path)
    datastore_files.write_text(path, content)
    return MetadataWriteResult(path=path, backup_path=backup_path)
