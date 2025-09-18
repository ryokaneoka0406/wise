"""Command handlers for chat-mode slash commands.

Currently supports:
- ``/login``: Re-run minimal setup to update the refresh token
- ``/init``: Generate BigQuery metadata markdown via the metadata manager
"""

from __future__ import annotations

from typing import Tuple

from ..auth import run_oauth_and_save_account
from ..bq import client as bq_client
from ..db import models
from ..metadata import manager as metadata_manager


def _prompt_user(prompt: str) -> str:
    return input(prompt)


def _active_account_id() -> int | None:
    for row in models.list_accounts():
        if row["refresh_token"]:
            return int(row["id"])
    return None


def _project_id_from(project: dict[str, object]) -> str:
    reference = project.get("projectReference") if isinstance(project, dict) else None
    if isinstance(reference, dict):
        ref_id = reference.get("projectId")
        if isinstance(ref_id, str) and ref_id:
            return ref_id
    for key in ("projectId", "id"):
        value = project.get(key) if isinstance(project, dict) else None
        if isinstance(value, str) and value:
            return value
    return ""


def _project_label(project: dict[str, object]) -> str:
    project_id = _project_id_from(project)
    friendly = project.get("friendlyName") if isinstance(project, dict) else None
    if isinstance(friendly, str) and friendly and friendly != project_id:
        return f"{project_id} ({friendly})"
    return project_id or "<unknown>"


def _init(sample_n: int = 3) -> str:
    print("\n=== Metadata Initialization ===")
    account_id = _active_account_id()
    if account_id is None:
        return "BigQuery へアクセスするには先に /login を実行してください。"

    print("利用可能な BigQuery プロジェクトを取得しています...")
    try:
        projects = bq_client.list_projects(account_id=account_id)
    except bq_client.BigQueryClientError as exc:
        return f"プロジェクト一覧の取得に失敗しました: {exc}"

    available = [p for p in projects if _project_id_from(p)]
    if not available:
        return "利用可能なプロジェクトが見つかりませんでした。"

    print(f"{len(available)} 件のプロジェクトが見つかりました。対象を選択してください (空行でキャンセル)。")
    for idx, project in enumerate(available, start=1):
        print(f"  [{idx}] {_project_label(project)}")

    selected: dict[str, object] | None = None
    while selected is None:
        try:
            raw = _prompt_user("project> ").strip()
        except KeyboardInterrupt:
            print()
            return "プロジェクト選択を中断しました。"
        if not raw:
            return "プロジェクト選択をキャンセルしました。"
        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(available):
                selected = available[idx - 1]
                break
        else:
            for candidate in available:
                if _project_id_from(candidate) == raw:
                    selected = candidate
                    break
        if selected is None:
            print("有効な番号または projectId を入力してください。")

    project_id = _project_id_from(selected)
    print(f"プロジェクト `{project_id}` を選択しました。データセットを確認しています...")

    try:
        client = bq_client.BQClient(project_id=project_id, account_id=account_id)
        dataset_ids = client.list_datasets()
    except bq_client.BigQueryClientError as exc:
        return f"データセットの取得に失敗しました: {exc}"

    if dataset_ids:
        print(f"{len(dataset_ids)} 件のデータセットが見つかりました:")
        for dataset in dataset_ids:
            print(f"  - {dataset}")
        try:
            confirmation = _prompt_user("Enter で続行 (キャンセルするには 'q'): ").strip().lower()
        except KeyboardInterrupt:
            print()
            return "メタデータ生成を中断しました。"
        if confirmation == "q":
            return "メタデータ生成をキャンセルしました。"
    else:
        print("対象データセットが見つかりませんでした。空のメタデータを生成します。")

    print("BigQuery からメタデータを収集中です...")
    try:
        snapshot = bq_client.metadata_snapshot(
            project_id=project_id,
            account_id=account_id,
            location=client.location,
            datasets=list(dataset_ids),
            sample_n=sample_n,
        )
    except bq_client.BigQueryClientError as exc:
        return f"メタデータの取得に失敗しました: {exc}"

    try:
        result = metadata_manager.save_metadata(snapshot)
    except Exception as exc:  # pragma: no cover - unexpected filesystem issues
        return f"メタデータの保存に失敗しました: {exc}"

    message = f"メタデータを生成し {result.path} に保存しました。"
    if result.backup_path:
        message += f" 既存ファイルは {result.backup_path} にバックアップしました。"
    return message


def _login() -> str:
    print("\n=== Re-login ===")
    print("ブラウザで Google 認証を実行し、リフレッシュトークンを更新します。")
    _ = run_oauth_and_save_account()
    return "認証が完了し、トークンを保存しました。"


def handle_command(command: str) -> Tuple[bool, str | None]:
    """Handle a slash command.

    Returns (handled, reply). If not handled, (False, None).
    """
    cmd = command.strip()
    if cmd in {"/login", "/reauth"}:
        return True, _login()
    if cmd == "/init":
        return True, _init()
    return False, None
