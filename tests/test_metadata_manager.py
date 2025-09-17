from pathlib import Path
import textwrap

import pytest

from wise.metadata import manager


@pytest.fixture()
def sample_snapshot():
    return {
        "projectId": "demo",
        "location": "US",
        "datasets": {
            "sales": {
                "tables": {
                    "orders": {
                        "schema": [
                            {
                                "name": "order_id",
                                "type": "STRING",
                                "mode": "REQUIRED",
                                "description": "注文ID",
                            },
                            {
                                "name": "amount",
                                "type": "INTEGER",
                                "mode": "NULLABLE",
                                "description": "",
                            },
                        ],
                        "sampleRows": [
                            {"order_id": "A-001", "amount": 100},
                            {"order_id": "A-002", "amount": 200},
                        ],
                    }
                }
            }
        },
    }


def test_render_metadata_snapshot(monkeypatch, sample_snapshot):
    monkeypatch.setattr(manager, "_current_timestamp", lambda: "2024-01-01T00:00:00Z")
    markdown = manager.render_metadata(sample_snapshot)
    expected = textwrap.dedent(
        """
        # BigQuery メタデータ: `demo`

        ## プロジェクト概要

        - プロジェクト ID: `demo`
        - ロケーション: `US`
        - データセット数: 1
        - 生成日時 (UTC): 2024-01-01T00:00:00Z

        ## 対象データセット一覧

        | データセット ID | テーブル数 |
        | --- | --- |
        | `sales` | 1 |

        
        ## データセット `sales`

        
        ### テーブル `sales.orders`

        #### フィールド定義

        | 名前 | 型 | モード | 説明 |
        | --- | --- | --- | --- |
        | order_id | STRING | REQUIRED | 注文ID |
        | amount | INTEGER | NULLABLE |  |

        
        #### サンプル行

        | order_id | amount |
        | --- | --- |
        | A-001 | 100 |
        | A-002 | 200 |
        """
    ).strip()
    assert markdown.strip() == expected


def test_save_metadata_creates_backup(tmp_path: Path, monkeypatch, sample_snapshot):
    monkeypatch.setattr(manager, "_current_timestamp", lambda: "2024-01-01T00:00:00Z")
    result = manager.save_metadata(sample_snapshot, base_dir=tmp_path)
    metadata_path = tmp_path / "project" / "demo" / "metadata.md"
    assert result.path == metadata_path
    assert metadata_path.exists()
    assert result.backup_path is None

    metadata_path.write_text("previous content", encoding="utf-8")
    monkeypatch.setattr(manager, "_current_timestamp", lambda: "2024-01-02T00:00:00Z")
    monkeypatch.setattr(manager.datastore_files, "_timestamp_for_backup", lambda: "20240102T000000")

    result2 = manager.save_metadata(sample_snapshot, base_dir=tmp_path)
    assert result2.backup_path == metadata_path.with_name("metadata.md.bak.20240102T000000")
    assert result2.backup_path.read_text(encoding="utf-8") == "previous content"
    assert metadata_path.read_text(encoding="utf-8").startswith("# BigQuery メタデータ")
