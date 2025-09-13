## フェーズ 0: 調査・共通基盤

## 目的

**BigQuery API の利用方法調査**

- 認証フロー、必要スコープ、Python クライアントライブラリの動作を確認する。

## ステータス

✅ Completed

## 調査結果

`docs/research/auth_bq.md` に格納

## 評価

- **結論**: `docs/designdoc.md` に記載の BigQuery 認証要件は、`docs/research/auth_bq.md` のサンプル（最小構成/分割版）で十分に実現可能（READ only 前提）。
- **根拠**
  - OAuth デスクトップフローでの取得/更新（`google-auth-oauthlib`、リフレッシュ対応）と、SQLite への永続化を実装済み。
  - スコープは `https://www.googleapis.com/auth/bigquery.readonly`。必要に応じて `https://www.googleapis.com/auth/cloud-platform.read-only` を追加すればプロジェクト列挙制約にも対応可能。
  - 認証済みでのプロジェクト選択、データセット一覧取得、SQL 実行ユーティリティを提供しており、CLI からの対話・非対話実行が可能。
- **差分/今後の組み込み**
  - トークン保存先を `tokens.db` から設計書の内部 DB（`wise.db` の `accounts` 等）へ統合。
  - CLI エントリポイント化（`pyproject.toml` のエントリポイント登録）。
  - 権限ロールの明示（対象プロジェクトに `roles/bigquery.user`、参照データセットに `roles/bigquery.dataViewer`）。

上記によりフェーズ 0 の目的（認証フロー/スコープ/動作確認）は達成したため、本タスクは completed とする。
