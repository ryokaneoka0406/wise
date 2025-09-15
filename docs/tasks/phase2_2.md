## フェーズ 2: 認証と BigQuery 接続

## 目的

**BigQuery クライアントの作成**

- 認証情報から BigQuery API を呼び出すラッパー `bq/client.py` を実装。

## ステータス

completed

## 実装計画

### 方針

- BQ との疎通（メタデータ取得と SQL 実行）を単一のクライアント `wise/bq/client.py` に集約する。
- 認証は `/login` で取得・保存した `refresh_token` と `cred.json` の `client_id/client_secret` を使い、`google-auth` でアクセストークンを都度リフレッシュ。
- ライブラリは `google-cloud-bigquery` は使わず、REST + `AuthorizedSession` で実装（既存依存のまま動作）。

### 提供 I/F（例）

- クラス: `BQClient(project_id: str, account_id: Optional[int] = None, location: str = "US")`

  - `list_datasets() -> list[str]`
  - `list_tables(dataset_id: str) -> list[str]`
  - `get_table_schema(dataset_id: str, table_id: str) -> list[dict]`
  - `sample_rows(dataset_id: str, table_id: str, max_results: int = 5) -> list[dict]`
  - `run_sql(sql: str, *, max_results: int = 1000, dry_run: bool = False, fetch_all: bool = True) -> dict`
    - 返り値: `{ schema: [fields], rows: [dict], totalRows: int, jobId: str }`

- 便宜関数:
  - `query(sql: str, *, project_id: str, account_id: Optional[int] = None, ...) -> list[dict]`
  - `metadata_snapshot(project_id: str, *, datasets: Optional[list[str]] = None, sample_n: int = 3) -> dict`

### 使用エンドポイント（REST）

- `GET  https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets`
- `GET  https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets/{dataset}/tables`
- `GET  https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets/{dataset}/tables/{table}`（schema）
- `GET  https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets/{dataset}/tables/{table}/data`（サンプル）
- `POST https://bigquery.googleapis.com/bigquery/v2/projects/{project}/queries`（jobs.query）
- `GET  https://bigquery.googleapis.com/bigquery/v2/projects/{project}/queries/{jobId}`（ページング）

### 認証

- DB（`accounts`）に保存済みの `refresh_token` を取得。
- `cred.json`（または `WISE_GOOGLE_CLIENT_SECRETS`）から `client_id/client_secret/token_uri` を読み取り、`Credentials(...).refresh(Request())` でアクセストークン取得。
- `AuthorizedSession` を再利用して API 呼び出し。

### `metadata/` の責務

（今回の実装では触らないが、今後を視野に入れた責務についての記述）

- `metadata_snapshot(...)` で取得したスナップショット（プロジェクト/データセット/テーブル/スキーマ/サンプル行）を受け取り、`metadata.md` にレンダリング・保存する（`metadata/manager.py`）。
- 再実行時はファイルを上書き（後続で差分マージの検討余地あり）。

### 受け入れ基準（最低限）

- `/login` 済みの環境で `BQClient(project_id).list_datasets()` が 200 レスポンスとなり dataset 一覧が取得できる。
- `BQClient(project_id).run_sql("SELECT 1 AS x")` が `rows=[{"x": "1"}]` のように行データを返す。
- `metadata_snapshot(project_id, sample_n=3)` がスキーマとサンプル行を含む辞書を返す。

### 動作確認手順（例）

1. `wise login`（もしくは `/login`）で Google 認証を完了させ、`accounts.refresh_token` を保存。
2. Python REPL などで次を実行:
   - `from wise.bq.client import BQClient; BQClient(project_id="<PJ>").list_datasets()`
   - `BQClient(project_id="<PJ>").run_sql("SELECT 1 AS x")`
   - `from wise.metadata.manager import generate_and_save; generate_and_save(project_id="<PJ>")`

### 備考

- 依存追加は不要（`google-auth`, `google-auth-oauthlib` のみ）。
- 複雑なネストスキーマ（RECORD/REPEATED）は段階的に対応（まずはフラット型をサポート）。
