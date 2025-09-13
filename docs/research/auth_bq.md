# BigQuery OAuth 認証と SQL 実行ツール

サクッと動く「最小構成」と、あとで拡張しやすい「分割版」を用意しました。どちらもブラウザで OAuth→SQLite にトークン保存 → プロジェクト選択 →SQL 実行まで通ります。

## クイックスタート（1 ファイル・最小構成）

### 0. 依存ライブラリのセットアップ

**uv 派：**

```bash
uv init wise-bq && cd wise-bq
uv add google-cloud-bigquery google-auth google-auth-oauthlib
```

**pip 派：**

```bash
python -m venv .venv && source .venv/bin/activate # Windows は .venv\Scripts\activate
pip install --upgrade google-cloud-bigquery google-auth google-auth-oauthlib
```

### 1. OAuth クライアントの準備

Google Cloud Console →「API とサービス」→「認証情報」→「認証情報を作成」→**OAuth クライアント ID（デスクトップアプリ）**を作成し、client_secrets.json をこのフォルダに保存。

### 2. 実行ファイル：wise_bq.py

```python
#!/usr/bin/env python
import argparse
import json
import sqlite3
from typing import Optional

from google.cloud import bigquery
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

TOK_DB = "tokens.db"
SCOPES = ["https://www.googleapis.com/auth/bigquery.readonly"]
CLIENT_SECRETS = "client_secrets.json"

def load_client_info():
    with open(CLIENT_SECRETS, "r", encoding="utf-8") as f:
        data = json.load(f)  # デスクトップアプリの client_secrets.json は "installed" キー配下
        return data["installed"]

def ensure_tokens_table():
    conn = sqlite3.connect(TOK_DB)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tokens (
            id INTEGER PRIMARY KEY,
            access_token TEXT,
            refresh_token TEXT,
            token_expiry TEXT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def load_credentials_from_sqlite() -> Optional[Credentials]:
    ensure_tokens_table()
    conn = sqlite3.connect(TOK_DB)
    cur = conn.cursor()
    cur.execute("SELECT access_token, refresh_token, token_expiry FROM tokens WHERE id=1")
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return None

    access_token, refresh_token, _expiry = row
    info = load_client_info()
    creds = Credentials(
        token=access_token,
        refresh_token=refresh_token,
        token_uri=info["token_uri"],
        client_id=info["client_id"],
        client_secret=info["client_secret"],
        scopes=SCOPES,
    )
    # 期限切れなら更新
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
    return creds

def save_credentials_to_sqlite(creds: Credentials):
    ensure_tokens_table()
    conn = sqlite3.connect(TOK_DB)
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO tokens (id, access_token, refresh_token, token_expiry)
        VALUES (1, ?, ?, ?)
    """, (creds.token, creds.refresh_token, creds.expiry.isoformat() if creds.expiry else None))
    conn.commit()
    cur.close()
    conn.close()

def run_oauth_flow() -> Credentials:
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS, scopes=SCOPES)  # refresh_token 確実取得
    creds = flow.run_local_server(
        port=0,
        prompt="consent",
        authorization_prompt_message="",
        access_type="offline",
    )
    save_credentials_to_sqlite(creds)
    return creds

def get_credentials() -> Credentials:
    creds = load_credentials_from_sqlite()
    if creds and creds.valid:
        return creds
    return run_oauth_flow()

def normalize_sql_argument(raw_sql: Optional[str]) -> Optional[str]:
    """シェルから渡された SQL 文字列の外側に余計な引用符が付いていた場合に除去する。
    例: '"SELECT 1"' → SELECT 1,  "'SELECT 1'" → SELECT 1
    """
    if raw_sql is None:
        return None
    s = raw_sql.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        s = s[1:-1].strip()
    return s


def select_project_interactively(creds: Credentials) -> str: # 課金先は未指定で OK（一覧取得のみ）
    client = bigquery.Client(project=None, credentials=creds)
    projects = list(client.list_projects())
    if not projects:
        raise RuntimeError(
            "到達可能な GCP プロジェクトが見つかりません。"
            "該当アカウントに BigQuery 権限（roles/bigquery.user など）が付与されているか確認してください。"
            )

    print("\n=== 利用可能なプロジェクト ===")
    for i, p in enumerate(projects, start=1):
        print(f"{i}. {p.project_id}")

    while True:
        choice = input("使用するプロジェクトを選んでください（番号 または project_id）: ").strip()
        if choice.isdigit():
            idx = int(choice)
            if 1 <= idx <= len(projects):
                return projects[idx - 1].project_id
            print("番号が範囲外です。")
        else:
            if any(p.project_id == choice for p in projects):
                return choice
            print("一覧にない project_id です。")

def main():
    parser = argparse.ArgumentParser(description="Wise BQ: OAuth→PJ 選択 →SQL 実行（READ）")
    parser.add_argument("--project", "-p", help="GCP プロジェクト ID（未指定なら一覧から選択）")
    parser.add_argument("--dataset", "-d", help="BigQuery データセット ID（任意）")
    parser.add_argument("--sql", "-q", help="実行する SQL。未指定なら簡単な案内を表示")
    parser.add_argument("--reauth", action="store_true", help="OAuth をやり直してスコープを再取得する")
    args = parser.parse_args()

    creds = run_oauth_flow() if args.reauth else get_credentials()

    # プロジェクト決定
    project_id = args.project or select_project_interactively(creds)
    print(f"\n選択されたプロジェクト: {project_id}")

    # BigQueryクライアント
    bq = bigquery.Client(project=project_id, credentials=creds)

    # データセット一覧の参考出力
    print(f"\n=== {project_id} のデータセット一覧 ===")
    any_ds = False
    for ds in bq.list_datasets(project=project_id):
        any_ds = True
        print(f"- {ds.dataset_id}")
    if not any_ds:
        print("(データセットが見つかりませんでした)")

    # SQL 実行
    if not args.sql:
        print("\n--sql でクエリを指定してください。例：")
        print(f"  python wise_bq.py -p {project_id} -q \"SELECT 1 AS x\"")
        return

    print("\n=== クエリ実行 ===")
    sql = normalize_sql_argument(args.sql)
    print(sql)
    job = bq.query(sql)
    rows = job.result(page_size=50)

    # 上位5行だけ表示
    print("\n=== 結果（上位5行） ===")
    for i, row in enumerate(rows, start=1):
        print(dict(row))
        if i >= 5:
            break

if __name__ == "__main__":
    main()
```

### 3. 使い方

```bash
# 初回はブラウザが開き、Google ログイン → 同意 →SQLite にトークン保存
python wise_bq.py

# 任意の SQL を実行
python wise_bq.py -q "SELECT 1 AS x"

# プロジェクトを指定（非対話）
python wise_bq.py -p my-gcp-project -q "SELECT 1 AS x"

# データセット名も渡したい場合（本サンプルでは一覧表示にのみ使用）
python wise_bq.py -p my-gcp-project -d my_dataset -q "SELECT COUNT(*) FROM \`my-gcp-project.my_dataset.my_table\`"
```

**注意**: 権限エラーが出る場合は、対象プロジェクトに roles/bigquery.user、参照データセットに roles/bigquery.dataViewer が付与されているかご確認ください。

---

## 拡張しやすい分割版（auth / select / run）

```
wise/
├─ auth.py # OAuth・SQLite 保存/復元
├─ select_project.py # プロジェクト一覧 → 選択
├─ run_sql.py # SQL 実行ユーティリティ
└─ main.py # CLI 入口
```

### auth.py

```python
import json
import sqlite3
from typing import Optional
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

TOK_DB = "tokens.db"
SCOPES = ["https://www.googleapis.com/auth/bigquery.readonly"]
CLIENT_SECRETS = "client_secrets.json"
```

```python
def _load_client_info():
    with open(CLIENT_SECRETS, "r", encoding="utf-8") as f:
        return json.load(f)["installed"]

def _ensure_table():
    conn = sqlite3.connect(TOK_DB)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tokens(
            id INTEGER PRIMARY KEY,
            access_token TEXT,
            refresh_token TEXT,
            token_expiry TEXT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
```

```python
def load_credentials() -> Optional[Credentials]:
    _ensure_table()
    conn = sqlite3.connect(TOK_DB)
    cur = conn.cursor()
    cur.execute("SELECT access_token, refresh_token, token_expiry FROM tokens WHERE id=1")
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return None

    access_token, refresh_token, _ = row
    info = _load_client_info()
    creds = Credentials(
        token=access_token,
        refresh_token=refresh_token,
        token_uri=info["token_uri"],
        client_id=info["client_id"],
        client_secret=info["client_secret"],
        scopes=SCOPES,
    )
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
    return creds
```

```python
def save_credentials(creds: Credentials):
    _ensure_table()
    conn = sqlite3.connect(TOK_DB)
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO tokens(id, access_token, refresh_token, token_expiry)
        VALUES (1, ?, ?, ?)
    """, (creds.token, creds.refresh_token, creds.expiry.isoformat() if creds.expiry else None))
    conn.commit()
    cur.close()
    conn.close()

def run_oauth_flow() -> Credentials:
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS, scopes=SCOPES)
    creds = flow.run_local_server(port=0, prompt="consent", authorization_prompt_message="", access_type="offline")
    save_credentials(creds)
    return creds
```

```python
def get_credentials() -> Credentials:
    creds = load_credentials()
    if creds and creds.valid:
        return creds
    return run_oauth_flow()
```

### select_project.py

```python
from google.cloud import bigquery
from google.oauth2.credentials import Credentials

def select_project_interactively(creds: Credentials) -> str:
    client = bigquery.Client(project=None, credentials=creds)
    projects = list(client.list_projects())
    if not projects:
        raise RuntimeError("到達可能な GCP プロジェクトがありません。権限をご確認ください。")

    print("\n=== 利用可能なプロジェクト ===")
    for i, p in enumerate(projects, start=1):
        print(f"{i}. {p.project_id}")

    while True:
        choice = input("使用するプロジェクトを選んでください（番号 または project_id）: ").strip()
        if choice.isdigit():
            idx = int(choice)
            if 1 <= idx <= len(projects):
                return projects[idx - 1].project_id
            print("番号が範囲外です。")
        else:
            if any(p.project_id == choice for p in projects):
                return choice
            print("一覧にない project_id です。")
```

### run_sql.py

```python
from google.cloud import bigquery
from google.oauth2.credentials import Credentials

def run_sql_readonly(project_id: str, sql: str, creds: Credentials, preview_rows: int = 5):
    client = bigquery.Client(project=project_id, credentials=creds)
    job = client.query(sql)
    rows = job.result()
    out = []
    for i, r in enumerate(rows, start=1):
        out.append(dict(r))
        if i >= preview_rows:
            break
    return out
```

### main.py

```python
import argparse
from auth import get_credentials,run_oauth_flow
from select_project import select_project_interactively
from run_sql import run_sql_readonly
from google.cloud import bigquery

def main():
    ap = argparse.ArgumentParser(description="Wise BQ (READ only)")
    ap.add_argument("-p", "--project", help="GCP project_id（未指定なら選択）")
    ap.add_argument("-d", "--dataset", help="dataset 名（任意：一覧表示用）")
    ap.add_argument("-q", "--sql", help="実行する SQL")
    ap.add_argument("--reauth", action="store_true", help="OAuth をやり直してスコープを再取得する")
    args = ap.parse_args()

    creds = run_oauth_flow() if args.reauth else get_credentials()
    project_id = args.project or select_project_interactively(creds)
    print(f"\n選択されたプロジェクト: {project_id}")

    # 参考：データセット一覧
    bq = bigquery.Client(project=project_id, credentials=creds)
    print(f"\n=== {project_id} のデータセット ===")
    any_ds = False
    for ds in bq.list_datasets(project=project_id):
        any_ds = True
        print(f"- {ds.dataset_id}")
    if not any_ds:
        print("(データセットなし)")

    if not args.sql:
        print("\n--sql でクエリを渡してください。例:")
        print(f"  python main.py -p {project_id} -q \"SELECT 1 AS x\"")
        return

    print("\n=== 実行SQL ===")
    sql = args.sql.strip()
    if len(sql) >= 2 and sql[0] == sql[-1] and sql[0] in ("'", '"'):
        sql = sql[1:-1].strip()
    print(sql)
    result = run_sql_readonly(project_id, sql, creds)
    print("\n=== 結果（上位5行） ===")
    for row in result:
        print(row)

if __name__ == "__main__":
    main()
```

### 実行例

```bash
python main.py # 初回はブラウザで認証 → PJ 選択
python main.py -p my-gcp-project -q "SELECT 1 AS x"
python main.py -p my-gcp-project -d my_dataset -q "SELECT COUNT(*) FROM \`my-gcp-project.my_dataset.my_table\`"
```

---

## トラブルシューティング

- **プロジェクトが一覧に出ない**: そのアカウントに対象プロジェクトで少なくとも roles/bigquery.user が必要です。参照データセットには roles/bigquery.dataViewer も。
- **組織ポリシーで制約**: プロジェクト列挙が Cloud Resource Manager API 側の制約に阻まれるケースがあります。必要なら追加スコープ https://www.googleapis.com/auth/cloud-platform.read-only を付けて実装します。
- **リフレッシュトークンが取れない**: `run_local_server(..., access_type="offline", prompt="consent")` を必ず指定。

## 拡張の可能性

必要なら、このまま wise コマンド化（pyproject.toml にエントリポイント追加）や、SQLite → Supabase への移行スクリプト例も提供できます。
