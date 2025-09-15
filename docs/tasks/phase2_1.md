## フェーズ 2: 認証と BigQuery 接続

## 目的

**CLI セットアップウィザードの実装**

- 初回起動時に Google 認証を行い、リフレッシュトークンを保存するフローを作成。

## ステータス

ongoing

## 実施内容（2025-09-14）

- 初回起動時のセットアップウィザードを実装。
  - ブラウザでの Google OAuth を実行し、取得したリフレッシュトークンと email を DB に保存。
  - 実装箇所: `wise/auth/google.py`（OAuth 実装）、`wise/chat/session.py` の `_run_setup_wizard()`（OAuth 呼び出し）。
- セットアップ完了後のチャットへの移行処理を実装。
  - アカウント確認→なければウィザード→`sessions` 作成→チャットループ開始（メッセージは DB 保存）。
  - 実装箇所: `wise/chat/session.py: start_session()`。
- CLI 入口をチャットセッションに接続。
  - 実装箇所: `wise/cli.py`（DB 初期化→`start_session()` を呼び出し）。
- 再ログインコマンドを OAuth 化。
  - `/login` / `/reauth` でブラウザ認証し、トークンを保存。
  - 実装箇所: `wise/chat/commands.py`。

## メモ

- OAuth 実装完了。BigQuery 呼び出しは未配線（次フェーズで実装）。
- DB スキーマは設計書（`docs/designdoc.md`）準拠（`accounts/sessions/messages`）。
- Google のトークンレスポンスでは `email/profile` が `userinfo.email/userinfo.profile` に変換されるため、スコープは `openid userinfo.email userinfo.profile bigquery.readonly` を要求するように調整済み。

## 次アクション候補

- OAuth フローの実装（ブラウザ起動→リフレッシュトークン取得→`accounts` 保存）。
- `/login` に OAuth 起動を統合し、暫定入力を置き換え。

## 動作確認方法（現時点）

- 依存のインストール（開発ローカル）
  - `pip install -e .` もしくは `uv pip install -e .`
- Google OAuth クライアントの配置
  - プロジェクト直下に `cred.json` を配置（もしくは環境変数 `WISE_GOOGLE_CLIENT_SECRETS` にパスを設定）。
- CLI の起動
  - `wise` を実行（または `python -m wise.cli`）
- 初回セットアップ
  - ブラウザが自動起動し Google ログイン → 同意。
  - リフレッシュトークンと email が `wise.db` の `accounts` に保存されます。
- チャットの利用
  - 任意のテキストを入力するとエコー応答が返り、履歴は DB に保存されます。
  - 再ログインは `/login` または `/reauth` でブラウザが起動し再認可します。
  - 終了は `exit` または `quit`。

注意: BigQuery API 呼び出しは未配線です（次ステップ）。仕様は `docs/research/auth_bq.md` を参照してください。
- BQ クライアントの実配線（`wise/bq/client.py` 置換）。

## 提案（要確認）

- リフレッシュトークンは将来的に複数アカウント対応を想定（現状は最新の有効トークンを自動選択）。必要なら「アカウント切替」コマンド（例: `\account switch`）を追加したい。
- OAuth 実装時に `cloud-platform.read-only` スコープ付与の検討（プロジェクト列挙のため）。
