# システム全体設計

## 目的

`wise` は CLI ベースのデータ分析エージェントであり、自然言語による対話を通じて BigQuery のクエリ生成、実行、可視化、レポート作成を自動化する。

## アーキテクチャ概要

```
┌───────────┐       ┌─────────────┐       ┌──────────┐
 │ CLI / Chat│──────▶│  LLM Engine │──────▶│BigQuery  │
└────┬───────┘       └─────┬───────┘       └────┬─────┘
     │                      │                   │
     │                      │                   │
     ▼                      ▼                   ▼
Setup Wizard          Metadata Manager      Data Store
     │                      │                   │
     ▼                      ▼                   ▼
Login/OAuth            SQL Generator         File Output
```

各コンポーネントは次の役割を持つ：

- **CLI / Chat インターフェース**: ユーザーからの自然言語入力を受け取り、指示結果を表示する。
- **LLM Engine**: 入力とメタデータを元に SQL を生成し、ユーザーに提示する。分析や可視化向けのプロンプトを扱うモジュールに分割される。
- **BigQuery API クライアント**: 認証済みアカウントで SQL を実行し、結果を取得する。
- **Metadata Manager**: BigQuery のテーブル構造やサンプル値を読み取り、`data.md` として保存する。
- **Data Store**: クエリ結果やメタデータ、可視化画像をフォルダに保存する。
- **Analysis Processor**: 保存済み CSV を読み込み、LLM を介して示唆を Markdown に出力する。
- **Visualization Engine**: 指示に応じてグラフを描画し、画像として保存する。

## 起動と認証フロー

1. パッケージマネージャーでインストール後、`wise` コマンドを実行するとウェルカムメッセージと共にチャットを開始する。
2. 初回実行時にはセットアップウィザードが起動し、CLI から Google 認証とアカウント作成を行う。
3. BigQuery のプロジェクトとデータセットを選択し、読み込み権限を付与する。
4. 再ログインは `\login` または `wise login` を利用する。

## メタデータ生成

- `\init` または `wise init` を実行すると、指定データセットのテーブル構造を取得し、カラム情報やリレーションを含むメタデータを `data.md` として生成する。
- メタデータはユーザーが編集可能であり、列名とサンプル値の両方を保持する。

## クエリ生成と実行

1. ユーザーがチャット欄に自然言語で質問を入力すると、LLM Engine がメタデータを参照して SQL を生成する。
2. 生成された SQL はユーザーに提示され、即時実行するかフィードバックするかを選択できる。
3. SQL 実行後、上位 5 行（10 行未満なら全行）を CLI に表示する。
4. CSV 書き出しを提案し、承諾された場合は目的を要約したフォルダを作成し、CSV とメタデータを保存する。

## 分析モード

- `@` を用いてフォルダを指定し、追加指示を与えることで分析モードに入る。
- LLM はデータを読み込み、示唆や考察をマークダウンとして同フォルダに出力する。

## 可視化モード

- `@` を用いて対象フォルダを指定し、可視化の指示を行う。
- 生成されたグラフ画像は同フォルダに保存される。

## プログラムファイル構造

```
wise/
 ├─ __init__.py          # パッケージ初期化
 ├─ cli.py               # CLI エントリーポイント
 ├─ chat/                # チャットセッション管理
 │   ├─ session.py       # ユーザーとの対話ループと会話ログ保存
 │   └─ commands.py      # `\login` や `\init` などのコマンド実装
 ├─ llm/
 │   ├─ base.py          # LLM 共通ラッパー
 │   ├─ sql.py           # SQL 生成プロンプト
 │   ├─ analysis.py      # データ分析プロンプト
 │   └─ visualize.py     # 可視化指示プロンプト
 ├─ analysis/
 │   └─ runner.py        # LLM を用いた分析の実行
 ├─ visualization/
 │   └─ plot.py          # グラフ生成ロジック
 ├─ bq/
 │   └─ client.py        # BigQuery API クライアント
 ├─ metadata/
 │   └─ manager.py       # メタデータ生成・更新ロジック
 ├─ datastore/
 │   └─ files.py         # 結果ファイルの保存・読み込み
 └─ db/
     └─ models.py        # 内部 DB へのアクセスラッパー
```

## 内部 DB 構造

認証情報やチャット履歴、分析履歴を保持するために SQLite を利用し、`wise.db` に次のテーブルを持つ。

- `accounts`
  - `id`: 主キー
  - `email`: Google アカウント
  - `refresh_token`: OAuth リフレッシュトークン
- `datasets`
  - `id`: 主キー
  - `project`: GCP プロジェクト ID
  - `dataset`: BigQuery データセット名
- `analyses`
  - `id`: 主キー
  - `dataset_id`: `datasets` への外部キー
  - `summary`: フォルダ名の元となる要約
  - `created_at`: 作成日時
- `queries`
  - `id`: 主キー
  - `analysis_id`: `analyses` への外部キー
  - `sql`: 実行した SQL
  - `executed_at`: 実行日時
- `sessions`
  - `id`: 主キー
  - `account_id`: `accounts` への外部キー
  - `started_at`: セッション開始日時
- `messages`
  - `id`: 主キー
  - `session_id`: `sessions` への外部キー
  - `role`: `user`/`assistant`/`system`
  - `content`: メッセージ本文
  - `created_at`: 送信日時

## データ保存構造

```
project/
 └─ analyses/
    └─ <summary>/
       ├─ result.csv
       ├─ metadata.md
       ├─ analysis.md
       └─ visualization.png
```

## 将来の拡張

- 追加のデータソースや BI ツール連携
- 分析テンプレートの共有機能
- 複数ユーザーでのコラボレーション

