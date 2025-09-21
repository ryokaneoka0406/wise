## フェーズ 5: LLM エンジン

1. **共通ラッパー `llm/base.py`**
   - API キー設定、リトライ、プロンプト送信の共通処理を実装。

## status

ongoing

## Gemini API クイックスタート

このクイックスタートでは、[ライブラリ](https://ai.google.dev/gemini-api/docs/libraries?hl=ja)をインストールして、最初の Gemini API リクエストを行う方法をまとめています。

- 参考リンク:
  - [始める前に](https://ai.google.dev/gemini-api/docs/quickstart?hl=ja#before_you_begin)
  - [Google GenAI SDK をインストールする](https://ai.google.dev/gemini-api/docs/quickstart?hl=ja#install-gemini-library)
  - [最初のリクエストを送信する](https://ai.google.dev/gemini-api/docs/quickstart?hl=ja#make-first-request)
  - [思考（Thinking）について](https://ai.google.dev/gemini-api/docs/thinking?hl=ja)

### 始める前に

Gemini API キーが必要です。キーがない場合は、[Google AI Studio で無料で取得](https://aistudio.google.com/app/apikey?hl=ja)できます。

### SDK のインストール（Python／uv）

```bash
uv add google-genai
```

### 最初のリクエストを送信する（Python）

[`generateContent`](https://ai.google.dev/api/generate-content?hl=ja#method:-models.generatecontent) を使って、`gemini-2.5-flash` にテキストを送る最小例です。

- メモ: [API キー](https://ai.google.dev/gemini-api/docs/api-key?hl=ja#set-api-env-var)を環境変数 `GEMINI_API_KEY` に設定すると、Python クライアントが自動取得します。明示的に渡す方法は[こちら](https://ai.google.dev/gemini-api/docs/api-key?hl=ja#provide-api-key-explicitly)。

```python
from google import genai

# The client gets the API key from the environment variable `GEMINI_API_KEY`.
client = genai.Client()

response = client.models.generate_content(
    model="gemini-2.5-flash", contents="Explain how AI works in a few words"
)
print(response.text)
```

## 思考（Thinking）について

多くのコードサンプルでは [Gemini 2.5 Flash](https://ai.google.dev/gemini-api/docs/models?hl=ja#gemini-2.5-flash) を使用しており、回答品質向上のため「思考」機能がデフォルトで有効になっています（[解説](https://ai.google.dev/gemini-api/docs/thinking?hl=ja)）。応答時間やトークン使用量を抑えたい場合は、思考予算をゼロに設定して無効化できます（[設定方法](https://ai.google.dev/gemini-api/docs/thinking?hl=ja#set-budget)）。

**注**: 思考は Gemini 2.5 シリーズでのみ利用可能で、Gemini 2.5 Pro では無効化できません。

### Python（思考を無効化する例）

```python
from google import genai
from google.genai import types

client = genai.Client()

response = client.models.generate_content(
    model="gemini-2.5-flash",
    contents="Explain how AI works in a few words",
    config=types.GenerateContentConfig(
        thinking_config=types.ThinkingConfig(thinking_budget=0)  # Disables thinking
    ),
)
print(response.text)
```
