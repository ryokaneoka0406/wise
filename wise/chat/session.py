"""Chat session and first-time setup wizard.

Implements:
- First-time refresh token save (temporary manual input, until OAuth wiring)
- Transition into a simple chat loop that persists messages to SQLite
"""

from __future__ import annotations

from typing import Optional

from ..db import models
from ..auth import run_oauth_and_save_account
from .commands import handle_command


def _select_active_account_id() -> Optional[int]:
    """Return an existing account id that has a refresh token, if any."""
    accounts = models.list_accounts()
    for row in accounts:
        if row["refresh_token"]:
            return int(row["id"])
    return None


def _run_setup_wizard() -> int:
    """Run OAuth-based setup to store refresh token and email."""
    print("\n=== Setup Wizard ===")
    print("ブラウザで Google 認証を実行し、リフレッシュトークンを保存します。")
    account_id = run_oauth_and_save_account()
    print("保存しました。以降のチャットに移行します。\n")
    return account_id


def start_session() -> None:
    """Start a chat session after ensuring an account exists with a token."""
    account_id = _select_active_account_id()
    if account_id is None:
        account_id = _run_setup_wizard()

    # Start DB-backed session
    session_id = models.create_session(account_id)
    print("assistant> セッションを開始しました。'exit' で終了します。")

    # Simple chat loop: save messages, echo response
    while True:
        try:
            user_input = input("you> ")
        except EOFError:
            print("\nassistant> Goodbye!")
            break
        text = user_input.strip()
        if not text:
            continue
        if text.lower() in {"exit", "quit"}:
            print("assistant> Goodbye!")
            break
        if text.startswith("/"):
            # Commands like /login handled here
            handled, reply = handle_command(text)
            if handled:
                if reply:
                    print(f"assistant> {reply}")
                # Special case: if login updated token, keep chatting
                continue
            # Fall-through if not handled: treat as normal text

        # Persist user message
        models.add_message(session_id, role="user", content=text)

        # For now, echo as assistant and persist
        assistant_reply = f"Echo: {text}"
        print(f"assistant> {assistant_reply}")
        models.add_message(session_id, role="assistant", content=assistant_reply)
