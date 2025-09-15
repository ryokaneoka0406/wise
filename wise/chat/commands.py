"""Command handlers for chat-mode backslash commands.

Currently supports:
- ``\login``: Re-run minimal setup to update the refresh token
"""

from __future__ import annotations

from typing import Tuple

from ..auth import run_oauth_and_save_account


def _login() -> str:
    print("\n=== Re-login ===")
    print("ブラウザで Google 認証を実行し、リフレッシュトークンを更新します。")
    _ = run_oauth_and_save_account()
    return "認証が完了し、トークンを保存しました。"


def handle_command(command: str) -> Tuple[bool, str | None]:
    """Handle a backslash command.

    Returns (handled, reply). If not handled, (False, None).
    """
    cmd = command.strip()
    # Normalize leading prefix to support both "/" and "\\"
    if cmd.startswith("/"):
        canonical = "\\" + cmd[1:]
    else:
        canonical = cmd
    if canonical in {"\\login", "\\reauth"}:
        return True, _login()
    return False, None
