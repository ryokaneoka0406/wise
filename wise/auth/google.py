"""Google OAuth flow and account persistence.

This module runs the InstalledApp OAuth flow in a local browser, obtains
offline refresh tokens, fetches the user's email via OIDC userinfo, and
persists the refresh token into the internal SQLite DB (``accounts`` table).

Requirements (declared in pyproject):
  - google-auth
  - google-auth-oauthlib
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from google.auth.transport.requests import Request, AuthorizedSession
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from ..db import models


# Scopes needed now: BigQuery read for future calls, and OIDC userinfo to get email
SCOPES = [
    # OIDC + explicit userinfo scopes to avoid oauthlib scope-mismatch warnings
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    # BigQuery read-only for future API calls
    "https://www.googleapis.com/auth/bigquery.readonly",
]


def _client_secrets_path() -> Path:
    """Resolve client secrets JSON path.

    Precedence:
      1) Env var WISE_GOOGLE_CLIENT_SECRETS
      2) ./cred.json (project root)
      3) ./client_secrets.json (alternative common name)
    """
    env = os.getenv("WISE_GOOGLE_CLIENT_SECRETS")
    if env:
        return Path(env)
    cwd = Path.cwd()
    for name in ("cred.json", "client_secrets.json"):
        p = cwd / name
        if p.exists():
            return p
    # Default fallback (may not exist; flow will error with clear message)
    return cwd / "cred.json"


def _load_client_info(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    # Support both web/installed formats; prefer "installed" for desktop apps
    if "installed" in data:
        return data["installed"]
    if "web" in data:
        return data["web"]
    return data


def _fetch_email(creds: Credentials) -> Optional[str]:
    """Fetch user's email via OIDC userinfo endpoint.

    Returns None if not available (e.g., scope missing or network issue).
    """
    try:
        session = AuthorizedSession(creds)
        resp = session.get("https://openidconnect.googleapis.com/v1/userinfo", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("email")
    except Exception:
        return None
    return None


def _save_account(creds: Credentials, fallback_email: Optional[str] = None) -> int:
    email = _fetch_email(creds) or (fallback_email or "")
    if not email:
        # Ensure we have an email to satisfy schema; prompt minimally
        email = input("Google アカウントのメールアドレス（email スコープ未許可時の入力）: ").strip()
    # Upsert account by email
    acct = models.get_account_by_email(email)
    if acct:
        models.update_account_refresh_token(int(acct["id"]), creds.refresh_token)
        return int(acct["id"])
    return models.create_account(email=email, refresh_token=creds.refresh_token)


def run_oauth_and_save_account() -> int:
    """Run OAuth in a local browser and persist refresh token; returns account_id.

    Expects client secrets JSON at path given by _client_secrets_path().
    """
    secrets = _client_secrets_path()
    if not secrets.exists():
        raise FileNotFoundError(
            f"OAuth クライアントシークレットが見つかりません: {secrets}. "
            "環境変数 WISE_GOOGLE_CLIENT_SECRETS または ./cred.json を用意してください。"
        )

    # Run OAuth (forces consent to guarantee refresh_token issuance)
    flow = InstalledAppFlow.from_client_secrets_file(str(secrets), scopes=SCOPES)
    creds = flow.run_local_server(
        port=0,
        prompt="consent",
        authorization_prompt_message="",
        access_type="offline",
    )

    if not creds.refresh_token:
        # Extremely rare edge if provider didn't issue it
        raise RuntimeError("リフレッシュトークンが取得できませんでした。権限付与をやり直してください。")

    return _save_account(creds)
