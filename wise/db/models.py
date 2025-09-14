"""SQLite-backed internal DB models and minimal CRUD helpers.

Design-doc compliant: only ``accounts``, ``sessions``, and ``messages`` tables
are managed here. Other artifacts (datasets, queries, analysis results) are
stored on the filesystem per the design.
"""

from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Optional


DEFAULT_DB_FILENAME = "wise.db"


def _connect(db_path: Optional[str] = None) -> sqlite3.Connection:
    """Return a connection with foreign keys enabled.

    Args:
        db_path: Path to the SQLite DB file. Defaults to ``./wise.db``.
    """
    db_file = Path(db_path) if db_path else Path.cwd() / DEFAULT_DB_FILENAME
    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(db_path: Optional[str] = None) -> None:
    """Create design-doc compliant tables if they do not already exist.

    Tables:
      - accounts(id, email, refresh_token, created_at)
      - sessions(id, account_id, started_at)
      - messages(id, session_id, role, content, created_at)
    """
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                refresh_token TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                started_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('user','assistant','system')),
                content TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
            );
            """
        )
        conn.commit()


# ------------------------
# Accounts CRUD
# ------------------------


def create_account(email: str, refresh_token: Optional[str] = None, db_path: Optional[str] = None) -> int:
    """Create an account, or return existing one if email already exists.

    If the email already exists, optionally updates the refresh token when
    provided and returns the existing account id.
    """
    with _connect(db_path) as conn:
        try:
            cur = conn.execute(
                "INSERT INTO accounts(email, refresh_token) VALUES (?, ?)",
                (email, refresh_token),
            )
            conn.commit()
            return int(cur.lastrowid)
        except sqlite3.IntegrityError:
            if refresh_token is not None:
                conn.execute(
                    "UPDATE accounts SET refresh_token = ? WHERE email = ?",
                    (refresh_token, email),
                )
                conn.commit()
            cur = conn.execute("SELECT id FROM accounts WHERE email = ?", (email,))
            row = cur.fetchone()
            return int(row[0])


def get_account_by_email(email: str, db_path: Optional[str] = None) -> Optional[sqlite3.Row]:
    with _connect(db_path) as conn:
        cur = conn.execute("SELECT * FROM accounts WHERE email = ?", (email,))
        return cur.fetchone()


def list_accounts(db_path: Optional[str] = None) -> list[sqlite3.Row]:
    with _connect(db_path) as conn:
        cur = conn.execute("SELECT * FROM accounts ORDER BY created_at DESC")
        return list(cur.fetchall())


def update_account_refresh_token(account_id: int, refresh_token: Optional[str], db_path: Optional[str] = None) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE accounts SET refresh_token = ? WHERE id = ?",
            (refresh_token, account_id),
        )
        conn.commit()


# ------------------------
# Sessions and Messages
# ------------------------


def create_session(account_id: int, db_path: Optional[str] = None) -> int:
    with _connect(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO sessions(account_id) VALUES (?)",
            (account_id,),
        )
        conn.commit()
        return int(cur.lastrowid)


def add_message(session_id: int, role: str, content: str, db_path: Optional[str] = None) -> int:
    if role not in {"user", "assistant", "system"}:
        raise ValueError("role must be 'user', 'assistant', or 'system'")
    with _connect(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO messages(session_id, role, content) VALUES (?, ?, ?)",
            (session_id, role, content),
        )
        conn.commit()
        return int(cur.lastrowid)


def list_messages(session_id: int, db_path: Optional[str] = None) -> list[sqlite3.Row]:
    with _connect(db_path) as conn:
        cur = conn.execute(
            "SELECT * FROM messages WHERE session_id = ? ORDER BY created_at ASC",
            (session_id,),
        )
        return list(cur.fetchall())


def list_sessions(account_id: int, db_path: Optional[str] = None) -> list[sqlite3.Row]:
    with _connect(db_path) as conn:
        cur = conn.execute(
            "SELECT * FROM sessions WHERE account_id = ? ORDER BY started_at DESC",
            (account_id,),
        )
        return list(cur.fetchall())


# Convenience utilities


def get_db_path(explicit: Optional[str] = None) -> str:
    """Return the DB path, defaulting to ``./wise.db`` if not provided."""
    return str(Path(explicit) if explicit else (Path.cwd() / DEFAULT_DB_FILENAME))


# ------------------------
# Maintenance helpers
# ------------------------


def list_tables(db_path: Optional[str] = None) -> list[str]:
    """Return user-defined table names (excluding SQLite internals)."""
    with _connect(db_path) as conn:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        names = [r[0] for r in cur.fetchall()]
    return [n for n in names if not n.startswith("sqlite_")]


def drop_tables(names: list[str], db_path: Optional[str] = None) -> list[str]:
    """Drop the specified tables if they exist. Returns dropped names."""
    existing = set(list_tables(db_path))
    to_drop = [n for n in names if n in existing]
    if not to_drop:
        return []
    with _connect(db_path) as conn:
        for n in to_drop:
            conn.execute(f"DROP TABLE IF EXISTS {n}")
        conn.commit()
    return to_drop


def drop_legacy_tables(db_path: Optional[str] = None) -> list[str]:
    """Drop legacy tables no longer used by the design doc.

    This targets only known legacy names for safety.
    """
    legacy = ["datasets", "queries", "analysis"]
    return drop_tables(legacy, db_path=db_path)
