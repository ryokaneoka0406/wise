"""Command-line interface for the wise project.

This wires the startup flow:
- Initialize the SQLite DB tables
- Start a chat session which triggers the first-time setup wizard
  to save a refresh token when none exists, then transitions into chat.
"""

from __future__ import annotations

from .db.models import init_db
from .chat.session import start_session


WELCOME_MESSAGE = "Welcome to Wise!"


def main() -> None:
    """Entry point for the `wise` command."""
    # Ensure local SQLite DB exists and has required tables
    init_db()
    print(WELCOME_MESSAGE)
    # Delegate to chat session which handles setup wizard and chat loop
    start_session()


if __name__ == "__main__":  # pragma: no cover
    main()
