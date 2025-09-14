"""Command-line interface for the wise project."""

from __future__ import annotations

from .db.models import init_db


WELCOME_MESSAGE = "Welcome to Wise! Type 'exit' to quit."


def main() -> None:
    """Entry point for the `wise` command."""
    # Ensure local SQLite DB exists and has required tables
    init_db()
    print(WELCOME_MESSAGE)
    while True:
        try:
            user_input = input("you> ")
        except EOFError:
            break
        if user_input.strip().lower() in {"exit", "quit"}:
            print("assistant> Goodbye!")
            break
        print(f"assistant> Echo: {user_input}")


if __name__ == "__main__":  # pragma: no cover
    main()
