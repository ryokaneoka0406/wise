from __future__ import annotations

import argparse
from typing import Sequence

from wise.db import models


def cmd_show(args: argparse.Namespace) -> None:
    tables = models.list_tables()
    print("tables:", tables)


def cmd_drop_legacy(args: argparse.Namespace) -> None:
    dropped = models.drop_legacy_tables()
    if dropped:
        print("dropped:", dropped)
    else:
        print("dropped: [] (no legacy tables found)")


def cmd_reinit(args: argparse.Namespace) -> None:
    models.init_db()
    print("reinitialized schema for: accounts, sessions, messages")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Wise DB maintenance")
    sub = p.add_subparsers(dest="cmd", required=True)

    s1 = sub.add_parser("show", help="List user tables")
    s1.set_defaults(func=cmd_show)

    s2 = sub.add_parser("drop-legacy", help="Drop legacy tables (datasets, queries, analysis)")
    s2.set_defaults(func=cmd_drop_legacy)

    s3 = sub.add_parser("reinit", help="Ensure current schema exists (accounts/sessions/messages)")
    s3.set_defaults(func=cmd_reinit)

    return p


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()

