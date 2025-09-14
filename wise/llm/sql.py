"""SQL generation prompt helpers."""

from __future__ import annotations


def generate_sql(nl_query: str) -> str:
    """Return mock SQL for a natural language query."""
    _ = nl_query
    return "SELECT 1"
