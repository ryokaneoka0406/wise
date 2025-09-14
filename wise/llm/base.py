"""Base interface for LLM wrappers."""

from __future__ import annotations


def generate(prompt: str) -> str:
    """Return a mock LLM response."""
    return "mock response"
