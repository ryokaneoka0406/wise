"""Run a quick Gemini prompt using the shared LLM wrapper."""

from __future__ import annotations

import argparse
import sys
from typing import Sequence

from wise.llm import base


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send a prompt to Gemini via wise.llm.base.generate",
    )
    parser.add_argument(
        "prompt",
        help="Text prompt to send. Surround with quotes to preserve spaces.",
    )
    parser.add_argument(
        "--model",
        default=base.DEFAULT_MODEL,
        help="Gemini model name (default: %(default)s)",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=None,
        help="Optional sampling temperature.",
    )
    parser.add_argument(
        "--max-output-tokens",
        type=int,
        default=None,
        help="Optional limit for output tokens.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    config = base.GenerationConfig(
        model=args.model,
        temperature=args.temperature,
        max_output_tokens=args.max_output_tokens,
    )

    try:
        result = base.generate(args.prompt, config=config)
    except base.MissingAPIKeyError as exc:
        print(exc, file=sys.stderr)
        return 2
    except base.MissingDependencyError as exc:
        print(exc, file=sys.stderr)
        return 3
    except base.LLMError as exc:
        print(f"LLM request failed: {exc}", file=sys.stderr)
        return 4

    print(result)
    return 0


if __name__ == "__main__":  # pragma: no cover - manual invocation helper
    raise SystemExit(main(sys.argv[1:]))
