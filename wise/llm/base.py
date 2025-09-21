"""Common Gemini client wrapper for LLM interactions."""

from __future__ import annotations

import logging
import os
import random
import time
from dataclasses import dataclass, fields
from importlib import import_module
from typing import Any, Callable, Sequence

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "gemini-2.5-flash"
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_INITIAL_DELAY = 1.0
DEFAULT_MAX_DELAY = 30.0
DEFAULT_BACKOFF_MULTIPLIER = 2.0
DEFAULT_JITTER = 0.1
_RETRYABLE_STATUS_CODES = {408, 409, 425, 429, 500, 502, 503, 504}

try:  # pragma: no cover - optional dependency at runtime
    from google.api_core import exceptions as google_api_exceptions  # type: ignore
except Exception:  # noqa: BLE001 - broad to support environments without google-api-core
    google_api_exceptions = None

try:  # pragma: no cover - optional dependency at runtime
    from dotenv import load_dotenv
except Exception:  # noqa: BLE001 - align with other optional imports
    load_dotenv = None

_default_retryable: list[type[Exception]] = [TimeoutError, ConnectionError]
if google_api_exceptions is not None:  # pragma: no cover - optional import
    for name in (
        "DeadlineExceeded",
        "ServiceUnavailable",
        "InternalServerError",
        "TooManyRequests",
    ):
        exc_type = getattr(google_api_exceptions, name, None)
        if exc_type is not None:
            _default_retryable.append(exc_type)

DEFAULT_RETRYABLE_EXCEPTIONS: tuple[type[Exception], ...] = tuple(_default_retryable)


_DOTENV_LOADED = False


class LLMError(RuntimeError):
    """Raised when the LLM backend fails to return a usable response."""


class MissingAPIKeyError(LLMError):
    """Raised when the Gemini API key is not configured."""


class MissingDependencyError(LLMError):
    """Raised when the Google GenAI SDK is not installed."""


@dataclass(slots=True)
class GenerationConfig:
    """Generation options shared across LLM calls."""

    model: str = DEFAULT_MODEL
    temperature: float | None = None
    top_p: float | None = None
    top_k: int | None = None
    max_output_tokens: int | None = None
    response_mime_type: str | None = None
    thinking_budget: int | None = None
    safety_settings: Any | None = None
    system_instruction: str | None = None


@dataclass(slots=True)
class RetryConfig:
    """Retry behaviour for LLM requests."""

    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    initial_delay: float = DEFAULT_INITIAL_DELAY
    max_delay: float = DEFAULT_MAX_DELAY
    multiplier: float = DEFAULT_BACKOFF_MULTIPLIER
    jitter: float = DEFAULT_JITTER
    retry_on: tuple[type[Exception], ...] = DEFAULT_RETRYABLE_EXCEPTIONS


class LLMClient:
    """Gemini API client with shared configuration and retry logic."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        default_config: GenerationConfig | None = None,
        retry_config: RetryConfig | None = None,
        client_factory: Callable[[str], Any] | None = None,
    ) -> None:
        """Initialise the client wrapper.

        Args:
            api_key: Explicit Gemini API key. If omitted the key is resolved from the
                environment (loading ``.env`` on demand).
            default_config: Default generation parameters applied to every request.
            retry_config: Retry behaviour for transient failures.
            client_factory: Factory that returns an object exposing
                ``models.generate_content``. The resolved API key is passed to it.

        Raises:
            MissingAPIKeyError: When the API key cannot be resolved.
        """
        self._api_key = _resolve_api_key(api_key)
        self._default_config = default_config or GenerationConfig()
        self._retry = retry_config or RetryConfig()
        self._client_factory = client_factory or self._create_default_client
        self._client = self._client_factory(self._api_key)

    def generate(
        self,
        prompt: str | Sequence[Any],
        *,
        config: GenerationConfig | None = None,
    ) -> str:
        """Generate a response for ``prompt`` using Gemini.

        Args:
            prompt: Plain text or a Gemini ``contents`` sequence.
            config: Optional overrides for generation parameters.

        Returns:
            The text extracted from the model response.

        Raises:
            ValueError: If the prompt is empty.
            TypeError: If the prompt is not a string or valid sequence.
            LLMError: When all retry attempts fail or the response is empty.
        """

        contents = _normalise_prompt(prompt)
        merged_config = self._merge_config(config)
        request_options = self._build_request_options(merged_config)

        delay = self._retry.initial_delay
        for attempt in range(self._retry.max_attempts):
            try:
                response = self._client.models.generate_content(
                    model=merged_config.model,
                    contents=contents,
                    **request_options,
                )
            except Exception as exc:  # noqa: BLE001 - handled by retry logic
                if not self._should_retry(exc, attempt):
                    logger.debug("Gemini request failed without retry: %s", exc)
                    raise LLMError("Failed to generate response from Gemini") from exc

                logger.debug("Gemini request retrying after error: %s", exc)
                _sleep_with_backoff(delay, self._retry.jitter)
                delay = min(delay * self._retry.multiplier, self._retry.max_delay)
                continue

            text = _extract_response_text(response)
            if not text:
                raise LLMError("Gemini response did not include text content")
            return text

        raise LLMError("Gemini request exhausted retry attempts")

    def _merge_config(self, override: GenerationConfig | None) -> GenerationConfig:
        """Combine the default and override generation configs.

        Args:
            override: Optional config provided at call time.

        Returns:
            A new :class:`GenerationConfig` with override values applied.
        """
        if override is None:
            return self._default_config

        values: dict[str, Any] = {}
        for field in fields(GenerationConfig):
            value = getattr(override, field.name)
            if field.name == "model":
                values[field.name] = value or getattr(self._default_config, field.name)
            elif value is not None:
                values[field.name] = value
            else:
                values[field.name] = getattr(self._default_config, field.name)
        return GenerationConfig(**values)

    def _build_request_options(self, config: GenerationConfig) -> dict[str, Any]:
        """Translate :class:`GenerationConfig` into SDK kwargs.

        Args:
            config: Final generation configuration.

        Returns:
            Keyword arguments passed to ``models.generate_content``.

        Raises:
            MissingDependencyError: When ``google-genai`` is not installed.
        """
        request_kwargs: dict[str, Any] = {}
        cfg_kwargs: dict[str, Any] = {}
        types_module: Any | None = None

        if config.temperature is not None:
            cfg_kwargs["temperature"] = config.temperature
        if config.top_p is not None:
            cfg_kwargs["top_p"] = config.top_p
        if config.top_k is not None:
            cfg_kwargs["top_k"] = config.top_k
        if config.max_output_tokens is not None:
            cfg_kwargs["max_output_tokens"] = config.max_output_tokens
        if config.response_mime_type is not None:
            cfg_kwargs["response_mime_type"] = config.response_mime_type
        if config.thinking_budget is not None:
            types_module = _load_genai_types()
            cfg_kwargs["thinking_config"] = types_module.ThinkingConfig(  # type: ignore[attr-defined]
                thinking_budget=config.thinking_budget,
            )

        if cfg_kwargs:
            if types_module is None:
                types_module = _load_genai_types()
            request_kwargs["config"] = types_module.GenerateContentConfig(**cfg_kwargs)

        if config.safety_settings is not None:
            request_kwargs["safety_settings"] = config.safety_settings
        if config.system_instruction is not None:
            request_kwargs["system_instruction"] = config.system_instruction

        return request_kwargs

    def _should_retry(self, exc: Exception, attempt: int) -> bool:
        """Return whether the request should be retried after ``exc``.

        Args:
            exc: The raised exception.
            attempt: Zero-based attempt number already used.

        Returns:
            ``True`` if the failure is considered transient and more attempts remain.
        """
        if attempt >= self._retry.max_attempts - 1:
            return False
        if isinstance(exc, (MissingAPIKeyError, MissingDependencyError, KeyboardInterrupt)):
            return False
        if self._retry.retry_on and isinstance(exc, self._retry.retry_on):
            return True

        status_code = _extract_status_code(exc)
        return status_code is not None and status_code in _RETRYABLE_STATUS_CODES

    @staticmethod
    def _create_default_client(api_key: str) -> Any:
        """Instantiate the default Google GenAI client.

        Args:
            api_key: Gemini API key to authenticate the client.

        Returns:
            A ``google.genai.Client`` instance.

        Raises:
            MissingDependencyError: When the GenAI SDK is not installed.
        """
        genai_module = _load_genai_module()
        return genai_module.Client(api_key=api_key)


_default_client: LLMClient | None = None


def get_default_client() -> LLMClient:
    """Return the process-wide default :class:`LLMClient` instance.

    Returns:
        A cached :class:`LLMClient` configured with environment defaults.

    Raises:
        MissingAPIKeyError: When the API key cannot be resolved.
        MissingDependencyError: When ``google-genai`` is absent.
    """

    global _default_client
    if _default_client is None:
        _default_client = LLMClient()
    return _default_client


def generate(
    prompt: str | Sequence[Any],
    *,
    config: GenerationConfig | None = None,
    client: LLMClient | None = None,
) -> str:
    """Generate a Gemini response using the default client.

    Args:
        prompt: Plain text or Gemini ``contents`` sequence.
        config: Optional generation parameters.
        client: Specific :class:`LLMClient` to use. Defaults to the shared client.

    Returns:
        The text portion of the Gemini response.

    Raises:
        ValueError: If the prompt is empty.
        TypeError: If the prompt is not a string or sequence.
        LLMError: On failure after retries.
    """

    active_client = client or get_default_client()
    return active_client.generate(prompt, config=config)


def _normalise_prompt(prompt: str | Sequence[Any]) -> str | Sequence[Any]:
    """Validate and tidy the prompt before sending to Gemini.

    Args:
        prompt: Raw prompt input.

    Returns:
        A stripped string or the original sequence.

    Raises:
        ValueError: If the string or sequence is empty.
        TypeError: If the prompt type is unsupported.
    """
    if isinstance(prompt, str):
        text = prompt.strip()
        if not text:
            raise ValueError("prompt must not be empty")
        return text

    if not isinstance(prompt, Sequence):
        raise TypeError("prompt must be a string or sequence of Gemini contents")
    if not prompt:
        raise ValueError("prompt sequence must not be empty")
    return prompt


def _resolve_api_key(explicit: str | None) -> str:
    """Resolve the Gemini API key from arguments or environment.

    Args:
        explicit: API key provided by the caller.

    Returns:
        A non-empty API key string.

    Raises:
        MissingAPIKeyError: If the key cannot be obtained.
    """
    if explicit:
        return explicit

    _ensure_dotenv_loaded()

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise MissingAPIKeyError(
            "Gemini API key is not configured. Set GEMINI_API_KEY or pass api_key explicitly.",
        )
    return api_key


def _ensure_dotenv_loaded() -> None:
    """Load environment variables from ``.env`` only once."""
    if _DOTENV_LOADED:
        return

    if load_dotenv is None:
        _mark_dotenv_loaded()
        return

    load_dotenv()
    _mark_dotenv_loaded()


def _mark_dotenv_loaded() -> None:
    """Mark the dotenv loader as having been invoked."""
    global _DOTENV_LOADED
    _DOTENV_LOADED = True


def _load_genai_module() -> Any:
    """Import ``google.genai`` with a helpful error message on failure.

    Returns:
        The imported ``google.genai`` module.

    Raises:
        MissingDependencyError: When the SDK is not installed.
    """
    try:
        return import_module("google.genai")
    except ModuleNotFoundError as exc:  # pragma: no cover - depends on runtime setup
        raise MissingDependencyError(
            "google-genai package is required. Install it with `uv add google-genai`.",
        ) from exc


def _load_genai_types() -> Any:
    """Import ``google.genai.types`` with a helpful error message on failure.

    Returns:
        The imported ``google.genai.types`` module.

    Raises:
        MissingDependencyError: When the SDK is not installed.
    """
    try:
        return import_module("google.genai.types")
    except ModuleNotFoundError as exc:  # pragma: no cover - depends on runtime setup
        raise MissingDependencyError(
            "google-genai package is required. Install it with `uv add google-genai`.",
        ) from exc


def _extract_response_text(response: Any) -> str | None:
    """Extract a text payload from a Gemini response object.

    Args:
        response: Response returned by ``generate_content``.

    Returns:
        The first non-empty text fragment or ``None`` if none is found.
    """
    text = getattr(response, "text", None)
    if isinstance(text, str) and text.strip():
        return text

    if isinstance(response, dict):
        candidate_text = response.get("text")
        if isinstance(candidate_text, str) and candidate_text.strip():
            return candidate_text

    candidates = getattr(response, "candidates", None)
    if candidates:
        for candidate in candidates:
            content = getattr(candidate, "content", None)
            parts = getattr(content, "parts", None) if content is not None else None
            if not parts:
                continue
            fragments: list[str] = []
            for part in parts:
                if isinstance(part, str):
                    fragments.append(part)
                else:
                    part_text = getattr(part, "text", None)
                    if isinstance(part_text, str):
                        fragments.append(part_text)
            if fragments:
                return "".join(fragments)

    if hasattr(response, "__str__"):
        rendered = str(response).strip()
        return rendered or None
    return None


def _extract_status_code(exc: Exception) -> int | None:
    """Extract an HTTP-like status code from an exception, if available.

    Args:
        exc: Exception raised by the SDK or HTTP client.

    Returns:
        The numeric status code, or ``None`` when unavailable.
    """
    for attr in ("status_code", "code", "http_status", "status"):
        value = getattr(exc, attr, None)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):  # noqa: PERF203 - explicit for clarity
            continue
    return None


def _sleep_with_backoff(delay: float, jitter_ratio: float) -> None:
    """Sleep for ``delay`` seconds applying optional jitter.

    Args:
        delay: Base delay in seconds.
        jitter_ratio: Proportion of jitter to apply (0 disables jitter).
    """
    if delay <= 0:
        return

    jitter = max(jitter_ratio, 0.0) * delay
    if jitter:
        delay += random.uniform(-jitter, jitter)
    time.sleep(max(delay, 0.0))


__all__ = [
    "GenerationConfig",
    "RetryConfig",
    "LLMClient",
    "LLMError",
    "MissingAPIKeyError",
    "MissingDependencyError",
    "generate",
    "get_default_client",
]
