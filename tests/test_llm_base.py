"""Tests for the Gemini LLM wrapper."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from dotenv import load_dotenv as real_load_dotenv

from wise.llm import base


class DummyModels:
    def __init__(self, responses: list[object]):
        self._responses = responses
        self.calls: list[dict[str, object]] = []

    def generate_content(self, **kwargs: object) -> object:
        self.calls.append(kwargs)
        if not self._responses:
            raise AssertionError("no responses configured")
        result = self._responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class DummyClient:
    def __init__(self, responses: list[object]):
        self.models = DummyModels(responses)


def make_client(responses: list[object]) -> DummyClient:
    return DummyClient(responses)


def test_generate_success_uses_client_factory() -> None:
    responses = [SimpleNamespace(text="hello world")]
    client = make_client(responses)
    llm = base.LLMClient(api_key="token", client_factory=lambda _: client)

    output = llm.generate(" Say hi ")

    assert output == "hello world"
    assert client.models.calls
    assert client.models.calls[0]["model"] == base.DEFAULT_MODEL
    assert client.models.calls[0]["contents"] == "Say hi"


def test_generate_retries_and_succeeds() -> None:
    class TemporaryError(RuntimeError):
        status_code = 503

    responses = [TemporaryError("service down"), SimpleNamespace(text="ok")]
    client = make_client(responses)
    retry = base.RetryConfig(
        max_attempts=2,
        initial_delay=0.0,
        max_delay=0.0,
        multiplier=1.0,
        jitter=0.0,
        retry_on=(TemporaryError,),
    )

    llm = base.LLMClient(api_key="token", client_factory=lambda _: client, retry_config=retry)

    assert llm.generate("Test") == "ok"
    assert len(client.models.calls) == 2


def test_generate_raises_for_non_retryable_error() -> None:
    responses = [ValueError("bad request")]
    client = make_client(responses)
    llm = base.LLMClient(api_key="token", client_factory=lambda _: client)

    with pytest.raises(base.LLMError):
        llm.generate("Test non retry")
    assert len(client.models.calls) == 1


def test_generate_rejects_empty_prompt() -> None:
    client = make_client([SimpleNamespace(text="unused")])
    llm = base.LLMClient(api_key="token", client_factory=lambda _: client)

    with pytest.raises(ValueError):
        llm.generate("   \n  ")

    assert not client.models.calls


def test_missing_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setattr(base, "_DOTENV_LOADED", False, raising=False)
    monkeypatch.setattr(base, "load_dotenv", lambda *_, **__: False)

    with pytest.raises(base.MissingAPIKeyError):
        base.LLMClient(client_factory=lambda _: make_client([]))


def test_missing_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "token")
    monkeypatch.setattr(base, "_default_client", None, raising=False)
    monkeypatch.setattr(base, "_DOTENV_LOADED", False, raising=False)

    original_import = base.import_module

    def fake_import(name: str):
        if name.startswith("google.genai"):
            raise ModuleNotFoundError(name)
        return original_import(name)

    monkeypatch.setattr(base, "import_module", fake_import)

    with pytest.raises(base.MissingDependencyError):
        base.get_default_client()


def test_response_without_text_uses_candidate_parts() -> None:
    parts = [SimpleNamespace(text="foo"), SimpleNamespace(text="bar")]
    candidate = SimpleNamespace(content=SimpleNamespace(parts=parts))
    response = SimpleNamespace(text="", candidates=[candidate])
    client = make_client([response])
    llm = base.LLMClient(api_key="token", client_factory=lambda _: client)

    assert llm.generate("Combine parts") == "foobar"


def test_api_key_loaded_from_dotenv(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("GEMINI_API_KEY=dotenv_key\n", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setattr(base, "_DOTENV_LOADED", False, raising=False)
    monkeypatch.setattr(
        base,
        "load_dotenv",
        lambda: real_load_dotenv(env_file, override=True),
    )

    captured: dict[str, str] = {}

    def factory(key: str) -> DummyClient:
        captured["key"] = key
        return make_client([SimpleNamespace(text="ready")])

    llm = base.LLMClient(client_factory=factory)

    assert captured["key"] == "dotenv_key"
    assert llm.generate("Hello") == "ready"
