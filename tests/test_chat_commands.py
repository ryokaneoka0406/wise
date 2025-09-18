import sys
import types


def _install_google_stubs() -> None:
    google_module = types.ModuleType("google")
    auth_module = types.ModuleType("google.auth")
    transport_module = types.ModuleType("google.auth.transport")
    requests_module = types.ModuleType("google.auth.transport.requests")

    class _Request:  # pragma: no cover - simple stub
        pass

    class _AuthorizedSession:  # pragma: no cover - simple stub
        def __init__(self, *args, **kwargs) -> None:
            self.args = args
            self.kwargs = kwargs

        def request(self, *args, **kwargs):
            raise RuntimeError("Network access not available in tests")

    requests_module.Request = _Request
    requests_module.AuthorizedSession = _AuthorizedSession

    oauth_module = types.ModuleType("google.oauth2")
    credentials_module = types.ModuleType("google.oauth2.credentials")
    oauthlib_module = types.ModuleType("google_auth_oauthlib")
    oauthlib_flow_module = types.ModuleType("google_auth_oauthlib.flow")

    class _Credentials:  # pragma: no cover - simple stub
        def __init__(self, *args, **kwargs) -> None:
            self.args = args
            self.kwargs = kwargs

        def refresh(self, *args, **kwargs) -> None:
            return None

    credentials_module.Credentials = _Credentials

    class _InstalledAppFlow:  # pragma: no cover - simple stub
        @classmethod
        def from_client_secrets_file(cls, *args, **kwargs):
            return cls()

        def run_local_server(self, *args, **kwargs):
            class _Creds:  # pragma: no cover - simple stub
                refresh_token = "stub-token"

            return _Creds()

    oauthlib_flow_module.InstalledAppFlow = _InstalledAppFlow
    oauthlib_module.flow = oauthlib_flow_module

    google_module.auth = auth_module
    google_module.oauth2 = oauth_module

    sys.modules.setdefault("google", google_module)
    sys.modules.setdefault("google.auth", auth_module)
    sys.modules.setdefault("google.auth.transport", transport_module)
    sys.modules.setdefault("google.auth.transport.requests", requests_module)
    sys.modules.setdefault("google.oauth2", oauth_module)
    sys.modules.setdefault("google.oauth2.credentials", credentials_module)
    sys.modules.setdefault("google_auth_oauthlib", oauthlib_module)
    sys.modules.setdefault("google_auth_oauthlib.flow", oauthlib_flow_module)


_install_google_stubs()

from wise.chat import commands
from wise.metadata import manager as metadata_manager


def test_init_requires_login(monkeypatch):
    monkeypatch.setattr(commands.models, "list_accounts", lambda: [])
    message = commands._init()
    assert "login" in message


def test_init_happy_path(monkeypatch, tmp_path):
    monkeypatch.setattr(commands.models, "list_accounts", lambda: [{"id": 1, "refresh_token": "tok"}])

    projects = [{"projectId": "demo", "friendlyName": "Demo Project"}]
    monkeypatch.setattr(commands.bq_client, "list_projects", lambda account_id: projects)

    class DummyClient:
        def __init__(self, project_id: str, account_id: int):
            self.project_id = project_id
            self.account_id = account_id
            self.location = "US"

        def list_datasets(self):
            return ["sales", "marketing"]

    monkeypatch.setattr(commands.bq_client, "BQClient", DummyClient)

    captured = {}

    def fake_metadata_snapshot(**kwargs):
        captured.update(kwargs)
        return {"projectId": kwargs["project_id"], "location": kwargs.get("location"), "datasets": {}}

    monkeypatch.setattr(commands.bq_client, "metadata_snapshot", fake_metadata_snapshot)

    save_path = tmp_path / "project" / "demo" / "metadata.md"
    monkeypatch.setattr(
        commands.metadata_manager,
        "save_metadata",
        lambda snapshot: metadata_manager.MetadataWriteResult(path=save_path, backup_path=None),
    )

    inputs = iter(["1", ""])
    monkeypatch.setattr(commands, "_prompt_user", lambda _: next(inputs))

    message = commands._init()

    assert "metadata.md" in message
    assert captured["datasets"] == ["sales", "marketing"]
    assert captured["sample_n"] == 3
    assert captured["project_id"] == "demo"
    assert captured["account_id"] == 1


def test_init_cancelled(monkeypatch):
    monkeypatch.setattr(commands.models, "list_accounts", lambda: [{"id": 1, "refresh_token": "tok"}])
    monkeypatch.setattr(commands.bq_client, "list_projects", lambda account_id: [{"projectId": "demo"}])

    created = {"client": False}

    class DummyClient:
        def __init__(self, *args, **kwargs):
            created["client"] = True

        def list_datasets(self):  # pragma: no cover - should not be called
            return []

    monkeypatch.setattr(commands.bq_client, "BQClient", DummyClient)
    monkeypatch.setattr(commands, "_prompt_user", lambda _: "")

    message = commands._init()

    assert "キャンセル" in message
    assert created["client"] is False


def test_handle_command_routes_init(monkeypatch):
    monkeypatch.setattr(commands, "_init", lambda: "done")
    handled, reply = commands.handle_command("/init")
    assert handled is True
    assert reply == "done"
