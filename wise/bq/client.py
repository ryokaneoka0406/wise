"""BigQuery REST client used by the wise CLI.

This module talks directly to BigQuery's REST API by combining
google-auth refreshable credentials with ``AuthorizedSession``. The
implementation intentionally avoids ``google-cloud-bigquery`` so that the
project can stay within the lightweight dependency set defined in the
design doc.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Iterable, Optional, Tuple

from google.auth.transport.requests import AuthorizedSession, Request
from google.oauth2.credentials import Credentials

from ..auth.google import SCOPES
from ..db import models

__all__ = [
    "BQClient",
    "BigQueryClientError",
    "list_projects",
    "metadata_snapshot",
    "query",
]


class BigQueryClientError(RuntimeError):
    """Raised when the BigQuery REST API or credential setup fails."""

    def __init__(self, message: str, *, status_code: int | None = None, payload: Any | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


def _request_json(
    session: AuthorizedSession,
    method: str,
    url: str,
    *,
    timeout: float,
    params: Optional[dict[str, Any]] = None,
    json_data: Any = None,
) -> dict[str, Any]:
    """Invoke an HTTP request and parse the JSON payload with shared error handling."""

    response = session.request(method, url, params=params, json=json_data, timeout=timeout)
    if response.status_code >= 400:
        try:
            payload = response.json()
        except ValueError:
            payload = response.text
        raise BigQueryClientError(
            f"BigQuery API 呼び出しエラー ({response.status_code})", status_code=response.status_code, payload=payload
        )
    if not response.content:
        return {}
    try:
        return response.json()
    except ValueError as exc:  # pragma: no cover - unexpected server bug
        raise BigQueryClientError("BigQuery API の応答を JSON として解析できませんでした。") from exc


def _resolve_account(account_id: Optional[int], *, db_path: Optional[str] = None) -> Any:
    accounts = models.list_accounts(db_path=db_path)
    if account_id is not None:
        for row in accounts:
            if int(row["id"]) == int(account_id):
                return row
        raise BigQueryClientError(
            f"account_id={account_id} が見つかりません。'wise login' を実行して認証してください。"
        )

    for row in accounts:
        if row["refresh_token"]:
            return row
    raise BigQueryClientError("利用可能なアカウントが見つかりません。'wise login' で認証してください。")


def _load_client_info() -> dict[str, str]:
    env_path = os.getenv("WISE_GOOGLE_CLIENT_SECRETS")
    candidate: Optional[Path] = Path(env_path) if env_path else None
    if not candidate:
        cwd = Path.cwd()
        for name in ("cred.json", "client_secrets.json"):
            p = cwd / name
            if p.exists():
                candidate = p
                break
        if not candidate:
            candidate = cwd / "cred.json"

    if not candidate.exists():
        raise BigQueryClientError(
            f"OAuth クライアントシークレットが見つかりません: {candidate}."
            " 環境変数 WISE_GOOGLE_CLIENT_SECRETS または ./cred.json を配置してください。"
        )

    with candidate.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if "installed" in data:
        config = data["installed"]
    elif "web" in data:
        config = data["web"]
    else:
        config = data

    missing = [key for key in ("client_id", "client_secret", "token_uri") if key not in config]
    if missing:
        joined = ", ".join(missing)
        raise BigQueryClientError(f"OAuth クライアント設定に不足項目があります: {joined}")
    return config


def _build_credentials(client_info: dict[str, str], refresh_token: str) -> Credentials:
    try:
        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            client_id=client_info["client_id"],
            client_secret=client_info["client_secret"],
            token_uri=client_info["token_uri"],
            scopes=SCOPES,
        )
        creds.refresh(Request())
    except Exception as exc:  # pragma: no cover - network failure is rare
        raise BigQueryClientError("アクセストークンのリフレッシュに失敗しました。") from exc
    return creds


def _create_authorized_session(
    account_id: Optional[int],
    *,
    db_path: Optional[str] = None,
) -> Tuple[AuthorizedSession, Credentials, Any]:
    client_info = _load_client_info()
    account = _resolve_account(account_id, db_path=db_path)
    refresh_token = account["refresh_token"]
    if not refresh_token:
        raise BigQueryClientError("指定されたアカウントに refresh_token が保存されていません。")

    credentials = _build_credentials(client_info, refresh_token)
    session = AuthorizedSession(credentials)
    return session, credentials, account


class BQClient:
    """Minimal BigQuery REST client for metadata operations and SQL execution."""

    BASE_URL = "https://bigquery.googleapis.com/bigquery/v2"
    DEFAULT_TIMEOUT = 30
    POLL_INTERVAL_SEC = 1.0
    MAX_POLL_ATTEMPTS = 30

    def __init__(
        self,
        project_id: str,
        account_id: Optional[int] = None,
        location: str = "US",
        *,
        db_path: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        if not project_id:
            raise ValueError("project_id は必須です。")

        self.project_id = project_id
        self.location = location
        self.account_id: int
        self._timeout = timeout if timeout is not None else self.DEFAULT_TIMEOUT

        session, credentials, account = _create_authorized_session(account_id, db_path=db_path)
        self.account_id = int(account["id"])
        self._credentials = credentials
        self._session = session

    # ----------
    # Public API
    # ----------

    def list_datasets(self) -> list[str]:
        """Return dataset IDs available in the configured project."""

        path = f"/projects/{self.project_id}/datasets"
        params = {"maxResults": 1000}
        dataset_ids: list[str] = []
        for page in self._paginate(path, params=params):
            for ds in page.get("datasets", []):
                ref = ds.get("datasetReference", {})
                dataset_id = ref.get("datasetId")
                if dataset_id:
                    dataset_ids.append(dataset_id)
        return dataset_ids

    def list_tables(self, dataset_id: str) -> list[str]:
        """Return table IDs for a given dataset."""

        if not dataset_id:
            raise ValueError("dataset_id は必須です。")
        path = f"/projects/{self.project_id}/datasets/{dataset_id}/tables"
        params = {"maxResults": 1000}
        table_ids: list[str] = []
        for page in self._paginate(path, params=params):
            for table in page.get("tables", []):
                ref = table.get("tableReference", {})
                table_id = ref.get("tableId")
                if table_id:
                    table_ids.append(table_id)
        return table_ids

    def get_table_schema(self, dataset_id: str, table_id: str) -> list[dict[str, Any]]:
        """Return BigQuery field definitions for the specified table."""

        if not dataset_id or not table_id:
            raise ValueError("dataset_id と table_id は必須です。")
        path = f"/projects/{self.project_id}/datasets/{dataset_id}/tables/{table_id}"
        data = self._request("GET", path)
        schema = (data.get("schema") or {}).get("fields")
        return list(schema or [])

    def sample_rows(self, dataset_id: str, table_id: str, max_results: int = 5) -> list[dict[str, Any]]:
        """Fetch up to ``max_results`` rows from the table using the data endpoint."""

        if max_results <= 0:
            return []
        path = f"/projects/{self.project_id}/datasets/{dataset_id}/tables/{table_id}/data"
        params = {"maxResults": max_results}
        data = self._request("GET", path, params=params)
        schema_fields = (data.get("schema") or {}).get("fields") or []
        rows = data.get("rows") or []
        return self._format_rows(rows, schema_fields)

    def run_sql(
        self,
        sql: str,
        *,
        max_results: int = 1000,
        dry_run: bool = False,
        fetch_all: bool = True,
    ) -> dict[str, Any]:
        """Execute SQL via ``jobs.query`` and return schema/rows metadata."""

        if not sql:
            raise ValueError("SQL 文が空です。")
        payload: dict[str, Any] = {
            "query": sql,
            "useLegacySql": False,
            "location": self.location,
            "maxResults": max_results,
            "dryRun": dry_run,
        }
        data = self._request("POST", f"/projects/{self.project_id}/queries", json=payload)

        job = data.get("jobReference", {})
        job_id = job.get("jobId")
        if not job_id:
            raise BigQueryClientError("jobs.query の応答に jobId が含まれていません。", payload=data)

        schema_fields = (data.get("schema") or {}).get("fields") or []
        total_rows = self._coerce_int(data.get("totalRows"))
        rows: list[dict[str, Any]] = []

        if not dry_run:
            rows.extend(self._format_rows(data.get("rows") or [], schema_fields))
            page_token = self._next_page_token(data)
            job_complete = data.get("jobComplete", True)

            if not job_complete:
                # Wait for the job to finish before returning rows.
                finished = self._poll_for_completion(job_id, max_results=max_results)
                if finished.get("schema") and not schema_fields:
                    schema_fields = (finished.get("schema") or {}).get("fields") or schema_fields
                rows.extend(self._format_rows(finished.get("rows") or [], schema_fields))
                total_rows = self._coerce_int(finished.get("totalRows") or total_rows)
                page_token = self._next_page_token(finished)
                job_complete = finished.get("jobComplete", True)

            if fetch_all and page_token:
                rows.extend(
                    self._fetch_remaining_rows(
                        job_id,
                        schema_fields=schema_fields,
                        start_token=page_token,
                        max_results=max_results,
                    )
                )
        result = {
            "schema": schema_fields,
            "rows": rows,
            "totalRows": total_rows,
            "jobId": job_id,
        }
        return result

    # ---------------
    # Helper routines
    # ---------------

    def _full_url(self, path: str) -> str:
        return f"{self.BASE_URL}{path}"

    def _request(self, method: str, path: str, *, params: Optional[dict[str, Any]] = None, json: Any = None) -> dict[str, Any]:
        url = self._full_url(path)
        return _request_json(
            self._session,
            method,
            url,
            timeout=self._timeout,
            params=params,
            json_data=json,
        )

    def _paginate(self, path: str, *, params: Optional[dict[str, Any]] = None) -> Iterable[dict[str, Any]]:
        token: Optional[str] = None
        base_params = params or {}
        while True:
            query = dict(base_params)
            if token:
                query["pageToken"] = token
            data = self._request("GET", path, params=query)
            yield data
            token = self._next_page_token(data)
            if not token:
                break

    def _poll_for_completion(self, job_id: str, *, max_results: int) -> dict[str, Any]:
        attempts = 0
        while True:
            if attempts >= self.MAX_POLL_ATTEMPTS:
                raise BigQueryClientError("BigQuery ジョブが完了しませんでした (タイムアウト)。")
            time.sleep(self.POLL_INTERVAL_SEC)
            data = self._request(
                "GET",
                f"/projects/{self.project_id}/queries/{job_id}",
                params={"maxResults": max_results, "location": self.location},
            )
            if data.get("jobComplete", True):
                return data
            attempts += 1

    def _fetch_remaining_rows(
        self,
        job_id: str,
        *,
        schema_fields: list[dict[str, Any]],
        start_token: str,
        max_results: int,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        token: Optional[str] = start_token
        while token:
            data = self._request(
                "GET",
                f"/projects/{self.project_id}/queries/{job_id}",
                params={
                    "maxResults": max_results,
                    "pageToken": token,
                    "location": self.location,
                },
            )
            if data.get("schema") and not schema_fields:
                schema_fields[:] = (data.get("schema") or {}).get("fields") or []
            rows.extend(self._format_rows(data.get("rows") or [], schema_fields))
            token = self._next_page_token(data)
        return rows

    @staticmethod
    def _next_page_token(data: dict[str, Any]) -> Optional[str]:
        return data.get("pageToken") or data.get("nextPageToken")

    @staticmethod
    def _coerce_int(value: Any) -> int:
        if value is None:
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _format_rows(rows: Iterable[Any], schema_fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows or not schema_fields:
            return []
        names = [field.get("name") for field in schema_fields]
        formatted: list[dict[str, Any]] = []
        for row in rows:
            cells = row.get("f", []) if isinstance(row, dict) else []
            mapped: dict[str, Any] = {}
            for idx, name in enumerate(names):
                if not name:
                    continue
                if idx < len(cells):
                    mapped[name] = cells[idx].get("v")
                else:
                    mapped[name] = None
            formatted.append(mapped)
        return formatted


def query(
    sql: str,
    *,
    project_id: str,
    account_id: Optional[int] = None,
    location: str = "US",
    max_results: int = 1000,
    dry_run: bool = False,
    fetch_all: bool = True,
) -> list[dict[str, Any]]:
    """Convenience wrapper that returns only the rows from ``run_sql``."""

    client = BQClient(project_id=project_id, account_id=account_id, location=location)
    result = client.run_sql(sql, max_results=max_results, dry_run=dry_run, fetch_all=fetch_all)
    return result.get("rows", [])


def list_projects(
    *,
    account_id: Optional[int] = None,
    db_path: Optional[str] = None,
    timeout: Optional[float] = None,
) -> list[dict[str, Any]]:
    """Return available BigQuery projects for the authenticated account."""

    session, _, _ = _create_authorized_session(account_id, db_path=db_path)
    url = f"{BQClient.BASE_URL}/projects"
    effective_timeout = timeout if timeout is not None else BQClient.DEFAULT_TIMEOUT
    projects: list[dict[str, Any]] = []
    params = {"maxResults": 1000}
    token: Optional[str] = None

    while True:
        query = dict(params)
        if token:
            query["pageToken"] = token
        data = _request_json(session, "GET", url, timeout=effective_timeout, params=query)
        projects.extend(data.get("projects", []))
        token = data.get("nextPageToken") or data.get("pageToken") or None
        if not token:
            break

    return projects


def metadata_snapshot(
    project_id: str,
    *,
    account_id: Optional[int] = None,
    location: str = "US",
    datasets: Optional[list[str]] = None,
    sample_n: int = 3,
) -> dict[str, Any]:
    """Collect datasets/tables/schema/sample rows for later rendering."""

    client = BQClient(project_id=project_id, account_id=account_id, location=location)
    dataset_ids = datasets or client.list_datasets()

    snapshot: dict[str, Any] = {
        "projectId": project_id,
        "location": location,
        "datasets": {},
    }

    for dataset_id in dataset_ids:
        tables = client.list_tables(dataset_id)
        dataset_entry: dict[str, Any] = {"tables": {}}
        for table_id in tables:
            schema = client.get_table_schema(dataset_id, table_id)
            samples = client.sample_rows(dataset_id, table_id, max_results=sample_n)
            dataset_entry["tables"][table_id] = {
                "schema": schema,
                "sampleRows": samples,
            }
        snapshot["datasets"][dataset_id] = dataset_entry

    return snapshot
