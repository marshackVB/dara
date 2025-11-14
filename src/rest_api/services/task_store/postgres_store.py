"""PostgreSQL-backed TaskStore implementation."""

from __future__ import annotations

import os
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
import asyncpg
import json
from .base import TaskStatus, TaskStore


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _env_int(name: str, default: int) -> int:
    """Ensure that environment variables expected to be integers can
    actually be parsed as an integers. If no environmen variable is available,
    return a default value. Used to validate the sql pools max size but could
    be used for additional variables added."""
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be an integer.") from exc

def _require_env(name: str, description: str) -> str:
    """
    Confirm that required environment variables are available and 
    return an informative message if they aren't.
    """
    value = os.getenv(name)
    if not value:
        raise ValueError(f"{name} must be set for Lakehouse authentication ({description}).")
    return value

def _validate_identifier(value: str, label: str) -> str:
    """Ensure any schema and/or tables names passed via TASK_STORE_POSTGRES_SCHEMA 
    / TASK_STORE_POSTGRES_TABLE conform to postgres naming conventions. Prevents
    SQL injection / invalid names.Names in SQL must begin with a letter (a-z) or underscore (_). 
    Subsequent characters in a name can be letters, digits (0-9), or underscores"""
    if not value:
        raise ValueError(f"{label} cannot be empty.")
    if not _IDENTIFIER_RE.match(value):
        raise ValueError(
            f"Invalid {label} '{value}'. "
            "Only alphanumeric characters and underscores are allowed, "
            "and the first character must be a letter or underscore."
        )
    return value

def _coerce_status(status: TaskStatus | str) -> TaskStatus:
    """Ensure we always work with TaskStatus enum values. This allows either the
    enum value or valid enum string representation to be passed via an API call;
    either way the enum value will be returned."""
    if isinstance(status, TaskStatus):
        return status
    return TaskStatus(str(status))

def _isoformat(dt: Optional[datetime]) -> Optional[str]:
    """Ensure date values are always iso formattted"""
    return dt.isoformat() if dt else None

@dataclass
class _ConnectionConfig:
    """Holds connection configuration for asyncpg.create_pool."""
    kwargs: Dict[str, Any]
    dsn: Optional[str] = None

class PostgreSQLTaskStore(TaskStore):
    """PostgreSQL implementation of the TaskStore abstraction.

    - Validates environment/configuration settings (schema, table, pool size).
    - Exposes the public async API (create/get/update/delete) while translating
      to/from JSON payloads and SQL rows.
    - Manages the Lakehouse connection pool and schema/table/index auto-creation."""
    def __init__(
        self,
        dsn: Optional[str] = None,
        *,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        ttl_seconds: Optional[int] = None,
        max_pool_size: Optional[int] = None,
    ) -> None:

        # Validate postgresql configuration for backend store
        self._connection_config = self._build_connection_config(dsn)

        self.schema = _validate_identifier(
            (schema or os.getenv("TASK_STORE_POSTGRES_SCHEMA") or "public"),
            "schema",
        )

        self.table = _validate_identifier(
            (table or os.getenv("TASK_STORE_POSTGRES_TABLE") or "workflow_tasks"),
            "table",
        )

        self._qualified_table = (
            f'{self._quote_identifier(self.schema)}.{self._quote_identifier(self.table)}'
        )

        self._status_index = _validate_identifier(f"{self.table}_status_idx", "index")

        self._created_at_index = _validate_identifier(
            f"{self.table}_created_at_idx", "index"
        )

        ttl_env = os.getenv("TASK_STORE_TTL_SECONDS")
        if ttl_seconds is not None:
            self.ttl_seconds = ttl_seconds if ttl_seconds > 0 else None

        elif ttl_env:
            parsed = int(ttl_env)
            self.ttl_seconds = parsed if parsed > 0 else None

        else:
            self.ttl_seconds = 86400  # 24 hours default

        if self.ttl_seconds is not None and self.ttl_seconds <= 0:
            self.ttl_seconds = None

        self.max_pool_size = max_pool_size or _env_int(
            "TASK_STORE_POSTGRES_POOL_SIZE", default = 10
        )

        self._pool: Optional[asyncpg.Pool] = None
        self._ddl_initialized: bool = False

    # Public API
    async def create_task(
        self,
        task_id: str,
        task_type: str,
        params: Dict,
        status: TaskStatus = TaskStatus.PENDING,
    ) -> Dict:
    """Insert a new task row and return the dictionary shape used by the API. 
    The WorkflowExecutor supplies the task fields as JSON-friendly values; this
    method persists them in Postgres and returns the normalized payload the
    FastAPI routes expect."""
        created_at = datetime.now(timezone.utc)
        expires_at = (
            created_at + timedelta(seconds=self.ttl_seconds)
            if self.ttl_seconds
            else None
        )

        status_enum = _coerce_status(status)

        async with self._connection() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self._qualified_table} (
                    id,
                    task_type,
                    status,
                    created_at,
                    started_at,
                    completed_at,
                    params,
                    result,
                    error,
                    expires_at
                )
                VALUES ($1, $2, $3, $4, NULL, NULL, $5::jsonb, NULL, NULL, $6)
                """,
                task_id,
                task_type,
                status_enum.value,
                created_at,
                params or {},
                expires_at,
            )

        return self._serialize_task(
            task_id=task_id,
            task_type=task_type,
            status=status_enum.value,
            created_at=created_at,
            params=params or {},
            result=None,
            error=None,
            started_at=None,
            completed_at=None,
            expires_at=expires_at,
        )

    async def get_task(self, task_id: str) -> Optional[Dict]:
        async with self._connection() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT
                    id,
                    task_type,
                    status,
                    created_at,
                    started_at,
                    completed_at,
                    params,
                    result,
                    error,
                    expires_at
                FROM {self._qualified_table}
                WHERE id = $1
                """,
                task_id,
            )

        if row is None:
            return None

        return self._serialize_row(row)

    async def update_task(self, task_id: str, **updates) -> Optional[Dict]:
        """To update a task, fetch the task's current row values. If the update is a
        task status update, determine if the task update requires start time or end time
        to be updated. If the update is of another type, overwrites"""
        async with self._connection() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT
                    id,
                    task_type,
                    status,
                    created_at,
                    started_at,
                    completed_at,
                    params,
                    result,
                    error,
                    expires_at
                FROM {self._qualified_table}
                WHERE id = $1
                FOR UPDATE
                """,
                task_id,
            )

            if row is None:
                return None

            status_value: str = row["status"]
            started_at: Optional[datetime] = row["started_at"]
            completed_at: Optional[datetime] = row["completed_at"]
            params: Dict = row["params"] or {}
            result: Optional[Dict] = row["result"]
            error: Optional[str] = row["error"]
            expires_at: Optional[datetime] = row["expires_at"]

            for key, value in updates.items():
                if key == "status":
                    status_enum = _coerce_status(value)
                    status_value = status_enum.value

                    if status_enum in (TaskStatus.UPLOADING, TaskStatus.PROCESSING):
                        if started_at is None:
                            started_at = datetime.now(timezone.utc)
                    elif status_enum in (TaskStatus.COMPLETED, TaskStatus.FAILED):
                        completed_at = datetime.now(timezone.utc)
                elif key == "params":
                    params = value or {}
                elif key == "result":
                    result = value
                elif key == "error":
                    error = value
                elif key == "started_at":
                    started_at = self._coerce_datetime(value)
                elif key == "completed_at":
                    completed_at = self._coerce_datetime(value)
                elif key == "expires_at":
                    expires_at = self._coerce_datetime(value)
                else:
                    # Ignore unknown keys to maintain compatibility
                    continue

            if self.ttl_seconds and "expires_at" not in updates:
                expires_at = datetime.now(timezone.utc) + timedelta(
                    seconds=self.ttl_seconds
                )

            await conn.execute(
                f"""
                UPDATE {self._qualified_table}
                SET
                    status = $2,
                    params = $3::jsonb,
                    result = $4::jsonb,
                    error = $5,
                    started_at = $6,
                    completed_at = $7,
                    expires_at = $8
                WHERE id = $1
                """,
                task_id,
                status_value,
                params,
                result,
                error,
                started_at,
                completed_at,
                expires_at,
            )

            return self._serialize_task(
                task_id=row["id"],
                task_type=row["task_type"],
                status=status_value,
                created_at=row["created_at"],
                params=params,
                result=result,
                error=error,
                started_at=started_at,
                completed_at=completed_at,
                expires_at=expires_at,
            )

    async def delete_task(self, task_id: str) -> None:
        async with self._connection() as conn:
            await conn.execute(
                f"DELETE FROM {self._qualified_table} WHERE id = $1",
                task_id,
            )

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    def _build_connection_config(self, dsn: Optional[str]) -> _ConnectionConfig:
        """Collect the Lakebase connection information that will be used to establish
        the asyncpg connection pool."""
        if dsn:
            return _ConnectionConfig(kwargs={}, dsn=dsn)

        host = _require_env("LAKEBASE_HOST", "hostname of the Lakehouse Postgres instance")
        port = int(_require_env("LAKEBASE_PORT", "port number"))
        role = _require_env("LAKEBASE_ROLE", "database role/user")
        password = _require_env("LAKEBASE_PASSWORD", "database role password")
        database = _require_env("LAKEBASE_DB_NAME", "target database name")
        instance_name = os.getenv("LAKEBASE_INSTANCE_NAME")

        kwargs: Dict[str, Any] = {
            "host": host,
            "port": port,
            "user": role,
            "database": database,
            "password": password,
        }
        return _ConnectionConfig(kwargs=kwargs, dsn=None)

    async def _init_connection(self, conn: asyncpg.Connection) -> None:
        """Install custom codecs so asyncpg automatically JSON-encodes Python dicts
        and decodes JSON/JSONB columns back into native Python objects."""
        await conn.set_type_codec(
            "json",
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog",
        )
        await conn.set_type_codec(
            "jsonb",
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog",
        )
            self._pool = None

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            kwargs = dict(self._connection_config.kwargs)
            if self._connection_config.dsn:
                kwargs["dsn"] = self._connection_config.dsn
            self._pool = await asyncpg.create_pool(
                min_size=1,
                max_size=self.max_pool_size,
                init=self._init_connection,
                **kwargs,
            )
        return self._pool

    @asynccontextmanager
    async def _connection(self) -> asyncpg.Connection:
        """Acquire a connection from the asyncpg pool, ensure the schema/table/index
        exist on first use, yield it for the caller’s query, and release it back
        to the pool afterward."""
        pool = await self._get_pool()
        conn = await pool.acquire()  # acquire a connection from the persisted pool
        try:
            if not self._ddl_initialized:
                if self.schema != "public":
                    await conn.execute(
                        f'CREATE SCHEMA IF NOT EXISTS {self._quote_identifier(self.schema)}'
                    )

                await conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self._qualified_table} (
                        id TEXT PRIMARY KEY,
                        task_type TEXT NOT NULL,
                        status TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL,
                        started_at TIMESTAMPTZ,
                        completed_at TIMESTAMPTZ,
                        params JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                        result JSONB,
                        error TEXT,
                        expires_at TIMESTAMPTZ
                    )
                    """
                )
                await conn.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS {self._quote_identifier(self._status_index)}
                    ON {self._qualified_table} (status)
                    """
                )
                await conn.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS {self._quote_identifier(self._created_at_index)}
                    ON {self._qualified_table} (created_at)
                    """
                )
                self._ddl_initialized = True
            yield conn
        finally:
            await pool.release(conn)  # return the connection to the pool

    def _serialize_row(self, row: asyncpg.Record) -> Dict[str, Any]:
        """Return a JSON-ready task dict with ISO8601 timestamps and normalized data."""
        return self._serialize_task(
            task_id=row["id"],
            task_type=row["task_type"],
            status=row["status"],
            created_at=row["created_at"],
            params=row["params"] or {},
            result=row["result"],
            error=row["error"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            expires_at=row["expires_at"],
        )

    def _serialize_task(
        self,
        *,
        task_id: str,
        task_type: str,
        status: str,
        created_at: datetime,
        params: Dict,
        result: Optional[Dict],
        error: Optional[str],
        started_at: Optional[datetime],
        completed_at: Optional[datetime],
        expires_at: Optional[datetime],
    ) -> Dict[str, Any]:
        """Normalize database values into the task payload shape used by the API.
        Converts datetimes to ISO8601 strings, ensures params/results are JSON-friendly,
        and keeps all standard fields (status, timestamps, error, etc.) in one dict."""
        return {
            "id": task_id,
            "task_type": task_type,
            "status": status,
            "created_at": _isoformat(created_at),
            "params": params or {},
            "result": result,
            "error": error,
            "started_at": _isoformat(started_at),
            "completed_at": _isoformat(completed_at),
            "expires_at": _isoformat(expires_at),
        }

    @staticmethod
    def _coerce_datetime(value: Any) -> Optional[datetime]:
        """Accept either a datetime or ISO8601 string (or None) and return a timezone-aware
        UTC datetime. Used to normalize timestamps coming from the database or API payloads."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt
        raise ValueError("Datetime values must be datetime objects or ISO 8601 strings.")

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Return an identifier wrapped in double quotes so it is safe to embed in SQL statements."""
        return f'"{identifier}"'
