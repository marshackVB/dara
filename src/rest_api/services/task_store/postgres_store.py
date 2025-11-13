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


def _env_bool(name: str, default: bool = False) -> bool:
    """Parse boolean environment variables."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    """Parse integer environment variables."""
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be an integer.") from exc


def _validate_identifier(value: str, label: str) -> str:
    """Ensure SQL identifiers are safe to interpolate."""
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
    """Ensure we always work with TaskStatus enum values."""
    if isinstance(status, TaskStatus):
        return status
    return TaskStatus(str(status))


def _isoformat(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None


@dataclass
class _ConnectionConfig:
    """Holds connection configuration for asyncpg.create_pool."""

    kwargs: Dict[str, Any]
    dsn: Optional[str] = None


class PostgreSQLTaskStore(TaskStore):
    """PostgreSQL implementation of the TaskStore abstraction."""

    def __init__(
        self,
        dsn: Optional[str] = None,
        *,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        ttl_seconds: Optional[int] = None,
        max_pool_size: Optional[int] = None,
    ) -> None:
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
            "TASK_STORE_POSTGRES_POOL_SIZE", default=10
        )

        self._pool: Optional[asyncpg.Pool] = None

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    async def create_task(
        self,
        task_id: str,
        task_type: str,
        params: Dict,
        status: TaskStatus = TaskStatus.PENDING,
    ) -> Dict:
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
            self._pool = None

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

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
        pool = await self._get_pool()
        conn = await pool.acquire()
        try:
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
            yield conn
        finally:
            await pool.release(conn)

    async def _init_connection(self, conn: asyncpg.Connection) -> None:
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

    def _serialize_row(self, row: asyncpg.Record) -> Dict[str, Any]:
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
        return f'"{identifier}"'

    def _build_connection_config(self, dsn: Optional[str]) -> _ConnectionConfig:
        if dsn:
            return _ConnectionConfig(kwargs={}, dsn=dsn)

        host = os.getenv("LAKEBASE_HOST")
        if not host:
            raise ValueError(
                "PostgreSQL configuration is missing. Set LAKEBASE_HOST "
                "to the hostname of your Lakehouse Postgres instance."
            )

        port = int(os.getenv("LAKEBASE_PORT") 
        )
        if not port:
            raise ValueError("LAKEBASE_PORT must be set for Lakehouse authentication.")

        role = os.getenv("LAKEBASE_ROLE")
        if not role:
            raise ValueError("LAKEBASE_ROLE must be set for Lakehouse authentication.")

        password = os.getenv("LAKEBASE_PASSWORD")
        if not password:
            raise ValueError("LAKEBASE_PASSWORD must be set for Lakehouse authentication.")

        database = os.getenv("LAKEBASE_DB_NAME")
        if not database:
            raise ValueError("LAKEBASE_DB_NAME must be set to select the target database.")

        instance_name = os.getenv("LAKEBASE_INSTANCE_NAME")

        kwargs: Dict[str, Any] = {
            "host": host,
            "port": port,
            "user": role,
            "database": database,
            "password": password,
        }
        return _ConnectionConfig(kwargs=kwargs, dsn=None)

