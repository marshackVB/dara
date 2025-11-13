"""
Task store package entrypoint.

Provides factory for selecting the desired backend and exports the global
task_store instance for use throughout the application.
"""

from __future__ import annotations

import os

from .base import TaskStatus, TaskStore


__all__ = [
    "TaskStatus",
    "TaskStore",
    "create_task_store",
    "task_store",
    "task_store_factory",
]


def create_task_store() -> TaskStore:
    """Instantiate the configured task store backend."""

    backend = os.getenv("TASK_STORE_BACKEND")
    
    if not backend:
        raise ValueError(
            "Task store configuration is missing. Set TASK_STORE_BACKEND."
            )
    backend = backend.strip().lower()

    if backend == "redis":
        try:
            from .redis_store import RedisTaskStore
        except ImportError as exc:
            raise ImportError(
                "Redis task store backend requested, but dependencies "
                "are missing. Install redis and ensure redis_store.py is available."
            ) from exc
        return RedisTaskStore()

    if backend == "lakebase":
        # Lazy import to avoid asyncpg dependency unless requested
        try:
            from .postgres_store import PostgreSQLTaskStore  # noqa: WPS433
        except ImportError as exc:  # pragma: no cover - temporary while optional backend
            raise ImportError(
                "PostgreSQL task store backend requested, but dependencies "
                "are missing. Install asyncpg and ensure postgres_store.py is available."
            ) from exc
        return PostgreSQLTaskStore()

    raise ValueError(
        f"Unsupported TASK_STORE_BACKEND '{backend}'. "
        "Supported values: redis or lakebase"
    )


# Global task store instance used by default across the application.
task_store: TaskStore = create_task_store()


def task_store_factory() -> TaskStore:
    """
    Convenience callable compatible with dependency injection systems.

    Returns the module-level task_store by default, but can be overridden
    or swapped in tests.
    """

    return task_store


