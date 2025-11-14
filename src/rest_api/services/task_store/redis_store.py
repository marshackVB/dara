"""Redis-backed TaskStore implementation."""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Dict, Optional

import redis.asyncio as aioredis

from .base import TaskStatus, TaskStore


class RedisTaskStore(TaskStore):
    """
    Redis-backed task store used for tracking workflow execution.

    - Stores task payloads as JSON values with an expiration (TTL) for automatic cleanup.
    - Supports both a single `REDIS_URL` and individual host/port/password environment variables.
    - Implements the same public interface as the PostgreSQLTaskStore so routes can remain agnostic.
    """

    def __init__(self, redis_url: Optional[str] = None, ttl_seconds: int = 86400):
        """
        Configure the Redis connection URL and TTL.

        Args:
            redis_url: Optional Redis URL. Falls back to env vars when omitted.
            ttl_seconds: Number of seconds before a task entry expires (default 24h).
        """
        redis_url = redis_url or os.getenv("REDIS_URL")

        # If REDIS_URL not set, try individual parameters (like redis_om pattern)
        if not redis_url:
            host = os.getenv("REDIS_HOST")
            port = os.getenv("REDIS_PORT")
            password = os.getenv("REDIS_PASSWORD")

            if host:
                # Build URL from individual parameters
                port = port or "6379"  # Default Redis port
                if password:
                    redis_url = f"redis://:{password}@{host}:{port}"
                else:
                    redis_url = f"redis://{host}:{port}"
            else:
                raise ValueError(
                    "Redis configuration is missing. Set REDIS_URL or a combination of "
                    "(REDIS_HOST, REDIS_PORT and REDIS_PASSWORD)"
                )

        self.redis_url = redis_url
        self.redis: Optional[aioredis.Redis] = None
        self.task_prefix = "task:"
        self.ttl_seconds = ttl_seconds  # 24 hours by default

    async def _get_redis(self) -> aioredis.Redis:
        """Get or lazily create the shared Redis connection with basic connectivity checks."""
        if self.redis is None:
            try:
                self.redis = aioredis.from_url(
                    self.redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                    max_connections=10,
                )
                # Test connection
                await self.redis.ping()
            except Exception as exc:  # pragma: no cover - network errors are runtime concerns
                raise ConnectionError(
                    f"Failed to connect to Redis at {self.redis_url}. "
                    f"Error: {str(exc)}. "
                    "Make sure Redis is running or REDIS_URL "
                    "(or REDIS_HOST/REDIS_PORT/REDIS_PASSWORD) is set correctly."
                ) from exc
        return self.redis

    async def create_task(
        self,
        task_id: str,
        task_type: str,
        params: Dict,
        status: TaskStatus = TaskStatus.PENDING,
    ) -> Dict:
        """Create a new task entry and return the JSON payload that was stored."""
        task = {
            "id": task_id,
            "task_type": task_type,
            "status": status.value,
            "created_at": datetime.utcnow().isoformat(),
            "params": params,
            "result": None,
            "error": None,
            "started_at": None,
            "completed_at": None,
        }

        redis = await self._get_redis()
        key = f"{self.task_prefix}{task_id}"
        await redis.setex(key, self.ttl_seconds, json.dumps(task))
        return task

    async def get_task(self, task_id: str) -> Optional[Dict]:
        """Retrieve a task by ID, returning the stored JSON as a dictionary if found."""
        redis = await self._get_redis()
        key = f"{self.task_prefix}{task_id}"
        data = await redis.get(key)
        if data:
            return json.loads(data)
        return None

    async def update_task(self, task_id: str, **updates) -> Optional[Dict]:
        """Update the task payload in-place, adjusting status-derived timestamps when needed.
        Returns the updated task dict, or None if the task does not exist."""
        task = await self.get_task(task_id)
        if not task:
            return None

        # Update fields
        for key, value in updates.items():
            if key == "status" and isinstance(value, TaskStatus):
                task["status"] = value.value
            else:
                task[key] = value

        # Update timestamps
        if "status" in updates:
            status = updates["status"]
            if isinstance(status, TaskStatus):
                status_value = status
            else:
                # allow raw string values if provided
                status_value = TaskStatus(status) if status else None

            if status_value in (TaskStatus.UPLOADING, TaskStatus.PROCESSING):
                if task.get("started_at") is None:
                    task["started_at"] = datetime.utcnow().isoformat()
            elif status_value in (TaskStatus.COMPLETED, TaskStatus.FAILED):
                task["completed_at"] = datetime.utcnow().isoformat()

        # Save back to Redis
        redis = await self._get_redis()
        key = f"{self.task_prefix}{task_id}"
        await redis.setex(key, self.ttl_seconds, json.dumps(task))
        return task

    async def delete_task(self, task_id: str) -> None:
        """Delete a task entry from Redis."""
        redis = await self._get_redis()
        key = f"{self.task_prefix}{task_id}"
        await redis.delete(key)

    async def close(self) -> None:
        """Close the shared Redis connection (idempotent)."""
        if self.redis:
            await self.redis.aclose()
            self.redis = None

