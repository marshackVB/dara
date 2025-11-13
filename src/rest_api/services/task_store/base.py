"""
Task store abstractions and shared types.

This module defines the core contract that any task store backend
must satisfy so that different persistence layers (Redis, Postgres, etc.)
can be used interchangeably within the application.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Optional


class TaskStatus(str, Enum):
    """Enumeration of task lifecycle states."""

    PENDING = "pending"
    UPLOADING = "uploading"
    UPLOADED = "uploaded"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class TaskStore(ABC):
    """Abstract interface for workflow task persistence backends."""

    @abstractmethod
    async def create_task(
        self,
        task_id: str,
        task_type: str,
        params: Dict,
        status: TaskStatus = TaskStatus.PENDING,
    ) -> Dict:
        """Create a new task entry."""

    @abstractmethod
    async def get_task(self, task_id: str) -> Optional[Dict]:
        """Fetch a task record by ID."""

    @abstractmethod
    async def update_task(self, task_id: str, **updates) -> Optional[Dict]:
        """Update fields on a task record."""

    @abstractmethod
    async def delete_task(self, task_id: str) -> None:
        """Delete a task record."""

    @abstractmethod
    async def close(self) -> None:
        """Release any underlying resources (connections, pools, etc.)."""

