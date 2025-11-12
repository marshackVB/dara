from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from dataclasses import dataclass, field 

from rest_api.services.task_store import TaskStatus

@dataclass
class StepContext:
    """Shared context passed between workflow steps"""
    task_id: str
    file_contents: Optional[bytes] = None
    filename: Optional[str] = None
    catalog: Optional[str] = None
    schema: Optional[str] = None
    volume: Optional[str] = None
    volume_path: Optional[str] = None
    run_id: Optional[int] = None
    job_id: Optional[int] = None
    job_params: Optional[Dict[str, Any]] = field(default_factory=dict)
    # Add other shared data as needed


class WorkflowStep(ABC):
    """Base class for workflow steps"""

    def __init__(self, task_store, databricks_client):
        self.task_store = task_store
        self.databricks_client = databricks_client

    @abstractmethod
    async def execute(self, context: StepContext) -> StepContext:
        """Execute the step and return updated context"""
        pass



class WorkflowPipeline:
    """Orchestrates execution of workflow steps"""
    def __init__(self, steps: list[WorkflowStep], task_store, databricks_client):
        self.steps = steps
        self.task_store = task_store
        self.databricks_client = databricks_client

    async def execute(self, context: StepContext) -> StepContext:
        """Execute all steps in sequence"""
        try:
            for step in self.steps:
                context = await step.execute(context)
            return context
        except Exception as e:
            await self.task_store.update_task(
                context.task_id,
                status=TaskStatus.FAILED,
                error=str(e)
            )
            raise