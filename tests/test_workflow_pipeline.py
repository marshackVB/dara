from unittest.mock import AsyncMock

import pytest

from rest_api.services.workflows.pipeline import WorkflowPipeline, WorkflowStep, StepContext
from rest_api.services.task_store import TaskStatus


class RecordingStep(WorkflowStep):
    def __init__(self, task_store, databricks_client, label, recorder):
        super().__init__(task_store, databricks_client)
        self.label = label
        self.recorder = recorder

    async def execute(self, context: StepContext) -> StepContext:
        self.recorder.append(self.label)
        return context


class FailingStep(WorkflowStep):
    async def execute(self, context: StepContext) -> StepContext:
        raise RuntimeError("boom")


@pytest.mark.asyncio
async def test_pipeline_executes_steps_in_order():
    recorder: list[str] = []
    task_store = AsyncMock()
    databricks_client = object()

    steps = [
        RecordingStep(task_store, databricks_client, "step-1", recorder),
        RecordingStep(task_store, databricks_client, "step-2", recorder),
    ]
    pipeline = WorkflowPipeline(steps, task_store, databricks_client)

    context = StepContext(task_id="task-123")
    result = await pipeline.execute(context)

    # Step execution order is correct
    assert recorder == ["step-1", "step-2"]
    # StepContext passed through correctly
    assert result is context
    # Update task only called by WorkflowPipeline if Step execution fails
    task_store.update_task.assert_not_called()


@pytest.mark.asyncio
async def test_pipeline_marks_task_failed_on_exception():
    task_store = AsyncMock()
    databricks_client = object()
    steps = [FailingStep(task_store, databricks_client)]
    pipeline = WorkflowPipeline(steps, task_store, databricks_client)
    context = StepContext(task_id="task-999")

    with pytest.raises(RuntimeError, match="boom"):
        await pipeline.execute(context)

    task_store.update_task.assert_awaited_once_with(
        context.task_id, status=TaskStatus.FAILED, error="boom"
    )

