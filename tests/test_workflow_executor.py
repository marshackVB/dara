import os

# Not used but required to be set by task_store.create_task_store
os.environ.setdefault("TASK_STORE_BACKEND", "redis")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379")

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from rest_api.services.workflows.executor import WorkflowExecutor
from rest_api.services.workflows.pipeline import StepContext
from rest_api.services.task_store import TaskStatus



@pytest.mark.asyncio
async def test_start_workflow_async_dispatches_pipeline(monkeypatch):
    """
    Test that WorkflowExecutor properly...
     - Correctly updates the TastStore and StepContext
    By doing the following... 
     - Build a dummy Step that records the StepContext.
     - Register a test workflow that asserts the executor passes the expected dependencies.
     - Monkeypatch asyncio.create_task so the scheduled coroutine is captured and awaited.

    Notes: 
        The WorkflowExecutor's start_workflow_async method calls asyncio.create_task, which normally schedules 
        the WorkflowPipeline to run in the background. That makes assertions tricky - test code could run 
        before the task finishes. To avoid that, the test monkeypatches create_task to return the coroutine 
        instead of scheduling it. The test can then await the coroutine explicitly, ensuring it completes 
        before any assertions that depend on its results.
    """

    task_store = AsyncMock()
    databricks_client = AsyncMock()
    executor = WorkflowExecutor(task_store=task_store, databricks_client=databricks_client)

    captured_context: dict[str, StepContext] = {}

    async def execute(context: StepContext):
        """A simulated Step that logs the StepContext it receives"""
        captured_context["context"] = context


    class DummyPipeline:
        """A simulated WorkflowPipeline that calls the simulated Step"""
        async def execute(self, context: StepContext):
            await execute(context)

    pipeline = DummyPipeline()

    def workflow_factory(task_store_param, databricks_param, job_id=None):
        """
        A workflow definition that tests its passed parameters and returns its pipeline.
        """
        assert task_store_param is task_store
        assert databricks_param is databricks_client
        assert job_id == 123
        return pipeline

    executor.workflows = {"custom": workflow_factory} # Make WorkflowExecutor aware of the workflow definition

    # Monkeypatch asyncio.create_task
    scheduled: dict[str, asyncio.Future] = {}

    def fake_create_task(coro):
        scheduled["coro"] = coro
        return SimpleNamespace()

    monkeypatch.setattr(asyncio, "create_task", fake_create_task)

    task_id = await executor.start_workflow_async(
        "custom", # Run the workflow_factory, triggers assert statements on parameters it receives,
                  # returns a DummyPipeline that logs it's passed StepContext
        file_contents=b"data-bytes", # An example parsed file input
        # Additional workflow input kwargs
        job_id=123,
        filename="demo.txt",
        catalog="main",
    )

    await scheduled["coro"] # Run the workflow coroutine

    # Test that new task successfully created in TaskStore
    args, kwargs = task_store.create_task.await_args
    assert args == () # Confirm no position arguments are passed (only keyword)
    assert kwargs["task_type"] == "custom"
    assert kwargs["status"] is TaskStatus.PENDING
    assert kwargs["params"] == {"filename": "demo.txt", "catalog": "main"}

    # Updated StepContext returned by WorkflowExecutor
    context = captured_context["context"]
    assert isinstance(context, StepContext)
    assert context.task_id == task_id
    assert context.file_contents == b"data-bytes"
    assert context.job_id == 123
    assert context.filename == "demo.txt"
    assert context.catalog == "main"


@pytest.mark.asyncio
async def test_start_workflow_async_without_job_id(monkeypatch):
    task_store = AsyncMock()
    databricks_client = AsyncMock()
    executor = WorkflowExecutor(task_store=task_store, databricks_client=databricks_client)

    async def execute(context: StepContext):
        return context

    class DummyPipeline:
        async def execute(self, context: StepContext):
            return await execute(context)

    executor.workflows = {"custom": lambda *args, **kwargs: DummyPipeline()}

    scheduled_no_job: dict[str, asyncio.Future] = {}

    def fake_create_task_no_job(coro):
        scheduled_no_job["coro"] = coro
        return SimpleNamespace()

    monkeypatch.setattr(asyncio, "create_task", fake_create_task_no_job)

    await executor.start_workflow_async("custom", filename="foo.txt")

    await scheduled_no_job["coro"]

    kwargs = task_store.create_task.await_args.kwargs
    assert kwargs["params"] == {"filename": "foo.txt"}


@pytest.mark.asyncio
async def test_start_workflow_async_unknown_type():
    executor = WorkflowExecutor(task_store=AsyncMock(), databricks_client=AsyncMock())
    with pytest.raises(ValueError):
        await executor.start_workflow_async("does-not-exist")