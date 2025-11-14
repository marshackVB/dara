import asyncio
from typing import Dict, Any
from uuid import uuid4

from rest_api.services.workflows.pipeline import StepContext
from rest_api.services.workflows.definitions import upload_and_process_file
from rest_api.services.task_store import task_store, TaskStatus
from rest_api.services.client import databricks_client


class WorkflowExecutor:
    """Registry-based executor for launching workflows by name"""
    
    def __init__(self, task_store=task_store, databricks_client=databricks_client):
        self.task_store = task_store
        self.databricks_client = databricks_client
        # Registry of available workflows - maps name to definition function
        self.workflows = {
            "upload_and_process_file": upload_and_process_file,
        }
    
    async def start_workflow_async(
        self,
        workflow_type: str,
        **kwargs
    ) -> str:
        """
        Start a workflow by type name.
        
        Args:
            workflow_type: Name of the workflow (e.g., "upload_and_process_file")
            **kwargs: Parameters for the workflow (file_contents, filename, catalog, schema, volume, job_id, job_params, etc.)
        
        Returns:
            Task ID for tracking progress
        """
        if workflow_type not in self.workflows:
            raise ValueError(f"Unknown workflow type: {workflow_type}. Available: {list(self.workflows.keys())}")
        
        # Separate file_contents from other params (file_contents shouldn't be stored in Redis)
        # Make it optional - workflows that need files can validate themselves
        file_contents = kwargs.pop('file_contents', None)
        
        # Extract job_id if provided (workflows that need it can validate)
        job_id = kwargs.pop('job_id', None)
        
        # Create task in the backend store (without file_contents - only store JSON-serializable params)
        task_id = str(uuid4())
        await self.task_store.create_task(
            task_id=task_id,
            task_type=workflow_type,
            params=kwargs,
            status=TaskStatus.PENDING
        )
        
        # Create context (with file_contents and job_id if provided)
        context = StepContext(
            task_id=task_id,
            file_contents=file_contents,
            job_id=job_id,
            **kwargs
        )
        
        # Get workflow definition function and create pipeline
        # Pass job_id only if provided (workflow function can validate if needed)
        workflow_func = self.workflows[workflow_type]
        if job_id is not None:
            pipeline = workflow_func(self.task_store, self.databricks_client, job_id=job_id)
        else:
            pipeline = workflow_func(self.task_store, self.databricks_client)
        
        # Execute in background
        asyncio.create_task(pipeline.execute(context))
        
        return task_id


# Global workflow executor instance
workflow_executor = WorkflowExecutor()
