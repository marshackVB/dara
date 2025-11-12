"""
Workflow endpoints for file upload and processing.
Uses modular workflow system with Redis task tracking.
"""
import os
from typing import Dict, Any, Optional
from fastapi import APIRouter, File, UploadFile, HTTPException, Query
from rest_api.services.workflows.executor import workflow_executor
from rest_api.services.task_store import task_store

router = APIRouter()

CATALOG = os.getenv("CATALOG", "main")
SCHEMA = os.getenv("SCHEMA", "default")
VOLUME = os.getenv("VOLUME", "uploads")


@router.post("/workflow/index-file")
async def upload_and_process(
    file: UploadFile = File(...),
    catalog: str = Query(default=CATALOG, description="Unity Catalog catalog name"),
    schema: str = Query(default=SCHEMA, description="Unity Catalog schema name"),
    volume: str = Query(default=VOLUME, description="Unity Catalog volume name"),
    job_id: Optional[int] = Query(default=None, description="Databricks job ID (uses JOB_ID env var if not provided)"),
    workflow_type: str = Query(default="upload_and_process_file", description="Workflow type to execute")
) -> Dict[str, Any]:
    """
    Upload a file and trigger a processing workflow.
    
    This endpoint:
    1. Uploads file to Unity Catalog Volume
    2. Triggers the specified Databricks workflow
    3. Returns task ID for tracking progress
    
    The workflow runs in the background. Use the task ID to:
    - Poll for status: GET /api/v1/workflow/status/{task_id}
    
    Args:
        file: File to upload and process
        catalog: Unity Catalog catalog name
        schema: Unity Catalog schema name
        volume: Unity Catalog volume name
        job_id: Databricks job ID (optional, uses JOB_ID env var if not provided)
        workflow_type: Type of workflow to execute (default: "upload_and_process_file")
    
    Returns:
        Dict with task_id, status, and filename
    """
    try:
        # Read file contents
        file_contents = await file.read()
        
        # Get job_id from parameter or environment
        if job_id is None:
            job_id = int(os.getenv("JOB_ID", "0"))
            if job_id == 0:
                raise ValueError("job_id must be provided as parameter or JOB_ID environment variable must be set")
        
        # Start workflow in background
        task_id = await workflow_executor.start_workflow_async(
            workflow_type=workflow_type,
            file_contents=file_contents,
            filename=file.filename or "uploaded_file",
            catalog=catalog,
            schema=schema,
            volume=volume,
            job_id=job_id
        )
        
        return {
            "task_id": task_id,
            "status": "started",
            "message": "Workflow started. Use task_id to track progress.",
            "filename": file.filename,
            "workflow_type": workflow_type
        }
        
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start workflow: {str(e)}"
        )


@router.get("/workflow/status/{task_id}")
async def get_workflow_status(task_id: str) -> Dict[str, Any]:
    """
    Get the current status of a workflow task.
    
    Use this endpoint to poll for task completion.
    Recommended: Poll every 2-5 seconds.
    
    Args:
        task_id: Task ID from workflow endpoint
    
    Returns:
        Task status with result/error information
    """
    task = await task_store.get_task(task_id)
    
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return task


@router.get("/workflow/types")
async def get_workflow_types() -> Dict[str, Any]:
    """
    Get list of available workflow types with descriptions.
    
    Returns:
        Dict with available workflow types and their descriptions
    """
    workflow_info = {}
    for name, workflow_func in workflow_executor.workflows.items():
        if workflow_func.__doc__:
            # Strip whitespace and replace newlines with spaces
            description = ' '.join(workflow_func.__doc__.strip().split())
        else:
            description = "No description available"
        
        workflow_info[name] = {
            "name": name,
            "description": description
        }
    
    return {
        "workflow_types": workflow_info,
        "count": len(workflow_executor.workflows)
    }
