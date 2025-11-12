from rest_api.services.workflows import WorkflowPipeline, UploadFileStep, RunJobStep, MonitorJobStep

def upload_and_process_file(task_store, databricks_client, *, job_id: int) -> WorkflowPipeline:
    """
    Upload a file to a Unity Catalog Volume, then proceed to launch a Databricks Job. Presumably,
    the job would contain a parameter indicating the location of the file to be processed.
    """
    if job_id is None:
        raise ValueError("job_id is required for upload_and_process_file workflow")

    steps = [
        UploadFileStep(task_store, databricks_client),
        RunJobStep(task_store, databricks_client, job_id=job_id),
        MonitorJobStep(task_store, databricks_client)
    ]

    return WorkflowPipeline(steps=steps, task_store=task_store, databricks_client=databricks_client)