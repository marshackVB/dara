from rest_api.services.workflows.pipeline import WorkflowStep, StepContext
from rest_api.services.task_store import TaskStatus


class RunJobStep(WorkflowStep):
    """Step: Trigger Databricks job. Currently configured to run only a Notebook
    as a job, passing in parameters if available"""

    def __init__(self, task_store, databricks_client, job_id: int):
        super().__init__(task_store, databricks_client)
        self.job_id = job_id

    async def execute(self, context: StepContext) -> StepContext:
        await self.task_store.update_task(
            context.task_id,
            status=TaskStatus.JOB_RUNNING
        )

        params = dict(context.job_params or {})

        run_response = await self.databricks_client.run_job(
            job_id = self.job_id,
            notebook_params = params
        )

        context.run_id = run_response.get("run_id")
        context.job_id = self.job_id

        # Get existing result to preserve it
        current_task = await self.task_store.get_task(context.task_id)
        current_result = current_task.get("result", {}) if current_task else {}
        
        await self.task_store.update_task(
            context.task_id,
            status=TaskStatus.PROCESSING,
            result={
                **current_result,
                "run_id": context.run_id,
                "job_id": self.job_id
            }
        )
        return context