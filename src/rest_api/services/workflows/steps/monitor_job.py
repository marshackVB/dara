import asyncio
from datetime import datetime
from rest_api.services.workflows.pipeline import WorkflowStep, StepContext
from rest_api.services.task_store import TaskStatus


class MonitorJobStep(WorkflowStep):
    """Step: Monitor Databricks job until completion"""
    
    def __init__(self, task_store, databricks_client, max_monitoring_time: int = 600, poll_interval: int = 5):
        super().__init__(task_store, databricks_client)
        self.max_monitoring_time = max_monitoring_time  # 10 minutes default
        self.poll_interval = poll_interval  # 5 seconds default
    
    async def execute(self, context: StepContext) -> StepContext:
        """Monitor job completion with polling"""
        if not context.run_id:
            raise ValueError("run_id is required in context for monitoring")
        
        start_monitoring = datetime.now()
        
        while True:
            await asyncio.sleep(self.poll_interval)
            
            # Check for timeout
            elapsed = (datetime.now() - start_monitoring).total_seconds()
            if elapsed > self.max_monitoring_time:
                await self.task_store.update_task(
                    context.task_id,
                    status=TaskStatus.FAILED,
                    error=f"Job monitoring timeout after {self.max_monitoring_time} seconds. Job may still be running."
                )
                return context
            
            # Get job status
            run_data = await self.databricks_client.get_run(run_id=int(context.run_id))
            state = run_data.get("state", {})
            life_cycle_state = state.get("life_cycle_state")
            
            if life_cycle_state == "TERMINATED":
                result_state = state.get("result_state")
                
                # Get existing result to preserve it
                current_task = await self.task_store.get_task(context.task_id)
                current_result = current_task.get("result", {}) if current_task else {}
                
                if result_state == "SUCCESS":
                    # Job completed successfully
                    await self.task_store.update_task(
                        context.task_id,
                        status=TaskStatus.COMPLETED,
                        result={
                            **current_result,
                            "run_id": context.run_id,
                            "job_id": context.job_id,
                            "message": "Job completed successfully",
                            # "job_result": run_data  # Commented out to avoid storing full job metadata
                        }
                    )
                    return context
                else:
                    # Job failed
                    error_msg = state.get("state_message", "Job execution failed")
                    await self.task_store.update_task(
                        context.task_id,
                        status=TaskStatus.FAILED,
                        error=f"Job failed: {error_msg}",
                        result={
                            **current_result,
                            "run_id": context.run_id,
                            "job_id": context.job_id
                        }
                    )
                    return context