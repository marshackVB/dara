from rest_api.services.workflows.pipeline import WorkflowStep, StepContext
from rest_api.services.task_store import TaskStatus


class UploadFileStep(WorkflowStep):
    """Step: Upload file to Unity Catalog"""

    async def execute(self, context: StepContext) -> StepContext:
        await self.task_store.update_task(
            context.task_id,
            status=TaskStatus.UPLOADING
        )

        volume_path = f"/Volumes/{context.catalog}/{context.schema}/{context.volume}/{context.filename}"

        await self.databricks_client.upload_file(
            file_path=volume_path,
            contents=context.file_contents,
            overwrite=True
        )

        context.volume_path = volume_path

        await self.task_store.update_task(
            context.task_id,
            status=TaskStatus.UPLOADED,
            result={"volume_path": volume_path, "filename": context.filename}
        )
        return context