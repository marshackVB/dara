## DARA - The Databricks Async REST API Framework for Apps

DARA is a FastAPI framework for building REST APIs that orchestrate Databricks activities. It provides a composable, [pipeline-based architecture](https://en.wikipedia.org/wiki/Pipeline_(software)) for managing async workflows with built-in task tracking, job execution, and monitoring. Use DARA to integrate Databricks workflow execution into your other Databricks apps. For example, your app users can upload and process raw files, inserting contents into a Vector Index for Agent consumption.

### Concepts:
**Steps:**  

Steps are individual and independent processing steps, such as uploading a file or running a Databricks job. Steps have a common interface (the execute method). They also share state, a StepContext. Steps require a StepContext as an input and return an updated StepContext. Steps are chained together using a WorkflowPipeline, which executes steps in a sequenced, passing the update state from one step to the next. You can create new steps to trigger any Databricks activies accesseable via the [Databricks Rest API](https://docs.databricks.com/api/workspace/introduction) (see caveates below).

**WorkflowPipeline:**  

Executes a workflow definition (series of steps). Workflow defitions are defined in the definitions directory. WorkflowPipelines are ultimately triggered to execute a sequence of steps you've definined.

**WorkflowExecutor:**

The entry point for all WorkflowPipeline execution. The WorkflowExecutor has a reference to all available workflow definitions. It configures the initial state of the workflow, creates a unique workflow id, and executes the relevent WorkflowPipeline. The Fast API routes defined in v1/workflow.py call the WorkflowExecutor.


#### To create your own workflow, follow these steps.  

1. Create a new step if one that performs the action you need does not already exist.

2. Consider if you need to update the StepContext data class to include more parameters. This is the information relvent to your step that can be shared with other steps, for example, a job id that will need to be referenced by proceeding steps.

3. Create a new workflow definition that defines the series of steps you want to execute, such as upload a file to a UC volume, execute a job that processes that file, etc.

4. At the new workflow information to the WorkflowExecutor, which your Fast API routes will call.

5. Add a new Fast API route to your v1/workflow file that triggers your new workflow definition.  

<br>
<br>

**Special notes:**  

This project leverages asyncio and is fully asynchronous. Since the Databricks Python SDK  operations are synchronous, this project implements its own async Databricks REST API client. When adding new steps, you may need to add a new entry in `client.py` to call the relevant Databricks API asynchronously.

The project also implements a task store using Redis. The task store allows tasks to be 
tracked over time. For instance, a task can be launched that may take several minutes. 
Steps update the task's state in the task store, allowing for features like task polling 
by exposing a `GET /api/v1/workflow/status/{task_id}` route in FastAPI.
