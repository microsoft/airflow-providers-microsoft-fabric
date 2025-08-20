from airflow import DAG
from airflow.providers.microsoft.fabric.operators.job_scheduler import MSFabricJobSchedulerOperator
from airflow.providers.microsoft.fabric.operators.user_data_function import MSFabricUserDataFunctionOperator

with DAG(
  dag_id="test_fabric_notebook_run",
  catchup=False,
) as dag:

# Notebook
  runNotebook1 = MSFabricJobSchedulerOperator(
    task_id="runNotebookTask1_deferred",
    fabric_conn_id="fabric-integration",
    workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
    item_id="5ea6c21f-dcb3-4c63-9c37-fe433ac6894b",
    job_type="RunNotebook",
    timeout=60 * 10, #10 minutes
    #job_params={"sleep_minutes": "5"}, not yet supported by the API
    deferrable=True
  )

  runNotebook2 = MSFabricJobSchedulerOperator(
    task_id="runNotebookTask2_sync",
    fabric_conn_id="fabric-integration",
    workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
    item_id="5ea6c21f-dcb3-4c63-9c37-fe433ac6894b",
    job_type="RunNotebook",
    timeout=60 * 10, #10 minutes
    deferrable=False, 
  )

  # Pipeline
  runPipeline1 = MSFabricJobSchedulerOperator(
    task_id="runPipelineTask1_deferred",
    fabric_conn_id="fabric-integration",
    workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
    item_id="3d99c6f8-b37e-4712-a80c-25c52b9e2ae2",
    job_type="Pipeline",
    timeout=60 * 10, #10 minutes
    # deferrable=True, # default value
  )

  runPipeline2 = MSFabricJobSchedulerOperator(
    task_id="runPipelineTask2_deferred",
    fabric_conn_id="fabric-integration",
    workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
    item_id="3d99c6f8-b37e-4712-a80c-25c52b9e2ae2",
    job_type="Pipeline",
    timeout=60 * 10, #10 minutes
    deferrable=False,
  )

  runFunction = MSFabricUserDataFunctionOperator(
    task_id="run_user_data_function",
    fabric_conn_id="fabric-integration",
    workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
    item_id="1cc9a6ff-862b-4c13-8685-750a2103c858",
    item_name="MyFunc1",
  )


  # Tasks will run in parallel by default since there are no dependencies defined
  [runNotebook1, runNotebook2, runPipeline1, runPipeline2]