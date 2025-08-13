from airflow import DAG
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator

with DAG(
  dag_id="test_fabric_notebook_run",
  catchup=False,
) as dag:

# Notebook
  runNotebook1 = MSFabricRunItemOperator(
    task_id="runNotebookTask1_deferred",
    fabric_conn_id="fabric-integration",
    workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
    item_id="5ea6c21f-dcb3-4c63-9c37-fe433ac6894b",
    job_type="RunNotebook",
    timeout=60 * 10, #10 minutes
    #job_params={"sleep_minutes": "5"}, not yet supported by the API
    deferrable=True
  )

  runNotebook2 = MSFabricRunItemOperator(
    task_id="runNotebookTask2_sync",
    fabric_conn_id="fabric-integration",
    workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
    item_id="5ea6c21f-dcb3-4c63-9c37-fe433ac6894b",
    job_type="RunNotebook",
    timeout=60 * 10, #10 minutes
    deferrable=False, 
  )

  # Pipeline
  runPipeline1 = MSFabricRunItemOperator(
    task_id="runPipelineTask1_deferred",
    fabric_conn_id="fabric-integration",
    workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
    item_id="3d99c6f8-b37e-4712-a80c-25c52b9e2ae2",
    job_type="Pipeline",
    timeout=60 * 10, #10 minutes
    # deferrable=True, # default value
  )

  runPipeline2 = MSFabricRunItemOperator(
    task_id="runPipelineTask2_deferred",
    fabric_conn_id="fabric-integration",
    workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
    item_id="3d99c6f8-b37e-4712-a80c-25c52b9e2ae2",
    job_type="Pipeline",
    timeout=60 * 10, #10 minutes
    deferrable=False,
  )

  # Tasks will run in parallel by default since there are no dependencies defined
  [runNotebook1, runNotebook2, runPipeline1, runPipeline2]