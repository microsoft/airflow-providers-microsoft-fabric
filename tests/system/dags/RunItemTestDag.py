from airflow import DAG
from datetime import datetime
from airflow.providers.microsoft.fabric.operators.fabric import MSFabricRunItemOperator

with DAG(
  dag_id="test_fabric_notebook_run",
  catchup=False,
) as dag:

  # Assumes the workspace_id and item_id are already set in the Airflow connection
  run_fabric_item_1 = MSFabricRunItemOperator(
    task_id="run_fabric_item_1",
    fabric_conn_id="fabric_integration",
    workspace_id="988a1272-9da5-4936-be68-39e9b62d85ef",
    item_id="38579073-b94d-4b88-86e5-898bc15a2542",
    job_type="RunNotebook",
    wait_for_termination=True,
    deferrable=True,
  )

  run_fabric_item_2 = MSFabricRunItemOperator(
    task_id="run_fabric_item_2",
    fabric_conn_id="fabric_integration",
    workspace_id="988a1272-9da5-4936-be68-39e9b62d85ef",
    item_id="38579073-b94d-4b88-86e5-898bc15a2542",
    job_type="RunNotebook",
    wait_for_termination=True,
    deferrable=False,
  )

  run_fabric_item_3 = MSFabricRunItemOperator(
  task_id="run_fabric_item_3",
  fabric_conn_id="fabric_integration",
  workspace_id="988a1272-9da5-4936-be68-39e9b62d85ef",
  item_id="38579073-b94d-4b88-86e5-898bc15a2542",
  job_type="RunNotebook",
  wait_for_termination=False)

  # Tasks will run in parallel by default since there are no dependencies defined
  [run_fabric_item_1, run_fabric_item_2, run_fabric_item_3]