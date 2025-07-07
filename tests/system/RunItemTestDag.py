from airflow import DAG
from datetime import datetime
from airflow.providers.microsoft.fabric.operators.fabric import FabricRunItemOperator

with DAG(
  dag_id="test_fabric_notebook_run",
  schedule_interval="@daily",
  start_date=datetime(2023, 8, 7),
  catchup=False,
) as dag:

  # Assumes the workspace_id and item_id are already set in the Airflow connection
  run_fabric_item = FabricRunItemOperator(
    task_id="run_fabric_item",
    fabric_conn_id="fabric_integration",
    workspace_id="0c01a268-cc37-4575-829b-88a716a26610",
    item_id="e6f12c73-75f1-4485-b543-2b9293c14236",
    job_type="RunNotebook",
    wait_for_termination=True,
    deferrable=True,
  )

  run_fabric_item