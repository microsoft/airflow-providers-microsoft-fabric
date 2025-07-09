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
  run_fabric_item_1 = FabricRunItemOperator(
    task_id="run_fabric_item_1",
    fabric_conn_id="FabricDailyConn",
    workspace_id="50fe240b-100b-485f-a434-b1e188d00637",
    item_id="08d738b5-d8ef-4730-8b10-d08d8dc5b4f7",
    job_type="RunNotebook",
    wait_for_termination=True,
    deferrable=True,
  )

  run_fabric_item_2 = FabricRunItemOperator(
    task_id="run_fabric_item_2",
    fabric_conn_id="FabricDailyConn",
    workspace_id="50fe240b-100b-485f-a434-b1e188d00637",
    item_id="08d738b5-d8ef-4730-8b10-d08d8dc5b4f7",
    job_type="RunNotebook",
    wait_for_termination=True,
    deferrable=False,
  )

  run_fabric_item_3 = FabricRunItemOperator(
  task_id="run_fabric_item_3",
  fabric_conn_id="FabricDailyConn",
  workspace_id="50fe240b-100b-485f-a434-b1e188d00637",
  item_id="08d738b5-d8ef-4730-8b10-d08d8dc5b4f7",
  job_type="RunNotebook",
  wait_for_termination=False)

  # Tasks will run in parallel by default since there are no dependencies defined
  [run_fabric_item_1, run_fabric_item_2, run_fabric_item_3]