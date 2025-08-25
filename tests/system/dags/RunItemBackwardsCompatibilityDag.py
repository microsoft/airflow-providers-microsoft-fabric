from airflow import DAG
from datetime import datetime
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator

with DAG(
    dag_id="fabric_run_item_dag_1",
    start_date=datetime(2025, 8, 21),
    catchup=False,
    schedule_interval=None
) as dag:

    # Pipeline
    run_item = MSFabricRunItemOperator(
        task_id="runPipelineTask1",
        fabric_conn_id="fabric-integration",
        workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",  # AirflowTest
        item_id="3d99c6f8-b37e-4712-a80c-25c52b9e2ae2",  # pipeline1
        job_type="RunPipeline",
        wait_for_termination=True,
        deferrable=True
    )