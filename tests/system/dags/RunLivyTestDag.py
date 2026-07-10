from airflow import DAG

from airflow.providers.microsoft.fabric.operators.run_item import (
    MSFabricLivyBatchOperator,
    MSFabricLivySessionOperator,
    MSFabricLivyBatchParameters,
)

WORKSPACE_ID = "cb9c7d63-3263-4996-9014-482eb8788007"
LAKEHOUSE_ID = "00000000-0000-0000-0000-000000000000"  # replace with a real Lakehouse id
APP = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/Files/livy/app.py"
WHEEL = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/Files/livy/lib.whl"

with DAG(
    dag_id="ci_livy_pipeline_dag",
    schedule=None,
    catchup=False,
) as dag:

    # Livy batch (deferrable) using explicit fields.
    runLivyBatchDeferred = MSFabricLivyBatchOperator(
        task_id="runLivyBatch_deferred",
        fabric_conn_id="fabric-integration",
        workspace_id=WORKSPACE_ID,
        lakehouse_id=LAKEHOUSE_ID,
        file=APP,
        py_files=[WHEEL],
        num_executors=1,
        executor_cores=8,
        executor_memory="56g",
        timeout=60 * 20,  # 20 minutes
        check_interval=15,
        deferrable=True,
    )

    # Livy batch (synchronous) using the fluent parameter builder.
    runLivyBatchSync = MSFabricLivyBatchOperator(
        task_id="runLivyBatch_sync",
        fabric_conn_id="fabric-integration",
        workspace_id=WORKSPACE_ID,
        lakehouse_id=LAKEHOUSE_ID,
        job_params=(
            MSFabricLivyBatchParameters()
            .set_file(APP)
            .add_py_file(WHEEL)
            .set_executors(num=1, cores=8, memory="56g")
            .set_name("ci-livy-batch-sync")
            .to_json()
        ),
        timeout=60 * 20,
        check_interval=15,
        deferrable=False,
    )

    # Livy session ("submission") — run interactive PySpark code and read output.
    runLivySession = MSFabricLivySessionOperator(
        task_id="runLivySession",
        fabric_conn_id="fabric-integration",
        workspace_id=WORKSPACE_ID,
        lakehouse_id=LAKEHOUSE_ID,
        code="print('Hello from a Fabric Livy session'); print(spark.range(5).count())",
        num_executors=1,
        executor_cores=8,
        executor_memory="56g",
        session_timeout=60 * 15,
        timeout=60 * 10,
        check_interval=10,
    )
