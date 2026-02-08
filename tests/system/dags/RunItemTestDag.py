from airflow import DAG

from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunJobOperator
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunUserDataFunctionOperator
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunSemanticModelRefreshOperator
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricNotebookJobParameters
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricPipelineJobParameters

with DAG(
  dag_id="ci_pipeline_dag",
  catchup=False,
) as dag:

  # Semantic Model - runs in daily only due to license issues.
  # runSemanticModel1 = MSFabricRunSemanticModelRefreshOperator(
  #   task_id="run_semantic_model_refresh",
  #   fabric_conn_id="fabric-powerbi",
  #   workspace_id="4358996c-23ee-4c85-8728-df1825fcc196",
  #   item_id="2dc933ab-ffd4-46f4-ba1a-27cb1219a20f",
  #   timeout=60 * 10, #10 minutes
  #   deferrable=False,
  #   api_host="https://dailyapi.fabric.microsoft.com")

  # runSemanticModel2 = MSFabricRunSemanticModelRefreshOperator(
  #   task_id="run_semantic_model_refresh2",
  #   fabric_conn_id="fabric-powerbi",
  #   workspace_id="4358996c-23ee-4c85-8728-df1825fcc196",
  #   item_id="2dc933ab-ffd4-46f4-ba1a-27cb1219a20f",
  #   timeout=60 * 10, #10 minutes
  #   deferrable=True,
  #   api_host="https://dailyapi.fabric.microsoft.com")

# SparkJon
  runSparkJob = MSFabricRunJobOperator(
    task_id="runSparkJobTask1_deferred",
    fabric_conn_id="fabric-integration",
    workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
    item_id="dded0af7-bb56-493b-a40b-934a39e6f01e",
    job_type="RunSparkJob",
    timeout=60 * 10, #10 minutes
    deferrable=True,
  )

  # Notebook
  runNotebook1 = MSFabricRunJobOperator(
    task_id="runNotebookTask1_deferred",
    fabric_conn_id="fabric-integration",
    workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
    item_id="5ea6c21f-dcb3-4c63-9c37-fe433ac6894b",
    job_type="RunNotebook",
    timeout=60 * 10, #10 minutes
    deferrable=True,
    job_params= MSFabricNotebookJobParameters()
        .set_parameter("sleep_seconds", 40, "int")
        .to_json()
  )

  runNotebook2 = MSFabricRunJobOperator(
    task_id="runNotebookTask2_sync",
    fabric_conn_id="fabric-integration",
    workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
    item_id="5ea6c21f-dcb3-4c63-9c37-fe433ac6894b",
    job_type="RunNotebook",
    timeout=60 * 10, #10 minutes
    deferrable=False, 
  )

  # Pipeline
  runPipeline1 = MSFabricRunJobOperator(
    task_id="runPipelineTask1_deferred",
    fabric_conn_id="fabric-integration",
    workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
    item_id="3d99c6f8-b37e-4712-a80c-25c52b9e2ae2",
    job_type="Pipeline",
    timeout=60 * 10, #10 minutes
    deferrable=True,
    job_params= MSFabricPipelineJobParameters()
        .set_parameter("SleepInSeconds", 45)
        .set_parameter("Message", "Sleeping")
        .to_json()
  )

  runPipeline2 = MSFabricRunJobOperator(
    task_id="runPipelineTask2_sync",
    fabric_conn_id="fabric-integration",
    workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
    item_id="3d99c6f8-b37e-4712-a80c-25c52b9e2ae2",
    job_type="Pipeline",
    timeout=60 * 10, #10 minutes
    deferrable=False,
  )

  # # DBT Deferred
  # runDbt1 = MSFabricRunJobOperator(
  #   task_id="runDbtTask_deferred",
  #   fabric_conn_id="fabric-integration",
  #   workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
  #   item_id="fcfbc4e7-51a2-4dc9-b100-00bc1722a39b",
  #   job_type="DBT",
  #   timeout=60 * 10, #10 minutes
  #   deferrable=True,
  # )

  # # DBT Synchronous
  # runDbt2 = MSFabricRunJobOperator(
  #   task_id="runDbtTask_sync",
  #   fabric_conn_id="fabric-integration",
  #   workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
  #   item_id="fcfbc4e7-51a2-4dc9-b100-00bc1722a39b",
  #   job_type="DBT",
  #   timeout=60 * 10, #10 minutes
  #   deferrable=False,
  # )

  # Lakehouse Materialized Views Refresh
  runLakehouseRefresh = MSFabricRunJobOperator(
    task_id="runLakehouseRefreshTask_deferred",
    fabric_conn_id="fabric-integration",
    workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
    item_id="1ac73be6-81a7-4a5e-81d5-46d927ad7bc6",
    job_type="RefreshMaterializedLakeViews",
    timeout=60 * 10, #10 minutes
    deferrable=True,
    scope="https://graph.microsoft.com/.default",
  )

  # User Function
  runFunction1 = MSFabricRunUserDataFunctionOperator(
    task_id="run_user_data_function1",
    fabric_conn_id="fabric-integration",
    workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
    item_id="1cc9a6ff-862b-4c13-8685-750a2103c858",
    item_name="MyFunc1",
    parameters=dict(name='value1', lastName='value2')
  )

  runFunction2 = MSFabricRunUserDataFunctionOperator(
    task_id="run_user_data_function2",
    fabric_conn_id="fabric-integration",
    workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
    item_id="1cc9a6ff-862b-4c13-8685-750a2103c858",
    item_name="MySleepFunc",
    parameters=dict(sleepSeconds=90)
  )

  runFunction3 = MSFabricRunUserDataFunctionOperator(
    task_id="run_user_data_function3",
    fabric_conn_id="fabric-integration",
    workspace_id="cb9c7d63-3263-4996-9014-482eb8788007",
    item_id="1cc9a6ff-862b-4c13-8685-750a2103c858",
    item_name="MyNoParamFunc",
    parameters=dict()  # No parameters, will use default from the function definition)
  )
