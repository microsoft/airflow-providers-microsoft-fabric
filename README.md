# Apache Airflow Provider for Microsoft Fabric

## Introduction

An Apache Airflow provider package for [Microsoft Fabric](https://www.microsoft.com/en-us/microsoft-fabric). It enables Data and Analytics engineers to trigger, monitor, and orchestrate Microsoft Fabric items from Airflow DAGs.

[Microsoft Fabric](https://www.microsoft.com/microsoft-fabric) is an end-to-end analytics and data platform designed for enterprises that require a unified solution. It encompasses data movement, processing, ingestion, transformation, real-time event routing, and report building.

## Installation

```bash
pip install apache-airflow-providers-microsoft-fabric
```

## Authentication

The provider supports the following authentication methods:

| Method | Description |
|--------|-------------|
| **Service Principal (SPN)** | Uses `client_id` and `client_secret` for automated pipelines. |
| **User Token** | Uses a refresh token obtained via Microsoft OAuth. |

### Connection setup

Create a connection in Airflow with the following settings:

| Field | Value |
|-------|-------|
| Connection Id | Your connection name |
| Connection Type | `microsoft-fabric` |
| Login | Client ID of your service principal or Entra ID app |
| Password | Client secret (SPN) or refresh token (User Token) |
| Extra | `{"tenantId": "<tenant-id>", "auth_type": "spn"}` |

> For user token auth, set `auth_type` to `token` and provide a refresh token as the password.

## Operators

### MSFabricRunJobOperator

Triggers and monitors a Fabric job item (notebook, pipeline, Spark job definition).

```python
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunJobOperator

run_notebook = MSFabricRunJobOperator(
    task_id="run_fabric_notebook",
    workspace_id="<workspace_id>",
    item_id="<item_id>",
    fabric_conn_id="fabric_conn_id",
    job_type="RunNotebook",
    wait_for_termination=True,
    deferrable=True,
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `workspace_id` | str | — | The workspace ID. |
| `item_id` | str | — | The item ID (notebook, pipeline, etc.). |
| `fabric_conn_id` | str | — | Airflow connection ID for Fabric. |
| `job_type` | str | — | The item type to run (see table below). |
| `wait_for_termination` | bool | `True` | Wait for the item run to complete. |
| `timeout` | int | `604800` | Timeout in seconds (default: 7 days). |
| `check_interval` | int | `60` | Polling interval in seconds. |
| `max_retries` | int | `5` | Max polling retries after job start. |
| `retry_delay` | int | `1` | Delay between polling retries (seconds). |
| `deferrable` | bool | `False` | Run the operator in deferrable mode. |
| `job_params` | dict | `None` | Parameters passed to the job. |

**Supported `job_type` values:**

| Fabric item | `job_type` values | API | Required permission |
|-------------|-------------------|-----|---------------------|
| Notebook | `RunNotebook`, `Notebook` | Fabric Job Scheduler | `Item.Execute.All` or `Notebook.Execute.All` |
| Data Pipeline | `RunPipeline`, `Pipeline` | Fabric Job Scheduler | `Item.Execute.All` or `DataPipeline.Execute.All` |
| Spark Job Definition | `RunSparkJob`, `SparkJob` | Fabric Job Scheduler | `Item.Execute.All` or `SparkJobDefinition.Execute.All` |
| DBT Job | `DataBuildToolJob`, `DBT` | Fabric Job Scheduler | `Item.Execute.All` |
| Copy Job | `CopyJobs` | Fabric Job Scheduler | `Item.Execute.All` |
| Materialized Lake Views | `RefreshMaterializedLakeViews` | Fabric Job Scheduler | `Item.Execute.All` |

#### Sending parameters

The `job_params` parameter accepts a **JSON string**. Use the built-in helper
classes to construct it.

**Notebook parameters** — use `MSFabricNotebookJobParameters`:

```python
from airflow.providers.microsoft.fabric.operators.run_item.notebook_parameters import (
    MSFabricNotebookJobParameters,
)

params = (
    MSFabricNotebookJobParameters()
    .set_parameter("input_path", "/data/raw")
    .set_parameter("threshold", 0.9)           # auto-inferred as float
    .set_parameter("debug_mode", True)          # auto-inferred as bool
    .set_conf("spark.executor.memory", "4g")
    .set_environment(environment_name="my-env")
    .set_default_lakehouse(name="bronze", id="<lakehouse-id>", workspace_id="<ws-id>")
    .set_use_starter_pool(False)
    .set_use_workspace_pool("my-pool")
)

run_notebook = MSFabricRunJobOperator(
    task_id="run_notebook",
    workspace_id="<workspace_id>",
    item_id="<notebook_id>",
    fabric_conn_id="fabric_conn_id",
    job_type="RunNotebook",
    job_params=params.to_json(),
    deferrable=True,
)
```

**Pipeline parameters** — use `MSFabricPipelineJobParameters`:

```python
from airflow.providers.microsoft.fabric.operators.run_item.pipeline_parameters import (
    MSFabricPipelineJobParameters,
)

params = (
    MSFabricPipelineJobParameters()
    .set_parameter("source_table", "orders")
    .set_parameter("batch_size", 1000)
    .set_parameter("full_refresh", True)
)

run_pipeline = MSFabricRunJobOperator(
    task_id="run_pipeline",
    workspace_id="<workspace_id>",
    item_id="<pipeline_id>",
    fabric_conn_id="fabric_conn_id",
    job_type="Pipeline",
    job_params=params.to_json(),
    deferrable=True,
)
```

**Spark Job / DBT / Copy Job / Materialized Lake Views** — pass a raw JSON
string if the API expects `executionData`:

```python
run_spark = MSFabricRunJobOperator(
    task_id="run_spark_job",
    workspace_id="<workspace_id>",
    item_id="<spark_job_id>",
    fabric_conn_id="fabric_conn_id",
    job_type="SparkJob",
    job_params='{"executionData": {}}',
    deferrable=True,
)
```

### MSFabricRunSemanticModelRefreshOperator

Triggers a Power BI semantic model refresh via the Power BI REST API.

| | |
|---|---|
| **API** | Power BI REST API (`https://api.powerbi.com`) |
| **Required permission** | `Dataset.ReadWrite.All` |

```python
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunSemanticModelRefreshOperator

refresh = MSFabricRunSemanticModelRefreshOperator(
    task_id="refresh_model",
    workspace_id="<workspace_id>",
    item_id="<semantic_model_id>",
    fabric_conn_id="fabric_conn_id",
    deferrable=True,
)
```

### MSFabricRunUserDataFunctionOperator

Invokes a User Data Function in Microsoft Fabric. This operator is
**synchronous only** — the API returns immediately with the function output.

| | |
|---|---|
| **API** | Fabric REST API (`https://api.fabric.microsoft.com`) |
| **Required permission** | `Execute` permission on the User Data Functions item |

`item_name` is the name of the function to invoke within the User Data Function
item. `parameters` is a dict of input values passed directly to the function.

```python
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunUserDataFunctionOperator

run_udf = MSFabricRunUserDataFunctionOperator(
    task_id="run_udf",
    workspace_id="<workspace_id>",
    item_id="<udf_item_id>",
    item_name="<function_name>",
    fabric_conn_id="fabric_conn_id",
    parameters={"key": "value"},
)
```

> **Backwards compatibility:** `MSFabricRunItemOperator` is available as an alias for `MSFabricRunJobOperator`.

## Secrets Backends

### Why Fabric needs a secrets backend

Connections in Microsoft Fabric are identified by **GUIDs**, not by the
human-readable names Airflow normally uses. When a DAG references a Fabric
connection (e.g. `fabric_conn_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"`),
Airflow must resolve that GUID to a valid access token at runtime.

The Fabric platform provides a connections API that holds pre-minted tokens for
each connection GUID. `FabricSecretBackend` calls this API, caches the token,
and returns a fully-formed `Connection` object — making Fabric connections
transparent to DAG authors.

### Why a chained backend

Customers running Fabric-managed Airflow may already have their own secrets
backend configured (e.g. HashiCorp Vault, AWS Secrets Manager, Azure Key Vault)
for non-Fabric connections. Installing `FabricSecretBackend` as the sole backend
would break those existing connections.

`FabricChainedSecretBackend` solves this by combining both backends in a chain.
The user's backend is queried first, then the Fabric backend, then the Airflow
metastore. This ensures Fabric GUID connections and customer-managed connections
are both resolved correctly without configuration conflicts.

### FabricSecretBackend

Resolves GUID-based connection IDs by fetching pre-minted access tokens from the Fabric connections API. Non-GUID connection IDs are skipped so other backends can handle them.

```
AIRFLOW__SECRETS__BACKEND=airflow.providers.microsoft.fabric.secrets.fabric_secret_backend.FabricSecretBackend
AIRFLOW__SECRETS__BACKEND_KWARGS={"api_base_url": "https://<fabric-api-host>", "api_scope": "<api-scope>"}
```

### FabricChainedSecretBackend

Chains an optional user-defined backend with the Fabric backend. The user backend is queried first, then the Fabric backend, then the Airflow metastore. All configuration is via environment variables:

| Variable | Description |
|----------|-------------|
| `AIRFLOW_SECRETS_FABRIC_BACKEND` | Fully qualified class name of the Fabric backend. |
| `AIRFLOW_SECRETS_FABRIC_BACKEND_KWARGS` | JSON kwargs for the Fabric backend (e.g. `api_base_url`, `api_scope`). |
| `AIRFLOW_SECRETS_USER_BACKEND` | Fully qualified class name of the user's backend (e.g. HashiCorp Vault). |
| `AIRFLOW_SECRETS_USER_BACKEND_KWARGS` | JSON kwargs for the user backend. |

**Example:**

```
AIRFLOW__SECRETS__BACKEND=airflow.providers.microsoft.fabric.secrets.fabric_chained_secret_backend.FabricChainedSecretBackend

AIRFLOW_SECRETS_FABRIC_BACKEND=airflow.providers.microsoft.fabric.secrets.fabric_secret_backend.FabricSecretBackend
AIRFLOW_SECRETS_FABRIC_BACKEND_KWARGS={"api_base_url": "https://<fabric-api-host>", "api_scope": "<api-scope>"}

AIRFLOW_SECRETS_USER_BACKEND=airflow.providers.hashicorp.secrets.vault.VaultBackend
AIRFLOW_SECRETS_USER_BACKEND_KWARGS={"url": "https://vault.example.com"}
```

## Features

- **Deferrable mode:** All operators support async execution via Airflow triggers.
- **XCom integration:** Run metadata (`run_id`, `run_status`, `run_location`, `error_message`) is pushed to XCom for downstream tasks.
- **External monitoring link:** Operators provide a deep link to the item run in the Fabric portal.
- **Secrets backend:** GUID-based Fabric connections are resolved transparently, with support for chaining a user-defined backend.
- **Fabric Status Plugin:** An Airflow plugin that exposes a `/fabric-status` endpoint in the Airflow UI.

## Sample DAG

```python
from airflow import DAG
from airflow.providers.microsoft.fabric.operators.run_item import (
    MSFabricRunJobOperator,
    MSFabricRunSemanticModelRefreshOperator,
)
from airflow.utils.dates import days_ago

with DAG(
    dag_id="fabric_items_dag",
    default_args={"owner": "airflow", "start_date": days_ago(1)},
    schedule_interval="@daily",
    catchup=False,
) as dag:

    run_notebook = MSFabricRunJobOperator(
        task_id="run_fabric_notebook",
        workspace_id="<workspace_id>",
        item_id="<item_id>",
        fabric_conn_id="fabric_conn_id",
        job_type="RunNotebook",
        wait_for_termination=True,
        deferrable=True,
    )

    refresh_model = MSFabricRunSemanticModelRefreshOperator(
        task_id="refresh_semantic_model",
        workspace_id="<workspace_id>",
        item_id="<semantic_model_id>",
        fabric_conn_id="fabric_conn_id",
        deferrable=True,
    )

    run_notebook >> refresh_model
```

## Development

### Setup

```bash
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
.venv\Scripts\Activate.ps1  # Windows PowerShell
pip install -e ".[test]"
```

### Run tests

```bash
python -m pytest tests/ -v
```

### Build

```bash
pip install build
python -m build
```

Distribution files are written to `dist/`.

## Contributing

We welcome contributions:

- Report enhancements, bugs, and tasks as [GitHub issues](https://github.com/microsoft/airflow-providers-microsoft-fabric/issues)
- Provide fixes or enhancements by opening pull requests.
