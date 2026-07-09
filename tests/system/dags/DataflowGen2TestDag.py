"""
Manual integration test DAG for Dataflows Gen2.

Target dataflow : "Dataflow1"
Tenant          : b3273975-61bb-4d27-9cb1-4df0bb8a0018
Workspace       : 12c5e906-5bfc-4ba4-bd76-c1ce68fc53c8
Item ID         : 55fb157-fd2f-4c71-8a84-db8907fecd5c

Airflow connection required  (conn_id = "fabric-integration"):
  Conn Type : microsoft-fabric  (or Generic / HTTP)
  Extra JSON:
    {
      "auth_type":    "spn",
      "tenantId":     "b3273975-61bb-4d27-9cb1-4df0bb8a0018",
      "clientId":     "11b7d4b6-f436-4dcb-b30f-c1ba12417db7",
      "clientSecret": "<secret stored in Airflow connection — do not hardcode here>"
    }

  The SPN must be a Workspace Member or above on workspace
  12c5e906-5bfc-4ba4-bd76-c1ce68fc53c8.

  NOTE: The API trigger itself works with SPN. However, if the dataflow's
  internal Cloud Connections use user-delegated OAuth2 credentials, the
  refresh will succeed at the API level but fail during data access. To fix
  that, reconfigure those connections in Fabric to use service-principal or
  key-based credentials.
"""

from airflow import DAG
from airflow.providers.microsoft.fabric.operators.run_item.dataflow_gen2 import (
    MSFabricRunDataflowGen2Operator,
)

DATAFLOW_ITEM_ID = "55fb157-fd2f-4c71-8a84-db8907fecd5c"
WORKSPACE_ID     = "12c5e906-5bfc-4ba4-bd76-c1ce68fc53c8"
FABRIC_CONN_ID = "fabric-integration"

with DAG(
    dag_id="dataflow_gen2_test",
    schedule=None,
    catchup=False,
    tags=["fabric", "dataflow-gen2", "test"],
) as dag:

    # ── Synchronous (blocking) run ──────────────────────────────────────────
    run_sync = MSFabricRunDataflowGen2Operator(
        task_id="run_airflowtest_sync",
        fabric_conn_id=FABRIC_CONN_ID,
        workspace_id=WORKSPACE_ID,
        item_id=DATAFLOW_ITEM_ID,
        timeout=60 * 15,   # 15 minutes
        check_interval=30,
        deferrable=False,
    )

    # ── Deferrable (async) run ──────────────────────────────────────────────
    run_deferred = MSFabricRunDataflowGen2Operator(
        task_id="run_airflowtest_deferred",
        fabric_conn_id=FABRIC_CONN_ID,
        workspace_id=WORKSPACE_ID,
        item_id=DATAFLOW_ITEM_ID,
        timeout=60 * 15,   # 15 minutes
        check_interval=30,
        deferrable=True,
    )

    # Run sync first, then deferred — so you can compare both code paths
    run_sync >> run_deferred
