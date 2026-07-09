"""
Smoke-test DAG for Dataflows Gen2 — fully self-contained, no provider install needed.

Uses plain `requests` to:
  1. Obtain an SPN access token (client-credentials flow)
  2. POST to the Fabric Job Scheduler to trigger a Dataflows Gen2 refresh
  3. Poll the Location URL until the job reaches a terminal state

Paste this file directly into your Fabric Airflow Job's DAGs folder.
No extra pip requirements, no Airflow connections to configure.

Credentials are read from Airflow Variables so no secrets live in the DAG file.
Set these in Admin → Variables before triggering:
  - dataflow_gen2_test_tenant_id
  - dataflow_gen2_test_client_id
  - dataflow_gen2_test_client_secret
  - dataflow_gen2_test_workspace_id
  - dataflow_gen2_test_item_id
"""

import logging
import time
from datetime import datetime, timezone

import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# ── Target (loaded at task runtime so no secrets are module-level) ────────────
def _cfg() -> dict:
    return {
        "tenant_id":     Variable.get("dataflow_gen2_test_tenant_id"),
        "client_id":     Variable.get("dataflow_gen2_test_client_id"),
        "client_secret": Variable.get("dataflow_gen2_test_client_secret"),
        "workspace_id":  Variable.get("dataflow_gen2_test_workspace_id"),
        "item_id":       Variable.get("dataflow_gen2_test_item_id"),
    }

# ── Tuning ────────────────────────────────────────────────────────────────────
SCOPE        = "https://api.fabric.microsoft.com/.default"
API_HOST     = "https://api.fabric.microsoft.com"
TIMEOUT_SECS = 15 * 60   # 15 minutes total
POLL_SECS    = 30         # poll every 30 s

# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_token(cfg: dict) -> str:
    """Obtain a bearer token via client-credentials grant."""
    url = f"https://login.microsoftonline.com/{cfg['tenant_id']}/oauth2/v2.0/token"
    resp = requests.post(
        url,
        data={
            "grant_type": "client_credentials",
            "client_id": cfg["client_id"],
            "client_secret": cfg["client_secret"],
            "scope": SCOPE,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ── Task callable ─────────────────────────────────────────────────────────────

def run_dataflow(**_):
    log = logging.getLogger(__name__)
    cfg = _cfg()

    # 1. Trigger the refresh
    token = _get_token(cfg)
    trigger_url = (
        f"{API_HOST}/v1/workspaces/{cfg['workspace_id']}"
        f"/items/{cfg['item_id']}/jobs/instances?jobType=Refresh"
    )
    log.info("Triggering Dataflows Gen2 refresh → %s", trigger_url)

    resp = requests.post(trigger_url, headers=_auth_headers(token), json={}, timeout=30)

    if resp.status_code not in (200, 202):
        raise RuntimeError(
            f"Trigger failed [{resp.status_code}]: {resp.text}"
        )

    location = resp.headers.get("Location")
    if not location:
        # 200 = synchronous completion
        log.info("Refresh completed synchronously (HTTP 200).")
        return

    log.info("Refresh accepted (HTTP 202). Polling: %s", location)

    # 2. Poll until terminal
    TERMINAL = {"Completed", "Failed", "Cancelled", "TimedOut", "Deduped"}
    FAILURE  = {"Failed", "Cancelled", "TimedOut", "Deduped"}

    deadline = time.monotonic() + TIMEOUT_SECS

    while time.monotonic() < deadline:
        time.sleep(POLL_SECS)

        token = _get_token(cfg)        # refresh token every cycle (avoids expiry on long runs)
        poll = requests.get(location, headers=_auth_headers(token), timeout=30)
        poll.raise_for_status()

        body   = poll.json()
        status = body.get("status", "Unknown")
        log.info("Status: %-12s | response: %s", status, body)

        if status in TERMINAL:
            if status in FAILURE:
                reason = body.get("failureReason") or body
                raise RuntimeError(f"Dataflow refresh ended with status '{status}': {reason}")
            log.info("Dataflow refresh completed successfully ✓")
            return

    raise RuntimeError(
        f"Timed out after {TIMEOUT_SECS // 60} minutes waiting for dataflow refresh to complete."
    )


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="dataflow_gen2_smoke_test",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["fabric", "dataflow-gen2", "smoke-test"],
) as dag:

    PythonOperator(
        task_id="run_dataflow1_refresh",
        python_callable=run_dataflow,
    )
