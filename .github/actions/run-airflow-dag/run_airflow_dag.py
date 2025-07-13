import os
import requests
import time
import sys
from requests.auth import HTTPBasicAuth

def fail(msg):
    print(f"::error::{msg}")
    sys.exit(1)

def main():
    airflow_url = os.environ["AIRFLOW_URL"]
    dag_id = os.environ["DAG_ID"]
    username = os.environ["USERNAME"]
    password = os.environ["PASSWORD"]
    timeout_minutes = int(os.environ.get("TIMEOUT_MINUTES", 15))

    # 1. Trigger DAG run
    trigger_url = f"{airflow_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns"
    resp = requests.post(
        trigger_url,
        auth=HTTPBasicAuth(username, password),
        json={},
    )
    if not resp.ok:
        fail(f"Failed to trigger DAG: {resp.status_code} {resp.text}")
    dag_run = resp.json()
    dag_run_id = dag_run.get("dag_run_id")
    if not dag_run_id:
        fail("DAG run ID not returned!")
    print(f"Triggered DAG run: {dag_run_id}")

    # 2. Poll DAG run status
    poll_url = f"{airflow_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
    end_time = time.time() + timeout_minutes * 60
    state = None
    while time.time() < end_time:
        time.sleep(10)
        poll_resp = requests.get(poll_url, auth=HTTPBasicAuth(username, password))
        if not poll_resp.ok:
            print(f"Warning: failed to get DAG run status: {poll_resp.status_code}")
            continue
        state = poll_resp.json().get("state")
        print(f"DAG run state: {state}")
        if state in ("success", "failed"):
            break
    else:
        fail("Timeout waiting for DAG run to complete")

    if state != "success":
        fail(f"DAG run ended with state: {state}")

    # 3. Check task statuses
    tasks_url = f"{airflow_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
    tasks_resp = requests.get(tasks_url, auth=HTTPBasicAuth(username, password))
    if not tasks_resp.ok:
        print("Warning: could not fetch task instances for final check.")
        return
    tasks = tasks_resp.json().get("task_instances", [])
    failed = [t['task_id'] for t in tasks if t['state'] and t['state'].lower() != "success"]
    if failed:
        fail(f"Some tasks did not succeed: {failed}")

    print("DAG run and all tasks completed successfully!")

if __name__ == "__main__":
    main()
