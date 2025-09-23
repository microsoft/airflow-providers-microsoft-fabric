import os
import requests
import time
import sys
from requests.auth import HTTPBasicAuth

def fail(msg):
    print(f"::error::{msg}")
    sys.exit(1)

def fetch_task_logs(airflow_url, dag_id, dag_run_id, task_id, username, password):
    """
    Fetch logs for a specific task instance.
    
    :param airflow_url: Base Airflow URL
    :param dag_id: DAG ID
    :param dag_run_id: DAG run ID
    :param task_id: Task ID to fetch logs for
    :param username: Airflow username
    :param password: Airflow password
    :return: Task logs as string or None if failed to fetch
    """
    try:
        # Get task instance details first to get the try_number
        task_instance_url = f"{airflow_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"
        task_resp = requests.get(task_instance_url, auth=HTTPBasicAuth(username, password))
        
        if not task_resp.ok:
            print(f"Warning: Could not fetch task instance details for '{task_id}': {task_resp.status_code}")
            return None
        
        task_data = task_resp.json()
        try_number = task_data.get('try_number', 1)
        
        # Fetch logs for the task
        logs_url = f"{airflow_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
        logs_resp = requests.get(logs_url, auth=HTTPBasicAuth(username, password))
        
        if logs_resp.ok:
            logs_data = logs_resp.json()
            # The logs are typically in the 'content' field
            return logs_data.get('content', 'No log content available')
        else:
            print(f"Warning: Could not fetch logs for task '{task_id}': {logs_resp.status_code}")
            return None
            
    except Exception as e:
        print(f"Warning: Exception while fetching logs for task '{task_id}': {str(e)}")
        return None

def check_dag_availability(airflow_url, dag_id, username, password, timeout_seconds=120):
    """
    Check if DAG exists and is available for execution.
    Wait for DAG to be processed if it's not yet available.
    
    :param airflow_url: Base Airflow URL
    :param dag_id: DAG ID to check
    :param username: Airflow username
    :param password: Airflow password  
    :param timeout_seconds: Maximum time to wait for DAG availability
    :return: True if DAG is available, False if timeout or processing errors
    """
    print(f"Checking DAG '{dag_id}' availability...")
    
    dag_url = f"{airflow_url.rstrip('/')}/api/v1/dags/{dag_id}"
    import_errors_url = f"{airflow_url.rstrip('/')}/api/v1/importErrors"
    
    start_time = time.time()
    end_time = start_time + timeout_seconds
    
    while time.time() < end_time:
        # Check if DAG exists and is available
        dag_resp = requests.get(dag_url, auth=HTTPBasicAuth(username, password))
        
        if dag_resp.ok:
            dag_info = dag_resp.json()
            is_paused = dag_info.get('is_paused', True)
            print(f"DAG '{dag_id}' found. Is paused: {is_paused}")
            return True
        
        elif dag_resp.status_code == 404:
            # DAG not found - check if it's a processing issue
            print(f"DAG '{dag_id}' not found. Checking for processing errors...")
            
            # Check for import/processing errors
            import_resp = requests.get(import_errors_url, auth=HTTPBasicAuth(username, password))
            
            if import_resp.ok:
                import_errors = import_resp.json().get('import_errors', [])
                
                # Look for errors related to our DAG
                dag_errors = [
                    error for error in import_errors 
                    if dag_id in error.get('filename', '') or dag_id in error.get('stack_trace', '')
                ]
                
                if dag_errors:
                    error_details = []
                    for error in dag_errors:
                        filename = error.get('filename', 'Unknown file')
                        stack_trace = error.get('stack_trace', 'No stack trace')
                        timestamp = error.get('timestamp', 'Unknown time')
                        
                        error_details.append(f"File: {filename}, Time: {timestamp}")
                        error_details.append(f"Error: {stack_trace}")
                    
                    fail(f"DAG processing errors found for '{dag_id}':\n" + "\n".join(error_details))
                
                # No specific errors for this DAG - it might still be processing
                elapsed = time.time() - start_time
                remaining = end_time - time.time()
                
                print(f"No processing errors found for '{dag_id}'. Waiting... (elapsed: {elapsed:.1f}s, remaining: {remaining:.1f}s)")
            else:
                print(f"Warning: Could not check import errors: {import_resp.status_code}")
        
        else:
            # Some other error when checking DAG
            print(f"Warning: Unexpected error checking DAG: {dag_resp.status_code} - {dag_resp.text}")
        
        # Wait 10 seconds before retrying
        print("Waiting 10 seconds before retrying...")
        time.sleep(10)
    
    # Timeout reached
    elapsed = time.time() - start_time
    fail(f"Timeout waiting for DAG '{dag_id}' to become available after {elapsed:.1f} seconds")

def main():
    airflow_url = os.environ["AIRFLOW_URL"]
    dag_id = os.environ["DAG_ID"]
    username = os.environ["USERNAME"]
    password = os.environ["PASSWORD"]
    timeout_minutes = int(os.environ.get("TIMEOUT_MINUTES", 15))
    
    # Check DAG availability before proceeding (wait up to 2 minutes)
    check_dag_availability(airflow_url, dag_id, username, password, timeout_seconds=120)

    # 1. Trigger DAG run
    print(f"Triggering DAG '{dag_id}' on Airflow at {airflow_url}. User: {username}, Password: {password}.")
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
        fail("No dag_run_id in response")
    print(f"Triggered DAG run: {dag_run_id}")

    # 2. Poll DAG run status
    print(f"Triggering DAG '{dag_id}' successfully. Waiting for it to complete. Timeout {timeout_minutes} min.")
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

    # 3. Check task statuses
    tasks_url = f"{airflow_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
    tasks_resp = requests.get(tasks_url, auth=HTTPBasicAuth(username, password))
    if not tasks_resp.ok:
        print("Warning: could not fetch task instances for final check.")
    else:
        tasks = tasks_resp.json().get("task_instances", [])
        failed = [t['task_id'] for t in tasks if t['state'] and t['state'].lower() != "success"]
        if failed:
            # Fetch logs for the first failed task
            first_failed_task = failed[0]
            print(f"Fetching logs for first failed task: '{first_failed_task}'")
            
            task_logs = fetch_task_logs(airflow_url, dag_id, dag_run_id, first_failed_task, username, password)
            
            error_message = f"Some tasks did not succeed: {failed}"
            if task_logs:
                error_message += f"\n\nLogs for failed task '{first_failed_task}':\n"
                error_message += "=" * 80 + "\n"
                error_message += task_logs
                error_message += "\n" + "=" * 80
            else:
                error_message += f"\n\nCould not fetch logs for failed task '{first_failed_task}'"
            
            fail(error_message)

    # Safe guard in case API returns no tasks - should not happen
    if state != "success":
        fail(f"DAG run ended with state: {state}")

    print("DAG run and all tasks completed successfully!")

if __name__ == "__main__":
    main()
