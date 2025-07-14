import os, sys, json, time, requests
from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth

AIRFLOW_URL = os.environ["AIRFLOW_URL"].rstrip("/") + "/health"
USERNAME    = os.environ["USERNAME"]
PASSWORD    = os.environ["PASSWORD"]
TIMEOUT_M   = int(os.environ.get("TIMEOUT_MINUTES", "5"))

def fail(msg: str):
    print(f"::error::{msg}")
    sys.exit(1)

def fetch_health():
    try:
        r = requests.get(
            AIRFLOW_URL,
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            timeout=10,
        )
        if r.status_code != 200:
            return None, f"status {r.status_code}"
        return r.json(), None
    except requests.RequestException as e:
        return None, str(e)

deadline = datetime.now() + timedelta(minutes=TIMEOUT_M)
print(f"Waiting up to {TIMEOUT_M} min for Airflow {AIRFLOW_URL} to become healthy…")

while datetime.now() < deadline:
    data, err = fetch_health()
    if data:
        unhealthy = [
            f"{comp}={info.get('status')}"
            for comp, info in data.items()
            if info.get("status") != "healthy"
        ]
        if not unhealthy:
            print("Airflow is healthy:")
            for comp, info in data.items():
                print(f"  • {comp}: {info.get('status')}")
            sys.exit(0)

        print(f"Still unhealthy: {', '.join(unhealthy)}")
    else:
        print(f"Could not reach endpoint ({err})")

    time.sleep(10)

fail(f"Timed out: Airflow did not become healthy within {TIMEOUT_M} minute(s).")
