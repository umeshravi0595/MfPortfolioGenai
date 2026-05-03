import requests
import base64
from config import DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_JOB_ID

 
headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}"
}

def upload_file(file_path, dbfs_path):
    with open(file_path, "rb") as f:
        data = base64.b64encode(f.read()).decode()

    url = f"{DATABRICKS_HOST}/api/2.0/dbfs/put"

    payload = {
        "path": dbfs_path,
        "contents": data,
        "overwrite": True
    }

    res = requests.post(url, headers=headers, json=payload)
    return res.json()

def trigger_job():
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/run-now"

    payload = {
        "job_id": DATABRICKS_JOB_ID
    }

    res = requests.post(url, headers=headers, json=payload)
    return res.json()