import requests
import streamlit as st
from config.settings import *

class DatabricksClient:

    def __init__(self):
        self.base_url = f"https://{DATABRICKS_HOST}"

        self.headers = {
            "Authorization": f"Bearer {DATABRICKS_TOKEN}"
        }

    # ✅ Upload file to volume
    def upload_file(self, file, filename):

        url = f"{self.base_url}/api/2.0/fs/files/Volumes/retail_analytics/portfolio/amfi_data/{filename}"

        response = requests.put(
            url,
            headers=self.headers,
            data=file.getvalue()
        )

        if response.status_code not in [200, 201]:
            st.error(f"Upload failed: {response.text}")
        else:
            st.success("✅ File uploaded to Databricks")

    # ✅ Trigger job
    def trigger_job(self, filename, year, month):

        url = f"{self.base_url}/api/2.1/jobs/run-now"

        payload = {
            "job_id": int(JOB_ID),
            "job_parameters": {
                "filename": filename,
                "year": str(year),
                "month": str(month)
            }
        }

        

        response = requests.post(
            url,
            headers=self.headers,
            json=payload
        )

        if response.status_code != 200:
            st.error(f"Job failed: {response.text}")
        else:
            st.success("🚀 Databricks Job Triggered")