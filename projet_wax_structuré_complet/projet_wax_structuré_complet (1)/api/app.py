from fastapi import FastAPI
from pydantic import BaseModel
import requests
import os

app = FastAPI()

DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "REPLACE_ME")
DATABRICKS_INSTANCE = os.getenv("DATABRICKS_INSTANCE", "https://<ton-workspace>.cloud.databricks.com")
DATABRICKS_JOB_ID = os.getenv("DATABRICKS_JOB_ID", "1234")

class PipelineParams(BaseModel):
    catalog: str
    schema: str
    volume: str
    env: str
    zip_path: str
    excel_path: str

@app.post("/launch-pipeline")
def launch_pipeline(params: PipelineParams):
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }

    payload = {
        "job_id": DATABRICKS_JOB_ID,
        "notebook_params": {
            "catalog_name": params.catalog,
            "schema_name": params.schema,
            "volume_name": params.volume,
            "env": params.env,
            "zip_path": params.zip_path,
            "excel_path": params.excel_path
        }
    }

    response = requests.post(
        f"{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now",
        headers=headers,
        json=payload
    )

    return {
        "status": "triggered" if response.status_code == 200 else "error",
        "response": response.json()
    }
