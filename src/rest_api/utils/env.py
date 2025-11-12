import os
from pathlib import Path
from dotenv import load_dotenv
from databricks.connect import DatabricksSession

def load_local_env():
    if os.getenv("DATABRICKS_APP_NAME"):
        return

    dotenv_path = Path(__file__).resolve().parents[3] / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path, override=False)
    else:
        warnings.warn(
            f".env file not found at {dotenv_path}. "
            "Environment variables must be set via system environment or Databricks Apps runtime.",
            UserWarning
        )


def get_spark_connect(profile: str) -> DatabricksSession:
    if os.getenv("DATABRICKS_APP_NAME"):
        return
    else: 
        spark = DatabricksSession.builder.serverless().profile("e2-demo-field-eng").getOrCreate()
        return spark