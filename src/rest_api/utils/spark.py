import os
from typing import Optional
from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession


def get_spark_session(profile: Optional[str] = None) -> SparkSession:
    """
    Get Spark session based on environment.
    
    - In Databricks: Returns active session or creates new one
    - Locally: Creates Spark Connect session using DatabricksSession
    
    Args:
        profile: Databricks profile name for local development (default: "e2-demo-field-eng")
    
    Returns:
        SparkSession instance
    """
    # Check if running in Databricks environment
    if os.getenv("DATABRICKS_RUNTIME_VERSION") or 'dbutils' in globals():
        # Production: Databricks environment
        return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    else:
        # Local: Spark Connect
        profile = profile or os.getenv("DATABRICKS_PROFILE")
        if not profile:
            raise ValueError(
                "Databricks profile is required for local development. "
                "Either pass 'profile' parameter or set DATABRICKS_PROFILE environment variable."
            )
        return DatabricksSession.builder.serverless().profile(profile).getOrCreate()