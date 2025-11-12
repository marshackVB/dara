from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
from rest_api.utils.spark import get_spark_session

class DatabricksJob(ABC):
    """Abstract base class for all Databricks jobs."""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        """Initialize job with Spark session."""
        self.spark = spark or get_spark_session()
    
    @abstractmethod
    def run(self) -> Dict[str, Any]:
        """
        Execute the job.
        
        Returns:
            Dict with job execution results (e.g., {"records_inserted": 100, "status": "success"})
        """
        pass
    
    @abstractmethod
    def validate(self) -> None:
        """
        Validate job configuration before execution.
        Raises ValueError if configuration is invalid.
        """
        pass