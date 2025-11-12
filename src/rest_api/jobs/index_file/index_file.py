from rest_api.utils import load_local_env

load_local_env()

import os
from typing import Optional, Dict, Any
import pyspark.sql.functions as func
from pyspark.sql import SparkSession, DataFrame
from databricks.vector_search.client import VectorSearchClient
from rest_api.jobs.base import DatabricksJob


class IndexFile(DatabricksJob):

  def __init__(self,
              spark: Optional[SparkSession] = None, 
              file_name: str,
              vs_endpoint_name: Optional[str] = None, 
              vs_index_name: Optional[str] = None, 
              uc_volume_path: Optional[str] = None,
              chunking_uc_function: Optional[str] = None,
              embedding_uc_function: Optional[str] = None,
              workspace_url: Optional[str] = None,
              client_id: Optional[str] = None,
              client_secret: Optional[str] = None)

    super().__init__(spark)
    
    self.file_name = file_name
    self.vs_endpoint_name = vs_endpoint_name
    self.vs_index_name = vs_index_name
    self.uc_volume_path = uc_volume_path
    self.chunking_uc_function = chunking_uc_function
    self.embedding_uc_function = embedding_uc_function
    self.workspace_url = workspace_url or os.getenv("DATABRICKS_URL")
    self.client_id = client_id or os.getenv("DATABRICKS_CLIENT_ID")
    self.client_secret = client_secret or os.getenv("DATABRICKS_CLIENT_SECRET")
    self._vs_client = None


  def _get_vs_client(self) -> VectorSearchClient:
    """Get or create VectorSearchClient."""
    if self._vs_client is None:
        if not self.workspace_url or not self.client_id or not self.client_secret:
            raise ValueError("VectorSearchClient requires workspace_url, client_id, and client_secret")
        self._vs_client = VectorSearchClient(
            workspace_url=self.workspace_url,
            service_principal_client_id=self.client_id,      
            service_principal_client_secret=self.client_secret
        )
    return self._vs_client

    
  def _concatenate_and_spaced(self) -> DataFrame:
    """Parse document and concatenate elements with spacing."""

    return self.spark.sql(f"""
      WITH parsed_docs AS (
        SELECT 
          path,
          ai_parse_document(content) as content
        FROM READ_FILES('{self.uc_volume_path}', format => 'binaryFile')
      ),
      exploded AS (
        SELECT
          d.path,
          e.pos AS element_index,
          e.value:type::string AS element_type,
          e.value:content::string AS element_content
        FROM parsed_docs d,
        LATERAL variant_explode(d.content:document.elements) AS e
      ),
      -- Add extra spacing for section headers
      formatted AS (
        SELECT
          path,
          element_index,
          CASE
            WHEN element_type = 'section_header'
              THEN concat('\n\n', element_content)
            ELSE element_content
          END AS formatted_content
        FROM exploded
      ),
      -- Ensure correct order before combining
      ordered AS (
        SELECT *
        FROM formatted
        ORDER BY path, element_index
      )
      SELECT
        path,
        concat_ws('\n', collect_list(formatted_content)) AS full_document_text
      FROM ordered
      GROUP BY path;
    """)


  def _raw_text_chunks(self) -> DataFrame:
    """Chunk text and generate embeddings."""
    concatenated_and_spaced = self._concatenate_and_spaced()

    return (concatenated_and_spaced
              .withColumn("document_name", func.substring_index("path", "/", -1))
              .withColumn("document_id", func.sha2(func.col("document_name"), 256))
              .withColumn("text_chunks", 
                func.expr(f"{self.chunking_uc_function}(full_document_text, 1000, 100)"))
              .select("*", func.explode("text_chunks").alias("chunk"))
              .drop("full_document_text")
              .withColumn("chunk_id_temp", func.monotonically_increasing_id())
              .withColumn("chunk_id", 
                func.sha2(func.concat_ws("_", func.col("chunk"), func.col("chunk_id_temp"), func.col("document_name")), 256))
              .withColumn("embedding", 
                func.expr(f"{self.embedding_uc_function}(chunk)"))
              .selectExpr(["chunk_id", "document_id", "document_name", "path", "chunk", "embedding"]))


  def _insert_vs_records(self) -> int:
    """Insert records into vector search index."""

    client = self._get_vs_client()
    vs_index = client.get_index(
        endpoint_name=self.vs_endpoint_name,
        index_name=self.vs_index_name
        )

    raw_text_chunks = self._raw_text_chunks()
    rows = (
      raw_text_chunks.select("chunk_id", "document_id", "document_name", "path", "chunk", "embedding")
      .collect()
      ) 

    docs = [
        {
            "chunk": r["chunk"],
            "path": r["path"],
            "embedding": r["embedding"],
            "document_id": r["document_id"],
            "document_name": r["document_name"],
            "chunk_id": r["chunk_id"],   
        }
        for r in rows
      ]

      vs_index.upsert(docs)
      return len(docs)


  def validate(self) -> None:
    """Validate job configuration before execution."""
    required_fields = {
        "uc_volume_path": self.uc_volume_path,
        "vs_endpoint_name": self.vs_endpoint_name,
        "vs_index_name": self.vs_index_name,
        "chunking_uc_function": self.chunking_uc_function,
        "embedding_uc_function": self.embedding_uc_function,
        "workspace_url": self.workspace_url,
        "client_id": self.client_id,
        "client_secret": self.client_secret,
    }
    
    missing = [field for field, value in required_fields.items() if not value]
    
    if missing:
        raise ValueError(
            f"Missing required configuration: {', '.join(missing)}. "
            "Set these directly or via environment variables."
        )


  def run(self) -> Dict[str, Any]:
        """Execute the indexing job."""
        self.validate()
        records_inserted = self._insert_vs_records()
        return {
            "status": "success",
            "records_inserted": records_inserted,
            "file_name": self.file_name
        }