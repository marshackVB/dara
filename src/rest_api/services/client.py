"""
Databricks async workspace client
See the docs at https://docs.databricks.com/api/workspace/introduction
"""

import os
import time
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import asyncio
import httpx


class DatabricksClient:
    """An async wrapper around the Databricks REST API. Using awaitable calls 
    keeps FastAPI's event loop from being blocked by API calls, so requests don't 
    get serialized under load."""

    def __init__(
        self, 
        workspace_url: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        token: Optional[str] = None
    ):

        #self.workspace_url = workspace_url or f"https://{os.getenv("DATABRICKS_HOST")}/" # This won't work for some reason.
        self.workspace_url = workspace_url or os.getenv("DATABRICKS_URL")
        self.client_id = client_id or os.getenv("DATABRICKS_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("DATABRICKS_CLIENT_SECRET")
        self.token = token
        self.token_expiry: Optional[datetime] = None
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_token(self) -> str:
        """Get or refresh OAuth token"""
        # Return a valid token if one exists
        if self.token and self.token_expiry and datetime.now() < self.token_expiry:
            return self.token

        # Otherwise, fetch a token.
        # First, confirm the client id and client secret exist
        if not self.client_id or not self.client_secret:
            raise ValueError("CLIENT_ID and CLIENT_SECRET must be set")
        
        if not self.workspace_url:
            raise ValueError(
                "DATABRICKS_URL must be set. "
                "Set it as an environment variable or pass workspace_url to DatabricksClient."
            )

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.workspace_url}/oidc/v1/token",
                auth = (self.client_id, self.client_secret),
                headers = {"Content-Type": "application/x-www-form-urlencoded"},
                data = {
                    "grant_type": "client_credentials",
                    "scope": "all-apis"
                }
            )
            response.raise_for_status()
            data = response.json()
            self.token = data.get("access_token")
            # Set a 5 minute expiration buffer
            expires_in  = data.get("expires_in", 3600)
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in - 300)
            return self.token


    async def _inject_token(self, request: httpx.Request) -> None:
        """Event hook to inject Authorization header on each request"""
        token = await self._get_token()
        request.headers["Authorization"] = f"Bearer {token}"


    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create httpx client"""
        if self._client is None:
            if not self.workspace_url:
                raise ValueError(
                    "DATABRICKS_URL must be set. "
                    "Set it as an environment variable or pass workspace_url to DatabricksClient."
                )
            self._client = httpx.AsyncClient(
                base_url = self.workspace_url,
                timeout = 30.0,
                event_hooks = {
                    "request": [self._inject_token]
                }
            )
        return self._client


    async def run_job(self, job_id: int, **kwargs) -> Dict[str, Any]:
        """
        Trigger a Databricks job run.

        Args:
            job_id: The ID of the Databricks job to run.
            **kwargs: Additional parameters(notebook_params, python_params, etc.)

        Returns:
            Response with run_id

        Notes: 
            The /api/2.2/jobs/runs/submit API is also of interest here. This API
            runs a job without creating a Workflow in the UI.
        """
        client = await self._get_client()
        response = await client.post(
            "/api/2.1/jobs/run-now",
            json={"job_id": job_id, **kwargs}
        )
        response.raise_for_status()
        return response.json()


    async def get_run(self, run_id: int) -> Dict[str, Any]:
        """
        Get the status of a job run.

        Args:
            run_id: The run ID to get status for

        Returns:
            Run status information

        Notes:
            The /api/2.2/jobs/runs/get-output is also of interest here; it returns 
            a value returned by a Notebook that uses dbutils.notebook.exit()
        """
        client = await self._get_client()
        response = await client.get(
            f"/api/2.1/jobs/runs/get",
            params={"run_id": run_id}
        )
        response.raise_for_status()
        return response.json()

    async def upload_file(
        self, 
        file_path: str,
        contents: bytes,
        overwrite: bool = True
    ) -> None:
        """
        Upload a file using the Databricks Files API.
        
        Uses PUT /api/2.0/fs/files{file_path}
        
        Args:
            file_path: Target file path (e.g., /Volumes/catalog/schema/volume/file.txt or /path/to/file.txt)
            contents: File contents as bytes
            overwrite: Whether to overwrite existing file (Note: API behavior may vary)
        """
        client = await self._get_client()
        endpoint = f"/api/2.0/fs/files{file_path}"
        
        response = await client.put(
            endpoint,
            content=contents,
            headers={"Content-Type": "application/octet-stream"}
        )
        response.raise_for_status()

    async def close(self):
        """Close httpx client connection"""
        if self._client:
            await self._client.aclose()
            self._client = None


databricks_client = DatabricksClient()