"""
To launch:
uvicorn rest_api.app:app --reload
"""
from rest_api.utils import load_local_env

load_local_env()

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from rest_api.routes import api_router
from rest_api.services.task_store import task_store
from rest_api.services.client import databricks_client



# Configure uvicorn access logger to filter Socket.IO requests
class SocketIOFilter(logging.Filter):
    def filter(self, record):
        # Filter out Socket.IO requests from access logs
        if hasattr(record, 'scope') and record.scope.get('path', '').startswith('/ws/socket.io/'):
            return False
        # Also check the message itself
        if '/ws/socket.io/' in str(record.getMessage()):
            return False
        return True


# Apply filter to uvicorn access logger
uvicorn_access_logger = logging.getLogger("uvicorn.access")
uvicorn_access_logger.addFilter(SocketIOFilter())


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: startup and shutdown"""
    # Startup
    yield
    # Shutdown: cleanup connections
    await task_store.close()
    await databricks_client.close()


app = FastAPI(
    title="FastAPI & Databricks Apps",
    description="A simple FastAPI application example for Databricks Apps runtime",
    version="0.0.1",
    lifespan=lifespan
)

app.include_router(api_router)


