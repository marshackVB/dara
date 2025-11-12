from fastapi import APIRouter

from .endpoints import router as endpoint_router

router = APIRouter()

router.include_router(endpoint_router)