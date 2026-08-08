"""Aggregated v1 router."""

from fastapi import APIRouter

from app.api.routes import files, folders, meta

api_router = APIRouter()
api_router.include_router(meta.router)
api_router.include_router(folders.router)
api_router.include_router(files.router)

__all__ = ["api_router"]
