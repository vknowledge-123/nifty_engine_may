from __future__ import annotations

from fastapi import APIRouter

from app.api.routes.engine import router as engine_router
from app.api.routes.instruments import router as instruments_router

router = APIRouter()
router.include_router(engine_router, tags=["engine"])
router.include_router(instruments_router, tags=["instruments"])
