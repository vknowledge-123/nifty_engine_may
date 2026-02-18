from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from fastapi.staticfiles import StaticFiles

from app.web.router import router as web_router
from app.api.router import router as api_router
from app.runtime.context import AppContext


def create_app() -> FastAPI:
    app = FastAPI(title="Nifty Semi Algo Options Trader", version="0.1.0", default_response_class=ORJSONResponse)

    base_dir = Path(__file__).resolve().parent
    static_dir = base_dir / "web" / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    app.include_router(web_router)
    app.include_router(api_router, prefix="/api")

    @app.on_event("startup")
    async def _startup() -> None:
        app.state.ctx = AppContext()
        await app.state.ctx.startup()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        ctx: AppContext = app.state.ctx
        await ctx.shutdown()

    return app


app = create_app()
