from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel, Field
from pydantic.config import ConfigDict

from app.runtime.paths import CONFIG_PATH
from app.runtime.persistence import read_json, write_json


class EngineConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    client_id: Optional[str] = None
    access_token: Optional[str] = None

    trading_enabled: bool = False

    # Weekly expiry selection for option contracts.
    weekly_expiry: Literal["CURRENT", "NEXT"] = "CURRENT"

    # Dashboard option chain (spot-centered)
    strike_step: int = Field(default=50, ge=1)
    chain_strikes_each_side: int = Field(default=10, ge=1, le=25)

    # Risk settings (percent of option premium at entry)
    stop_loss_pct: float = Field(default=20.0, ge=0.0, le=500.0)
    target_pct: float = Field(default=30.0, ge=0.0, le=500.0)
    trailing_stop_pct: float = Field(default=0.0, ge=0.0, le=500.0)

    # Order params
    lots: int = Field(default=1, ge=1, le=100)
    order_type: Literal["MARKET", "LIMIT"] = "MARKET"
    limit_price_offset: float = 0.0

    # Dhan instrument ids (optional overrides)
    nifty_spot_security_id: str = "13"


class ActivePosition(BaseModel):
    side: Literal["BUY", "SELL"]
    symbol: str
    security_id: str
    option_type: Literal["CE", "PE"]
    strike: int
    expiry: str
    qty: int
    entry_price: float
    last_ltp: Optional[float] = None
    stop_loss_price: float
    trailing_stop_price: Optional[float] = None
    target_price: float
    pnl: Optional[float] = None
    entry_ts: str
    exit_reason: Optional[str] = None


class EngineStatus(BaseModel):
    running: bool
    trading_enabled: bool
    spot_ltp: Optional[float]
    weekly_expiry: str
    instruments_loaded: bool
    active_position: Optional[ActivePosition] = None
    last_error: Optional[str] = None
    feed_error: Optional[str] = None


class EngineConfigStore:
    def __init__(self, path: Path | None = None) -> None:
        self._lock = asyncio.Lock()
        self._path = path or CONFIG_PATH
        loaded = read_json(self._path)
        if loaded is not None:
            try:
                self._cfg = EngineConfig.model_validate(loaded)
            except Exception:
                self._cfg = EngineConfig()
        else:
            self._cfg = EngineConfig()
        self._version = 0

    def current(self) -> EngineConfig:
        # Read-only snapshot for hot-path usage (do not mutate).
        return self._cfg

    def version(self) -> int:
        return self._version

    async def get(self) -> EngineConfig:
        async with self._lock:
            return self._cfg.model_copy(deep=True)

    async def set(self, new_cfg: EngineConfig) -> EngineConfig:
        async with self._lock:
            self._cfg = new_cfg
            self._version += 1
            write_json(self._path, self._cfg.model_dump())
            return self._cfg.model_copy(deep=True)

    async def patch(self, **kwargs) -> EngineConfig:
        async with self._lock:
            updated = self._cfg.model_copy(update=kwargs)
            self._cfg = updated
            self._version += 1
            write_json(self._path, self._cfg.model_dump())
            return self._cfg.model_copy(deep=True)
