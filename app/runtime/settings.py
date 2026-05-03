from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Literal, Optional

from pydantic import AliasChoices, BaseModel, Field
from pydantic.config import ConfigDict

from app.runtime.paths import CONFIG_PATH
from app.runtime.persistence import read_json, write_json


class EngineConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    client_id: Optional[str] = None
    access_token: Optional[str] = None

    trading_enabled: bool = False
    execution_mode: Literal["SIMULATION", "LIVE"] = "SIMULATION"

    # Ladder/automation controls
    full_automation: bool = False
    ghost_monitoring: bool = False
    last_trade: bool = False

    # Dynamic SL: absolute spot level that exits immediately and stops engine (no flip).
    dynamic_sl_enabled: bool = False
    dynamic_sl_spot: Optional[float] = Field(default=None, ge=0.0, le=1000000.0)

    # Weekly expiry selection for option contracts.
    weekly_expiry: Literal["CURRENT", "NEXT"] = "CURRENT"

    # Dashboard option chain (spot-centered)
    strike_step: int = Field(default=50, ge=1)
    chain_strikes_each_side: int = Field(default=10, ge=1, le=25)

    # Risk settings (SPOT points)
    # - target_points: spot move (favourable) to stop the engine on target hit.
    # - stop_loss_points: fixed SL distance (minimum). Candle-based SL uses this as a minimum.
    # Back-compat: accept previous *_pct keys from config.json / API callers.
    stop_loss_points: float = Field(
        default=14.0,
        ge=0.0,
        le=10000.0,
        validation_alias=AliasChoices("stop_loss_points", "stop_loss_pct"),
    )
    target_points: float = Field(
        default=30.0,
        ge=0.0,
        le=10000.0,
        validation_alias=AliasChoices("target_points", "target_pct"),
    )

    sl_buffer_points: float = Field(default=2.0, ge=0.0, le=10000.0)

    # Pyramiding: add lots on premium move
    add_on_rise_points: float = Field(default=5.0, ge=0.0, le=10000.0)
    max_add_ons: int = Field(default=2, ge=0, le=20)

    # Cost SL (break-even): when enabled, move SL to entry price once RR threshold is reached.
    cost_sl_enabled: bool = False
    cost_sl_rr: float = Field(default=1.0, ge=0.0, le=10000.0)

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


class ActiveLadder(BaseModel):
    engine_side: Literal["BUY", "SELL"]
    strike: int
    option_type: Literal["CE", "PE"]
    symbol: str
    security_id: str
    expiry: str

    is_ghost: bool
    execution_mode: Literal["SIMULATION", "LIVE"]

    lots: int
    lot_size: int
    qty: int
    adds_done: int
    max_add_ons: int
    add_on_rise_points: float
    next_add_price: Optional[float] = None

    entry_premium_avg: float
    last_premium: Optional[float] = None

    entry_spot: float
    last_spot: Optional[float] = None
    stop_mode: str = "FIXED"
    stop_ref_spot: float = 0.0
    last_add_spot: Optional[float] = None
    stop_spot: float
    target_spot: float

    pnl_estimated: Optional[float] = None
    entry_ts: str
    exit_reason: Optional[str] = None


class EngineStatus(BaseModel):
    running: bool
    trading_enabled: bool
    execution_mode: str
    spot_ltp: Optional[float]
    spot_updated_at: Optional[str] = None
    spot_source: Optional[str] = None
    weekly_expiry: str
    instruments_loaded: bool
    active_position: Optional[ActivePosition] = None
    active_ladder: Optional[ActiveLadder] = None
    broker_mtm: Optional[float] = None
    realized_pnl_total: Optional[float] = None
    realized_pnl_real: Optional[float] = None
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
