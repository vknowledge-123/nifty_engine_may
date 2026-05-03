from __future__ import annotations

import asyncio
import contextlib
import logging
import math
from collections import deque
from dataclasses import dataclass
from datetime import datetime, time, timedelta
import time as monotime
from typing import Literal, Optional

from zoneinfo import ZoneInfo

from app.runtime.instruments import InstrumentStore, OptionContract
from app.runtime.settings import ActiveLadder, ActivePosition, EngineConfig, EngineConfigStore, EngineStatus
from app.services.dhan.feed import DhanMarketFeed, FeedTick
from app.services.dhan.rest import DhanRest, PlacedOrder


log = logging.getLogger("niftyalgo.ladder")
IST = ZoneInfo("Asia/Kolkata")

Txn = Literal["BUY", "SELL"]
OptType = Literal["CE", "PE"]
ExpirySel = Literal["CURRENT", "NEXT"]


@dataclass(slots=True)
class _ChainLeg:
    contract: OptionContract
    ltp: Optional[float] = None


@dataclass(slots=True)
class _ChainRow:
    strike: int
    ce: Optional[_ChainLeg] = None
    pe: Optional[_ChainLeg] = None


@dataclass(frozen=True, slots=True)
class _Candle:
    ts: datetime  # minute start (IST)
    open: float
    high: float
    low: float
    close: float


@dataclass(slots=True)
class _EntryFill:
    qty: int
    premium: float
    spot: float
    ts: datetime
    is_ghost: bool
    execution_mode: Literal["SIMULATION", "LIVE"]


@dataclass(slots=True)
class _Ladder:
    engine_side: Txn  # BUY=option buying engine, SELL=option selling engine
    strike: int
    option_type: OptType  # current leg
    start_option_type: OptType
    contract: OptionContract
    lot_size: int
    base_lots: int
    is_ghost: bool
    execution_mode: Literal["SIMULATION", "LIVE"]

    qty: int
    adds_done: int
    max_add_ons: int
    add_on_rise_points: float
    base_add_ref_price: float

    entry_ts: datetime
    entry_spot: float
    entry_minute: datetime

    entry_premium_avg: float
    last_premium: Optional[float] = None
    last_spot: Optional[float] = None

    stop_spot: float = 0.0
    target_spot: float = 0.0
    fixed_stop_ref_spot: float = 0.0
    stop_mode: Literal["FIXED", "CANDLE"] = "FIXED"
    stop_candle_minute: Optional[datetime] = None
    stop_rule: Optional[str] = None

    candle_stop_applied: bool = False
    had_add_before_entry_candle: bool = False
    exit_reason: Optional[str] = None
    realized_pnl: float = 0.0
    fills: list[_EntryFill] | None = None

    def bullish(self) -> bool:
        # Whether favourable move is SPOT up.
        return (self.engine_side == "BUY" and self.option_type == "CE") or (self.engine_side == "SELL" and self.option_type == "PE")


class LadderEngineController:
    """
    Option buying / selling engine with CE<->PE ladder flip and pyramiding.

    - Start/connect: websocket feed for spot + option LTP.
    - User clicks BUY/SELL on any strike/CE/PE -> starts a ladder session.
    - Stop/Target and SL are based on NIFTY SPOT.
    - Pyramiding (adds) is based on option premium move:
      - BUY engine: add on premium rise.
      - SELL engine: add on premium fall.
    """

    def __init__(self, config_store: EngineConfigStore, instruments: InstrumentStore) -> None:
        self._cfg_store = config_store
        self._instruments = instruments

        self._lock = asyncio.Lock()
        self._trade_lock = asyncio.Lock()

        self._running: bool = False
        self._market_task: Optional[asyncio.Task] = None
        self._chain_task: Optional[asyncio.Task] = None
        self._candle_task: Optional[asyncio.Task] = None
        self._mtm_task: Optional[asyncio.Task] = None
        self._spot_task: Optional[asyncio.Task] = None

        self._feed: Optional[DhanMarketFeed] = None
        self._rest: Optional[DhanRest] = None

        self._spot_security_id: Optional[str] = None
        self._spot_ltp: Optional[float] = None
        self._spot_updated_at: Optional[datetime] = None
        self._spot_source: Optional[str] = None  # "WS" | "REST" | "CANDLE"
        self._ltps: dict[str, float] = {}

        self._subscribed_option_ids: set[str] = set()
        self._sub_lock = asyncio.Lock()
        self._feed_generation_seen: int = 0
        self._chain_rows: list[_ChainRow] = []
        self._chain_atm: Optional[int] = None
        self._chain_sig: Optional[tuple[int, str, int, int]] = None

        self._ladder: Optional[_Ladder] = None
        self._close_inflight: bool = False
        self._add_inflight: bool = False

        self._candles: dict[datetime, _Candle] = {}
        self._last_candle_fetch_ts: Optional[datetime] = None

        self._broker_mtm: Optional[float] = None
        self._broker_positions: Optional[list[dict]] = None
        self._last_mtm_ts: Optional[datetime] = None

        self._trade_log: deque[dict] = deque(maxlen=250)
        self._closed_ladders: deque[dict] = deque(maxlen=150)
        self._realized_pnl_total: float = 0.0
        self._realized_pnl_real: float = 0.0

        self._rest_ltp_inflight: bool = False
        self._rest_ltp_last_ts: float = 0.0

        self._last_error: Optional[str] = None
        self._feed_error: Optional[str] = None
        self._cfg_version_seen: int = 0
        self._flip_task: Optional[asyncio.Task] = None

        # Spot freshness: use throttled REST LTP fallback so spot-based exits still trigger
        # even if marketfeed ticks stall (common source of "SL hit but not squared off").
        self._last_spot_tick_mono: float = 0.0
        self._last_spot_ws_mono: float = 0.0
        self._last_spot_rest_mono: float = 0.0
        self._spot_rest_inflight: bool = False
        # Keep this reasonably low for tighter SL execution when spot ticks are sparse.
        self._spot_rest_interval_s: float = 0.25
        self._spot_stale_s: float = 3.0
        self._spot_rest_backoff_until_mono: float = 0.0

    @staticmethod
    def _now_ist() -> datetime:
        return datetime.now(tz=IST)

    @staticmethod
    def _expiry_offset(sel: ExpirySel) -> int:
        return 0 if sel == "CURRENT" else 1

    @staticmethod
    def _round_to_step(spot: float, step: int) -> int:
        if step <= 0:
            step = 50
        return int(math.floor((float(spot) + (step / 2.0)) / step) * step)

    @staticmethod
    def _floor_minute(ts: datetime) -> datetime:
        if ts.tzinfo is None:
            raise ValueError("ts must be timezone-aware")
        return ts.replace(second=0, microsecond=0)

    def _log_event(self, event: str, **fields: object) -> None:
        self._trade_log.append(
            {
                "ts": self._now_ist().isoformat(),
                "event": event,
                **fields,
            }
        )

    async def start(self) -> None:
        async with self._lock:
            if self._running:
                return

            cfg = await self._cfg_store.get()
            if not cfg.client_id or not cfg.access_token:
                msg = "Set client_id and access_token in settings before connecting."
                self._last_error = msg
                raise RuntimeError(msg)

            spot_security_id = str(cfg.nifty_spot_security_id)
            if self._instruments.loaded:
                try:
                    spot_security_id = await self._instruments.nifty_spot_security_id(default=spot_security_id)
                except Exception:
                    pass

            self._spot_security_id = spot_security_id
            self._spot_ltp = None
            self._spot_updated_at = None
            self._spot_source = None
            self._ltps.clear()
            self._subscribed_option_ids.clear()
            self._feed_generation_seen = 0
            self._chain_rows = []
            self._chain_atm = None
            self._chain_sig = None

            self._candles.clear()
            self._last_candle_fetch_ts = None

            self._broker_mtm = None
            self._broker_positions = None
            self._last_mtm_ts = None

            self._feed_error = None
            self._last_error = None

            self._feed = DhanMarketFeed(cfg.client_id, cfg.access_token, spot_security_id=spot_security_id)
            self._rest = DhanRest(cfg.client_id, cfg.access_token)
            self._cfg_version_seen = int(self._cfg_store.version())

            self._running = True
            self._market_task = asyncio.create_task(self._market_loop(), name="ladder_market")
            self._chain_task = asyncio.create_task(self._chain_loop(), name="ladder_chain")
            self._candle_task = asyncio.create_task(self._candle_loop(), name="ladder_candles")
            self._mtm_task = asyncio.create_task(self._mtm_loop(), name="ladder_mtm")
            self._spot_task = asyncio.create_task(self._spot_fallback_loop(), name="ladder_spot_fallback")

    async def stop(self, *, force: bool = False) -> None:
        async with self._lock:
            if not self._running:
                return
            if self._ladder is not None and not force:
                raise RuntimeError("Active ladder exists. Stop button (square-off) before disconnecting.")

            self._running = False
            tasks = [
                t
                for t in (self._market_task, self._chain_task, self._candle_task, self._mtm_task, self._spot_task)
                if t is not None
            ]
            for t in tasks:
                if not t.done():
                    t.cancel()
            for t in tasks:
                try:
                    await t
                except Exception:
                    pass

            if self._flip_task is not None and not self._flip_task.done():
                self._flip_task.cancel()
                with contextlib.suppress(Exception):
                    await self._flip_task
            self._flip_task = None

            if self._feed is not None:
                try:
                    await self._feed.disconnect()
                except Exception:
                    pass
            self._feed = None
            self._rest = None

            self._spot_ltp = None
            self._ltps.clear()
            self._subscribed_option_ids.clear()
            self._chain_rows = []
            self._chain_atm = None
            self._feed_error = None
            self._spot_task = None
            self._last_spot_tick_mono = 0.0
            self._last_spot_ws_mono = 0.0
            self._last_spot_rest_mono = 0.0
            self._spot_rest_inflight = False
            self._spot_updated_at = None
            self._spot_source = None

    async def stop_button(self) -> None:
        # Manual emergency stop:
        # - square off any live ladder
        # - disconnect engine (no more flips / orders)
        if self._ladder is not None and not self._close_inflight:
            self._close_inflight = True
            try:
                await self._close_ladder(reason="STOP_BUTTON")
            finally:
                self._close_inflight = False
        await self.stop(force=True)

    async def status(self) -> EngineStatus:
        cfg = self._cfg_store.current()
        active_pos = self._active_position_model()
        active_ladder = self._active_ladder_model()
        return EngineStatus(
            running=bool(self._running),
            trading_enabled=bool(cfg.trading_enabled),
            execution_mode=str(cfg.execution_mode),
            spot_ltp=self._spot_ltp,
            spot_updated_at=self._spot_updated_at.isoformat() if self._spot_updated_at is not None else None,
            spot_source=self._spot_source,
            weekly_expiry=str(cfg.weekly_expiry),
            instruments_loaded=bool(self._instruments.loaded),
            active_position=active_pos,
            active_ladder=active_ladder,
            broker_mtm=self._broker_mtm,
            realized_pnl_total=float(self._realized_pnl_total),
            realized_pnl_real=float(self._realized_pnl_real),
            last_error=self._last_error,
            feed_error=self._feed_error,
        )

    def _active_position_model(self) -> Optional[ActivePosition]:
        # Back-compat: expose the current ladder leg as an "active_position".
        lad = self._ladder
        if lad is None:
            return None

        ltp = lad.last_premium
        pnl: Optional[float] = None
        if ltp is not None:
            if lad.engine_side == "BUY":
                pnl = (ltp - lad.entry_premium_avg) * lad.qty
            else:
                pnl = (lad.entry_premium_avg - ltp) * lad.qty

        return ActivePosition(
            side=lad.engine_side,
            symbol=lad.contract.trading_symbol,
            security_id=str(lad.contract.security_id),
            option_type=str(lad.contract.option_type),
            strike=int(lad.contract.strike),
            expiry=lad.contract.expiry.date().isoformat(),
            qty=int(lad.qty),
            entry_price=float(lad.entry_premium_avg),
            last_ltp=float(ltp) if ltp is not None else None,
            stop_loss_price=float(lad.stop_spot),
            trailing_stop_price=None,
            target_price=float(lad.target_spot),
            pnl=float(pnl) if pnl is not None else None,
            entry_ts=lad.entry_ts.isoformat(),
            exit_reason=lad.exit_reason,
        )

    def _active_ladder_model(self) -> Optional[ActiveLadder]:
        lad = self._ladder
        if lad is None:
            return None

        next_add = self._next_add_threshold(lad)
        last_add_spot: Optional[float] = None
        if lad.fills:
            # last fill spot helps explain SL ref when pyramiding is done before candle SL is applied.
            last_add_spot = float(lad.fills[-1].spot)
        pnl_est: Optional[float] = None
        if lad.last_premium is not None:
            if lad.engine_side == "BUY":
                pnl_est = (lad.last_premium - lad.entry_premium_avg) * lad.qty
            else:
                pnl_est = (lad.entry_premium_avg - lad.last_premium) * lad.qty

        return ActiveLadder(
            engine_side=lad.engine_side,
            strike=int(lad.strike),
            option_type=str(lad.option_type),
            symbol=lad.contract.trading_symbol,
            security_id=str(lad.contract.security_id),
            expiry=lad.contract.expiry.date().isoformat(),
            is_ghost=bool(lad.is_ghost),
            execution_mode=str(lad.execution_mode),
            lots=int(lad.base_lots),
            lot_size=int(lad.lot_size),
            qty=int(lad.qty),
            adds_done=int(lad.adds_done),
            max_add_ons=int(lad.max_add_ons),
            add_on_rise_points=float(lad.add_on_rise_points),
            next_add_price=float(next_add) if next_add is not None else None,
            entry_premium_avg=float(lad.entry_premium_avg),
            last_premium=float(lad.last_premium) if lad.last_premium is not None else None,
            entry_spot=float(lad.entry_spot),
            last_spot=float(lad.last_spot) if lad.last_spot is not None else None,
            stop_mode=str(getattr(lad, "stop_mode", "FIXED")),
            stop_ref_spot=float(getattr(lad, "fixed_stop_ref_spot", lad.entry_spot)),
            last_add_spot=float(last_add_spot) if last_add_spot is not None else None,
            stop_spot=float(lad.stop_spot),
            target_spot=float(lad.target_spot),
            pnl_estimated=float(pnl_est) if pnl_est is not None else None,
            entry_ts=lad.entry_ts.isoformat(),
            exit_reason=lad.exit_reason,
        )

    async def dashboard_snapshot(self) -> dict:
        st = await self.status()
        now = self._now_ist()
        candle = self._latest_candle_at_or_before(self._floor_minute(now))
        candle_completed = (candle is not None) and (candle.ts < self._floor_minute(now))
        chain = []
        for row in self._chain_rows:
            ce = row.ce
            pe = row.pe
            chain.append(
                {
                    "strike": row.strike,
                    "ce": None
                    if ce is None
                    else {"symbol": ce.contract.trading_symbol, "security_id": ce.contract.security_id, "ltp": ce.ltp},
                    "pe": None
                    if pe is None
                    else {"symbol": pe.contract.trading_symbol, "security_id": pe.contract.security_id, "ltp": pe.ltp},
                }
            )
        return {
            "status": st.model_dump(),
            "atm_strike": self._chain_atm,
            "chain": chain,
            "latest_1m_candle": None
            if candle is None
            else {
                "ts": candle.ts.isoformat(),
                "open": float(candle.open),
                "high": float(candle.high),
                "low": float(candle.low),
                "close": float(candle.close),
                "completed": bool(candle_completed),
            },
            "last_candle_fetch_ts": self._last_candle_fetch_ts.isoformat() if self._last_candle_fetch_ts is not None else None,
            "trade_log": list(self._trade_log),
            "closed_ladders": list(self._closed_ladders),
            "net_pnl_total": float(self._realized_pnl_total),
            "net_pnl_real": float(self._realized_pnl_real),
            "broker_positions": self._broker_positions,
            "last_mtm_ts": self._last_mtm_ts.isoformat() if self._last_mtm_ts is not None else None,
        }

    async def open_position(self, *, strike: int, option_type: OptType, side: Txn) -> EngineStatus:
        async with self._trade_lock:
            if not self._running or self._feed is None or self._rest is None:
                raise RuntimeError("Not connected. Connect first.")
            if self._ladder is not None:
                raise RuntimeError("An active ladder already exists. Stop it before starting a new one.")
            if not self._instruments.loaded:
                raise RuntimeError("Instrument master not loaded. Refresh instruments first.")

            opt = str(option_type).upper().strip()
            if opt not in ("CE", "PE"):
                raise RuntimeError("option_type must be CE or PE")

            cfg = self._cfg_store.current()
            now = self._now_ist()
            expiry_offset = self._expiry_offset(cfg.weekly_expiry)
            contract = await self._instruments.get_weekly_option(
                now_ist=now, strike=int(strike), option_type=opt, expiry_offset=expiry_offset
            )

            secid = str(contract.security_id)
            await self._ensure_subscriptions({secid}, prune=False)

        ltp = self._ltps.get(secid)
        if ltp is None:
            ltp = await self._best_effort_option_ltp(secid, timeout_s=8.0)
        if ltp is None:
            raise RuntimeError(
                "Option LTP not available yet. Wait for chain/quotes to populate and try again. "
                "If this persists, your account may not have F&O market data enabled."
            )
        spot = self._spot_ltp
        if spot is None:
            raise RuntimeError("Spot LTP not available yet. Wait for spot to populate and try again.")

        execution_mode: Literal["SIMULATION", "LIVE"]
        if str(cfg.execution_mode).upper() == "LIVE":
            if not cfg.trading_enabled:
                raise RuntimeError("LIVE mode requires trading_enabled=true.")
            execution_mode = "LIVE"
        else:
            execution_mode = "SIMULATION"
        is_ghost = False  # first ladder is always real; ghost applies only after flip

        lot_size = max(1, int(contract.lot_size or 1))
        lots = max(1, int(getattr(cfg, "lots", 1) or 1))
        qty = lot_size * lots

        entry_premium = float(ltp)
        if execution_mode == "LIVE":
            tag = f"ladder_open_{side.lower()}_{secid}"
            placed = await self._place_order(txn=side, security_id=secid, qty=qty, tag=tag)
            entry_premium = await self._best_effort_entry_price(placed, correlation_id=tag, fallback=float(ltp))

        target_points = float(cfg.target_points or 0.0)
        bullish = (side == "BUY" and opt == "CE") or (side == "SELL" and opt == "PE")
        target_spot = float(spot + target_points) if bullish else float(spot - target_points)
        # Stop loss must be candle-based (prev 1m candle low/high +/- buffer) only.
        if not self._candles:
            await self._fetch_candles()
        entry_minute = self._floor_minute(now)
        stop_candle = self._latest_candle_at_or_before(entry_minute - timedelta(minutes=1)) or self._latest_candle_at_or_before(entry_minute)
        if stop_candle is None:
            raise RuntimeError("1m candle data not available yet for candle-based SL. Wait a moment and try again.")
        buffer = float(getattr(cfg, "sl_buffer_points", 0.0) or 0.0)
        if bullish:
            stop_ref = float(stop_candle.low)
            stop_spot = float(stop_candle.low - buffer)
        else:
            stop_ref = float(stop_candle.high)
            stop_spot = float(stop_candle.high + buffer)

        lad = _Ladder(
            engine_side=side,
            strike=int(strike),
            option_type=opt,  # type: ignore[arg-type]
            start_option_type=opt,  # type: ignore[arg-type]
            contract=contract,
            lot_size=lot_size,
            base_lots=lots,
            is_ghost=is_ghost,
            execution_mode=execution_mode,
            qty=qty,
            adds_done=0,
            max_add_ons=int(getattr(cfg, "max_add_ons", 0) or 0),
            add_on_rise_points=float(getattr(cfg, "add_on_rise_points", 0.0) or 0.0),
            base_add_ref_price=float(entry_premium),
            entry_ts=now,
            entry_spot=float(spot),
            entry_minute=entry_minute,
            entry_premium_avg=float(entry_premium),
            last_premium=float(ltp),
            last_spot=float(spot),
            stop_spot=float(stop_spot),
            target_spot=float(target_spot),
            fixed_stop_ref_spot=float(stop_ref),
            stop_mode="CANDLE",
            stop_candle_minute=stop_candle.ts,
            stop_rule="CANDLE_REF",
            candle_stop_applied=True,
            had_add_before_entry_candle=False,
            exit_reason=None,
            realized_pnl=0.0,
            fills=[
                _EntryFill(
                    qty=qty,
                    premium=float(entry_premium),
                    spot=float(spot),
                    ts=now,
                    is_ghost=is_ghost,
                    execution_mode=execution_mode,
                )
            ],
        )
        self._ladder = lad
        self._log_event(
            "OPEN",
            engine_side=side,
            option_type=opt,
            strike=int(strike),
            qty=int(qty),
            premium=float(entry_premium),
            spot=float(spot),
            execution_mode=execution_mode,
            is_ghost=bool(is_ghost),
        )
        # Already candle-based at entry; still try in case the entry-minute candle arrives later.
        self._maybe_apply_entry_candle_stop()
        return await self.status()

    async def square_off(self, *, reason: str = "MANUAL") -> EngineStatus:
        async with self._trade_lock:
            lad = self._ladder
            if lad is None:
                return await self.status()
            if self._close_inflight:
                return await self.status()
            self._close_inflight = True
            asyncio.create_task(self._close_ladder(reason=str(reason)), name="ladder_manual_close")
            return await self.status()

    async def _market_loop(self) -> None:
        assert self._feed is not None
        spot_id = str(self._spot_security_id or "")
        try:
            await self._feed.connect()
        except Exception as e:
            self._feed_error = str(e)
            self._last_error = f"marketfeed connect failed: {e}"
            log.exception("marketfeed connect failed: %s", e)

        while self._running:
            try:
                tick = await self._feed.recv_tick()
                # Surface feed-side errors even when no ticks are arriving.
                self._feed_error = getattr(self._feed, "last_error", None)
                if tick is None:
                    continue
                self._apply_tick(tick, spot_id=spot_id)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._feed_error = str(e)
                self._last_error = f"marketfeed error: {e}"
                log.exception("marketfeed error: %s", e)
                await asyncio.sleep(0.2)

    def _apply_tick(self, tick: FeedTick, *, spot_id: str) -> None:
        secid = str(tick.security_id)
        ltp = float(tick.ltp)
        self._feed_error = getattr(self._feed, "last_error", None) if self._feed is not None else None

        lad = self._ladder

        if secid == spot_id:
            now = self._now_ist()
            self._spot_ltp = ltp
            self._spot_updated_at = now
            self._spot_source = "WS"
            self._last_spot_tick_mono = monotime.monotonic()
            self._last_spot_ws_mono = self._last_spot_tick_mono
            if lad is not None:
                prev_spot = lad.last_spot
                lad.last_spot = ltp
                self._apply_config_updates_if_needed()
                self._check_dynamic_sl(lad, prev_spot=prev_spot, spot=ltp)
                self._check_spot_exits(lad, spot=ltp)
            return

        self._ltps[secid] = ltp

        if lad is None:
            return
        if secid != str(lad.contract.security_id):
            return
        lad.last_premium = ltp
        self._apply_config_updates_if_needed()
        self._maybe_pyramid(lad, premium=ltp)

    def _apply_config_updates_if_needed(self) -> None:
        v = int(self._cfg_store.version())
        if v == self._cfg_version_seen:
            return
        self._cfg_version_seen = v

        lad = self._ladder
        if lad is None:
            return
        cfg = self._cfg_store.current()

        # Apply pyramiding changes to the active ladder.
        lad.base_lots = max(1, int(getattr(cfg, "lots", lad.base_lots) or lad.base_lots))
        lad.max_add_ons = int(getattr(cfg, "max_add_ons", lad.max_add_ons) or 0)
        lad.add_on_rise_points = float(getattr(cfg, "add_on_rise_points", lad.add_on_rise_points) or 0.0)

        # Apply target changes (relative to current leg entry spot).
        target_points = float(getattr(cfg, "target_points", 0.0) or 0.0)
        if lad.bullish():
            lad.target_spot = float(lad.entry_spot + target_points)
        else:
            lad.target_spot = float(lad.entry_spot - target_points)

    def _check_dynamic_sl(self, lad: _Ladder, *, prev_spot: Optional[float], spot: float) -> None:
        if self._close_inflight:
            return
        cfg = self._cfg_store.current()
        if not bool(getattr(cfg, "dynamic_sl_enabled", False)):
            return
        level = getattr(cfg, "dynamic_sl_spot", None)
        if level is None:
            return
        try:
            lvl = float(level)
        except Exception:
            return
        if prev_spot is None:
            return

        crossed = False
        if spot == lvl or prev_spot == lvl:
            crossed = True
        elif prev_spot < lvl <= spot:
            crossed = True
        elif prev_spot > lvl >= spot:
            crossed = True

        if not crossed:
            return

        self._close_inflight = True
        asyncio.create_task(self._close_ladder(reason="DYNAMIC_SL"), name="ladder_close_dynamic_sl")

    def _check_spot_exits(self, lad: _Ladder, *, spot: float) -> None:
        if self._close_inflight:
            return

        bullish = lad.bullish()
        if bullish:
            if spot >= float(lad.target_spot):
                self._close_inflight = True
                asyncio.create_task(self._close_ladder(reason="TARGET"), name="ladder_close_target")
                return
            if spot <= float(lad.stop_spot):
                self._close_inflight = True
                asyncio.create_task(self._close_ladder(reason="STOP"), name="ladder_close_stop")
                return
        else:
            if spot <= float(lad.target_spot):
                self._close_inflight = True
                asyncio.create_task(self._close_ladder(reason="TARGET"), name="ladder_close_target")
                return
            if spot >= float(lad.stop_spot):
                self._close_inflight = True
                asyncio.create_task(self._close_ladder(reason="STOP"), name="ladder_close_stop")
                return

    def _next_add_threshold(self, lad: _Ladder) -> Optional[float]:
        if lad.add_on_rise_points <= 0.0:
            return None
        if lad.max_add_ons <= 0:
            return None
        if lad.adds_done >= lad.max_add_ons:
            return None
        step = float(lad.add_on_rise_points)
        n = int(lad.adds_done + 1)
        if lad.engine_side == "BUY":
            return float(lad.base_add_ref_price + (n * step))
        return float(lad.base_add_ref_price - (n * step))

    def _maybe_pyramid(self, lad: _Ladder, *, premium: float) -> None:
        if self._add_inflight or self._close_inflight:
            return
        threshold = self._next_add_threshold(lad)
        if threshold is None:
            return
        if lad.engine_side == "BUY":
            if premium < threshold:
                return
        else:
            if premium > threshold:
                return
        self._add_inflight = True
        asyncio.create_task(self._do_adds(), name="ladder_adds")

    async def _do_adds(self) -> None:
        try:
            async with self._trade_lock:
                lad = self._ladder
                if lad is None or self._rest is None:
                    return
                cfg = self._cfg_store.current()

                while True:
                    if lad.adds_done >= lad.max_add_ons or lad.add_on_rise_points <= 0.0:
                        return

                    secid = str(lad.contract.security_id)
                    premium = self._ltps.get(secid)
                    spot = self._spot_ltp
                    if premium is None or spot is None:
                        return

                    threshold = self._next_add_threshold(lad)
                    if threshold is None:
                        return

                    if lad.engine_side == "BUY":
                        if premium < threshold:
                            return
                    else:
                        if premium > threshold:
                            return

                    add_qty = int(lad.lot_size * lad.base_lots)
                    fill_premium = float(premium)
                    if lad.execution_mode == "LIVE":
                        tag = f"ladder_add_{lad.engine_side.lower()}_{secid}_{lad.adds_done+1}"
                        placed = await self._place_order(txn=lad.engine_side, security_id=secid, qty=add_qty, tag=tag)
                        fill_premium = await self._best_effort_entry_price(placed, correlation_id=tag, fallback=float(premium))

                    total_qty = int(lad.qty + add_qty)
                    lad.entry_premium_avg = float(
                        ((lad.entry_premium_avg * lad.qty) + (fill_premium * add_qty)) / max(1, total_qty)
                    )
                    lad.qty = total_qty
                    lad.adds_done += 1
                    lad.last_premium = float(premium)
                    lad.last_spot = float(spot)

                    if lad.fills is None:
                        lad.fills = []
                    lad.fills.append(
                        _EntryFill(
                            qty=add_qty,
                            premium=float(fill_premium),
                            spot=float(spot),
                            ts=self._now_ist(),
                            is_ghost=bool(lad.is_ghost),
                            execution_mode=lad.execution_mode,
                        )
                    )

                    if not lad.candle_stop_applied:
                        lad.had_add_before_entry_candle = True

                    self._log_event(
                        "ADD",
                        engine_side=lad.engine_side,
                        option_type=lad.option_type,
                        strike=int(lad.strike),
                        add_no=int(lad.adds_done),
                        qty=int(add_qty),
                        premium=float(fill_premium),
                        spot=float(spot),
                        execution_mode=lad.execution_mode,
                        is_ghost=bool(lad.is_ghost),
                        new_qty=int(lad.qty),
                        new_avg=float(lad.entry_premium_avg),
                        stop_ref_spot=float(lad.fixed_stop_ref_spot),
                        sl_points=float(cfg.stop_loss_points or 0.0),
                        stop_mode=str(getattr(lad, "stop_mode", "FIXED")),
                        new_stop_spot=float(lad.stop_spot),
                    )

                    # Re-evaluate candle trailing after add (tighten-only).
                    self._trail_stop_from_latest_candle()
        except Exception as e:
            self._last_error = f"add error: {e}"
            log.exception("add error: %s", e)
        finally:
            self._add_inflight = False

    async def _close_ladder(self, *, reason: str) -> None:
        try:
            async with self._trade_lock:
                lad = self._ladder
                if lad is None:
                    return
                cfg = self._cfg_store.current()

                secid = str(lad.contract.security_id)
                exit_premium = lad.last_premium or self._ltps.get(secid)
                exit_premium_f = float(exit_premium) if exit_premium is not None else float(lad.entry_premium_avg)
                exit_spot_f = float(lad.last_spot) if lad.last_spot is not None else float(self._spot_ltp or lad.entry_spot)

                if lad.execution_mode == "LIVE" and self._rest is not None:
                    close_txn: Txn = "SELL" if lad.engine_side == "BUY" else "BUY"
                    qty = int(lad.qty)
                    tag = f"ladder_close_{reason.lower()}_{secid}"
                    placed = await self._place_order(txn=close_txn, security_id=secid, qty=qty, tag=tag)
                    exit_premium_f = await self._best_effort_entry_price(placed, correlation_id=tag, fallback=exit_premium_f)

                # Realized P&L (premium-based estimate; stop/target is spot-based but user wants P&L on dashboard).
                if lad.engine_side == "BUY":
                    lad.realized_pnl += float((exit_premium_f - lad.entry_premium_avg) * lad.qty)
                else:
                    lad.realized_pnl += float((lad.entry_premium_avg - exit_premium_f) * lad.qty)

                lad.exit_reason = str(reason)

                pnl_val = float(lad.realized_pnl)
                self._realized_pnl_total += pnl_val
                if not bool(lad.is_ghost):
                    self._realized_pnl_real += pnl_val

                fills = []
                if lad.fills:
                    for f in lad.fills:
                        fills.append(
                            {
                                "ts": f.ts.isoformat(),
                                "qty": int(f.qty),
                                "premium": float(f.premium),
                                "spot": float(f.spot),
                                "is_ghost": bool(f.is_ghost),
                                "execution_mode": str(f.execution_mode),
                            }
                        )

                self._closed_ladders.appendleft(
                    {
                        "exit_ts": self._now_ist().isoformat(),
                        "reason": str(reason),
                        "pnl": pnl_val,
                        "engine_side": lad.engine_side,
                        "option_type": lad.option_type,
                        "strike": int(lad.strike),
                        "symbol": lad.contract.trading_symbol,
                        "security_id": str(lad.contract.security_id),
                        "expiry": lad.contract.expiry.date().isoformat(),
                        "qty": int(lad.qty),
                        "adds_done": int(lad.adds_done),
                        "is_ghost": bool(lad.is_ghost),
                        "execution_mode": str(lad.execution_mode),
                        "entry_ts": lad.entry_ts.isoformat(),
                        "entry_spot": float(lad.entry_spot),
                        "exit_spot": float(exit_spot_f),
                        "entry_premium_avg": float(lad.entry_premium_avg),
                        "exit_premium": float(exit_premium_f),
                        "stop_mode": str(getattr(lad, "stop_mode", "FIXED")),
                        "stop_ref_spot": float(getattr(lad, "fixed_stop_ref_spot", lad.entry_spot)),
                        "stop_spot": float(lad.stop_spot),
                        "target_spot": float(lad.target_spot),
                        "fills": fills,
                    }
                )
                self._log_event(
                    "CLOSE",
                    reason=str(reason),
                    engine_side=lad.engine_side,
                    option_type=lad.option_type,
                    strike=int(lad.strike),
                    qty=int(lad.qty),
                    exit_premium=float(exit_premium_f),
                    exit_spot=float(exit_spot_f),
                    stop_spot=float(lad.stop_spot),
                    stop_ref_spot=float(getattr(lad, "fixed_stop_ref_spot", lad.entry_spot)),
                    stop_mode=str(getattr(lad, "stop_mode", "FIXED")),
                    realized_pnl=pnl_val,
                    is_ghost=bool(lad.is_ghost),
                    execution_mode=lad.execution_mode,
                )

                # Decide what to do next.
                stop_engine = False
                start_next = False
                next_opt: Optional[OptType] = None

                if str(reason).upper() in ("TARGET", "DYNAMIC_SL"):
                    stop_engine = True
                elif str(reason).upper() == "STOP":
                    if bool(getattr(cfg, "last_trade", False)):
                        stop_engine = True
                    elif bool(getattr(cfg, "full_automation", False)):
                        start_next = True
                        next_opt = "PE" if lad.option_type == "CE" else "CE"

                # Clear current ladder before flipping/stopping.
                self._ladder = None

                if start_next and next_opt is not None:
                    try:
                        if self._flip_task is not None and not self._flip_task.done():
                            self._flip_task.cancel()
                        self._flip_task = asyncio.create_task(
                            self._flip_with_retry(
                                strike=int(lad.strike),
                                option_type=next_opt,
                                engine_side=lad.engine_side,
                                start_option_type=lad.start_option_type,
                            ),
                            name="ladder_flip_retry",
                        )
                    except Exception as e:
                        self._last_error = f"flip schedule failed: {e}"
                        log.exception("flip schedule failed: %s", e)

                if stop_engine:
                    asyncio.create_task(self.stop(force=True), name="ladder_stop_after_close")
        except Exception as e:
            self._last_error = f"close error: {e}"
            log.exception("close error: %s", e)
        finally:
            self._close_inflight = False

    async def _start_next_ladder(
        self,
        *,
        strike: int,
        option_type: OptType,
        engine_side: Txn,
        start_option_type: OptType,
    ) -> None:
        if not self._running or self._feed is None or self._rest is None:
            raise RuntimeError("Not connected.")
        cfg = self._cfg_store.current()
        now = self._now_ist()
        expiry_offset = self._expiry_offset(cfg.weekly_expiry)
        contract = await self._instruments.get_weekly_option(
            now_ist=now, strike=int(strike), option_type=option_type, expiry_offset=expiry_offset
        )
        secid = str(contract.security_id)
        await self._ensure_subscriptions({secid}, prune=False)

        ltp = self._ltps.get(secid)
        if ltp is None:
            ltp = await self._best_effort_option_ltp(secid, timeout_s=8.0)
        if ltp is None:
            # Provide more context for debugging.
            raise RuntimeError(
                "Option LTP not available for flipped ladder yet. "
                "If your Dhan account doesn't have F&O market data enabled, option LTP may stay unavailable."
            )
        spot = self._spot_ltp
        if spot is None:
            raise RuntimeError("Spot LTP not available for flipped ladder yet.")

        is_ghost = False
        execution_mode: Literal["SIMULATION", "LIVE"]
        if str(cfg.execution_mode).upper() == "LIVE":
            if not cfg.trading_enabled:
                raise RuntimeError("LIVE mode requires trading_enabled=true.")
            execution_mode = "LIVE"
        else:
            execution_mode = "SIMULATION"

        if bool(getattr(cfg, "ghost_monitoring", False)):
            is_ghost = bool(option_type != start_option_type)
        if is_ghost:
            execution_mode = "SIMULATION"

        lot_size = max(1, int(contract.lot_size or 1))
        lots = max(1, int(getattr(cfg, "lots", 1) or 1))
        qty = lot_size * lots

        entry_premium = float(ltp)
        if execution_mode == "LIVE":
            tag = f"ladder_flip_open_{engine_side.lower()}_{secid}"
            placed = await self._place_order(txn=engine_side, security_id=secid, qty=qty, tag=tag)
            entry_premium = await self._best_effort_entry_price(placed, correlation_id=tag, fallback=float(ltp))

        target_points = float(cfg.target_points or 0.0)
        bullish = (engine_side == "BUY" and option_type == "CE") or (engine_side == "SELL" and option_type == "PE")
        target_spot = float(spot + target_points) if bullish else float(spot - target_points)

        # Stop loss must be candle-based (prev 1m candle low/high +/- buffer) only.
        if not self._candles:
            await self._fetch_candles()
        entry_minute = self._floor_minute(now)
        stop_candle = self._latest_candle_at_or_before(entry_minute - timedelta(minutes=1)) or self._latest_candle_at_or_before(entry_minute)
        if stop_candle is None:
            raise RuntimeError("1m candle data not available yet for candle-based SL (flip).")
        buffer = float(getattr(cfg, "sl_buffer_points", 0.0) or 0.0)
        if bullish:
            stop_ref = float(stop_candle.low)
            stop_spot = float(stop_candle.low - buffer)
        else:
            stop_ref = float(stop_candle.high)
            stop_spot = float(stop_candle.high + buffer)

        lad = _Ladder(
            engine_side=engine_side,
            strike=int(strike),
            option_type=option_type,
            start_option_type=start_option_type,
            contract=contract,
            lot_size=lot_size,
            base_lots=lots,
            is_ghost=is_ghost,
            execution_mode=execution_mode,
            qty=qty,
            adds_done=0,
            max_add_ons=int(getattr(cfg, "max_add_ons", 0) or 0),
            add_on_rise_points=float(getattr(cfg, "add_on_rise_points", 0.0) or 0.0),
            base_add_ref_price=float(entry_premium),
            entry_ts=now,
            entry_spot=float(spot),
            entry_minute=entry_minute,
            entry_premium_avg=float(entry_premium),
            last_premium=float(ltp),
            last_spot=float(spot),
            stop_spot=float(stop_spot),
            target_spot=float(target_spot),
            fixed_stop_ref_spot=float(stop_ref),
            stop_mode="CANDLE",
            stop_candle_minute=stop_candle.ts,
            stop_rule="CANDLE_REF",
            candle_stop_applied=True,
            had_add_before_entry_candle=False,
            exit_reason=None,
            realized_pnl=0.0,
            fills=[_EntryFill(qty=qty, premium=float(entry_premium), spot=float(spot), ts=now, is_ghost=is_ghost, execution_mode=execution_mode)],
        )
        self._ladder = lad
        self._log_event(
            "FLIP_OPEN",
            engine_side=engine_side,
            option_type=option_type,
            strike=int(strike),
            qty=int(qty),
            premium=float(entry_premium),
            spot=float(spot),
            execution_mode=execution_mode,
            is_ghost=bool(is_ghost),
        )
        self._maybe_apply_entry_candle_stop()

    async def _flip_with_retry(
        self,
        *,
        strike: int,
        option_type: OptType,
        engine_side: Txn,
        start_option_type: OptType,
    ) -> None:
        attempt = 0
        while self._running and self._ladder is None:
            attempt += 1
            try:
                await self._start_next_ladder(
                    strike=int(strike),
                    option_type=option_type,
                    engine_side=engine_side,
                    start_option_type=start_option_type,
                )
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._last_error = f"flip pending (attempt {attempt}): {e}"
                await asyncio.sleep(min(3.0, 0.25 * (1.4 ** min(12, attempt))))

    async def _best_effort_option_ltp(self, security_id: str, *, timeout_s: float) -> Optional[float]:
        secid = str(security_id).strip()
        if not secid:
            return None
        cached = self._ltps.get(secid)
        if cached is not None:
            return float(cached)

        # Websocket-only: wait briefly for a tick to arrive after subscription.
        deadline = asyncio.get_running_loop().time() + max(0.2, float(timeout_s))
        while asyncio.get_running_loop().time() < deadline:
            cached = self._ltps.get(secid)
            if cached is not None:
                return float(cached)
            await asyncio.sleep(0.05)
        return None

    async def _chain_loop(self) -> None:
        while self._running:
            try:
                await self._rebuild_chain_if_needed()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._last_error = str(e)
                log.exception("chain rebuild error: %s", e)
            await asyncio.sleep(0.5)

    async def _rebuild_chain_if_needed(self) -> None:
        cfg = self._cfg_store.current()
        spot = self._spot_ltp
        if spot is None:
            return
        if not self._instruments.loaded:
            self._chain_rows = []
            self._chain_atm = None
            self._chain_sig = None
            return

        step = int(cfg.strike_step or 50)
        n = int(cfg.chain_strikes_each_side or 10)
        atm = self._round_to_step(spot, step)
        sig = (atm, str(cfg.weekly_expiry), step, n)
        if self._chain_sig == sig and self._chain_rows:
            self._refresh_chain_ltps()
            return

        now = self._now_ist()
        expiry_offset = self._expiry_offset(cfg.weekly_expiry)
        rows: list[_ChainRow] = []
        wanted: set[str] = set()

        for i in range(-n, n + 1):
            strike = int(atm + (i * step))
            ce = await self._instruments.get_weekly_option(now_ist=now, strike=strike, option_type="CE", expiry_offset=expiry_offset)
            pe = await self._instruments.get_weekly_option(now_ist=now, strike=strike, option_type="PE", expiry_offset=expiry_offset)
            ce_leg = _ChainLeg(contract=ce, ltp=self._ltps.get(str(ce.security_id)))
            pe_leg = _ChainLeg(contract=pe, ltp=self._ltps.get(str(pe.security_id)))
            rows.append(_ChainRow(strike=strike, ce=ce_leg, pe=pe_leg))
            wanted.add(str(ce.security_id))
            wanted.add(str(pe.security_id))

        if self._ladder is not None:
            wanted.add(str(self._ladder.contract.security_id))

        await self._ensure_subscriptions(wanted, prune=False)
        self._chain_rows = rows
        self._chain_atm = atm
        self._chain_sig = sig
        self._refresh_chain_ltps()

    def _refresh_chain_ltps(self) -> None:
        for row in self._chain_rows:
            if row.ce is not None:
                row.ce.ltp = self._ltps.get(str(row.ce.contract.security_id))
            if row.pe is not None:
                row.pe.ltp = self._ltps.get(str(row.pe.contract.security_id))

    async def _ensure_subscriptions(self, wanted: set[str], *, prune: bool = True) -> None:
        if not self._running or self._feed is None:
            return
        wanted = {str(x) for x in wanted if x}
        async with self._sub_lock:
            gen = getattr(self._feed, "connection_generation", None)
            if isinstance(gen, int) and gen != self._feed_generation_seen:
                self._feed_generation_seen = int(gen)
                self._subscribed_option_ids.clear()

            to_add = wanted - self._subscribed_option_ids
            to_remove = (self._subscribed_option_ids - wanted) if prune else set()

            if to_add:
                try:
                    await self._feed.subscribe_options({str(s) for s in to_add})
                    self._subscribed_option_ids.update(to_add)
                except Exception as e:
                    self._last_error = f"subscribe failed: {e}"

            if to_remove:
                try:
                    await self._feed.unsubscribe_options({str(s) for s in to_remove})
                except Exception:
                    pass
                finally:
                    for secid in to_remove:
                        self._subscribed_option_ids.discard(secid)

    async def _candle_loop(self) -> None:
        # Backfill from 09:15 to now on connect, then poll shortly after each minute closes.
        while self._running:
            try:
                await self._fetch_candles()
                self._maybe_apply_entry_candle_stop()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._last_error = f"candles error: {e}"
                log.exception("candles error: %s", e)
            now = self._now_ist()
            next_run = self._floor_minute(now) + timedelta(minutes=1, seconds=2)
            sleep_s = max(0.5, float((next_run - now).total_seconds()))
            await asyncio.sleep(sleep_s)

    def _market_open_start(self, now: datetime) -> datetime:
        d = now.astimezone(IST).date()
        return datetime.combine(d, time(9, 15), tzinfo=IST)

    async def _fetch_candles(self) -> None:
        if self._rest is None:
            return
        cfg = self._cfg_store.current()
        now = self._now_ist()
        start = self._market_open_start(now)
        # Use last known fetch window to reduce payload after initial backfill.
        if self._last_candle_fetch_ts is None:
            from_dt = start
        else:
            from_dt = max(start, self._last_candle_fetch_ts - timedelta(minutes=5))

        to_dt = now
        from_s = from_dt.strftime("%Y-%m-%d %H:%M:%S")
        to_s = to_dt.strftime("%Y-%m-%d %H:%M:%S")

        resp = await asyncio.to_thread(
            self._rest.client.intraday_minute_data,
            security_id=str(self._spot_security_id or cfg.nifty_spot_security_id or "13"),
            exchange_segment="IDX_I",
            instrument_type="INDEX",
            from_date=from_s,
            to_date=to_s,
        )
        candles = self._parse_intraday_candles(resp)
        if not candles:
            return
        for c in candles:
            self._candles[c.ts] = c
        self._last_candle_fetch_ts = now

        # Bootstrap spot if websocket ticks haven't arrived yet (common right after connect
        # and during sparse sessions). This enables the option chain to build and subscribe.
        if self._spot_ltp is None:
            self._spot_ltp = float(candles[-1].close)
            self._last_spot_tick_mono = monotime.monotonic()
            self._spot_updated_at = now
            self._spot_source = "CANDLE"

        # Candle-based trailing SL (previous completed candle).
        if self._ladder is not None:
            self._trail_stop_from_latest_candle()

        # Do not overwrite fresh tick/REST spot with candle close (candle close can lag).
        # Only apply candle-close spot if no real-time update recently.
        if self._ladder is not None:
            age = monotime.monotonic() - float(self._last_spot_tick_mono or 0.0)
            if self._spot_ltp is None or age >= 5.0:
                self._apply_spot_snapshot(float(candles[-1].close), source="CANDLE")

    def _apply_spot_snapshot(self, spot: float, *, source: str) -> None:
        lad = self._ladder
        prev_spot = lad.last_spot if lad is not None else None
        self._spot_ltp = float(spot)
        self._spot_updated_at = self._now_ist()
        self._spot_source = str(source)
        if lad is None:
            return
        lad.last_spot = float(spot)
        self._apply_config_updates_if_needed()
        self._check_dynamic_sl(lad, prev_spot=prev_spot, spot=float(spot))
        self._check_spot_exits(lad, spot=float(spot))

    def _latest_candle_at_or_before(self, ts: datetime) -> Optional[_Candle]:
        if not self._candles:
            return None
        if ts.tzinfo is None:
            raise ValueError("ts must be timezone-aware")
        exact = self._candles.get(ts)
        if exact is not None:
            return exact
        k = max((k for k in self._candles.keys() if k <= ts), default=None)
        return None if k is None else self._candles.get(k)

    def _latest_completed_candle(self) -> Optional[_Candle]:
        # Latest completed candle means previous minute relative to now.
        if not self._candles:
            return None
        now = self._now_ist()
        prev_ts = self._floor_minute(now) - timedelta(minutes=1)
        return self._latest_candle_at_or_before(prev_ts)

    def _trail_stop_from_latest_candle(self) -> None:
        lad = self._ladder
        if lad is None or self._close_inflight:
            return
        if not self._candles:
            return

        now = self._now_ist()
        prev_ts = self._floor_minute(now) - timedelta(minutes=1)
        candle = self._latest_candle_at_or_before(prev_ts)
        if candle is None:
            return

        cfg = self._cfg_store.current()
        buffer = float(getattr(cfg, "sl_buffer_points", 0.0) or 0.0)
        candle_size = float(candle.high - candle.low)

        mode: Literal["CANDLE"] = "CANDLE"
        rule = "CANDLE_REF"
        if lad.bullish():
            stop_ref = float(candle.low)
            stop_candidate = float(candle.low - buffer)
        else:
            stop_ref = float(candle.high)
            stop_candidate = float(candle.high + buffer)

        prev_stop = float(lad.stop_spot)

        # Trailing: only tighten.
        if lad.bullish():
            if stop_candidate <= prev_stop:
                return
        else:
            if stop_candidate >= prev_stop:
                return

        lad.stop_mode = mode
        lad.stop_rule = rule
        lad.fixed_stop_ref_spot = float(stop_ref)
        lad.stop_spot = float(stop_candidate)
        lad.stop_candle_minute = candle.ts if mode == "CANDLE" else None

        self._log_event(
            "SL_TRAIL",
            candle_ts=candle.ts.isoformat(),
            candle_size=float(candle_size),
            rule=str(rule),
            stop_mode=str(mode),
            stop_ref_spot=float(stop_ref),
            prev_stop_spot=float(prev_stop),
            new_stop_spot=float(stop_candidate),
            buffer=float(buffer),
        )

    @staticmethod
    def _extract_ltp_from_ticker_resp(resp: object, *, security_id: str) -> Optional[float]:
        if not isinstance(resp, dict):
            return None
        status = resp.get("status")
        if str(status).lower() != "success":
            return None
        data = resp.get("data")
        if isinstance(data, dict) and "data" in data:
            data = data.get("data")

        secid_s = str(security_id).strip()
        if not secid_s:
            return None

        def parse_ltp(obj: object) -> Optional[float]:
            if isinstance(obj, dict):
                if secid_s in obj:
                    return parse_ltp(obj.get(secid_s))

                sid = (
                    obj.get("security_id")
                    or obj.get("securityId")
                    or obj.get("SecurityId")
                    or obj.get("SecurityID")
                )
                if sid is not None and str(sid) == secid_s:
                    for k in (
                        "LTP",
                        "ltp",
                        "last_price",
                        "lastPrice",
                        "last_traded_price",
                        "lastTradedPrice",
                    ):
                        if k in obj:
                            with contextlib.suppress(Exception):
                                return float(obj.get(k))  # type: ignore[arg-type]
                            return None

                for v in obj.values():
                    got = parse_ltp(v)
                    if got is not None:
                        return got
                return None

            if isinstance(obj, list):
                for it in obj:
                    got = parse_ltp(it)
                    if got is not None:
                        return got
            return None

        return parse_ltp(data)

    async def _refresh_spot_from_rest(self) -> None:
        if self._rest is None or not self._running:
            return
        if self._spot_rest_inflight:
            return
        now_m = monotime.monotonic()
        if now_m < float(self._spot_rest_backoff_until_mono or 0.0):
            return
        if now_m - float(self._last_spot_rest_mono) < float(self._spot_rest_interval_s):
            return

        self._spot_rest_inflight = True
        self._last_spot_rest_mono = now_m
        try:
            secid = str(self._spot_security_id or "13").strip() or "13"
            try:
                secid_payload: object = [int(secid)]
            except Exception:
                secid_payload = [secid]

            resp = await asyncio.to_thread(self._rest.client.ticker_data, {"IDX_I": secid_payload})
            ltp = self._extract_ltp_from_ticker_resp(resp, security_id=secid)
            if ltp is not None and float(ltp) > 0.0:
                self._apply_spot_snapshot(float(ltp), source="REST")
                self._last_spot_tick_mono = monotime.monotonic()
            else:
                # Avoid hammering REST if response is valid but doesn't contain LTP.
                self._spot_rest_backoff_until_mono = monotime.monotonic() + 1.0
        except Exception as e:
            self._feed_error = f"spot REST fallback error: {e}"
            # Temporary backoff (rate limits / transient outages).
            self._spot_rest_backoff_until_mono = monotime.monotonic() + 2.0
        finally:
            self._spot_rest_inflight = False

    async def _spot_fallback_loop(self) -> None:
        # Continuous spot update when websocket ticks are sparse.
        while self._running:
            try:
                if self._rest is not None:
                    # If websocket isn't updating frequently, poll REST every ~1s.
                    now_m = monotime.monotonic()
                    ws_age = now_m - float(self._last_spot_ws_mono or 0.0) if self._last_spot_ws_mono else 9999.0
                    if self._spot_ltp is None or ws_age >= 0.4:
                        await self._refresh_spot_from_rest()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._feed_error = f"spot fallback loop error: {e}"
            await asyncio.sleep(0.2)

    @staticmethod
    def _parse_intraday_candles(resp: object) -> list[_Candle]:
        if not isinstance(resp, dict):
            return []
        if resp.get("status") != "success":
            return []
        data = resp.get("data")
        # API returns python_response; sometimes nested.
        if isinstance(data, dict) and "data" in data:
            data = data.get("data")

        candles: list[_Candle] = []

        def parse_ts(v: object) -> Optional[datetime]:
            if v is None:
                return None
            if isinstance(v, (int, float)):
                x = float(v)
                # guess ms
                if x > 1e12:
                    x = x / 1000.0
                return datetime.fromtimestamp(x, tz=IST).replace(second=0, microsecond=0)
            if isinstance(v, str):
                s = v.strip()
                if not s:
                    return None
                # try isoformat
                try:
                    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=IST)
                    return dt.astimezone(IST).replace(second=0, microsecond=0)
                except Exception:
                    pass
                # try "YYYY-MM-DD HH:MM:SS"
                try:
                    dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
                    return dt.replace(second=0, microsecond=0)
                except Exception:
                    return None
            return None

        # Variant 1: list of candle dicts
        if isinstance(data, list):
            for it in data:
                if not isinstance(it, dict):
                    continue
                ts = parse_ts(it.get("startTime") or it.get("start_time") or it.get("timestamp") or it.get("time"))
                if ts is None:
                    continue
                try:
                    o = float(it.get("open") or it.get("Open") or it.get("o"))
                    h = float(it.get("high") or it.get("High") or it.get("h"))
                    l = float(it.get("low") or it.get("Low") or it.get("l"))
                    c = float(it.get("close") or it.get("Close") or it.get("c"))
                except Exception:
                    continue
                candles.append(_Candle(ts=ts, open=o, high=h, low=l, close=c))
            return candles

        # Variant 2: dict of arrays
        if isinstance(data, dict):
            ts_arr = data.get("timestamp") or data.get("time") or data.get("t")
            o_arr = data.get("open") or data.get("o")
            h_arr = data.get("high") or data.get("h")
            l_arr = data.get("low") or data.get("l")
            c_arr = data.get("close") or data.get("c")
            if isinstance(ts_arr, list) and isinstance(o_arr, list) and isinstance(h_arr, list) and isinstance(l_arr, list) and isinstance(c_arr, list):
                n = min(len(ts_arr), len(o_arr), len(h_arr), len(l_arr), len(c_arr))
                for i in range(n):
                    ts = parse_ts(ts_arr[i])
                    if ts is None:
                        continue
                    try:
                        candles.append(_Candle(ts=ts, open=float(o_arr[i]), high=float(h_arr[i]), low=float(l_arr[i]), close=float(c_arr[i])))
                    except Exception:
                        continue
        return candles

    def _maybe_apply_entry_candle_stop(self) -> None:
        lad = self._ladder
        if lad is None:
            return
        if lad.candle_stop_applied or lad.had_add_before_entry_candle:
            return
        cfg = self._cfg_store.current()
        buffer = float(getattr(cfg, "sl_buffer_points", 0.0) or 0.0)

        # Strategy: SL based on the *previous* completed 1-min candle on NIFTY spot.
        prev_ts = lad.entry_minute - timedelta(minutes=1)
        candle = self._latest_candle_at_or_before(prev_ts) or self._latest_candle_at_or_before(lad.entry_minute)
        if candle is None:
            return

        candle_size = float(candle.high - candle.low)
        rule = "CANDLE_REF"
        lad.stop_mode = "CANDLE"
        lad.stop_candle_minute = candle.ts
        if lad.bullish():
            lad.fixed_stop_ref_spot = float(candle.low)
            lad.stop_spot = float(candle.low - buffer)
        else:
            lad.fixed_stop_ref_spot = float(candle.high)
            lad.stop_spot = float(candle.high + buffer)

        lad.stop_rule = str(rule)
        lad.candle_stop_applied = True
        self._log_event(
            "SL_RULE",
            entry_minute=lad.entry_minute.isoformat(),
            candle_ts=candle.ts.isoformat(),
            candle_size=float(candle_size),
            rule=rule,
            bullish=bool(lad.bullish()),
            stop_mode=str(lad.stop_mode),
            stop_ref_spot=float(lad.fixed_stop_ref_spot),
            stop_spot=float(lad.stop_spot),
            candle_low=float(candle.low),
            candle_high=float(candle.high),
            buffer=float(buffer),
        )

    async def _mtm_loop(self) -> None:
        while self._running:
            try:
                await self._poll_mtm()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._last_error = f"mtm error: {e}"
                log.exception("mtm error: %s", e)
            await asyncio.sleep(10.0)

    async def _poll_mtm(self) -> None:
        if self._rest is None:
            return
        cfg = self._cfg_store.current()
        if str(cfg.execution_mode).upper() != "LIVE":
            return
        resp = await asyncio.to_thread(self._rest.client.get_positions)
        if not isinstance(resp, dict) or resp.get("status") != "success":
            return
        data = resp.get("data")
        if isinstance(data, dict) and "data" in data:
            data = data.get("data")
        positions: list[dict] = []
        if isinstance(data, list):
            positions = [p for p in data if isinstance(p, dict)]
        elif isinstance(data, dict) and "positions" in data and isinstance(data.get("positions"), list):
            positions = [p for p in data.get("positions") if isinstance(p, dict)]
        else:
            positions = []

        mtm_total: float = 0.0
        for p in positions:
            v = (
                p.get("mtm")
                or p.get("Mtm")
                or p.get("unrealizedPnl")
                or p.get("unrealisedPnl")
                or p.get("unrealizedProfit")
                or p.get("unrealisedProfit")
            )
            try:
                if v is not None:
                    mtm_total += float(v)
            except Exception:
                pass

        self._broker_positions = positions
        self._broker_mtm = float(mtm_total)
        self._last_mtm_ts = self._now_ist()

    async def _place_order(self, *, txn: Txn, security_id: str, qty: int, tag: str) -> PlacedOrder:
        if self._rest is None:
            raise RuntimeError("REST client not ready.")
        if qty <= 0:
            return PlacedOrder(ok=True, raw={"status": "success", "data": {"remarks": "qty<=0 skipped"}})

        cfg = self._cfg_store.current()
        order_type = cfg.order_type
        price = 0.0
        if order_type == "LIMIT":
            ltp = self._ltps.get(str(security_id))
            if ltp is None:
                raise RuntimeError(f"LIMIT order requested but option LTP not available for security_id={security_id}.")
            if txn == "BUY":
                price = float(ltp + cfg.limit_price_offset)
            else:
                price = float(max(0.05, ltp - cfg.limit_price_offset))

        placed = await asyncio.to_thread(
            self._rest.place_intraday_option_order,
            security_id=str(security_id),
            transaction_type=txn,
            quantity=int(qty),
            order_type=order_type,
            price=float(price),
            tag=tag,
        )
        if not placed.ok:
            self._last_error = f"order failed tag={tag} secid={security_id} qty={qty} resp={placed.raw}"
            raise RuntimeError(self._last_error)
        return placed

    @staticmethod
    def _extract_order_id(resp: object) -> Optional[str]:
        if not isinstance(resp, dict):
            return None
        data = resp.get("data")
        for obj in (resp, data):
            if not isinstance(obj, dict):
                continue
            v = obj.get("orderId") or obj.get("order_id") or obj.get("id")
            if v is None:
                continue
            s = str(v).strip()
            if s:
                return s
        return None

    @staticmethod
    def _extract_avg_price(resp: object) -> Optional[float]:
        if not isinstance(resp, dict):
            return None
        if resp.get("status") != "success":
            return None

        data: object = resp.get("data")
        if isinstance(data, dict) and "data" in data:
            data = data.get("data")
        if isinstance(data, list) and data:
            data = data[0]
        if not isinstance(data, dict):
            return None

        for k in (
            "averageTradedPrice",
            "avgTradedPrice",
            "averageTradePrice",
            "avgTradePrice",
            "avgPrice",
            "averagePrice",
            "average_price",
        ):
            v = data.get(k)
            if v is None:
                continue
            try:
                f = float(v)
            except Exception:
                continue
            if f > 0:
                return f
        return None

    async def _best_effort_entry_price(self, placed: PlacedOrder, *, correlation_id: str, fallback: float) -> float:
        resp = placed.raw
        avg = self._extract_avg_price(resp)
        if avg is not None:
            return float(avg)

        if self._rest is None:
            return float(fallback)

        order_id = self._extract_order_id(resp)
        for _ in range(10):
            await asyncio.sleep(0.25)
            try:
                if order_id:
                    oresp = await asyncio.to_thread(self._rest.client.get_order_by_id, order_id)
                else:
                    oresp = await asyncio.to_thread(self._rest.client.get_order_by_correlationID, correlation_id)
            except Exception:
                oresp = None
            avg = self._extract_avg_price(oresp)
            if avg is not None:
                return float(avg)
        return float(fallback)
