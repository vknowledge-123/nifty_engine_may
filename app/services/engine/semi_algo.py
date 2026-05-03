from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional

from zoneinfo import ZoneInfo

from app.runtime.instruments import InstrumentStore, OptionContract
from app.runtime.settings import ActivePosition, EngineConfig, EngineConfigStore, EngineStatus
from app.services.dhan.feed import DhanMarketFeed, FeedTick
from app.services.dhan.rest import DhanRest, PlacedOrder


log = logging.getLogger("niftyalgo.semi_algo")
IST = ZoneInfo("Asia/Kolkata")

Txn = Literal["BUY", "SELL"]
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


@dataclass(slots=True)
class _Position:
    side: Txn  # transaction used to open the position (BUY=long premium, SELL=short premium)
    contract: OptionContract
    qty: int
    entry_price: float
    entry_ts: datetime
    risk_points: float
    stop_loss_price: float
    target_price: float
    trailing_stop_price: Optional[float] = None
    last_ltp: Optional[float] = None
    exit_reason: Optional[str] = None
    cost_sl_applied: bool = False


class SemiAlgoController:
    """
    Semi-algo controller:
    - Shows a spot-centered option chain (±N strikes) for current/next weekly expiry.
    - User clicks BUY/SELL on a CE/PE contract.
    - The controller monitors option LTP for SL/Target (and optional Cost SL) and squares off on exit.
    """

    def __init__(self, config_store: EngineConfigStore, instruments: InstrumentStore) -> None:
        self._cfg_store = config_store
        self._instruments = instruments

        self._lock = asyncio.Lock()
        self._trade_lock = asyncio.Lock()

        self._running: bool = False
        self._market_task: Optional[asyncio.Task] = None
        self._chain_task: Optional[asyncio.Task] = None

        self._feed: Optional[DhanMarketFeed] = None
        self._rest: Optional[DhanRest] = None

        self._spot_security_id: Optional[str] = None
        self._spot_ltp: Optional[float] = None
        self._ltps: dict[str, float] = {}

        self._subscribed_option_ids: set[str] = set()
        self._chain_rows: list[_ChainRow] = []
        self._chain_atm: Optional[int] = None
        self._chain_sig: Optional[tuple[int, str, int, int]] = None
        self._last_error: Optional[str] = None
        self._feed_error: Optional[str] = None

        self._active: Optional[_Position] = None
        self._close_inflight: bool = False

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
        # Nearest strike step.
        return int(math.floor((float(spot) + (step / 2.0)) / step) * step)

    async def start(self) -> None:
        async with self._lock:
            if self._running:
                return

            cfg = await self._cfg_store.get()
            if not cfg.client_id or not cfg.access_token:
                msg = "Set client_id and access_token in config before starting."
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
            self._ltps.clear()
            self._subscribed_option_ids.clear()
            self._chain_rows = []
            self._chain_atm = None
            self._chain_sig = None
            self._feed_error = None

            self._feed = DhanMarketFeed(cfg.client_id, cfg.access_token, spot_security_id=spot_security_id)
            self._rest = DhanRest(cfg.client_id, cfg.access_token)

            self._running = True
            self._market_task = asyncio.create_task(self._market_loop(), name="semi_algo_market")
            self._chain_task = asyncio.create_task(self._chain_loop(), name="semi_algo_chain")

    async def stop(self, *, force: bool = False) -> None:
        async with self._lock:
            if not self._running:
                return
            if self._active is not None and not force:
                raise RuntimeError("Active position exists. Square off before disconnecting.")
            self._running = False

            tasks = [t for t in (self._market_task, self._chain_task) if t is not None]
            for t in tasks:
                t.cancel()
            for t in tasks:
                try:
                    await t
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass

            self._market_task = None
            self._chain_task = None

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

    async def status(self) -> EngineStatus:
        cfg = self._cfg_store.current()
        active = self._active_position_model()
        return EngineStatus(
            running=bool(self._running),
            trading_enabled=bool(cfg.trading_enabled),
            spot_ltp=self._spot_ltp,
            weekly_expiry=str(cfg.weekly_expiry),
            instruments_loaded=bool(self._instruments.loaded),
            active_position=active,
            last_error=self._last_error,
            feed_error=self._feed_error,
        )

    def _active_position_model(self) -> Optional[ActivePosition]:
        pos = self._active
        if pos is None:
            return None

        ltp = pos.last_ltp
        pnl: Optional[float] = None
        if ltp is not None:
            if pos.side == "BUY":
                pnl = (ltp - pos.entry_price) * pos.qty
            else:
                pnl = (pos.entry_price - ltp) * pos.qty

        return ActivePosition(
            side=pos.side,
            symbol=pos.contract.trading_symbol,
            security_id=str(pos.contract.security_id),
            option_type=str(pos.contract.option_type),
            strike=int(pos.contract.strike),
            expiry=pos.contract.expiry.date().isoformat(),
            qty=int(pos.qty),
            entry_price=float(pos.entry_price),
            last_ltp=float(ltp) if ltp is not None else None,
            stop_loss_price=float(pos.stop_loss_price),
            trailing_stop_price=float(pos.trailing_stop_price) if pos.trailing_stop_price is not None else None,
            target_price=float(pos.target_price),
            pnl=float(pnl) if pnl is not None else None,
            entry_ts=pos.entry_ts.isoformat(),
            exit_reason=pos.exit_reason,
        )

    async def dashboard_snapshot(self) -> dict:
        st = await self.status()

        chain = []
        for row in self._chain_rows:
            ce = row.ce
            pe = row.pe
            chain.append(
                {
                    "strike": row.strike,
                    "ce": None
                    if ce is None
                    else {
                        "symbol": ce.contract.trading_symbol,
                        "security_id": ce.contract.security_id,
                        "ltp": ce.ltp,
                    },
                    "pe": None
                    if pe is None
                    else {
                        "symbol": pe.contract.trading_symbol,
                        "security_id": pe.contract.security_id,
                        "ltp": pe.ltp,
                    },
                }
            )

        return {
            "status": st.model_dump(),
            "atm_strike": self._chain_atm,
            "chain": chain,
        }

    async def open_position(self, *, strike: int, option_type: Literal["CE", "PE"], side: Txn) -> EngineStatus:
        async with self._trade_lock:
            if not self._running or self._feed is None or self._rest is None:
                raise RuntimeError("Not connected. Start first.")

            cfg = self._cfg_store.current()
            if not cfg.trading_enabled:
                raise RuntimeError("trading_enabled=false. Enable trading in settings before placing orders.")

            if self._active is not None:
                raise RuntimeError("An active position already exists. Square off first.")

            now = self._now_ist()
            expiry_offset = self._expiry_offset(cfg.weekly_expiry)
            contract = await self._instruments.get_weekly_option(
                now_ist=now,
                strike=int(strike),
                option_type=option_type,
                expiry_offset=expiry_offset,
            )

            secid = str(contract.security_id)
            await self._ensure_subscriptions({secid})
            ltp = self._ltps.get(secid)
            if ltp is None:
                raise RuntimeError("Option LTP not available yet. Wait for chain/quotes to populate and try again.")

            lot_size = max(1, int(contract.lot_size or 1))
            lots = max(1, int(getattr(cfg, "lots", 1) or 1))
            qty = lot_size * lots
            tag = f"open_{side.lower()}_{secid}"
            placed = await self._place_order(txn=side, security_id=secid, qty=qty, tag=tag)

            entry_price = await self._best_effort_entry_price(placed, correlation_id=tag, fallback=float(ltp))
            sl_points = float(cfg.stop_loss_points)
            tgt_points = float(cfg.target_points)

            if side == "BUY":
                stop_loss_price = max(0.05, entry_price - sl_points)
                target_price = max(0.05, entry_price + tgt_points)
                risk_points = max(0.0, entry_price - stop_loss_price)
            else:
                stop_loss_price = max(0.05, entry_price + sl_points)
                target_price = max(0.05, entry_price - tgt_points)
                risk_points = max(0.0, stop_loss_price - entry_price)

            self._active = _Position(
                side=side,
                contract=contract,
                qty=qty,
                entry_price=entry_price,
                entry_ts=now,
                risk_points=risk_points,
                stop_loss_price=stop_loss_price,
                target_price=target_price,
                trailing_stop_price=None,
                last_ltp=entry_price,
            )
            return await self.status()

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
            "tradedPrice",
            "tradePrice",
            "executedPrice",
            "executionPrice",
            "filledPrice",
            "price",
        ):
            v = data.get(k)
            try:
                x = float(v)  # type: ignore[arg-type]
            except Exception:
                continue
            if math.isfinite(x) and x > 0.0:
                return x
        return None

    async def _best_effort_entry_price(self, placed: PlacedOrder, *, correlation_id: str, fallback: float) -> float:
        if self._rest is None:
            return fallback

        order_id = self._extract_order_id(placed.raw)
        for _ in range(8):
            try:
                if order_id:
                    resp = await asyncio.to_thread(self._rest.client.get_order_by_id, order_id)
                else:
                    resp = await asyncio.to_thread(self._rest.client.get_order_by_correlationID, correlation_id)
            except Exception:
                resp = None
            price = self._extract_avg_price(resp)
            if price is not None:
                return price
            await asyncio.sleep(0.25)
        return fallback

    async def square_off(self, *, reason: str = "MANUAL") -> EngineStatus:
        await self._close_position(reason=reason)
        return await self.status()

    async def _close_position(self, *, reason: str) -> None:
        async with self._trade_lock:
            if self._active is None:
                return
            if not self._running or self._rest is None:
                # Can't place an order; keep the position visible to the UI.
                self._active.exit_reason = f"{reason}_FAILED_NOT_CONNECTED"
                return

            pos = self._active
            secid = str(pos.contract.security_id)
            qty = int(pos.qty)
            close_txn: Txn = "SELL" if pos.side == "BUY" else "BUY"
            try:
                await self._place_order(txn=close_txn, security_id=secid, qty=qty, tag=f"close_{reason.lower()}_{secid}")
                pos.exit_reason = str(reason)
                self._active = None
            finally:
                self._close_inflight = False

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
                if tick is None:
                    continue
                self._apply_tick(tick, spot_id=spot_id)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._feed_error = str(e)
                self._last_error = f"marketfeed error: {e}"
                log.exception("marketfeed error: %s", e)
                await asyncio.sleep(0.5)

    def _apply_tick(self, tick: FeedTick, *, spot_id: str) -> None:
        secid = str(tick.security_id)
        ltp = float(tick.ltp)
        self._feed_error = getattr(self._feed, "last_error", None) if self._feed is not None else None

        if secid == spot_id:
            self._spot_ltp = ltp
            return

        self._ltps[secid] = ltp

        pos = self._active
        if pos is None:
            return

        if secid != str(pos.contract.security_id):
            return

        pos.last_ltp = ltp
        self._update_trailing_and_maybe_exit(pos, ltp)

    def _update_trailing_and_maybe_exit(self, pos: _Position, ltp: float) -> None:
        cfg: EngineConfig = self._cfg_store.current()
        cost_sl_enabled = bool(getattr(cfg, "cost_sl_enabled", False))
        cost_sl_rr = float(getattr(cfg, "cost_sl_rr", 1.0) or 0.0)
        # Trailing SL feature removed; keep field for compatibility but ensure it is always unset.
        pos.trailing_stop_price = None

        if cost_sl_enabled and not pos.cost_sl_applied:
            risk = float(getattr(pos, "risk_points", 0.0) or 0.0)
            if risk > 0.0 and cost_sl_rr >= 0.0:
                if pos.side == "BUY":
                    reward = float(ltp) - float(pos.entry_price)
                else:
                    reward = float(pos.entry_price) - float(ltp)
                if reward > 0.0 and (reward / risk) >= cost_sl_rr:
                    pos.stop_loss_price = max(0.05, float(pos.entry_price))
                    pos.trailing_stop_price = None
                    pos.cost_sl_applied = True

        if pos.side == "BUY":
            stop = float(pos.stop_loss_price)
            if ltp <= stop and not self._close_inflight:
                self._close_inflight = True
                asyncio.create_task(self._close_position(reason="STOP"), name="semi_algo_auto_close_stop")
                return
            if ltp >= float(pos.target_price) and not self._close_inflight:
                self._close_inflight = True
                asyncio.create_task(self._close_position(reason="TARGET"), name="semi_algo_auto_close_target")
                return
        else:
            stop = float(pos.stop_loss_price)
            if ltp >= stop and not self._close_inflight:
                self._close_inflight = True
                asyncio.create_task(self._close_position(reason="STOP"), name="semi_algo_auto_close_stop")
                return
            if ltp <= float(pos.target_price) and not self._close_inflight:
                self._close_inflight = True
                asyncio.create_task(self._close_position(reason="TARGET"), name="semi_algo_auto_close_target")
                return

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
            # Only refresh LTPs.
            self._refresh_chain_ltps()
            return

        now = self._now_ist()
        expiry_offset = self._expiry_offset(cfg.weekly_expiry)
        rows: list[_ChainRow] = []
        wanted: set[str] = set()

        for i in range(-n, n + 1):
            strike = int(atm + (i * step))
            ce = await self._instruments.get_weekly_option(
                now_ist=now, strike=strike, option_type="CE", expiry_offset=expiry_offset
            )
            pe = await self._instruments.get_weekly_option(
                now_ist=now, strike=strike, option_type="PE", expiry_offset=expiry_offset
            )
            ce_leg = _ChainLeg(contract=ce, ltp=self._ltps.get(str(ce.security_id)))
            pe_leg = _ChainLeg(contract=pe, ltp=self._ltps.get(str(pe.security_id)))
            rows.append(_ChainRow(strike=strike, ce=ce_leg, pe=pe_leg))
            wanted.add(str(ce.security_id))
            wanted.add(str(pe.security_id))

        # Ensure active contract stays subscribed even if spot moves and shifts chain.
        if self._active is not None:
            wanted.add(str(self._active.contract.security_id))

        await self._ensure_subscriptions(wanted)
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

    async def _ensure_subscriptions(self, wanted: set[str]) -> None:
        if not self._running or self._feed is None:
            return

        wanted = {str(x) for x in wanted if x}
        to_add = wanted - self._subscribed_option_ids
        to_remove = self._subscribed_option_ids - wanted

        for secid in to_add:
            try:
                await self._feed.subscribe_option(secid)
                self._subscribed_option_ids.add(secid)
            except Exception as e:
                self._last_error = f"subscribe failed secid={secid}: {e}"

        for secid in to_remove:
            try:
                await self._feed.unsubscribe_option(secid)
                self._subscribed_option_ids.discard(secid)
            except Exception:
                # Best-effort.
                self._subscribed_option_ids.discard(secid)
