from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import logging
import random
import struct
import time
from typing import Optional

import websockets
from dhanhq import dhanhq as DhanHQ


def _patch_websockets_closed_attr() -> None:
    """
    dhanhq<=2.0.2 expects `ws.closed` (legacy websockets API), but websockets>=12
    returns `ClientConnection` without a `closed` attribute.

    Add a compatible `closed` property based on the connection state.
    """

    try:
        from websockets.asyncio.client import ClientConnection  # type: ignore[import-not-found]
        from websockets.protocol import State  # type: ignore[import-not-found]
    except Exception:
        return

    if hasattr(ClientConnection, "closed"):
        return

    ClientConnection.closed = property(lambda self: self.state is State.CLOSED)  # type: ignore[attr-defined]


_patch_websockets_closed_attr()

from dhanhq.marketfeed import DhanFeed, IDX

log = logging.getLogger("niftyalgo.dhan.feed")


@dataclass(slots=True)
class FeedTick:
    exchange_segment: int
    security_id: str
    ltp: float


class DhanMarketFeed:
    def __init__(self, client_id: str, access_token: str, spot_security_id: str) -> None:
        self._client_id = client_id
        self._access_token = access_token
        self._spot_security_id = str(spot_security_id)

        # Default subscription: NIFTY spot
        # Important: dhanhq.marketfeed requires *all* instrument tuples to be the same size
        # (either all 2-tuples or all 3-tuples). We use 3-tuples so we can request a richer
        # packet type for options without breaking reconnect resubscription.
        self._feed = DhanFeed(
            client_id=self._client_id,
            access_token=self._access_token,
            # Subscribe to both Ticker (15) and Quote (17) for the spot to reduce
            # "no ticks" situations (some sessions/markets can be sparse on Ticker-only).
            # Also subscribe to FULL (21) to maximize spot tick frequency (used for low-latency exits).
            instruments=[(IDX, self._spot_security_id, 15), (IDX, self._spot_security_id, 17), (IDX, self._spot_security_id, 21)],
            # Dhan v1 feed often rejects handshake (HTTP 400) on newer infra.
            # v2 uses token+clientId in the URL query and is more reliable.
            version="v2",
        )
        self._lock = asyncio.Lock()
        self._reconnect_attempt: int = 0
        self._last_disconnect_log_ts: float = 0.0
        self._last_disconnect_http_status: Optional[int] = None
        self._last_disconnect_retry_after_s: Optional[float] = None
        self.last_error: Optional[str] = None

        self._closing: bool = False
        self._reconnect_task: Optional[asyncio.Task] = None

        # If you want to tune these (e.g. behind flaky networks), make them configurable.
        self._ping_interval_s: float = 20.0
        self._ping_timeout_s: float = 20.0

        # Market data must come from websocket only (no REST polling fallback).
        self._use_rest_fallback: bool = False
        self._conn_generation: int = 0

        # For "connected but no data" debugging and idle reconnects.
        self._last_rx_ts: float = 0.0
        self._last_tick_ts: float = 0.0
        self._idle_reconnect_s: float = 25.0

    @property
    def connection_generation(self) -> int:
        return int(self._conn_generation)

    async def connect(self) -> None:
        async with self._lock:
            if self._closing:
                return
            ws = getattr(self._feed, "ws", None)
            if ws is not None and not getattr(ws, "closed", False):
                # Already connected.
                self._reconnect_attempt = 0
                self._last_disconnect_http_status = None
                self._last_disconnect_retry_after_s = None
                self.last_error = None
                return

            # Implement our own connect so we can control timeouts / keepalive.
            if self._feed.version == "v1":
                url = "wss://api-feed.dhan.co"
            else:
                url = (
                    f"wss://api-feed.dhan.co"
                    f"?version=2&token={self._access_token}&clientId={self._client_id}&authType=2"
                )

            ws = await websockets.connect(
                url,
                ping_interval=self._ping_interval_s,
                ping_timeout=self._ping_timeout_s,
                open_timeout=15,
                close_timeout=5,
                compression=None,
            )
            self._feed.ws = ws  # type: ignore[attr-defined]
            if self._feed.version == "v1":
                await self._feed.authorize()
            await self._feed.subscribe_instruments()
            self._conn_generation += 1
            self._last_rx_ts = time.monotonic()
            self._last_tick_ts = 0.0

            # Success: clear error / backoff state.
            self._reconnect_attempt = 0
            self._last_disconnect_http_status = None
            self._last_disconnect_retry_after_s = None
            self.last_error = None

    async def disconnect(self) -> None:
        self._closing = True
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
        async with self._lock:
            try:
                await self._feed.disconnect()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

            # dhanhq doesn't always close the underlying websocket; do best-effort here.
            ws = getattr(self._feed, "ws", None)
            if ws is not None:
                try:
                    await ws.close()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass
            with contextlib.suppress(Exception):
                self._feed.ws = None  # type: ignore[attr-defined]

    async def subscribe_option(self, security_id: str) -> None:
        await self.subscribe_options({str(security_id)})

    async def unsubscribe_option(self, security_id: str) -> None:
        await self.unsubscribe_options({str(security_id)})

    async def subscribe_options(self, security_ids: set[str]) -> None:
        # Derivatives can be sparse in ticker-only mode for some accounts.
        # Subscribe using QUOTE (17) and FULL (21) to maximize chance of receiving LTP updates.
        from dhanhq.marketfeed import NSE_FNO

        ids = [str(s).strip() for s in security_ids if str(s).strip()]
        symbols = (
            [(NSE_FNO, secid, 15) for secid in ids]
            + [(NSE_FNO, secid, 17) for secid in ids]
            + [(NSE_FNO, secid, 21) for secid in ids]
        )
        if not symbols:
            return
        async with self._lock:
            self._feed.subscribe_symbols(symbols)
            # Persist subscriptions so reconnect `subscribe_instruments()` restores them.
            inst = list(getattr(self._feed, "instruments", None) or [])
            existing = set(inst)
            for sym in symbols:
                if sym not in existing:
                    inst.append(sym)
            with contextlib.suppress(Exception):
                self._feed.instruments = inst  # type: ignore[attr-defined]

    async def unsubscribe_options(self, security_ids: set[str]) -> None:
        from dhanhq.marketfeed import NSE_FNO

        ids = [str(s).strip() for s in security_ids if str(s).strip()]
        symbols = (
            [(NSE_FNO, secid, 15) for secid in ids]
            + [(NSE_FNO, secid, 17) for secid in ids]
            + [(NSE_FNO, secid, 21) for secid in ids]
        )
        if not symbols:
            return
        async with self._lock:
            self._feed.unsubscribe_symbols(symbols)
            inst = list(getattr(self._feed, "instruments", None) or [])
            if inst:
                remove = set(symbols)
                inst = [t for t in inst if t not in remove]
                with contextlib.suppress(Exception):
                    self._feed.instruments = inst  # type: ignore[attr-defined]

    @staticmethod
    def _http_status_from_exc(exc: BaseException) -> Optional[int]:
        resp = getattr(exc, "response", None)
        if resp is not None:
            code = getattr(resp, "status_code", None)
            if isinstance(code, int):
                return code
        code = getattr(exc, "status_code", None)
        if isinstance(code, int):
            return code
        return None

    @staticmethod
    def _retry_after_s_from_exc(exc: BaseException) -> Optional[float]:
        headers = None
        resp = getattr(exc, "response", None)
        if resp is not None:
            headers = getattr(resp, "headers", None)
        if headers is None:
            headers = getattr(exc, "headers", None) or getattr(exc, "response_headers", None)

        retry_after = None
        if headers is not None:
            # websockets.http11.Response.headers may be a list of (name, value) tuples.
            if isinstance(headers, dict):
                retry_after = headers.get("Retry-After") or headers.get("retry-after")
            elif isinstance(headers, (list, tuple)):
                for k, v in headers:
                    if str(k).lower() == "retry-after":
                        retry_after = v
                        break
            else:
                with contextlib.suppress(Exception):
                    retry_after = headers.get("Retry-After")  # type: ignore[attr-defined]
                if retry_after is None:
                    with contextlib.suppress(Exception):
                        retry_after = headers.get("retry-after")  # type: ignore[attr-defined]

        if retry_after is None:
            return None

        val = str(retry_after).strip()
        if not val:
            return None
        try:
            return float(val)
        except Exception:
            pass

        # Retry-After can also be an HTTP-date.
        with contextlib.suppress(Exception):
            dt = parsedate_to_datetime(val)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            delta = (dt - datetime.now(timezone.utc)).total_seconds()
            if delta > 0:
                return float(delta)
        return None

    def _next_reconnect_delay_s(self) -> float:
        # Default exponential backoff with jitter: 0.5, 1, 2, 4, ... up to 30s (+ small jitter)
        if self._last_disconnect_http_status == 429:
            # On rate limit, back off more aggressively (30s, 60s, 120s, ... up to 15m).
            base = min(15 * 60.0, 30.0 * (2 ** max(0, self._reconnect_attempt - 1)))
        else:
            base = min(30.0, 0.5 * (2 ** max(0, self._reconnect_attempt - 1)))

        if self._last_disconnect_retry_after_s is not None:
            base = max(base, float(self._last_disconnect_retry_after_s))

        return base + random.random() * 0.25

    @staticmethod
    def _describe_disconnect(exc: BaseException) -> str:
        code = getattr(exc, "code", None)
        reason = getattr(exc, "reason", None)
        if code is not None:
            if reason:
                return f"{type(exc).__name__}: {exc} (code={code}, reason={reason})"
            return f"{type(exc).__name__}: {exc} (code={code})"
        return f"{type(exc).__name__}: {exc}"

    async def _close_ws(self) -> None:
        async with self._lock:
            ws = getattr(self._feed, "ws", None)
            if ws is not None:
                try:
                    await ws.close()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass
            with contextlib.suppress(Exception):
                self._feed.ws = None  # type: ignore[attr-defined]

    def _note_ws_disconnect(self, exc: BaseException) -> None:
        self._reconnect_attempt = min(self._reconnect_attempt + 1, 16)
        self._last_disconnect_http_status = self._http_status_from_exc(exc)
        self._last_disconnect_retry_after_s = self._retry_after_s_from_exc(exc)
        delay_s = self._next_reconnect_delay_s()
        desc = self._describe_disconnect(exc)
        extra = ""
        if self._last_disconnect_http_status == 429:
            extra = " Rate limited (HTTP 429)."
        elif self._last_disconnect_retry_after_s is not None:
            extra = f" Retry-After={self._last_disconnect_retry_after_s:.0f}s."
        self.last_error = (
            f"Marketfeed disconnected ({desc}).{extra} "
            f"Retrying websocket in {delay_s:.2f}s (attempt {self._reconnect_attempt}). "
            f"Waiting for websocket reconnect."
        )

        now = time.monotonic()
        if now - self._last_disconnect_log_ts >= 5.0:
            self._last_disconnect_log_ts = now
            log.warning("%s", self.last_error)

    async def _reconnect_loop(self) -> None:
        try:
            while not self._closing:
                delay_s = self._next_reconnect_delay_s()
                await asyncio.sleep(delay_s)
                if self._closing:
                    return
                try:
                    await self.connect()
                    # Success: reset and clear error.
                    self._reconnect_attempt = 0
                    self.last_error = None
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self._note_ws_disconnect(e)
                    await self._close_ws()
        finally:
            self._reconnect_task = None

    def _ensure_reconnect(self) -> None:
        if self._closing:
            return
        if self._reconnect_task is not None and not self._reconnect_task.done():
            return
        self._reconnect_task = asyncio.create_task(self._reconnect_loop(), name="dhan_ws_reconnect")

    def notify_ws_error(self, exc: BaseException) -> None:
        self._note_ws_disconnect(exc)
        self._ensure_reconnect()

    @staticmethod
    def _exchange_segment_str(exchange_segment: int) -> Optional[str]:
        return {
            0: "IDX_I",
            1: "NSE_EQ",
            2: "NSE_FNO",
            3: "NSE_CURRENCY",
            4: "BSE_EQ",
            5: "MCX_COMM",
            7: "BSE_CURRENCY",
            8: "BSE_FNO",
        }.get(int(exchange_segment))

    async def _poll_rest_into_buffer(self) -> None:
        # Disabled: websocket-only mode (no REST ticker/quote polling).
        return

    @staticmethod
    def _dhan_disconnect_reason(code: int) -> str:
        # Based on dhanhq.marketfeed.DhanFeed.server_disconnection()
        return {
            805: "No. of active websocket connections exceeded",
            806: "Subscribe to Data APIs to continue",
            807: "Access Token is expired",
            808: "Invalid Client ID",
            809: "Authentication Failed",
        }.get(int(code), f"Unknown disconnect code={code}")

    def _handle_server_disconnect_packet(self, payload: bytes) -> None:
        # Packet schema (dhanhq): struct '<BHBIH' from first 10 bytes.
        # - last field is the server-disconnect reason code.
        if len(payload) < 10:
            self.last_error = "Marketfeed disconnected by server (malformed disconnect packet)."
            return
        try:
            unpack = struct.unpack("<BHBIH", payload[0:10])
            reason_code = int(unpack[4])
        except Exception as e:
            self.last_error = f"Marketfeed disconnected by server (failed to parse disconnect packet: {e})."
            return
        self.last_error = f"Marketfeed disconnected by server: {self._dhan_disconnect_reason(reason_code)}."
        log.warning("%s", self.last_error)

        # Backoff tuning: for auth / entitlement failures, do not hammer reconnect.
        self._reconnect_attempt = min(self._reconnect_attempt + 1, 16)
        if reason_code in (805,):
            self._last_disconnect_retry_after_s = 30.0
        elif reason_code in (806, 807, 808, 809):
            self._last_disconnect_retry_after_s = 60.0
        else:
            self._last_disconnect_retry_after_s = max(float(self._last_disconnect_retry_after_s or 0.0), 5.0)

    async def recv_tick(self) -> Optional[FeedTick]:
        ws = getattr(self._feed, "ws", None)
        if ws is not None and not getattr(ws, "closed", False):
            try:
                raw = await ws.recv()
            except Exception as e:
                try:
                    from websockets.exceptions import ConnectionClosed
                except Exception:
                    ConnectionClosed = ()  # type: ignore[assignment]

                is_ws_attr_error = isinstance(e, AttributeError) and ("recv" in str(e) or "ws" in str(e))
                if isinstance(e, ConnectionClosed) or isinstance(e, OSError) or is_ws_attr_error:
                    self._note_ws_disconnect(e)
                    await self._close_ws()
                    self._ensure_reconnect()
                    raw = None
                else:
                    raise
        else:
            raw = None
            self._ensure_reconnect()

        if raw is None:
            # Websocket-only mode: no REST polling fallback.
            await asyncio.sleep(0.05)
            return None

        self._last_rx_ts = time.monotonic()

        # v2 subscriptions are JSON, but ticks are binary. Handle both safely.
        if isinstance(raw, str):
            # Ignore text frames (rare); keep last_rx_ts updated for idle reconnect logic.
            return None

        if not isinstance(raw, (bytes, bytearray, memoryview)):
            return None

        payload = bytes(raw)
        if not payload:
            return None

        # Detect Dhan server-side disconnect packet (first byte 50).
        # dhanhq currently turns this into `None` (print-only), which makes the app
        # appear "connected but with no data". We surface it and force reconnect.
        if payload[0] == 50:
            self._handle_server_disconnect_packet(payload)
            await self._close_ws()
            self._ensure_reconnect()
            await asyncio.sleep(0.1)
            return None

        try:
            data = self._feed.process_data(payload)
        except Exception as e:
            self.last_error = f"Marketfeed decode error: {e}"
            await asyncio.sleep(0.05)
            return None

        if not isinstance(data, dict):
            return None

        def first_present(*keys: str) -> object | None:
            for key in keys:
                if key in data and data.get(key) is not None:
                    return data.get(key)
            return None

        secid = first_present("security_id", "securityId", "SecurityId", "SecurityID")
        ex = first_present("exchange_segment", "exchangeSegment", "ExchangeSegment", "exchange_segment_id")
        ltp_val = (
            data.get("LTP")
            or data.get("ltp")
            or data.get("last_price")
            or data.get("lastPrice")
            or data.get("last_traded_price")
            or data.get("lastTradedPrice")
        )
        if secid is None or ex is None or ltp_val is None:
            return None
        try:
            ltp = float(ltp_val)
        except (TypeError, ValueError):
            return None

        self._last_tick_ts = time.monotonic()
        return FeedTick(
            exchange_segment=int(ex),
            security_id=str(secid),
            ltp=ltp,
        )
