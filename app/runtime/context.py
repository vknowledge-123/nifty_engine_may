from __future__ import annotations

import asyncio

from app.runtime.instruments import InstrumentStore
from app.runtime.settings import EngineConfigStore
from app.services.engine.ladder import LadderEngineController


class AppContext:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.config_store = EngineConfigStore()
        self.instruments = InstrumentStore()
        self.engine = LadderEngineController(self.config_store, self.instruments)

    async def startup(self) -> None:
        await self.instruments.load_from_disk_if_present()

    async def shutdown(self) -> None:
        try:
            await self.engine.stop(force=True)
        except Exception:
            pass
