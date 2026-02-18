# Nifty Semi Algo Options Trader (FastAPI + Dhan)

FastAPI web app for **manual entry + automatic exit** NIFTY weekly options trading using the **Dhan** Python SDK.

## What’s included
- Web dashboard to set `client_id`, `access_token`, order lots, and risk params (SL/Target/TSL as % of option premium)
- Dhan MarketFeed WebSocket subscription to **NIFTY spot** (used only to center the strike chain)
- Dashboard shows **±10 strikes** (CE/PE) with **BUY/SELL** buttons
- Position monitoring on option LTP with **auto square-off** on SL/Target/TSL
- Instrument master downloader + weekly option selector from Dhan scrip-master CSV

## Setup
```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
py -m pip install -r requirements.txt
```

## Run
```powershell
.\.venv\Scripts\Activate.ps1
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Open: `http://127.0.0.1:8000/`

If you prefer `uvicorn main:app`, this repo includes a top-level `main.py` shim.

## First-time steps (important)
1. Click **Refresh instruments** (downloads Dhan scrip-master CSV to `data/dhan_scrip_master.csv`).
2. Enter `client_id` + `access_token`, set params, click **Save settings**.
3. Click **Connect**.
4. Use the dashboard strike chain to place a **BUY** or **SELL** on CE/PE.

## Notes
- `LIMIT` orders need option LTP ticks (the dashboard subscribes to the displayed chain).
- Runtime files are stored in your user data folder (Windows: `%LOCALAPPDATA%\\niftyalgo\\`).
- `config.json` contains your `access_token`; keep your Windows user profile secure.
