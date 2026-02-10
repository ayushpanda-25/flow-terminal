# FLOW TERMINAL

Bloomberg-terminal-style live options flow dashboard powered by LSEG/Refinitiv real-time data.

**Zero cost** — every user brings their own LSEG credentials. Nothing is paid for by the developer.

---

## Three Ways to Run

| Mode | What You Need | Who It's For |
|------|--------------|-------------|
| **Demo** | Just the HTML file | Anyone — see the dashboard instantly |
| **Local** | HTML + Python + LSEG Workspace | Traders with LSEG on their machine |
| **Server** | **Just the HTML file** + a server URL | Remote users — no Python, no Workspace |

---

## Demo Mode (No Setup Required)

Open `options-flow-dashboard.html` in your browser and click **"Launch Demo Mode"**. Done. No Python, no API key, no accounts needed.

---

## Local Mode (Your Machine)

### What You Need

1. **LSEG Workspace Desktop** — installed and signed in on your machine
2. **Python 3.10+** — [python.org/downloads](https://python.org/downloads)
3. **An App Key** (free) — see [How to Get Your App Key](#how-to-get-your-app-key-free)

### Step-by-Step

```bash
# 1. Install Python dependencies
pip install refinitiv-data websockets

# 2. Make sure LSEG Workspace is running and signed in

# 3. Start the bridge
python local_bridge.py

# 4. Open the dashboard
#    Double-click options-flow-dashboard.html
#    or on Mac:  open options-flow-dashboard.html
#    or on Win:  start options-flow-dashboard.html

# 5. On the connection overlay, click "Connect Local"
```

You should see live streaming prices for SPY, AAPL, NVDA, TSLA, QQQ, and AMZN with options chain data updating every 30 seconds.

### Troubleshooting

| Problem | Fix |
|---------|-----|
| "Failed to open LSEG session" | Make sure Workspace is **running and signed in** (not just open) |
| Connection times out | Check proxy port — look at `%APPDATA%/Refinitiv/Data API Proxy/.portInUse` (Windows) |
| Blocked by firewall/antivirus | Run `set NO_PROXY=localhost` (Windows) or `export NO_PROXY=localhost` (Mac) before starting |
| `ModuleNotFoundError: refinitiv` | Run `pip install refinitiv-data websockets` again |
| Chain data shows all zeros | Your LSEG tier may not include options data — equity prices should still work |

---

## Server Mode (Remote Access)

### Connecting to a Server (No Python Required)

If someone is hosting `server.py` for you, **all you need is the HTML file**. No Python install, no LSEG Workspace, no terminal commands.

1. Open `options-flow-dashboard.html` in your browser
2. Select **"Server Mode"**
3. Enter the server URL they gave you (e.g. `wss://their-server.com/ws`)
4. Enter **your own** LSEG App Key, username, and password
5. Click **"Connect Server"**

That's it. Your credentials are sent over WebSocket to authenticate on your behalf. **Nothing is stored** — when you close the tab, your session and credentials are gone.

> **You still need an LSEG App Key + account** — the server just acts as a relay so you don't need Workspace Desktop installed locally. See [How to Get Your App Key](#how-to-get-your-app-key-free).

### Hosting a Server for Others (Python Required)

This is only for the person running the relay server, not the people connecting to it.

```bash
pip install -r requirements.txt
python server.py
# Runs on ws://localhost:8000/ws
```

Each person who connects uses their own LSEG credentials. You (the host) never see or store their credentials.

### Free Deployment Options

You need to make `server.py` accessible on the internet so others can connect. All of these are free:

**Option A: ngrok (quickest — run from your laptop)**
```bash
# 1. Install ngrok: https://ngrok.com/download (free account)
# 2. Start the server
python server.py
# 3. In another terminal, expose it
ngrok http 8000
# 4. Share the URL ngrok gives you (e.g. wss://abc123.ngrok-free.app/ws)
```
Good for testing. Stops when you close your laptop.

**Option B: Railway.app (easiest permanent deploy)**
1. Push `server.py`, `requirements.txt`, and `lseg-data.config.json` to a GitHub repo
2. Go to [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub repo**
3. Set the start command: `python server.py`
4. Railway gives you a public URL — share `wss://your-app.up.railway.app/ws`

**Option C: Render.com**
1. Push to GitHub
2. Go to [render.com](https://render.com) → **New Web Service** → connect your repo
3. Set build command: `pip install -r requirements.txt`
4. Set start command: `python server.py`
5. Share `wss://your-app.onrender.com/ws`

> **Note:** The server is very lightweight — it doesn't store data or credentials. A free tier is more than enough.

---

## How to Get Your App Key (Free)

1. Open **LSEG Workspace** on your desktop (or go to workspace.refinitiv.com)
2. In the search bar at the top, type **"appkey"** and press Enter
3. Click on **"App Key Generator"** in the results
4. Click **"Register New App"**
5. Fill in:
   - **App Display Name**: anything (e.g. "Flow Terminal")
   - **Type**: select "Desktop"
6. Click **"Register"**
7. Copy the **App Key** string that appears (looks like `d5f36229fb7344078aa2fa3de7e5fca8be6e11a6`)

That's it — the key is free and tied to your Workspace account.

---

## Files Included

| File | What It Does |
|------|-------------|
| `options-flow-dashboard.html` | The dashboard — open in any browser |
| `local_bridge.py` | Bridges LSEG Workspace to the dashboard via WebSocket |
| `server.py` | Cloud relay server for remote access (optional) |
| `requirements.txt` | Python dependencies (local bridge + server hosting only) |
| `lseg-data.config.json` | LSEG library config template |
| `.env.example` | Environment variables template |

## Architecture

```
LOCAL MODE:
  Browser (options-flow-dashboard.html)
      ↕ WebSocket on localhost:8765
  local_bridge.py (Python)
      ↕ LSEG Data Library
  LSEG Workspace Desktop
      ↕ Internet
  LSEG/Refinitiv data servers

SERVER MODE:
  Browser (options-flow-dashboard.html)     ← no Python needed here
      ↕ WebSocket (wss://...)
  server.py (Python, hosted somewhere)      ← Python needed here only
      ↕ LSEG Data Library
  LSEG/Refinitiv data servers
```

## Security

- **No credentials stored anywhere** — not on disk, not in a database, not in logs
- Local mode: all data stays on your machine (browser <-> localhost <-> Workspace)
- Server mode: credentials in memory only, garbage collected on disconnect
- Each user's LSEG session is fully isolated

## Features

- Real-time equity streaming (SPY, AAPL, NVDA, TSLA, QQQ, AMZN)
- Options chain viewer with 11-15 strikes centered on ATM
- Unusual options flow detection ranked by premium
- Put/Call ratio analysis and flow distribution charts
- Multi-expiration support
- Delayed data auto-detection with visual indicator
- Bloomberg dark theme (JetBrains Mono, black background, orange accents)
- All Chart.js animations disabled for terminal feel
